use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use kvproto::enginepb::{
    CommandRequestBatch, CommandResponse, CommandResponseBatch, CommandResponseHeader,
};
use kvproto::raft_cmdpb::{CmdType, Request};
use kvproto::raft_serverpb::RaftApplyState;
use rocksdb::{Writable, WriteBatch, WriteOptions, DB};

use super::keys::{self, escape};
use super::rocksdb_util;
use super::worker::{Runnable, Scheduler, Worker};

pub struct Engine {
    db: Arc<DB>,
    apply: Worker<Task>,
    applied_receiver: Option<UnboundedReceiver<CommandResponseBatch>>,
}

impl Engine {
    pub fn new(db: Arc<DB>) -> Engine {
        Engine {
            db,
            apply: Worker::new("apply"),
            applied_receiver: None,
        }
    }

    pub fn apply_scheduler(&self) -> Scheduler<Task> {
        self.apply.scheduler()
    }

    pub fn start(&mut self) {
        let (tx, rx) = unbounded();
        self.applied_receiver = Some(rx);
        let applier = ApplyWorker::new(self.db.clone(), tx);
        self.apply.start(applier).unwrap();
    }

    pub fn take_apply_receiver(&mut self) -> Option<UnboundedReceiver<CommandResponseBatch>> {
        self.applied_receiver.take()
    }

    pub fn stop(&mut self) {
        if let Some(handler) = self.apply.stop() {
            handler.join().unwrap()
        }
    }
}

pub struct Task {
    commands: CommandRequestBatch,
}

impl Task {
    pub fn new(commands: CommandRequestBatch) -> Task {
        Task { commands }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Apply Task")
    }
}

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;

struct ApplyContext {
    wb: WriteBatch,
}

impl ApplyContext {
    fn new() -> ApplyContext {
        ApplyContext {
            wb: WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE),
        }
    }
}

struct ApplyWorker {
    db: Arc<DB>,
    notifier: UnboundedSender<CommandResponseBatch>,

    apply_states: HashMap<u64, RaftApplyState>,
    apply_ctx: ApplyContext,
}

impl ApplyWorker {
    fn new(db: Arc<DB>, notifier: UnboundedSender<CommandResponseBatch>) -> ApplyWorker {
        ApplyWorker {
            db,
            notifier,
            apply_states: HashMap::new(),
            apply_ctx: ApplyContext::new(),
        }
    }

    fn apply_cmds(&mut self, mut cmds: CommandRequestBatch) {
        for mut cmd in cmds.take_requests().into_vec() {
            assert!(cmd.has_header());
            let header = cmd.take_header();
            for req in cmd.take_requests().into_vec() {
                let cmd_type = req.get_cmd_type();
                match cmd_type {
                    CmdType::Put => self.handle_put(header.get_region_id(), &req),
                    CmdType::Delete => self.handle_delete(header.get_region_id(), &req),
                    // CmdType::DeleteRange => {
                    //     self.handle_delete_range(req, &mut ranges, ctx.use_delete_range)
                    // }
                    // Readonly commands are handled in raftstore directly.
                    // Don't panic here in case there are old entries need to be applied.
                    // It's also safe to skip them here, because a restart must have happened,
                    // hence there is no callback to be called.
                    CmdType::DeleteRange | CmdType::IngestSST | CmdType::Snap | CmdType::Get => {
                        warn!(
                            "[region {}] skip unsupported command: {:?}",
                            header.get_region_id(),
                            req
                        );
                        continue;
                    }
                    CmdType::Prewrite | CmdType::Invalid => {
                        panic!("invalid cmd type, message maybe currupted")
                    }
                }
                self.apply_states
                    .entry(header.get_region_id())
                    .or_insert_with(RaftApplyState::new)
                    .set_applied_index(header.get_index())
            }
        }
    }

    fn handle_put(&mut self, region_id: u64, req: &Request) {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());

        let key = keys::data_key(key);
        if !req.get_put().get_cf().is_empty() {
            let cf = req.get_put().get_cf();
            rocksdb_util::get_cf_handle(&self.db, cf)
                .and_then(|handle| self.apply_ctx.wb.put_cf(handle, &key, value))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to write ({}, {}) to cf {}: {:?}",
                        region_id,
                        escape(&key),
                        escape(value),
                        cf,
                        e
                    )
                });
        } else {
            self.apply_ctx.wb.put(&key, value).unwrap_or_else(|e| {
                panic!(
                    "[region {}] failed to write ({}, {}): {:?}",
                    region_id,
                    escape(&key),
                    escape(value),
                    e
                );
            });
        }
    }

    fn handle_delete(&mut self, region_id: u64, req: &Request) {
        let key = req.get_delete().get_key();

        let key = keys::data_key(key);
        if !req.get_delete().get_cf().is_empty() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb_util::get_cf_handle(&self.db, cf)
                .and_then(|handle| self.apply_ctx.wb.delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to delete {}: {:?}",
                        region_id,
                        escape(&key),
                        e
                    )
                });
        } else {
            self.apply_ctx.wb.delete(&key).unwrap_or_else(|e| {
                panic!(
                    "[region {}] failed to delete {}: {:?}",
                    region_id,
                    escape(&key),
                    e
                )
            });
        }
    }

    fn report_apply_states(&self) {
        let mut resps = Vec::with_capacity(self.apply_states.len());
        for (region_id, applied_state) in &self.apply_states {
            let mut header = CommandResponseHeader::new();
            header.set_region_id(*region_id);
            let mut resp = CommandResponse::new();
            resp.set_header(header);
            resp.set_apply_state(applied_state.clone());
            resps.push(resp);
        }
        let mut resps_batch = CommandResponseBatch::new();
        resps_batch.set_responses(resps.into());
        self.notifier.unbounded_send(resps_batch).unwrap();
    }
}

impl Runnable<Task> for ApplyWorker {
    fn run_batch(&mut self, batch: &mut Vec<Task>) {
        for task in batch.drain(..) {
            self.apply_cmds(task.commands);
        }
    }

    fn on_tick(&mut self) {
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true /* TODO: consider header.sync_log */);
        let mut wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        ::std::mem::swap(&mut self.apply_ctx.wb, &mut wb);
        self.db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        self.report_apply_states();
    }

    fn shutdown(&mut self) {
        self.on_tick();
    }
}

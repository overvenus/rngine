use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use futures::sync::mpsc::UnboundedSender;
use kvproto::enginepb::{
    CommandRequestBatch, CommandResponse, CommandResponseBatch, CommandResponseHeader,
    SnapshotState,
};
use kvproto::raft_cmdpb::{CmdType, Request};
use kvproto::raft_serverpb::RaftApplyState;
use protobuf::Message;
use rocksdb::{DBIterator, Writable, WriteBatch, WriteOptions, DB};

use super::super::keys::{self, escape};
use super::super::rocksdb_util::{self, CF_DEFAULT};
use super::super::worker::Runnable;

pub enum Task {
    Commands { commands: CommandRequestBatch },
    Snapshot { state: SnapshotState },
}

impl Task {
    pub fn commands(commands: CommandRequestBatch) -> Task {
        Task::Commands { commands }
    }

    pub fn snapshot(state: SnapshotState) -> Task {
        Task::Snapshot { state }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Apply Task")
    }
}

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;

pub struct Runner {
    db: Arc<DB>,
    notifier: UnboundedSender<CommandResponseBatch>,

    // region id -> apply state
    apply_states: HashMap<u64, RaftApplyState>,
    wb: WriteBatch,
}

impl Runner {
    pub fn new(db: Arc<DB>, notifier: UnboundedSender<CommandResponseBatch>) -> Runner {
        let mut worker = Runner {
            db,
            notifier,
            apply_states: HashMap::new(),
            wb: WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE),
        };

        worker.restore_apply_states();
        // Report apply states as soon as possible.
        worker.report_apply_states();
        worker
    }

    fn restore_apply_states(&mut self) {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;

        let iter_opt =
            rocksdb_util::IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), false);
        let handle = rocksdb_util::get_cf_handle(&self.db, CF_DEFAULT).unwrap();
        let readopts = iter_opt.build_read_opts();
        let iter = DBIterator::new_cf(self.db.as_ref(), handle, readopts);

        let apply_states = &mut self.apply_states;
        rocksdb_util::scan_db_iterator(iter, start_key, |key, value| {
            let (region_id, suffix) = keys::decode_apply_state_key(key)?;
            match suffix {
                keys::APPLY_STATE_SUFFIX => {
                    let state =
                        protobuf::parse_from_bytes::<RaftApplyState>(value).unwrap_or_else(|e| {
                            panic!(
                                "[region {}] currupted apply state, key: {:?}, error: {:?}",
                                region_id,
                                escape(key),
                                e
                            )
                        });
                    apply_states.insert(region_id, state);
                }
                keys::SNAPSHOT_RAFT_STATE_SUFFIX => {
                    // TODO: restore snapshot states.
                }
                _ => {
                    warn!("unknown key: {:?}", escape(key));
                }
            }
            Ok(true)
        }).unwrap();
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
                .and_then(|handle| self.wb.put_cf(handle, &key, value))
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
            self.wb.put(&key, value).unwrap_or_else(|e| {
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
                .and_then(|handle| self.wb.delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to delete {}: {:?}",
                        region_id,
                        escape(&key),
                        e
                    )
                });
        } else {
            self.wb.delete(&key).unwrap_or_else(|e| {
                panic!(
                    "[region {}] failed to delete {}: {:?}",
                    region_id,
                    escape(&key),
                    e
                )
            });
        }
    }

    fn persist_apply(&mut self) {
        let mut buffer = Vec::new();
        for (region_id, applied_state) in &self.apply_states {
            applied_state.write_to_writer(&mut buffer).unwrap();
            let region_key = keys::apply_state_key(*region_id);
            self.wb.put(&region_key, &buffer).unwrap_or_else(|e| {
                panic!(
                    "[region {}] failed to delete {}: {:?}",
                    region_id,
                    escape(&region_key),
                    e
                )
            });
        }
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true /* TODO: consider header.sync_log */);
        let mut wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        ::std::mem::swap(&mut self.wb, &mut wb);
        self.db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });
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

impl Runnable<Task> for Runner {
    fn run_batch(&mut self, batch: &mut Vec<Task>) {
        for task in batch.drain(..) {
            match task {
                Task::Commands { commands } => {
                    self.apply_cmds(commands);
                }
                Task::Snapshot { mut state } => {
                    let region_id = state.get_region().get_id();
                    let apply_state = state.take_apply_state();
                    self.apply_states.insert(region_id, apply_state);
                }
            }
        }
    }

    fn on_tick(&mut self) {
        self.persist_apply();
        self.report_apply_states();
    }

    fn shutdown(&mut self) {
        self.on_tick();
    }
}

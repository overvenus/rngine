use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::sync::mpsc::UnboundedSender;
use kvproto::enginepb::{
    CommandRequest, CommandRequestBatch, CommandResponse, CommandResponseBatch,
};
use kvproto::metapb;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use raft::eraftpb;
use rocksdb::{DBIterator, Writable, WriteBatch, WriteOptions, DB};

use super::super::keys::{self, escape};
use super::super::rocksdb_util::{self, CF_DEFAULT};
use super::super::worker::{Runnable, RunnableWithTimer, Timer};
use super::{initial_apply_state, RegionMeta};

pub enum Task {
    Commands { commands: CommandRequestBatch },
    Snap { meta: RegionMeta },
}

impl Task {
    pub fn commands(commands: CommandRequestBatch) -> Task {
        Task::Commands { commands }
    }

    pub fn snap(meta: RegionMeta) -> Task {
        Task::Snap { meta }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Apply Task")
    }
}

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;

struct Delegate {
    meta: RegionMeta,
    wb: WriteBatch,
    pending_remove: bool,

    tag: String,
}

impl Delegate {
    fn from_region_meta(meta: RegionMeta) -> Delegate {
        let tag = format!("[region {}]", meta.region.get_id());
        info!("{} create delegate with {:?}", tag, meta);
        Delegate {
            meta,
            wb: WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE),
            pending_remove: false,
            tag,
        }
    }

    fn persist(&mut self, db: &DB) -> CommandResponse {
        info!(
            "{} persist apply delegate {:?}",
            self.tag, self.meta.apply_state
        );

        let apply_state_key = keys::apply_state_key(self.meta.region.get_id());
        let mut buffer = Vec::new();
        self.meta.write_to(&mut buffer).unwrap();
        self.wb.put(&apply_state_key, &buffer).unwrap_or_else(|e| {
            panic!(
                "{} failed to persist apply state {}: {:?}",
                self.tag,
                escape(&apply_state_key),
                e
            )
        });

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        let mut wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        ::std::mem::swap(&mut self.wb, &mut wb);
        db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        self.meta.to_command_response()
    }

    fn destroy(&mut self, db: &DB) -> CommandResponse {
        assert!(self.pending_remove);
        info!(
            "{} destroy apply delegate {:?}",
            self.tag, self.meta.apply_state
        );

        let start_key = keys::enc_start_key(&self.meta.region);
        let end_key = keys::enc_end_key(&self.meta.region);
        assert!(start_key <= end_key);

        rocksdb_util::delete_all_in_range(db, &start_key, &end_key, &self.wb);
        let apply_state_key = keys::apply_state_key(self.meta.region.get_id());
        self.wb.delete(&apply_state_key).unwrap_or_else(|e| {
            panic!(
                "{} failed to delete {}: {:?}",
                self.tag,
                escape(&apply_state_key),
                e
            )
        });

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        let mut wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        ::std::mem::swap(&mut self.wb, &mut wb);
        db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        self.meta.to_command_response()
    }

    fn apply_cmd(&mut self, mut cmd: CommandRequest, db: &DB) -> (bool, Option<Vec<RegionMeta>>) {
        assert!(!self.pending_remove);
        let header = cmd.take_header();
        let term = header.get_term();
        let index = header.get_index();

        let mut metas = None;
        if cmd.has_admin_request() {
            let request = cmd.take_admin_request();
            let response = cmd.take_admin_response();
            let cmd_type = request.get_cmd_type();
            info!(
                "{} execute admin command {:?} at [term: {}, index: {}]",
                self.tag, request, term, index
            );

            // Caller must mark it as a sync log request.
            assert!(header.get_sync_log());
            match cmd_type {
                AdminCmdType::ChangePeer => self.exec_change_peer(&request, response),
                AdminCmdType::BatchSplit => metas = self.exec_batch_split(&request, response),
                // To support compact log, we need to persist all changes to disk.
                AdminCmdType::CompactLog => (),
                other => {
                    error!("unsupported admin command type {:?}", other);
                }
            }
        } else {
            for req in cmd.take_requests().into_vec() {
                let cmd_type = req.get_cmd_type();
                match cmd_type {
                    CmdType::Put => self.handle_put(&req, db),
                    CmdType::Delete => self.handle_delete(&req, db),
                    // Following commands are not supported.
                    CmdType::DeleteRange | CmdType::IngestSST | CmdType::Snap | CmdType::Get => {
                        warn!("{} skip unsupported command: {:?}", self.tag, req);
                        continue;
                    }
                    CmdType::Prewrite | CmdType::Invalid => {
                        panic!("invalid cmd type, message maybe currupted")
                    }
                }
            }
        }

        // Advance applied index and term.
        debug!(
            "{} advance applied index {} and term {}",
            self.tag, index, term
        );
        self.meta.apply_state.set_applied_index(index);
        self.meta.applied_term = term;

        (header.get_sync_log(), metas)
    }

    fn handle_put(&mut self, req: &Request, db: &DB) {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());

        let key = keys::data_key(key);
        if !req.get_put().get_cf().is_empty() {
            let cf = req.get_put().get_cf();
            rocksdb_util::get_cf_handle(db, cf)
                .and_then(|handle| self.wb.put_cf(handle, &key, value))
                .unwrap_or_else(|e| {
                    panic!(
                        "{} failed to write ({}, {}) to cf {}: {:?}",
                        self.tag,
                        escape(&key),
                        escape(value),
                        cf,
                        e
                    )
                });
        } else {
            self.wb.put(&key, value).unwrap_or_else(|e| {
                panic!(
                    "{} failed to write ({}, {}): {:?}",
                    self.tag,
                    escape(&key),
                    escape(value),
                    e
                );
            });
        }
    }

    fn handle_delete(&mut self, req: &Request, db: &DB) {
        let key = req.get_delete().get_key();

        let key = keys::data_key(key);
        if !req.get_delete().get_cf().is_empty() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb_util::get_cf_handle(db, cf)
                .and_then(|handle| self.wb.delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
                });
        } else {
            self.wb.delete(&key).unwrap_or_else(|e| {
                panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
            });
        }
    }

    fn exec_change_peer(&mut self, request: &AdminRequest, mut response: AdminResponse) {
        let request = request.get_change_peer();
        let new_region = response.take_change_peer().take_region();

        info!(
            "{} exec {:?}, region {:?} -> {:?}",
            self.tag,
            request,
            self.meta.region.get_peers(),
            new_region.get_peers()
        );

        match request.get_change_type() {
            eraftpb::ConfChangeType::AddNode | eraftpb::ConfChangeType::AddLearnerNode => {
                self.meta.region = new_region;
            }
            eraftpb::ConfChangeType::RemoveNode => {
                let peer = request.get_peer();
                if self.meta.peer.get_id() == peer.get_id() {
                    // Remove ourself, we will destroy all region data later.
                    // So we need not to apply following logs.
                    self.pending_remove = true;
                    info!("{} removes self", self.tag);
                }
            }
        }
    }

    fn exec_batch_split(
        &mut self,
        req: &AdminRequest,
        mut response: AdminResponse,
    ) -> Option<Vec<RegionMeta>> {
        let split_reqs = req.get_splits();
        let new_regions = response.take_splits().take_regions().into_vec();
        if split_reqs.get_requests().is_empty() {
            error!("missing split requests");
            return None;
        }
        info!(
            "{} exec {:?}, region {:?} -> regions {:?}",
            self.tag, split_reqs, self.meta.region, new_regions,
        );

        let mut metas = Vec::with_capacity(split_reqs.get_requests().len() - 1);
        let store_id = self.meta.peer.get_store_id();
        for r in new_regions {
            if r.get_id() == self.meta.region.get_id() {
                self.meta.region = r;
            } else {
                let peer = find_peer(&r, store_id).unwrap();
                let apply_state = initial_apply_state();
                metas.push(RegionMeta::new(peer.to_owned(), r, apply_state));
            }
        }
        Some(metas)
    }
}

fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    region
        .get_peers()
        .iter()
        .find(|&p| p.get_store_id() == store_id)
}

pub struct Runner {
    db: Arc<DB>,
    notifier: UnboundedSender<CommandResponseBatch>,

    // region id -> apply state
    delegates: HashMap<u64, Delegate>,
    pending_sync_delegates: HashSet<u64>,
    persist_interval: Duration,
}

impl Runner {
    pub fn new(
        db: Arc<DB>,
        notifier: UnboundedSender<CommandResponseBatch>,
        persist_interval: Duration,
    ) -> Runner {
        let mut runner = Runner {
            db,
            notifier,
            persist_interval,
            delegates: HashMap::new(),
            pending_sync_delegates: HashSet::new(),
        };

        let resps = runner.restore_apply_states();
        // Report apply states as soon as possible.
        runner.report_applied(resps);
        runner
    }

    pub fn timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);

        timer.add_task(self.persist_interval, ());
        timer
    }

    fn restore_apply_states(&mut self) -> Vec<CommandResponse> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_RAFT_MIN_KEY;
        let end_key = keys::REGION_RAFT_MAX_KEY;

        let iter_opt =
            rocksdb_util::IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), false);
        let handle = rocksdb_util::get_cf_handle(&self.db, CF_DEFAULT).unwrap();
        let readopts = iter_opt.build_read_opts();
        let iter = DBIterator::new_cf(self.db.as_ref(), handle, readopts);

        let mut resps = Vec::new();
        let delegates = &mut self.delegates;
        rocksdb_util::scan_db_iterator(iter, start_key, |key, value| {
            let (region_id, suffix) = keys::decode_apply_state_key(key)?;
            if suffix == keys::APPLY_STATE_SUFFIX {
                let meta = RegionMeta::parse(value);
                info!("[region {}] restore apply delegate {:?}", region_id, meta);
                resps.push(meta.to_command_response());
                let d = Delegate::from_region_meta(meta);
                delegates.insert(region_id, d);
            } else {
                warn!("unknown key: {:?}", escape(key));
            }
            Ok(true)
        })
        .unwrap();
        info!("restore apply delegates, total: {}", delegates.len());
        resps
    }

    fn report_applied(&self, resps: Vec<CommandResponse>) {
        info!("report apply delegates, total: {}", resps.len());
        let mut resps_batch = CommandResponseBatch::new();
        resps_batch.set_responses(resps.into());
        if let Err(e) = self.notifier.unbounded_send(resps_batch) {
            error!("response notifier is closed {:?}", e);
        }
    }

    fn apply_cmds(&mut self, mut cmds: CommandRequestBatch) {
        for cmd in cmds.take_requests().into_vec() {
            assert!(cmd.has_header());
            let region_id = cmd.get_header().get_region_id();
            let delegate;
            if let Some(d) = self.delegates.get_mut(&region_id) {
                delegate = d;
            } else {
                error!("[region {}] is missing", region_id);
                continue;
            }
            if delegate.pending_remove {
                debug!("{} is pending remove, drop cmd {:?}", delegate.tag, cmd);
                continue;
            }
            let (sync_log, metas) = delegate.apply_cmd(cmd, &self.db);
            if sync_log {
                self.pending_sync_delegates.insert(region_id);
            }
            if let Some(metas) = metas {
                for m in metas {
                    // The region is created by split.
                    self.insert_delegates(m, false);
                }
            }
        }
    }

    fn insert_delegates(&mut self, meta: RegionMeta, report: bool) {
        let region_id = meta.region.get_id();
        self.delegates
            .insert(region_id, Delegate::from_region_meta(meta));
        if report {
            self.pending_sync_delegates.insert(region_id);
        }
    }
}

impl Runnable<Task> for Runner {
    fn run_batch(&mut self, batch: &mut Vec<Task>) {
        for task in batch.drain(..) {
            match task {
                Task::Commands { commands } => {
                    self.apply_cmds(commands);
                }
                Task::Snap { meta } => {
                    // The region is created by snapshot.
                    self.insert_delegates(meta, true);
                }
            }
        }
    }

    fn on_tick(&mut self) {
        let mut resps = Vec::with_capacity(self.pending_sync_delegates.len());
        for region_id in self.pending_sync_delegates.drain() {
            let remove = {
                let delegate = self.delegates.get_mut(&region_id).unwrap();
                let resp = if delegate.pending_remove {
                    delegate.destroy(&self.db)
                } else {
                    delegate.persist(&self.db)
                };
                resps.push(resp);
                delegate.pending_remove
            };
            if remove {
                info!("[region {}] is removed", region_id,);
                self.delegates.remove(&region_id);
            }
        }
        if !resps.is_empty() {
            self.report_applied(resps);
        }
    }

    fn shutdown(&mut self) {
        info!(
            "persist all apply delegates before shutdown, total: {}",
            self.delegates.len()
        );
        let mut resps = Vec::with_capacity(self.pending_sync_delegates.len());
        for delegate in &mut self.delegates.values_mut() {
            resps.push(delegate.persist(&self.db));
        }
        if !resps.is_empty() {
            self.report_applied(resps);
        }
    }
}

impl RunnableWithTimer<Task, ()> for Runner {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        info!("persist apply delegates, total: {}", self.delegates.len());
        for region_id in self.delegates.keys() {
            self.pending_sync_delegates.insert(*region_id);
        }

        timer.add_task(self.persist_interval, ());
    }
}

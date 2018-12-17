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
use kvproto::raft_serverpb::RaftApplyState;
use raft::eraftpb;
use rocksdb::{DBIterator, Writable, WriteBatch, WriteOptions, DB};

use super::super::keys::{self, escape};
use super::super::rocksdb_util::{self, Snapshot, CF_DEFAULT};
use super::super::worker::{Runnable, RunnableWithTimer, Scheduler, Timer};
use super::{initial_apply_state, write_region_meta, RegionMeta};
use super::{ConsistencyTask, RegionTask};

pub enum Task {
    Commands {
        commands: CommandRequestBatch,
    },
    Snap {
        meta: RegionMeta,
    },
    Destroyed {
        region_id: u64,
        // applied_term and apply state.
        apply_state: Option<(u64, RaftApplyState)>,
    },
}

impl Task {
    pub fn commands(commands: CommandRequestBatch) -> Task {
        Task::Commands { commands }
    }

    pub fn snap(meta: RegionMeta) -> Task {
        Task::Snap { meta }
    }

    pub fn destroyed(region_id: u64, apply_state: Option<(u64, RaftApplyState)>) -> Task {
        Task::Destroyed {
            region_id,
            apply_state,
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Apply Task")
    }
}

#[derive(Debug)]
enum ExecRes {
    Split {
        splitted_region_metas: Vec<RegionMeta>,
    },
    ComputeHash {
        index: u64,
        region: metapb::Region,
        raft_local_state: Vec<u8>,
    },
    VerifyHash {
        index: u64,
        region: metapb::Region,
        hash: Vec<u8>,
    },
    RemoveNode,
}

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;

struct Delegate {
    meta: RegionMeta,
    wb: WriteBatch,
    pending_remove: bool,
    pending_exec_res: Option<ExecRes>,

    tag: String,
}

impl Delegate {
    fn from_region_meta(meta: RegionMeta) -> Delegate {
        let tag = format!("[region {}]", meta.region.get_id());
        info!("{} create delegate with {:?}", tag, meta);
        Delegate {
            meta,
            wb: WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE),
            pending_exec_res: None,
            pending_remove: false,
            tag,
        }
    }

    fn persist(&mut self, db: &DB) -> CommandResponse {
        info!(
            "{} persist apply delegate {:?}",
            self.tag, self.meta.apply_state
        );

        write_region_meta(&self.meta, &self.wb);

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        let mut wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        ::std::mem::swap(&mut self.wb, &mut wb);
        db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        self.meta.to_command_response()
    }

    fn apply_cmd(&mut self, mut cmd: CommandRequest, db: &DB) -> bool {
        assert!(!self.pending_remove);
        let mut header = cmd.take_header();
        let term = header.get_term();
        let index = header.get_index();

        // Zero index and term means TiKV wants us to persist data.
        if term == 0 && index == 0 {
            info!("{} try to persist", self.tag);
            assert!(header.get_sync_log());
            return true;
        }

        let mut exec_res = None;
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
                AdminCmdType::ChangePeer => exec_res = self.exec_change_peer(&request, response),
                AdminCmdType::BatchSplit => exec_res = self.exec_batch_split(&request, response),
                // To support compact log, we need to persist all changes to disk.
                AdminCmdType::CompactLog => (),
                AdminCmdType::ComputeHash => {
                    let ctx = header.take_context();
                    exec_res = self.exec_compute_hash(index, ctx);
                }
                AdminCmdType::VerifyHash => {
                    exec_res = self.exec_verify_hash(&request);
                }
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

        if exec_res.is_some() {
            assert!(self.pending_exec_res.is_none());
            self.pending_exec_res = exec_res;
        }
        header.get_sync_log()
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

    fn exec_change_peer(
        &mut self,
        request: &AdminRequest,
        mut response: AdminResponse,
    ) -> Option<ExecRes> {
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
                None
            }
            eraftpb::ConfChangeType::RemoveNode => {
                let peer = request.get_peer();
                if self.meta.peer.get_id() == peer.get_id() {
                    // Remove ourself, we will destroy all region data later.
                    // So we need not to apply following logs.
                    self.pending_remove = true;
                    info!("{} removes self", self.tag);
                    Some(ExecRes::RemoveNode)
                } else {
                    None
                }
            }
        }
    }

    fn exec_batch_split(
        &mut self,
        req: &AdminRequest,
        mut response: AdminResponse,
    ) -> Option<ExecRes> {
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
                let meta = RegionMeta::new(peer.to_owned(), r, apply_state);
                // Write new region metas and batch split in the same write.
                write_region_meta(&meta, &self.wb);
                metas.push(meta);
            }
        }
        Some(ExecRes::Split {
            splitted_region_metas: metas,
        })
    }

    fn exec_compute_hash(&mut self, index: u64, req_ctx: Vec<u8>) -> Option<ExecRes> {
        Some(ExecRes::ComputeHash {
            index,
            region: self.meta.region.clone(),
            raft_local_state: req_ctx,
        })
    }

    fn exec_verify_hash(&mut self, req: &AdminRequest) -> Option<ExecRes> {
        let verify_req = req.get_verify_hash();
        let index = verify_req.get_index();
        let hash = verify_req.get_hash().to_vec();

        Some(ExecRes::VerifyHash {
            index,
            region: self.meta.region.clone(),
            hash,
        })
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
    pending_sync_delegates: Option<HashSet<u64>>,
    persist_interval: Duration,
    consistency_scheduler: Scheduler<ConsistencyTask>,
    region_scheduler: Scheduler<RegionTask>,
}

impl Runner {
    pub fn new(
        db: Arc<DB>,
        notifier: UnboundedSender<CommandResponseBatch>,
        persist_interval: Duration,
        consistency_scheduler: Scheduler<ConsistencyTask>,
        region_scheduler: Scheduler<RegionTask>,
    ) -> Runner {
        let mut runner = Runner {
            db,
            notifier,
            persist_interval,
            consistency_scheduler,
            region_scheduler,
            delegates: HashMap::new(),
            pending_sync_delegates: Some(HashSet::new()),
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

    fn report_destoryed(&self, region_id: u64, apply_state: Option<(u64, RaftApplyState)>) {
        let mut resp = CommandResponse::new();
        resp.mut_header().set_region_id(region_id);
        if let Some((term, apply_state)) = apply_state {
            // The region was destroyed by conf change(remove node).
            info!(
                "[region {}] report apply delegates destroyed by remove node\
                 at term {} apply state {:?}",
                region_id, term, apply_state
            );
            resp.set_applied_term(term);
            resp.set_apply_state(apply_state);
        } else {
            // The region was destroyed by tombstone messages.
            info!(
                "[region {}] report apply delegates destroyed by tombstone message",
                region_id
            );
            resp.mut_header().set_destroyed(true);
        }
        let mut resps_batch = CommandResponseBatch::new();
        resps_batch.set_responses(vec![resp].into());
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
            if cmd.get_header().get_destroy() {
                info!(
                    "[region {}] is going to be destroyed by tombstone ...",
                    region_id
                );
                delegate.pending_remove = true;
                self.pending_sync_delegates
                    .as_mut()
                    .unwrap()
                    .insert(region_id);
                continue;
            }
            if delegate.pending_remove {
                debug!("{} is pending remove, drop cmd {:?}", delegate.tag, cmd);
                continue;
            }
            let sync_log = delegate.apply_cmd(cmd, &self.db);
            if sync_log {
                self.pending_sync_delegates
                    .as_mut()
                    .unwrap()
                    .insert(region_id);
            }
        }
    }

    fn handle_exec_res(&mut self, exec_res: ExecRes) {
        match exec_res {
            ExecRes::Split {
                splitted_region_metas,
            } => {
                for m in splitted_region_metas {
                    // The region is created by split.
                    self.insert_delegates(m);
                }
            }
            ExecRes::ComputeHash {
                index,
                region,
                raft_local_state,
            } => {
                let snap = Snapshot::new(self.db.clone());
                let reigon_id = region.get_id();
                let task = ConsistencyTask::compute_hash(region, index, snap, raft_local_state);
                if let Err(e) = self.consistency_scheduler.schedule(task) {
                    error!(
                        "[region {}] fail to schedule consistency task {}",
                        reigon_id,
                        e.into_inner()
                    );
                }
            }
            ExecRes::VerifyHash {
                index,
                region,
                hash,
            } => {
                let reigon_id = region.get_id();
                let task = ConsistencyTask::verify_hash(region, index, hash);
                if let Err(e) = self.consistency_scheduler.schedule(task) {
                    error!(
                        "[region {}] fail to schedule consistency task {}",
                        reigon_id,
                        e.into_inner()
                    );
                }
            }
            ExecRes::RemoveNode => unreachable!(),
        }
    }

    fn insert_delegates(&mut self, meta: RegionMeta) {
        let region_id = meta.region.get_id();
        self.delegates
            .insert(region_id, Delegate::from_region_meta(meta));
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
                    self.insert_delegates(meta);
                }
                Task::Destroyed {
                    region_id,
                    apply_state,
                } => {
                    self.report_destoryed(region_id, apply_state);
                }
            }
        }
    }

    fn on_tick(&mut self) {
        let mut pending_sync_delegates = self.pending_sync_delegates.take().unwrap();

        let mut resps = Vec::with_capacity(pending_sync_delegates.len());
        for region_id in pending_sync_delegates.drain() {
            let (pending_remove, exec_res) = {
                let delegate = self.delegates.get_mut(&region_id).unwrap();
                if !delegate.pending_remove {
                    let resp = delegate.persist(&self.db);
                    resps.push(resp);
                };
                (delegate.pending_remove, delegate.pending_exec_res.take())
            };
            if pending_remove {
                debug!(
                    "[region {}] is removed with pending_exec_res {:?}",
                    region_id, exec_res
                );
                let d = self.delegates.remove(&region_id).unwrap();
                let by_conchange = match exec_res {
                    Some(ExecRes::RemoveNode) => true,
                    None => false,
                    other => panic!("unexpected {:?}", other),
                };
                self.region_scheduler
                    .schedule(RegionTask::destroy(d.meta, by_conchange))
                    .unwrap();
                continue;
            }
            if let Some(exec_res) = exec_res {
                self.handle_exec_res(exec_res);
            }
        }
        if !resps.is_empty() {
            self.report_applied(resps);
        }

        self.pending_sync_delegates = Some(pending_sync_delegates);
    }

    fn shutdown(&mut self) {
        info!(
            "persist all apply delegates before shutdown, total: {}",
            self.delegates.len()
        );
        let mut resps = Vec::with_capacity(self.delegates.len());
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
        let pending_sync_delegates = self.pending_sync_delegates.as_mut().unwrap();
        for region_id in self.delegates.keys() {
            pending_sync_delegates.insert(*region_id);
        }

        timer.add_task(self.persist_interval, ());
    }
}

#[cfg(test)]
mod tests {
    use kvproto::raft_cmdpb::{BatchSplitRequest, SplitRequest};

    use self::rocksdb_util::create_tmp_db;
    use super::*;

    use crate::worker::Worker;

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::new();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    #[test]
    fn test_split() {
        let (_tmp, db) = create_tmp_db("apply-basic");
        let (notifier, _rx) = futures::sync::mpsc::unbounded();
        let rworker = Worker::new("region-worker");
        let cworker = Worker::new("consistency-worker");
        let mut runner = Runner::new(
            db.clone(),
            notifier,
            Duration::from_secs(1),
            cworker.scheduler(),
            rworker.scheduler(),
        );

        let mut peer2 = metapb::Peer::new();
        peer2.set_id(2);
        let mut peer3 = metapb::Peer::new();
        peer3.set_id(3);
        peer3.set_is_learner(true);
        let mut region1 = metapb::Region::new();
        region1.set_id(1);
        region1.set_peers(vec![peer2.clone(), peer3.clone()].into());
        let apply_state = initial_apply_state();

        let meta = RegionMeta::new(peer3, region1.clone(), apply_state);
        runner.delegates.insert(1, Delegate::from_region_meta(meta));

        // split region 1 to region4["", "k1") and region1["k1", "")
        let mut admin = AdminRequest::new();
        admin.set_cmd_type(AdminCmdType::BatchSplit);
        let mut splits = BatchSplitRequest::new();
        splits.set_right_derive(true);
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 4, vec![5, 6]));
        admin.set_splits(splits);

        let mut peer5 = metapb::Peer::new();
        peer5.set_id(5);
        let mut peer6 = metapb::Peer::new();
        peer6.set_id(6);
        peer6.set_is_learner(true);
        let mut region4 = metapb::Region::new();
        region4.set_id(4);
        region4.set_peers(vec![peer5.clone(), peer6.clone()].into());
        region4.set_end_key("k1".into());

        let mut admin_resp = AdminResponse::new();
        region1.set_start_key("k1".into());
        admin_resp
            .mut_splits()
            .set_regions(vec![region1, region4].into());

        let mut req = CommandRequest::new();
        req.mut_header().set_region_id(1);
        req.mut_header().set_index(100);
        req.mut_header().set_term(100);
        req.mut_header().set_sync_log(true);
        req.set_admin_request(admin);
        req.set_admin_response(admin_resp);

        let mut cmd = CommandRequestBatch::new();
        cmd.set_requests(vec![req].into());

        let split_task = Task::commands(cmd);

        runner.run_batch(&mut vec![split_task]);
        assert!(runner.pending_sync_delegates.as_mut().unwrap().contains(&1));
        assert!(!runner.pending_sync_delegates.as_mut().unwrap().contains(&4));

        // runner persist pending sync region in on_tick.
        assert!(!runner.delegates.get(&1).unwrap().wb.is_empty());
        runner.on_tick();
        assert!(runner.delegates.get(&4).is_some());
        assert!(runner.delegates.get(&1).unwrap().wb.is_empty());

        // Make sure splitted regions and split commands are applied atomically.
        let key_r1 = keys::apply_state_key(1);
        let value1 = db.get(&key_r1).unwrap().unwrap();
        let meta1 = RegionMeta::parse(&*value1);
        assert_eq!(meta1, runner.delegates.get(&1).unwrap().meta);

        let key_r4 = keys::apply_state_key(4);
        let value4 = db.get(&key_r4).unwrap().unwrap();
        let meta4 = RegionMeta::parse(&*value4);
        assert_eq!(meta4, runner.delegates.get(&4).unwrap().meta);
    }
}

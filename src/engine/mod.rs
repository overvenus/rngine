use std::io::{self, Write};
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use kvproto::enginepb::{CommandResponse, CommandResponseBatch, CommandResponseHeader};
use kvproto::metapb;
use kvproto::raft_serverpb::RaftApplyState;
use protobuf::Message;
use rocksdb::{Writable, WriteBatch, DB};

use super::worker::{Scheduler, Worker};
use super::{config, keys};

mod apply;
mod consistency;
mod region;

pub use self::apply::{Runner as ApplyRunner, Task as ApplyTask};
pub use self::consistency::{Runner as ConsistencyRunner, Task as ConsistencyTask};
pub use self::region::{Runner as RegionRunner, Task as RegionTask};

pub struct Engine {
    db: Arc<DB>,
    apply_worker: Worker<ApplyTask>,
    consistency_worker: Worker<ConsistencyTask>,
    region_worker: Worker<RegionTask>,
    applied_receiver: Option<UnboundedReceiver<CommandResponseBatch>>,
}

impl Engine {
    pub fn new(db: Arc<DB>) -> Engine {
        Engine {
            db,
            apply_worker: Worker::new("apply-worker"),
            region_worker: Worker::new("region-worker"),
            consistency_worker: Worker::new("consistency-worker"),
            applied_receiver: None,
        }
    }

    pub fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    pub fn region_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    pub fn start(&mut self, cfg: &config::RgConfig) {
        let consistency_scheduler = self.consistency_worker.scheduler();
        let apply_scheduler = self.apply_scheduler();
        let region_scheduler = self.region_scheduler();

        // Start consistency_worker.
        let consistency_runner = ConsistencyRunner::new();
        self.consistency_worker.start(consistency_runner).unwrap();

        // Start region_worker.
        let region_runner = RegionRunner::new(self.db.clone(), apply_scheduler);
        self.region_worker.start(region_runner).unwrap();

        // Start apply_worker.
        let (tx, rx) = unbounded();
        self.applied_receiver = Some(rx);
        let apply_runner = ApplyRunner::new(
            self.db.clone(),
            tx,
            cfg.persist_interval.0,
            consistency_scheduler,
            region_scheduler,
        );
        let apply_timer = apply_runner.timer();
        self.apply_worker
            .start_with_timer(apply_runner, apply_timer)
            .unwrap();
    }

    pub fn take_apply_receiver(&mut self) -> Option<UnboundedReceiver<CommandResponseBatch>> {
        self.applied_receiver.take()
    }

    pub fn stop(&mut self) {
        if let Some(handler) = self.apply_worker.stop() {
            handler.join().unwrap()
        }

        if let Some(handler) = self.region_worker.stop() {
            handler.join().unwrap()
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct RegionMeta {
    pub peer: metapb::Peer,
    pub region: metapb::Region,
    pub apply_state: RaftApplyState,
    pub applied_term: u64,
}

impl RegionMeta {
    pub fn new(
        peer: metapb::Peer,
        region: metapb::Region,
        apply_state: RaftApplyState,
    ) -> RegionMeta {
        RegionMeta {
            peer,
            region,
            applied_term: apply_state.get_truncated_state().get_term(),
            apply_state,
        }
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buf = vec![0u8; 8];
        // applied_term: u64,
        BigEndian::write_u64(&mut buf, self.applied_term);
        writer.write_all(&buf)?;
        buf.clear();

        let mut len_buf = [0u8; 8];

        // apply_state: RaftApplyState,
        self.apply_state.write_to_vec(&mut buf)?;
        BigEndian::write_u64(&mut len_buf, buf.len() as u64);
        writer.write_all(&len_buf)?;
        writer.write_all(&buf)?;
        buf.clear();
        len_buf = [0u8; 8];

        // region: metapb::Region,
        self.region.write_to_vec(&mut buf)?;
        BigEndian::write_u64(&mut len_buf, buf.len() as u64);
        writer.write_all(&len_buf)?;
        writer.write_all(&buf)?;
        buf.clear();
        len_buf = [0u8; 8];

        // peer: metapb::Peer,
        self.peer.write_to_vec(&mut buf)?;
        BigEndian::write_u64(&mut len_buf, buf.len() as u64);
        writer.write_all(&len_buf)?;
        writer.write_all(&buf)?;

        return Ok(());
    }

    pub fn parse(bytes: &[u8]) -> RegionMeta {
        assert!(bytes.len() >= 8 /* size of u64 */);

        // applied_term: u64,
        let applied_term = BigEndian::read_u64(&bytes[..8]);

        // apply_state: RaftApplyState,
        let raft_apply_state_offset = 8;
        let raft_apply_state_len =
            BigEndian::read_u64(&bytes[raft_apply_state_offset..raft_apply_state_offset + 8])
                as usize;
        let apply_state = protobuf::parse_from_bytes::<RaftApplyState>(
            &bytes[raft_apply_state_offset + 8..raft_apply_state_offset + 8 + raft_apply_state_len],
        )
        .unwrap_or_else(|e| panic!("currupted apply state, error: {:?}", e));

        // region: metapb::Region,
        let region_offset = raft_apply_state_offset + 8 + raft_apply_state_len;
        let region_len = BigEndian::read_u64(&bytes[region_offset..region_offset + 8]) as usize;
        let region = protobuf::parse_from_bytes::<metapb::Region>(
            &bytes[region_offset + 8..region_offset + 8 + region_len],
        )
        .unwrap_or_else(|e| panic!("currupted region, error: {:?}", e));

        // peer: metapb::Peer,
        let peer_offset = region_offset + 8 + region_len;
        let peer_len = BigEndian::read_u64(&bytes[peer_offset..peer_offset + 8]) as usize;
        let peer = protobuf::parse_from_bytes::<metapb::Peer>(
            &bytes[peer_offset + 8..peer_offset + 8 + peer_len],
        )
        .unwrap_or_else(|e| panic!("currupted peer, error: {:?}", e));

        RegionMeta {
            peer,
            region,
            apply_state,
            applied_term,
        }
    }

    pub fn to_command_response(&self) -> CommandResponse {
        let mut header = CommandResponseHeader::new();
        header.set_region_id(self.region.get_id());
        let mut resp = CommandResponse::new();
        resp.set_header(header);
        resp.set_apply_state(self.apply_state.clone());
        resp.set_applied_term(self.applied_term);
        resp
    }
}

pub fn write_region_meta(meta: &RegionMeta, wb: &WriteBatch) {
    let region_id = meta.region.get_id();
    let apply_state_key = keys::apply_state_key(region_id);
    let mut buffer = Vec::new();
    meta.write_to(&mut buffer).unwrap();
    wb.put(&apply_state_key, &buffer).unwrap_or_else(|e| {
        panic!(
            "[region {}] failed to persist apply state {}: {:?}",
            region_id,
            keys::escape(&apply_state_key),
            e
        )
    });
}

pub fn remove_region_meta(meta: &RegionMeta, wb: &WriteBatch) {
    let region_id = meta.region.get_id();
    let apply_state_key = keys::apply_state_key(region_id);
    wb.delete(&apply_state_key).unwrap_or_else(|e| {
        panic!(
            "[region {}] failed to remove apply state {}: {:?}",
            region_id,
            keys::escape(&apply_state_key),
            e
        )
    });
}

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;

// When we bootstrap the region or handling split new region, we must
// call this to initialize region apply state first.
pub fn initial_apply_state() -> RaftApplyState {
    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);

    apply_state
}

#[cfg(test)]
mod tests {
    use kvproto::metapb;

    use super::*;

    #[test]
    fn test_region_meta_serde() {
        let applied_term = 20181108;

        let mut peer = metapb::Peer::new();
        peer.set_id(11);
        peer.set_store_id(2018);

        let mut region = metapb::Region::new();
        region.set_id(17);
        region.set_start_key(b"v: ::std::vec::Vec<u7>".to_vec());
        region.set_end_key(b"v: ::std::vec::Vec<u9>".to_vec());
        region.set_peers(vec![peer.clone()].into());

        let mut apply_state = RaftApplyState::new();
        apply_state.set_applied_index(applied_term * 2);
        apply_state.mut_truncated_state().set_index(1);

        let meta = RegionMeta::new(peer, region, apply_state);
        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();
        assert_eq!(RegionMeta::parse(&buf), meta);
    }
}

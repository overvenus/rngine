use std::io::{self, Write};
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use kvproto::enginepb::CommandResponseBatch;
use kvproto::raft_serverpb::RaftApplyState;
use protobuf::Message;
use rocksdb::DB;

use super::worker::{Scheduler, Worker};

mod apply;
mod snapshot;

pub use self::apply::{Runner as ApplyRunner, Task as ApplyTask};
pub use self::snapshot::{Runner as SnapshotRunner, Task as SnapshotTask};

pub struct Engine {
    db: Arc<DB>,
    apply_worker: Worker<ApplyTask>,
    snapshot_worker: Worker<SnapshotTask>,
    applied_receiver: Option<UnboundedReceiver<CommandResponseBatch>>,
}

impl Engine {
    pub fn new(db: Arc<DB>) -> Engine {
        Engine {
            db,
            apply_worker: Worker::new("apply"),
            snapshot_worker: Worker::new("snapshot"),
            applied_receiver: None,
        }
    }

    pub fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    pub fn snapshot_scheduler(&self) -> Scheduler<SnapshotTask> {
        self.snapshot_worker.scheduler()
    }

    pub fn start(&mut self) {
        let (tx, rx) = unbounded();
        self.applied_receiver = Some(rx);
        let applier = ApplyRunner::new(self.db.clone(), tx);
        self.apply_worker.start(applier).unwrap();

        let snap_runner = SnapshotRunner::new(self.db.clone(), self.apply_worker.scheduler());
        self.snapshot_worker.start(snap_runner).unwrap();
    }

    pub fn take_apply_receiver(&mut self) -> Option<UnboundedReceiver<CommandResponseBatch>> {
        self.applied_receiver.take()
    }

    pub fn stop(&mut self) {
        if let Some(handler) = self.apply_worker.stop() {
            handler.join().unwrap()
        }

        if let Some(handler) = self.snapshot_worker.stop() {
            handler.join().unwrap()
        }
    }
}

#[derive(Default, Debug)]
pub struct ApplyState {
    pub state: RaftApplyState,
    pub applied_term: u64,
}

impl ApplyState {
    pub fn from_raft_apply_state(state: RaftApplyState) -> ApplyState {
        ApplyState {
            applied_term: state.get_truncated_state().get_term(),
            state,
        }
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Applied term.
        let mut buf = [0u8; 8];
        BigEndian::write_u64(&mut buf[..], self.applied_term);
        writer.write_all(&buf)?;

        // RaftApplyState
        self.state.write_to_writer(writer).map(|_| ())?;
        return Ok(());
    }

    pub fn parse(bytes: &[u8]) -> ApplyState {
        assert!(bytes.len() >= 8 /* size of u64 */);

        let applied_term = BigEndian::read_u64(&bytes[..8]);
        let state = protobuf::parse_from_bytes::<RaftApplyState>(&bytes[8..])
            .unwrap_or_else(|e| panic!("currupted apply state, error: {:?}", e));

        ApplyState {
            state,
            applied_term,
        }
    }
}

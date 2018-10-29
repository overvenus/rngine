use std::sync::Arc;

use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use kvproto::enginepb::CommandResponseBatch;
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

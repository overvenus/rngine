use std::sync::Arc;

use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use kvproto::enginepb::CommandResponseBatch;
use rocksdb::DB;

use super::worker::{Scheduler, Worker};

mod apply;

pub use self::apply::{Runner as ApplyRunner, Task as ApplyTask};

pub struct Engine {
    db: Arc<DB>,
    apply: Worker<ApplyTask>,
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

    pub fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply.scheduler()
    }

    pub fn start(&mut self) {
        let (tx, rx) = unbounded();
        self.applied_receiver = Some(rx);
        let applier = ApplyRunner::new(self.db.clone(), tx);
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

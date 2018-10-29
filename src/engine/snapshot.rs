use std::fmt;
use std::sync::mpsc::{channel, Receiver, RecvError, Sender};
use std::sync::Arc;

use futures::sync::oneshot::Sender as OneshotSender;
use kvproto::enginepb::{SnapshotRequest, SnapshotState};
use protobuf::Message;
use rocksdb::{Writable, WriteBatch, WriteOptions, DB};

use super::super::keys::{self, escape};
use super::super::rocksdb_util;
use super::super::worker::{Runnable, Scheduler};
use super::ApplyTask;

pub struct Task {
    requests: Receiver<SnapshotRequest>,
    done: OneshotSender<()>,
}

impl Task {
    pub fn new(done: OneshotSender<()>) -> (Sender<SnapshotRequest>, Task) {
        let (tx, requests) = channel();
        (tx, Task { requests, done })
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Snapshot Task")
    }
}

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;

pub struct Runner {
    db: Arc<DB>,
    apply_scheduler: Scheduler<ApplyTask>,
}

impl Runner {
    pub fn new(db: Arc<DB>, apply_scheduler: Scheduler<ApplyTask>) -> Runner {
        Runner {
            db,
            apply_scheduler,
        }
    }

    fn apply_snapshot(&mut self, task: Task) {
        let requests = task.requests;
        let done = task.done;

        let wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        let mut state: Option<SnapshotState> = None;
        loop {
            let mut chunk = match requests.recv() {
                Ok(chunk) => chunk,
                Err(RecvError) => {
                    info!(
                        "[region {}] receive all snapshot",
                        state.as_ref().unwrap().get_region().get_id(),
                    );
                    break;
                }
            };

            if state.is_none() {
                state = Some(chunk.take_state());
                continue;
            }

            let data = chunk.take_data();
            let cf = data.get_cf();
            let cf_handle = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
            for pair in data.get_data() {
                let (key, value) = (pair.get_key(), pair.get_value());
                wb.put_cf(cf_handle, &keys::data_key(key), value)
                    .unwrap_or_else(|e| {
                        panic!(
                            "[region {}] failed to write ({}, {}) to cf {}: {:?}",
                            state.as_ref().unwrap().get_region().get_id(),
                            escape(&key),
                            escape(value),
                            cf,
                            e
                        )
                    });
            }
        }

        // Persist snapshot data.
        let region_id = state.as_ref().unwrap().get_region().get_id();
        let snapshot_state = state.unwrap();
        let mut buffer = Vec::new();
        snapshot_state
            .get_apply_state()
            .write_to_writer(&mut buffer)
            .unwrap();
        let region_key = keys::apply_state_key(region_id);
        wb.put(&region_key, &buffer).unwrap_or_else(|e| {
            panic!(
                "[region {}] failed to delete {}: {:?}",
                region_id,
                escape(&region_key),
                e
            )
        });
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true /* TODO: consider header.sync_log */);
        self.db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        // Send snapshot state to apply worker.
        // TODO: notify apply state to tikv.
        self.apply_scheduler
            .schedule(ApplyTask::snapshot(snapshot_state))
            .unwrap();

        // Notify apply snapshot finished.
        done.send(()).unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run_batch(&mut self, batch: &mut Vec<Task>) {
        for task in batch.drain(..) {
            self.apply_snapshot(task);
        }
    }
}

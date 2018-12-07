use std::fmt;
use std::sync::mpsc::{channel, Receiver, RecvError, Sender};
use std::sync::Arc;

use futures::sync::oneshot::Sender as OneshotSender;
use kvproto::enginepb::{SnapshotRequest, SnapshotState};
use rocksdb::{Writable, WriteBatch, WriteOptions, DB};

use super::super::keys::{self, escape};
use super::super::rocksdb_util;
use super::super::worker::{Runnable, Scheduler};
use super::{remove_region_meta, write_region_meta, ApplyTask, RegionMeta};

pub enum Task {
    Snapshot {
        requests: Receiver<SnapshotRequest>,
        done: OneshotSender<()>,
    },
    Destroy {
        meta: RegionMeta,
        by_confchange: bool,
    },
}

impl Task {
    pub fn snapshot(done: OneshotSender<()>) -> (Sender<SnapshotRequest>, Task) {
        let (tx, requests) = channel();
        (tx, Task::Snapshot { requests, done })
    }

    pub fn destroy(meta: RegionMeta, by_confchange: bool) -> Task {
        Task::Destroy {
            meta,
            by_confchange,
        }
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

    fn apply_snapshot(&mut self, requests: Receiver<SnapshotRequest>, done: OneshotSender<()>) {
        let wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        let mut state: Option<SnapshotState> = None;
        loop {
            let mut chunk = match requests.recv() {
                Ok(chunk) => chunk,
                Err(RecvError) => {
                    info!(
                        "[region {}] receive all snapshot chunks",
                        state.as_ref().unwrap().get_region().get_id(),
                    );
                    break;
                }
            };

            if state.is_none() {
                let s = chunk.take_state();
                info!(
                    "[region {}] applying snapshot, {:?}",
                    s.get_region().get_id(),
                    s,
                );

                // We must clean stale data first.
                assert!(s.has_region());
                let start_key = keys::enc_start_key(s.get_region());
                let end_key = keys::enc_end_key(s.get_region());
                assert!(start_key <= end_key);
                rocksdb_util::delete_all_in_range(&self.db, &start_key, &end_key, &wb);

                state = Some(s);
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
        let mut snapshot_state = state.unwrap();

        let region_meta = RegionMeta::new(
            snapshot_state.take_peer(),
            snapshot_state.take_region(),
            snapshot_state.take_apply_state(),
        );

        write_region_meta(&region_meta, &wb);

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        self.db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        // Send snapshot state to apply worker.
        self.apply_scheduler
            .schedule(ApplyTask::snap(region_meta))
            .unwrap();

        // Notify apply snapshot finished.
        done.send(()).unwrap();
        info!("[region {}] snapshot applied", region_id);
    }

    fn apply_destroy(&mut self, meta: RegionMeta, by_confchange: bool) {
        let wb = WriteBatch::with_capacity(DEFAULT_APPLY_WB_SIZE);
        let start_key = keys::enc_start_key(&meta.region);
        let end_key = keys::enc_end_key(&meta.region);
        assert!(start_key <= end_key);

        // Clean up data.
        rocksdb_util::delete_all_in_range(&self.db, &start_key, &end_key, &wb);

        // Clean up meta data.
        remove_region_meta(&meta, &wb);

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        self.db.write_opt(wb, &write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });

        let apply_state = if by_confchange {
            info!("[region {}] is delete by conf change", meta.region.get_id());
            Some((meta.applied_term, meta.apply_state))
        } else {
            info!("[region {}] is delete by tombstone", meta.region.get_id());
            None
        };
        self.apply_scheduler
            .schedule(ApplyTask::destroyed(meta.region.get_id(), apply_state))
            .unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run_batch(&mut self, batch: &mut Vec<Task>) {
        for task in batch.drain(..) {
            match task {
                Task::Snapshot { requests, done } => self.apply_snapshot(requests, done),
                Task::Destroy {
                    meta,
                    by_confchange,
                } => self.apply_destroy(meta, by_confchange),
            }
        }
    }
}

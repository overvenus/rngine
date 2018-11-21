// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::{Entry, HashMap};
use std::fmt::{self, Display, Formatter};

use byteorder::{BigEndian, WriteBytesExt};
use crc::crc32::{self, Digest, Hasher32};

use kvproto::metapb::Region;

use crate::keys::{self, escape};
use crate::rocksdb_util::Snapshot;
use crate::worker::Runnable;

/// Consistency checking task.
pub enum Task {
    ComputeHash {
        index: u64,
        region: Region,
        raft_local_state: Vec<u8>,
        snap: Snapshot,
    },
    VerifyHash {
        index: u64,
        region: Region,
        hash: Vec<u8>,
    },
}

impl Task {
    pub fn compute_hash(
        region: Region,
        index: u64,
        snap: Snapshot,
        raft_local_state: Vec<u8>,
    ) -> Task {
        Task::ComputeHash {
            region,
            index,
            snap,
            raft_local_state,
        }
    }

    pub fn verify_hash(region: Region, index: u64, hash: Vec<u8>) -> Task {
        Task::VerifyHash {
            region,
            index,
            hash,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::ComputeHash {
                ref region, index, ..
            } => write!(
                f,
                "Compute Hash Task for [region {}] at {}",
                region.get_id(),
                index
            ),
            Task::VerifyHash {
                ref region,
                index,
                ref hash,
            } => write!(
                f,
                "Verify Hash Task for [region {}] at {}, hash {:?}",
                region.get_id(),
                index,
                hash
            ),
        }
    }
}

#[derive(Debug)]
struct ConsistencyState {
    // (computed_result_or_to_be_verified, index, hash)
    index: u64,
    hash: Vec<u8>,
}

pub struct Runner {
    states: HashMap<u64, ConsistencyState>,
}

impl Runner {
    pub fn new() -> Runner {
        Runner {
            states: HashMap::new(),
        }
    }

    fn compute_hash(
        &mut self,
        region: Region,
        index: u64,
        snap: Snapshot,
        raft_local_state: Vec<u8>,
    ) {
        let region_id = region.get_id();
        info!("[region {}] computing hash at {}", region_id, index);

        let mut digest = Digest::new(crc32::IEEE);
        let mut cf_names = snap.cf_names();
        cf_names.sort();
        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        for cf in cf_names {
            let res = snap.scan_cf(cf, &start_key, &end_key, false, |k, v| {
                digest.write(k);
                digest.write(v);
                Ok(true)
            });
            if let Err(e) = res {
                error!("[region {}] failed to calculate hash: {:?}", region_id, e);
                return;
            }
        }
        let region_state_key = keys::region_state_key(region_id);
        digest.write(&region_state_key);
        digest.write(&raft_local_state);
        let sum = digest.sum32();

        let mut hash = Vec::with_capacity(4);
        hash.write_u32::<BigEndian>(sum).unwrap();

        info!(
            "[region {}] hash is {:?} at index {}",
            region_id, hash, index
        );
        self.store_and_verify_hash(region_id, index, hash);
    }

    fn store_and_verify_hash(
        &mut self,
        region_id: u64,
        expected_index: u64,
        expected_hash: Vec<u8>,
    ) {
        match self.states.entry(region_id) {
            Entry::Occupied(mut e) => {
                let state_index = e.get().index;
                if expected_index < state_index {
                    warn!(
                        "[region {}] has scheduled a new hash: {} > {}, skip.",
                        region_id, state_index, expected_index
                    );
                    return;
                }

                if expected_index == state_index {
                    // Remvoe the entry if indexes are matched.
                    let state = e.remove();
                    if state.hash.is_empty() {
                        warn!(
                            "[region {}] duplicated consistency check detected, skip.",
                            region_id
                        );
                        return;
                    }
                    if state.hash != expected_hash {
                        panic!(
                            "[region {}] hash at {} not correct, want \"{}\", got \"{}\"!!!",
                            region_id,
                            state_index,
                            escape(&expected_hash),
                            escape(&state.hash)
                        );
                    }
                    info!(
                        "[region {}] consistency check at {} pass",
                        region_id, state_index
                    );
                    return;
                }

                let new_state = ConsistencyState {
                    index: expected_index,
                    hash: expected_hash,
                };

                // Maybe computing is too slow or computed result is dropped due to channel full.
                // If computing is too slow, miss count will be increased twice.
                warn!(
                    "[region {}] replace old consistency state {:?} with a new one {:?}",
                    region_id,
                    e.get(),
                    new_state
                );

                e.insert(new_state);
            }
            Entry::Vacant(e) => {
                info!(
                    "[region {}] save hash {:?} at {} for consistency check later.",
                    region_id, expected_hash, expected_index,
                );
                let state = ConsistencyState {
                    index: expected_index,
                    hash: expected_hash,
                };
                e.insert(state);
            }
        }
    }
}

impl Runnable<Task> for Runner {
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        for task in tasks.drain(..) {
            match task {
                Task::ComputeHash {
                    region,
                    index,
                    snap,
                    raft_local_state,
                } => self.compute_hash(region, index, snap, raft_local_state),
                Task::VerifyHash {
                    index,
                    region,
                    hash,
                } => {
                    info!(
                        "[region {}] verify hash {:?} at index {}",
                        region.get_id(),
                        hash,
                        index
                    );
                    self.store_and_verify_hash(region.get_id(), index, hash);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, WriteBytesExt};
    use crc::crc32::{self, Digest, Hasher32};
    use kvproto::metapb::*;
    use rocksdb::Writable;

    use super::*;
    use crate::rocksdb_util::create_tmp_db;

    #[test]
    fn test_consistency_check() {
        let (_tmp, db) = create_tmp_db("consistency_check");

        let mut region = Region::new();
        region.mut_peers().push(Peer::new());

        let mut runner = Runner::new();
        let mut digest = Digest::new(crc32::IEEE);
        let kvs = vec![(b"k1", b"v1"), (b"k2", b"v2")];
        for (k, v) in kvs {
            let key = keys::data_key(k);
            db.put(&key, v).unwrap();
            // hash should contain all kvs
            digest.write(&key);
            digest.write(v);
        }
        // hash should also contains region state key.
        digest.write(&keys::region_state_key(region.get_id()));
        digest.write(b"");

        let sum = digest.sum32();
        let index = 10;
        let snap = Snapshot::new(db.clone());
        let task = Task::compute_hash(region.clone(), index, snap, Vec::new());
        runner.run_batch(&mut vec![task]);
        assert_eq!(runner.states.len(), 1);

        let mut checksum_bytes = vec![];
        checksum_bytes.write_u32::<BigEndian>(sum).unwrap();
        let task = Task::verify_hash(region.clone(), 10, checksum_bytes);
        runner.run_batch(&mut vec![task]);
        assert_eq!(runner.states.len(), 0);
    }
}

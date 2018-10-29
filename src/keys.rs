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

use byteorder::{BigEndian, ByteOrder};

use kvproto::metapb::Region;
use std::{mem, result::Result as StdResult};

pub const MIN_KEY: &[u8] = &[];
pub const MAX_KEY: &[u8] = &[0xFF];

pub const EMPTY_KEY: &[u8] = &[];

// local is in (0x01, 0x02);
const LOCAL_PREFIX: u8 = 0x01;
pub const LOCAL_MIN_KEY: &[u8] = &[LOCAL_PREFIX];
pub const LOCAL_MAX_KEY: &[u8] = &[LOCAL_PREFIX + 1];

const DATA_PREFIX: u8 = b'z';
const DATA_PREFIX_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MIN_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MAX_KEY: &[u8] = &[DATA_PREFIX + 1];

// We save two types region data in DB, for raft and other meta data.
// When the store starts, we should iterate all region meta data to
// construct peer, no need to travel large raft data, so we separate them
// with different prefixes.
const REGION_RAFT_PREFIX: u8 = 0x02;
const REGION_RAFT_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_RAFT_PREFIX];
const REGION_META_PREFIX: u8 = 0x03;
// const REGION_META_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX];
pub const REGION_META_MIN_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX];
pub const REGION_META_MAX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX + 1];

// Following are the suffix after the local prefix.
// For region id
// const RAFT_LOG_SUFFIX: u8 = 0x01;
// const RAFT_STATE_SUFFIX: u8 = 0x02;
pub const APPLY_STATE_SUFFIX: u8 = 0x03;
pub const SNAPSHOT_RAFT_STATE_SUFFIX: u8 = 0x04;

// For region meta
pub const REGION_STATE_SUFFIX: u8 = 0x01;

pub type Result<T> = StdResult<T, String>;

#[inline]
fn make_region_prefix(region_id: u64, suffix: u8) -> [u8; 11] {
    let mut key = [0; 11];
    key[..2].copy_from_slice(REGION_RAFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], region_id);
    key[10] = suffix;
    key
}

pub fn snapshot_raft_state_key(region_id: u64) -> [u8; 11] {
    make_region_prefix(region_id, SNAPSHOT_RAFT_STATE_SUFFIX)
}

pub fn apply_state_key(region_id: u64) -> [u8; 11] {
    make_region_prefix(region_id, APPLY_STATE_SUFFIX)
}

// Decode region apply key, return the region key and meta suffix type.
pub fn decode_apply_state_key(key: &[u8]) -> Result<(u64, u8)> {
    if REGION_RAFT_PREFIX_KEY.len() + mem::size_of::<u64>() + mem::size_of::<u8>() != key.len() {
        return Err(format!(
            "invalid region apply key length for key {}",
            escape(key)
        ));
    }

    if !key.starts_with(REGION_RAFT_PREFIX_KEY) {
        return Err(format!(
            "invalid region apply prefix for key {}",
            escape(key)
        ));
    }

    let region_id = BigEndian::read_u64(
        &key[REGION_RAFT_PREFIX_KEY.len()..REGION_RAFT_PREFIX_KEY.len() + mem::size_of::<u64>()],
    );

    Ok((region_id, key[key.len() - 1]))
}

pub fn validate_data_key(key: &[u8]) -> bool {
    key.starts_with(DATA_PREFIX_KEY)
}

pub fn data_key(key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(DATA_PREFIX_KEY.len() + key.len());
    v.extend_from_slice(DATA_PREFIX_KEY);
    v.extend_from_slice(key);
    v
}

pub fn origin_key(key: &[u8]) -> &[u8] {
    assert!(validate_data_key(key), "invalid data key {:?}", escape(key));
    &key[DATA_PREFIX_KEY.len()..]
}

/// Get the `start_key` of current region in encoded form.
pub fn enc_start_key(region: &Region) -> Vec<u8> {
    // only initialized region's start_key can be encoded, otherwise there must be bugs
    // somewhere.
    assert!(!region.get_peers().is_empty());
    data_key(region.get_start_key())
}

/// Get the `end_key` of current region in encoded form.
pub fn enc_end_key(region: &Region) -> Vec<u8> {
    // only initialized region's end_key can be encoded, otherwise there must be bugs
    // somewhere.
    assert!(!region.get_peers().is_empty());
    data_end_key(region.get_end_key())
}

#[inline]
pub fn data_end_key(region_end_key: &[u8]) -> Vec<u8> {
    if region_end_key.is_empty() {
        DATA_MAX_KEY.to_vec()
    } else {
        data_key(region_end_key)
    }
}

pub fn escape(data: &[u8]) -> String {
    let mut escaped = Vec::with_capacity(data.len() * 4);
    for &c in data {
        match c {
            b'\n' => escaped.extend_from_slice(br"\n"),
            b'\r' => escaped.extend_from_slice(br"\r"),
            b'\t' => escaped.extend_from_slice(br"\t"),
            b'"' => escaped.extend_from_slice(b"\\\""),
            b'\\' => escaped.extend_from_slice(br"\\"),
            _ => {
                if c >= 0x20 && c < 0x7f {
                    // c is printable
                    escaped.push(c);
                } else {
                    escaped.push(b'\\');
                    escaped.push(b'0' + (c >> 6));
                    escaped.push(b'0' + ((c >> 3) & 7));
                    escaped.push(b'0' + (c & 7));
                }
            }
        }
    }
    escaped.shrink_to_fit();
    unsafe { String::from_utf8_unchecked(escaped) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::metapb::{Peer, Region};

    #[test]
    fn test_data_key() {
        assert!(validate_data_key(&data_key(b"abc")));
        assert!(!validate_data_key(b"abc"));

        let mut region = Region::new();
        // uninitialised region should not be passed in `enc_start_key` and `enc_end_key`.
        // assert!(::panic_hook::recover_safe(|| enc_start_key(&region)).is_err());
        // assert!(::panic_hook::recover_safe(|| enc_end_key(&region)).is_err());

        region.mut_peers().push(Peer::new());
        assert_eq!(enc_start_key(&region), vec![DATA_PREFIX]);
        assert_eq!(enc_end_key(&region), vec![DATA_PREFIX + 1]);

        region.set_start_key(vec![1]);
        region.set_end_key(vec![2]);
        assert_eq!(enc_start_key(&region), vec![DATA_PREFIX, 1]);
        assert_eq!(enc_end_key(&region), vec![DATA_PREFIX, 2]);
    }
}

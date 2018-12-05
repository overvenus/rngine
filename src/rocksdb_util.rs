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

use std::fs;
use std::path::Path;
use std::sync::Arc;

use rocksdb::load_latest_options;
use rocksdb::rocksdb::supported_compression;
use rocksdb::rocksdb_options::UnsafeSnap;
use rocksdb::CFHandle;
use rocksdb::{
    CColumnFamilyDescriptor, ColumnFamilyOptions, CompactOptions, DBCompressionType, DBIterator,
    DBOptions, Env, ReadOptions, Writable, WriteBatch, DB,
};

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_WRITE];
pub const ALL_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

pub fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .into_iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle, String> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found.", cf))
}

pub fn open_opt(
    opts: DBOptions,
    path: &str,
    cfs: Vec<&str>,
    cfs_opts: Vec<ColumnFamilyOptions>,
) -> Result<DB, String> {
    DB::open_cf(opts, path, cfs.into_iter().zip(cfs_opts).collect())
}

pub struct CFOptions<'a> {
    cf: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> CFOptions<'a> {
        CFOptions { cf, options }
    }
}

pub fn new_engine(path: &str, cfs: &[&str], opts: Option<Vec<CFOptions>>) -> Result<DB, String> {
    let mut db_opts = DBOptions::new();
    db_opts.enable_statistics(true);
    let cf_opts = match opts {
        Some(opts_vec) => opts_vec,
        None => {
            let mut default_cfs_opts = Vec::with_capacity(cfs.len());
            for cf in cfs {
                default_cfs_opts.push(CFOptions::new(*cf, ColumnFamilyOptions::new()));
            }
            default_cfs_opts
        }
    };
    new_engine_opt(path, db_opts, cf_opts)
}

// Turn "dynamic level size" off for existing column family which was off before.
// column families are small, HashMap isn't necessary
fn adjust_dynamic_level_bytes(cf_descs: &[CColumnFamilyDescriptor], cf_options: &mut CFOptions) {
    if let Some(ref cf_desc) = cf_descs
        .iter()
        .find(|cf_desc| cf_desc.name() == cf_options.cf)
    {
        let existed_dynamic_level_bytes =
            cf_desc.options().get_level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes
            != cf_options
                .options
                .get_level_compaction_dynamic_level_bytes()
        {
            println!(
                "change dynamic_level_bytes for existing column family is danger, old: {}, new: {}",
                existed_dynamic_level_bytes,
                cf_options
                    .options
                    .get_level_compaction_dynamic_level_bytes()
            );
        }
        cf_options
            .options
            .set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

fn check_and_open(
    path: &str,
    mut db_opt: DBOptions,
    cfs_opts: Vec<CFOptions>,
) -> Result<DB, String> {
    // If db not exist, create it.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut cfs_v = vec![];
        let mut cf_opts_v = vec![];
        if let Some(x) = cfs_opts.iter().find(|x| x.cf == CF_DEFAULT) {
            cfs_v.push(x.cf);
            cf_opts_v.push(x.options.clone());
        }
        let mut db = DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cf_opts_v).collect())?;
        for x in cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            db.create_cf((x.cf, x.options))?;
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // List all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cfs_opts.iter().map(|x| x.cf).collect();

    let cf_descs = if !existed.is_empty() {
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(path, &Env::default(), true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS file"));
        tmp
    } else {
        vec![]
    };

    // If all column families are exist, just open db.
    if existed == needed {
        let mut cfs_v = vec![];
        let mut cfs_opts_v = vec![];
        for mut x in cfs_opts {
            adjust_dynamic_level_bytes(&cf_descs, &mut x);
            cfs_v.push(x.cf);
            cfs_opts_v.push(x.options);
        }

        return DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cfs_opts_v).collect());
    }

    // Open db.
    let mut cfs_v: Vec<&str> = Vec::new();
    let mut cfs_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for cf in &existed {
        cfs_v.push(cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                let mut tmp = CFOptions::new(x.cf, x.options.clone());
                adjust_dynamic_level_bytes(&cf_descs, &mut tmp);
                cfs_opts_v.push(tmp.options);
            }
            None => {
                cfs_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
    let mut db = DB::open_cf(db_opt, path, cfds).unwrap();

    // Drop discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            db.drop_cf(cf)?;
        }
    }

    // Create needed column families not existed yet.
    for cf in cfs_diff(&needed, &existed) {
        db.create_cf((
            cf,
            cfs_opts
                .iter()
                .find(|x| x.cf == cf)
                .unwrap()
                .options
                .clone(),
        ))?;
    }
    Ok(db)
}

// `cfs_diff' Returns a Vec of cf which is in `a' but not in `b'.
pub fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| b.iter().find(|y| y == x).is_none())
        .map(|x| *x)
        .collect()
}

pub fn new_engine_opt(path: &str, opts: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<DB, String> {
    check_and_open(path, opts, cfs_opts)
}

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, DB::list_column_families will failed and we can cleanup
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

pub fn auto_compactions_is_disabled(engine: &DB) -> bool {
    for cf_name in engine.cf_names() {
        let cf = engine.cf_handle(cf_name).unwrap();
        if engine.get_options_cf(cf).get_disable_auto_compactions() {
            return true;
        }
    }
    false
}

pub fn scan_db_iterator<F>(
    mut it: DBIterator<&DB>,
    start_key: &[u8],
    mut f: F,
) -> Result<(), String>
where
    F: FnMut(&[u8], &[u8]) -> Result<bool, String>,
{
    it.seek(start_key.into());
    if !it.valid() {
        error!("iter an invalid start key");
    }
    while it.valid() {
        let r = f(it.key(), it.value())?;

        if !r || !it.next() {
            break;
        }
    }

    Ok(())
}

pub struct IterOption {
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    fill_cache: bool,
}

impl IterOption {
    pub fn new(
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
        fill_cache: bool,
    ) -> IterOption {
        IterOption {
            lower_bound,
            upper_bound,
            fill_cache,
        }
    }

    pub fn build_read_opts(&self) -> ReadOptions {
        let mut opts = ReadOptions::new();
        opts.fill_cache(self.fill_cache);
        opts.set_total_order_seek(true);
        if let Some(ref key) = self.lower_bound {
            opts.set_iterate_lower_bound(key);
        }
        if let Some(ref key) = self.upper_bound {
            opts.set_iterate_upper_bound(key);
        }
        opts
    }
}

impl Default for IterOption {
    fn default() -> IterOption {
        IterOption {
            lower_bound: None,
            upper_bound: None,
            fill_cache: true,
        }
    }
}

pub struct Snapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}

impl Snapshot {
    pub fn new(db: Arc<DB>) -> Snapshot {
        unsafe {
            Snapshot {
                snap: db.unsafe_snap(),
                db,
            }
        }
    }

    pub fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }

    pub fn cf_handle(&self, cf: &str) -> Result<&CFHandle, String> {
        get_cf_handle(&self.db, cf)
    }

    pub fn get_db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }

    pub fn db_iterator(&self, iter_opt: IterOption) -> DBIterator<Arc<DB>> {
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(Arc::clone(&self.db), opt)
    }

    pub fn db_iterator_cf(
        &self,
        cf: &str,
        iter_opt: IterOption,
    ) -> Result<DBIterator<Arc<DB>>, String> {
        let handle = get_cf_handle(&self.db, cf)?;
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(Arc::clone(&self.db), handle, opt))
    }

    fn new_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>, String> {
        let handle = get_cf_handle(&self.db, cf)?;
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(&self.db, handle, opt))
    }

    pub fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<(), String>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, String>,
    {
        let iter_opt =
            IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), fill_cache);
        scan_db_iterator(self.new_iterator_cf(cf, iter_opt)?, start_key, f)
    }
}

/// Compact the cf in the specified range by manual or not.
pub fn compact_range(
    db: &DB,
    handle: &CFHandle,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    exclusive_manual: bool,
    max_subcompactions: u32,
) {
    let mut compact_opts = CompactOptions::new();
    // `exclusive_manual == false` means manual compaction can
    // concurrently run with other background compactions.
    compact_opts.set_exclusive_manual_compaction(exclusive_manual);
    compact_opts.set_max_subcompactions(max_subcompactions as i32);
    db.compact_range_cf_opt(handle, &compact_opts, start_key, end_key);
}

fn new_iterator_cf_for_db<'a>(
    db: &'a DB,
    cf: &str,
    iter_opt: IterOption,
) -> Result<DBIterator<&'a DB>, String> {
    let handle = get_cf_handle(db, cf)?;
    let readopts = iter_opt.build_read_opts();
    Ok(DBIterator::new_cf(db, handle, readopts))
}

pub fn delete_all_in_range(db: &DB, start_key: &[u8], end_key: &[u8], wb: &WriteBatch) {
    // Do not use delete range, it aborts at rocksdb::RangeDelAggregator::AddTombstones.
    // wb.delete_range_cf(handle, start_key, end_key).unwrap();
    for cf in ALL_CFS {
        let handle = get_cf_handle(db, cf).unwrap();
        let iter_opt = IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), false);
        let mut it = new_iterator_cf_for_db(db, cf, iter_opt).unwrap();
        it.seek(start_key.into());
        while it.valid() {
            wb.delete_cf(handle, it.key()).unwrap();
            if !it.next() {
                break;
            }
        }
    }
}

#[cfg(test)]
use tempdir::TempDir;

#[cfg(test)]
pub fn create_tmp_db(path: &str) -> (TempDir, Arc<DB>) {
    let path = TempDir::new(path).unwrap();
    let db = Arc::new(new_engine(path.path().join("db").to_str().unwrap(), ALL_CFS, None).unwrap());
    (path, db)
}

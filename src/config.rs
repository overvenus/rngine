use std::error::Error;
use std::fmt::{self, Write};
use std::fs;
use std::io::Read;
use std::ops::{Div, Mul};
use std::path::Path;
use std::str::{self, FromStr};
use std::time::Duration;

use rocksdb::{
    BlockBasedOptions, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle, DBOptions,
    DBRecoveryMode,
};
use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::rocksdb_util::{CFOptions, CF_DEFAULT, CF_LOCK, CF_WRITE};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RgConfig {
    pub path: String,
    pub address: String,
    pub rocksdb: DbConfig,
}

impl RgConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self
    where
        P: fmt::Debug,
    {
        fs::File::open(&path)
            .map_err::<Box<Error>, _>(|e| Box::new(e))
            .and_then(|mut f| {
                let mut s = String::new();
                f.read_to_string(&mut s)?;
                let c = toml::from_str(&s)?;
                Ok(c)
            }).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {:?}, err {}",
                    path, e
                );
            })
    }
}

impl Default for RgConfig {
    fn default() -> RgConfig {
        RgConfig {
            path: "./data".to_owned(),
            address: "127.0.0.1:3930".to_owned(),
            rocksdb: DbConfig::default(),
        }
    }
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            pub disable_block_cache: bool,
            pub cache_index_and_filter_blocks: bool,
            pub pin_l0_filter_and_index_blocks: bool,
            pub use_bloom_filter: bool,
            pub whole_key_filtering: bool,
            pub bloom_filter_bits_per_key: i32,
            pub block_based_bloom_filter: bool,
            pub read_amp_bytes_per_bit: u32,
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "compaction_pri_serde")]
            pub compaction_pri: CompactionPriority,
            pub dynamic_level_bytes: bool,
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "compaction_style_serde")]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: ReadableSize,
        }
    };
}

macro_rules! build_cf_opt {
    ($opt:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        block_base_opts.set_lru_cache($opt.block_cache_size.0 as usize, -1, 0, 0.0);
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter(
                $opt.bloom_filter_bits_per_key,
                $opt.block_based_bloom_filter,
            );
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.set_num_levels($opt.num_levels);
        cf_opts.set_write_buffer_size($opt.write_buffer_size.0);
        cf_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base($opt.target_file_size_base.0);
        cf_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger($opt.level0_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        cf_opts.compaction_priority($opt.compaction_pri);
        cf_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        cf_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);
        cf_opts.set_compaction_style($opt.compaction_style);
        cf_opts.set_disable_auto_compactions($opt.disable_auto_compactions);
        cf_opts.set_soft_pending_compaction_bytes_limit($opt.soft_pending_compaction_bytes_limit.0);
        cf_opts.set_hard_pending_compaction_bytes_limit($opt.hard_pending_compaction_bytes_limit.0);

        cf_opts
    }};
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(100),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        build_cf_opt!(self)
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(100),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        // Collects user defined properties.
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(20),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[serde(with = "recovery_mode_serde")]
    pub wal_recovery_mode: DBRecoveryMode,
    pub wal_dir: String,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_manifest_file_size: ReadableSize,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub enable_statistics: bool,
    pub compaction_readahead_size: ReadableSize,
    pub info_log_max_size: ReadableSize,
    pub info_log_roll_time: ReadableDuration,
    pub info_log_keep_log_file_num: u64,
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub defaultcf: DefaultCfConfig,
    pub writecf: WriteCfConfig,
    pub lockcf: LockCfConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: 6,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            rate_bytes_per_sec: ReadableSize::kb(0),
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            defaultcf: DefaultCfConfig::default(),
            writecf: WriteCfConfig::default(),
            lockcf: LockCfConfig::default(),
        }
    }
}

impl DbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if !self.info_log_dir.is_empty() {
            opts.create_info_log(&self.info_log_dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "create RocksDB info log {} error: {:?}",
                        self.info_log_dir, e
                    );
                })
        }
        if self.rate_bytes_per_sec.0 > 0 {
            opts.set_ratelimiter(self.rate_bytes_per_sec.0 as i64);
        }
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts
    }

    pub fn build_cf_opts(&self) -> Vec<CFOptions> {
        vec![
            CFOptions::new(CF_DEFAULT, self.defaultcf.build_opt()),
            CFOptions::new(CF_LOCK, self.lockcf.build_opt()),
            CFOptions::new(CF_WRITE, self.writecf.build_opt()),
        ]
    }
}

const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
pub const KB: u64 = UNIT * DATA_MAGNITUDE;
pub const MB: u64 = KB * DATA_MAGNITUDE;
pub const GB: u64 = MB * DATA_MAGNITUDE;

// Make sure it will not overflow.
const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: u64 = 1000;
const TIME_MAGNITUDE_2: u64 = 60;
const MS: u64 = UNIT;
const SECOND: u64 = MS * TIME_MAGNITUDE_1;
const MINUTE: u64 = SECOND * TIME_MAGNITUDE_2;
const HOUR: u64 = MINUTE * TIME_MAGNITUDE_2;

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KB)
    }

    pub fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MB)
    }

    pub fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GB)
    }

    pub fn as_mb(self) -> u64 {
        self.0 / MB
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{:?} is not a valid size.", s));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {:?}", s));
        }

        let mut chrs = size_str.chars();
        let mut number_str = size_str;
        let mut unit_char = chrs.next_back().unwrap();
        if unit_char < '0' || unit_char > '9' {
            number_str = chrs.as_str();
            if unit_char == 'B' {
                let b = match chrs.next_back() {
                    Some(b) => b,
                    None => return Err(format!("numeric value is expected: {:?}", s)),
                };
                if b < '0' || b > '9' {
                    number_str = chrs.as_str();
                    unit_char = b;
                }
            }
        } else {
            unit_char = 'B';
        }

        let unit = match unit_char {
            'K' => KB,
            'M' => MB,
            'G' => GB,
            'T' => TB,
            'P' => PB,
            'B' => UNIT,
            _ => return Err(format!("only B, KB, MB, GB, TB, PB are supported: {:?}", s)),
        };
        match number_str.trim().parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {:?}", s)),
        }
    }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{}KB", size).unwrap();
        } else if size % PB == 0 {
            write!(buffer, "{}PB", size / PB).unwrap();
        } else if size % TB == 0 {
            write!(buffer, "{}TB", size / TB).unwrap();
        } else if size % GB as u64 == 0 {
            write!(buffer, "{}GB", size / GB).unwrap();
        } else if size % MB as u64 == 0 {
            write!(buffer, "{}MB", size / MB).unwrap();
        } else if size % KB as u64 == 0 {
            write!(buffer, "{}KB", size / KB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadableDuration(pub Duration);

impl Into<Duration> for ReadableDuration {
    fn into(self) -> Duration {
        self.0
    }
}

impl ReadableDuration {
    pub fn secs(secs: u64) -> ReadableDuration {
        ReadableDuration(Duration::new(secs, 0))
    }

    pub fn millis(millis: u64) -> ReadableDuration {
        ReadableDuration(Duration::new(
            millis / 1000,
            (millis % 1000) as u32 * 1_000_000,
        ))
    }

    pub fn minutes(minutes: u64) -> ReadableDuration {
        ReadableDuration::secs(minutes * 60)
    }

    pub fn hours(hours: u64) -> ReadableDuration {
        ReadableDuration::minutes(hours * 60)
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_millis(&self) -> u64 {
        duration_to_ms(self.0)
    }
}

impl Serialize for ReadableDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut dur = duration_to_ms(self.0);
        let mut buffer = String::new();
        if dur >= HOUR {
            write!(buffer, "{}h", dur / HOUR).unwrap();
            dur %= HOUR;
        }
        if dur >= MINUTE {
            write!(buffer, "{}m", dur / MINUTE).unwrap();
            dur %= MINUTE;
        }
        if dur >= SECOND {
            write!(buffer, "{}s", dur / SECOND).unwrap();
            dur %= SECOND;
        }
        if dur > 0 {
            write!(buffer, "{}ms", dur).unwrap();
        }
        if buffer.is_empty() && dur == 0 {
            write!(buffer, "0s").unwrap();
        }
        serializer.serialize_str(&buffer)
    }
}

impl<'de> Deserialize<'de> for ReadableDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurVisitor;

        impl<'de> Visitor<'de> for DurVisitor {
            type Value = ReadableDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("valid duration")
            }

            fn visit_str<E>(self, dur_str: &str) -> Result<ReadableDuration, E>
            where
                E: de::Error,
            {
                let dur_str = dur_str.trim();
                if !dur_str.is_ascii() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str), &"ascii string"));
                }
                let err_msg = "valid duration, only h, m, s, ms are supported.";
                let mut left = dur_str.as_bytes();
                let mut last_unit = HOUR + 1;
                let mut dur = 0f64;
                while let Some(idx) = left.iter().position(|c| b"hms".contains(c)) {
                    let (first, second) = left.split_at(idx);
                    let unit = if second.starts_with(b"ms") {
                        left = &left[idx + 2..];
                        MS
                    } else {
                        let u = match second[0] {
                            b'h' => HOUR,
                            b'm' => MINUTE,
                            b's' => SECOND,
                            _ => return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg)),
                        };
                        left = &left[idx + 1..];
                        u
                    };
                    if unit >= last_unit {
                        return Err(E::invalid_value(
                            Unexpected::Str(dur_str),
                            &"h, m, s, ms should occur in giving order.",
                        ));
                    }
                    // do we need to check 12h360m?
                    let number_str = unsafe { str::from_utf8_unchecked(first) };
                    dur += match number_str.trim().parse::<f64>() {
                        Ok(n) => n * unit as f64,
                        Err(_) => return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg)),
                    };
                    last_unit = unit;
                }
                if !left.is_empty() {
                    return Err(E::invalid_value(Unexpected::Str(dur_str), &err_msg));
                }
                if dur.is_sign_negative() {
                    return Err(E::invalid_value(
                        Unexpected::Str(dur_str),
                        &"duration should be positive.",
                    ));
                }
                let secs = dur as u64 / SECOND as u64;
                let millis = (dur as u64 % SECOND as u64) as u32 * 1_000_000;
                Ok(ReadableDuration(Duration::new(secs, millis)))
            }
        }

        deserializer.deserialize_str(DurVisitor)
    }
}

macro_rules! numeric_enum_mod {
    ($name:ident $enum:ident { $($variant:ident = $value:expr, )* }) => {
        pub mod $name {
            use std::fmt;

            use serde::{Serializer, Deserializer};
            use serde::de::{self, Unexpected, Visitor};
            use rocksdb::$enum;

            pub fn serialize<S>(mode: &$enum, serializer: S) -> Result<S::Ok, S::Error>
                where S: Serializer
            {
                serializer.serialize_i64(*mode as i64)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<$enum, D::Error>
                where D: Deserializer<'de>
            {
                struct EnumVisitor;

                impl<'de> Visitor<'de> for EnumVisitor {
                    type Value = $enum;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        write!(formatter, concat!("valid ", stringify!($enum)))
                    }

                    fn visit_i64<E>(self, value: i64) -> Result<$enum, E>
                        where E: de::Error
                    {
                        match value {
                            $( $value => Ok($enum::$variant), )*
                            _ => Err(E::invalid_value(Unexpected::Signed(value), &self))
                        }
                    }
                }

                deserializer.deserialize_i64(EnumVisitor)
            }
        }
    }
}

numeric_enum_mod!{compaction_pri_serde CompactionPriority {
    ByCompensatedSize = 0,
    OldestLargestSeqFirst = 1,
    OldestSmallestSeqFirst = 2,
    MinOverlappingRatio = 3,
}}

numeric_enum_mod!{compaction_style_serde DBCompactionStyle {
    Level = 0,
    Universal = 1,
}}

numeric_enum_mod!{recovery_mode_serde DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}}

/// Convert Duration to milliseconds.
#[inline]
pub fn duration_to_ms(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

/// Convert Duration to seconds.
#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

/// Convert Duration to nanoseconds.
#[inline]
pub fn duration_to_nanos(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000_000_000 + nanos
}

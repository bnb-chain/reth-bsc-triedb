//! PathProvider trait definitions for key-value database operations.

use std::fmt::Debug;

// Default configuration constants
pub const DEFAULT_MAX_OPEN_FILES: i32 = -1; // Use OS default (unlimited)
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 256 * 1024 * 1024; // 256MB
pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 4;
pub const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 64 * 1024 * 1024; // 64MB
pub const DEFAULT_MAX_BACKGROUND_JOBS: i32 = 4;
pub const DEFAULT_CREATE_IF_MISSING: bool = true;
pub const DEFAULT_TRIE_NODECACHE_SIZE: u32 = 20_000_000; // 20M entries (~7GB); each entry ~350B (key 74B + blob avg 224B + moka overhead 52B)
pub const DEFAULT_STORAGE_ROOT_CACHE_SIZE: u32 = 6_000_000; // 6M entries (~1GB); each entry ~164B (key 56B + value 56B + moka overhead 52B)

// BlockBasedTable configuration constants (directly impacts random reads)
//
// NOTE: RocksDB's default block cache is ~8MB if unset, which is far too small
// for trie node random reads. We expose these explicitly so callers can tune
// based on machine memory and workload.
pub const DEFAULT_BLOCK_CACHE_SIZE_BYTES: usize = 16 * 1024 * 1024 * 1024; // 16GB
pub const DEFAULT_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;
pub const DEFAULT_BLOOM_FILTER_BLOCK_BASED: bool = false; // false = full/partition filter (recommended since RocksDB 5.x; block-based is deprecated)
pub const DEFAULT_CACHE_INDEX_AND_FILTER_BLOCKS: bool = true;
pub const DEFAULT_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE: bool = true;

// ReadOptions configuration constants
pub const DEFAULT_FILL_CACHE: bool = true;
// For trie node random reads, large readahead can add unnecessary IO and cache pollution.
pub const DEFAULT_READAHEAD_SIZE: usize = 4 * 1024; // 4KB
pub const DEFAULT_ASYNC_IO: bool = true;
pub const DEFAULT_VERIFY_CHECKSUMS: bool = false;

// WAL / obsolete-file retention constants.
//
// Why these matter (see bnb-chain/reth-bsc#322):
//
// PathDB opens RocksDB with four column families, but live writes land almost
// exclusively on the `default` (trie_node) and `storage_root` CFs. The
// `meta_data` CF is touched only on DiffLayer commits, and `trie_node` (the
// migration target) is currently idle. RocksDB cannot truncate a WAL segment
// while *any* CF memtable that was writing into it is still unflushed — so an
// idle CF pins every WAL segment written since its last flush.
//
// With the C++ defaults (`max_total_wal_size = 0`, `WAL_size_limit_MB = 0`,
// `WAL_ttl_seconds = 0`), this pinning is unbounded: the only thing that
// reclaims WAL is a process restart, which replays+flushes all WAL segments
// during recovery. Long-running full nodes therefore see the `rust_eth_triedb/`
// directory grow at ~10 GB/hour (one 4 GB `.log` every ~25 min) until restart.
//
// We fix this by bounding WAL on-disk size so RocksDB is forced to flush the
// CF pinning the oldest WAL, and by periodically sweeping obsolete files
// (stale SSTs from compaction) without requiring the DB to be reopened.
pub const DEFAULT_MAX_TOTAL_WAL_SIZE_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GB
pub const DEFAULT_WAL_SIZE_LIMIT_MB: u64 = 2 * 1024; // 2 GB — second line of defense for on-disk purge
pub const DEFAULT_KEEP_LOG_FILE_NUM: usize = 10; // info LOG files (separate from WAL); C++ default 1000 is excessive
pub const DEFAULT_DELETE_OBSOLETE_FILES_PERIOD_MICROS: u64 = 10 * 60 * 1_000_000; // 10 minutes (C++ default is 6 hours)

/// Result type for PathProvider operations.
pub type PathProviderResult<T> = Result<T, PathProviderError>;

/// Error type for PathProvider operations.
#[derive(Debug, thiserror::Error)]
pub enum PathProviderError {
    #[error("Database error: {0}")]
    Database(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Key not found: {0:?}")]
    KeyNotFound(Vec<u8>),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

/// Trait for database management operations.
pub trait PathProviderManager: Send + Sync + Debug {
    /// Close the database.
    fn close(&self) -> PathProviderResult<()>;

    /// Flush all pending writes to disk.
    fn flush(&self) -> PathProviderResult<()>;

    /// Compact the database.
    fn compact(&self) -> PathProviderResult<()>;
}

/// Configuration for PathProvider.
#[derive(Debug, Clone)]
pub struct PathProviderConfig {
    /// Maximum number of open files.
    pub max_open_files: i32,
    /// Write buffer size in bytes.
    pub write_buffer_size: usize,
    /// Maximum write buffer number.
    pub max_write_buffer_number: i32,
    /// Target file size for compaction.
    pub target_file_size_base: u64,
    /// Maximum background jobs.
    pub max_background_jobs: i32,
    /// Whether to create the database if it doesn't exist.
    pub create_if_missing: bool,
    /// LRU cache size in number of entries (default: 1M entries).
    pub trie_node_cache_size: u32,
    /// LRU cache size in number of entries (default: 1M entries).
    pub storage_root_cache_size: u32,

    /// RocksDB BlockBasedTable: block cache size in bytes.
    pub block_cache_size_bytes: usize,
    /// RocksDB BlockBasedTable: bloom filter bits per key.
    pub bloom_filter_bits_per_key: f64,
    /// RocksDB BlockBasedTable: whether to use block-based bloom filter.
    pub bloom_filter_block_based: bool,
    /// RocksDB BlockBasedTable: cache index/filter blocks in block cache.
    pub cache_index_and_filter_blocks: bool,
    /// RocksDB BlockBasedTable: pin L0 index/filter blocks in cache.
    pub pin_l0_filter_and_index_blocks_in_cache: bool,

    /// Whether to fill cache on reads.
    pub fill_cache: bool,
    /// Readahead size in bytes for sequential reads.
    pub readahead_size: usize,
    /// Whether to enable async IO for reads.
    pub async_io: bool,
    /// Whether to verify checksums on reads.
    pub verify_checksums: bool,

    /// Max total on-disk WAL size before RocksDB force-flushes the CF backing
    /// the oldest WAL. `0` = unbounded (C++ default). See #322: leaving this
    /// unbounded causes the data directory to grow ~10 GB/hour until restart
    /// because idle CFs pin old WAL segments indefinitely.
    pub max_total_wal_size_bytes: u64,
    /// Total WAL size cap in MiB above which RocksDB starts deleting the
    /// earliest WAL files. `0` = disabled (C++ default). Used alongside
    /// `max_total_wal_size_bytes` as a second line of defense.
    pub wal_size_limit_mb: u64,
    /// Maximum number of info LOG files (LOG, LOG.old.*) to keep. C++ default
    /// is 1000 which is overkill for a node operator.
    pub keep_log_file_num: usize,
    /// Periodicity of the RocksDB obsolete-file sweep. Stale SSTs left behind
    /// by compaction are released on every compaction, but this knob ensures
    /// they are also swept on a wall-clock schedule without needing a restart.
    pub delete_obsolete_files_period_micros: u64,
}

impl Default for PathProviderConfig {
    fn default() -> Self {
        Self {
            max_open_files: DEFAULT_MAX_OPEN_FILES,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            max_write_buffer_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            target_file_size_base: DEFAULT_TARGET_FILE_SIZE_BASE,
            max_background_jobs: DEFAULT_MAX_BACKGROUND_JOBS,
            create_if_missing: DEFAULT_CREATE_IF_MISSING,
            trie_node_cache_size: DEFAULT_TRIE_NODECACHE_SIZE,
            storage_root_cache_size: DEFAULT_STORAGE_ROOT_CACHE_SIZE,
            block_cache_size_bytes: DEFAULT_BLOCK_CACHE_SIZE_BYTES,
            bloom_filter_bits_per_key: DEFAULT_BLOOM_FILTER_BITS_PER_KEY,
            bloom_filter_block_based: DEFAULT_BLOOM_FILTER_BLOCK_BASED,
            cache_index_and_filter_blocks: DEFAULT_CACHE_INDEX_AND_FILTER_BLOCKS,
            pin_l0_filter_and_index_blocks_in_cache:
                DEFAULT_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE,
            fill_cache: DEFAULT_FILL_CACHE,
            readahead_size: DEFAULT_READAHEAD_SIZE,
            async_io: DEFAULT_ASYNC_IO,
            verify_checksums: DEFAULT_VERIFY_CHECKSUMS,
            max_total_wal_size_bytes: DEFAULT_MAX_TOTAL_WAL_SIZE_BYTES,
            wal_size_limit_mb: DEFAULT_WAL_SIZE_LIMIT_MB,
            keep_log_file_num: DEFAULT_KEEP_LOG_FILE_NUM,
            delete_obsolete_files_period_micros: DEFAULT_DELETE_OBSOLETE_FILES_PERIOD_MICROS,
        }
    }
}

impl PathProviderConfig {
    /// Apply overrides from `RETHBSC_ROCKSDB_*` environment variables on top of
    /// an existing config. Only set variables are applied; unset ones keep the
    /// current value. Parse errors are ignored so a bad value doesn't crash the
    /// node — the default is used instead. Supported vars:
    ///
    /// - `RETHBSC_ROCKSDB_WRITE_BUFFER_SIZE_MB`   (default 256)
    /// - `RETHBSC_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` (default 4)
    /// - `RETHBSC_ROCKSDB_TARGET_FILE_SIZE_MB`    (default 64)
    /// - `RETHBSC_ROCKSDB_MAX_BACKGROUND_JOBS`    (default 4)
    /// - `RETHBSC_ROCKSDB_BLOCK_CACHE_GB`         (default 16)
    /// - `RETHBSC_ROCKSDB_BLOOM_BITS_PER_KEY`     (default 10.0)
    /// - `RETHBSC_ROCKSDB_TRIE_NODE_CACHE_ENTRIES` (default 20_000_000)
    /// - `RETHBSC_ROCKSDB_MAX_TOTAL_WAL_MB`       (default 2048)
    /// - `RETHBSC_ROCKSDB_WAL_SIZE_LIMIT_MB`      (default 2048)
    /// - `RETHBSC_ROCKSDB_KEEP_LOG_FILE_NUM`      (default 10)
    /// - `RETHBSC_ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_SECS` (default 600)
    pub fn apply_env_overrides(mut self) -> Self {
        fn env_usize(key: &str) -> Option<usize> {
            std::env::var(key).ok().and_then(|s| s.parse().ok())
        }
        fn env_i32(key: &str) -> Option<i32> {
            std::env::var(key).ok().and_then(|s| s.parse().ok())
        }
        fn env_u64(key: &str) -> Option<u64> {
            std::env::var(key).ok().and_then(|s| s.parse().ok())
        }
        fn env_f64(key: &str) -> Option<f64> {
            std::env::var(key).ok().and_then(|s| s.parse().ok())
        }
        fn env_u32(key: &str) -> Option<u32> {
            std::env::var(key).ok().and_then(|s| s.parse().ok())
        }
        if let Some(mb) = env_usize("RETHBSC_ROCKSDB_WRITE_BUFFER_SIZE_MB") {
            self.write_buffer_size = mb.saturating_mul(1024 * 1024);
        }
        if let Some(n) = env_i32("RETHBSC_ROCKSDB_MAX_WRITE_BUFFER_NUMBER") {
            self.max_write_buffer_number = n.max(1);
        }
        if let Some(mb) = env_u64("RETHBSC_ROCKSDB_TARGET_FILE_SIZE_MB") {
            self.target_file_size_base = mb.saturating_mul(1024 * 1024);
        }
        if let Some(n) = env_i32("RETHBSC_ROCKSDB_MAX_BACKGROUND_JOBS") {
            self.max_background_jobs = n.max(1);
        }
        if let Some(gb) = env_usize("RETHBSC_ROCKSDB_BLOCK_CACHE_GB") {
            self.block_cache_size_bytes = gb.saturating_mul(1024 * 1024 * 1024);
        }
        if let Some(f) = env_f64("RETHBSC_ROCKSDB_BLOOM_BITS_PER_KEY") {
            self.bloom_filter_bits_per_key = f;
        }
        if let Some(n) = env_u32("RETHBSC_ROCKSDB_TRIE_NODE_CACHE_ENTRIES") {
            self.trie_node_cache_size = n;
        }
        if let Some(mb) = env_u64("RETHBSC_ROCKSDB_MAX_TOTAL_WAL_MB") {
            self.max_total_wal_size_bytes = mb.saturating_mul(1024 * 1024);
        }
        if let Some(mb) = env_u64("RETHBSC_ROCKSDB_WAL_SIZE_LIMIT_MB") {
            self.wal_size_limit_mb = mb;
        }
        if let Some(n) = env_usize("RETHBSC_ROCKSDB_KEEP_LOG_FILE_NUM") {
            self.keep_log_file_num = n.max(1);
        }
        if let Some(secs) = env_u64("RETHBSC_ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_SECS") {
            self.delete_obsolete_files_period_micros = secs.saturating_mul(1_000_000);
        }
        self
    }
}

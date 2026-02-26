//! PathProvider trait definitions for key-value database operations.

use std::fmt::Debug;

// Default configuration constants
pub const DEFAULT_MAX_OPEN_FILES: i32 = 10000000;
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 256 * 1024 * 1024; // 256MB
pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 4;
pub const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 64 * 1024 * 1024; // 64MB
pub const DEFAULT_MAX_BACKGROUND_JOBS: i32 = 4;
pub const DEFAULT_CREATE_IF_MISSING: bool = true;
pub const DEFAULT_TRIE_NODECACHE_SIZE: u32 = 40_000_000; // 4KW entries
pub const DEFAULT_STORAGE_ROOT_CACHE_SIZE: u32 = 200_000_000; // 20KW entries

// BlockBasedTable configuration constants (directly impacts random reads)
//
// NOTE: RocksDB's default block cache is ~8MB if unset, which is far too small
// for trie node random reads. We expose these explicitly so callers can tune
// based on machine memory and workload.
pub const DEFAULT_BLOCK_CACHE_SIZE_BYTES: usize = 8 * 1024 * 1024 * 1024; // 8GB
pub const DEFAULT_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;
pub const DEFAULT_BLOOM_FILTER_BLOCK_BASED: bool = true;
pub const DEFAULT_CACHE_INDEX_AND_FILTER_BLOCKS: bool = true;
pub const DEFAULT_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE: bool = true;

// ReadOptions configuration constants
pub const DEFAULT_FILL_CACHE: bool = true;
// For trie node random reads, large readahead can add unnecessary IO and cache pollution.
pub const DEFAULT_READAHEAD_SIZE: usize = 4 * 1024; // 4KB
pub const DEFAULT_ASYNC_IO: bool = true;
pub const DEFAULT_VERIFY_CHECKSUMS: bool = false;

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
        }
    }
}

//! PathDB implementation for RocksDB integration.

use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use rocksdb::{
    BlockBasedOptions, Cache as RocksCache, ColumnFamilyDescriptor, DB, Options, ReadOptions,
    WriteBatch, WriteOptions,
};
// use schnellru::{ByLength, LruMap};
use mini_moka::sync::{Cache as MokaCache, CacheBuilder};
use tracing::{error, trace, warn};

use alloy_primitives::B256;
use alloy_trie::EMPTY_ROOT_HASH;
use crate::traits::*;
use rust_eth_triedb_common::{TrieDatabase, DiffLayer, TRIE_STATE_ROOT_KEY, TRIE_STATE_BLOCK_NUMBER_KEY};

// use reth_metrics::{
//     metrics::{Counter},
//     Metrics,
// };

/// The default column family name used for storing trie nodes.
///
/// This column family currently stores the actual trie node data, where each key
/// represents a path prefix in the trie structure, and the value contains
/// the encoded node data (RLP-encoded or similar format).
///
/// # Future Migration
///
/// The trie node data stored in this column family will be migrated to
/// `TRIE_NODE_COLUMN_FAMILY_NAME` in the future. After migration, this column
/// family may be deprecated or repurposed. The migration is planned to improve
/// data organization and separation of concerns.
pub const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

/// The column family name used for storing trie nodes.
///
/// This column family is the target destination for trie node data migration
/// from `DEFAULT_COLUMN_FAMILY_NAME`. It stores the actual trie node data,
/// where each key represents a path prefix in the trie structure, and the
/// value contains the encoded node data (RLP-encoded or similar format).
///
/// # Migration Status
///
/// Currently, trie nodes are still stored in `DEFAULT_COLUMN_FAMILY_NAME`.
/// Future versions will migrate all trie node data to this column family for
/// better organization and clearer separation from metadata.
pub const TRIE_NODE_COLUMN_FAMILY_NAME: &str = "trie_node";

/// The column family name used for storing trie metadata.
///
/// This column family stores metadata related to the trie state, including:
/// - State root hash (`TRIE_STATE_ROOT_KEY`)
/// - Block number (`TRIE_STATE_BLOCK_NUMBER_KEY`)
pub const META_COLUMN_FAMILY_NAME: &str = "meta_data";

/// The column family name used for storing storage trie roots.
///
/// This column family maps account address hashes (Keccak-256 hashed addresses)
/// to their corresponding storage trie root hashes. Each Ethereum account has
/// its own storage trie, and this column family maintains the root hash for
/// each account's storage state.
///
/// # Key-Value Format
///
/// - **Key**: `B256` (32 bytes) - The Keccak-256 hash of an account address
/// - **Value**: `B256` (32 bytes) - The root hash of the account's storage trie
pub const STORAGE_ROOT_COLUMN_FAMILY_NAME: &str = "storage_root";

/// An array containing all column family names used by PathDB.
///
/// This array is used during database initialization to ensure all required
/// column families are created if they don't already exist. The order of
/// column families in this array is not significant, but all four must be
/// present for PathDB to function correctly.
///
/// # Column Families
///
/// 1. `DEFAULT_COLUMN_FAMILY_NAME` - Currently stores trie node data (will be migrated to `TRIE_NODE_COLUMN_FAMILY_NAME`)
/// 2. `META_COLUMN_FAMILY_NAME` - Stores trie metadata (state root, block number)
/// 3. `STORAGE_ROOT_COLUMN_FAMILY_NAME` - Stores storage trie roots
/// 4. `TRIE_NODE_COLUMN_FAMILY_NAME` - Target destination for trie node data migration
const COLUMN_FAMILY_NAMES: [&str; 4] = [DEFAULT_COLUMN_FAMILY_NAME, META_COLUMN_FAMILY_NAME, STORAGE_ROOT_COLUMN_FAMILY_NAME, TRIE_NODE_COLUMN_FAMILY_NAME];

// Metrics for the `PathDB`.
// #[derive(Metrics, Clone)]
// #[metrics(scope = "rust.eth.triedb.pathdb")]
// pub(crate) struct PathDBMetrics {
//     /// Counter of cache hits
//     pub(crate) trie_node_cache_hits: Counter,
//     /// Counter of cache misses
//     pub(crate) trie_node_cache_misses: Counter,
//     /// Counter of storage root cache hits
//     pub(crate) storage_root_cache_hits: Counter,
//     /// Counter of storage root cache misses
//     pub(crate) storage_root_cache_misses: Counter,
// }

/// PathDB implementation using RocksDB.
pub struct PathDB {
    /// The underlying RocksDB instance.
    pub db: Arc<DB>,
    /// Set of Column Family names that exist in the database.
    column_family_names: Arc<Mutex<HashSet<String>>>,
    /// Configuration for the database.
    pub config: PathProviderConfig,
    /// Write options for batch operations.
    pub write_options: WriteOptions,
    /// Read options for read operations.
    pub read_options: ReadOptions,
    /// Thread-safe LRU cache for trie node key-value pairs.
    /// Uses mini_moka for high-concurrency performance with sharded locks.
    pub trie_node_cache: Arc<MokaCache<Vec<u8>, Option<Vec<u8>>>>,
    /// Thread-safe LRU cache for storage root key-value pairs.
    /// Uses mini_moka for high-concurrency performance with sharded locks.
    pub storage_root_cache: Arc<MokaCache<Vec<u8>, Option<Vec<u8>>>>,
    /// Recently committed diff layers, pinned in memory for fast lookup.
    /// Newest layer is at the front. Bounded by `config.max_committed_difflayers`.
    committed_difflayers: Arc<RwLock<VecDeque<Arc<DiffLayer>>>>,
    /// Shared counters for diagnosing trie-node cache hit ratio.
    trie_node_cache_counters: Arc<TrieNodeCacheCounters>,
    // /// Metrics for the PathDB.
    // metrics: PathDBMetrics,
}

#[derive(Debug, Default)]
struct TrieNodeCacheCounters {
    committed_difflayer_hits: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    rocksdb_get_calls: AtomicU64,
    rocksdb_get_found: AtomicU64,
    rocksdb_get_not_found: AtomicU64,
    rocksdb_get_errors: AtomicU64,
    rocksdb_get_us_total: AtomicU64,
}

/// Build a consistent RocksDB BlockBasedTable configuration for trie workloads.
///
/// Returns the cache object alongside the options to ensure the cache stays alive while the
/// options are being installed into DB/CF options.
fn build_block_based_options(config: &PathProviderConfig) -> (RocksCache, BlockBasedOptions) {
    let rocks_block_cache = RocksCache::new_lru_cache(config.block_cache_size_bytes);
    let mut block_based = BlockBasedOptions::default();
    block_based.set_block_cache(&rocks_block_cache);
    block_based.set_bloom_filter(config.bloom_filter_bits_per_key, config.bloom_filter_block_based);
    block_based.set_cache_index_and_filter_blocks(config.cache_index_and_filter_blocks);
    block_based.set_pin_l0_filter_and_index_blocks_in_cache(
        config.pin_l0_filter_and_index_blocks_in_cache,
    );
    (rocks_block_cache, block_based)
}

impl Debug for PathDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PathDB")
            .field("config", &self.config)
            .field("column_family_names", &self.column_family_names)
            .finish()
    }
}

impl Clone for PathDB {
    fn clone(&self) -> Self {
        let write_options = WriteOptions::default();
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(self.config.fill_cache);
        read_options.set_readahead_size(self.config.readahead_size);
        read_options.set_async_io(self.config.async_io);
        read_options.set_verify_checksums(self.config.verify_checksums);

        Self {
            db: self.db.clone(),
            column_family_names: self.column_family_names.clone(),
            config: self.config.clone(),
            write_options,
            read_options,
            trie_node_cache: self.trie_node_cache.clone(),
            storage_root_cache: self.storage_root_cache.clone(),
            committed_difflayers: self.committed_difflayers.clone(),
            trie_node_cache_counters: self.trie_node_cache_counters.clone(),
            // metrics: self.metrics.clone(),
        }
    }
}

impl PathDB {
    /// Create a new PathDB instance.
    pub fn new(path: &str, config: PathProviderConfig) -> PathProviderResult<Self> {
        let mut db_opts = Options::default();
        db_opts.set_max_open_files(config.max_open_files);
        db_opts.set_write_buffer_size(config.write_buffer_size);
        db_opts.set_max_write_buffer_number(config.max_write_buffer_number);
        db_opts.set_target_file_size_base(config.target_file_size_base);
        db_opts.set_max_background_jobs(config.max_background_jobs);
        db_opts.create_if_missing(config.create_if_missing);

        // Explicitly configure BlockBasedTable. This directly impacts random reads
        // of trie nodes. If unset, RocksDB defaults to a tiny internal cache (~8MB).
        let (_rocks_block_cache, block_based) = build_block_based_options(&config);
        db_opts.set_block_based_table_factory(&block_based);
        
        // Disable auto compaction during startup to avoid slow initialization
        // Compaction will happen automatically in the background during runtime
        db_opts.set_disable_auto_compactions(true);

        // Ensure all required Column Families exist
        ensure_column_families(path, &db_opts, &config, &block_based)?;

        // Now open database with all required Column Families
        let mut cf_descriptors = Vec::new();
        for cf_name in COLUMN_FAMILY_NAMES {
            let mut cf_opts = Options::default();
            cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
            cf_opts.set_write_buffer_size(config.write_buffer_size);
            cf_opts.set_block_based_table_factory(&block_based);
            // Disable auto compaction for each column family as well
            cf_opts.set_disable_auto_compactions(true);
            cf_descriptors.push(ColumnFamilyDescriptor::new(cf_name, cf_opts));
        }

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .map_err(|e| PathProviderError::Database(format!("Failed to open RocksDB: {}", e)))?;
        
        // Re-enable auto compaction after database is opened
        // This allows compaction to happen gradually in the background during runtime
        // without blocking startup
        for cf_name in COLUMN_FAMILY_NAMES {
            if let Some(cf) = db.cf_handle(cf_name) {
                if let Err(e) = db.set_options_cf(&cf, &[("disable_auto_compactions", "false")]) {
                    warn!(
                        target: "pathdb::rocksdb",
                        "Failed to re-enable auto compaction for column family '{}': {}", cf_name, e
                    );
                } else {
                    trace!(
                        target: "pathdb::rocksdb",
                        "Re-enabled auto compaction for column family '{}'", cf_name
                    );
                }
            }
        }

        let cf_names_set: HashSet<String> = COLUMN_FAMILY_NAMES.iter().map(|s| s.to_string()).collect();

        let write_options = WriteOptions::default();

        let mut read_options = ReadOptions::default();
        read_options.fill_cache(config.fill_cache);
        read_options.set_readahead_size(config.readahead_size);
        read_options.set_async_io(config.async_io);
        read_options.set_verify_checksums(config.verify_checksums);

        // Create byte-weighted MokaCache instances.
        // The weigher estimates the heap cost of each entry (key + value + per-entry overhead).
        // max_capacity is set to the byte budget so eviction is driven by memory, not entry count.
        let trie_node_cache = Arc::new(
            CacheBuilder::new(config.trie_node_cache_capacity_bytes as u64)
                .weigher(|k: &Vec<u8>, v: &Option<Vec<u8>>| -> u32 {
                    let size = k.len() + v.as_ref().map_or(0, |v| v.len()) + 64;
                    u32::try_from(size).unwrap_or(u32::MAX)
                })
                .max_capacity(config.trie_node_cache_capacity_bytes as u64)
                .build()
        );
        let storage_root_cache = Arc::new(
            CacheBuilder::new(config.storage_root_cache_capacity_bytes as u64)
                .weigher(|k: &Vec<u8>, v: &Option<Vec<u8>>| -> u32 {
                    let size = k.len() + v.as_ref().map_or(0, |v| v.len()) + 64;
                    u32::try_from(size).unwrap_or(u32::MAX)
                })
                .max_capacity(config.storage_root_cache_capacity_bytes as u64)
                .build()
        );

        Ok(Self {
            db: Arc::new(db),
            column_family_names: Arc::new(Mutex::new(cf_names_set)),
            config,
            write_options,
            read_options,
            trie_node_cache,
            storage_root_cache,
            committed_difflayers: Arc::new(RwLock::new(VecDeque::new())),
            trie_node_cache_counters: Arc::new(TrieNodeCacheCounters::default()),
            // metrics: PathDBMetrics::new_with_labels(&[("instance", "default")]),
        })
    }

    /// Get the underlying RocksDB instance.
    pub fn inner(&self) -> &Arc<DB> {
        &self.db
    }

    /// Get the configuration.
    pub fn config(&self) -> &PathProviderConfig {
        &self.config
    }

    /// Clear the LRU cache and committed diff layers.
    pub fn clear_cache(&self) {
        warn!(target: "pathdb::rocksdb", "Clearing LRU cache and committed difflayers");
        self.trie_node_cache.invalidate_all();
        self.storage_root_cache.invalidate_all();
        self.committed_difflayers.write().clear();
    }

    /// Get cache statistics (entry counts).
    pub fn cache_stats(&self) -> (usize, usize) {
        // mini_moka Cache is thread-safe, no locking needed
        (
            self.trie_node_cache.entry_count() as usize,
            self.storage_root_cache.entry_count() as usize,
        )
    }

    /// Get byte-weighted cache sizes (trie_node_weighted_bytes, storage_root_weighted_bytes).
    pub fn cache_weight_stats(&self) -> (u64, u64) {
        (
            self.trie_node_cache.weighted_size(),
            self.storage_root_cache.weighted_size(),
        )
    }

    /// Returns the current number of pinned committed diff layers.
    pub fn committed_difflayers_depth(&self) -> usize {
        self.committed_difflayers.read().len()
    }

    /// Returns the cumulative number of committed-difflayer hits.
    pub fn committed_difflayer_hit_count(&self) -> u64 {
        self.trie_node_cache_counters.committed_difflayer_hits.load(Ordering::Relaxed)
    }

    // /// Create a new metrics instance for the PathDB.
    // pub fn with_new_metrics(&mut self, instance_name: &str) {
    //     self.metrics = PathDBMetrics::new_with_labels(&[("instance", instance_name.to_string())]);
    // }
}

impl PathDB {
    pub fn get_raw_trie_node(&self, key: &[u8]) -> PathProviderResult<Option<Vec<u8>>> {
        trace!(target: "pathdb::rocksdb", "Getting key: {:?}", key);

        // Allocate key_vec once — needed by MokaCache (Arc<K> doesn't impl Borrow<[u8]>)
        // and reused for the insert path on miss.
        let key_vec = key.to_vec();

        // 1. Check MokaCache first — this is the common-case fast path.
        //    Most reads (~90%+) hit here, so we avoid the 128-layer committed scan.
        if let Some(cached_value) = self.trie_node_cache.get(&key_vec) {
            self.trie_node_cache_counters.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached_value);
        }

        // 2. Check committed diff layers (pinned, zero-alloc via Borrow<[u8]>).
        //    Catches recently-committed nodes that were evicted from MokaCache.
        //    The ~10μs scan cost is negligible vs a RocksDB read (50-500μs).
        if self.config.max_committed_difflayers > 0 {
            let layers = self.committed_difflayers.read();
            if !layers.is_empty() {
                for layer in layers.iter() {
                    if let Some(node) = layer.diff_nodes.get(key) {
                        self.trie_node_cache_counters
                            .committed_difflayer_hits
                            .fetch_add(1, Ordering::Relaxed);
                        if node.is_deleted() {
                            // Re-promote into MokaCache to avoid repeated layer scans
                            self.trie_node_cache.insert(key_vec, None);
                            return Ok(None);
                        }
                        let blob = node.blob.clone();
                        // Re-promote into MokaCache so subsequent reads take the fast path
                        self.trie_node_cache.insert(key_vec, blob.clone());
                        return Ok(blob);
                    }
                }
            }
        }

        // Neither MokaCache nor committed layers had it — this is a true miss.
        self.trie_node_cache_counters.misses.fetch_add(1, Ordering::Relaxed);

        // 3. RocksDB fallback
        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        self.trie_node_cache_counters
            .rocksdb_get_calls
            .fetch_add(1, Ordering::Relaxed);
        let rocksdb_get_start = std::time::Instant::now();
        let res = self.db.get_cf_opt(&cf, key, &self.read_options);
        let rocksdb_get_us = rocksdb_get_start.elapsed().as_micros() as u64;
        self.trie_node_cache_counters
            .rocksdb_get_us_total
            .fetch_add(rocksdb_get_us, Ordering::Relaxed);

        match res {
            Ok(Some(value)) => {
                self.trie_node_cache_counters
                    .rocksdb_get_found
                    .fetch_add(1, Ordering::Relaxed);
                trace!(target: "pathdb::rocksdb", "Found value in CF '{}' for key: {:?}", DEFAULT_COLUMN_FAMILY_NAME, key);
                self.trie_node_cache.insert(key_vec, Some(value.clone()));
                Ok(Some(value))
            }
            Ok(None) => {
                self.trie_node_cache_counters
                    .rocksdb_get_not_found
                    .fetch_add(1, Ordering::Relaxed);
                trace!(target: "pathdb::rocksdb", "Key not found in CF '{}': {:?}", DEFAULT_COLUMN_FAMILY_NAME, key);
                self.trie_node_cache.insert(key_vec, None);
                Ok(None)
            }
            Err(e) => {
                self.trie_node_cache_counters
                    .rocksdb_get_errors
                    .fetch_add(1, Ordering::Relaxed);
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Error getting in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB get in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn put_raw_trie_node(&self, key: &[u8], value: &[u8]) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Putting key: {:?}, value_len: {}", key, value.len());

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        // Write to DB first
        match self.db.put_cf_opt(&cf, key, value, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully put in CF '{}' for key {:?}", DEFAULT_COLUMN_FAMILY_NAME, key);
                // Update cache after successful write - mini_moka is thread-safe
                self.trie_node_cache.insert(key.to_vec(), Some(value.to_vec()));
                Ok(())
            }
            Err(e) => {
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Error putting in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB put in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn delete_raw_trie_node(&self, key: &[u8]) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Deleting key: {:?}", key);

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        let key_vec = key.to_vec();

        // Delete from DB first
        match self.db.delete_cf_opt(&cf, key, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully deleted in CF '{}' for key {:?}", DEFAULT_COLUMN_FAMILY_NAME, key);
                // Remove from cache after successful delete
                self.trie_node_cache.invalidate(&key_vec);
                Ok(())
            }
            Err(e) => {
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Error deleting in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB delete in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn exists_raw_trie_node(&self, key: &[u8]) -> PathProviderResult<bool> {
        trace!(target: "pathdb::rocksdb", "Checking existence of key: {:?}", key);

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        // Cache miss, check DB
        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(_)) => {
                trace!(target: "pathdb::rocksdb", "Key exists in CF '{}' for key {:?}", DEFAULT_COLUMN_FAMILY_NAME, key);
                Ok(true)
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key does not exist in CF '{}' for key {:?}", DEFAULT_COLUMN_FAMILY_NAME, key);
                Ok(false)
            }
            Err(e) => {
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Error checking existence of key in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB exists in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn get_raw_storage_root(&self, key: &[u8]) -> PathProviderResult<Option<Vec<u8>>> {
        trace!(target: "pathdb::rocksdb", "Getting key: {:?}", key);

        let key_vec = key.to_vec();

        // 1. Check MokaCache first — common-case fast path
        if let Some(cached_value) = self.storage_root_cache.get(&key_vec) {
            return Ok(cached_value);
        }

        // 2. Check committed diff layers for storage roots before RocksDB
        if self.config.max_committed_difflayers > 0 && key.len() == 32 {
            let key_b256 = B256::from_slice(key);
            let layers = self.committed_difflayers.read();
            if !layers.is_empty() {
                for layer in layers.iter() {
                    if let Some(root) = layer.diff_storage_roots.get(&key_b256) {
                        let value = root.as_slice().to_vec();
                        // Re-promote into MokaCache
                        self.storage_root_cache.insert(key_vec, Some(value.clone()));
                        return Ok(Some(value));
                    }
                }
            }
        }

        // 3. RocksDB fallback
        let cf = self.db.cf_handle(STORAGE_ROOT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", STORAGE_ROOT_COLUMN_FAMILY_NAME))
        })?;

        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(value)) => {
                trace!(target: "pathdb::rocksdb", "Found value in CF '{}' for key {:?}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key);
                self.storage_root_cache.insert(key_vec, Some(value.clone()));
                Ok(Some(value))
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key not found in CF '{}' for key {:?}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key);
                self.storage_root_cache.insert(key_vec, None);
                Ok(None)
            }
            Err(e) => {
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Error getting in CF '{}' for key 0x{}: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB get in CF '{}' for key 0x{} error: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn put_raw_storage_root(&self, key: &[u8], value: &[u8]) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Putting storage root key: {:?}, value_len: {}", key, value.len());

        let cf = self.db.cf_handle(STORAGE_ROOT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", STORAGE_ROOT_COLUMN_FAMILY_NAME))
        })?;

        // Write to DB first
        match self.db.put_cf_opt(&cf, key, value, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully put value in CF '{}' for key {:?}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key);
                // Update cache after successful write - mini_moka is thread-safe
                self.storage_root_cache.insert(key.to_vec(), Some(value.to_vec()));
                Ok(())
            }
            Err(e) => {
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Error putting in CF '{}' for key 0x{}: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB put in CF '{}' for key 0x{} error: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn get_raw_meta_data(&self, key: &[u8]) -> PathProviderResult<Option<Vec<u8>>> {
        // Check cache first - metadata uses trie_node_cache
        let key_vec = key.to_vec();
        if let Some(cached_value) = self.trie_node_cache.get(&key_vec) {
            trace!(target: "pathdb::rocksdb", "Found value in cache for key: {:?}", key);
            return Ok(cached_value);
        }

        // TODO:: change to META_COLUMN_FAMILY_NAME from default CF in the future.
        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        // Convert key to readable string: try UTF-8 first, fallback to hex if invalid
        let key_string = String::from_utf8_lossy(key).to_string();
        
        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(value)) => {
                trace!(target: "pathdb::rocksdb", "Found value in CF '{}' for key: {}", DEFAULT_COLUMN_FAMILY_NAME, key_string);
                // Insert into cache - mini_moka handles LRU eviction automatically
                self.trie_node_cache.insert(key_vec, Some(value.clone()));
                Ok(Some(value))
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key not found in CF '{}' for key: {}", DEFAULT_COLUMN_FAMILY_NAME, key_string);
                // Cache None values to avoid repeated DB lookups
                self.trie_node_cache.insert(key_vec, None);
                Ok(None)
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error getting in CF '{}' for key {}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_string, e);
                Err(PathProviderError::Database(format!("RocksDB get in CF '{}' for key {} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_string, e)))
            }
        }
    }
}

impl PathProviderManager for PathDB {
    fn close(&self) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Closing database");

        // RocksDB automatically closes when the last Arc is dropped
        Ok(())
    }

    fn flush(&self) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Flushing database");

        match self.db.flush() {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully flushed database");
                Ok(())
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error flushing database: {}", e);
                Err(PathProviderError::Database(format!("Flush error: {}", e)))
            }
        }
    }

    fn compact(&self) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Compacting database");

        // Compact all column families over the full key-range.
        //
        // This can be useful to:
        // - Rewrite existing SSTs so bloom/filter/index blocks match current table settings.
        // - Reduce read amplification after large write bursts / state transitions.
        for cf_name in COLUMN_FAMILY_NAMES {
            let cf = self.db.cf_handle(cf_name).ok_or_else(|| {
                PathProviderError::Database(format!(
                    "Column Family '{}' handle not found for compaction",
                    cf_name
                ))
            })?;
            self.db
                .compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }
}

impl TrieDatabase for PathDB {
    type Error = PathProviderError;

    fn get_trie_node(&self, path: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_raw_trie_node(path)
    }

    fn trie_node_cache_counters(&self) -> Option<(u64, u64)> {
        // Include committed_difflayer_hits in the hit count so the hit-ratio
        // accurately reflects "how often we avoided RocksDB".
        let moka_hits = self.trie_node_cache_counters.hits.load(Ordering::Relaxed);
        let difflayer_hits = self.trie_node_cache_counters.committed_difflayer_hits.load(Ordering::Relaxed);
        let misses = self.trie_node_cache_counters.misses.load(Ordering::Relaxed);
        Some((moka_hits + difflayer_hits, misses))
    }

    fn trie_node_rocksdb_counters(&self) -> Option<(u64, u64, u64, u64, u64)> {
        Some((
            self.trie_node_cache_counters.rocksdb_get_calls.load(Ordering::Relaxed),
            self.trie_node_cache_counters.rocksdb_get_found.load(Ordering::Relaxed),
            self.trie_node_cache_counters.rocksdb_get_not_found.load(Ordering::Relaxed),
            self.trie_node_cache_counters.rocksdb_get_errors.load(Ordering::Relaxed),
            self.trie_node_cache_counters.rocksdb_get_us_total.load(Ordering::Relaxed),
        ))
    }

    fn insert_trie_node(&self, path: &[u8], data: Vec<u8>) -> Result<(), Self::Error> {
        self.put_raw_trie_node(path, &data)
    }

    fn contains_trie_node(&self, path: &[u8]) -> Result<bool, Self::Error> {
        self.exists_raw_trie_node(path)
    }

    fn remove_trie_node(&self, path: &[u8]) {
        let _ = self.delete_raw_trie_node(path);
    }

    fn get_storage_root(&self, hased_address: B256) -> Result<Option<B256>, Self::Error> {
        let value = self.get_raw_storage_root(hased_address.as_slice())?;
        if let Some(value) = value {
            if value.len() == 32 {
                Ok(Some(B256::from_slice(&value)))
            } else {
                let address_hex = format!("0x{:x}", hased_address);
                let value_hex = value.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                error!(target: "pathdb::rocksdb", "Storage root value length is not 32 for address: {}, value_len: {}, value: 0x{}", address_hex, value.len(), value_hex);
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn put_storage_root(&self, hased_address: B256, value: B256) -> Result<(), Self::Error> {
        self.put_raw_storage_root(hased_address.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn clear_cache(&self) {
        self.clear_cache();
    }

    fn latest_persist_state(&self) -> Result<(u64, B256), Self::Error> {
        let block_number_bytes = self.get_raw_meta_data(TRIE_STATE_BLOCK_NUMBER_KEY)?;
        let state_root_bytes = self.get_raw_meta_data(TRIE_STATE_ROOT_KEY)?;
        
        if let (Some(block_number_bytes), Some(state_root_bytes)) = (block_number_bytes, state_root_bytes) {
            let block_number = u64::from_le_bytes(block_number_bytes.try_into().unwrap());
            let state_root = B256::from_slice(&state_root_bytes);
            Ok((block_number, state_root))
        } else {
            Ok((0, EMPTY_ROOT_HASH))
        }
    }

    fn commit_difflayer(&self, block_number: u64, state_root: B256, difflayer: &Option<Arc<DiffLayer>>) -> Result<(), Self::Error> {
        // Get Column Family handle for default CF
        let default_cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        let meta_cf = self.db.cf_handle(META_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", META_COLUMN_FAMILY_NAME))
        })?;

        let storage_root_cf = self.db.cf_handle(STORAGE_ROOT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", STORAGE_ROOT_COLUMN_FAMILY_NAME))
        })?;

        let mut diff_nodes_len = 0;
        let mut diff_storage_roots_len = 0;

        let mut batch = WriteBatch::default();
        {
            batch.put_cf(&default_cf, TRIE_STATE_ROOT_KEY, state_root.as_slice());
            batch.put_cf(&default_cf, TRIE_STATE_BLOCK_NUMBER_KEY, block_number.to_le_bytes());

            // TODO:: double Write to meta CF using put_cf, will be delete default CF in the future.
            batch.put_cf(&meta_cf, TRIE_STATE_ROOT_KEY, state_root.as_slice());
            batch.put_cf(&meta_cf, TRIE_STATE_BLOCK_NUMBER_KEY, block_number.to_le_bytes());
        
            self.trie_node_cache.insert(TRIE_STATE_ROOT_KEY.to_vec(), Some(state_root.as_slice().to_vec()));
            self.trie_node_cache.insert(TRIE_STATE_BLOCK_NUMBER_KEY.to_vec(), Some(block_number.to_le_bytes().to_vec()));

            if let Some(difflayer) = difflayer {
                diff_nodes_len = difflayer.diff_nodes.len();
                diff_storage_roots_len = difflayer.diff_storage_roots.len();

                for (key, node) in difflayer.diff_nodes.iter() {
                    if node.is_deleted() {
                        self.trie_node_cache.invalidate(key);
                        batch.delete_cf(&default_cf, key);
                    } else if let Some(blob) = &node.blob {
                        self.trie_node_cache.insert(key.clone(), Some(blob.clone()));
                        batch.put_cf(&default_cf, key, blob);
                    }
                }

                for (key, value) in difflayer.diff_storage_roots.iter() {
                    self.storage_root_cache.insert(key.as_slice().to_vec(), Some(value.as_slice().to_vec()));
                    batch.put_cf(&storage_root_cf, key.as_slice(), value.as_slice());
                }
            }
        }

        // Write batch and update caches after successful write - mini_moka is thread-safe
        match self.db.write_opt(batch, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::batch", "Successfully committed batch to database, block_number: {}, state_root: {:?}, diff_nodes_len: {}, diff_storage_roots_len: {}", block_number, state_root, diff_nodes_len, diff_storage_roots_len);

                // Pin the committed diff layer for fast subsequent reads
                if let Some(difflayer) = difflayer {
                    if difflayer.is_empty() {
                        return Ok(());
                    }
                    let max = self.config.max_committed_difflayers;
                    if max > 0 {
                        let mut layers = self.committed_difflayers.write();
                        layers.push_front(difflayer.clone());
                        while layers.len() > max { layers.pop_back(); }
                    }
                }

                Ok(())
            }
            Err(e) => {
                error!(target: "pathdb::batch", "Error committing batch: block_number: {}, state_root: {:?}, error: {}", block_number, state_root, e);
                Err(PathProviderError::Database(format!("Batch commit error: {}", e)))
            }

        }
    }
}


/// Ensure all required Column Families exist in the database.
/// Creates missing Column Families if they don't exist.
///
/// # Arguments
/// * `path` - Path to the RocksDB database
/// * `db_opts` - Database options
/// * `config` - Path provider configuration
///
/// # Returns
/// * `Ok(())` if all Column Families exist or were successfully created
/// * `Err(PathProviderError)` if there was an error creating Column Families
fn ensure_column_families(
    path: &str,
    db_opts: &Options,
    config: &PathProviderConfig,
    block_based: &BlockBasedOptions,
) -> PathProviderResult<()> {
    // List existing Column Families in the database
    let existing_cfs = DB::list_cf(db_opts, path)
        .unwrap_or_else(|_| vec!["default".to_string()]);
    let existing_cfs_set: HashSet<String> = existing_cfs.iter().cloned().collect();

    // Find missing Column Families
    let missing_cfs: Vec<&str> = COLUMN_FAMILY_NAMES
        .iter()
        .filter(|&&cf_name| !existing_cfs_set.contains(cf_name))
        .copied()
        .collect();

    // If no missing CFs, we're done
    if missing_cfs.is_empty() {
        trace!(
            target: "pathdb::rocksdb",
            "All required Column Families already exist"
        );
        return Ok(());
    }

    trace!(
        target: "pathdb::rocksdb",
        "Found {} missing Column Families: {:?}",
        missing_cfs.len(),
        missing_cfs
    );

    // Open database with existing CFs first
    let mut existing_cf_descriptors = Vec::new();
    for cf_name in &existing_cfs {
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
        cf_opts.set_write_buffer_size(config.write_buffer_size);
        cf_opts.set_block_based_table_factory(block_based);
        // Disable auto compaction during startup
        cf_opts.set_disable_auto_compactions(true);
        existing_cf_descriptors.push(ColumnFamilyDescriptor::new(cf_name, cf_opts));
    }

    let temp_db = DB::open_cf_descriptors(db_opts, path, existing_cf_descriptors)
        .map_err(|e| PathProviderError::Database(format!("Failed to open RocksDB: {}", e)))?;

    // Create missing Column Families
    for cf_name in missing_cfs {
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
        cf_opts.set_write_buffer_size(config.write_buffer_size);
        cf_opts.set_block_based_table_factory(block_based);
        temp_db.create_cf(cf_name, &cf_opts).map_err(|e| {
            PathProviderError::Database(format!(
                "Failed to create Column Family '{}': {}",
                cf_name, e
            ))
        })?;
        trace!(
            target: "pathdb::rocksdb",
            "Created Column Family '{}'",
            cf_name
        );
    }
    // Drop temp_db to close it before reopening with all CFs
    drop(temp_db);

    Ok(())
}

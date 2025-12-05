//! PathDB implementation for RocksDB integration.

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use rocksdb::{ColumnFamilyDescriptor,DB, Options, ReadOptions, WriteBatch, WriteOptions};
// use schnellru::{ByLength, LruMap};
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

/// Metrics for the `PathDB`.
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
    // /// LRU cache for key-value pairs.
    // pub trie_node_cache: Arc<Mutex<LruMap<Vec<u8>, Option<Vec<u8>>, ByLength>>>,
    // /// LRU cache for storage root key-value pairs.
    // pub storage_root_cache: Arc<Mutex<LruMap<Vec<u8>, Option<Vec<u8>>, ByLength>>>,
    // /// Metrics for the PathDB.
    // metrics: PathDBMetrics,
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
            // trie_node_cache: self.trie_node_cache.clone(),
            // storage_root_cache: self.storage_root_cache.clone(),
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

        // Ensure all required Column Families exist
        ensure_column_families(path, &db_opts, &config)?;

        // Now open database with all required Column Families
        let mut cf_descriptors = Vec::new();
        for cf_name in COLUMN_FAMILY_NAMES {
            let mut cf_opts = Options::default();
            cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
            cf_opts.set_write_buffer_size(config.write_buffer_size);
            cf_descriptors.push(ColumnFamilyDescriptor::new(cf_name, cf_opts));
        }

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .map_err(|e| PathProviderError::Database(format!("Failed to open RocksDB: {}", e)))?;

        let cf_names_set: HashSet<String> = COLUMN_FAMILY_NAMES.iter().map(|s| s.to_string()).collect();

        let write_options = WriteOptions::default();

        let mut read_options = ReadOptions::default();
        read_options.fill_cache(config.fill_cache);
        read_options.set_readahead_size(config.readahead_size);
        read_options.set_async_io(config.async_io);
        read_options.set_verify_checksums(config.verify_checksums);

        // let trie_node_cache_size = config.trie_node_cache_size;
        // let storage_root_cache_size = config.storage_root_cache_size;

        Ok(Self {
            db: Arc::new(db),
            column_family_names: Arc::new(Mutex::new(cf_names_set)),
            config,
            write_options,
            read_options,
            // trie_node_cache: Arc::new(Mutex::new(LruMap::new(ByLength::new(trie_node_cache_size)))),
            // storage_root_cache: Arc::new(Mutex::new(LruMap::new(ByLength::new(storage_root_cache_size)))),
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

    /// Clear the LRU cache.
    pub fn clear_cache(&self) {
        warn!(target: "pathdb::rocksdb", "Clearing LRU cache");
        // self.trie_node_cache.lock().unwrap().clear();
        // self.storage_root_cache.lock().unwrap().clear();
    }

    // /// Get cache statistics.
    // pub fn cache_stats(&self) -> (usize, usize) {
    //     let trie_node_cache = self.trie_node_cache.lock().unwrap();
    //     let storage_root_cache = self.storage_root_cache.lock().unwrap();

    //     (trie_node_cache.len(), storage_root_cache.len())
    // }

    // /// Create a new metrics instance for the PathDB.
    // pub fn with_new_metrics(&mut self, instance_name: &str) {
    //     self.metrics = PathDBMetrics::new_with_labels(&[("instance", instance_name.to_string())]);
    // }
}

impl PathDB {
    pub fn get_raw_trie_node(&self, key: &[u8]) -> PathProviderResult<Option<Vec<u8>>> {
        trace!(target: "pathdb::rocksdb", "Getting key: {:?}", key);

        // Check cache first
        // {
        //     let cache = self.trie_node_cache.lock().unwrap();
        //     if let Some(cached_value) = cache.peek(key) {
        //         self.metrics.trie_node_cache_hits.increment(1);
        //         trace!(target: "pathdb::rocksdb", "Found value in cache for key: {:?}", key);
        //         return Ok(cached_value.clone());
        //     } else {
        //         self.metrics.trie_node_cache_misses.increment(1);
        //     }
        // }

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;
        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();

        // Cache miss, read from DB
        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(value)) => {
                trace!(target: "pathdb::rocksdb", "Found value in CF '{}' for key: 0x{}", DEFAULT_COLUMN_FAMILY_NAME, key_hex);
                // self.trie_node_cache.lock().unwrap().insert(key.to_vec(), Some(value.to_vec()));
                Ok(Some(value))
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key not found in CF '{}': 0x{}", DEFAULT_COLUMN_FAMILY_NAME, key_hex);
                Ok(None)
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error getting in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB get in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn put_raw_trie_node(&self, key: &[u8], value: &[u8]) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Putting key: {:?}, value_len: {}", key, value.len());

        // Update cache first
        // self.trie_node_cache.lock().unwrap().insert(key.to_vec(), Some(value.to_vec()));

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();

        // Then write to DB
        match self.db.put_cf_opt(&cf, key, value, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully put in CF '{}' for key 0x{}", DEFAULT_COLUMN_FAMILY_NAME, key_hex);
                Ok(())
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error putting in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                // self.trie_node_cache.lock().unwrap().remove(key);
                Err(PathProviderError::Database(format!("RocksDB put in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn delete_raw_trie_node(&self, key: &[u8]) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Deleting key: {:?}", key);

        // Remove from cache first
        // self.trie_node_cache.lock().unwrap().remove(key);

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();

        // Then delete from DB
        match self.db.delete_cf_opt(&cf, key, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully deleted in CF '{}' for key 0x{}", DEFAULT_COLUMN_FAMILY_NAME, key_hex);
                Ok(())
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error deleting in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB delete in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn exists_raw_trie_node(&self, key: &[u8]) -> PathProviderResult<bool> {
        trace!(target: "pathdb::rocksdb", "Checking existence of key: {:?}", key);

        // Check cache first
        // {
        //     let cache = self.trie_node_cache.lock().unwrap();
        //     if let Some(cached_value) = cache.peek(key) {
        //         trace!(target: "pathdb::rocksdb", "Key exists in cache: {:?}", key);
        //         self.metrics.trie_node_cache_hits.increment(1);
        //         return Ok(cached_value.is_some());
        //     } else {
        //         self.metrics.trie_node_cache_misses.increment(1);
        //     }
        // }

        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;
            
        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();

        // Cache miss, check DB
        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(_)) => {
                trace!(target: "pathdb::rocksdb", "Key exists in CF '{}' for key 0x{}", DEFAULT_COLUMN_FAMILY_NAME, key_hex);
                // self.trie_node_cache.lock().unwrap().insert(key.to_vec(), Some(vec![]));
                Ok(true)
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key does not exist in CF '{}' for key 0x{}", DEFAULT_COLUMN_FAMILY_NAME, key_hex);
                Ok(false)
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error checking existence of key in CF '{}' for key 0x{}: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB exists in CF '{}' for key 0x{} error: {}", DEFAULT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn get_raw_storage_root(&self, key: &[u8]) -> PathProviderResult<Option<Vec<u8>>> {
        trace!(target: "pathdb::rocksdb", "Getting key: {:?}", key);

        // Check cache first
        // {
        //     let cache = self.storage_root_cache.lock().unwrap();
        //     if let Some(cached_value) = cache.peek(key) {
        //         self.metrics.storage_root_cache_hits.increment(1);
        //         trace!(target: "pathdb::rocksdb", "Found value in cache for key: {:?}", key);
        //         return Ok(cached_value.clone());
        //     } else {
        //         self.metrics.storage_root_cache_misses.increment(1);
        //     }
        // }

        let cf = self.db.cf_handle(STORAGE_ROOT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", STORAGE_ROOT_COLUMN_FAMILY_NAME))
        })?;

        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();

        // Cache miss, read from DB
        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(value)) => {
                trace!(target: "pathdb::rocksdb", "Found value in CF '{}' for key 0x{}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex);
                // self.storage_root_cache.lock().unwrap().insert(key.to_vec(), Some(value.to_vec()));
                Ok(Some(value))
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key not found in CF '{}' for key 0x{}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex);
                Ok(None)
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error getting in CF '{}' for key 0x{}: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB get in CF '{}' for key 0x{} error: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn put_raw_storage_root(&self, key: &[u8], value: &[u8]) -> PathProviderResult<()> {
        trace!(target: "pathdb::rocksdb", "Putting storage root key: {:?}, value_len: {}", key, value.len());

        // {
        //     // Update cache first
        //     let mut cache = self.storage_root_cache.lock().unwrap();
        //     cache.insert(key.to_vec(), Some(value.to_vec()));
        // }
        

        let cf = self.db.cf_handle(STORAGE_ROOT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", STORAGE_ROOT_COLUMN_FAMILY_NAME))
        })?;

        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();

        match self.db.put_cf_opt(&cf, key, value, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::rocksdb", "Successfully put value in CF '{}' for key 0x{}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex);
                Ok(())
            }
            Err(e) => {
                error!(target: "pathdb::rocksdb", "Error putting in CF '{}' for key 0x{}: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e);
                Err(PathProviderError::Database(format!("RocksDB put in CF '{}' for key 0x{} error: {}", STORAGE_ROOT_COLUMN_FAMILY_NAME, key_hex, e)))
            }
        }
    }

    pub fn get_raw_meta_data(&self, key: &[u8]) -> PathProviderResult<Option<Vec<u8>>> {
        // Check cache first
        // {
        //     let cache = self.trie_node_cache.lock().unwrap();
        //     if let Some(cached_value) = cache.peek(key) {
        //         trace!(target: "pathdb::rocksdb", "Found value in cache for key: {:?}", key);
        //         return Ok(cached_value.clone());
        //     }
        // }

        // TODO:: change to META_COLUMN_FAMILY_NAME from default CF in the future.
        let cf = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME).ok_or_else(|| {
            PathProviderError::Database(format!("Column Family '{}' handle not found", DEFAULT_COLUMN_FAMILY_NAME))
        })?;

        // Convert key to readable string: try UTF-8 first, fallback to hex if invalid
        let key_string = String::from_utf8_lossy(key).to_string();
        
        match self.db.get_cf_opt(&cf, key, &self.read_options) {
            Ok(Some(value)) => {
                trace!(target: "pathdb::rocksdb", "Found value in CF '{}' for key: {}", DEFAULT_COLUMN_FAMILY_NAME, key_string);
                // self.trie_node_cache.lock().unwrap().insert(key.to_vec(), Some(value.clone()));
                Ok(Some(value))
            }
            Ok(None) => {
                trace!(target: "pathdb::rocksdb", "Key not found in CF '{}' for key: {}", DEFAULT_COLUMN_FAMILY_NAME, key_string);
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

        // Simplified compact implementation
        Ok(())
    }
}

impl TrieDatabase for PathDB {
    type Error = PathProviderError;

    fn get_trie_node(&self, path: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_raw_trie_node(path)
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
            // let mut trie_node_cache = self.trie_node_cache.lock().unwrap();
            // let mut storage_root_cache = self.storage_root_cache.lock().unwrap();

            batch.put_cf(&default_cf, TRIE_STATE_ROOT_KEY, state_root.as_slice());
            batch.put_cf(&default_cf, TRIE_STATE_BLOCK_NUMBER_KEY, &block_number.to_le_bytes());

            // TODO:: double Write to meta CF using put_cf, will be delete default CF in the future.
            batch.put_cf(&meta_cf, TRIE_STATE_ROOT_KEY, state_root.as_slice());
            batch.put_cf(&meta_cf, TRIE_STATE_BLOCK_NUMBER_KEY, &block_number.to_le_bytes());

            // trie_node_cache.insert(TRIE_STATE_ROOT_KEY.to_vec(), Some(state_root.as_slice().to_vec()));
            // trie_node_cache.insert(TRIE_STATE_BLOCK_NUMBER_KEY.to_vec(), Some(block_number.to_le_bytes().to_vec()));
        
            if let Some(difflayer) = difflayer {
                diff_nodes_len = difflayer.diff_nodes.len();
                diff_storage_roots_len = difflayer.diff_storage_roots.len();

                for (key, node) in difflayer.diff_nodes.iter() {
                    if node.is_deleted() {
                        // trie_node_cache.remove(key);
                        batch.delete_cf(&default_cf, key);
                        
                    } else {
                        if let Some(blob) = &node.blob {
                            // trie_node_cache.insert(key.clone(), Some(blob.clone()));
                            batch.put_cf(&default_cf, key, blob);
                        }
                    }
                }

                for (key, value) in difflayer.diff_storage_roots.iter() {
                    // storage_root_cache.insert(key.as_slice().to_vec(), Some(value.as_slice().to_vec()));
                    batch.put_cf(&storage_root_cf, key.as_slice(), value.as_slice());
                }
            }
        }

        match self.db.write_opt(batch, &self.write_options) {
            Ok(()) => {
                trace!(target: "pathdb::batch", "Successfully committed batch to database, block_number: {}, state_root: {:?}, diff_nodes_len: {}, diff_storage_roots_len: {}", block_number, state_root, diff_nodes_len, diff_storage_roots_len);
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
        existing_cf_descriptors.push(ColumnFamilyDescriptor::new(cf_name, cf_opts));
    }

    let temp_db = DB::open_cf_descriptors(db_opts, path, existing_cf_descriptors)
        .map_err(|e| PathProviderError::Database(format!("Failed to open RocksDB: {}", e)))?;

    // Create missing Column Families
    for cf_name in missing_cfs {
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
        cf_opts.set_write_buffer_size(config.write_buffer_size);
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


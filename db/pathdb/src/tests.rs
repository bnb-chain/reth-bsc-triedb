//! Tests for PathDB implementation.

use tempfile::TempDir;
use crate::{PathDB, PathProviderConfig};
use rust_eth_triedb_common::TrieDatabase;

#[test]
fn test_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    // Test put and get
    let key = b"test_key";
    let value = b"test_value";
    db.put_raw_trie_node(key, value).unwrap();
    
    let retrieved = db.get_raw_trie_node(key).unwrap();
    assert_eq!(retrieved, Some(value.to_vec()));

    // Test exists
    assert!(db.exists_raw_trie_node(key).unwrap());
    assert!(!db.exists_raw_trie_node(b"non_existent_key").unwrap());

    // Test delete
    db.delete_raw_trie_node(key).unwrap();
    assert_eq!(db.get_raw_trie_node(key).unwrap(), None);
    assert!(!db.exists_raw_trie_node(key).unwrap());
}

#[test]
fn test_cache_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    // Test cache operations
    let key = b"cache_test_key";
    let value = b"cache_test_value";
    
    // Write to database
    db.put_raw_trie_node(key, value).unwrap();

    // Read from database - this will populate the cache
    let retrieved = db.get_raw_trie_node(key).unwrap();
    assert_eq!(retrieved, Some(value.to_vec()));
    
    // Get cache stats - should have entries after read
    let (cache_len, _) = db.cache_stats();
    assert!(cache_len > 0, "Cache should have entries after read");
    
    db.clear_cache();
    // After clear, the previously cached key should no longer be present.
    assert!(db.trie_node_cache.get(&key.to_vec()).is_none());
}

#[test]
fn test_configuration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    
    let mut config = PathProviderConfig::default();
    config.trie_node_cache_size = 1000;
    config.fill_cache = false;
    config.readahead_size = 256 * 1024; // 256KB
    config.async_io = false;
    config.verify_checksums = true;
    
    let db = PathDB::new(db_path.to_str().unwrap(), config.clone()).unwrap();
    
    let retrieved_config = db.config();
    assert_eq!(retrieved_config.trie_node_cache_size, 1000);
    assert_eq!(retrieved_config.fill_cache, false);
    assert_eq!(retrieved_config.readahead_size, 256 * 1024);
    assert_eq!(retrieved_config.async_io, false);
    assert_eq!(retrieved_config.verify_checksums, true);
}

#[test]
fn test_error_handling() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    // Test get non-existent key
    let result = db.get_trie_node(b"non_existent");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);

    // Test exists non-existent key
    let result = db.exists_raw_trie_node(b"non_existent");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), false);
}

#[test]
fn test_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = Arc::new(PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap());

    let db_clone = db.clone();
    let handle = thread::spawn(move || {
        for i in 0..100 {
            let key = format!("thread_key_{}", i).into_bytes();
            let value = format!("thread_value_{}", i).into_bytes();
            db_clone.put_raw_trie_node(&key, &value).unwrap();
        }
    });

    handle.join().unwrap();

    // Verify all values were written
    for i in 0..100 {
        let key = format!("thread_key_{}", i).into_bytes();
        let expected_value = format!("thread_value_{}", i).into_bytes();
        let retrieved = db.get_raw_trie_node(&key).unwrap();
        assert_eq!(retrieved, Some(expected_value));
    }
}

#[test]
fn test_default_wal_retention_is_bounded() {
    // Regression test for bnb-chain/reth-bsc#322: the default config MUST bound
    // WAL and info-log retention. If any of these are 0 (RocksDB C++ default),
    // the data directory grows ~10 GB/hour on a live node until restart.
    let config = PathProviderConfig::default();
    assert!(
        config.max_total_wal_size_bytes > 0,
        "max_total_wal_size_bytes must be bounded — 0 means unbounded WAL (issue #322)"
    );
    assert!(
        config.wal_size_limit_mb > 0,
        "wal_size_limit_mb must be bounded — 0 disables WAL file purge (issue #322)"
    );
    assert!(
        config.keep_log_file_num >= 1,
        "keep_log_file_num must retain at least one info LOG file"
    );
    assert!(
        config.delete_obsolete_files_period_micros > 0,
        "delete_obsolete_files_period_micros must be set so obsolete SSTs are swept"
    );
}

#[test]
fn test_db_opens_with_wal_retention_config() {
    // Sanity-check that the new WAL/log retention options are accepted by
    // RocksDB and a DB can be opened, written to, and read back. This catches
    // API regressions if rocksdb-0.x ever renames/removes the setters.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let config = PathProviderConfig {
        max_total_wal_size_bytes: 64 * 1024 * 1024, // 64 MiB, small so WAL flush kicks in during the test
        wal_size_limit_mb: 64,
        keep_log_file_num: 3,
        delete_obsolete_files_period_micros: 60 * 1_000_000, // 60 s
        ..PathProviderConfig::default()
    };

    let db = PathDB::new(db_path.to_str().unwrap(), config).unwrap();

    // Write enough data to force at least one memtable rotation + WAL roll.
    // Use a 1 KB value so a few thousand puts exceeds the 256 MB write buffer
    // only after many iterations; the point here is just that the DB is
    // operational with the new options, not to measure WAL size.
    let value = vec![0xABu8; 1024];
    for i in 0..1_000u32 {
        let key = format!("wal_test_key_{i:08}").into_bytes();
        db.put_raw_trie_node(&key, &value).unwrap();
    }
    // Confirm reads still work.
    let retrieved = db.get_raw_trie_node(b"wal_test_key_00000000").unwrap();
    assert_eq!(retrieved.as_deref(), Some(value.as_slice()));
}

#[test]
fn test_env_overrides_for_wal_retention() {
    // Overriding via env vars should flow through to the config. We only
    // check the parse path here; actually mutating process env across tests
    // is racy, so each var gets a unique name check in isolation.
    //
    // Env var names are unique to this test so parallel tests in other files
    // won't race on them. We still clean up at the end to avoid leaking into
    // other tests that might call `apply_env_overrides`.
    std::env::set_var("RETHBSC_ROCKSDB_MAX_TOTAL_WAL_MB", "512");
    std::env::set_var("RETHBSC_ROCKSDB_WAL_SIZE_LIMIT_MB", "256");
    std::env::set_var("RETHBSC_ROCKSDB_KEEP_LOG_FILE_NUM", "5");
    std::env::set_var("RETHBSC_ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_SECS", "120");

    let config = PathProviderConfig::default().apply_env_overrides();

    assert_eq!(config.max_total_wal_size_bytes, 512 * 1024 * 1024);
    assert_eq!(config.wal_size_limit_mb, 256);
    assert_eq!(config.keep_log_file_num, 5);
    assert_eq!(config.delete_obsolete_files_period_micros, 120 * 1_000_000);

    std::env::remove_var("RETHBSC_ROCKSDB_MAX_TOTAL_WAL_MB");
    std::env::remove_var("RETHBSC_ROCKSDB_WAL_SIZE_LIMIT_MB");
    std::env::remove_var("RETHBSC_ROCKSDB_KEEP_LOG_FILE_NUM");
    std::env::remove_var("RETHBSC_ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_SECS");
}
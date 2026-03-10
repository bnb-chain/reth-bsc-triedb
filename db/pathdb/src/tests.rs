//! Tests for PathDB implementation.

use std::{collections::HashMap, sync::Arc};

use alloy_primitives::B256;
use tempfile::TempDir;
use crate::{PathDB, PathProviderConfig};
use rust_eth_triedb_common::{DiffLayer, TrieDatabase, TrieNode};

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
    config.trie_node_cache_capacity_bytes = 64 * 1024 * 1024; // 64MB
    config.fill_cache = false;
    config.readahead_size = 256 * 1024; // 256KB
    config.async_io = false;
    config.verify_checksums = true;

    let db = PathDB::new(db_path.to_str().unwrap(), config.clone()).unwrap();

    let retrieved_config = db.config();
    assert_eq!(retrieved_config.trie_node_cache_capacity_bytes, 64 * 1024 * 1024);
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
fn test_commit_difflayer_empty_layer_not_pinned() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    let mut diff_nodes = HashMap::new();
    diff_nodes.insert(vec![0x01], Arc::new(TrieNode::new(None, Some(vec![0xAA]))));
    let non_empty_difflayer = Arc::new(DiffLayer::new(
        Arc::new(diff_nodes),
        Arc::new(HashMap::new()),
    ));
    db.commit_difflayer(1, B256::ZERO, &Some(non_empty_difflayer)).unwrap();
    assert_eq!(db.committed_difflayers_depth(), 1);

    let empty_difflayer = Arc::new(DiffLayer::default());
    db.commit_difflayer(2, B256::ZERO, &Some(empty_difflayer)).unwrap();
    assert_eq!(db.committed_difflayers_depth(), 1);
}

#[test]
fn test_committed_difflayers_disabled_skips_pinning() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let mut config = PathProviderConfig::default();
    config.max_committed_difflayers = 0;
    let db = PathDB::new(db_path.to_str().unwrap(), config).unwrap();

    let mut diff_nodes = HashMap::new();
    diff_nodes.insert(vec![0x01], Arc::new(TrieNode::new(None, Some(vec![0xAA]))));
    let non_empty_difflayer = Arc::new(DiffLayer::new(
        Arc::new(diff_nodes),
        Arc::new(HashMap::new()),
    ));
    db.commit_difflayer(1, B256::ZERO, &Some(non_empty_difflayer)).unwrap();
    assert_eq!(db.committed_difflayers_depth(), 0);
}

#[test]
fn test_commit_difflayers_batches_range() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    let mut first_nodes = HashMap::new();
    first_nodes.insert(vec![0x01], Arc::new(TrieNode::new(None, Some(vec![0xAA]))));
    let mut first_storage_roots = HashMap::new();
    first_storage_roots.insert(B256::from([0x11; 32]), B256::from([0x22; 32]));

    let mut second_nodes = HashMap::new();
    second_nodes.insert(vec![0x02], Arc::new(TrieNode::new(None, Some(vec![0xBB]))));
    let mut second_storage_roots = HashMap::new();
    second_storage_roots.insert(B256::from([0x33; 32]), B256::from([0x44; 32]));

    let commits = vec![
        (
            1,
            B256::from([0x01; 32]),
            Some(Arc::new(DiffLayer::new(Arc::new(first_nodes), Arc::new(first_storage_roots)))),
        ),
        (
            2,
            B256::from([0x02; 32]),
            Some(Arc::new(DiffLayer::new(Arc::new(second_nodes), Arc::new(second_storage_roots)))),
        ),
    ];

    db.commit_difflayers(&commits).unwrap();

    assert_eq!(db.latest_persist_state().unwrap(), (2, B256::from([0x02; 32])));
    assert_eq!(db.get_raw_trie_node(&[0x01]).unwrap(), Some(vec![0xAA]));
    assert_eq!(db.get_raw_trie_node(&[0x02]).unwrap(), Some(vec![0xBB]));
    assert_eq!(
        db.get_storage_root(B256::from([0x11; 32])).unwrap(),
        Some(B256::from([0x22; 32]))
    );
    assert_eq!(
        db.get_storage_root(B256::from([0x33; 32])).unwrap(),
        Some(B256::from([0x44; 32]))
    );
    assert_eq!(db.committed_difflayers_depth(), 2);
}

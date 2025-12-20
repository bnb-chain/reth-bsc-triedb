//! Tests for PathDB implementation.

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use alloy_primitives::keccak256;
use crate::{PathDB, PathProviderConfig};
use rust_eth_triedb_common::{TrieDatabase, DiffLayer, TrieNode};
use rust_eth_triedb_common::node::Node;
use rust_eth_triedb_common::encoding::{account_trie_node_key, storage_trie_node_key};

#[test]
fn test_commit_difflayer_and_read() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    // Initialize empty root node
    rust_eth_triedb_common::node::init_empty_root_node();

    // Prepare test data
    let block_number = 100u64;
    let state_root = keccak256(b"test_state_root");
    
    // Create test trie nodes
    let mut diff_nodes = HashMap::new();
    let mut diff_storage_roots = HashMap::new();

    // Create a test node (FullNode with some data)
    let mut full_node = rust_eth_triedb_common::node::FullNode::new();
    let test_value = vec![0x01, 0x02, 0x03, 0x04];
    full_node.set_child(16, &Node::Value(test_value.clone()));
    
    let node_arc = Arc::new(Node::Full(Arc::new(full_node)));
    let node_blob = Node::node_to_bytes(node_arc.clone());
    let node_hash = keccak256(&node_blob);
    
    // Create TrieNode with hash and blob
    let rlp_node = Arc::new(TrieNode::new(Some(node_hash), Some(node_blob.clone())));
    
    // Add node to diff_nodes with account trie node key
    let node_path = b"test_node_path";
    let account_key = account_trie_node_key(node_path);
    diff_nodes.insert(account_key.clone(), (rlp_node.clone(), Some(node_arc.clone())));

    // Create storage root data
    let test_address_hash = keccak256(b"test_account_address");
    let test_storage_root = keccak256(b"test_storage_root");
    diff_storage_roots.insert(test_address_hash, test_storage_root);

    // Create DiffLayer
    let difflayer = Arc::new(DiffLayer::new(
        Arc::new(diff_nodes.clone()),
        Arc::new(diff_storage_roots.clone()),
    ));

    // Commit difflayer
    db.commit_difflayer(block_number, state_root, &Some(difflayer)).unwrap();

    // Verify using latest_persist_state
    let (read_block_number, read_state_root) = db.latest_persist_state().unwrap();
    assert_eq!(read_block_number, block_number);
    assert_eq!(read_state_root, state_root);

    // Verify using get_trie_node
    let read_node = db.get_trie_node(&account_key).unwrap();
    assert!(read_node.is_some());
    let read_node_arc = read_node.unwrap();
    
    // Verify the node structure matches
    match read_node_arc.as_ref() {
        Node::Full(full) => {
            match full.get_child(16).as_ref() {
                Node::Value(value) => {
                    assert_eq!(value, &test_value);
                }
                _ => panic!("Expected Value node at index 16"),
            }
        }
        _ => panic!("Expected Full node"),
    }

    // Verify using get_storage_root
    let read_storage_root = db.get_storage_root(test_address_hash).unwrap();
    assert_eq!(read_storage_root, Some(test_storage_root));
}

#[test]
fn test_commit_difflayer_with_multiple_nodes_and_storage_roots() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    // Initialize empty root node
    rust_eth_triedb_common::node::init_empty_root_node();

    // Prepare test data
    let block_number = 200u64;
    let state_root = keccak256(b"test_state_root_2");
    
    let mut diff_nodes = HashMap::new();
    let mut diff_storage_roots = HashMap::new();

    // Create multiple test nodes
    for i in 0..5 {
        let mut full_node = rust_eth_triedb_common::node::FullNode::new();
        let test_value = vec![i, i + 1, i + 2];
        full_node.set_child(16, &Node::Value(test_value.clone()));
        
        let node_arc = Arc::new(Node::Full(Arc::new(full_node)));
        let node_blob = Node::node_to_bytes(node_arc.clone());
        let node_hash = keccak256(&node_blob);
        
        let rlp_node = Arc::new(TrieNode::new(Some(node_hash), Some(node_blob)));
        
        let node_path = format!("test_node_path_{}", i).into_bytes();
        let account_key = account_trie_node_key(&node_path);
        diff_nodes.insert(account_key, (rlp_node, Some(node_arc)));
    }

    // Create multiple storage roots
    for i in 0..3 {
        let address_hash = keccak256(format!("test_address_{}", i).as_bytes());
        let storage_root = keccak256(format!("test_storage_{}", i).as_bytes());
        diff_storage_roots.insert(address_hash, storage_root);
    }

    // Create DiffLayer
    let difflayer = Arc::new(DiffLayer::new(
        Arc::new(diff_nodes.clone()),
        Arc::new(diff_storage_roots.clone()),
    ));

    // Commit difflayer
    db.commit_difflayer(block_number, state_root, &Some(difflayer)).unwrap();

    // Verify latest_persist_state
    let (read_block_number, read_state_root) = db.latest_persist_state().unwrap();
    assert_eq!(read_block_number, block_number);
    assert_eq!(read_state_root, state_root);

    // Verify all nodes can be read
    for i in 0..5 {
        let node_path = format!("test_node_path_{}", i).into_bytes();
        let account_key = account_trie_node_key(&node_path);
        let read_node = db.get_trie_node(&account_key).unwrap();
        assert!(read_node.is_some(), "Node {} should exist", i);
    }

    // Verify all storage roots can be read
    for i in 0..3 {
        let address_hash = keccak256(format!("test_address_{}", i).as_bytes());
        let expected_storage_root = keccak256(format!("test_storage_{}", i).as_bytes());
        let read_storage_root = db.get_storage_root(address_hash).unwrap();
        assert_eq!(read_storage_root, Some(expected_storage_root), "Storage root {} should match", i);
    }
}

#[test]
fn test_commit_difflayer_with_storage_trie_nodes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    let db = PathDB::new(db_path.to_str().unwrap(), PathProviderConfig::default()).unwrap();

    // Initialize empty root node
    rust_eth_triedb_common::node::init_empty_root_node();

    // Prepare test data
    let block_number = 300u64;
    let state_root = keccak256(b"test_state_root_3");
    
    let mut diff_nodes = HashMap::new();
    let mut diff_storage_roots = HashMap::new();

    // Create a storage trie node (for a specific account)
    let account_hash = keccak256(b"test_account");
    let storage_path = b"storage_path_1";
    let storage_key = storage_trie_node_key(account_hash.as_slice(), storage_path);
    
    let mut full_node = rust_eth_triedb_common::node::FullNode::new();
    let storage_value = vec![0xAA, 0xBB, 0xCC];
    full_node.set_child(16, &Node::Value(storage_value.clone()));
    
    let node_arc = Arc::new(Node::Full(Arc::new(full_node)));
    let node_blob = Node::node_to_bytes(node_arc.clone());
    let node_hash = keccak256(&node_blob);
    
    let rlp_node = Arc::new(TrieNode::new(Some(node_hash), Some(node_blob)));
    diff_nodes.insert(storage_key.clone(), (rlp_node, Some(node_arc.clone())));

    // Create storage root for the account
    let storage_root = keccak256(b"test_storage_root_for_account");
    diff_storage_roots.insert(account_hash, storage_root);

    // Create DiffLayer
    let difflayer = Arc::new(DiffLayer::new(
        Arc::new(diff_nodes.clone()),
        Arc::new(diff_storage_roots.clone()),
    ));

    // Commit difflayer
    db.commit_difflayer(block_number, state_root, &Some(difflayer)).unwrap();

    // Verify latest_persist_state
    let (read_block_number, read_state_root) = db.latest_persist_state().unwrap();
    assert_eq!(read_block_number, block_number);
    assert_eq!(read_state_root, state_root);

    // Verify storage trie node can be read
    let read_node = db.get_trie_node(&storage_key).unwrap();
    assert!(read_node.is_some());
    let read_node_arc = read_node.unwrap();
    
    match read_node_arc.as_ref() {
        Node::Full(full) => {
            match full.get_child(16).as_ref() {
                Node::Value(value) => {
                    assert_eq!(value, &storage_value);
                }
                _ => panic!("Expected Value node at index 16"),
            }
        }
        _ => panic!("Expected Full node"),
    }

    // Verify storage root can be read
    let read_storage_root = db.get_storage_root(account_hash).unwrap();
    assert_eq!(read_storage_root, Some(storage_root));
}

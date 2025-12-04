use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::str::FromStr;
use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_trie::{EMPTY_ROOT_HASH};
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::node::{MergedNodeSet, DiffLayer, DiffLayers, init_empty_root_node};
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig};
use crate::{TrieDB, TrieDBError};
use tempfile::TempDir;
use once_cell::sync::Lazy;
use serial_test::serial;

/// Test basic TrieDB functionality
#[test]
#[serial]
fn test_triedb_update_all_operations_without_difflayer() {
    init_empty_root_node();

    // Create temporary directories for databases
    let path_db_temp_dir = TempDir::new().expect("Failed to create temp directory for PathDB");
    let path_db_path = path_db_temp_dir.path().to_str().unwrap();
    
    // Create path database and TrieDB instance
    let path_db = PathDB::new(path_db_path, PathProviderConfig::default()).expect("Failed to create PathDB");
    let mut triedb = TrieDB::new(path_db);
    
    println!("=== Starting TrieDB Test ===");
    
    // Test 1: Call update_all interface
    let result_one = test_update_all_initial(&mut triedb);

    // Test 2: Update operations based on update_all results
    test_update_all_modifications(result_one.unwrap().0, None, &mut triedb);
    
    println!("=== TrieDB Test Completed ===");
}

#[test]
#[serial]
fn test_triedb_update_all_operations_with_difflayer() {
    init_empty_root_node();
    // Create temporary directories for databases
    let path_db_temp_dir = TempDir::new().expect("Failed to create temp directory for PathDB");
    let path_db_path = path_db_temp_dir.path().to_str().unwrap();
    
    // Create path database and TrieDB instance
    let path_db = PathDB::new(path_db_path, PathProviderConfig::default()).expect("Failed to create PathDB");
    let mut triedb = TrieDB::new(path_db);
    
    println!("=== Starting TrieDB Test With Difflayer===");
    
    // Test 1: Call update_all interface
    let result_one = test_update_all_initial(&mut triedb);

    // Test 2: Update operations based on update_all results
    let (root_hash, difflayer) = result_one.unwrap();
    test_update_all_modifications(root_hash, difflayer, &mut triedb);
    
    println!("=== TrieDB Test Completed With Difflayer===");
}

/// Test initial update_all operation
fn test_update_all_initial(triedb: &mut TrieDB<PathDB>) -> Result<(B256, Option<Arc<MergedNodeSet>>), TrieDBError>{
    println!("\n--- Test 1: Initial update_all operation ---");
    
    // Construct 100 accounts with addresses 1-100
    let mut states = HashMap::new();
    let mut storage_states = HashMap::new();
    
    // Select 5 addresses for storage_states
    let storage_addresses = vec![1, 2, 3, 4, 5];
    
    for i in 1..=100 {
        let address = Address::from_slice(&[i as u8; 20]);
        let hashed_address = keccak256(address.as_slice());
        
        // Create default StateAccount
        let account = StateAccount::default();
        states.insert(hashed_address, Some(account));
        
        // Construct storage_states for the selected 5 addresses
        if storage_addresses.contains(&i) {
            let mut storage_kvs = HashMap::new();
            
            // Insert 10 kv pairs, k == v, k is hash of 1-10
            for j in 1..=10 {
                let key = keccak256(&[j as u8]);
                let value = vec![j as u8; 32]; // 32-byte value
                storage_kvs.insert(key, Some(U256::from_be_slice(&value)));
            }
            
            storage_states.insert(hashed_address, storage_kvs);
        }
    }
    
    println!("Constructed {} accounts", states.len());
    println!("Constructed {} storage states", storage_states.len());
    
    // Call update_all interface
    let result = triedb.finalise(EMPTY_ROOT_HASH, None, states, HashSet::new(), storage_states);
    match &result {
        Ok((root_hash, node_set, diff_storage_roots)) => {    
            // Assert that root_hash matches BSC implementation result
            let expected_hash = B256::from_str("0xadcc848b76bace28ea81dd449a735bad44663a36f18f40980d586d5315eb3800")
                .expect("Failed to parse expected hash");
            assert_eq!(*root_hash, expected_hash, "Root hash should match BSC implementation");
            println!("✅ Root hash assertion passed: matches BSC implementation, root hash: {:?}", root_hash);

            for (owner, nodes) in node_set.sets.iter() {                    
                if let Some(expected_signature) = BSC_SIGNATURES_ONE.get(owner) {
                    assert_eq!(
                        nodes.signature(), 
                        *expected_signature, 
                        "Signature for owner {:?} should match BSC implementation", 
                        owner
                    );
                } else {
                    panic!("⚠️  No BSC signature found for owner {:?}", owner);
                }
            }
            println!("✅ NodeSet signature assertion passed: matches BSC implementation");

            // Call flush and print hash
            let diff_nodes = (*node_set.to_diff_nodes()).clone();
            let difflayer = Arc::new(DiffLayer::new(diff_nodes, diff_storage_roots.clone()));
            let flush_result = triedb.flush(0, B256::ZERO, &Some(difflayer));
            match flush_result {
                Ok(()) => println!("flush executed successfully"),
                Err(e) => println!("flush failed: {:?}", e),
            }
        }
        Err(e) => {
            println!("update_all_one failed: {:?}", e);
        }
    }
    let (root_hash, node_set, _) = result.unwrap();
    Ok((root_hash, Some(node_set)))
}

/// Test modification operations based on update_all results
fn test_update_all_modifications(root_hash: B256, difflayer: Option<Arc<MergedNodeSet>>, triedb: &mut TrieDB<PathDB>) {
    println!("\n--- Test 2: Modification operations ---");
        
    // Construct new states and storage_states
    let mut states = HashMap::new();
    let mut storage_states = HashMap::new();
    
    // 1. Delete the last 10 accounts out of 100
    for i in 91..=100 {
        let address = Address::from_slice(&[i as u8; 20]);
        let hashed_address = keccak256(address.as_slice());
        states.insert(hashed_address, None); // None means delete
    }
    
    // 2. Update 5 storage_states, delete first 5 k values, update the last 5 values
    let storage_addresses = vec![1, 2, 3, 4, 5];
    
    for i in storage_addresses {
        let address = Address::from_slice(&[i as u8; 20]);
        let hashed_address = keccak256(address.as_slice());
        
        let account = StateAccount::default();
        states.insert(hashed_address, Some(account));

        let mut storage_kvs = HashMap::new();
        
        // Delete first 5 k values (1-5)
        for j in 1..=5 {
            let key = keccak256(&[j as u8]);
            storage_kvs.insert(key, None); // None means delete
        }
        
        // Update the last 5 k values (6-10)
        for j in 6..=10 {
            let key = keccak256(&[j as u8]);
            let new_value = vec![(j * 2) as u8; 32]; // New value: j * 2
            storage_kvs.insert(key, Some(U256::from_be_slice(&new_value)));
        }
        
        storage_states.insert(hashed_address, storage_kvs);
    }
    
    println!("Preparing to delete {} accounts", 10);
    println!("Preparing to update {} storage states", storage_states.len());
    
    let difflayers = if let Some(d) = difflayer.as_ref() {
        let diff_nodes = (*d.to_diff_nodes()).clone();
        let mut difflayers = DiffLayers::default();
        difflayers.insert_difflayer(Arc::new(DiffLayer::new(diff_nodes, HashMap::new())));
        Some(difflayers)
    } else {
        None
    };
    // Call update_all interface
    let result = triedb.finalise(root_hash, difflayers.as_ref(), states, HashSet::new(), storage_states);
    
    match result {
        Ok((root_hash, node_set, diff_storage_roots)) => {
            // Assert that the root hash matches the BSC result
            let expected_hash = B256::from_str("0x626ca0a9ca91a1fe5e3a4f438f11015e6e64510b6a29c3a6362d98abad5e4875")
                .expect("Failed to parse expected hash");
            assert_eq!(*root_hash, expected_hash, "Root hash assertion passed: matches BSC implementation");
            println!("✅ Root hash assertion passed: matches BSC implementation, root hash: {:?}", root_hash);
            
            // Assert that the NodeSet signatures match BSC implementation and call flush
            // First, verify signatures
            for (owner, nodes) in node_set.sets.iter() {                    
                if let Some(expected_signature) = BSC_SIGNATURES_TWO.get(owner) {
                    assert_eq!(
                        nodes.signature(), 
                        *expected_signature, 
                        "Signature for owner {:?} should match BSC implementation", 
                        owner
                    );
                } else {
                    panic!("⚠️  No BSC signature found for owner {:?}", owner);
                }
            }
            println!("✅ NodeSet signature assertion passed: matches BSC implementation");
            
            let diff_nodes = (*node_set.to_diff_nodes()).clone();
            let difflayer = Arc::new(DiffLayer::new(diff_nodes, diff_storage_roots));
            // Call flush and print hash
            let flush_result = triedb.flush(0, B256::ZERO, &Some(difflayer));
            match flush_result {
                Ok(()) => println!("Modification flush executed successfully"),
                Err(e) => println!("Modification flush failed: {:?}", e),
            }
        }
        Err(e) => {
            println!("Modification update_all failed: {:?}", e);
        }
    }
}

/// Global BSC signatures hash map for testing
/// Maps owner addresses to their expected signature values
static BSC_SIGNATURES_ONE: Lazy<HashMap<B256, B256>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert(
        B256::from_str("0x685e6e68197229ce85c17dc36118fe13f0bfde48652d7e991793b6710233fe1c")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0xd0ae98bff7b58f014068421e4e51ee4534a8a328f9dde9b053b135a2638feb19")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0xe9654a4d194318e8ef7e64c6cbc31c341c650a6a039ea448faf8101af403da4d")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x69f3330ba3766603e32f4c3fbe0ce6dd33f7d493315935c31a20e4a3d3193fe3")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0xab40727044881a0015f3d04d723757bf0fd40eac11565ede1640f7fd76410e93")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x77c211bee4f6b55f6e5c59c4bfcb315a72852f6c7b0e9572b8e5bf6ee3f33625")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0x92c2f498f37adab9c7a4bf0aae161bb929b33867f5b5976848450005f577b8cb")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x1573cf1c97f9e906504d24410a6439536f109cef9136c312e3d614672a04ac8c")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0x096172dff854a4d9f67fb972ad494924c83beb6624b06ec2b047119c5c20978e")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0xe795383fef0402e55890a95e36ec24c5908e8b041dea294d89a28774b2a9aa5c")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::ZERO, // 0x0000000000000000000000000000000000000000000000000000000000000000
        B256::from_str("0x8d8a3ac91309a1315bc5f01021c44066679d4a7070a39a6db4c09e9dd28ec178")
            .expect("Failed to parse BSC signature")
    );
    map
});

//// Global BSC signatures hash map for testing (second phase)
//// Maps owner addresses to their expected signature values after modifications
static BSC_SIGNATURES_TWO: Lazy<HashMap<B256, B256>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert(
        B256::from_str("0xe9654a4d194318e8ef7e64c6cbc31c341c650a6a039ea448faf8101af403da4d")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0xa88cc2dd758e2d22a983252f13124334c173d7570901c5802ee49b7b831e3911")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0xab40727044881a0015f3d04d723757bf0fd40eac11565ede1640f7fd76410e93")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x2c90170468991f1c11f6a8af4a920b0b8b852d98bf613099f3b84575a8eb65c7")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0x92c2f498f37adab9c7a4bf0aae161bb929b33867f5b5976848450005f577b8cb")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x8ed378fb4c0fa800eed175f6978d71f027f4a9d07ac50a3f6c2844ea50a74818")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0x096172dff854a4d9f67fb972ad494924c83beb6624b06ec2b047119c5c20978e")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x7feaec82f5a4c977f98fbba6e71dae61eb7b3ec61b2bd88e7e5b06bdf91e50ed")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::from_str("0x685e6e68197229ce85c17dc36118fe13f0bfde48652d7e991793b6710233fe1c")
            .expect("Failed to parse BSC signature"),
        B256::from_str("0x523824b05d0da3067cec66c12988e2ceb10116b5fc017a85cbbe17b34760a07b")
            .expect("Failed to parse BSC signature")
    );
    map.insert(
        B256::ZERO, // 0x0000000000000000000000000000000000000000000000000000000000000000
        B256::from_str("0x857f4c28235bc6eb3bc3e8f08a85102c62e0ff505a6eb0e6daaa7886a5ed4207")
            .expect("Failed to parse BSC signature")
    );
    map
});

/// Simple test for TrieDB functionality
///
/// This test demonstrates basic TrieDB operations:
/// 1. Initialize global manager
/// 2. Create TrieDB instance with PathDB
/// 3. Update an account
/// 4. Commit changes
#[test]
#[serial]
fn test_multiple_accounts_update() {
    // Initialize global manager
    init_empty_root_node();

    // Create temporary directories for databases
    let path_db_temp_dir = TempDir::new().expect("Failed to create temp directory for PathDB");
    let path_db_path = path_db_temp_dir.path().to_str().unwrap();

    // Create path database and TrieDB instance
    let config = PathProviderConfig::default();
    let path_db = PathDB::new(path_db_path, config).expect("Failed to create PathDB");
    let mut triedb = TrieDB::new(path_db);

    let total_operations = 10000;

    let mut states = HashMap::new();
    let states_rebuild = HashSet::new();
    let storage_states = HashMap::new();

    for i in 0..total_operations {
        let hashed_address = keccak256((i as u64).to_le_bytes());
        let account = StateAccount::default()
            .with_nonce(i as u64)
            .with_balance(U256::from(i as u64));

        states.insert(hashed_address, Some(account));
    }
    // Update and commit
    let (root_hash, merged_node_set, diff_storage_roots) = triedb.finalise(
        B256::ZERO,
        None,
        states,
        states_rebuild,
        storage_states,
    ).unwrap();

    let diff_nodes = (*merged_node_set.to_diff_nodes()).clone();
    let difflayer = Arc::new(DiffLayer::new(diff_nodes, diff_storage_roots));
    triedb.flush(0, root_hash, &Some(difflayer)).unwrap();
    

    triedb.state_at(root_hash, None).unwrap();

    for i in 0..total_operations {
        let hashed_address = keccak256((i as u64).to_le_bytes());
        triedb.get_account_with_hash_state(hashed_address).unwrap().unwrap();
    }
    triedb.clean();

    println!("Result: {:?}", root_hash);

    
}


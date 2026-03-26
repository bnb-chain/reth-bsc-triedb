use eyre::Result;
use alloy_primitives::{B256, U256, keccak256};
use rust_eth_triedb::{init_global_triedb_manager, get_global_triedb, TrieDBHashedPostState};
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_common::{TrieDatabase};
use std::collections::HashMap;
use tempfile::TempDir;

use super::GenerateStorageRootMainTask;

#[test]
fn test_generate_storage_root_smoke() -> Result<()> {
    // Initialize tracing for test output
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();

    // Create a temporary directory for the triedb
    let temp_dir = TempDir::new()?;
    let triedb_path = temp_dir.path().to_path_buf();
    
    tracing::info!("Using temporary directory: {:?}", triedb_path);

    // Step 1: Initialize the global triedb manager
    let path_str = triedb_path.to_str()
        .ok_or_else(|| eyre::eyre!("Invalid path: {:?}", triedb_path))?;
    init_global_triedb_manager(path_str);

    // Step 2: Get the global triedb instance
    let mut triedb = get_global_triedb();

    // Step 3: Create 1 million hash_addresses with storage data
    const NUM_ACCOUNTS: usize = 1_000_000;
    let mut hashed_addresses: Vec<B256> = Vec::new();
    let mut hashed_post_state = TrieDBHashedPostState::default();

    tracing::info!("Creating {} accounts with random storage roots...", NUM_ACCOUNTS);

    for i in 0..NUM_ACCOUNTS {
        // Generate a unique hash_address by hashing the index
        let address_bytes = format!("account_{}", i).into_bytes();
        let hashed_address = keccak256(address_bytes);

        // Generate a random storage root by creating actual storage data
        // This ensures the storage root is properly calculated and saved
        let storage_key_bytes = format!("storage_key_{}", i).into_bytes();
        let hashed_storage_key = keccak256(storage_key_bytes);
        let storage_value = U256::from(i as u64);

        // Create StateAccount (storage_root will be calculated from actual storage data)
        let state_account = StateAccount::default()
            .with_nonce(i as u64)
            .with_balance(U256::from(i as u64))
            .with_code_hash(keccak256(b"code_hash"));

        // Add storage data to hashed_post_state
        // This will cause the storage root to be calculated correctly
        let mut storage_map = HashMap::new();
        storage_map.insert(hashed_storage_key, Some(storage_value));
        hashed_post_state.storage_states.insert(hashed_address, storage_map);

        // Add to hashed_post_state
        hashed_post_state.states.insert(hashed_address, Some(state_account));
        
        // Store hashed_address for later verification
        hashed_addresses.push(hashed_address);

        if (i + 1) % 100000 == 0 {
            tracing::info!("Created {} accounts...", i + 1);
        }
    }

    tracing::info!("All {} accounts created. Committing to triedb...", NUM_ACCOUNTS);

    // Step 4: Commit the hashed post state
    let initial_root = alloy_trie::EMPTY_ROOT_HASH;
    let (new_root, difflayer) = triedb.intermediate_and_commit_hashed_post_state(
        initial_root,
        None,
        &hashed_post_state,
        None
    )?;

    tracing::info!("Committed state root: {:?}", new_root);

    // Flush to disk (using block number 1 and the new root)
    triedb.flush(1, new_root, &Some(difflayer))?;

    tracing::info!("Flushed changes to disk");

    triedb.state_at(new_root, None, None)?;
    // Step 5: Read expected storage roots from committed accounts
    // These are the storage roots that should be saved by GenerateStorageRootMainTask
    tracing::info!("Reading expected storage roots from committed accounts...");
    let mut expected_mapping: HashMap<B256, B256> = HashMap::new();
    for hashed_address in &hashed_addresses {
        match triedb.get_account_with_hash_state(*hashed_address)? {
            Some(account) => {
                expected_mapping.insert(*hashed_address, account.storage_root);
            }
            None => {
                return Err(eyre::eyre!("Account not found for hashed_address: {:?}", hashed_address));
            }
        }
    }
    tracing::info!("Read {} expected storage roots", expected_mapping.len());

    // Step 6: Create GenerateStorageRootMainTask and start
    tracing::info!("Creating GenerateStorageRootMainTask...");
    let path_db = triedb.get_mut_path_db_ref().clone();

    let mut main_task = GenerateStorageRootMainTask::new(
        "main_task".to_string(), 
        path_db.clone());
    tracing::info!("Starting GenerateStorageRootMainTask...");
    main_task.start()?;
    tracing::info!("GenerateStorageRootMainTask execution completed");

    // Step 7: Verify storage roots
    tracing::info!("Verifying storage roots...");
    let mut verified_count = 0;
    let mut mismatch_count = 0;

    for (hashed_address, expected_storage_root) in &expected_mapping {
        match path_db.get_storage_root(*hashed_address)? {
            Some(actual_storage_root) => {
                if actual_storage_root == *expected_storage_root {
                    verified_count += 1;
                } else {
                    mismatch_count += 1;
                    if mismatch_count <= 10 {
                        tracing::error!(
                            "Mismatch for address {:?}: expected {:?}, got {:?}",
                            hashed_address,
                            expected_storage_root,
                            actual_storage_root
                        );
                    }
                }
            }
            None => {
                mismatch_count += 1;
                if mismatch_count <= 10 {
                    tracing::error!(
                        "Storage root not found for address: {:?}",
                        hashed_address
                    );
                }
            }
        }

        if (verified_count + mismatch_count) % 100000 == 0 {
            tracing::info!(
                "Verified {} accounts, {} mismatches so far...",
                verified_count,
                mismatch_count
            );
        }
    }

    tracing::info!(
        "Verification complete: {} verified, {} mismatches out of {} total",
        verified_count,
        mismatch_count,
        expected_mapping.len()
    );

    // Assert that all storage roots match
    assert_eq!(
        mismatch_count, 0,
        "Expected all storage roots to match, but found {} mismatches",
        mismatch_count
    );

    assert_eq!(
        verified_count, NUM_ACCOUNTS,
        "Expected to verify {} accounts, but only verified {}",
        NUM_ACCOUNTS,
        verified_count
    );

    tracing::info!("✅ Smoke test passed! All {} storage roots verified successfully.", NUM_ACCOUNTS);

    Ok(())
}


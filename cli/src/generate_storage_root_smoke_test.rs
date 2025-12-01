use eyre::Result;
use alloy_primitives::{B256, U256, keccak256};
use rust_eth_triedb::{init_global_triedb_manager, get_global_triedb, TrieDBHashedPostState};
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_common::{TrieDatabase, DiffLayer};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

use crate::generate_storage_root::GenerateStorageRootMainTask;

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

    // Step 3: Create 1 million hash_addresses with random storage roots
    const NUM_ACCOUNTS: usize = 1_000_000;
    let mut expected_mapping: HashMap<B256, B256> = HashMap::new();
    let mut hashed_post_state = TrieDBHashedPostState::default();

    tracing::info!("Creating {} accounts with random storage roots...", NUM_ACCOUNTS);

    for i in 0..NUM_ACCOUNTS {
        // Generate a unique hash_address by hashing the index
        let address_bytes = format!("account_{}", i).into_bytes();
        let hashed_address = keccak256(address_bytes);

        // Generate a random storage root
        let storage_root_bytes = format!("storage_root_{}", i).into_bytes();
        let storage_root = keccak256(storage_root_bytes);

        // Create StateAccount with the random storage root
        let state_account = StateAccount::default()
            .with_nonce(i as u64)
            .with_balance(U256::from(i as u64))
            .with_storage_root(storage_root)
            .with_code_hash(keccak256(b"code_hash"));

        // Store the mapping
        expected_mapping.insert(hashed_address, storage_root);

        // Add to hashed_post_state
        hashed_post_state.states.insert(hashed_address, Some(state_account));

        if (i + 1) % 100000 == 0 {
            tracing::info!("Created {} accounts...", i + 1);
        }
    }

    tracing::info!("All {} accounts created. Committing to triedb...", NUM_ACCOUNTS);

    // Step 4: Commit the hashed post state
    let initial_root = alloy_trie::EMPTY_ROOT_HASH;
    let (new_root, difflayers) = triedb.commit_hashed_post_state(
        initial_root,
        None,
        &hashed_post_state
    )?;

    let difflayer = if let Some(difflayer) = difflayers {
        let new_difflayer = DiffLayer::new(
            difflayer.diff_nodes.clone(),
            HashMap::new()  // Empty diff_storage_roots
        );
        Some(Arc::new(new_difflayer))
    } else {
        None
    };

    tracing::info!("Committed state root: {:?}", new_root);

    // Flush to disk (using block number 1 and the new root)
    triedb.flush(1, new_root, &difflayer)?;

    tracing::info!("Flushed changes to disk");

    // Step 5: Create GenerateStorageRootMainTask and start
    tracing::info!("Creating GenerateStorageRootMainTask...");
    let mut main_task = GenerateStorageRootMainTask::new("main_task".to_string());
    tracing::info!("Starting GenerateStorageRootMainTask...");
    main_task.start()?;
    tracing::info!("GenerateStorageRootMainTask execution completed");

    // Step 6: Get pathdb from triedb and verify storage roots
    let path_db = triedb.get_mut_path_db_ref().clone();

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


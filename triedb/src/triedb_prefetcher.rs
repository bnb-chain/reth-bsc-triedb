//! Prefetcher operations for TrieDB.
//!
//! This module provides prefetching functionality to preload account and storage
//! data into the cache, improving performance for subsequent operations.

use std::collections::HashMap;
use alloy_primitives::B256;
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::node::DiffLayers;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieBuilder, SecureTrieTrait};
use crate::triedb::{TrieDB, TrieDBError};
use crate::triedb_reth::TrieDBHashedPostState;

/// Prefetcher structure for caching account and storage tries
#[derive(Clone)]
pub struct Prefetcher<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub account_trie: Option<StateTrie<DB>>,
    pub storage_tries: HashMap<B256, StateTrie<DB>>,
    pub touched_storage_roots: HashMap<B256, B256>,
}

impl<DB> Default for Prefetcher<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    fn default() -> Self {
        Self {
            account_trie: None,
            storage_tries: HashMap::new(),
            touched_storage_roots: HashMap::new(),
        }
    }
}

impl<DB> Prefetcher<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub fn new(path_db: DB, root_hash: B256, difflayer: Option<&DiffLayers>) -> Result<Self, TrieDBError> {
        let id = SecureTrieId::new(root_hash);
        let account_trie = SecureTrieBuilder::new(path_db)
            .with_id(id)
            .build_with_difflayer(difflayer)
            .map_err(|e| TrieDBError::StateTrie(e))?;

        Ok(Self {
            account_trie: Some(account_trie),
            storage_tries: HashMap::new(),
            touched_storage_roots: HashMap::new(),
        })
    }
}

impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{

    pub fn prefetcher_hashed_posted_state(&mut self, hashed_posted_state: &TrieDBHashedPostState) -> Result<(), TrieDBError> {        
        for (hashed_address, _account) in hashed_posted_state.states.iter() {
            self.prefetcher_account(*hashed_address)?;
        }
        for (hashed_address, storage_states) in hashed_posted_state.storage_states.iter() {
            for (hashed_key, _storage_state) in storage_states.iter() {
                self.prefetcher_storage(*hashed_address, *hashed_key)?;
            }
        }
        Ok(())
    }

    pub fn prefetcher_account(&mut self, hashed_address: B256) -> Result<(), TrieDBError> {
        // Get storage root using public method
        let storage_root = self.get_storage_root(hashed_address)?;
        self.prefetcher.touched_storage_roots.insert(hashed_address, storage_root);
        
        // Prefetch account from prefetcher's account trie if available
        self.prefetcher.account_trie.as_mut().unwrap().get_account_with_hash_state(hashed_address)
            .map_err(|e| TrieDBError::StateTrie(e))?;

        Ok(())
    }

    pub fn prefetcher_storage(&mut self, hashed_address: B256, hashed_key: B256) -> Result<(), TrieDBError> {
        // Get or compute storage root
        let storage_root = if let Some(root) = self.prefetcher.touched_storage_roots.get(&hashed_address) {
            *root
        } else {
            let storage_root = self.get_storage_root(hashed_address)?;
            self.prefetcher.touched_storage_roots.insert(hashed_address, storage_root);
            storage_root
        };
        
        // Check if storage trie exists, if not create and insert it
        if !self.prefetcher.storage_tries.contains_key(&hashed_address) {
            let id = SecureTrieId::new(storage_root)
                .with_owner(hashed_address);
            let storage_trie = SecureTrieBuilder::new(self.path_db.clone())
                .with_id(id)
                .build_with_difflayer(self.difflayer.as_ref())
                .map_err(|e| TrieDBError::StateTrie(e))?;
            self.prefetcher.storage_tries.insert(hashed_address, storage_trie);
        }
        
        // Get storage trie from HashMap and prefetch
        self.prefetcher.storage_tries.get_mut(&hashed_address).unwrap().get_storage_with_hash_state(hashed_address, hashed_key)
            .map_err(|e| TrieDBError::StateTrie(e))?;

        
        
        Ok(())
    }
}


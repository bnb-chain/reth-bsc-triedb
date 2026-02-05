//! Reth-compatible implementations for TrieDB.

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use std::time::Instant;
use std::sync::{mpsc, Mutex};
use std::thread;

use alloy_primitives::{B256, U256, hex};
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::{MergedNodeSet, NodeSet, DiffLayer, DiffLayers};
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieTrait, SecureTrieBuilder};

use crate::triedb::{TrieDB, TrieDBError};

/// Reth-compatible interface functions using hashed keys for TrieDB.
///
/// This module provides interfaces compatible with clients that use hashed keys
/// (Keccak-256 hashes of addresses and storage keys) to access the trie database.
///
/// # Write Operations (Batch Only)
///
/// **Important**: `TrieDB` only supports batch write operations. Individual storage 
/// key-value write operations are **not supported** and will not persist correctly.
///
/// All write operations must be performed through one of the following batch methods:
/// - [`batch_update_and_commit`](Self::batch_update_and_commit) - 
///   Batch update accounts and storage, then commit all changes atomically
/// - [`commit_hashed_post_state`](Self::commit_hashed_post_state) - 
///   Commit a complete post-state with all account and storage changes
///
/// The modification functions (`update_account_with_hash_state`, `delete_account_with_hash_state`, 
/// `update_storage_with_hash_state`, `delete_storage_with_hash_state`) in this module are 
/// **not intended for external use**. They are:
/// - Marked as `#[allow(dead_code)]` or kept internal for internal batch operations
/// - Only modify in-memory state without proper commit handling
/// - Do not update storage roots correctly in the account trie
/// - Do not integrate with the diff layer system
/// - Individual writes would be inefficient and break consistency guarantees
///
/// # Read Operations (Public API)
///
/// The query functions (`get_account_with_hash_state`, `get_storage_with_hash_state`) are 
/// **public and safe to use**. They support:
/// - Reading account data from the state trie using hashed addresses
/// - Reading storage values from account storage tries using hashed keys
/// - **Pre-warming**: These functions can be used to preload and cache frequently
///   accessed tries into memory, improving subsequent batch operation performance.
///   When you call `get_account_with_hash_state` or `get_storage_with_hash_state`, the 
///   underlying tries are loaded and cached, which helps optimize batch operations that 
///   access the same data.
///
/// # Usage Pattern
///
/// ```ignore
/// // ✅ Correct: Use batch operations for writes
/// triedb.batch_update_and_commit(root_hash, difflayer, accounts, rebuild_set, storage)?;
///
/// // ✅ Correct: Use query functions for reads and pre-warming
/// let account = triedb.get_account_with_hash_state(hashed_address)?;
/// let storage_value = triedb.get_storage_with_hash_state(hashed_address, hashed_key)?;
///
/// // ❌ Incorrect: Do not use individual write functions
/// // triedb.update_storage_with_hash_state(hashed_address, hashed_key, value)?;  // Will not persist correctly!
/// ```
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub fn get_account_with_hash_state(&mut self, hashed_address: B256) -> Result<Option<StateAccount>, TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().get_account_with_hash_state(hashed_address)?)
    }

    pub fn update_account_with_hash_state(&mut self, hashed_address: B256, account: &StateAccount) -> Result<(), TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().update_account_with_hash_state(hashed_address, account)?)
    }
    
    pub fn delete_account_with_hash_state(&mut self, hashed_address: B256) -> Result<(), TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().delete_account_with_hash_state(hashed_address)?)
    }

    pub fn get_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<Option<Vec<u8>>, TrieDBError> {
        let mut storage_trie = self.get_storage_trie_with_hash_state(hashed_address)?;
        Ok(storage_trie.get_storage_with_hash_state(hashed_address, hashed_key)?)
    }

    #[allow(dead_code)]
    fn update_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256, value: &[u8]) -> Result<(), TrieDBError> {
        let mut storage_trie = self.get_storage_trie_with_hash_state(hashed_address)?;
        Ok(storage_trie.update_storage_with_hash_state(hashed_address, hashed_key, value)?)
    }

    #[allow(dead_code)]
    fn delete_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<(), TrieDBError> {
        let mut storage_trie = self.get_storage_trie_with_hash_state(hashed_address)?;
        Ok(storage_trie.delete_storage_with_hash_state(hashed_address, hashed_key)?)
    }
}

#[derive(Default, Clone, Debug)]
pub struct TrieDBHashedPostState {
    pub states: HashMap<B256, Option<StateAccount>>,
    pub states_rebuild: HashSet<B256>,
    pub storage_states: HashMap<B256, HashMap<B256, Option<U256>>>
}

#[derive(Clone, Debug)]
pub struct TrieDBPrefetchState<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub account_trie: StateTrie<DB>,
    pub storage_roots: HashMap<B256, B256>,
    pub storage_tries: HashMap<B256, StateTrie<DB>>,
}

/// Compatible with Reth client usage scenarios
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{  
    pub fn finalise(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        states: HashMap<B256, Option<StateAccount>>,
        states_rebuild: HashSet<B256>,
        storage_states: HashMap<B256, HashMap<B256, Option<U256>>>,
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> 
        Result<(B256, Arc<MergedNodeSet>, Arc<HashMap<B256, B256>>), TrieDBError>
    where
        DB: 'static,
    {
        
        self.state_at(parent_root, difflayer, prefetcher)?;
        self.intermediate_inner(states, storage_states, states_rebuild)?;
        return self.commit_inner(true)
    }

    fn intermediate_inner(
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>,
        states_rebuild: HashSet<B256>) -> 
        Result<B256, TrieDBError> {
        
        let accounts_len = accounts.len();
        let storages_accounts_len = storages.len();
        let storages_slots_len: usize = storages.values().map(|m| m.len()).sum();
        let states_rebuild_len = states_rebuild.len();

        let intermediate_root_start = Instant::now();

        let intermediate_state_objects_start = Instant::now();
        let updated_accounts = self.update_state_objects(accounts, storages, states_rebuild.clone())?;
        let intermediate_state_objects_elapsed = intermediate_state_objects_start.elapsed();
        self.metrics.record_intermediate_state_objects_duration(intermediate_state_objects_elapsed.as_secs_f64());
        
        let delete_rebuild_start = Instant::now();
        for hashed_address in states_rebuild {
            self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
        }
        let delete_rebuild_elapsed = delete_rebuild_start.elapsed();
        
        let apply_updated_accounts_start = Instant::now();
        for (hashed_address, account) in updated_accounts {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
        }
        let apply_updated_accounts_elapsed = apply_updated_accounts_start.elapsed();

        let account_trie_hash_start = Instant::now();
        let root_hash = self.account_trie.as_mut().unwrap().hash();
        let account_trie_hash_elapsed = account_trie_hash_start.elapsed();

        let intermediate_total_elapsed = intermediate_root_start.elapsed();
        self.metrics.record_intermediate_root_duration(intermediate_total_elapsed.as_secs_f64());

        tracing::debug!(
            target: "triedb::reth",
            total_ms = intermediate_total_elapsed.as_millis(),
            update_state_objects_ms = intermediate_state_objects_elapsed.as_millis(),
            delete_rebuild_ms = delete_rebuild_elapsed.as_millis(),
            apply_updated_accounts_ms = apply_updated_accounts_elapsed.as_millis(),
            account_trie_hash_ms = account_trie_hash_elapsed.as_millis(),
            accounts_len,
            storages_accounts_len,
            storages_slots_len,
            states_rebuild_len,
            "intermediate_inner finished"
        );

        return Ok(root_hash);
    }

    fn update_state_objects (
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>, 
        states_rebuild: HashSet<B256>) -> 
        Result<HashMap<B256, Option<StateAccount>>, TrieDBError> {
       
        let update_state_objects_start = Instant::now();

        // Replace Rayon inside this function with a std::thread worker pool + work queue.
        let num_workers = 48;

        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());
        let metrics_clone = self.metrics.clone();
        let prefetcher_clone = self.prefetcher.clone();
        let prefetcher_for_roots = self.prefetcher.clone();

        let storages_keys: HashSet<B256> = storages.keys().cloned().collect();
        let storages_vec: Vec<(B256, HashMap<B256, Option<U256>>)> = storages.into_iter().collect();
        let accounts_arc = Arc::new(accounts);
        let states_rebuild = Arc::new(states_rebuild);

        // Build work items
        let accounts_no_storage_items: Vec<(B256, Option<StateAccount>)> = accounts_arc
            .iter()
            .filter(|(hashed_address, _)| !storages_keys.contains(*hashed_address))
            .map(|(hashed_address, account)| (*hashed_address, account.clone()))
            .collect();

        // Chunk helper (moves items into owned Vecs for worker queue).
        fn chunk_items<T>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
            let mut out = Vec::new();
            let mut it = items.into_iter();
            loop {
                let mut chunk = Vec::with_capacity(chunk_size);
                for _ in 0..chunk_size {
                    if let Some(item) = it.next() {
                        chunk.push(item);
                    } else {
                        break;
                    }
                }
                if chunk.is_empty() {
                    break;
                }
                out.push(chunk);
            }
            out
        }

        let target_chunks = (num_workers * 4).max(1);
        let acc_chunk_size =
            ((accounts_no_storage_items.len() + target_chunks - 1) / target_chunks).max(1);
        let sto_chunk_size = ((storages_vec.len() + target_chunks - 1) / target_chunks).max(1);

        let acc_chunks = chunk_items(accounts_no_storage_items, acc_chunk_size);
        let sto_chunks = chunk_items(storages_vec, sto_chunk_size);
        let mut acc_left = acc_chunks.len();
        let mut sto_left = sto_chunks.len();

        enum WorkResult<DB> {
            Accounts(Result<Vec<(B256, Option<StateAccount>, B256)>, TrieDBError>),
            Storages(Result<Vec<(B256, Option<StateAccount>, B256, StateTrie<DB>)>, TrieDBError>),
        }

        enum WorkItem {
            Accounts(Vec<(B256, Option<StateAccount>)>),
            Storages(Vec<(B256, HashMap<B256, Option<U256>>)>),
        }

        let mut accounts_no_storage: HashMap<B256, Option<StateAccount>> = HashMap::new();
        let mut roots_no_storage: Box<HashMap<B256, B256>> = Box::new(HashMap::new());
        let mut accounts_with_storage: HashMap<B256, Option<StateAccount>> = HashMap::new();
        let mut roots_with_storage: Box<HashMap<B256, B256>> = Box::new(HashMap::new());
        let mut storage_tries: HashMap<B256, StateTrie<DB>> = HashMap::new();

        // Run a scoped worker pool so we can execute tasks without requiring `DB: 'static`.
        thread::scope(|scope| -> Result<(), TrieDBError> {
            let (work_tx, work_rx) = mpsc::channel::<WorkItem>();
            let work_rx = Arc::new(Mutex::new(work_rx));
            let (result_tx, result_rx) = mpsc::channel::<WorkResult<DB>>();

            // Spawn worker threads.
            for i in 0..num_workers {
                let work_rx = Arc::clone(&work_rx);
                let result_tx = result_tx.clone();

                let path_db = path_db_clone.clone();
                let difflayer = difflayer_clone.clone();
                let prefetcher = prefetcher_clone.clone();
                let prefetcher_roots = prefetcher_for_roots.clone();
                let accounts_arc = Arc::clone(&accounts_arc);
                let states_rebuild = Arc::clone(&states_rebuild);

                let builder = thread::Builder::new().name(format!("triedb-update-state-{i}"));
                builder
                    .spawn_scoped(scope, move || loop {
                        let item = {
                            let guard = work_rx.lock().expect("work queue receiver poisoned");
                            guard.recv()
                        };
                        let Ok(item) = item else { break };

                        let get_storage_root = |hashed_address: B256| -> Result<B256, TrieDBError> {
                            if states_rebuild.contains(&hashed_address) {
                                return Ok(alloy_trie::EMPTY_ROOT_HASH);
                            }

                            if let Some(prefetcher) = &prefetcher_roots {
                                if let Some(root) = prefetcher.storage_roots.get(&hashed_address) {
                                    return Ok(*root);
                                }
                            }

                            if let Some(dl) = difflayer.as_ref() {
                                if let Some(root) = dl.get_storage_root(hashed_address) {
                                    return Ok(root);
                                }
                            }

                            path_db
                                .get_storage_root(hashed_address)
                                .map_err(|e| {
                                    TrieDBError::Database(format!(
                                        "Failed to get storage root for hashed_address: 0x{}, error: {:?}",
                                        hex::encode(hashed_address),
                                        e
                                    ))
                                })
                                .map(|opt| opt.unwrap_or(alloy_trie::EMPTY_ROOT_HASH))
                        };

                        let msg = match item {
                            WorkItem::Accounts(chunk) => {
                                let mut out = Vec::with_capacity(chunk.len());
                                let res: Result<Vec<_>, TrieDBError> = (|| {
                                    for (hashed_address, account) in chunk {
                                        match account {
                                            Some(mut account) => {
                                                let storage_root = get_storage_root(hashed_address)?;
                                                account.storage_root = storage_root;
                                                out.push((hashed_address, Some(account), storage_root));
                                            }
                                            None => out.push((hashed_address, None, alloy_trie::EMPTY_ROOT_HASH)),
                                        }
                                    }
                                    Ok(out)
                                })();
                                WorkResult::Accounts(res)
                            }
                            WorkItem::Storages(chunk) => {
                                let mut out = Vec::with_capacity(chunk.len());
                                let res: Result<Vec<_>, TrieDBError> = (|| {
                                    for (hashed_address, kvs) in chunk {
                                        // Try to get storage_trie from prefetcher, otherwise create a new one
                                        let mut storage_trie = match prefetcher
                                            .as_ref()
                                            .and_then(|p| p.storage_tries.get(&hashed_address))
                                            .cloned()
                                        {
                                            Some(trie) => trie,
                                            None => {
                                                let storage_root = get_storage_root(hashed_address)?;
                                                let id = SecureTrieId::new(storage_root).with_owner(hashed_address);
                                                SecureTrieBuilder::new(path_db.clone())
                                                    .with_id(id)
                                                    .build_with_difflayer(difflayer.as_ref())
                                                    .map_err(|e| {
                                                        TrieDBError::Database(format!(
                                                            "Failed to build storage trie for hashed_address: 0x{}, error: {}",
                                                            hex::encode(hashed_address),
                                                            e
                                                        ))
                                                    })?
                                            }
                                        };

                                        for (hashed_key, new_value) in kvs {
                                            if let Some(new_value) = new_value {
                                                storage_trie.update_storage_u256_with_hash_state(hashed_address, hashed_key, new_value)
                                                    .map_err(|e| TrieDBError::Database(format!(
                                                        "Failed to update storage for hashed_address: 0x{}, hashed_key: 0x{}, new_value: {:#x}, error: {}",
                                                        hex::encode(hashed_address), hex::encode(hashed_key), new_value, e
                                                    )))?;
                                            } else {
                                                storage_trie.delete_storage_with_hash_state(hashed_address, hashed_key)
                                                    .map_err(|e| TrieDBError::Database(format!(
                                                        "Failed to delete storage for hashed_address: 0x{}, hashed_key: 0x{}, error: {}",
                                                        hex::encode(hashed_address), hex::encode(hashed_key), e
                                                    )))?;
                                            }
                                        }

                                        let new_storage_root = storage_trie.hash();
                                        let mut new_account = accounts_arc.get(&hashed_address).unwrap().as_ref().unwrap().clone();
                                        new_account.storage_root = new_storage_root;

                                        out.push((hashed_address, Some(new_account), new_storage_root, storage_trie));
                                    }
                                    Ok(out)
                                })();
                                WorkResult::Storages(res)
                            }
                        };

                        let _ = result_tx.send(msg);
                    })
                    .expect("failed to spawn triedb update_state_objects worker");
            }

            drop(result_tx);

            // Enqueue work.
            let acc_start = Instant::now();
            let sto_start = Instant::now();

            for chunk in acc_chunks {
                work_tx
                    .send(WorkItem::Accounts(chunk))
                    .map_err(|_| TrieDBError::Database("failed to enqueue accounts work".to_string()))?;
            }
            for chunk in sto_chunks {
                work_tx
                    .send(WorkItem::Storages(chunk))
                    .map_err(|_| TrieDBError::Database("failed to enqueue storages work".to_string()))?;
            }
            drop(work_tx);

            // Collect results.
            while acc_left > 0 || sto_left > 0 {
                let msg = result_rx.recv().map_err(|e| {
                    TrieDBError::Database(format!("thread pool result channel dropped: {e}"))
                })?;

                match msg {
                    WorkResult::Accounts(res) => {
                        acc_left = acc_left.saturating_sub(1);
                        let chunk = res?;
                        for (hashed_address, account, storage_root) in chunk {
                            accounts_no_storage.insert(hashed_address, account);
                            roots_no_storage.insert(hashed_address, storage_root);
                        }
                        if acc_left == 0 {
                            metrics_clone.record_intermediate_state_objects_account_duration(
                                acc_start.elapsed().as_secs_f64(),
                            );
                        }
                    }
                    WorkResult::Storages(res) => {
                        sto_left = sto_left.saturating_sub(1);
                        let chunk = res?;
                        for (hashed_address, account, storage_root, storage_trie) in chunk {
                            accounts_with_storage.insert(hashed_address, account);
                            roots_with_storage.insert(hashed_address, storage_root);
                            storage_tries.insert(hashed_address, storage_trie);
                        }
                        if sto_left == 0 {
                            metrics_clone.record_intermediate_state_objects_storage_duration(
                                sto_start.elapsed().as_secs_f64(),
                            );
                        }
                    }
                }
            }

            // If a group had no work, still record near-zero duration for consistency.
            if accounts_no_storage.is_empty() {
                metrics_clone.record_intermediate_state_objects_account_duration(0.0);
            }
            if storage_tries.is_empty() {
                metrics_clone.record_intermediate_state_objects_storage_duration(0.0);
            }

            Ok(())
        })?;

        // Merge results
        accounts_no_storage.extend(accounts_with_storage);
        roots_no_storage.extend(roots_with_storage.into_iter());

        let updated_storage_trie_count = storage_tries.len();
        self.storage_tries = storage_tries;
        self.updated_storage_roots = roots_no_storage;

        let elapsed = update_state_objects_start.elapsed();
        tracing::debug!(
            target: "triedb::reth",
            elapsed_ms = elapsed.as_millis(),
            updated_storage_trie_count,
            "update_state_objects finished"
        );

        Ok(accounts_no_storage)
    }

    fn commit_inner(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>, Arc<HashMap<B256, B256>>), TrieDBError>
    where
        DB: 'static,
    {
        let commit_start = Instant::now();
        let (root_hash, node_set) = self.commit_state_objects(true)?;
        self.metrics.record_commit_duration(commit_start.elapsed().as_secs_f64());

        let diff_storage_roots = Arc::from(*self.updated_storage_roots.clone());
        self.clean();

        Ok((root_hash, node_set, diff_storage_roots))
    }

    fn commit_state_objects(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>), TrieDBError> {        
        let mut merged_node_set = Box::new(MergedNodeSet::new());

        // Start both tasks in parallel using rayon
        let mut account_trie_clone = self.account_trie.as_mut().unwrap().clone();
        let (account_commit_result, storage_commit_results): (Result<(B256, Option<Arc<NodeSet>>), _>, Vec<(B256, Option<Arc<NodeSet>>)>) = rayon::join(
            || account_trie_clone.commit(true),
            || self.storage_tries
                .par_iter()
                .map(|(hashed_address, trie)| {
                    let (_, node_set) = trie.clone().commit(false).unwrap();
                    (*hashed_address, node_set)
                })
                .collect()
        );

        let (root_hash, account_node_set) = account_commit_result?;

        if let Some(node_set) = account_node_set {
            merged_node_set.merge(node_set)
                .map_err(|e| TrieDBError::Database(e))?;
        }

        for (_, node_set) in storage_commit_results {
            if let Some(node_set) = node_set {
                merged_node_set.merge(node_set)
                    .map_err(|e| TrieDBError::Database(e))?;
            }
        }
        Ok((root_hash, Arc::from(*merged_node_set)))
    }

    pub fn intermediate_and_commit_hashed_post_state(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState, 
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> 
        Result<(B256, Arc<DiffLayer>), TrieDBError>
    where
        DB: 'static,
    {
        self.state_at(parent_root, difflayer, prefetcher)?;
        self.intermediate_inner(
            hashed_post_state.states.clone(), 
            hashed_post_state.storage_states.clone(), 
            hashed_post_state.states_rebuild.clone())?;
        return self.commit(true)
    }

    pub fn intermediate_hashed_post_state(
        &mut self,
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState, 
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>
    ) -> Result<B256, TrieDBError>
    where
        DB: 'static,
    {
        self.state_at(parent_root, difflayer, prefetcher)?;
        return self.intermediate_inner(
            hashed_post_state.states.clone(), 
            hashed_post_state.storage_states.clone(), 
            hashed_post_state.states_rebuild.clone());
    }

    pub fn commit(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<DiffLayer>), TrieDBError> 
    where
        DB: 'static,
    {
        let (root_hash, node_set, diff_storage_roots) = self.commit_inner(true)?;
        let difflayer = Arc::new(DiffLayer::new(node_set.to_diff_nodes(), diff_storage_roots));
        Ok((root_hash, difflayer)) 
    }

    pub fn intermediate_and_commit_hashed_post_state_v2(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState, 
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> 
        Result<(B256, Arc<DiffLayer>), TrieDBError>
    where
        DB: 'static,
    {
        self.state_at(parent_root, difflayer, prefetcher)?;

        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());
        let accounts_clone = hashed_post_state.states.clone();
        let storages_keys: HashSet<B256> = hashed_post_state.storage_states.keys().cloned().collect();
        let storages_for_task2 = hashed_post_state.storage_states.clone();
        let metrics_clone = self.metrics.clone();
        let prefetcher_clone = self.prefetcher.clone();

        // Closure to get storage root from difflayer or path_db
        let get_storage_root = |hashed_address: B256| -> Result<B256, TrieDBError> {
            if hashed_post_state.states_rebuild.contains(&hashed_address) {
                return Ok(alloy_trie::EMPTY_ROOT_HASH);
            }

            if let Some(prefetcher) = &self.prefetcher {
                if let Some(root) = prefetcher.storage_roots.get(&hashed_address) {
                    return Ok(*root);
                }
            }

            if let Some(dl) = difflayer_clone.as_ref() {
                if let Some(root) = dl.get_storage_root(hashed_address) {
                    return Ok(root);
                }
            }
            path_db_clone.get_storage_root(hashed_address)
                .map_err(|e| TrieDBError::Database(format!("Failed to get storage root for hashed_address: 0x{}, error: {:?}", hex::encode(hashed_address), e)))
                .map(|opt| opt.unwrap_or(alloy_trie::EMPTY_ROOT_HASH))
        };

        // Parallel execution: process accounts and storages simultaneously
        let (account_result, storage_result): (
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>), TrieDBError>,
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>, Box<MergedNodeSet>), TrieDBError>
        ) = rayon::join(
            || {
                // Task 1: Process accounts that don't have storage updates (parallel)
                let task1_start = Instant::now();
                let result = accounts_clone
                    .par_iter()
                    .filter(|(hashed_address, _)| !storages_keys.contains(*hashed_address))
                    .map(|(hashed_address, account)| {
                        match account {
                            Some(account) => {
                                let mut new_account = account.clone();
                                let storage_root = get_storage_root(*hashed_address)?;
                                new_account.storage_root = storage_root;
                                Ok((*hashed_address, (Some(new_account), storage_root)))
                            }
                            None => {
                                Ok((*hashed_address, (None, alloy_trie::EMPTY_ROOT_HASH)))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vec| {
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        for (hashed_address, (account, storage_root)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                        }
                        (new_accounts, diff_account_storage_roots)
                    });
                metrics_clone.record_intermediate_state_objects_account_duration(task1_start.elapsed().as_secs_f64());
                result
            },
            || {
                // Task 2: Process accounts with storage updates (parallel)
                let task2_start = Instant::now();
                let result = storages_for_task2
                    .into_par_iter()
                    .map(|(hashed_address, kvs)| {

                        // Try to get storage_trie from prefetcher, otherwise create a new one
                        let mut storage_trie = match prefetcher_clone.as_ref()
                            .and_then(|p| p.storage_tries.get(&hashed_address))
                            .cloned()
                        {
                            Some(trie) => trie,
                            None => {
                                // Get storage root from path_db or difflayer
                                let storage_root = get_storage_root(hashed_address)?;
                                let id = SecureTrieId::new(storage_root)
                                    .with_owner(hashed_address);
                                SecureTrieBuilder::new(path_db_clone.clone())
                                    .with_id(id)
                                    .build_with_difflayer(difflayer_clone.as_ref())
                                    .map_err(|e| TrieDBError::Database(format!("Failed to build storage trie for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?
                            }
                        };

                        // Parallel execution for kvs within each address
                        let kvs_vec: Vec<_> = kvs.into_iter().collect();
                        for (hashed_key, new_value) in kvs_vec {
                            if let Some(new_value) = new_value {
                                storage_trie.update_storage_u256_with_hash_state(hashed_address, hashed_key, new_value)
                                    .map_err(|e| TrieDBError::Database(format!("Failed to update storage for hashed_address: 0x{}, hashed_key: 0x{}, new_value: {:#x}, error: {}", hex::encode(hashed_address), hex::encode(hashed_key), new_value, e)))?;
                            } else {
                                storage_trie.delete_storage_with_hash_state(hashed_address, hashed_key)
                                    .map_err(|e| TrieDBError::Database(format!("Failed to delete storage for hashed_address: 0x{}, hashed_key: 0x{}, error: {}", hex::encode(hashed_address), hex::encode(hashed_key), e)))?;
                            }
                        }

                        let (new_storage_root, node_set) = storage_trie.commit(false)?;
                        let mut new_account = accounts_clone.get(&hashed_address).unwrap().unwrap().clone();
                        new_account.storage_root = new_storage_root;

                        Ok((hashed_address, (Some(new_account), new_storage_root, node_set)))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vec| {
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        let mut merged_node_set = Box::new(MergedNodeSet::new());
                        for (hashed_address, (account, storage_root, node_set)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                            if let Some(node_set) = node_set {
                                merged_node_set.merge(node_set).unwrap();
                            }
                        }
                        (new_accounts, diff_account_storage_roots, merged_node_set)
                    });
                metrics_clone.record_intermediate_state_objects_storage_duration(task2_start.elapsed().as_secs_f64());
                result
            }
        );

        let (mut accounts_no_storage, mut roots_no_storage) = account_result?;
        let (accounts_with_storage, roots_with_storage, mut merged_node_set) = storage_result?;

        accounts_no_storage.extend(accounts_with_storage);
        roots_no_storage.extend(roots_with_storage.into_iter());

        for hashed_address in hashed_post_state.states_rebuild.clone() {
            self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
        }
        
        for (hashed_address, account) in accounts_no_storage {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
        }

        let (root_hash, node_set) = self.account_trie.as_mut().unwrap().commit(true)?;
        if let Some(node_set) = node_set {
            merged_node_set.merge(node_set).unwrap();
        }

        let difflayer = Arc::new(DiffLayer::new(merged_node_set.to_diff_nodes(), Arc::from(*roots_no_storage)));
        self.clean();
        Ok((root_hash, difflayer))
    }
}



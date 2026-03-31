//! Streaming storage trie updater for pipelined block production.
//!
//! During transaction execution a state-hook sends per-tx storage slot changes
//! to a background thread via a crossbeam channel. The background thread
//! maintains per-account storage tries and applies slot updates incrementally.
//! When [`StreamingTrieUpdater::finish`] is called (after all txs are done),
//! the background thread hashes all storage tries in parallel and returns the
//! results.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use std::thread::{self, JoinHandle};

use alloy_primitives::{B256, U256};
use alloy_trie::EMPTY_ROOT_HASH;
use crossbeam_channel::{Receiver, Sender, bounded};
use rayon::prelude::*;
use tracing::debug;

use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::DiffLayers;
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieTrait, SecureTrieBuilder};

use crate::triedb::TrieDBError;
use crate::triedb_reth::TrieDBPrefetchState;

/// Message sent over the channel from the executor to the background storage-trie thread.
pub enum StorageTrieMsg {
    /// A batch of slot updates for one account.
    Update {
        /// Keccak-256 hash of the account address.
        hashed_address: B256,
        /// Slot updates: `(hashed_slot, new_value)`.  `None` means deletion.
        slots: Vec<(B256, Option<U256>)>,
    },
    /// Signals that no more updates will be sent; the thread should finalise.
    Finish,
}

/// The result produced by the background thread once all updates have been applied
/// and all storage tries have been hashed.
pub struct PrecomputedStorageResult<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Mapping from hashed address to the new storage root hash.
    pub storage_roots: HashMap<B256, B256>,
    /// Mapping from hashed address to the updated `StateTrie` (ready for commit).
    pub storage_tries: HashMap<B256, StateTrie<DB>>,
}

/// Drives incremental per-account storage trie updates on a dedicated background
/// thread and produces pre-hashed storage roots for the finalisation step.
///
/// # Usage
///
/// 1. Create a `StreamingTrieUpdater` before block execution begins.
/// 2. Obtain a [`Sender`] via [`sender`](Self::sender) and pass it to the
///    transaction executor / state-hook.
/// 3. After all transactions have been executed, call [`finish`](Self::finish)
///    to collect the results.
pub struct StreamingTrieUpdater<DB>
where
    DB: TrieDatabase + Clone + Send + Sync + 'static,
    DB::Error: std::fmt::Debug + Send,
{
    /// Channel endpoint used to send messages to the background thread.
    /// Wrapped in `Option` so it can be taken in `finish` and `drop`.
    tx: Option<Sender<StorageTrieMsg>>,
    /// Handle to the background thread.  `None` after `finish` has been called.
    bg_handle: Option<JoinHandle<Result<PrecomputedStorageResult<DB>, TrieDBError>>>,
}

impl<DB> StreamingTrieUpdater<DB>
where
    DB: TrieDatabase + Clone + Send + Sync + 'static,
    DB::Error: std::fmt::Debug + Send,
{
    /// Creates a new `StreamingTrieUpdater` and spawns the background thread.
    ///
    /// # Parameters
    ///
    /// - `parent_root` – The state root of the parent block (used to resolve
    ///   storage roots that are not yet in any diff layer).
    /// - `path_db` – The underlying path database, cloned for the background thread.
    /// - `difflayers` – Optional diff layers from ancestor blocks.
    /// - `prefetcher` – Optional pre-fetched state that may already contain built
    ///   storage tries or cached storage roots.
    pub fn new(
        _parent_root: B256,
        path_db: DB,
        difflayers: Option<DiffLayers>,
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>,
    ) -> Self {
        let (tx, rx): (Sender<StorageTrieMsg>, Receiver<StorageTrieMsg>) = bounded(4096);

        let bg_handle = thread::Builder::new()
            .name("streaming-storage-trie".to_owned())
            .spawn(move || {
                background_thread(rx, path_db, difflayers, prefetcher)
            })
            .expect("failed to spawn streaming-storage-trie thread");

        Self { tx: Some(tx), bg_handle: Some(bg_handle) }
    }

    /// Returns a clone of the sender so that multiple producers (e.g. the
    /// executor and its sub-tasks) can forward updates to the background thread.
    pub fn sender(&self) -> Sender<StorageTrieMsg> {
        self.tx.as_ref().expect("sender already consumed").clone()
    }

    /// Signals the background thread that all updates have been sent, waits for
    /// it to finish hashing, and returns the pre-computed results.
    pub fn finish(mut self) -> Result<PrecomputedStorageResult<DB>, TrieDBError> {
        // Send the sentinel — ignore send errors (background thread may have
        // already exited due to a processing error).
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(StorageTrieMsg::Finish);
            // `tx` is dropped here, closing our end of the channel.
        }

        // Safety: bg_handle is always Some until finish() is called.
        let handle = self.bg_handle.take().expect("bg_handle missing in finish()");
        handle
            .join()
            .map_err(|_| TrieDBError::Database("streaming-storage-trie thread panicked".into()))?
    }
}

impl<DB> Drop for StreamingTrieUpdater<DB>
where
    DB: TrieDatabase + Clone + Send + Sync + 'static,
    DB::Error: std::fmt::Debug + Send,
{
    fn drop(&mut self) {
        // If the caller forgot to call finish(), try to tell the background
        // thread to stop gracefully.  We do not join here to avoid blocking
        // the calling thread (or causing a panic if the thread already exited).
        if self.bg_handle.is_some() {
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(StorageTrieMsg::Finish);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Background thread implementation
// ---------------------------------------------------------------------------

fn background_thread<DB>(
    rx: Receiver<StorageTrieMsg>,
    path_db: DB,
    difflayers: Option<DiffLayers>,
    prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>,
) -> Result<PrecomputedStorageResult<DB>, TrieDBError>
where
    DB: TrieDatabase + Clone + Send + Sync + 'static,
    DB::Error: std::fmt::Debug + Send,
{
    let start = Instant::now();
    let mut storage_tries: HashMap<B256, StateTrie<DB>> = HashMap::new();
    let mut update_count: usize = 0;

    loop {
        let msg = match rx.recv() {
            Ok(m) => m,
            // Channel disconnected — treat as an implicit Finish.
            Err(_) => break,
        };

        match msg {
            StorageTrieMsg::Update { hashed_address, slots } => {
                // Lazily build / retrieve the storage trie for this address.
                let trie = match storage_tries.entry(hashed_address) {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let trie = build_storage_trie(
                            hashed_address,
                            &path_db,
                            difflayers.as_ref(),
                            prefetcher.as_deref(),
                        )?;
                        e.insert(trie)
                    }
                };

                // Apply slot updates.
                for (hashed_slot, new_value) in slots {
                    if let Some(value) = new_value {
                        trie.update_storage_u256_with_hash_state(hashed_address, hashed_slot, value)
                            .map_err(|e| {
                                TrieDBError::Database(format!(
                                    "streaming: failed to update slot 0x{} for addr 0x{}: {}",
                                    alloy_primitives::hex::encode(hashed_slot),
                                    alloy_primitives::hex::encode(hashed_address),
                                    e
                                ))
                            })?;
                    } else {
                        trie.delete_storage_with_hash_state(hashed_address, hashed_slot)
                            .map_err(|e| {
                                TrieDBError::Database(format!(
                                    "streaming: failed to delete slot 0x{} for addr 0x{}: {}",
                                    alloy_primitives::hex::encode(hashed_slot),
                                    alloy_primitives::hex::encode(hashed_address),
                                    e
                                ))
                            })?;
                    }
                    update_count += 1;
                }
            }
            StorageTrieMsg::Finish => break,
        }
    }

    let apply_elapsed = start.elapsed();
    debug!(
        target: "triedb::streaming",
        accounts = storage_tries.len(),
        updates = update_count,
        apply_ms = apply_elapsed.as_millis(),
        "streaming storage trie: applied all updates, hashing in parallel"
    );

    // Hash all storage tries in parallel.
    let hash_start = Instant::now();
    let entries: Vec<(B256, StateTrie<DB>)> = storage_tries.into_iter().collect();

    let hashed: Vec<(B256, B256, StateTrie<DB>)> = entries
        .into_par_iter()
        .map(|(addr, mut trie)| {
            let root = trie.hash();
            (addr, root, trie)
        })
        .collect();

    let mut storage_roots = HashMap::with_capacity(hashed.len());
    let mut result_tries = HashMap::with_capacity(hashed.len());
    for (addr, root, trie) in hashed {
        storage_roots.insert(addr, root);
        result_tries.insert(addr, trie);
    }

    debug!(
        target: "triedb::streaming",
        accounts = storage_roots.len(),
        hash_ms = hash_start.elapsed().as_millis(),
        total_ms = start.elapsed().as_millis(),
        "streaming storage trie: hashing complete"
    );

    Ok(PrecomputedStorageResult { storage_roots, storage_tries: result_tries })
}

/// Resolves and constructs a `StateTrie` for the given account's storage.
///
/// The storage root is determined by consulting (in order):
/// 1. The prefetcher's pre-built storage tries.
/// 2. The prefetcher's cached storage roots.
/// 3. The diff layers.
/// 4. The path database.
/// 5. `EMPTY_ROOT_HASH` as the final fallback.
fn build_storage_trie<DB>(
    hashed_address: B256,
    path_db: &DB,
    difflayers: Option<&DiffLayers>,
    prefetcher: Option<&TrieDBPrefetchState<DB>>,
) -> Result<StateTrie<DB>, TrieDBError>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    // 1. Try a pre-built trie from the prefetcher.
    if let Some(p) = prefetcher {
        if let Some(trie) = p.storage_tries.get(&hashed_address) {
            return Ok(trie.clone());
        }
    }

    // 2. Resolve the storage root.
    let storage_root = resolve_storage_root(hashed_address, path_db, difflayers, prefetcher)?;

    // 3. Build via SecureTrieBuilder.
    let id = SecureTrieId::new(storage_root).with_owner(hashed_address);
    SecureTrieBuilder::new(path_db.clone())
        .with_id(id)
        .build_with_difflayer(difflayers)
        .map_err(|e| {
            TrieDBError::Database(format!(
                "streaming: failed to build storage trie for addr 0x{}: {}",
                alloy_primitives::hex::encode(hashed_address),
                e
            ))
        })
}

/// Resolves the current storage root for an account, without building the trie.
fn resolve_storage_root<DB>(
    hashed_address: B256,
    path_db: &DB,
    difflayers: Option<&DiffLayers>,
    prefetcher: Option<&TrieDBPrefetchState<DB>>,
) -> Result<B256, TrieDBError>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    // Prefetcher cached roots.
    if let Some(p) = prefetcher {
        if let Some(root) = p.storage_roots.get(&hashed_address) {
            return Ok(*root);
        }
    }

    // Diff layers.
    if let Some(dl) = difflayers {
        if let Some(root) = dl.get_storage_root(hashed_address) {
            return Ok(root);
        }
    }

    // Path database.
    path_db
        .get_storage_root(hashed_address)
        .map_err(|e| {
            TrieDBError::Database(format!(
                "streaming: failed to get storage root for addr 0x{}: {:?}",
                alloy_primitives::hex::encode(hashed_address),
                e
            ))
        })
        .map(|opt| opt.unwrap_or(EMPTY_ROOT_HASH))
}

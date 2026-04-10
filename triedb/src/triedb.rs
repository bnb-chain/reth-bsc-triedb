//! Trie database implementation.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::B256;
use alloy_trie::EMPTY_ROOT_HASH;

use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::DiffLayers;
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieBuilder};

use crate::triedb_metrics::TrieDBMetrics;
use crate::triedb_reth::TrieDBPrefetchState;
use crate::triedb_manager::take_cached_account_trie_root;

/// Error type for trie database operations
#[derive(Debug, thiserror::Error)]
pub enum TrieDBError {
    #[error("Database operation failed: {0}")]
    Database(String),
    
    #[error("Invalid data format: {0}")]
    InvalidData(String),
    
    #[error("Operation not supported: {0}")]
    NotSupported(String),
    
    #[error("State trie error: {0}")]
    StateTrie(#[from] rust_eth_triedb_state_trie::secure_trie::SecureTrieError),
}

/// Ethereum-compatible trie database implementation for managing state and storage tries.
///
/// `TrieDB` is the main structure for managing Ethereum state data, including the
/// account trie (state trie) and individual storage tries for each account. It
/// provides a unified interface for reading and writing account and storage data
/// while maintaining consistency across multiple tries.
///
/// This structure is fully compatible with Ethereum's state management model and
/// can be used with Ethereum-compatible blockchain networks, including BSC and
/// other EVM-compatible chains.
///
/// # Architecture
///
/// The `TrieDB` maintains:
/// - One account trie (state trie) that stores all account data
/// - Multiple storage tries, one per account that has storage
/// - Caching mechanisms to optimize access to frequently used tries
/// - Diff layers for tracking uncommitted state changes
///
/// # Type Parameters
///
/// * `DB` - The database type that implements `TrieDatabase`. This provides the
///   storage backend for persisting and retrieving trie nodes.
///
/// # ⚠️ Important: Initialization Required
///
/// **Before using `TrieDB` for any operations, you MUST call [`state_at`](Self::state_at)**
/// to reset the `TrieDB` state and build the `account_trie`.
///
/// The `account_trie` field is `Option<StateTrie<DB>>` and starts as `None` when
/// `TrieDB` is created. Without calling `state_at`, all operations that depend on
/// the account trie will panic (e.g., `get_account`, `get_storage`, etc.).
///
/// ## Usage Pattern
///
/// ```ignore
/// // 1. Create a new TrieDB instance
/// let mut triedb = TrieDB::new(path_db);
///
/// // 2. ⚠️ REQUIRED: Initialize with a root hash and optional diff layer
/// triedb.state_at(root_hash, difflayer)?;
///
/// // 3. Now you can safely use query or batch operations
/// let account = triedb.get_account(address)?;
/// triedb.batch_update_and_commit(...)?;
/// ```
///
/// The `state_at` method:
/// - Resets the `TrieDB` to the specified state root hash
/// - Builds and initializes the `account_trie` from the root hash
/// - Clears all cached storage tries and account data
/// - Sets up the diff layer for tracking changes
/// - Must be called before any read or write operations
pub struct TrieDB<DB> 
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// The root hash of the current account trie (state trie).
    ///
    /// This represents the root hash of the entire Ethereum state at a specific
    /// point in time. Any change to any account or storage will result in a
    /// different root hash.
    pub(crate) root_hash: B256,
    
    /// The account trie (state trie) instance.
    ///
    /// This is the main trie that stores all Ethereum accounts. Each account's
    /// data (nonce, balance, storage root, code hash) is stored in this trie,
    /// keyed by the Keccak-256 hash of the account address.
    ///
    /// This field is `Option` because the trie may not be initialized until
    /// `state_at` is called with a specific root hash.
    pub(crate) account_trie: Option<StateTrie<DB>>,
    
    /// A cache of storage trie instances, keyed by account address hash.
    ///
    /// This map stores `StateTrie` instances for accounts that have been accessed
    /// or modified. The key is the Keccak-256 hash of the account address (`B256`),
    /// and the value is the `StateTrie` instance for that account's storage.
    ///
    /// **Purpose**: This cache allows efficient access to storage tries without
    /// recreating them on every access. When a storage trie is first accessed,
    /// it is created and stored here for subsequent operations.
    ///
    /// **Difference from `accounts_with_storage_trie`**: This field stores the
    /// actual trie objects (`StateTrie`) that can be used to read/write storage
    /// data, while `accounts_with_storage_trie` stores only the account metadata
    /// (`StateAccount`) for tracking purposes.
    pub(crate) storage_tries: HashMap<B256, StateTrie<DB>>,
    
    /// A cache of account data for accounts that have storage tries.
    ///
    /// This map stores `StateAccount` instances for accounts whose storage has
    /// been accessed or modified. The key is the Keccak-256 hash of the account
    /// address (`B256`), and the value is the complete account data.
    ///
    /// **Purpose**: This cache tracks which accounts have storage operations and
    /// maintains their account state (including the storage root) for efficient
    /// updates during commit operations.
    ///
    /// **Difference from `storage_tries`**: This field stores account metadata
    /// (`StateAccount`) including nonce, balance, storage root, and code hash,
    /// while `storage_tries` stores the actual trie objects (`StateTrie`) used
    /// for storage operations. When committing changes, this field helps track
    /// which accounts need their storage roots updated.
    ///
    /// **Usage**: This is primarily used during batch updates to track accounts
    /// that have storage modifications, ensuring their storage roots are correctly
    /// updated in the account trie when committing.
    pub(crate) accounts_with_storage_trie: HashMap<B256, StateAccount>,
    
    /// A map tracking updated storage root hashes for accounts.
    ///
    /// This map stores the new storage root hashes for accounts whose storage
    /// has been modified. The key is the Keccak-256 hash of the account address,
    /// and the value is the new storage root hash.
    ///
    /// **Purpose**: When storage is modified, the storage trie's root hash changes.
    /// This map tracks these changes so that the account's `storage_root` field
    /// can be updated in the account trie during commit operations.
    pub(crate) updated_storage_roots: Box<HashMap<B256, B256>>,
    
    /// Uncommitted diff layers for tracking state changes.
    ///
    /// This contains a collection of `DiffLayer` instances, one per uncommitted
    /// block, that track all trie node and storage root modifications before they
    /// are persisted to the database.
    pub(crate) difflayer: Option<DiffLayers>,
    
    /// The underlying database instance for storing and retrieving trie nodes.
    ///
    /// This database provides the persistent storage backend for all trie operations.
    pub(crate) path_db: DB,

    /// The prefetch state of the trie db
    ///
    /// This is used to store the prefetched state of the trie db.
    pub(crate) prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>,
    
    /// Metrics for monitoring trie database operations and performance.
    pub(crate) metrics: TrieDBMetrics,
}

/// External Initializer and getters 
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Creates a new trie database
    pub fn new(path_db: DB) -> Self {
        Self {
            root_hash: EMPTY_ROOT_HASH,
            account_trie: None,
            storage_tries: HashMap::new(),
            accounts_with_storage_trie: HashMap::new(),
            updated_storage_roots: Box::new(HashMap::new()),
            difflayer: None,
            path_db: path_db.clone(),
            prefetcher: None,
            metrics: TrieDBMetrics::new_with_labels(&[("instance", "default")]),
        }
    }

    /// Reset the state of the trie db to the given root hash and difflayer.
    ///
    /// Account trie source priority: cached root > prefetcher > build from PathDB.
    pub fn state_at(&mut self, root_hash: B256, difflayer: Option<&DiffLayers>, prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> Result<(), TrieDBError> {
        self.prefetcher = prefetcher;

        if let Some(cached_root) = take_cached_account_trie_root(root_hash) {
            let id = SecureTrieId::new(root_hash);
            self.account_trie = Some(
                StateTrie::new_from_cached_root(id, cached_root, self.path_db.clone(), difflayer)
            );
            tracing::debug!(
                target: "triedb::timing",
                root_hash = %root_hash,
                has_prefetcher = self.prefetcher.is_some(),
                "state_at: reused cached account trie root"
            );
        } else if let Some(prefetcher) = &self.prefetcher {
            self.account_trie = Some(prefetcher.account_trie.clone());
        } else {
            let id = SecureTrieId::new(root_hash);
            self.account_trie = Some(
                SecureTrieBuilder::new(self.path_db.clone())
                .with_id(id)
                .build_with_difflayer(difflayer)?
            );
        }
        self.root_hash = root_hash;
        self.updated_storage_roots.clear();
        self.difflayer = difflayer.map(|d| d.clone());
        self.storage_tries.clear();
        self.accounts_with_storage_trie.clear();
        Ok(())
    }

    /// Gets a mutable reference to the database
    pub fn get_mut_path_db_ref(&mut self) -> &mut DB {
        &mut self.path_db
    }

    /// Clean the trie db
    /// 
    /// This method resets all fields immediately and asynchronously releases
    /// Arc references in a background thread to avoid blocking.
    pub fn clean(&mut self)
    where
        DB: 'static,
    {
        // Move out values that contain Arc references for async cleanup
        let account_trie = std::mem::take(&mut self.account_trie);
        let storage_tries = std::mem::take(&mut self.storage_tries);
        let accounts_with_storage_trie = std::mem::take(&mut self.accounts_with_storage_trie);
        let updated_storage_roots = std::mem::take(&mut self.updated_storage_roots);
        let difflayer = std::mem::take(&mut self.difflayer);
        let prefetcher = std::mem::take(&mut self.prefetcher);
        
        // Reset simple fields immediately
        self.root_hash = EMPTY_ROOT_HASH;
        
        // Offload Arc-reference drops to the rayon global thread pool to avoid
        // blocking the caller. Unlike std::thread::spawn, rayon::spawn reuses
        // existing worker threads and avoids per-block OS thread creation.
        rayon::spawn(move || {
            drop(prefetcher);
            drop(account_trie);
            drop(storage_tries);
            drop(accounts_with_storage_trie);
            drop(updated_storage_roots);
            drop(difflayer);
        });
    }
}

impl<DB> Clone for TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    fn clone(&self) -> Self {
        Self {
            root_hash: EMPTY_ROOT_HASH,
            account_trie: None,
            storage_tries: HashMap::new(),
            accounts_with_storage_trie: HashMap::new(),
            updated_storage_roots: Box::new(HashMap::new()),
            difflayer: None,
            path_db: self.path_db.clone(),
            prefetcher: None,
            metrics: self.metrics.clone()
        }
    }
}

impl<DB> std::fmt::Debug for TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync + std::fmt::Debug,
    DB::Error: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrieDB")
            .field("root_hash", &self.root_hash)
            .field("account_trie", &format!("<StateTrie<{}>>", std::any::type_name::<DB>()))
            .field("storage_tries_count", &self.storage_tries.len())
            .field("accounts_with_storage_trie_count", &self.accounts_with_storage_trie.len())
            .field("updated_storage_roots_count", &self.updated_storage_roots.len())
            .field("difflayer", &self.difflayer.as_ref().map(|_| "<Difflayer>"))
            .field("db", &format!("<{}>", std::any::type_name::<DB>()))
            .finish()
    }
}










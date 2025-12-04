//! Database traits for trie operations.

use std::sync::Arc;
use alloy_primitives::B256;
use auto_impl::auto_impl;
use crate::difflayer::DiffLayer;

/// A trait defining the interface for trie database operations.
///
/// This trait provides a unified abstraction for interacting with trie databases,
/// allowing implementations to work with different storage backends (e.g., RocksDB,
/// in-memory caches, or other key-value stores) while maintaining a consistent API.
///
/// The trait supports both state trie nodes and storage trie roots, enabling
/// efficient storage and retrieval of Ethereum state data. It also provides
/// methods for committing diff layers, which represent incremental state changes
/// across multiple blocks.
///
/// # Type Parameters
///
/// * `Error` - The error type returned by database operations. Implementations
///   should define their own error types that capture backend-specific failures.
///
/// # Auto-implementation
///
/// This trait is automatically implemented for `Box<T>`, `Arc<T>`, and `Clone`
/// where `T: TrieDatabase`, making it easy to share database instances across
/// threads and compose different database implementations.
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) to allow concurrent
/// access from multiple threads. The `auto_impl` attribute ensures that
/// wrapped implementations maintain thread safety.
#[auto_impl(Box, Arc, Clone, Send + Sync + Debug + Unpin + 'static)]
pub trait TrieDatabase {
    /// The error type returned by database operations.
    ///
    /// This type should capture all possible errors that can occur during
    /// database operations, such as I/O errors, serialization failures,
    /// or backend-specific errors.
    type Error;

    /// Retrieves a trie node from the database by its path.
    ///
    /// The path is a byte sequence that uniquely identifies the location
    /// of the node within the trie structure. This method returns the encoded
    /// node data if found, or `None` if the node does not exist.
    ///
    /// # Arguments
    ///
    /// * `path` - A byte slice representing the path to the trie node in the
    ///   trie structure. The path typically corresponds to the nibble path
    ///   from the root to the target node.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(data))` - The node was found and `data` contains the encoded
    ///   node data (RLP-encoded or similar format).
    /// * `Ok(None)` - The node does not exist in the database.
    /// * `Err(error)` - An error occurred during the database lookup.
    ///
    /// # Errors
    ///
    /// This method may return errors related to database I/O, serialization,
    /// or backend-specific failures.
    fn get_trie_node(&self, path: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Inserts or updates a trie node in the database.
    ///
    /// This method stores the encoded node data at the specified path. If a
    /// node already exists at this path, it will be overwritten with the new data.
    ///
    /// # Arguments
    ///
    /// * `path` - A byte slice representing the path where the node should be stored.
    /// * `data` - The encoded node data to store. This is typically RLP-encoded
    ///   or in another format specific to the trie implementation.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The node was successfully stored.
    /// * `Err(error)` - An error occurred during the database write operation.
    ///
    /// # Errors
    ///
    /// This method may return errors related to database I/O, serialization,
    /// or backend-specific write failures.
    fn insert_trie_node(&self, path: &[u8], data: Vec<u8>) -> Result<(), Self::Error>;

    /// Checks whether a trie node exists in the database.
    ///
    /// This method performs a lightweight existence check without retrieving
    /// the full node data, which can be more efficient than calling
    /// `get_trie_node` when only the presence of the node is needed.
    ///
    /// # Arguments
    ///
    /// * `path` - A byte slice representing the path to check.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - The node exists in the database.
    /// * `Ok(false)` - The node does not exist in the database.
    /// * `Err(error)` - An error occurred during the database lookup.
    ///
    /// # Errors
    ///
    /// This method may return errors related to database I/O or backend-specific
    /// failures.
    fn contains_trie_node(&self, path: &[u8]) -> Result<bool, Self::Error>;

    /// Removes a trie node from the database.
    ///
    /// This method deletes the node at the specified path. If the node does
    /// not exist, this operation is a no-op and does not return an error.
    ///
    /// # Arguments
    ///
    /// * `path` - A byte slice representing the path of the node to remove.
    ///
    /// # Note
    ///
    /// Unlike other methods in this trait, this method does not return a `Result`.
    /// Implementations should handle errors internally, typically by logging them
    /// or ignoring them if the node doesn't exist. This design choice allows for
    /// simpler error handling in common use cases where node deletion failures
    /// are not critical.
    fn remove_trie_node(&self, path: &[u8]);

    /// Retrieves the storage trie root for a given account address.
    ///
    /// Each Ethereum account has its own storage trie, and this method retrieves
    /// the root hash of that storage trie. The root hash uniquely identifies the
    /// current state of the account's storage.
    ///
    /// # Arguments
    ///
    /// * `hased_address` - The Keccak-256 hash of the account address (`B256`).
    ///   This is used as the key to look up the storage root.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(root))` - The storage root was found for the given address.
    /// * `Ok(None)` - No storage root exists for the given address (the account
    ///   may not exist or may have empty storage).
    /// * `Err(error)` - An error occurred during the database lookup.
    ///
    /// # Errors
    ///
    /// This method may return errors related to database I/O or backend-specific
    /// failures.
    fn get_storage_root(&self, hased_address: B256) -> Result<Option<B256>, Self::Error>;
    
    /// Inserts or updates a storage root in the database.
    ///
    /// This method stores the storage root at the specified address. If a
    /// storage root already exists for this address, it will be overwritten with the new value.
    ///
    /// # Arguments
    ///
    /// * `hased_address` - The Keccak-256 hash of the account address (`B256`).
    ///   This is used as the key to store the storage root.
    /// * `value` - The storage root hash (`B256`) to store.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The storage root was successfully stored.
    /// * `Err(error)` - An error occurred during the database write operation.
    ///
    /// # Errors
    ///
    /// This method may return errors related to database I/O or backend-specific
    /// failures.
    fn put_storage_root(&self, hased_address: B256, value: B256) -> Result<(), Self::Error>;

    /// Commits a diff layer to the database, persisting state changes for a block.
    ///
    /// This method is responsible for atomically writing all state changes
    /// represented by a `DiffLayer` to persistent storage. It typically
    /// writes both trie nodes and storage roots, and updates the latest
    /// persisted state metadata.
    ///
    /// # Arguments
    ///
    /// * `block_number` - The block number associated with this diff layer.
    ///   This is used to track the latest persisted block.
    /// * `state_root` - The state root hash (`B256`) for this block. This
    ///   represents the root of the entire state trie after applying all
    ///   changes in the diff layer.
    /// * `difflayer` - An optional reference to the `DiffLayer` containing
    ///   all trie node and storage root changes for this block. If `None`,
    ///   this may represent a state where no changes occurred or the layer
    ///   should be cleared.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The diff layer was successfully committed to persistent storage.
    /// * `Err(error)` - An error occurred during the commit operation.
    ///
    /// # Errors
    ///
    /// This method may return errors related to:
    /// - Database I/O failures
    /// - Transaction/atomicity failures
    /// - Serialization errors
    /// - Backend-specific commit failures
    ///
    /// # Implementation Note
    ///
    /// Implementations should ensure that this operation is atomic. Either all
    /// changes in the diff layer are persisted, or none are. This is critical
    /// for maintaining database consistency.
    fn commit_difflayer(&self, block_number: u64, state_root: B256, difflayer: &Option<Arc<DiffLayer>>) -> Result<(), Self::Error>;

    /// Retrieves the latest persisted state information from the database.
    ///
    /// This method returns the block number and state root of the most recent
    /// block that has been fully committed to persistent storage. This information
    /// is used to track synchronization progress and determine which blocks
    /// have been safely persisted.
    ///
    /// # Returns
    ///
    /// * `Ok((block_number, state_root))` - The latest persisted block number
    ///   and its corresponding state root hash.
    /// * `Err(error)` - An error occurred during the database lookup.
    ///
    /// # Errors
    ///
    /// This method may return errors if:
    /// - The database is empty or uninitialized
    /// - The metadata cannot be read
    /// - Backend-specific errors occur
    ///
    /// # Note
    ///
    /// If no state has been persisted yet, implementations should return an
    /// appropriate error or a default value (e.g., block 0 with empty root).
    fn latest_persist_state(&self) -> Result<(u64, B256), Self::Error>;

    /// Clears all cached data in the database implementation.
    ///
    /// This method invalidates any internal caches maintained by the database
    /// implementation. After calling this method, subsequent operations will
    /// read directly from the underlying storage, ensuring that stale cached
    /// data does not affect queries.
    ///
    /// # Use Cases
    ///
    /// This method is typically called:
    /// - After committing state changes to ensure cache consistency
    /// - When switching between different state views
    /// - During testing or debugging to ensure fresh data reads
    /// - When memory pressure requires cache eviction
    ///
    /// # Note
    ///
    /// This method only clears implementation-specific caches. It does not
    /// affect the persistent storage or committed data. The behavior is
    /// implementation-dependent, and some implementations may be no-ops if
    /// they don't maintain caches.
    fn clear_cache(&self);
}

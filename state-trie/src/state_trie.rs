// ! State trie implementation for secure trie operations.

use alloy_primitives::{Address, B256, U256, keccak256};
use std::{sync::Arc};

use alloy_rlp::{Encodable, Decodable};
use rust_eth_triedb_common::TrieDatabase;

use super::account::StateAccount;
use super::secure_trie::{SecureTrieId, SecureTrieError};
use super::traits::SecureTrieTrait;
use super::trie::Trie;
use super::node::{NodeSet, DiffLayers};
use super::node::rlp_raw;

/// RLP-encodes a `U256` into a fixed-size stack buffer and returns the written slice.
///
/// This matches `alloy_rlp::encode(&value)` for all `U256` values, but avoids heap
/// allocations on the hot path.
///
/// Output length is at most 33 bytes: 1-byte header + up to 32 bytes payload.
fn rlp_encode_u256_into<'a>(value: &U256, out: &'a mut [u8; 33]) -> &'a [u8] {
    // Convert to minimal big-endian bytes (no leading zeros).
    let be = value.to_be_bytes::<32>();
    let first_nonzero = be.iter().position(|b| *b != 0);
    let payload = match first_nonzero {
        None => &be[32..32], // empty slice => zero
        Some(i) => &be[i..],
    };

    match payload.len() {
        0 => {
            // RLP empty string (used for integer zero)
            out[0] = 0x80;
            &out[..1]
        }
        1 if payload[0] < 0x80 => {
            // Single byte whose value is in [0x00, 0x7f] is its own RLP encoding.
            out[0] = payload[0];
            &out[..1]
        }
        len => {
            debug_assert!(len <= 32);
            // Short string: 0x80 + len, followed by payload.
            out[0] = 0x80u8 + (len as u8);
            out[1..1 + len].copy_from_slice(payload);
            &out[..1 + len]
        }
    }
}

/// Ethereum-compatible state trie implementation with secure key hashing.
///
/// `StateTrie` is a high-level wrapper around the underlying `Trie` structure that
/// provides secure key hashing functionality. It implements the Ethereum state trie
/// specification, where all keys (addresses and storage keys) are hashed using
/// Keccak-256 before being stored in the trie.
///
/// This structure is fully compatible with Ethereum's state trie implementation
/// and can be used to manage state for Ethereum-compatible blockchain networks,
/// including BSC and other EVM-compatible chains.
///
/// # Key Features
///
/// - **Secure Key Hashing**: All keys are automatically hashed using Keccak-256
///   before being used in trie operations, ensuring uniform key distribution and
///   security.
/// - **Account Management**: Provides methods to get, update, and delete Ethereum
///   accounts from the state trie.
/// - **Storage Management**: Supports reading and writing storage values for accounts,
///   with automatic key hashing.
/// - **State Tracking**: Maintains a unique identifier (`SecureTrieId`) that tracks
///   the state root and owner of the trie.
///
/// # Architecture
///
/// `StateTrie` wraps a `Trie<DB>` instance and adds a `SecureTrieId` for identification.
/// The underlying trie handles the actual Merkle Patricia Trie operations, while
/// `StateTrie` provides the Ethereum-specific interface and key hashing.
///
/// # Thread Safety
///
/// This structure is `Clone`, allowing it to be shared across threads. However,
/// mutable operations are not thread-safe and should be protected by appropriate
/// synchronization primitives if used in concurrent contexts.
///
/// # Type Parameters
///
/// * `DB` - The database type that implements `TrieDatabase`. This provides the
///   storage backend for persisting and retrieving trie nodes.
#[derive(Clone)]
pub struct StateTrie<DB> {
    /// The underlying trie structure that handles Merkle Patricia Trie operations.
    ///
    /// This trie stores all the actual node data and performs the core trie
    /// operations (get, update, delete, commit, etc.). The `StateTrie` wrapper
    /// adds key hashing and Ethereum-specific semantics on top of this base trie.
    trie: Trie<DB>,
    
    /// The unique identifier for this state trie instance.
    ///
    /// This identifier combines the state root hash and an optional owner address,
    /// allowing the system to distinguish between different trie instances and
    /// track the current state of the trie.
    id: SecureTrieId,
}

impl<DB> std::fmt::Debug for StateTrie<DB>
where
    DB: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateTrie")
            // .field("trie", &self.trie)
            .field("id", &self.id)
            .finish()
    }
}

impl<DB> StateTrie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Creates a new state trie with the given identifier and database
    pub fn new(id: SecureTrieId, database: DB, difflayer: Option<&DiffLayers>) -> Result<Self, SecureTrieError> {
        let trie = Trie::new(&id, database, difflayer)?;
        Ok(Self { trie, id })
    }

    /// Returns the identifier of this state trie
    pub fn id(&self) -> &SecureTrieId {
        &self.id
    }

    /// Returns a reference to the underlying trie
    pub fn trie(&self) -> &Trie<DB> {
        &self.trie
    }

    /// Returns a mutable reference to the underlying trie
    pub fn trie_mut(&mut self) -> &mut Trie<DB> {
        &mut self.trie
    }

    /// Creates a copy of this state trie
    pub fn copy(&self) -> Self {
        Self {
            trie: self.trie.clone(),
            id: self.id.clone(),
        }
    }

    /// Hashes a key using keccak256
    pub fn hash_key(&self, key: &[u8]) -> B256 {
        keccak256(key)
    }
}

impl<DB> SecureTrieTrait for StateTrie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    type Error = SecureTrieError;

    fn id(&self) -> &SecureTrieId {
        &self.id
    }

    fn get_account(&mut self, address: Address) -> Result<Option<StateAccount>, Self::Error> {
        let hashed_address = self.hash_key(address.as_slice());
        if let Some(data) = self.trie.get(hashed_address.as_slice())? {
            let account = StateAccount::decode(&mut &data[..])
                .map_err(|_| SecureTrieError::InvalidAccount)?;
            Ok(Some(account))
        } else {
            Ok(None)
        }
    }

    fn update_account(&mut self, address: Address, account: &StateAccount) -> Result<(), Self::Error> {
        let hashed_address = self.hash_key(address.as_slice());
        let mut encoded_account = Vec::new();
        account.encode(&mut encoded_account);
        self.trie.update(hashed_address.as_slice(), &encoded_account)?;
        Ok(())
    }

    fn delete_account(&mut self, address: Address) -> Result<(), Self::Error> {
        let hashed_address = self.hash_key(address.as_slice());
        self.trie.delete(hashed_address.as_slice())?;
        Ok(())
    }

    fn get_storage(&mut self, _address: Address, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let hashed_key = self.hash_key(key);
        let enc = self.trie.get(hashed_key.as_slice())?;

        if enc.is_none() {
            return Ok(None);
        }

        let enc = enc.unwrap();
        if enc.is_empty() {
            return Ok(None);
        }

        // Extract the RLP string/content. Map any raw-RLP error to our domain error.
        let (_, value, _) = rlp_raw::split(&enc).map_err(|_| SecureTrieError::InvalidStorage)?;
        Ok(Some(value.to_vec()))
    }

    fn update_storage(&mut self, _address: Address, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let hashed_key = self.hash_key(key);
        let encoded_value = alloy_rlp::encode(value);
        self.trie.update(hashed_key.as_slice(), &encoded_value)?;
        Ok(())
    }

    fn delete_storage(&mut self, _address: Address, key: &[u8]) -> Result<(), Self::Error> {
        let hashed_key = self.hash_key(key);
        self.trie.delete(hashed_key.as_slice())?;
        Ok(())
    }

    fn get_account_with_hash_state(&mut self, hashed_address: B256) -> Result<Option<StateAccount>, Self::Error> {
        if let Some(data) = self.trie.get(hashed_address.as_slice())? {
            let account = StateAccount::decode(&mut &data[..])
                .map_err(|_| SecureTrieError::InvalidAccount)?;
            Ok(Some(account))
        } else {
            Ok(None)
        }
    }

    fn touch_account_with_hash_state(&mut self, hashed_address: B256) -> Result<(), Self::Error> {
        self.trie.get(hashed_address.as_slice())?;
        Ok(())
    }

    fn update_account_with_hash_state(&mut self, hashed_address: B256, account: &StateAccount) -> Result<(), Self::Error> {
        // Pre-allocate the exact RLP length to avoid repeated growth/reallocations on the hot path.
        let mut encoded_account = Vec::with_capacity(account.length());
        account.encode(&mut encoded_account);
        self.trie.update(hashed_address.as_slice(), &encoded_account)?;
        Ok(())
    }

    fn delete_account_with_hash_state(&mut self, hashed_address: B256) -> Result<(), Self::Error> {
        self.trie.delete(hashed_address.as_slice())?;
        Ok(())
    }

    fn get_storage_with_hash_state(&mut self, _: B256, hashed_key: B256) -> Result<Option<Vec<u8>>, Self::Error> {
        let enc = self.trie.get(hashed_key.as_slice())?;

        if enc.is_none() {
            return Ok(None);
        }

        let enc = enc.unwrap();
        if enc.is_empty() {
            return Ok(None);
        }

        // Extract the RLP string/content. Map any raw-RLP error to our domain error.
        let (_, value, _) = rlp_raw::split(&enc).map_err(|_| SecureTrieError::InvalidStorage)?;
        Ok(Some(value.to_vec()))
    }

    fn touch_storage_with_hash_state(&mut self, hashed_key: B256) -> Result<(), Self::Error> {
        self.trie.get(hashed_key.as_slice())?;
        Ok(())
    }

    fn update_storage_with_hash_state(&mut self, _: B256, hashed_key: B256, value: &[u8]) -> Result<(), Self::Error> {
        let encoded_value = alloy_rlp::encode(value);
        self.trie.update(hashed_key.as_slice(), &encoded_value)?;
        Ok(())
    }

    fn update_storage_u256_with_hash_state(&mut self, _: B256, hashed_key: B256, value: U256) -> Result<(), Self::Error> {
        let mut buf = [0u8; 33];
        let encoded = rlp_encode_u256_into(&value, &mut buf);
        self.trie.update(hashed_key.as_slice(), encoded)?;
        Ok(())
    }

    fn get_storage_u256_with_hash_state(&mut self, _: B256, hashed_key: B256) -> Result<Option<U256>, Self::Error> {
        let enc = self.trie.get(hashed_key.as_slice())?;

        if enc.is_none() {
            return Ok(None);
        }

        let enc = enc.unwrap();
        if enc.is_empty() {
            return Ok(None);
        }

        // Decode the U256 value from RLP
        let value_dec = U256::decode(&mut &enc[..]).map_err(|_| SecureTrieError::InvalidStorage)?;
        Ok(Some(value_dec))
    }
    
    fn delete_storage_with_hash_state(&mut self, _: B256, hashed_key: B256) -> Result<(), Self::Error> {
        self.trie.delete(hashed_key.as_slice())?;
        Ok(())
    }

    fn hash(&mut self) -> B256 {
        self.trie.hash()
    }

    fn commit(&mut self, _collect_leaf: bool) -> Result<(B256, Option<Arc<NodeSet>>), Self::Error> {
        self.trie.commit(_collect_leaf)
    }
}

/// Type alias for secure trie
pub type SecureTrie<DB> = StateTrie<DB>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rlp_encode_u256_matches_alloy_rlp() {
        // Representative edge cases and patterns.
        let cases = [
            U256::ZERO,
            U256::from(1u64),
            U256::from(0x7fu64),
            U256::from(0x80u64),
            U256::from(0xffu64),
            U256::from(0x0100u64),
            U256::from(0x1234u64),
            U256::from(0xabcdefu64),
            U256::MAX,
            // High-bit set with trailing zeros.
            U256::from_be_bytes([0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            // Alternating pattern.
            U256::from_be_bytes([0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55]),
        ];

        for value in cases {
            let mut buf = [0u8; 33];
            let ours = rlp_encode_u256_into(&value, &mut buf);
            let theirs = alloy_rlp::encode(&value);
            assert_eq!(ours, &theirs[..], "mismatch for value={value:?}");
        }
    }
}

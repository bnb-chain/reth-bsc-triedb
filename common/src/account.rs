//! State account structure and implementation.

use alloy_primitives::{B256, U256, keccak256};
use alloy_rlp::{Decodable, RlpDecodable, RlpEncodable};


/// Ethereum-compatible state account structure.
///
/// This structure represents an account in the Ethereum state trie, following
/// the standard Ethereum account format. It is fully compatible with Ethereum's
/// account encoding and can be used interchangeably with Ethereum-compatible
/// blockchain networks (including BSC, which is EVM-compatible).
///
/// The account data is stored in the state trie and encoded using RLP (Recursive
/// Length Prefix) encoding, which is the standard serialization format used by
/// Ethereum and EVM-compatible chains.
///
/// # Field Descriptions
///
/// - `nonce`: The number of transactions sent from this account (for EOA) or
///   the number of contract creations made by this account (for contract accounts).
/// - `balance`: The account's balance in wei (the smallest unit of Ether).
/// - `storage_root`: The root hash of the account's storage trie. For accounts
///   with no storage, this is `EMPTY_ROOT_HASH`.
/// - `code_hash`: The Keccak-256 hash of the account's contract code. For
///   externally owned accounts (EOA), this is `KECCAK_EMPTY`.
///
/// # RLP Encoding
///
/// The account is encoded as an RLP list containing four elements in order:
/// `[nonce, balance, storage_root, code_hash]`. This encoding format is identical
/// to Ethereum's account encoding, ensuring full compatibility.
///
/// # Compatibility
///
/// This structure is fully compatible with:
/// - Ethereum mainnet and testnets
/// - BSC (Binance Smart Chain) and other EVM-compatible chains
/// - Any blockchain that follows Ethereum's account model
#[derive(Copy, Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable)]
pub struct StateAccount {
    /// Account nonce - the number of transactions sent from this account.
    ///
    /// For externally owned accounts (EOA), this represents the transaction count.
    /// For contract accounts, this represents the number of contract creations.
    pub nonce: u64,
    
    /// Account balance in wei (the smallest unit of Ether).
    ///
    /// This is stored as a `U256` to support the full range of possible balances
    /// without overflow.
    pub balance: U256,
    
    /// Storage trie root hash for this account's storage.
    ///
    /// Each account has its own storage trie, and this field stores the root hash
    /// of that trie. For accounts with no storage (empty storage), this should be
    /// set to `EMPTY_ROOT_HASH`.
    pub storage_root: B256,
    
    /// Keccak-256 hash of the account's contract code.
    ///
    /// For externally owned accounts (EOA), this is `KECCAK_EMPTY` (hash of empty code).
    /// For contract accounts, this is the Keccak-256 hash of the deployed bytecode.
    pub code_hash: B256,
}

impl Default for StateAccount {
    fn default() -> Self {
        Self {
            nonce: 0,
            balance: U256::ZERO,
            storage_root: alloy_trie::EMPTY_ROOT_HASH,
            code_hash: alloy_trie::KECCAK_EMPTY,
        }
    }
}

impl StateAccount {
    /// Set custom nonce
    pub fn with_nonce(mut self, nonce: u64) -> Self {
        self.nonce = nonce;
        self
    }
    /// Set custom balance
    pub fn with_balance(mut self, balance: U256) -> Self {
        self.balance = balance;
        self
    }
    /// Set custom storage_root
    pub fn with_storage_root(mut self, storage_root: B256) -> Self {
        self.storage_root = storage_root;
        self
    }

    /// Set custom code_hash
    pub fn with_code_hash(mut self, code_hash: B256) -> Self {
        self.code_hash = code_hash;
        self
    }

    /// Compute  hash as committed to in the MPT trie without memorizing.
    pub fn trie_hash(&self) -> B256 {
        keccak256(self.to_rlp())
    }

    /// Encode the account as RLP.
    pub fn to_rlp(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    /// Decode a StateAccount from RLP encoded bytes
    pub fn from_rlp(data: &[u8]) -> Result<Self, alloy_rlp::Error> {
        StateAccount::decode(&mut &*data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Encodable;

    #[test]
    fn test_state_account_empty() {
        let account = StateAccount::default();
        let mut encoded = Vec::new();
        account.encode(&mut encoded);
        let encoded_hash = account.trie_hash();

        // the expected hash is from the BSC method
        let expected_hex = "0943e8ddb43403e237cc56ac8ec3e256006e0f75d8e79ca1457b123e5d51a45c";
        let actual_hex = format!("{:x}", encoded_hash);

        assert_eq!(actual_hex, expected_hex);

        let decoded_account = StateAccount::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded_account, account);
    }

    #[test]
    fn test_state_account_rlp_encode_and_decode() {
        let account = StateAccount::default()
        .with_nonce(99)
        .with_balance(U256::from(100))
        .with_storage_root(keccak256(b"test_account_storage_root_1"))
        .with_code_hash(keccak256(b"test_account_code_hash_1"));

        let mut encoded = Vec::new();
        account.encode(&mut encoded);
        let encoded_hash = account.trie_hash();

        // the expected hash is from the BSC method
        let expected_hex = "50ff7a13cd631ecb8098f811526d74d03c319f90ef01012930c6de21534cf4f6";
        let actual_hex = format!("{:x}", encoded_hash);

        assert_eq!(actual_hex, expected_hex);

        let decoded_account = StateAccount::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded_account, account);
    }
}

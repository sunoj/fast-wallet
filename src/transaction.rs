//! Fast transaction encoding with RLP
//!
//! This module provides high-performance transaction encoding using alloy-rlp.
//! It supports Legacy, EIP-2930, and EIP-1559 transactions.
//!
//! Optimizations:
//! - Thread-local buffer reuse to avoid allocations
//! - Inline hints for hot path functions

use crate::crypto::keccak256;
use crate::error::WalletResult;
use crate::signer::{FastSigner, RecoverableSignature};
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rlp::Encodable;
use std::cell::RefCell;

// Thread-local buffer for encoding to avoid repeated allocations
thread_local! {
    static ENCODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(512));
}

/// Transaction request (unsigned transaction parameters)
#[derive(Debug, Clone, Default)]
pub struct TransactionRequest {
    pub to: Option<Address>,
    pub value: U256,
    pub data: Bytes,
    pub gas_limit: u64,
    pub nonce: u64,
    pub chain_id: u64,
    // Legacy gas price
    pub gas_price: Option<U256>,
    // EIP-1559 gas parameters
    pub max_fee_per_gas: Option<U256>,
    pub max_priority_fee_per_gas: Option<U256>,
    // EIP-2930 access list
    pub access_list: Option<Vec<AccessListItem>>,
}

/// Access list item for EIP-2930
#[derive(Debug, Clone)]
pub struct AccessListItem {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

impl Encodable for AccessListItem {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        // Encode as [address, [storage_keys...]]
        let storage_len: usize = self.storage_keys.iter().map(|k| k.length()).sum();
        let storage_header_len = alloy_rlp::length_of_length(storage_len);

        let list_len = self.address.length() + storage_header_len + storage_len;

        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(out);

        self.address.encode(out);

        // Encode storage keys as a list
        alloy_rlp::Header {
            list: true,
            payload_length: storage_len,
        }
        .encode(out);

        for key in &self.storage_keys {
            key.encode(out);
        }
    }

    fn length(&self) -> usize {
        let storage_len: usize = self.storage_keys.iter().map(|k| k.length()).sum();
        let storage_header_len = alloy_rlp::length_of_length(storage_len);
        let list_len = self.address.length() + storage_header_len + storage_len;
        alloy_rlp::length_of_length(list_len) + list_len
    }
}

/// Typed transaction enum
#[derive(Debug, Clone)]
pub enum TypedTransaction {
    Legacy(LegacyTransaction),
    Eip2930(Eip2930Transaction),
    Eip1559(Eip1559Transaction),
}

/// Legacy transaction (pre-EIP-2718)
#[derive(Debug, Clone)]
pub struct LegacyTransaction {
    pub nonce: u64,
    pub gas_price: U256,
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Bytes,
    pub chain_id: u64,
}

/// EIP-2930 transaction (access list)
#[derive(Debug, Clone)]
pub struct Eip2930Transaction {
    pub chain_id: u64,
    pub nonce: u64,
    pub gas_price: U256,
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Bytes,
    pub access_list: Vec<AccessListItem>,
}

/// EIP-1559 transaction (dynamic fee)
#[derive(Debug, Clone)]
pub struct Eip1559Transaction {
    pub chain_id: u64,
    pub nonce: u64,
    pub max_priority_fee_per_gas: U256,
    pub max_fee_per_gas: U256,
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Bytes,
    pub access_list: Vec<AccessListItem>,
}

/// Signed transaction ready for broadcasting
#[derive(Debug, Clone)]
pub struct Transaction {
    pub typed_tx: TypedTransaction,
    pub signature: RecoverableSignature,
    /// Cached encoded bytes
    encoded: Option<Bytes>,
    /// Cached transaction hash
    hash: Option<B256>,
}

impl LegacyTransaction {
    /// Encode for signing (EIP-155)
    ///
    /// Uses thread-local buffer to minimize allocations in hot path.
    #[inline]
    pub fn encode_for_signing(&self) -> Vec<u8> {
        ENCODE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            self.encode_signing_fields(&mut buf);
            buf.clone()
        })
    }

    /// Encode signing fields to buffer
    #[inline]
    fn encode_signing_fields(&self, buf: &mut Vec<u8>) {
        // [nonce, gasPrice, gasLimit, to, value, data, chainId, 0, 0]
        let list_len = self.rlp_list_len_for_signing();
        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(buf);

        self.nonce.encode(buf);
        self.gas_price.encode(buf);
        self.gas_limit.encode(buf);
        encode_to(self.to, buf);
        self.value.encode(buf);
        self.data.encode(buf);
        self.chain_id.encode(buf);
        0u8.encode(buf);
        0u8.encode(buf);
    }

    fn rlp_list_len_for_signing(&self) -> usize {
        self.nonce.length()
            + self.gas_price.length()
            + self.gas_limit.length()
            + to_length(self.to)
            + self.value.length()
            + self.data.length()
            + self.chain_id.length()
            + 1 // 0
            + 1 // 0
    }

    /// Encode signed transaction
    pub fn encode_signed(&self, sig: &RecoverableSignature) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        let list_len = self.rlp_list_len_signed(sig);
        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(&mut buf);

        self.nonce.encode(&mut buf);
        self.gas_price.encode(&mut buf);
        self.gas_limit.encode(&mut buf);
        encode_to(self.to, &mut buf);
        self.value.encode(&mut buf);
        self.data.encode(&mut buf);
        sig.v.encode(&mut buf);
        sig.r.encode(&mut buf);
        sig.s.encode(&mut buf);

        buf
    }

    fn rlp_list_len_signed(&self, sig: &RecoverableSignature) -> usize {
        self.nonce.length()
            + self.gas_price.length()
            + self.gas_limit.length()
            + to_length(self.to)
            + self.value.length()
            + self.data.length()
            + sig.v.length()
            + sig.r.length()
            + sig.s.length()
    }
}

impl Eip1559Transaction {
    /// Encode for signing (no type prefix, just the list)
    ///
    /// Uses thread-local buffer to minimize allocations.
    #[inline]
    pub fn encode_for_signing(&self) -> Vec<u8> {
        ENCODE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            // Type 2 prefix
            buf.push(0x02);
            self.encode_fields(&mut buf);
            buf.clone()
        })
    }

    #[inline]
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        let list_len = self.rlp_list_len();
        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(buf);

        self.chain_id.encode(buf);
        self.nonce.encode(buf);
        self.max_priority_fee_per_gas.encode(buf);
        self.max_fee_per_gas.encode(buf);
        self.gas_limit.encode(buf);
        encode_to(self.to, buf);
        self.value.encode(buf);
        self.data.encode(buf);
        encode_access_list(&self.access_list, buf);
    }

    fn rlp_list_len(&self) -> usize {
        self.chain_id.length()
            + self.nonce.length()
            + self.max_priority_fee_per_gas.length()
            + self.max_fee_per_gas.length()
            + self.gas_limit.length()
            + to_length(self.to)
            + self.value.length()
            + self.data.length()
            + access_list_length(&self.access_list)
    }

    /// Encode signed transaction
    pub fn encode_signed(&self, sig: &RecoverableSignature) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        // Type 2 prefix
        buf.push(0x02);

        let list_len = self.rlp_list_len_signed(sig);
        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(&mut buf);

        self.chain_id.encode(&mut buf);
        self.nonce.encode(&mut buf);
        self.max_priority_fee_per_gas.encode(&mut buf);
        self.max_fee_per_gas.encode(&mut buf);
        self.gas_limit.encode(&mut buf);
        encode_to(self.to, &mut buf);
        self.value.encode(&mut buf);
        self.data.encode(&mut buf);
        encode_access_list(&self.access_list, &mut buf);
        sig.v.encode(&mut buf);
        sig.r.encode(&mut buf);
        sig.s.encode(&mut buf);

        buf
    }

    fn rlp_list_len_signed(&self, sig: &RecoverableSignature) -> usize {
        self.rlp_list_len() + sig.v.length() + sig.r.length() + sig.s.length()
    }
}

impl Eip2930Transaction {
    /// Encode for signing
    ///
    /// Uses thread-local buffer to minimize allocations.
    #[inline]
    pub fn encode_for_signing(&self) -> Vec<u8> {
        ENCODE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            // Type 1 prefix
            buf.push(0x01);
            self.encode_fields(&mut buf);
            buf.clone()
        })
    }

    #[inline]
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        let list_len = self.rlp_list_len();
        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(buf);

        self.chain_id.encode(buf);
        self.nonce.encode(buf);
        self.gas_price.encode(buf);
        self.gas_limit.encode(buf);
        encode_to(self.to, buf);
        self.value.encode(buf);
        self.data.encode(buf);
        encode_access_list(&self.access_list, buf);
    }

    fn rlp_list_len(&self) -> usize {
        self.chain_id.length()
            + self.nonce.length()
            + self.gas_price.length()
            + self.gas_limit.length()
            + to_length(self.to)
            + self.value.length()
            + self.data.length()
            + access_list_length(&self.access_list)
    }

    /// Encode signed transaction
    pub fn encode_signed(&self, sig: &RecoverableSignature) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        // Type 1 prefix
        buf.push(0x01);

        let list_len = self.rlp_list_len_signed(sig);
        alloy_rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(&mut buf);

        self.chain_id.encode(&mut buf);
        self.nonce.encode(&mut buf);
        self.gas_price.encode(&mut buf);
        self.gas_limit.encode(&mut buf);
        encode_to(self.to, &mut buf);
        self.value.encode(&mut buf);
        self.data.encode(&mut buf);
        encode_access_list(&self.access_list, &mut buf);
        sig.v.encode(&mut buf);
        sig.r.encode(&mut buf);
        sig.s.encode(&mut buf);

        buf
    }

    fn rlp_list_len_signed(&self, sig: &RecoverableSignature) -> usize {
        self.rlp_list_len() + sig.v.length() + sig.r.length() + sig.s.length()
    }
}

/// Helper to encode Option<Address>
#[inline]
fn encode_to(to: Option<Address>, buf: &mut Vec<u8>) {
    match to {
        Some(addr) => addr.encode(buf),
        None => {
            // Empty RLP string
            buf.push(0x80);
        }
    }
}

/// Helper to get length of Option<Address>
#[inline]
fn to_length(to: Option<Address>) -> usize {
    match to {
        Some(addr) => addr.length(),
        None => 1, // Empty RLP string
    }
}

/// Encode access list
#[inline]
fn encode_access_list(access_list: &[AccessListItem], buf: &mut Vec<u8>) {
    let len: usize = access_list.iter().map(|item| item.length()).sum();
    alloy_rlp::Header {
        list: true,
        payload_length: len,
    }
    .encode(buf);
    for item in access_list {
        item.encode(buf);
    }
}

/// Get access list RLP length
#[inline]
fn access_list_length(access_list: &[AccessListItem]) -> usize {
    let inner_len: usize = access_list.iter().map(|item| item.length()).sum();
    alloy_rlp::length_of_length(inner_len) + inner_len
}

impl TypedTransaction {
    /// Encode for signing
    pub fn encode_for_signing(&self) -> Vec<u8> {
        match self {
            TypedTransaction::Legacy(tx) => tx.encode_for_signing(),
            TypedTransaction::Eip2930(tx) => tx.encode_for_signing(),
            TypedTransaction::Eip1559(tx) => tx.encode_for_signing(),
        }
    }

    /// Get signing hash
    pub fn signing_hash(&self) -> B256 {
        keccak256(&self.encode_for_signing())
    }

    /// Encode signed transaction
    pub fn encode_signed(&self, sig: &RecoverableSignature) -> Vec<u8> {
        match self {
            TypedTransaction::Legacy(tx) => tx.encode_signed(sig),
            TypedTransaction::Eip2930(tx) => tx.encode_signed(sig),
            TypedTransaction::Eip1559(tx) => tx.encode_signed(sig),
        }
    }

    /// Get chain ID
    pub fn chain_id(&self) -> u64 {
        match self {
            TypedTransaction::Legacy(tx) => tx.chain_id,
            TypedTransaction::Eip2930(tx) => tx.chain_id,
            TypedTransaction::Eip1559(tx) => tx.chain_id,
        }
    }

    /// Get nonce
    pub fn nonce(&self) -> u64 {
        match self {
            TypedTransaction::Legacy(tx) => tx.nonce,
            TypedTransaction::Eip2930(tx) => tx.nonce,
            TypedTransaction::Eip1559(tx) => tx.nonce,
        }
    }
}

impl Transaction {
    /// Create and sign a transaction
    pub fn sign(typed_tx: TypedTransaction, signer: &FastSigner) -> WalletResult<Self> {
        let hash = typed_tx.signing_hash();

        let signature = match &typed_tx {
            TypedTransaction::Legacy(_) => signer.sign_hash(&hash, typed_tx.chain_id())?,
            TypedTransaction::Eip2930(_) | TypedTransaction::Eip1559(_) => {
                signer.sign_hash_typed(&hash)?
            }
        };

        let encoded = typed_tx.encode_signed(&signature);
        let tx_hash = keccak256(&encoded);

        Ok(Self {
            typed_tx,
            signature,
            encoded: Some(Bytes::from(encoded)),
            hash: Some(tx_hash),
        })
    }

    /// Get the encoded transaction bytes
    pub fn encoded(&self) -> &Bytes {
        self.encoded.as_ref().expect("Transaction should be encoded")
    }

    /// Get the transaction hash
    pub fn hash(&self) -> B256 {
        self.hash.expect("Transaction should have hash")
    }

    /// Get hex-encoded transaction for RPC submission
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.encoded()))
    }

    /// Get the nonce
    pub fn nonce(&self) -> u64 {
        self.typed_tx.nonce()
    }
}

impl TransactionRequest {
    /// Create a new transaction request
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set recipient
    #[inline]
    pub fn to(mut self, to: Address) -> Self {
        self.to = Some(to);
        self
    }

    /// Set value
    #[inline]
    pub fn value(mut self, value: U256) -> Self {
        self.value = value;
        self
    }

    /// Set data
    #[inline]
    pub fn data(mut self, data: impl Into<Bytes>) -> Self {
        self.data = data.into();
        self
    }

    /// Set gas limit
    #[inline]
    pub fn gas_limit(mut self, gas_limit: u64) -> Self {
        self.gas_limit = gas_limit;
        self
    }

    /// Set nonce
    #[inline]
    pub fn nonce(mut self, nonce: u64) -> Self {
        self.nonce = nonce;
        self
    }

    /// Set chain ID
    #[inline]
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }

    /// Set legacy gas price
    #[inline]
    pub fn gas_price(mut self, gas_price: U256) -> Self {
        self.gas_price = Some(gas_price);
        self
    }

    /// Set EIP-1559 max fee
    #[inline]
    pub fn max_fee_per_gas(mut self, max_fee: U256) -> Self {
        self.max_fee_per_gas = Some(max_fee);
        self
    }

    /// Set EIP-1559 max priority fee
    #[inline]
    pub fn max_priority_fee_per_gas(mut self, max_priority_fee: U256) -> Self {
        self.max_priority_fee_per_gas = Some(max_priority_fee);
        self
    }

    /// Build into a typed transaction
    pub fn build(self) -> WalletResult<TypedTransaction> {
        // Determine transaction type based on provided gas parameters
        if self.max_fee_per_gas.is_some() || self.max_priority_fee_per_gas.is_some() {
            // EIP-1559
            Ok(TypedTransaction::Eip1559(Eip1559Transaction {
                chain_id: self.chain_id,
                nonce: self.nonce,
                max_priority_fee_per_gas: self
                    .max_priority_fee_per_gas
                    .unwrap_or(U256::from(1_000_000_000u64)), // 1 gwei default
                max_fee_per_gas: self
                    .max_fee_per_gas
                    .unwrap_or(U256::from(100_000_000_000u64)), // 100 gwei default
                gas_limit: self.gas_limit,
                to: self.to,
                value: self.value,
                data: self.data,
                access_list: self.access_list.unwrap_or_default(),
            }))
        } else if self.access_list.is_some() {
            // EIP-2930
            Ok(TypedTransaction::Eip2930(Eip2930Transaction {
                chain_id: self.chain_id,
                nonce: self.nonce,
                gas_price: self.gas_price.unwrap_or(U256::from(50_000_000_000u64)), // 50 gwei default
                gas_limit: self.gas_limit,
                to: self.to,
                value: self.value,
                data: self.data,
                access_list: self.access_list.unwrap_or_default(),
            }))
        } else {
            // Legacy
            Ok(TypedTransaction::Legacy(LegacyTransaction {
                nonce: self.nonce,
                gas_price: self.gas_price.unwrap_or(U256::from(50_000_000_000u64)), // 50 gwei default
                gas_limit: self.gas_limit,
                to: self.to,
                value: self.value,
                data: self.data,
                chain_id: self.chain_id,
            }))
        }
    }

    /// Build and sign transaction in one step
    pub fn build_and_sign(self, signer: &FastSigner) -> WalletResult<Transaction> {
        let typed_tx = self.build()?;
        Transaction::sign(typed_tx, signer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PRIVATE_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[test]
    fn test_legacy_transaction_encoding() {
        let tx = LegacyTransaction {
            nonce: 0,
            gas_price: U256::from(20_000_000_000u64), // 20 gwei
            gas_limit: 21000,
            to: Some(Address::repeat_byte(1)),
            value: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            data: Bytes::new(),
            chain_id: 1,
        };

        let encoded = tx.encode_for_signing();
        assert!(!encoded.is_empty());

        // Hash should be deterministic
        let hash = keccak256(&encoded);
        let hash2 = keccak256(&encoded);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_eip1559_transaction_encoding() {
        let tx = Eip1559Transaction {
            chain_id: 1,
            nonce: 0,
            max_priority_fee_per_gas: U256::from(1_000_000_000u64), // 1 gwei
            max_fee_per_gas: U256::from(20_000_000_000u64),         // 20 gwei
            gas_limit: 21000,
            to: Some(Address::repeat_byte(1)),
            value: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            data: Bytes::new(),
            access_list: vec![],
        };

        let encoded = tx.encode_for_signing();
        // Should start with 0x02 (type 2)
        assert_eq!(encoded[0], 0x02);
    }

    #[test]
    fn test_transaction_request_build() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

        // Legacy transaction
        let tx = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build_and_sign(&signer)
            .unwrap();

        assert!(!tx.encoded().is_empty());
        assert_ne!(tx.hash(), B256::ZERO);
    }

    #[test]
    fn test_eip1559_transaction_build() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

        let tx = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .max_fee_per_gas(U256::from(20_000_000_000u64))
            .max_priority_fee_per_gas(U256::from(1_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build_and_sign(&signer)
            .unwrap();

        // Should start with 0x02 (type 2)
        assert_eq!(tx.encoded()[0], 0x02);
    }

    #[test]
    fn test_transaction_hash_deterministic() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

        let req = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
            .nonce(0)
            .chain_id(1);

        let tx1 = req.clone().build_and_sign(&signer).unwrap();
        let tx2 = req.build_and_sign(&signer).unwrap();

        // Same transaction should produce same hash
        assert_eq!(tx1.hash(), tx2.hash());
    }

    #[test]
    fn test_contract_deployment() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

        // Contract deployment (no 'to' address)
        let tx = TransactionRequest::new()
            .data(vec![0x60, 0x80, 0x60, 0x40]) // Sample bytecode
            .gas_limit(100000)
            .gas_price(U256::from(20_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build_and_sign(&signer)
            .unwrap();

        assert!(!tx.encoded().is_empty());
    }
}

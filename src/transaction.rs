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
use alloy::primitives::{Address, Bytes, B256, U256};
use alloy::rlp::Encodable;
use std::cell::RefCell;

// Thread-local buffer for encoding to avoid repeated allocations
thread_local! {
    static ENCODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(512));
}

/// Minimum replace-by-fee bump, in basis points. Geth's default `PriceBump` is
/// 10%; we use 12.5% as a safe margin so a re-broadcast strictly clears the floor.
pub const RBF_MIN_BUMP_BPS: u32 = 1250;

/// Multiply `v` by `(1 + bump_bps/10000)`, rounding up so the result is strictly
/// greater than `v` for any non-zero `v` (needed to satisfy RBF replacement).
#[inline]
pub(crate) fn bump_u256(v: U256, bump_bps: u32) -> U256 {
    let denom = U256::from(10_000u32);
    let num = v * U256::from(10_000u32 + bump_bps);
    (num + denom - U256::from(1u8)) / denom
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
    fn encode(&self, out: &mut dyn alloy::rlp::BufMut) {
        // Encode as [address, [storage_keys...]]
        let storage_len: usize = self.storage_keys.iter().map(|k| k.length()).sum();
        let storage_header_len = alloy::rlp::length_of_length(storage_len);

        let list_len = self.address.length() + storage_header_len + storage_len;

        alloy::rlp::Header {
            list: true,
            payload_length: list_len,
        }
        .encode(out);

        self.address.encode(out);

        // Encode storage keys as a list
        alloy::rlp::Header {
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
        let storage_header_len = alloy::rlp::length_of_length(storage_len);
        let list_len = self.address.length() + storage_header_len + storage_len;
        alloy::rlp::length_of_length(list_len) + list_len
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

    /// Compute signing hash directly without returning encoded bytes
    ///
    /// This is more efficient than encode_for_signing() + keccak256() as it
    /// avoids the Vec clone.
    #[inline]
    pub fn signing_hash(&self) -> B256 {
        ENCODE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            self.encode_signing_fields(&mut buf);
            keccak256(&buf)
        })
    }

    /// Encode signing fields to buffer
    #[inline]
    fn encode_signing_fields(&self, buf: &mut Vec<u8>) {
        // [nonce, gasPrice, gasLimit, to, value, data, chainId, 0, 0]
        let list_len = self.rlp_list_len_for_signing();
        alloy::rlp::Header {
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
        alloy::rlp::Header {
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
        encode_signature_scalar(&sig.r, &mut buf);
        encode_signature_scalar(&sig.s, &mut buf);

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
            + signature_scalar_length(&sig.r)
            + signature_scalar_length(&sig.s)
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

    /// Compute signing hash directly without returning encoded bytes
    #[inline]
    pub fn signing_hash(&self) -> B256 {
        ENCODE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            buf.push(0x02);
            self.encode_fields(&mut buf);
            keccak256(&buf)
        })
    }

    #[inline]
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        let list_len = self.rlp_list_len();
        alloy::rlp::Header {
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
        alloy::rlp::Header {
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
        encode_signature_scalar(&sig.r, &mut buf);
        encode_signature_scalar(&sig.s, &mut buf);

        buf
    }

    fn rlp_list_len_signed(&self, sig: &RecoverableSignature) -> usize {
        self.rlp_list_len()
            + sig.v.length()
            + signature_scalar_length(&sig.r)
            + signature_scalar_length(&sig.s)
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

    /// Compute signing hash directly without returning encoded bytes
    #[inline]
    pub fn signing_hash(&self) -> B256 {
        ENCODE_BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            buf.push(0x01);
            self.encode_fields(&mut buf);
            keccak256(&buf)
        })
    }

    #[inline]
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        let list_len = self.rlp_list_len();
        alloy::rlp::Header {
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
        alloy::rlp::Header {
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
        encode_signature_scalar(&sig.r, &mut buf);
        encode_signature_scalar(&sig.s, &mut buf);

        buf
    }

    fn rlp_list_len_signed(&self, sig: &RecoverableSignature) -> usize {
        self.rlp_list_len()
            + sig.v.length()
            + signature_scalar_length(&sig.r)
            + signature_scalar_length(&sig.s)
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
    alloy::rlp::Header {
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
    alloy::rlp::length_of_length(inner_len) + inner_len
}

#[inline]
fn encode_signature_scalar(value: &B256, buf: &mut Vec<u8>) {
    canonical_signature_scalar_bytes(value).encode(buf);
}

#[inline]
fn signature_scalar_length(value: &B256) -> usize {
    canonical_signature_scalar_bytes(value).length()
}

#[inline]
fn canonical_signature_scalar_bytes(value: &B256) -> &[u8] {
    let bytes = value.as_slice();
    let first_nonzero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len());
    &bytes[first_nonzero..]
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

    /// Get signing hash (optimized - no Vec allocation)
    #[inline]
    pub fn signing_hash(&self) -> B256 {
        match self {
            TypedTransaction::Legacy(tx) => tx.signing_hash(),
            TypedTransaction::Eip2930(tx) => tx.signing_hash(),
            TypedTransaction::Eip1559(tx) => tx.signing_hash(),
        }
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

    /// EIP-1559 max fee per gas (`None` for legacy/2930).
    pub fn max_fee_per_gas(&self) -> Option<U256> {
        match self {
            TypedTransaction::Eip1559(tx) => Some(tx.max_fee_per_gas),
            _ => None,
        }
    }

    /// EIP-1559 max priority fee per gas (`None` for legacy/2930).
    pub fn max_priority_fee_per_gas(&self) -> Option<U256> {
        match self {
            TypedTransaction::Eip1559(tx) => Some(tx.max_priority_fee_per_gas),
            _ => None,
        }
    }

    /// Clone with EIP-1559 fees (both tip and max-fee) bumped by `bump_bps`.
    /// Returns `None` for non-EIP-1559 txs. The result is unsigned — re-sign it.
    pub fn with_bumped_fees(&self, bump_bps: u32) -> Option<TypedTransaction> {
        match self {
            TypedTransaction::Eip1559(tx) => {
                let mut bumped = tx.clone();
                bumped.max_priority_fee_per_gas = bump_u256(tx.max_priority_fee_per_gas, bump_bps);
                bumped.max_fee_per_gas = bump_u256(tx.max_fee_per_gas, bump_bps);
                Some(TypedTransaction::Eip1559(bumped))
            }
            _ => None,
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
        self.encoded
            .as_ref()
            .expect("Transaction should be encoded")
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

    /// EIP-1559 max fee per gas (`None` for legacy/2930).
    pub fn max_fee_per_gas(&self) -> Option<U256> {
        self.typed_tx.max_fee_per_gas()
    }

    /// EIP-1559 max priority fee per gas (`None` for legacy/2930).
    pub fn max_priority_fee_per_gas(&self) -> Option<U256> {
        self.typed_tx.max_priority_fee_per_gas()
    }

    /// Re-sign this transaction with EIP-1559 fees bumped by `bump_bps` (for an
    /// RBF re-broadcast of an evicted/underpriced tx). Same nonce, higher fees,
    /// so at most one of the original and the replacement can mine. Returns
    /// `None` for non-EIP-1559 txs (caller falls back to a same-gas re-send).
    pub fn bumped_resign(
        &self,
        signer: &FastSigner,
        bump_bps: u32,
    ) -> Option<WalletResult<Transaction>> {
        let bumped = self.typed_tx.with_bumped_fees(bump_bps)?;
        Some(Transaction::sign(bumped, signer))
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

    fn leading_zero_signature() -> RecoverableSignature {
        RecoverableSignature {
            v: 1,
            r: B256::from([
                0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0,
                0xe0, 0xf0, 0x01, 0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78, 0x89, 0x9a, 0xab, 0xbc,
                0xcd, 0xde, 0xef, 0x01,
            ]),
            s: B256::from([
                0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
                0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c,
                0x2d, 0x2e, 0x2f, 0x30,
            ]),
        }
    }

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
    fn test_signature_scalar_encoding_strips_leading_zero() {
        let mut encoded = Vec::new();
        let sig = leading_zero_signature();

        encode_signature_scalar(&sig.r, &mut encoded);

        assert_eq!(signature_scalar_length(&sig.r), 32);
        assert_eq!(
            hex::encode(encoded),
            "9f102030405060708090a0b0c0d0e0f00112233445566778899aabbccddeef01"
        );
    }

    #[test]
    fn test_eip1559_signed_encoding_canonicalizes_signature_scalars() {
        let tx = Eip1559Transaction {
            chain_id: 8453,
            nonce: 7,
            max_priority_fee_per_gas: U256::from(259_311u64),
            max_fee_per_gas: U256::from(1_000_000u64),
            gas_limit: 210_000,
            to: Some(Address::repeat_byte(0x11)),
            value: U256::ZERO,
            data: Bytes::new(),
            access_list: vec![],
        };

        let encoded = tx.encode_signed(&leading_zero_signature());

        assert_eq!(
            hex::encode(encoded),
            "02f86a822105078303f4ef830f4240830334509411111111111111111111111111111111111111118080c0019f102030405060708090a0b0c0d0e0f00112233445566778899aabbccddeef01a01112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30"
        );
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

    #[test]
    fn test_bump_u256_ceils_strictly_above() {
        // 12.5% of 1000 = 1125 exactly.
        assert_eq!(bump_u256(U256::from(1000u64), 1250), U256::from(1125u64));
        // Fractional results round UP so the bump always strictly exceeds the input.
        assert_eq!(bump_u256(U256::from(1u64), 1250), U256::from(2u64)); // ceil(1.125)
        assert_eq!(bump_u256(U256::from(8u64), 1250), U256::from(9u64)); // ceil(9.0)
                                                                         // Zero stays zero (no fee to bump).
        assert_eq!(bump_u256(U256::ZERO, 1250), U256::ZERO);
    }

    fn signed_eip1559(tip: u64, max_fee: u64, nonce: u64) -> Transaction {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::ZERO)
            .gas_limit(21000)
            .max_priority_fee_per_gas(U256::from(tip))
            .max_fee_per_gas(U256::from(max_fee))
            .nonce(nonce)
            .chain_id(1)
            .build_and_sign(&signer)
            .unwrap()
    }

    #[test]
    fn test_bumped_resign_bumps_both_fees_keeps_nonce_changes_hash() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        let tx = signed_eip1559(1_000_000_000, 20_000_000_000, 7);

        let bumped = tx
            .bumped_resign(&signer, RBF_MIN_BUMP_BPS)
            .unwrap()
            .unwrap();

        // Both tip and max-fee strictly increased (RBF requires BOTH).
        assert!(
            bumped.max_priority_fee_per_gas().unwrap() > tx.max_priority_fee_per_gas().unwrap()
        );
        assert!(bumped.max_fee_per_gas().unwrap() > tx.max_fee_per_gas().unwrap());
        // Same nonce so original and replacement are mutually exclusive.
        assert_eq!(bumped.nonce(), tx.nonce());
        // New fees ⇒ a different signed tx hash.
        assert_ne!(bumped.hash(), tx.hash());
    }

    #[test]
    fn test_bumped_resign_none_for_legacy() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        let legacy = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::ZERO)
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build_and_sign(&signer)
            .unwrap();
        assert!(legacy.bumped_resign(&signer, RBF_MIN_BUMP_BPS).is_none());
        assert!(legacy.max_fee_per_gas().is_none());
    }
}

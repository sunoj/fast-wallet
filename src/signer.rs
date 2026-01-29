//! Fast transaction signer using k256
//!
//! This module provides high-performance ECDSA signing using the k256 crate,
//! which is one of the fastest pure-Rust secp256k1 implementations.

use crate::crypto::{keccak256, public_key_to_address};
use crate::error::{WalletError, WalletResult};
use alloy_primitives::{Address, B256};
use k256::ecdsa::{SigningKey, VerifyingKey};
use k256::elliptic_curve::sec1::ToEncodedPoint as _;

/// Recovery ID with chain ID for EIP-155
#[derive(Debug, Clone, Copy)]
pub struct RecoverableSignature {
    pub r: B256,
    pub s: B256,
    pub v: u64,
}

impl RecoverableSignature {
    /// Create signature bytes for RLP encoding (r || s || v as single byte for legacy)
    #[inline]
    pub fn to_bytes(&self) -> [u8; 65] {
        let mut bytes = [0u8; 65];
        bytes[..32].copy_from_slice(self.r.as_slice());
        bytes[32..64].copy_from_slice(self.s.as_slice());
        bytes[64] = self.v as u8;
        bytes
    }
}

/// Fast ECDSA signer with pre-computed public key and address
pub struct FastSigner {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    address: Address,
}

impl FastSigner {
    /// Create a new signer from a private key (32 bytes)
    pub fn new(private_key: &[u8; 32]) -> WalletResult<Self> {
        let signing_key = SigningKey::from_bytes(private_key.into())
            .map_err(|e| WalletError::InvalidPrivateKey(e.to_string()))?;

        let verifying_key = *signing_key.verifying_key();

        // Get uncompressed public key (without 0x04 prefix)
        let pubkey_point = verifying_key.to_encoded_point(false);
        let pubkey_bytes = pubkey_point.as_bytes();
        // Skip the 0x04 prefix
        let address = public_key_to_address(&pubkey_bytes[1..]);

        Ok(Self {
            signing_key,
            verifying_key,
            address,
        })
    }

    /// Create a new signer from a hex-encoded private key
    pub fn from_hex(hex_key: &str) -> WalletResult<Self> {
        let hex_key = hex_key.strip_prefix("0x").unwrap_or(hex_key);
        let key_bytes = hex::decode(hex_key)?;

        if key_bytes.len() != 32 {
            return Err(WalletError::InvalidPrivateKey(format!(
                "Expected 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let mut private_key = [0u8; 32];
        private_key.copy_from_slice(&key_bytes);
        Self::new(&private_key)
    }

    /// Get the Ethereum address
    #[inline]
    pub fn address(&self) -> Address {
        self.address
    }

    /// Get the verifying (public) key
    #[inline]
    pub fn verifying_key(&self) -> &VerifyingKey {
        &self.verifying_key
    }

    /// Sign a message hash (32 bytes) with EIP-155 recovery ID
    ///
    /// This is the core signing function, optimized for speed.
    /// The chain_id is used to compute the v value for EIP-155.
    #[inline]
    pub fn sign_hash(&self, hash: &B256, chain_id: u64) -> WalletResult<RecoverableSignature> {
        // Sign the hash using k256's recoverable signature
        let (signature, recovery_id) = self
            .signing_key
            .sign_prehash_recoverable(hash.as_slice())
            .map_err(|e| WalletError::SigningError(e.to_string()))?;

        let r = B256::from_slice(&signature.r().to_bytes());
        let s = B256::from_slice(&signature.s().to_bytes());

        // EIP-155: v = recovery_id + 35 + chain_id * 2
        let v = recovery_id.to_byte() as u64 + 35 + chain_id * 2;

        Ok(RecoverableSignature { r, s, v })
    }

    /// Sign a message hash for EIP-1559 transactions (different v calculation)
    ///
    /// For EIP-1559/2930 transactions, v is just the recovery ID (0 or 1)
    #[inline]
    pub fn sign_hash_typed(&self, hash: &B256) -> WalletResult<RecoverableSignature> {
        let (signature, recovery_id) = self
            .signing_key
            .sign_prehash_recoverable(hash.as_slice())
            .map_err(|e| WalletError::SigningError(e.to_string()))?;

        let r = B256::from_slice(&signature.r().to_bytes());
        let s = B256::from_slice(&signature.s().to_bytes());
        let v = recovery_id.to_byte() as u64;

        Ok(RecoverableSignature { r, s, v })
    }

    /// Sign arbitrary data by first hashing with Keccak256
    #[inline]
    pub fn sign_data(&self, data: &[u8], chain_id: u64) -> WalletResult<RecoverableSignature> {
        let hash = keccak256(data);
        self.sign_hash(&hash, chain_id)
    }

    /// Sign with Ethereum signed message prefix
    pub fn sign_message(&self, message: &[u8], chain_id: u64) -> WalletResult<RecoverableSignature> {
        let prefixed = format!("\x19Ethereum Signed Message:\n{}", message.len());
        let mut data = prefixed.into_bytes();
        data.extend_from_slice(message);
        self.sign_data(&data, chain_id)
    }
}

impl Clone for FastSigner {
    fn clone(&self) -> Self {
        Self {
            signing_key: self.signing_key.clone(),
            verifying_key: self.verifying_key,
            address: self.address,
        }
    }
}

// Manually implement Debug to avoid exposing the private key
impl std::fmt::Debug for FastSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastSigner")
            .field("address", &self.address)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test private key (DO NOT USE IN PRODUCTION)
    const TEST_PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

    #[test]
    fn test_signer_creation() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        // Compare lowercase addresses to avoid checksum differences
        let expected = TEST_ADDRESS.to_lowercase();
        let actual = format!("{:?}", signer.address()).to_lowercase();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_signer_clone() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        let cloned = signer.clone();
        assert_eq!(signer.address(), cloned.address());
    }

    #[test]
    fn test_sign_hash() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        let hash = B256::ZERO;
        let sig = signer.sign_hash(&hash, 1).unwrap();

        // Verify signature components are non-zero
        assert_ne!(sig.r, B256::ZERO);
        assert_ne!(sig.s, B256::ZERO);

        // Verify v is correct for chain_id 1
        // v = recovery_id + 35 + 1 * 2 = 37 or 38
        assert!(sig.v == 37 || sig.v == 38);
    }

    #[test]
    fn test_sign_hash_typed() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        let hash = B256::ZERO;
        let sig = signer.sign_hash_typed(&hash).unwrap();

        // For typed transactions, v is just 0 or 1
        assert!(sig.v == 0 || sig.v == 1);
    }

    #[test]
    fn test_invalid_private_key() {
        let result = FastSigner::from_hex("0xinvalid");
        assert!(result.is_err());

        let result = FastSigner::from_hex("0x1234"); // Too short
        assert!(result.is_err());
    }

    #[test]
    fn test_deterministic_signing() {
        let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
        let hash = keccak256(b"test message");

        let sig1 = signer.sign_hash(&hash, 1).unwrap();
        let sig2 = signer.sign_hash(&hash, 1).unwrap();

        // Signatures should be deterministic (RFC 6979)
        assert_eq!(sig1.r, sig2.r);
        assert_eq!(sig1.s, sig2.s);
        assert_eq!(sig1.v, sig2.v);
    }
}

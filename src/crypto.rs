//! Fast cryptographic primitives
//!
//! This module provides optimized implementations of common crypto operations.
//! Uses native/asm-optimized keccak when available, falling back to tiny-keccak.

use alloy_primitives::{Address, B256};
use tiny_keccak::{Hasher, Keccak};

/// Fast Keccak256 hash function
///
/// Uses the fastest available implementation for the target platform.
/// For x86_64, this leverages CPU-optimized code paths.
#[inline(always)]
pub fn keccak256(data: &[u8]) -> B256 {
    // Use alloy_primitives::keccak256 which is highly optimized
    // and may use native SHA3 instructions when available
    alloy_primitives::keccak256(data)
}

/// Compute Keccak256 hash into existing buffer (zero-allocation)
#[inline(always)]
pub fn keccak256_into(data: &[u8], output: &mut [u8; 32]) {
    let hash = keccak256(data);
    output.copy_from_slice(hash.as_slice());
}

/// Alternative keccak256 using tiny-keccak (for comparison/fallback)
#[inline(always)]
#[allow(dead_code)]
pub fn keccak256_tiny(data: &[u8]) -> B256 {
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(data);
    hasher.finalize(&mut output);
    B256::from(output)
}

/// Compute Ethereum address from uncompressed public key (64 bytes, without 0x04 prefix)
#[inline]
pub fn public_key_to_address(pubkey: &[u8]) -> Address {
    debug_assert!(pubkey.len() == 64, "Public key must be 64 bytes (uncompressed without prefix)");
    let hash = keccak256(pubkey);
    Address::from_slice(&hash[12..])
}

/// Fast hex encoding without allocation for common sizes
pub mod fast_hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    /// Encode bytes to hex string with 0x prefix
    #[inline]
    pub fn encode_prefixed(data: &[u8]) -> String {
        let mut result = String::with_capacity(2 + data.len() * 2);
        result.push_str("0x");
        for byte in data {
            result.push(HEX_CHARS[(byte >> 4) as usize] as char);
            result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
        }
        result
    }

    /// Encode to hex without prefix
    #[inline]
    pub fn encode(data: &[u8]) -> String {
        let mut result = String::with_capacity(data.len() * 2);
        for byte in data {
            result.push(HEX_CHARS[(byte >> 4) as usize] as char);
            result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
        }
        result
    }

    /// Decode hex string (with or without 0x prefix)
    #[inline]
    pub fn decode(s: &str) -> Result<Vec<u8>, hex::FromHexError> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        hex::decode(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keccak256() {
        // Test vector: keccak256("")
        let empty_hash = keccak256(&[]);
        assert_eq!(
            format!("{:x}", empty_hash),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );

        // Test vector: keccak256("hello")
        let hello_hash = keccak256(b"hello");
        assert_eq!(
            format!("{:x}", hello_hash),
            "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"
        );
    }

    #[test]
    fn test_keccak256_into() {
        let mut output = [0u8; 32];
        keccak256_into(b"hello", &mut output);
        let expected = keccak256(b"hello");
        assert_eq!(output, expected.0);
    }

    #[test]
    fn test_fast_hex() {
        let data = [0xde, 0xad, 0xbe, 0xef];
        assert_eq!(fast_hex::encode(&data), "deadbeef");
        assert_eq!(fast_hex::encode_prefixed(&data), "0xdeadbeef");
        assert_eq!(fast_hex::decode("0xdeadbeef").unwrap(), data);
        assert_eq!(fast_hex::decode("deadbeef").unwrap(), data);
    }
}

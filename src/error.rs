//! Error types for the fast wallet

use thiserror::Error;

/// Parse the authoritative next nonce from a recognized nonce-too-low error.
///
/// When the reported state exceeds the locally believed chain nonce, the
/// healing action is `sync_forward(state)` and the lane is treated as healed;
/// never re-send the rejected transaction nonce. Unrecognized phrasing
/// returns `None` so callers can retain their existing behavior.
pub fn parse_nonce_too_low(message: &str) -> Option<u64> {
    let details = nonce_too_low_details(message)?.trim();

    parse_arbitrum_nonce(details).or_else(|| parse_geth_nonce(details))
}

fn nonce_too_low_details(message: &str) -> Option<&str> {
    if let Some(details) = message.strip_prefix("nonce too low: ") {
        return Some(details);
    }

    let (prefix, details) = message.rsplit_once(": nonce too low: ")?;
    let code = prefix.strip_prefix("RPC error ").unwrap_or(prefix);
    code.parse::<i64>().ok()?;
    Some(details)
}

fn parse_arbitrum_nonce(details: &str) -> Option<u64> {
    let (address, tx_and_state) = details.split_once(", tx: ")?;
    let (tx_nonce, state_nonce) = tx_and_state.split_once(" state: ")?;

    if address.strip_prefix("address 0x")?.trim().is_empty() {
        return None;
    }

    parse_decimal(tx_nonce)?;
    parse_decimal(state_nonce)
}

fn parse_geth_nonce(details: &str) -> Option<u64> {
    let details = details.strip_prefix("next nonce ")?;
    let (state_nonce, tx_nonce) = details.split_once(", tx nonce ")?;

    parse_decimal(tx_nonce)?;
    parse_decimal(state_nonce)
}

fn parse_decimal(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

/// Custom error type for wallet operations
#[derive(Debug, Error)]
pub enum WalletError {
    #[error("Invalid private key: {0}")]
    InvalidPrivateKey(String),

    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("Signing error: {0}")]
    SigningError(String),

    #[error("RPC error: {0}")]
    RpcError(String),

    #[error("Nonce error: {0}")]
    NonceError(String),

    #[error("Transaction encoding error: {0}")]
    EncodingError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Hex decoding error: {0}")]
    HexError(#[from] hex::FromHexError),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Invalid chain ID: {0}")]
    InvalidChainId(u64),

    #[error("Nonce too low: expected {expected}, got {got}")]
    NonceTooLow { expected: u64, got: u64 },

    #[error("Transaction underpriced")]
    TransactionUnderpriced,

    #[error("Insufficient funds")]
    InsufficientFunds,

    #[error("Gas limit exceeded")]
    GasLimitExceeded,

    #[error("Transaction timeout")]
    Timeout,

    #[error("Invalid wallet configuration: {0}")]
    InvalidConfig(String),

    #[error("CREDENTIALS_DIRECTORY not set (not running under systemd credential delivery)")]
    NoCredentialsDir,

    #[error("Credential read error: {0}")]
    CredentialError(String),
}

impl WalletError {
    /// Return the sequencer-reported next nonce when this is a recognized RPC
    /// nonce-too-low error.
    pub fn authoritative_nonce(&self) -> Option<u64> {
        match self {
            Self::RpcError(message) => parse_nonce_too_low(message),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_nonce_too_low, WalletError};

    #[test]
    fn parses_observed_arbitrum_nonce_too_low_message() {
        let message = "nonce too low: address 0xabc…, tx: 5154 state: 5155";

        assert_eq!(parse_nonce_too_low(message), Some(5155));
        assert_eq!(
            WalletError::RpcError(message.to_string()).authoritative_nonce(),
            Some(5155)
        );
        assert_eq!(
            WalletError::RpcError(format!("RPC error -32000: {message}")).authoritative_nonce(),
            Some(5155)
        );
    }

    #[test]
    fn parses_geth_nonce_too_low_message() {
        assert_eq!(
            parse_nonce_too_low("nonce too low: next nonce 5155, tx nonce 5154"),
            Some(5155)
        );
    }

    #[test]
    fn rejects_unrecognized_nonce_too_low_message() {
        assert_eq!(
            parse_nonce_too_low("nonce too low: retry after the chain advances to 5155"),
            None
        );
    }
}

/// Result type alias for wallet operations
pub type WalletResult<T> = Result<T, WalletError>;

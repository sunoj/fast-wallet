//! Error types for the fast wallet

use thiserror::Error;

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

/// Result type alias for wallet operations
pub type WalletResult<T> = Result<T, WalletError>;

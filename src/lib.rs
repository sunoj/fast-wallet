//! Fast EVM Wallet - High-performance wallet for liquidation bots
//!
//! This library provides an extremely optimized EVM wallet implementation
//! focused on minimizing transaction submission latency.
//!
//! # Key Features
//! - Local nonce management to avoid RPC calls
//! - Async transaction sending with parallel submissions
//! - Multi-RPC broadcasting for fastest transaction inclusion
//! - Fast crypto primitives using k256/secp256k1
//! - Pre-computed values where possible
//! - Zero-copy RLP encoding
//!
//! # Example
//!
//! ```rust,no_run
//! use fast_wallet::{FastWalletBuilder, TransactionRequest};
//! use alloy::primitives::{Address, U256};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a wallet with multiple RPC endpoints
//! let wallet = FastWalletBuilder::new(
//!     "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
//!     "https://eth-mainnet.alchemyapi.io/v2/YOUR_KEY"
//! )
//! .chain_id(1)
//! .broadcast_rpcs(vec![
//!     "https://rpc.flashbots.net".to_string(),
//!     "https://eth.llamarpc.com".to_string(),
//! ])
//! .build().await?;
//!
//! // Sign and send a transaction
//! let tx_hash = wallet.send_eth(
//!     Address::repeat_byte(1),
//!     U256::from(1_000_000_000_000_000_000u64), // 1 ETH
//!     None,
//! ).await?;
//! # Ok(())
//! # }
//! ```

pub mod broadcast;
pub mod crypto;
pub mod error;
pub mod gas_provider;
pub mod inflight;
pub mod nonce;
pub mod rpc;
pub mod signer;
pub mod tls;
pub mod transaction;
pub mod wallet;

pub use broadcast::{BroadcastStrategy, BroadcasterBuilder, RpcEndpoint, TransactionBroadcaster};
pub use error::{WalletError, WalletResult};
pub use gas_provider::GasPriceProvider;
pub use inflight::{InflightNonceLedger, InflightNonceSnapshot, InflightNonceStatus};
pub use nonce::{NonceManager, ReservedNonce};
pub use rpc::SendResult;
pub use signer::{FastSigner, KeySource};
pub use transaction::{Transaction, TransactionRequest, TypedTransaction};
pub use wallet::{
    FastWallet, FastWalletBuilder, NonceHealth, PreheatedContext, ReplaceOutcome, VerifyOutcome,
    WalletConfig, WalletStatus,
};

/// Re-export commonly used types
pub mod types {
    pub use alloy::primitives::{Address, Bytes, B256, U256};
}

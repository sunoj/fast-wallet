//! External gas-price provider trait — lets a process share one chain-level
//! gas cache across multiple [`FastWallet`](crate::FastWallet) instances.
//!
//! When a `GasPriceProvider` is supplied via
//! [`FastWalletBuilder::gas_provider`](crate::FastWalletBuilder::gas_provider),
//! the wallet's own gas cache and background refresh task are bypassed
//! entirely — the provider becomes the sole source of truth.

use crate::error::WalletResult;
use alloy::primitives::U256;
use async_trait::async_trait;

/// Pluggable chain-level source of gas prices.
///
/// Implementations are expected to be cheap to clone (typically `Arc<Inner>`)
/// and to do their own caching / background refresh so that multiple wallets
/// pointing at the same provider observe consistent values.
#[async_trait]
pub trait GasPriceProvider: Send + Sync {
    /// Current chain `gas_price` (wei).
    async fn gas_price(&self) -> WalletResult<U256>;
    /// Current chain `max_priority_fee_per_gas` (wei).
    async fn priority_fee(&self) -> WalletResult<U256>;
}

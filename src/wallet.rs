//! Fast EVM Wallet - Core implementation
//!
//! This module provides the main wallet interface that combines:
//! - Fast signing with automatic nonce management
//! - Pre-warming/preheating for lowest latency
//! - Async transaction submission
//! - Parallel transaction broadcasting

use crate::error::{WalletError, WalletResult};
use crate::nonce::SingleAddressNonceManager;
use crate::rpc::{BatchRpcClient, RpcClient};
use crate::signer::FastSigner;
use crate::transaction::{Transaction, TransactionRequest};
use alloy_primitives::{Address, Bytes, B256, U256};
use futures::future::join_all;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Configuration for FastWallet
#[derive(Debug, Clone)]
pub struct WalletConfig {
    /// Chain ID
    pub chain_id: u64,
    /// Default gas limit for simple transfers
    pub default_gas_limit: u64,
    /// Maximum concurrent pending transactions
    pub max_pending_txs: usize,
    /// Transaction confirmation timeout
    pub confirmation_timeout: Duration,
    /// Poll interval for receipt checking
    pub poll_interval: Duration,
    /// Whether to use EIP-1559 transactions by default
    pub use_eip1559: bool,
    /// Gas price cache duration
    pub gas_cache_duration: Duration,
}

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            chain_id: 1,
            default_gas_limit: 21000,
            max_pending_txs: 100,
            confirmation_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_millis(500),
            use_eip1559: true,
            gas_cache_duration: Duration::from_millis(500),
        }
    }
}

/// Preheated transaction context - prepared signing context for fastest execution
///
/// Use this when you know a transaction is likely coming but don't have all params yet.
/// Call `wallet.preheat()` to get this context, then use it to sign when ready.
pub struct PreheatedContext {
    /// Pre-allocated nonce
    pub nonce: u64,
    /// Cached gas price (if fetched)
    pub gas_price: Option<U256>,
    /// Cached priority fee (if fetched)
    pub priority_fee: Option<U256>,
    /// Cached max fee (if fetched)
    pub max_fee: Option<U256>,
    /// Timestamp when preheated
    pub preheated_at: Instant,
    /// Whether this context has been used
    used: AtomicBool,
}

impl PreheatedContext {
    fn new(nonce: u64) -> Self {
        Self {
            nonce,
            gas_price: None,
            priority_fee: None,
            max_fee: None,
            preheated_at: Instant::now(),
            used: AtomicBool::new(false),
        }
    }

    /// Check if this context is still valid (not used and not too old)
    pub fn is_valid(&self, max_age: Duration) -> bool {
        !self.used.load(Ordering::Acquire) && self.preheated_at.elapsed() < max_age
    }

    /// Mark as used (call only once)
    pub fn mark_used(&self) -> bool {
        self.used
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Get age of this context
    pub fn age(&self) -> Duration {
        self.preheated_at.elapsed()
    }
}

/// High-performance EVM wallet optimized for liquidation bots
///
/// # Features
/// - Automatic nonce management (no need to pass nonce)
/// - Preheating API for lowest latency
/// - Multi-RPC broadcasting
/// - Gas price caching
pub struct FastWallet {
    signer: FastSigner,
    nonce_manager: SingleAddressNonceManager,
    rpc_client: Arc<RpcClient>,
    batch_client: Option<Arc<BatchRpcClient>>,
    config: WalletConfig,
    /// Semaphore to limit concurrent pending transactions
    pending_semaphore: Semaphore,
    /// Cached gas price
    gas_price_cache: RwLock<Option<(U256, Instant)>>,
    /// Cached priority fee
    priority_fee_cache: RwLock<Option<(U256, Instant)>>,
    /// Cached base fee
    base_fee_cache: RwLock<Option<(U256, Instant)>>,
    /// Whether warmup has been done
    warmed_up: AtomicBool,
}

impl FastWallet {
    /// Create a new wallet with a single RPC endpoint
    ///
    /// This will fetch the initial nonce from the chain.
    pub async fn new(
        private_key: &str,
        rpc_url: &str,
        config: WalletConfig,
    ) -> WalletResult<Self> {
        let signer = FastSigner::from_hex(private_key)?;
        let rpc_client = Arc::new(RpcClient::new(rpc_url)?);

        // Fetch initial nonce
        let initial_nonce = rpc_client.get_nonce(signer.address()).await?;

        let nonce_manager = SingleAddressNonceManager::new(signer.address(), initial_nonce);

        Ok(Self {
            signer,
            nonce_manager,
            rpc_client,
            batch_client: None,
            pending_semaphore: Semaphore::new(config.max_pending_txs),
            gas_price_cache: RwLock::new(None),
            priority_fee_cache: RwLock::new(None),
            base_fee_cache: RwLock::new(None),
            warmed_up: AtomicBool::new(false),
            config,
        })
    }

    /// Create a wallet with multiple RPC endpoints for broadcasting
    pub async fn with_multiple_rpcs(
        private_key: &str,
        primary_rpc: &str,
        broadcast_rpcs: Vec<String>,
        config: WalletConfig,
    ) -> WalletResult<Self> {
        let mut wallet = Self::new(private_key, primary_rpc, config).await?;

        if !broadcast_rpcs.is_empty() {
            wallet.batch_client = Some(Arc::new(BatchRpcClient::new(broadcast_rpcs)?));
        }

        Ok(wallet)
    }

    /// Create a wallet with known initial nonce (faster startup, no RPC call)
    pub fn with_known_nonce(
        private_key: &str,
        rpc_url: &str,
        initial_nonce: u64,
        config: WalletConfig,
    ) -> WalletResult<Self> {
        let signer = FastSigner::from_hex(private_key)?;
        let rpc_client = Arc::new(RpcClient::new(rpc_url)?);
        let nonce_manager = SingleAddressNonceManager::new(signer.address(), initial_nonce);

        Ok(Self {
            signer,
            nonce_manager,
            rpc_client,
            batch_client: None,
            pending_semaphore: Semaphore::new(config.max_pending_txs),
            gas_price_cache: RwLock::new(None),
            priority_fee_cache: RwLock::new(None),
            base_fee_cache: RwLock::new(None),
            warmed_up: AtomicBool::new(false),
            config,
        })
    }

    // ==================== Basic Getters ====================

    /// Get the wallet address
    #[inline]
    pub fn address(&self) -> Address {
        self.signer.address()
    }

    /// Get current nonce (without incrementing)
    #[inline]
    pub fn current_nonce(&self) -> u64 {
        self.nonce_manager.peek()
    }

    /// Get pending transaction count
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.nonce_manager.pending_count()
    }

    /// Get RPC client reference
    pub fn rpc(&self) -> &RpcClient {
        &self.rpc_client
    }

    /// Get config reference
    pub fn config(&self) -> &WalletConfig {
        &self.config
    }

    // ==================== Warmup / Preheating ====================

    /// Warmup the wallet - call this before you need to send transactions
    ///
    /// This will:
    /// 1. Sync nonce from chain
    /// 2. Fetch and cache gas prices
    /// 3. Warm up HTTP connection pool
    ///
    /// Call this during initialization or when anticipating transactions.
    pub async fn warmup(&self) -> WalletResult<()> {
        // Sync nonce
        let _ = self.sync_nonce().await?;

        // Fetch gas prices to warm up connection and cache
        let _ = self.get_gas_price().await?;

        // Try to get priority fee (might fail on non-EIP1559 chains)
        let _ = self.get_priority_fee().await.ok();

        self.warmed_up.store(true, Ordering::Release);
        Ok(())
    }

    /// Check if wallet has been warmed up
    #[inline]
    pub fn is_warmed_up(&self) -> bool {
        self.warmed_up.load(Ordering::Acquire)
    }

    /// Preheat a transaction - allocate nonce and optionally fetch gas prices
    ///
    /// Use this when you detect a potential opportunity but are still calculating
    /// the exact transaction parameters. This reserves a nonce and can prefetch
    /// gas prices.
    ///
    /// # Arguments
    /// * `fetch_gas` - Whether to also fetch current gas prices
    ///
    /// # Returns
    /// A `PreheatedContext` that can be used to sign a transaction later.
    ///
    /// # Warning
    /// The allocated nonce will be "wasted" if you don't use it. Only call this
    /// when you're fairly confident a transaction will be sent.
    pub async fn preheat(&self, fetch_gas: bool) -> WalletResult<PreheatedContext> {
        let nonce = self.nonce_manager.get_nonce();
        let mut ctx = PreheatedContext::new(nonce);

        if fetch_gas {
            // Fetch gas prices in parallel
            let (gas_price, priority_fee) =
                tokio::join!(self.get_gas_price(), self.get_priority_fee());

            ctx.gas_price = gas_price.ok();
            ctx.priority_fee = priority_fee.ok();

            // Calculate max fee if we have both
            if let (Some(base), Some(priority)) = (ctx.gas_price, ctx.priority_fee) {
                // max_fee = base_fee * 2 + priority_fee (common strategy)
                ctx.max_fee = Some(base + base + priority);
            }
        }

        Ok(ctx)
    }

    /// Preheat multiple transactions at once
    ///
    /// Useful when you know you'll send N transactions and want to reserve nonces.
    pub async fn preheat_batch(&self, count: usize, fetch_gas: bool) -> WalletResult<Vec<PreheatedContext>> {
        let mut contexts = Vec::with_capacity(count);

        // Allocate all nonces first (lock-free, very fast)
        for _ in 0..count {
            let nonce = self.nonce_manager.get_nonce();
            contexts.push(PreheatedContext::new(nonce));
        }

        // Fetch gas once if needed
        if fetch_gas {
            let (gas_price, priority_fee) =
                tokio::join!(self.get_gas_price(), self.get_priority_fee());

            let gas_price = gas_price.ok();
            let priority_fee = priority_fee.ok();
            let max_fee = match (gas_price, priority_fee) {
                (Some(base), Some(priority)) => Some(base + base + priority),
                _ => None,
            };

            // Apply to all contexts
            for ctx in &mut contexts {
                ctx.gas_price = gas_price;
                ctx.priority_fee = priority_fee;
                ctx.max_fee = max_fee;
            }
        }

        Ok(contexts)
    }

    /// Cancel a preheated context (return the nonce)
    ///
    /// Call this if you preheated but decided not to send the transaction.
    pub fn cancel_preheat(&self, ctx: PreheatedContext) {
        if ctx.mark_used() {
            // Mark the nonce as failed so it can be reused
            self.nonce_manager.fail(ctx.nonce);
        }
    }

    // ==================== Nonce Management ====================

    /// Sync nonce from chain
    pub async fn sync_nonce(&self) -> WalletResult<u64> {
        let chain_nonce = self.rpc_client.get_nonce(self.address()).await?;
        self.nonce_manager.sync(chain_nonce);
        Ok(chain_nonce)
    }

    /// Confirm a nonce was used (call after transaction is confirmed)
    pub fn confirm_nonce(&self) {
        self.nonce_manager.confirm();
    }

    // ==================== Gas Price ====================

    /// Get cached or fresh gas price
    pub async fn get_gas_price(&self) -> WalletResult<U256> {
        // Check cache first
        {
            let cache = self.gas_price_cache.read();
            if let Some((price, timestamp)) = cache.as_ref() {
                if timestamp.elapsed() < self.config.gas_cache_duration {
                    return Ok(*price);
                }
            }
        }

        // Fetch fresh price
        let price = self.rpc_client.gas_price().await?;

        // Update cache
        {
            let mut cache = self.gas_price_cache.write();
            *cache = Some((price, Instant::now()));
        }

        Ok(price)
    }

    /// Get cached or fresh priority fee (for EIP-1559)
    pub async fn get_priority_fee(&self) -> WalletResult<U256> {
        // Check cache first
        {
            let cache = self.priority_fee_cache.read();
            if let Some((fee, timestamp)) = cache.as_ref() {
                if timestamp.elapsed() < self.config.gas_cache_duration {
                    return Ok(*fee);
                }
            }
        }

        // Fetch fresh fee
        let fee = self.rpc_client.max_priority_fee().await?;

        // Update cache
        {
            let mut cache = self.priority_fee_cache.write();
            *cache = Some((fee, Instant::now()));
        }

        Ok(fee)
    }

    /// Force refresh gas prices
    pub async fn refresh_gas_prices(&self) -> WalletResult<(U256, Option<U256>)> {
        let (gas_price, priority_fee) =
            tokio::join!(self.rpc_client.gas_price(), self.rpc_client.max_priority_fee());

        let gas_price = gas_price?;

        // Update caches
        {
            let mut cache = self.gas_price_cache.write();
            *cache = Some((gas_price, Instant::now()));
        }

        let priority_fee = priority_fee.ok();
        if let Some(fee) = priority_fee {
            let mut cache = self.priority_fee_cache.write();
            *cache = Some((fee, Instant::now()));
        }

        Ok((gas_price, priority_fee))
    }

    // ==================== Transaction Signing (Auto Nonce) ====================

    /// Sign a transaction with automatic nonce management
    ///
    /// This is the simplest API - just provide transaction params without nonce.
    /// The wallet automatically assigns the next nonce.
    #[inline]
    pub fn sign(&self, mut request: TransactionRequest) -> WalletResult<Transaction> {
        // Auto-assign nonce
        request.nonce = self.nonce_manager.get_nonce();
        request.chain_id = self.config.chain_id;

        if request.gas_limit == 0 {
            request.gas_limit = self.config.default_gas_limit;
        }

        request.build_and_sign(&self.signer)
    }

    /// Sign using a preheated context
    ///
    /// Use the nonce and gas prices from a previously created PreheatedContext.
    pub fn sign_with_preheat(
        &self,
        ctx: &PreheatedContext,
        mut request: TransactionRequest,
    ) -> WalletResult<Transaction> {
        if !ctx.mark_used() {
            return Err(WalletError::NonceError(
                "PreheatedContext already used".to_string(),
            ));
        }

        request.nonce = ctx.nonce;
        request.chain_id = self.config.chain_id;

        // Use preheated gas prices if not specified
        if request.gas_price.is_none() && request.max_fee_per_gas.is_none() {
            if let Some(gas_price) = ctx.gas_price {
                request.gas_price = Some(gas_price);
            }
        }

        if request.max_fee_per_gas.is_none() {
            if let Some(max_fee) = ctx.max_fee {
                request.max_fee_per_gas = Some(max_fee);
            }
        }

        if request.max_priority_fee_per_gas.is_none() {
            if let Some(priority) = ctx.priority_fee {
                request.max_priority_fee_per_gas = Some(priority);
            }
        }

        if request.gas_limit == 0 {
            request.gas_limit = self.config.default_gas_limit;
        }

        request.build_and_sign(&self.signer)
    }

    /// Sign an EIP-1559 transaction
    #[inline]
    pub fn sign_eip1559(
        &self,
        to: Address,
        value: U256,
        data: Bytes,
        gas_limit: u64,
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
    ) -> WalletResult<Transaction> {
        let request = TransactionRequest::new()
            .to(to)
            .value(value)
            .data(data)
            .gas_limit(gas_limit)
            .max_fee_per_gas(max_fee_per_gas)
            .max_priority_fee_per_gas(max_priority_fee_per_gas)
            .nonce(self.nonce_manager.get_nonce())
            .chain_id(self.config.chain_id);

        request.build_and_sign(&self.signer)
    }

    /// Sign a legacy transaction
    #[inline]
    pub fn sign_legacy(
        &self,
        to: Address,
        value: U256,
        data: Bytes,
        gas_limit: u64,
        gas_price: U256,
    ) -> WalletResult<Transaction> {
        let request = TransactionRequest::new()
            .to(to)
            .value(value)
            .data(data)
            .gas_limit(gas_limit)
            .gas_price(gas_price)
            .nonce(self.nonce_manager.get_nonce())
            .chain_id(self.config.chain_id);

        request.build_and_sign(&self.signer)
    }

    // Backwards compatibility aliases
    #[inline]
    pub fn sign_transaction(&self, request: TransactionRequest) -> WalletResult<Transaction> {
        self.sign(request)
    }

    #[inline]
    pub fn sign_eip1559_transaction(
        &self,
        to: Address,
        value: U256,
        data: Bytes,
        gas_limit: u64,
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
    ) -> WalletResult<Transaction> {
        self.sign_eip1559(to, value, data, gas_limit, max_fee_per_gas, max_priority_fee_per_gas)
    }

    #[inline]
    pub fn sign_legacy_transaction(
        &self,
        to: Address,
        value: U256,
        data: Bytes,
        gas_limit: u64,
        gas_price: U256,
    ) -> WalletResult<Transaction> {
        self.sign_legacy(to, value, data, gas_limit, gas_price)
    }

    // ==================== Transaction Sending ====================

    /// Send a pre-signed transaction
    pub async fn send_signed(&self, tx: &Transaction) -> WalletResult<B256> {
        let _permit = self.pending_semaphore.acquire().await.unwrap();

        let hex_tx = tx.to_hex();

        // If we have multiple RPC endpoints, broadcast to all
        let result = if let Some(batch_client) = &self.batch_client {
            batch_client.broadcast_transaction(&hex_tx).await
        } else {
            self.rpc_client.send_raw_transaction(&hex_tx).await
        };

        match &result {
            Ok(_) => {
                // Transaction accepted
            }
            Err(e) => {
                // Mark nonce as failed
                self.nonce_manager.fail(tx.nonce());

                // Check for specific errors
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("nonce too low") {
                    // Need to resync nonce
                    let _ = self.sync_nonce().await;
                }
            }
        }

        result
    }

    /// Sign and send a transaction in one call
    pub async fn send(&self, request: TransactionRequest) -> WalletResult<B256> {
        let tx = self.sign(request)?;
        self.send_signed(&tx).await
    }

    /// Sign and send using preheated context
    pub async fn send_with_preheat(
        &self,
        ctx: &PreheatedContext,
        request: TransactionRequest,
    ) -> WalletResult<B256> {
        let tx = self.sign_with_preheat(ctx, request)?;
        self.send_signed(&tx).await
    }

    // Backwards compatibility alias
    pub async fn sign_and_send(&self, request: TransactionRequest) -> WalletResult<B256> {
        self.send(request).await
    }

    /// Send ETH transfer (convenience method)
    pub async fn send_eth(
        &self,
        to: Address,
        value: U256,
        gas_price: Option<U256>,
    ) -> WalletResult<B256> {
        let gas_price = match gas_price {
            Some(p) => p,
            None => self.get_gas_price().await?,
        };

        let request = TransactionRequest::new()
            .to(to)
            .value(value)
            .gas_limit(21000)
            .gas_price(gas_price);

        self.send(request).await
    }

    /// Send contract call (convenience method)
    pub async fn send_contract_call(
        &self,
        to: Address,
        data: Bytes,
        gas_limit: u64,
        value: Option<U256>,
    ) -> WalletResult<B256> {
        let gas_price = self.get_gas_price().await?;

        let mut request = TransactionRequest::new()
            .to(to)
            .data(data)
            .gas_limit(gas_limit)
            .gas_price(gas_price);

        if let Some(v) = value {
            request = request.value(v);
        }

        self.send(request).await
    }

    /// Send multiple transactions in parallel
    ///
    /// This is the fastest way to send multiple transactions.
    /// Returns results for each transaction (in order).
    pub async fn send_batch(&self, requests: Vec<TransactionRequest>) -> Vec<WalletResult<B256>> {
        // Sign all transactions first (CPU-bound, sequential for nonce ordering)
        let signed: Vec<_> = requests.into_iter().map(|req| self.sign(req)).collect();

        // Send all in parallel (IO-bound)
        let futures: Vec<_> = signed
            .into_iter()
            .map(|tx_result| async move {
                match tx_result {
                    Ok(tx) => self.send_signed(&tx).await,
                    Err(e) => Err(e),
                }
            })
            .collect();

        join_all(futures).await
    }

    /// Wait for transaction confirmation
    pub async fn wait_for_confirmation(&self, tx_hash: B256) -> WalletResult<serde_json::Value> {
        self.rpc_client
            .wait_for_receipt(
                tx_hash,
                self.config.confirmation_timeout,
                self.config.poll_interval,
            )
            .await
    }
}

// Manual Debug impl to avoid exposing private key
impl std::fmt::Debug for FastWallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastWallet")
            .field("address", &self.address())
            .field("current_nonce", &self.current_nonce())
            .field("pending_count", &self.pending_count())
            .field("chain_id", &self.config.chain_id)
            .field("warmed_up", &self.is_warmed_up())
            .finish()
    }
}

/// Builder for creating FastWallet with custom configuration
pub struct FastWalletBuilder {
    private_key: String,
    primary_rpc: String,
    broadcast_rpcs: Vec<String>,
    config: WalletConfig,
    initial_nonce: Option<u64>,
}

impl FastWalletBuilder {
    pub fn new(private_key: impl Into<String>, primary_rpc: impl Into<String>) -> Self {
        Self {
            private_key: private_key.into(),
            primary_rpc: primary_rpc.into(),
            broadcast_rpcs: Vec::new(),
            config: WalletConfig::default(),
            initial_nonce: None,
        }
    }

    /// Add RPC endpoints for parallel broadcasting
    pub fn broadcast_rpcs(mut self, rpcs: Vec<String>) -> Self {
        self.broadcast_rpcs = rpcs;
        self
    }

    /// Set chain ID
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.config.chain_id = chain_id;
        self
    }

    /// Set initial nonce (skip RPC call)
    pub fn initial_nonce(mut self, nonce: u64) -> Self {
        self.initial_nonce = Some(nonce);
        self
    }

    /// Set max pending transactions
    pub fn max_pending_txs(mut self, max: usize) -> Self {
        self.config.max_pending_txs = max;
        self
    }

    /// Use EIP-1559 transactions by default
    pub fn use_eip1559(mut self, use_eip1559: bool) -> Self {
        self.config.use_eip1559 = use_eip1559;
        self
    }

    /// Set confirmation timeout
    pub fn confirmation_timeout(mut self, timeout: Duration) -> Self {
        self.config.confirmation_timeout = timeout;
        self
    }

    /// Set gas cache duration
    pub fn gas_cache_duration(mut self, duration: Duration) -> Self {
        self.config.gas_cache_duration = duration;
        self
    }

    /// Build the wallet (async - fetches nonce from chain if not provided)
    pub async fn build(self) -> WalletResult<FastWallet> {
        if let Some(nonce) = self.initial_nonce {
            let mut wallet = FastWallet::with_known_nonce(
                &self.private_key,
                &self.primary_rpc,
                nonce,
                self.config,
            )?;

            if !self.broadcast_rpcs.is_empty() {
                wallet.batch_client = Some(Arc::new(BatchRpcClient::new(self.broadcast_rpcs)?));
            }

            Ok(wallet)
        } else if !self.broadcast_rpcs.is_empty() {
            FastWallet::with_multiple_rpcs(
                &self.private_key,
                &self.primary_rpc,
                self.broadcast_rpcs,
                self.config,
            )
            .await
        } else {
            FastWallet::new(&self.private_key, &self.primary_rpc, self.config).await
        }
    }

    /// Build with known nonce (synchronous - no RPC call)
    pub fn build_with_nonce(self, nonce: u64) -> WalletResult<FastWallet> {
        let mut wallet = FastWallet::with_known_nonce(
            &self.private_key,
            &self.primary_rpc,
            nonce,
            self.config,
        )?;

        if !self.broadcast_rpcs.is_empty() {
            wallet.batch_client = Some(Arc::new(BatchRpcClient::new(self.broadcast_rpcs)?));
        }

        Ok(wallet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PRIVATE_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[test]
    fn test_wallet_builder_sync() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .max_pending_txs(50)
            .build_with_nonce(0)
            .unwrap();

        assert_eq!(wallet.current_nonce(), 0);
        assert_eq!(wallet.config.chain_id, 1);
    }

    #[test]
    fn test_sign_auto_nonce() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();

        // No nonce specified - should auto-assign
        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64));

        let tx = wallet.sign(request).unwrap();
        assert_eq!(tx.nonce(), 0);
        assert!(!tx.encoded().is_empty());
    }

    #[test]
    fn test_nonce_auto_increment() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(10)
            .unwrap();

        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64));

        let tx1 = wallet.sign(request.clone()).unwrap();
        let tx2 = wallet.sign(request.clone()).unwrap();
        let tx3 = wallet.sign(request).unwrap();

        assert_eq!(tx1.nonce(), 10);
        assert_eq!(tx2.nonce(), 11);
        assert_eq!(tx3.nonce(), 12);
        assert_eq!(wallet.current_nonce(), 13);
    }

    #[test]
    fn test_preheated_context() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(100)
            .unwrap();

        // Create preheated context (without gas fetch since no RPC)
        let ctx = PreheatedContext::new(wallet.nonce_manager.get_nonce());
        assert_eq!(ctx.nonce, 100);
        assert!(ctx.is_valid(Duration::from_secs(60)));

        // Use it
        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64));

        let tx = wallet.sign_with_preheat(&ctx, request).unwrap();
        assert_eq!(tx.nonce(), 100);

        // Context should be marked as used
        assert!(!ctx.is_valid(Duration::from_secs(60)));
    }

    #[test]
    fn test_preheated_context_double_use() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();

        let ctx = PreheatedContext::new(wallet.nonce_manager.get_nonce());

        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64));

        // First use should succeed
        let _ = wallet.sign_with_preheat(&ctx, request.clone()).unwrap();

        // Second use should fail
        let result = wallet.sign_with_preheat(&ctx, request);
        assert!(result.is_err());
    }

    #[test]
    fn test_eip1559_transaction() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();

        let tx = wallet
            .sign_eip1559(
                Address::repeat_byte(1),
                U256::from(1_000_000_000_000_000_000u64),
                Bytes::new(),
                21000,
                U256::from(50_000_000_000u64), // 50 gwei max
                U256::from(2_000_000_000u64),  // 2 gwei priority
            )
            .unwrap();

        // EIP-1559 transactions start with 0x02
        assert_eq!(tx.encoded()[0], 0x02);
    }

    #[test]
    fn test_legacy_transaction() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();

        let tx = wallet
            .sign_legacy(
                Address::repeat_byte(1),
                U256::from(1_000_000_000_000_000_000u64),
                Bytes::new(),
                21000,
                U256::from(20_000_000_000u64),
            )
            .unwrap();

        // Legacy transactions don't have a type prefix (starts with RLP list header)
        assert!(tx.encoded()[0] >= 0xc0); // RLP list
    }
}

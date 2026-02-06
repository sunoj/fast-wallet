//! Fast EVM Wallet - Core implementation
//!
//! This module provides the main wallet interface that combines:
//! - Fast signing with automatic nonce management
//! - Pre-warming/preheating for lowest latency
//! - Async transaction submission
//! - Parallel transaction broadcasting
//! - Gas request coalescing for reduced RPC calls

use crate::error::{WalletError, WalletResult};
use crate::nonce::SingleAddressNonceManager;
use crate::rpc::{BatchRpcClient, RpcClient};
use crate::signer::FastSigner;
use crate::transaction::{Transaction, TransactionRequest};
use alloy::primitives::{Address, Bytes, B256, U256};
use futures::future::join_all;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Gas fetch serializer - uses double-checked locking to ensure only one RPC call
/// is made when multiple concurrent requests need gas prices.
struct GasFetchSerializer {
    /// Mutex to serialize gas price fetch (only one RPC call at a time)
    gas_price_fetch: tokio::sync::Mutex<()>,
    /// Mutex to serialize priority fee fetch
    priority_fee_fetch: tokio::sync::Mutex<()>,
}

impl GasFetchSerializer {
    fn new() -> Self {
        Self {
            gas_price_fetch: tokio::sync::Mutex::new(()),
            priority_fee_fetch: tokio::sync::Mutex::new(()),
        }
    }
}

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
    /// Background gas refresh interval (None = disabled)
    pub background_gas_refresh: Option<Duration>,
    /// Default gas price for optimistic sends when cache is empty (in wei)
    /// Default: 1 gwei
    pub optimistic_default_gas_price: U256,
    /// Default gas limit for optimistic liquidation sends
    /// Default: 150,000 (typical liquidation cost)
    pub optimistic_default_gas_limit: u64,
    /// Maximum age for cached gas prices before they are considered stale
    /// Even if background refresh is running, prices older than this are rejected.
    /// Default: 30 seconds
    pub max_gas_cache_age: Duration,
    /// Timeout for acquiring pending transaction permit
    /// Default: 100ms (fail fast for liquidation bots)
    pub pending_acquire_timeout: Duration,
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
            background_gas_refresh: None, // Disabled by default
            optimistic_default_gas_price: U256::from(1_000_000_000u64), // 1 gwei
            optimistic_default_gas_limit: 150_000, // Typical liquidation gas
            max_gas_cache_age: Duration::from_secs(30),
            pending_acquire_timeout: Duration::from_millis(100),
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
/// - Gas price caching with request coalescing
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
    /// Whether warmup has been done
    warmed_up: AtomicBool,
    /// Gas fetch serializer (double-checked locking for coalescing)
    gas_serializer: GasFetchSerializer,
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
            warmed_up: AtomicBool::new(false),
            gas_serializer: GasFetchSerializer::new(),
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
            warmed_up: AtomicBool::new(false),
            gas_serializer: GasFetchSerializer::new(),
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

    /// Start background gas price refresh
    ///
    /// This spawns a background task that periodically fetches gas prices,
    /// ensuring they're always fresh when `preheat()` is called.
    ///
    /// Returns a handle that can be used to stop the background task.
    pub fn start_background_gas_refresh(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let interval = self.config.background_gas_refresh?;
        let wallet = Arc::clone(self);

        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                // Refresh gas prices - ignore errors
                let _ = tokio::join!(
                    wallet.get_gas_price(),
                    wallet.get_priority_fee()
                );
            }
        }))
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

    // ==================== Connection Warmup ====================

    /// Warm up HTTP connections with detailed results
    ///
    /// This establishes TCP/TLS connections to:
    /// - Primary RPC endpoint
    /// - All batch RPC endpoints (if configured)
    ///
    /// Can save 5-20ms per endpoint on subsequent requests.
    /// Call this once during initialization or before time-critical operations.
    ///
    /// Returns (primary_ok, batch_count) where batch_count is the number
    /// of successfully warmed batch endpoints.
    pub async fn warmup_connections(&self) -> (bool, usize) {
        // Warm up primary RPC
        let primary_ok = self.rpc_client.warmup().await.is_ok();

        // Warm up batch RPC endpoints
        let batch_count = if let Some(batch_client) = &self.batch_client {
            batch_client.warmup().await
        } else {
            0
        };

        (primary_ok, batch_count)
    }

    /// Full preheat: warm connections + allocate nonce + fetch gas
    ///
    /// This is the most comprehensive preheat, ideal for liquidation bots:
    /// 1. Warms up HTTP connections (TCP/TLS handshakes)
    /// 2. Allocates a nonce
    /// 3. Fetches current gas prices
    ///
    /// Call this ~100-500ms before you expect to send a transaction.
    pub async fn preheat_full(&self) -> WalletResult<PreheatedContext> {
        // Warm connections and fetch gas in parallel
        let (_, ctx) = tokio::join!(
            self.warmup_connections(),
            self.preheat(true)
        );
        ctx
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

    /// Get cached or fresh gas price (with double-checked locking for coalescing)
    ///
    /// Uses a tokio::sync::Mutex to serialize RPC calls: multiple concurrent callers
    /// will share a single fetch result. This is correct even if the fetch fails.
    pub async fn get_gas_price(&self) -> WalletResult<U256> {
        // Fast path: check cache (lock-free read)
        if let Some(price) = self.read_gas_cache(&self.gas_price_cache) {
            return Ok(price);
        }

        // Slow path: serialize fetch via async mutex (only one RPC call at a time)
        let _guard = self.gas_serializer.gas_price_fetch.lock().await;

        // Double-check: another thread may have refreshed while we waited
        if let Some(price) = self.read_gas_cache(&self.gas_price_cache) {
            return Ok(price);
        }

        // We're the sole fetcher - make the RPC call
        let price = self.rpc_client.gas_price().await?;

        // Update cache
        {
            let mut cache = self.gas_price_cache.write();
            *cache = Some((price, Instant::now()));
        }

        Ok(price)
    }

    /// Get cached or fresh priority fee (with double-checked locking for coalescing)
    pub async fn get_priority_fee(&self) -> WalletResult<U256> {
        // Fast path: check cache (lock-free read)
        if let Some(fee) = self.read_gas_cache(&self.priority_fee_cache) {
            return Ok(fee);
        }

        // Slow path: serialize fetch
        let _guard = self.gas_serializer.priority_fee_fetch.lock().await;

        // Double-check
        if let Some(fee) = self.read_gas_cache(&self.priority_fee_cache) {
            return Ok(fee);
        }

        let fee = self.rpc_client.max_priority_fee().await?;

        {
            let mut cache = self.priority_fee_cache.write();
            *cache = Some((fee, Instant::now()));
        }

        Ok(fee)
    }

    /// Read a gas cache value if it's fresh enough.
    /// Returns None if cache is empty, expired, or exceeds max age.
    #[inline]
    fn read_gas_cache(&self, cache: &RwLock<Option<(U256, Instant)>>) -> Option<U256> {
        let guard = cache.read();
        guard.as_ref().and_then(|(price, timestamp)| {
            let age = timestamp.elapsed();
            if age < self.config.gas_cache_duration && age < self.config.max_gas_cache_age {
                Some(*price)
            } else {
                None
            }
        })
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

    /// Sign a transaction with explicit nonce (does NOT increment internal counter)
    ///
    /// Use this when you want to pre-sign multiple alternative transactions
    /// with the same nonce and choose which one to send later.
    ///
    /// WARNING: Using this incorrectly can result in nonce conflicts.
    #[inline]
    pub fn sign_with_nonce(&self, mut request: TransactionRequest, nonce: u64) -> WalletResult<Transaction> {
        request.nonce = nonce;
        request.chain_id = self.config.chain_id;

        if request.gas_limit == 0 {
            request.gas_limit = self.config.default_gas_limit;
        }

        request.build_and_sign(&self.signer)
    }

    /// Sign multiple alternative transactions with the same nonce
    ///
    /// Useful for liquidation bots that want to pre-sign different strategies
    /// and choose the best one based on simulation results.
    ///
    /// Returns transactions signed with the CURRENT nonce (without incrementing).
    #[inline]
    pub fn sign_alternatives(&self, requests: Vec<TransactionRequest>) -> Vec<WalletResult<Transaction>> {
        let nonce = self.nonce_manager.peek();
        requests
            .into_iter()
            .map(|req| self.sign_with_nonce(req, nonce))
            .collect()
    }

    /// Reserve a nonce for later use
    ///
    /// Increments the internal counter and returns the nonce.
    /// Use with `sign_with_nonce()` to sign a transaction later.
    #[inline]
    pub fn reserve_nonce(&self) -> u64 {
        self.nonce_manager.get_nonce()
    }

    /// Release a reserved nonce (mark as failed)
    ///
    /// Call this if you reserved a nonce but decided not to use it.
    #[inline]
    pub fn release_nonce(&self, nonce: u64) {
        self.nonce_manager.fail(nonce);
    }

    // ==================== Transaction Signing ====================

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

    // ==================== Transaction Sending ====================

    /// Send a pre-signed transaction
    ///
    /// Uses a timeout when acquiring the pending transaction permit to avoid
    /// blocking indefinitely when many transactions are in-flight.
    pub async fn send_signed(&self, tx: &Transaction) -> WalletResult<B256> {
        let _permit = tokio::time::timeout(
            self.config.pending_acquire_timeout,
            self.pending_semaphore.acquire(),
        )
        .await
        .map_err(|_| WalletError::Timeout)?
        .map_err(|_| WalletError::RpcError("Semaphore closed".to_string()))?;

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

    // ========================================================================
    // Optimistic Execution (skip simulation for lowest latency)
    // ========================================================================

    /// Send transaction optimistically without simulation
    ///
    /// This skips eth_call simulation and sends directly to the network.
    /// Use this when:
    /// - Speed is more important than gas savings
    /// - Your contract reverts early with minimal gas cost on failure
    /// - You're competing for MEV opportunities
    ///
    /// Failed transactions still consume gas up to the revert point.
    /// For liquidation contracts that check health factor early, this is typically 3-5k gas.
    #[inline]
    pub async fn send_optimistic(&self, request: TransactionRequest) -> WalletResult<B256> {
        let tx = self.sign(request)?;
        self.send_signed(&tx).await
    }

    /// Send optimistic liquidation with pre-configured low gas settings
    ///
    /// Optimized for liquidation bots:
    /// - Uses provided gas price (no RPC fetch)
    /// - Uses provided gas limit (no estimation)
    /// - Skips all simulation
    ///
    /// # Arguments
    /// * `to` - Liquidation contract address
    /// * `data` - Encoded liquidation call (e.g., `liquidate(user, asset, amount)`)
    /// * `gas_limit` - Pre-calculated gas limit for the liquidation
    /// * `gas_price` - Pre-fetched gas price (from background refresh)
    ///
    /// # Example
    /// ```ignore
    /// // Pre-calculate during idle time
    /// let gas_limit = 150_000; // Known liquidation cost
    /// let gas_price = wallet.get_gas_price().await?; // From cache
    ///
    /// // When opportunity detected - fastest path
    /// let tx_hash = wallet.send_optimistic_liquidation(
    ///     liquidation_contract,
    ///     encoded_call,
    ///     gas_limit,
    ///     gas_price,
    /// ).await?;
    /// ```
    #[inline]
    pub async fn send_optimistic_liquidation(
        &self,
        to: Address,
        data: Bytes,
        gas_limit: u64,
        gas_price: U256,
    ) -> WalletResult<B256> {
        let request = TransactionRequest::new()
            .to(to)
            .data(data)
            .gas_limit(gas_limit)
            .gas_price(gas_price);

        self.send_optimistic(request).await
    }

    /// Send optimistic with preheated context (fastest possible path)
    ///
    /// Combines preheating with optimistic execution:
    /// - Nonce already reserved
    /// - Gas prices already cached
    /// - No simulation
    /// - Direct to network
    ///
    /// This is the absolute lowest latency path for liquidation bots.
    #[inline]
    pub async fn send_optimistic_with_preheat(
        &self,
        ctx: &PreheatedContext,
        to: Address,
        data: Bytes,
        gas_limit: u64,
    ) -> WalletResult<B256> {
        let gas_price = ctx
            .gas_price
            .unwrap_or(self.config.optimistic_default_gas_price);

        let request = TransactionRequest::new()
            .to(to)
            .data(data)
            .gas_limit(gas_limit)
            .gas_price(gas_price);

        let tx = self.sign_with_preheat(ctx, request)?;
        self.send_signed(&tx).await
    }

    /// Quick liquidation with minimal parameters (uses config defaults)
    ///
    /// Fastest path when you just have the contract call data:
    /// - Uses preheated or cached gas price
    /// - Uses default gas limit from config
    /// - No simulation, no RPC calls
    #[inline]
    pub async fn send_quick_liquidation(
        &self,
        to: Address,
        data: Bytes,
    ) -> WalletResult<B256> {
        let gas_price = self.gas_price_cache.read()
            .as_ref()
            .and_then(|(price, ts)| {
                if ts.elapsed() < self.config.max_gas_cache_age {
                    Some(*price)
                } else {
                    None
                }
            })
            .unwrap_or(self.config.optimistic_default_gas_price);

        let request = TransactionRequest::new()
            .to(to)
            .data(data)
            .gas_limit(self.config.optimistic_default_gas_limit)
            .gas_price(gas_price);

        self.send_optimistic(request).await
    }

    /// Fire-and-forget optimistic send (don't wait for tx hash)
    ///
    /// Returns immediately after signing, sends in background.
    /// Use when you need absolute minimum latency and don't need the tx hash immediately.
    ///
    /// # Returns
    /// - Transaction hash (computed locally, not confirmed by RPC)
    /// - JoinHandle to await the actual send result if needed
    #[inline]
    pub fn send_optimistic_fire_and_forget(
        self: &Arc<Self>,
        request: TransactionRequest,
    ) -> WalletResult<(B256, tokio::task::JoinHandle<WalletResult<B256>>)> {
        let tx = self.sign(request)?;
        let tx_hash = tx.hash();
        let wallet = Arc::clone(self);

        let handle = tokio::spawn(async move { wallet.send_signed(&tx).await });

        Ok((tx_hash, handle))
    }

    // ========================================================================
    // Regular send methods
    // ========================================================================

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

    /// Create a wallet using Blink Labs RPC (Ethereum mainnet)
    ///
    /// Blink Labs provides low-latency RPC optimized for MEV and liquidation bots.
    ///
    /// # Example
    /// ```ignore
    /// let wallet = FastWalletBuilder::with_blink(
    ///     "0xYOUR_PRIVATE_KEY",
    ///     "your_blink_api_key"
    /// )
    /// .chain_id(1)
    /// .build()
    /// .await?;
    /// ```
    pub fn with_blink(private_key: impl Into<String>, api_key: impl Into<String>) -> Self {
        let api_key = api_key.into();
        Self {
            private_key: private_key.into(),
            primary_rpc: format!("https://eth.blinklabs.xyz/v1/{}", api_key),
            broadcast_rpcs: Vec::new(),
            config: WalletConfig::default(),
            initial_nonce: None,
        }
    }

    /// Create a wallet using Blink Labs RPC for a specific chain
    ///
    /// Supported chains: eth, arb, base, opt, polygon
    pub fn with_blink_chain(
        private_key: impl Into<String>,
        chain: &str,
        api_key: impl Into<String>,
    ) -> Self {
        let api_key = api_key.into();
        Self {
            private_key: private_key.into(),
            primary_rpc: format!("https://{}.blinklabs.xyz/v1/{}", chain, api_key),
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

    /// Add Blink Labs RPC as a broadcast endpoint
    pub fn add_blink_broadcast(mut self, api_key: impl Into<String>) -> Self {
        self.broadcast_rpcs
            .push(format!("https://eth.blinklabs.xyz/v1/{}", api_key.into()));
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

    /// Set background gas refresh interval
    pub fn background_gas_refresh(mut self, interval: Duration) -> Self {
        self.config.background_gas_refresh = Some(interval);
        self
    }

    /// Set default gas price for optimistic sends
    pub fn optimistic_default_gas_price(mut self, price: U256) -> Self {
        self.config.optimistic_default_gas_price = price;
        self
    }

    /// Set default gas limit for optimistic liquidation sends
    pub fn optimistic_default_gas_limit(mut self, limit: u64) -> Self {
        self.config.optimistic_default_gas_limit = limit;
        self
    }

    /// Set the entire config at once
    pub fn config(mut self, config: WalletConfig) -> Self {
        self.config = config;
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

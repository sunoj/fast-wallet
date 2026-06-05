//! Fast EVM Wallet - Core implementation
//!
//! This module provides the main wallet interface that combines:
//! - Fast signing with automatic nonce management
//! - Pre-warming/preheating for lowest latency
//! - Async transaction submission
//! - Parallel transaction broadcasting
//! - Gas request coalescing for reduced RPC calls

use crate::error::{WalletError, WalletResult};
use crate::gas_provider::GasPriceProvider;
use crate::inflight::{InflightNonceLedger, InflightNonceSnapshot};
use crate::nonce::{ReservedNonce, SingleAddressNonceManager};
use crate::rpc::{BatchRpcClient, RpcClient, SendResult};
use crate::signer::FastSigner;
use crate::transaction::{Transaction, TransactionRequest};
use alloy::primitives::{Address, Bytes, B256, U256};
use futures_util::future::join_all;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::info;

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
#[derive(Clone)]
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
    /// Explicit gas price cache TTL override.
    ///
    /// `None` (default) auto-infers the effective TTL:
    /// - if `background_gas_refresh` is set, the cache stays fresh for that
    ///   interval (so each refreshed value is valid until the next tick);
    /// - otherwise, falls back to `DEFAULT_GAS_CACHE_FALLBACK` (500ms).
    ///
    /// `max_gas_cache_age` is always also enforced as an upper bound.
    /// Set `Some(_)` to opt in to a stricter freshness window.
    pub gas_cache_duration: Option<Duration>,
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
    /// External chain-level gas provider.
    ///
    /// When `Some`, the wallet's own gas cache and `start_background_gas_refresh`
    /// task are bypassed entirely — the provider becomes the sole source for
    /// `get_gas_price()`, `get_priority_fee()`, `refresh_gas_prices()`, and
    /// the gas values populated into `PreheatedContext` by `preheat(true)`.
    pub gas_provider: Option<Arc<dyn GasPriceProvider>>,
}

impl std::fmt::Debug for WalletConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalletConfig")
            .field("chain_id", &self.chain_id)
            .field("default_gas_limit", &self.default_gas_limit)
            .field("max_pending_txs", &self.max_pending_txs)
            .field("confirmation_timeout", &self.confirmation_timeout)
            .field("poll_interval", &self.poll_interval)
            .field("use_eip1559", &self.use_eip1559)
            .field("gas_cache_duration", &self.gas_cache_duration)
            .field("background_gas_refresh", &self.background_gas_refresh)
            .field(
                "optimistic_default_gas_price",
                &self.optimistic_default_gas_price,
            )
            .field(
                "optimistic_default_gas_limit",
                &self.optimistic_default_gas_limit,
            )
            .field("max_gas_cache_age", &self.max_gas_cache_age)
            .field("pending_acquire_timeout", &self.pending_acquire_timeout)
            .field(
                "gas_provider",
                &self.gas_provider.as_ref().map(|_| "<dyn GasPriceProvider>"),
            )
            .finish()
    }
}

/// Fallback gas cache TTL when neither `gas_cache_duration` nor
/// `background_gas_refresh` is configured.
const DEFAULT_GAS_CACHE_FALLBACK: Duration = Duration::from_millis(500);

impl WalletConfig {
    /// Resolve the effective gas-cache TTL based on the current config.
    ///
    /// Resolution order:
    /// 1. explicit `gas_cache_duration`
    /// 2. `background_gas_refresh` interval (so a refreshed value stays
    ///    valid until the next refresh tick)
    /// 3. `DEFAULT_GAS_CACHE_FALLBACK` (500ms)
    #[inline]
    pub fn effective_gas_cache_duration(&self) -> Duration {
        self.gas_cache_duration
            .or(self.background_gas_refresh)
            .unwrap_or(DEFAULT_GAS_CACHE_FALLBACK)
    }
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
            gas_cache_duration: None,
            background_gas_refresh: None, // Disabled by default
            optimistic_default_gas_price: U256::from(1_000_000_000u64), // 1 gwei
            optimistic_default_gas_limit: 150_000, // Typical liquidation gas
            max_gas_cache_age: Duration::from_secs(30),
            pending_acquire_timeout: Duration::from_millis(100),
            gas_provider: None,
        }
    }
}

/// Preheated transaction context - prepared signing context for fastest execution
///
/// Use this when you know a transaction is likely coming but don't have all params yet.
/// Call `wallet.preheat()` to get this context, then use it to sign when ready.
pub struct PreheatedContext {
    /// Pre-allocated nonce. For warmup-only contexts (see
    /// [`Wallet::preheat_warmup_only`]) this is [`u64::MAX`] until the nonce is
    /// lazily reserved in [`Wallet::sign_with_preheat`]; do NOT read it as the
    /// broadcast nonce — the bound value is sourced from `reservation`.
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
    /// RAII nonce reservation backing this preheat. `None` for a warmup-only
    /// context until `sign_with_preheat` reserves one at sign time.
    reservation: parking_lot::Mutex<Option<ReservedNonce>>,
}

impl PreheatedContext {
    fn new(reservation: ReservedNonce) -> Self {
        let nonce = reservation.nonce();
        Self {
            nonce,
            gas_price: None,
            priority_fee: None,
            max_fee: None,
            preheated_at: Instant::now(),
            used: AtomicBool::new(false),
            reservation: parking_lot::Mutex::new(Some(reservation)),
        }
    }

    /// Build a warmup-only context with NO nonce reservation. The nonce is
    /// reserved lazily in [`Wallet::sign_with_preheat`] so that broadcasts bind
    /// nonces in send order rather than preheat order (avoids out-of-order
    /// strands when many contexts are held concurrently across a long
    /// pre-broadcast wait).
    fn new_warmup_only() -> Self {
        Self {
            nonce: u64::MAX,
            gas_price: None,
            priority_fee: None,
            max_fee: None,
            preheated_at: Instant::now(),
            used: AtomicBool::new(false),
            reservation: parking_lot::Mutex::new(None),
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

    fn mark_broadcasting(&self) -> WalletResult<()> {
        let mut reservation = self.reservation.lock();
        let Some(reservation) = reservation.as_mut() else {
            return Err(WalletError::NonceError(
                "PreheatedContext reservation already finalized".to_string(),
            ));
        };
        if reservation.mark_broadcasting() {
            Ok(())
        } else {
            Err(WalletError::NonceError(
                "PreheatedContext reservation already finalized".to_string(),
            ))
        }
    }

    fn commit_reservation(&self) {
        self.used.store(true, Ordering::Release);
        let mut reservation = self.reservation.lock();
        if let Some(mut reservation) = reservation.take() {
            reservation.commit();
        }
    }

    fn release_reservation(&self) {
        self.used.store(true, Ordering::Release);
        let mut reservation = self.reservation.lock();
        if let Some(mut reservation) = reservation.take() {
            reservation.release();
        }
    }

    fn reserved_nonce(&self) -> Option<u64> {
        self.reservation.lock().as_ref().map(|nonce| nonce.nonce())
    }
}

/// Wallet status returned by `self_check()`
#[derive(Debug, Clone)]
pub struct WalletStatus {
    /// Wallet address
    pub address: Address,
    /// Balance in wei
    pub balance: U256,
    /// Latest nonce from chain (pending)
    pub nonce: u64,
    /// Chain ID
    pub chain_id: u64,
}

impl WalletStatus {
    /// Get balance in ETH (as f64, for display purposes)
    pub fn balance_eth(&self) -> f64 {
        let wei = self.balance;
        // Convert to f64: divide by 1e18
        let divisor = U256::from(1_000_000_000_000_000_000u64);
        let whole = wei / divisor;
        let remainder = wei % divisor;
        whole.to::<u64>() as f64 + remainder.to::<u64>() as f64 / 1e18
    }
}

impl std::fmt::Display for WalletStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Address: {:?} | Balance: {:.6} ETH ({} wei) | Nonce: {} | Chain: {}",
            self.address,
            self.balance_eth(),
            self.balance,
            self.nonce,
            self.chain_id,
        )
    }
}

/// Nonce health state returned by `nonce_health_check()`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonceHealth {
    /// Local high-water nonce.
    pub current: u64,
    /// Next nonce that would be reserved, including recycled gaps.
    pub effective_next: u64,
    /// Next nonce reported by the chain.
    pub chain_next: u64,
    /// Next nonce reported by latest state, excluding provider-local pending.
    pub chain_latest: Option<u64>,
    /// Number of recycled gaps available for reuse.
    pub gap_count: u64,
    /// Count of signed/broadcast nonces not released or mined in the local ledger.
    pub inflight_count: usize,
    /// Lowest unresolved local ledger entry at or above `chain_next`.
    pub lowest_unresolved: Option<InflightNonceSnapshot>,
    /// Whether effective next nonce is far ahead of chain.
    pub stalled: bool,
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
    gas_rpc_client: Option<Arc<RpcClient>>,
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
    /// Local per-nonce signed/broadcast ledger for recovery observability.
    inflight_nonces: InflightNonceLedger,
}

impl FastWallet {
    /// Create a new wallet with a single RPC endpoint
    ///
    /// This will fetch the initial nonce from the chain.
    pub async fn new(private_key: &str, rpc_url: &str, config: WalletConfig) -> WalletResult<Self> {
        let signer = FastSigner::from_hex(private_key)?;
        let rpc_client = Arc::new(RpcClient::new(rpc_url)?);

        // Fetch initial nonce
        let initial_nonce = rpc_client.get_nonce(signer.address()).await?;

        let nonce_manager = SingleAddressNonceManager::new(signer.address(), initial_nonce);

        Ok(Self {
            signer,
            nonce_manager,
            rpc_client,
            gas_rpc_client: None,
            batch_client: None,
            pending_semaphore: Semaphore::new(config.max_pending_txs),
            gas_price_cache: RwLock::new(None),
            priority_fee_cache: RwLock::new(None),
            warmed_up: AtomicBool::new(false),
            gas_serializer: GasFetchSerializer::new(),
            inflight_nonces: InflightNonceLedger::default(),
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
            let mut all_rpcs = vec![primary_rpc.to_string()];
            all_rpcs.extend(broadcast_rpcs);
            wallet.batch_client = Some(Arc::new(BatchRpcClient::new(all_rpcs)?));
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
            gas_rpc_client: None,
            batch_client: None,
            pending_semaphore: Semaphore::new(config.max_pending_txs),
            gas_price_cache: RwLock::new(None),
            priority_fee_cache: RwLock::new(None),
            warmed_up: AtomicBool::new(false),
            gas_serializer: GasFetchSerializer::new(),
            inflight_nonces: InflightNonceLedger::default(),
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

    /// Get the next nonce that would be reserved, including recycled gaps
    #[inline]
    pub fn effective_next_nonce(&self) -> u64 {
        self.nonce_manager.effective_next_nonce()
    }

    /// Get pending transaction count
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.nonce_manager.pending_count()
    }

    /// Recover a stranded-nonce stall by rewinding the local nonce to
    /// `chain_nonce`, but only when nothing is in flight (`pending_count == 0`)
    /// and the chain is behind local. See [`NonceTracker::recover_stalled`].
    /// Returns true if it rewound.
    ///
    /// Intended for a health-check task that has observed a sustained stall
    /// (`nonce_health_check().stalled` with `pending_count() == 0` across
    /// several checks). The `pending_count == 0` gate makes it safe to call;
    /// the sustained-stall precondition avoids clobbering a slow-to-mine tx.
    #[inline]
    pub fn recover_stalled(&self, chain_nonce: u64) -> bool {
        self.nonce_manager.recover_stalled(chain_nonce)
    }

    /// Get RPC client reference
    pub fn rpc(&self) -> &RpcClient {
        &self.rpc_client
    }

    fn gas_rpc(&self) -> &Arc<RpcClient> {
        self.gas_rpc_client.as_ref().unwrap_or(&self.rpc_client)
    }

    /// Get config reference
    pub fn config(&self) -> &WalletConfig {
        &self.config
    }

    // ==================== Self Check / Status ====================

    /// Perform startup self-check: query balance, nonce from chain and log wallet status
    ///
    /// This is intended to be called once after wallet creation to verify
    /// connectivity and display the wallet's on-chain state.
    ///
    /// Logs address, balance (in ETH), and latest nonce via `tracing::info!`.
    pub async fn self_check(&self) -> WalletResult<WalletStatus> {
        let address = self.address();

        // Fetch balance and nonce in parallel
        let (balance_result, nonce_result) = tokio::join!(
            self.rpc_client.get_balance(address),
            self.rpc_client.get_nonce(address),
        );

        let balance = balance_result?;
        let nonce = nonce_result?;

        // Sync local nonce manager with chain state
        self.nonce_manager.sync(nonce);

        let status = WalletStatus {
            address,
            balance,
            nonce,
            chain_id: self.config.chain_id,
        };

        info!("========== Wallet Self-Check ==========");
        info!("  Address:  {:?}", status.address);
        info!(
            "  Balance:  {:.6} ETH ({} wei)",
            status.balance_eth(),
            status.balance
        );
        info!("  Nonce:    {}", status.nonce);
        info!("  Chain ID: {}", status.chain_id);
        info!("=======================================");

        Ok(status)
    }

    /// Query the wallet's current on-chain balance
    pub async fn get_balance(&self) -> WalletResult<U256> {
        self.rpc_client.get_balance(self.address()).await
    }

    /// Query the wallet's current on-chain nonce (pending, includes mempool)
    pub async fn get_nonce(&self) -> WalletResult<u64> {
        self.rpc_client.get_nonce(self.address()).await
    }

    /// Query the wallet's confirmed nonce (latest block only)
    pub async fn get_nonce_latest(&self) -> WalletResult<u64> {
        self.rpc_client.get_nonce_latest(self.address()).await
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

    /// Start background gas price refresh.
    ///
    /// Spawns a background task that periodically fetches gas prices so the
    /// internal cache is fresh when `preheat()` is called.
    ///
    /// **No-op when a `gas_provider` is configured** — the external provider
    /// owns refresh, and running both would just duplicate RPC traffic.
    /// Returns `None` in that case.
    pub fn start_background_gas_refresh(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        // External provider owns refresh — bypass the internal task entirely.
        if self.config.gas_provider.is_some() {
            return None;
        }
        let interval = self.config.background_gas_refresh?;
        let wallet = Arc::clone(self);

        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                // Refresh gas prices - ignore errors
                let _ = tokio::join!(wallet.get_gas_price(), wallet.get_priority_fee());
            }
        }))
    }

    /// Start background nonce refresh
    ///
    /// This spawns a background task that periodically syncs the local nonce
    /// tracker from the latest confirmed chain state.
    ///
    /// Returns a handle that can be used to stop the background task.
    pub fn start_background_nonce_refresh(
        self: &Arc<Self>,
        interval: Duration,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let wallet = Arc::clone(self);

        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                // Refresh nonce from latest chain state - ignore errors
                let _ = wallet.sync_nonce_latest().await;
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
    /// Dropping an unused context releases its nonce. After signing, callers
    /// must commit or release the context based on broadcast acceptance.
    pub async fn preheat(&self, fetch_gas: bool) -> WalletResult<PreheatedContext> {
        let total_start = Instant::now();

        let nonce_start = Instant::now();
        let reservation = self.nonce_manager.reserve();
        let nonce = reservation.nonce();
        let nonce_ms = nonce_start.elapsed().as_millis();
        let mut ctx = PreheatedContext::new(reservation);

        if fetch_gas {
            let gas_cached = self.read_gas_cache(&self.gas_price_cache).is_some();
            let priority_cached = self.read_gas_cache(&self.priority_fee_cache).is_some();
            let gas = async {
                let start = Instant::now();
                let result = self.get_gas_price().await;
                (start.elapsed().as_millis(), result)
            };
            let priority = async {
                let start = Instant::now();
                let result = self.get_priority_fee().await;
                (start.elapsed().as_millis(), result)
            };
            // Fetch gas prices in parallel
            let ((gas_ms, gas_price), (priority_fee_ms, priority_fee)) =
                tokio::join!(gas, priority);

            ctx.gas_price = gas_price.ok();
            ctx.priority_fee = priority_fee.ok();

            // Calculate max fee if we have both
            if let (Some(base), Some(priority)) = (ctx.gas_price, ctx.priority_fee) {
                // max_fee = base_fee * 2 + priority_fee (common strategy)
                ctx.max_fee = Some(base + base + priority);
            }
            info!(
                nonce,
                nonce_ms,
                gas_cached,
                priority_cached,
                gas_ms,
                priority_fee_ms,
                total_ms = total_start.elapsed().as_millis(),
                gas_ready = ctx.gas_price.is_some(),
                priority_fee_ready = ctx.priority_fee.is_some(),
                "fast-wallet preheat gas ready"
            );
        } else {
            info!(
                nonce,
                nonce_ms,
                total_ms = total_start.elapsed().as_millis(),
                "fast-wallet preheat nonce ready"
            );
        }

        Ok(ctx)
    }

    /// Preheat multiple transactions at once
    ///
    /// Useful when you know you'll send N transactions and want to reserve nonces.
    pub async fn preheat_batch(
        &self,
        count: usize,
        fetch_gas: bool,
    ) -> WalletResult<Vec<PreheatedContext>> {
        let mut contexts = Vec::with_capacity(count);

        // Allocate all nonces first (lock-free, very fast)
        for _ in 0..count {
            let reservation = self.nonce_manager.reserve();
            contexts.push(PreheatedContext::new(reservation));
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
        ctx.release_reservation();
    }

    /// Mark a preheated context as entering broadcast (Held -> Broadcasting)
    /// for sign-now-broadcast-later flows where the context is dropped before
    /// the send completes. A `Broadcasting` reservation's `Drop` is a no-op
    /// (left for chain sync), so dropping the context will NOT release a nonce
    /// that is bound to a signed, soon-to-be-broadcast transaction — while a
    /// caller that later abandons the lane still reclaims it explicitly via
    /// `release_nonce`. This preserves the pre-v0.1.31 (no-RAII) lifecycle for
    /// callers that own nonce reclaim themselves. Returns Err if the
    /// reservation was already finalized.
    pub fn mark_preheat_broadcasting(&self, ctx: &PreheatedContext) -> WalletResult<()> {
        ctx.mark_broadcasting()
    }

    /// Commit a preheated context after an externally managed broadcast succeeds.
    pub fn commit_preheat(&self, ctx: &PreheatedContext) {
        if let Some(nonce) = ctx.reserved_nonce() {
            self.inflight_nonces.mark_committed(nonce);
        }
        ctx.commit_reservation();
    }

    /// Release a preheated context after signing but before broadcast acceptance.
    pub fn release_preheat(&self, ctx: &PreheatedContext) {
        if let Some(nonce) = ctx.reserved_nonce() {
            self.inflight_nonces.mark_released(nonce, "preheat_release");
        }
        ctx.release_reservation();
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
        let total_start = Instant::now();

        // Warm up primary RPC
        let primary_start = Instant::now();
        let primary_ok = self.rpc_client.warmup().await.is_ok();
        let primary_ms = primary_start.elapsed().as_millis();

        // Warm up batch RPC endpoints
        let batch_start = Instant::now();
        let batch_count = if let Some(batch_client) = &self.batch_client {
            batch_client.warmup().await
        } else {
            0
        };
        let batch_ms = batch_start.elapsed().as_millis();

        info!(
            primary_ok,
            primary_ms,
            batch_count,
            batch_ms,
            total_ms = total_start.elapsed().as_millis(),
            "fast-wallet connection warmup complete"
        );
        (primary_ok, batch_count)
    }

    /// Full preheat: warm connections + allocate nonce + (optionally) fetch gas.
    ///
    /// Runs `warmup_connections` and `preheat(fetch_gas)` in parallel and
    /// emits a single structured `fast-wallet full preheat complete` log
    /// with timing breakdowns.
    ///
    /// Pass `fetch_gas = false` when the caller supplies explicit gas prices
    /// (e.g. via `sign_with_preheat` + `priority_fee_override`) so gas RPC
    /// latency cannot block the broadcast hot path.
    ///
    /// Call this ~100-500ms before you expect to send a transaction.
    pub async fn preheat_full(&self, fetch_gas: bool) -> WalletResult<PreheatedContext> {
        let total_start = Instant::now();
        let warmup = async {
            let start = Instant::now();
            let result = self.warmup_connections().await;
            (start.elapsed().as_millis(), result)
        };
        let context = async {
            let start = Instant::now();
            let result = self.preheat(fetch_gas).await;
            (start.elapsed().as_millis(), result)
        };

        // Warm connections and (optionally) fetch gas in parallel.
        let ((warmup_ms, (primary_ok, batch_count)), (context_ms, ctx)) =
            tokio::join!(warmup, context);
        let ctx = ctx?;
        info!(
            total_ms = total_start.elapsed().as_millis(),
            warmup_ms,
            context_ms,
            primary_ok,
            batch_count,
            fetch_gas,
            nonce = ctx.nonce,
            gas_ready = ctx.gas_price.is_some(),
            priority_fee_ready = ctx.priority_fee.is_some(),
            "fast-wallet full preheat complete"
        );
        Ok(ctx)
    }

    /// Like [`preheat`](Self::preheat) but does NOT reserve a nonce. The nonce
    /// is reserved lazily in [`sign_with_preheat`](Self::sign_with_preheat) at
    /// sign time, so concurrently-held contexts bind nonces in broadcast order
    /// rather than preheat order (avoids out-of-order strands). Still prefetches
    /// gas when `fetch_gas` is set.
    pub async fn preheat_warmup_only(&self, fetch_gas: bool) -> WalletResult<PreheatedContext> {
        let total_start = Instant::now();
        let mut ctx = PreheatedContext::new_warmup_only();

        if fetch_gas {
            let gas_cached = self.read_gas_cache(&self.gas_price_cache).is_some();
            let priority_cached = self.read_gas_cache(&self.priority_fee_cache).is_some();
            let gas = async {
                let start = Instant::now();
                let result = self.get_gas_price().await;
                (start.elapsed().as_millis(), result)
            };
            let priority = async {
                let start = Instant::now();
                let result = self.get_priority_fee().await;
                (start.elapsed().as_millis(), result)
            };
            // Fetch gas prices in parallel
            let ((gas_ms, gas_price), (priority_fee_ms, priority_fee)) =
                tokio::join!(gas, priority);

            ctx.gas_price = gas_price.ok();
            ctx.priority_fee = priority_fee.ok();

            // Calculate max fee if we have both
            if let (Some(base), Some(priority)) = (ctx.gas_price, ctx.priority_fee) {
                // max_fee = base_fee * 2 + priority_fee (common strategy)
                ctx.max_fee = Some(base + base + priority);
            }
            info!(
                nonce_deferred = true,
                gas_cached,
                priority_cached,
                gas_ms,
                priority_fee_ms,
                total_ms = total_start.elapsed().as_millis(),
                gas_ready = ctx.gas_price.is_some(),
                priority_fee_ready = ctx.priority_fee.is_some(),
                "fast-wallet warmup-only preheat gas ready"
            );
        } else {
            info!(
                nonce_deferred = true,
                total_ms = total_start.elapsed().as_millis(),
                "fast-wallet warmup-only preheat ready"
            );
        }

        Ok(ctx)
    }

    /// Like [`preheat_full`](Self::preheat_full) but defers the nonce
    /// reservation to sign time (see [`preheat_warmup_only`](Self::preheat_warmup_only)).
    pub async fn preheat_full_warmup_only(
        &self,
        fetch_gas: bool,
    ) -> WalletResult<PreheatedContext> {
        let total_start = Instant::now();
        let warmup = async {
            let start = Instant::now();
            let result = self.warmup_connections().await;
            (start.elapsed().as_millis(), result)
        };
        let context = async {
            let start = Instant::now();
            let result = self.preheat_warmup_only(fetch_gas).await;
            (start.elapsed().as_millis(), result)
        };

        // Warm connections and (optionally) fetch gas in parallel.
        let ((warmup_ms, (primary_ok, batch_count)), (context_ms, ctx)) =
            tokio::join!(warmup, context);
        let ctx = ctx?;
        info!(
            total_ms = total_start.elapsed().as_millis(),
            warmup_ms,
            context_ms,
            primary_ok,
            batch_count,
            fetch_gas,
            nonce_deferred = true,
            gas_ready = ctx.gas_price.is_some(),
            priority_fee_ready = ctx.priority_fee.is_some(),
            "fast-wallet full warmup-only preheat complete"
        );
        Ok(ctx)
    }

    // ==================== Nonce Management ====================

    /// Sync nonce from chain using the `pending` tag (includes mempool).
    /// Use after a `"nonce too low"` error: chain has advanced past our local view.
    pub async fn sync_nonce(&self) -> WalletResult<u64> {
        let chain_nonce = self.rpc_client.get_nonce(self.address()).await?;
        self.nonce_manager.sync(chain_nonce);
        self.inflight_nonces.prune_consumed(chain_nonce);
        Ok(chain_nonce)
    }

    /// Sync nonce from chain using the `latest` tag (confirmed state only).
    /// Use after a `"nonce too high"` error: Nitro / geth pre-check compares
    /// the tx nonce against the state nonce, which corresponds to the `latest`
    /// block tag. Syncing to `pending` can leave the tracker above state when
    /// a dangling tx sits in the mempool at the gap nonce.
    pub async fn sync_nonce_latest(&self) -> WalletResult<u64> {
        let chain_nonce = self.rpc_client.get_nonce_latest(self.address()).await?;
        self.nonce_manager.sync(chain_nonce);
        self.inflight_nonces.prune_consumed(chain_nonce);
        Ok(chain_nonce)
    }

    /// Confirm a nonce was used (call after transaction is confirmed)
    pub fn confirm_nonce(&self) {
        self.nonce_manager.confirm();
    }

    /// Mark a known transaction as mined in the local in-flight ledger.
    pub fn mark_transaction_mined(&self, tx: &Transaction) {
        self.inflight_nonces.mark_mined(tx.nonce(), tx.hash());
    }

    /// Count signed/broadcast nonces still unresolved in the local ledger.
    pub fn inflight_count(&self) -> usize {
        self.inflight_nonces.unresolved_count()
    }

    /// Inspect the lowest unresolved nonce at or above a chain nonce.
    pub fn lowest_unresolved_inflight(&self, chain_next: u64) -> Option<InflightNonceSnapshot> {
        self.inflight_nonces
            .lowest_unresolved_at_or_above(chain_next)
    }

    // ==================== Gas Price ====================

    /// Get gas price.
    ///
    /// When an external `gas_provider` is configured the call is delegated
    /// to it (the internal cache is bypassed). Otherwise: lock-free read
    /// from the local cache, then double-checked-locking fetch via the
    /// gas RPC client.
    pub async fn get_gas_price(&self) -> WalletResult<U256> {
        // External provider owns the cache — delegate without touching ours.
        if let Some(provider) = &self.config.gas_provider {
            return provider.gas_price().await;
        }

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
        let price = self.gas_rpc().gas_price().await?;

        // Update cache
        {
            let mut cache = self.gas_price_cache.write();
            *cache = Some((price, Instant::now()));
        }

        Ok(price)
    }

    /// Get priority fee.
    ///
    /// When an external `gas_provider` is configured the call is delegated
    /// to it (the internal cache is bypassed). Otherwise: lock-free read
    /// from the local cache, then double-checked-locking fetch.
    pub async fn get_priority_fee(&self) -> WalletResult<U256> {
        // External provider owns the cache — delegate without touching ours.
        if let Some(provider) = &self.config.gas_provider {
            return provider.priority_fee().await;
        }

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

        let fee = self.gas_rpc().max_priority_fee().await?;

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
            if age < self.config.effective_gas_cache_duration()
                && age < self.config.max_gas_cache_age
            {
                Some(*price)
            } else {
                None
            }
        })
    }

    /// Force refresh gas prices.
    ///
    /// When an external `gas_provider` is configured this delegates to it
    /// without touching the (bypassed) internal cache. Otherwise it does a
    /// direct RPC fetch and writes both internal caches.
    pub async fn refresh_gas_prices(&self) -> WalletResult<(U256, Option<U256>)> {
        if let Some(provider) = &self.config.gas_provider {
            let (gas_price, priority_fee) =
                tokio::join!(provider.gas_price(), provider.priority_fee());
            return Ok((gas_price?, priority_fee.ok()));
        }

        let (gas_price, priority_fee) = tokio::join!(
            self.rpc_client.gas_price(),
            self.rpc_client.max_priority_fee()
        );

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
        let nonce = self.nonce_manager.get_nonce();
        request.nonce = nonce;
        request.chain_id = self.config.chain_id;

        if request.gas_limit == 0 {
            request.gas_limit = self.config.default_gas_limit;
        }

        self.record_signed_or_release(nonce, request.build_and_sign(&self.signer))
    }

    /// Sign a transaction with explicit nonce (does NOT increment internal counter)
    ///
    /// Use this when you want to pre-sign multiple alternative transactions
    /// with the same nonce and choose which one to send later.
    ///
    /// WARNING: Using this incorrectly can result in nonce conflicts.
    #[inline]
    pub fn sign_with_nonce(
        &self,
        mut request: TransactionRequest,
        nonce: u64,
    ) -> WalletResult<Transaction> {
        request.nonce = nonce;
        request.chain_id = self.config.chain_id;

        if request.gas_limit == 0 {
            request.gas_limit = self.config.default_gas_limit;
        }

        self.record_signed_without_release(request.build_and_sign(&self.signer))
    }

    /// Sign multiple alternative transactions with the same nonce
    ///
    /// Useful for liquidation bots that want to pre-sign different strategies
    /// and choose the best one based on simulation results.
    ///
    /// Returns transactions signed with the CURRENT nonce (without incrementing).
    #[inline]
    pub fn sign_alternatives(
        &self,
        requests: Vec<TransactionRequest>,
    ) -> Vec<WalletResult<Transaction>> {
        let nonce = self.nonce_manager.peek();
        requests
            .into_iter()
            .map(|req| self.sign_with_nonce(req, nonce))
            .collect()
    }

    /// Reserve a nonce for later use with RAII release on drop.
    ///
    /// Increments the internal counter and returns a guard.
    /// Use with `sign_with_nonce()` to sign a transaction later.
    #[inline]
    pub fn reserve_nonce(&self) -> ReservedNonce {
        self.nonce_manager.reserve()
    }

    /// Release a reserved nonce number back to the recycling pool.
    ///
    /// Call this if you reserved a nonce but decided not to use it.
    #[inline]
    pub fn release_nonce(&self, nonce: u64) {
        self.nonce_manager.release(nonce);
        self.inflight_nonces.mark_released(nonce, "manual_release");
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

        // Source the nonce from the reservation, reserving lazily for a
        // warmup-only context. This binds the nonce at sign time (broadcast
        // order) rather than preheat order, so concurrently-held contexts that
        // fire out of order do not strand a high nonce behind a lower gap.
        let nonce = {
            let mut reservation = ctx.reservation.lock();
            if reservation.is_none() {
                *reservation = Some(self.nonce_manager.reserve());
            }
            reservation
                .as_ref()
                .expect("reservation present after lazy reserve")
                .nonce()
        };
        request.nonce = nonce;
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

        let result = request.build_and_sign(&self.signer);
        if result.is_err() {
            ctx.release_reservation();
        }
        self.record_signed_without_release(result)
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
        let nonce = self.nonce_manager.get_nonce();
        let request = TransactionRequest::new()
            .to(to)
            .value(value)
            .data(data)
            .gas_limit(gas_limit)
            .max_fee_per_gas(max_fee_per_gas)
            .max_priority_fee_per_gas(max_priority_fee_per_gas)
            .nonce(nonce)
            .chain_id(self.config.chain_id);

        self.record_signed_or_release(nonce, request.build_and_sign(&self.signer))
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
        let nonce = self.nonce_manager.get_nonce();
        let request = TransactionRequest::new()
            .to(to)
            .value(value)
            .data(data)
            .gas_limit(gas_limit)
            .gas_price(gas_price)
            .nonce(nonce)
            .chain_id(self.config.chain_id);

        self.record_signed_or_release(nonce, request.build_and_sign(&self.signer))
    }

    // ==================== Transaction Sending ====================

    fn record_signed_or_release(
        &self,
        nonce: u64,
        result: WalletResult<Transaction>,
    ) -> WalletResult<Transaction> {
        match result {
            Ok(tx) => {
                self.inflight_nonces.record_signed(tx.nonce(), tx.hash());
                Ok(tx)
            }
            Err(error) => {
                self.nonce_manager.release(nonce);
                self.inflight_nonces.mark_released(nonce, "sign_error");
                Err(error)
            }
        }
    }

    fn record_signed_without_release(
        &self,
        result: WalletResult<Transaction>,
    ) -> WalletResult<Transaction> {
        if let Ok(tx) = result.as_ref() {
            self.inflight_nonces.record_signed(tx.nonce(), tx.hash());
        }
        result
    }

    async fn broadcast_signed_hash(&self, tx: &Transaction) -> WalletResult<B256> {
        let _permit = tokio::time::timeout(
            self.config.pending_acquire_timeout,
            self.pending_semaphore.acquire(),
        )
        .await
        .map_err(|_| WalletError::Timeout)?
        .map_err(|_| WalletError::RpcError("Semaphore closed".to_string()))?;

        let hex_tx = tx.to_hex();
        if let Some(batch_client) = &self.batch_client {
            batch_client.broadcast_transaction(&hex_tx).await
        } else {
            self.rpc_client.send_raw_transaction(&hex_tx).await
        }
    }

    async fn broadcast_signed_result(
        &self,
        tx: &Transaction,
        sign_ms: f64,
    ) -> WalletResult<SendResult> {
        let semaphore_started = Instant::now();
        let _permit = tokio::time::timeout(
            self.config.pending_acquire_timeout,
            self.pending_semaphore.acquire(),
        )
        .await
        .map_err(|_| WalletError::Timeout)?
        .map_err(|_| WalletError::RpcError("Semaphore closed".to_string()))?;
        let semaphore_wait_ms = semaphore_started.elapsed().as_millis() as u64;

        let hex_tx = tx.to_hex();
        let result = if let Some(batch_client) = &self.batch_client {
            batch_client.broadcast_transaction_detailed(&hex_tx).await
        } else {
            self.rpc_client.send_raw_transaction_detailed(&hex_tx).await
        };

        result.map(|mut send_result| {
            send_result.sign_ms = sign_ms;
            send_result.semaphore_wait_ms = semaphore_wait_ms;
            send_result
        })
    }

    async fn recover_nonce_error(&self, error: &WalletError) {
        let err_str = error.to_string().to_lowercase();
        if err_str.contains("nonce too high") {
            let _ = self.sync_nonce_latest().await;
        } else if err_str.contains("nonce too low") {
            let _ = self.sync_nonce().await;
        }
    }

    /// Send a pre-signed transaction
    ///
    /// Uses a timeout when acquiring the pending transaction permit to avoid
    /// blocking indefinitely when many transactions are in-flight.
    pub async fn send_signed(&self, tx: &Transaction) -> WalletResult<B256> {
        let result = self.broadcast_signed_hash(tx).await;

        match &result {
            Ok(tx_hash) => {
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), *tx_hash);
            }
            Err(e) => {
                // Release nonce for reuse
                self.nonce_manager.release(tx.nonce());
                self.inflight_nonces
                    .mark_released(tx.nonce(), "send_signed_error");

                // Nonce-drift recovery. Both "too low" (we replayed an already-mined
                // nonce) and "too high" (local counter skipped ahead of chain) indicate
                // the local tracker has diverged from the chain, so force an RPC resync.
                //
                // The sync target differs: Nitro / geth tx_pre_checker compares
                // tx.nonce against stateNonce (the `latest` tag) when emitting
                // "nonce too high", so sync from `latest` — syncing from `pending`
                // can leave the tracker above state when a dangling mempool tx sits
                // at the gap nonce. For "nonce too low" we want `pending` because
                // the chain has moved ahead (mempool/just-mined txs count).
                self.recover_nonce_error(e).await;
            }
        }

        result
    }

    /// Send a pre-signed transaction and return broadcast timing details.
    pub async fn send_signed_detailed(
        &self,
        tx: &Transaction,
        sign_ms: f64,
    ) -> WalletResult<SendResult> {
        let result = self.broadcast_signed_result(tx, sign_ms).await;

        match &result {
            Ok(_) => {
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), tx.hash());
            }
            Err(e) => {
                self.nonce_manager.release(tx.nonce());
                self.inflight_nonces
                    .mark_released(tx.nonce(), "send_signed_detailed_error");
                self.recover_nonce_error(e).await;
            }
        }

        result
    }

    /// Verify a broadcast TX is visible in the mempool/chain.
    /// Polls eth_getTransactionByHash for up to `max_wait`. If not found, re-broadcasts.
    /// Returns Ok(true) if verified in mempool, Ok(false) if re-broadcast was accepted.
    /// Releases the nonce and returns the rebroadcast error if the TX cannot be found or resent.
    pub async fn verify_broadcast(
        &self,
        tx: &Transaction,
        tx_hash: B256,
        max_wait: Duration,
    ) -> WalletResult<bool> {
        let poll_interval = Duration::from_millis(500);
        let start = Instant::now();

        while start.elapsed() < max_wait {
            if self.rpc_client.tx_exists(tx_hash).await.unwrap_or(false) {
                self.inflight_nonces.mark_mempool_seen(tx.nonce(), tx_hash);
                return Ok(true);
            }
            tokio::time::sleep(poll_interval).await;
        }

        // TX not found — re-broadcast to all endpoints
        tracing::warn!(
            %tx_hash,
            nonce = tx.nonce(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "TX not in mempool after verification window, re-broadcasting"
        );
        let hex_tx = tx.to_hex();
        let result = if let Some(batch_client) = &self.batch_client {
            batch_client.broadcast_transaction(&hex_tx).await
        } else {
            self.rpc_client.send_raw_transaction(&hex_tx).await
        };

        if let Err(error) = result {
            tracing::warn!(%tx_hash, error = %error, "re-broadcast failed; releasing nonce");
            self.nonce_manager.release(tx.nonce());
            self.inflight_nonces
                .mark_released(tx.nonce(), "verify_rebroadcast_error");
            return Err(error);
        };
        self.inflight_nonces
            .mark_rebroadcast_accepted(tx.nonce(), tx_hash);
        tracing::info!(%tx_hash, "re-broadcast succeeded");
        Ok(false)
    }

    /// Compare local effective next nonce with on-chain nonce.
    pub async fn nonce_health_check(&self) -> WalletResult<NonceHealth> {
        let current = self.current_nonce();
        let effective_next = self.effective_next_nonce();
        let chain_next = self.rpc_client.get_nonce(self.address()).await?;
        let chain_latest = self.rpc_client.get_nonce_latest(self.address()).await.ok();
        let gap_count = self.nonce_manager.gap_count();
        self.inflight_nonces.prune_consumed(chain_next);
        let lowest_unresolved = self
            .inflight_nonces
            .lowest_unresolved_at_or_above(chain_next);
        let inflight_count = self.inflight_nonces.unresolved_count();
        // If local is >2 ahead of chain, nonces are stuck in-flight
        let stalled = effective_next > chain_next + 2;
        Ok(NonceHealth {
            current,
            effective_next,
            chain_next,
            chain_latest,
            gap_count,
            inflight_count,
            lowest_unresolved,
            stalled,
        })
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
        ctx.mark_broadcasting()?;
        let result = self.broadcast_signed_hash(&tx).await;
        match &result {
            Ok(tx_hash) => {
                ctx.commit_reservation();
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), *tx_hash);
            }
            Err(error) => {
                ctx.release_reservation();
                self.inflight_nonces
                    .mark_released(tx.nonce(), "send_with_preheat_error");
                self.recover_nonce_error(error).await;
            }
        }
        result
    }

    /// Sign and send using preheated context with timing details.
    pub async fn send_with_preheat_detailed(
        &self,
        ctx: &PreheatedContext,
        request: TransactionRequest,
    ) -> WalletResult<SendResult> {
        let sign_start = Instant::now();
        let tx = self.sign_with_preheat(ctx, request)?;
        let sign_ms = sign_start.elapsed().as_secs_f64() * 1000.0;
        ctx.mark_broadcasting()?;
        let result = self.broadcast_signed_result(&tx, sign_ms).await;
        match &result {
            Ok(_) => {
                ctx.commit_reservation();
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), tx.hash());
            }
            Err(error) => {
                ctx.release_reservation();
                self.inflight_nonces
                    .mark_released(tx.nonce(), "send_with_preheat_detailed_error");
                self.recover_nonce_error(error).await;
            }
        }
        result
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
        ctx.mark_broadcasting()?;
        let result = self.broadcast_signed_hash(&tx).await;
        match &result {
            Ok(tx_hash) => {
                ctx.commit_reservation();
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), *tx_hash);
            }
            Err(error) => {
                ctx.release_reservation();
                self.inflight_nonces
                    .mark_released(tx.nonce(), "send_optimistic_with_preheat_error");
                self.recover_nonce_error(error).await;
            }
        }
        result
    }

    /// Quick liquidation with minimal parameters (uses config defaults)
    ///
    /// Fastest path when you just have the contract call data:
    /// - Uses preheated or cached gas price
    /// - Uses default gas limit from config
    /// - No simulation, no RPC calls
    #[inline]
    pub async fn send_quick_liquidation(&self, to: Address, data: Bytes) -> WalletResult<B256> {
        let gas_price = self
            .gas_price_cache
            .read()
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

    /// Send EIP-1559 contract call with explicit fee parameters.
    ///
    /// Used for Priority orders on Base where maxPriorityFeePerGas is the auction bid.
    pub async fn send_contract_call_eip1559(
        &self,
        to: Address,
        data: Bytes,
        gas_limit: u64,
        value: Option<U256>,
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
    ) -> WalletResult<B256> {
        let mut request = TransactionRequest::new()
            .to(to)
            .data(data)
            .gas_limit(gas_limit)
            .max_fee_per_gas(max_fee_per_gas)
            .max_priority_fee_per_gas(max_priority_fee_per_gas);

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
    gas_rpc_url: Option<String>,
    broadcast_rpcs: Vec<String>,
    config: WalletConfig,
    initial_nonce: Option<u64>,
}

impl FastWalletBuilder {
    pub fn new(private_key: impl Into<String>, primary_rpc: impl Into<String>) -> Self {
        Self {
            private_key: private_key.into(),
            primary_rpc: primary_rpc.into(),
            gas_rpc_url: None,
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
            gas_rpc_url: None,
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
            gas_rpc_url: None,
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

    pub fn gas_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.gas_rpc_url = Some(url.into());
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

    /// Set receipt polling interval
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    /// Set an explicit gas cache TTL.
    ///
    /// Most callers should leave this unset and rely on
    /// `background_gas_refresh` — the cache then stays fresh for one full
    /// refresh interval. Call this only to opt in to a stricter freshness
    /// window than the background refresh provides.
    pub fn gas_cache_duration(mut self, duration: Duration) -> Self {
        self.config.gas_cache_duration = Some(duration);
        self
    }

    /// Set background gas refresh interval
    pub fn background_gas_refresh(mut self, interval: Duration) -> Self {
        self.config.background_gas_refresh = Some(interval);
        self
    }

    /// Inject an external chain-level gas-price provider.
    ///
    /// While a provider is configured, this wallet's internal gas cache and
    /// background refresh task are bypassed entirely — the provider becomes
    /// the source of truth for `get_gas_price`, `get_priority_fee`,
    /// `refresh_gas_prices`, and the gas values inside `PreheatedContext`.
    ///
    /// Use this when a single process holds multiple wallets on the same
    /// chain (e.g. several lanes per chain) and you want them to share one
    /// cache + one background refresh task.
    pub fn gas_provider(mut self, provider: Arc<dyn GasPriceProvider>) -> Self {
        self.config.gas_provider = Some(provider);
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
        let gas_rpc_url = self.gas_rpc_url;

        if let Some(nonce) = self.initial_nonce {
            let mut wallet = FastWallet::with_known_nonce(
                &self.private_key,
                &self.primary_rpc,
                nonce,
                self.config,
            )?;

            if !self.broadcast_rpcs.is_empty() {
                let mut all_rpcs = vec![self.primary_rpc.clone()];
                all_rpcs.extend(self.broadcast_rpcs);
                wallet.batch_client = Some(Arc::new(BatchRpcClient::new(all_rpcs)?));
            }

            if let Some(url) = gas_rpc_url {
                wallet.gas_rpc_client = Some(Arc::new(RpcClient::new(&url)?));
            }

            Ok(wallet)
        } else if !self.broadcast_rpcs.is_empty() {
            let mut wallet = FastWallet::with_multiple_rpcs(
                &self.private_key,
                &self.primary_rpc,
                self.broadcast_rpcs,
                self.config,
            )
            .await?;

            if let Some(url) = gas_rpc_url {
                wallet.gas_rpc_client = Some(Arc::new(RpcClient::new(&url)?));
            }

            Ok(wallet)
        } else {
            let mut wallet =
                FastWallet::new(&self.private_key, &self.primary_rpc, self.config).await?;

            if let Some(url) = gas_rpc_url {
                wallet.gas_rpc_client = Some(Arc::new(RpcClient::new(&url)?));
            }

            Ok(wallet)
        }
    }

    /// Build with known nonce (synchronous - no RPC call)
    pub fn build_with_nonce(self, nonce: u64) -> WalletResult<FastWallet> {
        let mut wallet =
            FastWallet::with_known_nonce(&self.private_key, &self.primary_rpc, nonce, self.config)?;

        if !self.broadcast_rpcs.is_empty() {
            let mut all_rpcs = vec![self.primary_rpc.clone()];
            all_rpcs.extend(self.broadcast_rpcs);
            wallet.batch_client = Some(Arc::new(BatchRpcClient::new(all_rpcs)?));
        }

        if let Some(url) = self.gas_rpc_url {
            wallet.gas_rpc_client = Some(Arc::new(RpcClient::new(&url)?));
        }

        Ok(wallet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PRIVATE_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    fn test_request() -> TransactionRequest {
        TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
    }

    async fn rpc_error_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf)
                .await
                .unwrap();
            let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rebroadcast rejected"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

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
    fn test_sign_records_inflight_nonce_and_manual_release_resolves_it() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(42)
            .unwrap();

        let tx = wallet.sign(test_request()).unwrap();

        let inflight = wallet.lowest_unresolved_inflight(42).unwrap();
        assert_eq!(inflight.nonce, 42);
        assert_eq!(inflight.tx_hashes, vec![tx.hash()]);
        assert_eq!(wallet.inflight_count(), 1);

        wallet.release_nonce(42);

        assert!(wallet.lowest_unresolved_inflight(42).is_none());
        assert_eq!(wallet.inflight_count(), 0);
    }

    #[test]
    fn test_sign_with_nonce_records_replacement_hashes() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(7)
            .unwrap();
        let first = wallet.sign_with_nonce(test_request(), 7).unwrap();
        let second = wallet
            .sign_with_nonce(
                TransactionRequest::new()
                    .to(Address::repeat_byte(2))
                    .value(U256::from(1u64))
                    .gas_limit(21000)
                    .gas_price(U256::from(21_000_000_000u64)),
                7,
            )
            .unwrap();

        let inflight = wallet.lowest_unresolved_inflight(7).unwrap();
        assert_eq!(inflight.nonce, 7);
        assert_eq!(inflight.tx_hashes.len(), 2);
        assert!(inflight.tx_hashes.contains(&first.hash()));
        assert!(inflight.tx_hashes.contains(&second.hash()));
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
    fn test_effective_gas_cache_duration_inference() {
        // Default: no override, no background refresh → 500ms fallback
        let cfg = WalletConfig::default();
        assert_eq!(
            cfg.effective_gas_cache_duration(),
            DEFAULT_GAS_CACHE_FALLBACK
        );

        // Background refresh enabled, no explicit override → inferred to interval
        let cfg = WalletConfig {
            background_gas_refresh: Some(Duration::from_secs(30)),
            ..Default::default()
        };
        assert_eq!(cfg.effective_gas_cache_duration(), Duration::from_secs(30));

        // Explicit override always wins, even with background refresh
        let cfg = WalletConfig {
            gas_cache_duration: Some(Duration::from_millis(100)),
            background_gas_refresh: Some(Duration::from_secs(30)),
            ..Default::default()
        };
        assert_eq!(
            cfg.effective_gas_cache_duration(),
            Duration::from_millis(100)
        );

        // Builder sets the explicit override
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .background_gas_refresh(Duration::from_secs(10))
            .build_with_nonce(0)
            .unwrap();
        assert_eq!(
            wallet.config.effective_gas_cache_duration(),
            Duration::from_secs(10)
        );

        // Explicit override longer than max_gas_cache_age is *still capped*
        // by the upper bound at read time — exercise read_gas_cache directly.
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .gas_cache_duration(Duration::from_secs(120))
            .build_with_nonce(0)
            .unwrap();
        assert_eq!(wallet.config.max_gas_cache_age, Duration::from_secs(30));
        // Seed cache with an old entry: stale relative to max_gas_cache_age (30s)
        // but fresh relative to gas_cache_duration override (120s).
        *wallet.gas_price_cache.write() = Some((
            U256::from(1_000_000_000u64),
            Instant::now() - Duration::from_secs(45),
        ));
        assert!(
            wallet.read_gas_cache(&wallet.gas_price_cache).is_none(),
            "max_gas_cache_age must cap freshness even when gas_cache_duration is larger"
        );
    }

    #[test]
    fn test_preheated_context() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(100)
            .unwrap();

        // Create preheated context (without gas fetch since no RPC)
        let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());
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
        wallet.commit_preheat(&ctx);

        // Context should be marked as used
        assert!(!ctx.is_valid(Duration::from_secs(60)));
    }

    #[test]
    fn test_preheated_context_double_use() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();

        let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());

        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64));

        // First use should succeed
        let _ = wallet.sign_with_preheat(&ctx, request.clone()).unwrap();
        wallet.commit_preheat(&ctx);

        // Second use should fail
        let result = wallet.sign_with_preheat(&ctx, request);
        assert!(result.is_err());
    }

    #[test]
    fn test_signed_but_never_broadcast_preheat_releases_on_drop() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(100)
            .unwrap();

        {
            let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());
            let tx = wallet.sign_with_preheat(&ctx, test_request()).unwrap();
            assert_eq!(tx.nonce(), 100);
            assert_eq!(wallet.current_nonce(), 101);
        }

        assert_eq!(wallet.current_nonce(), 100);
        assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 100);
    }

    #[test]
    fn test_warmup_only_reserves_nonce_at_sign() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(100)
            .unwrap();

        // Warmup-only context holds NO reservation: nonce is the sentinel and
        // the wallet counter has not advanced.
        let ctx = PreheatedContext::new_warmup_only();
        assert_eq!(ctx.nonce, u64::MAX);
        assert_eq!(wallet.current_nonce(), 100);

        // Signing reserves the nonce lazily, sourced from the reservation.
        let tx = wallet.sign_with_preheat(&ctx, test_request()).unwrap();
        assert_eq!(tx.nonce(), 100);
        assert_eq!(wallet.current_nonce(), 101);
        wallet.commit_preheat(&ctx);
    }

    #[test]
    fn test_warmup_only_binds_nonce_in_sign_order_not_creation_order() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(100)
            .unwrap();

        // Two warmup-only contexts created in order A then B...
        let ctx_a = PreheatedContext::new_warmup_only();
        let ctx_b = PreheatedContext::new_warmup_only();

        // ...but signed B-first: B must get the lower nonce. This is the fix —
        // eager preheat would have bound A=100, B=101 at creation, stranding
        // B's tx behind A if A never broadcasts.
        let tx_b = wallet.sign_with_preheat(&ctx_b, test_request()).unwrap();
        let tx_a = wallet.sign_with_preheat(&ctx_a, test_request()).unwrap();
        assert_eq!(tx_b.nonce(), 100);
        assert_eq!(tx_a.nonce(), 101);
        wallet.commit_preheat(&ctx_a);
        wallet.commit_preheat(&ctx_b);
    }

    #[test]
    fn test_warmup_only_unused_drop_consumes_no_nonce() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(100)
            .unwrap();

        {
            let ctx = PreheatedContext::new_warmup_only();
            wallet.cancel_preheat(ctx); // never signed
        }
        // No reservation was ever taken, so the counter is untouched.
        assert_eq!(wallet.current_nonce(), 100);
        assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 100);
    }

    #[test]
    fn test_auto_sign_error_releases_allocated_nonce() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();

        let nonce = wallet.nonce_manager.get_nonce();
        let result = wallet.record_signed_or_release(
            nonce,
            Err(WalletError::SigningError(
                "forced signing error".to_string(),
            )),
        );

        assert!(result.is_err());
        assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 0);
    }

    #[tokio::test]
    async fn test_verify_broadcast_rebroadcast_failure_releases_nonce() {
        let url = rpc_error_server().await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();
        let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());
        let tx = wallet.sign_with_preheat(&ctx, test_request()).unwrap();
        wallet.commit_preheat(&ctx);

        let result = wallet
            .verify_broadcast(&tx, tx.hash(), Duration::ZERO)
            .await;

        assert!(result.is_err());
        assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 0);
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

    // ==================== GasPriceProvider injection tests (#15) ====================

    use crate::gas_provider::GasPriceProvider;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    /// Fixed-value provider that also counts how many times each method was called,
    /// so tests can assert delegation actually happened.
    struct FixedProvider {
        gas_price_wei: U256,
        priority_fee_wei: U256,
        gas_calls: AtomicU64,
        priority_calls: AtomicU64,
    }

    impl FixedProvider {
        fn new(gas_price_wei: u64, priority_fee_wei: u64) -> Arc<Self> {
            Arc::new(Self {
                gas_price_wei: U256::from(gas_price_wei),
                priority_fee_wei: U256::from(priority_fee_wei),
                gas_calls: AtomicU64::new(0),
                priority_calls: AtomicU64::new(0),
            })
        }
    }

    #[async_trait]
    impl GasPriceProvider for FixedProvider {
        async fn gas_price(&self) -> WalletResult<U256> {
            self.gas_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(self.gas_price_wei)
        }
        async fn priority_fee(&self) -> WalletResult<U256> {
            self.priority_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(self.priority_fee_wei)
        }
    }

    #[tokio::test]
    async fn gas_provider_delegates_get_gas_price_and_skips_local_cache() {
        // Contract #1: get_gas_price()/get_priority_fee() must delegate to the
        // provider and must NOT consult or populate the internal cache.
        let provider = FixedProvider::new(42_000_000_000, 3_000_000_000); // 42 / 3 gwei
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .gas_provider(provider.clone())
            .build_with_nonce(0)
            .unwrap();

        let gas = wallet.get_gas_price().await.unwrap();
        let prio = wallet.get_priority_fee().await.unwrap();
        assert_eq!(gas, U256::from(42_000_000_000u64));
        assert_eq!(prio, U256::from(3_000_000_000u64));
        assert_eq!(provider.gas_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(provider.priority_calls.load(AtomicOrdering::SeqCst), 1);

        // Internal caches must remain empty — they are bypassed when a
        // provider is set.
        assert!(wallet.gas_price_cache.read().is_none());
        assert!(wallet.priority_fee_cache.read().is_none());
    }

    #[tokio::test]
    async fn gas_provider_disables_background_refresh() {
        // Contract #2: start_background_gas_refresh() is a no-op (returns None)
        // when a provider is set, even if background_gas_refresh interval is
        // configured.
        let provider = FixedProvider::new(1, 1);
        let wallet = Arc::new(
            FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
                .chain_id(1)
                .background_gas_refresh(Duration::from_millis(50))
                .gas_provider(provider)
                .build_with_nonce(0)
                .unwrap(),
        );

        let handle = wallet.start_background_gas_refresh();
        assert!(
            handle.is_none(),
            "provider must suppress background refresh"
        );
    }

    #[tokio::test]
    async fn preheat_populates_context_from_gas_provider() {
        // Contract #4: preheat(true) populates ctx.gas_price / priority_fee
        // from the provider; existing call-site code that reads ctx.gas_price
        // is unchanged.
        let provider = FixedProvider::new(7_000_000_000, 1_000_000_000);
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .gas_provider(provider.clone())
            .build_with_nonce(99)
            .unwrap();

        let ctx = wallet.preheat(true).await.unwrap();
        assert_eq!(ctx.gas_price, Some(U256::from(7_000_000_000u64)));
        assert_eq!(ctx.priority_fee, Some(U256::from(1_000_000_000u64)));
        // max_fee = base*2 + priority
        assert_eq!(
            ctx.max_fee,
            Some(U256::from(7_000_000_000u64 * 2 + 1_000_000_000u64))
        );
        assert_eq!(provider.gas_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(provider.priority_calls.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refresh_gas_prices_delegates_to_provider_and_skips_cache() {
        // refresh_gas_prices() should also delegate when a provider is set.
        let provider = FixedProvider::new(9_000_000_000, 2_000_000_000);
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .gas_provider(provider.clone())
            .build_with_nonce(0)
            .unwrap();

        let (gas, prio) = wallet.refresh_gas_prices().await.unwrap();
        assert_eq!(gas, U256::from(9_000_000_000u64));
        assert_eq!(prio, Some(U256::from(2_000_000_000u64)));
        // Internal caches must remain untouched.
        assert!(wallet.gas_price_cache.read().is_none());
        assert!(wallet.priority_fee_cache.read().is_none());
    }

    #[test]
    fn gas_provider_default_is_none_for_backwards_compat() {
        // Contract: when gas_provider is unset, behavior is identical to
        // pre-#15 — the existing test suite already covers that path. Here
        // we just pin the default so a future change can't silently flip it.
        let cfg = WalletConfig::default();
        assert!(cfg.gas_provider.is_none());

        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();
        assert!(wallet.config.gas_provider.is_none());
    }
}

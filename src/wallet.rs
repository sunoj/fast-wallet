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
use crate::transaction::{bump_u256, Transaction, TransactionRequest, RBF_MIN_BUMP_BPS};
use alloy::primitives::{Address, Bytes, B256, U256};
use futures_util::future::join_all;
use parking_lot::{Mutex, RwLock};
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

    fn release_reservation(&self) -> bool {
        self.used.store(true, Ordering::Release);
        let mut reservation = self.reservation.lock();
        if let Some(mut reservation) = reservation.take() {
            return reservation.release();
        }
        false
    }

    fn release_rejected_broadcast(&self) -> bool {
        self.used.store(true, Ordering::Release);
        let mut reservation = self.reservation.lock();
        if let Some(mut reservation) = reservation.take() {
            return reservation.release_rejected_broadcast();
        }
        false
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

/// Idempotency window for [`FastWallet::replace_stalled_nonce`]: a second call for
/// the same nonce within this window is a no-op (avoids spamming replacements while
/// a streak-driven caller polls each block).
const STALL_REPLACE_WINDOW: Duration = Duration::from_secs(3);

fn is_definitive_precheck_rejection(error: &WalletError) -> bool {
    match error {
        WalletError::InsufficientFunds | WalletError::GasLimitExceeded => return true,
        WalletError::Timeout
        | WalletError::NetworkError(_)
        | WalletError::HttpError(_)
        | WalletError::TransactionUnderpriced => return false,
        _ => {}
    }

    let message = error.to_string().to_lowercase();
    let ambiguous = [
        "already known",
        "known transaction",
        "underpriced",
        "fee too low",
        "timeout",
        "connection",
        "network",
        "temporarily",
        "nonce too high",
        "nonce too low",
    ];
    if ambiguous.iter().any(|needle| message.contains(needle)) {
        return false;
    }

    let definitive = [
        "insufficient funds",
        "exceeds block gas limit",
        "exceed block gas limit",
        "gas limit exceeded",
        "intrinsic gas too high",
        "invalid sender",
        "tx type not supported",
        "transaction type not supported",
        "unsupported transaction type",
        "malformed",
        "invalid rlp",
        "rlp",
    ];
    definitive.iter().any(|needle| message.contains(needle))
}

/// Outcome of [`FastWallet::replace_stalled_nonce`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceOutcome {
    /// A higher-fee replacement was broadcast at `nonce` to unblock the queue.
    /// (v0.1.40 ships only the cancel form; RBF of the original payload lands later.)
    Cancelled {
        /// The nonce that was replaced (`chain_next`).
        nonce: u64,
        /// Hash of the broadcast replacement tx.
        tx_hash: B256,
    },
    /// Not in a head-of-line stall (chain disagrees `chain_next` is next, or this
    /// nonce was already replaced within [`STALL_REPLACE_WINDOW`]). No action taken.
    NotStalled,
    /// No unresolved in-flight nonce at or above `chain_next`. No action taken.
    NoCandidate,
    /// A concurrent reservation/external consumption moved the local counter behind
    /// `chain_next`; replacing would be unsafe. No action taken.
    RaceAborted,
}

/// Outcome of [`FastWallet::verify_broadcast`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifyOutcome {
    /// True if the original tx was found in the mempool/chain as broadcast.
    pub in_mempool: bool,
    /// The hash the caller should now track. Equals the original hash when found;
    /// when the tx vanished and was re-broadcast with bumped fees, this is the NEW
    /// (bumped) tx hash — callers MUST rebind to it, since the original hash will
    /// never appear on-chain.
    pub effective_hash: B256,
}

/// Compute the fees for a replacement/cancel tx. Bumps by `bump_bps` over the MAX
/// of the current network price and the stranded tx's own fee, so the replacement
/// out-bids an overpaid stranded tx (satisfying RBF) while staying competitive now.
pub(crate) fn compute_cancel_fees(
    network_tip: U256,
    network_gas: U256,
    stranded_tip: Option<U256>,
    stranded_max_fee: Option<U256>,
    bump_bps: u32,
) -> (U256, U256) {
    let tip_basis = network_tip.max(stranded_tip.unwrap_or(U256::ZERO));
    let gas_basis = network_gas.max(stranded_max_fee.unwrap_or(U256::ZERO));
    let tip = bump_u256(tip_basis, bump_bps);
    let max_fee = bump_u256(gas_basis, bump_bps).max(tip);
    (tip, max_fee)
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
    /// Last `(nonce, at)` replaced by `replace_stalled_nonce`, for idempotency.
    last_stall_replace: Mutex<Option<(u64, Instant)>>,
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
            last_stall_replace: Mutex::new(None),
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
            last_stall_replace: Mutex::new(None),
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
    /// `chain_nonce` when the chain is behind local. See
    /// [`NonceTracker::recover_stalled`]. Returns true if it rewound.
    ///
    /// This is NOT internally gated on `pending_count` or any in-flight check —
    /// it only verifies `chain_nonce < current`, then rewinds and clears gaps.
    /// ALL safety proof is the CALLER's responsibility: before calling, the
    /// caller must establish that no transaction is genuinely live at or above
    /// `chain_nonce`. A sustained stall plus a single pending-nonce read is a
    /// weak proof under multi-RPC/preconf propagation; prefer corroborating it
    /// with a second chain view (pending == latest), the in-flight ledger
    /// (`nonce_health_check().lowest_unresolved` aged out), and/or an
    /// independent-RPC liveness probe of the stranded nonce's tx hashes.
    ///
    /// This rewind path handles only the DROPPED-reservation case (chain behind
    /// local, nothing in flight). For the head-of-line stall where txs ARE in
    /// flight (`inflight_count > 0`), do NOT rewind — use
    /// [`replace_stalled_nonce`](Self::replace_stalled_nonce) instead, which is
    /// safe precisely because it replaces the stuck nonce rather than rewinding
    /// past live broadcasts.
    #[inline]
    pub fn recover_stalled(&self, chain_nonce: u64) -> bool {
        self.nonce_manager.recover_stalled(chain_nonce)
    }

    /// Get RPC client reference.
    ///
    /// LEDGER CONTRACT: this is a raw escape hatch. Broadcasting a transaction at a
    /// managed nonce through `wallet.rpc().send_raw_transaction(..)` (instead of
    /// [`send`](Self::send)/[`send_signed`](Self::send_signed)/
    /// [`send_with_preheat`](Self::send_with_preheat)) bypasses the in-flight fee
    /// ledger, so [`replace_stalled_nonce`](Self::replace_stalled_nonce) will compute
    /// its bump from a STALE fee basis and may underprice the real (unrecorded) tx.
    /// Use `rpc()` for reads (`call`, receipts, nonce); broadcast managed nonces only
    /// through the wallet's send methods.
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
    /// may release only before broadcast starts; mark it broadcasting before
    /// handing the signed bytes to an RPC, then commit after inclusion.
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
    /// that is bound to a signed, soon-to-be-broadcast transaction. Chain
    /// reconciliation owns it after that transition. Returns Err if the
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

    /// Release a preheated context before broadcasting its signed transaction.
    pub fn release_preheat(&self, ctx: &PreheatedContext) {
        let nonce = ctx.reserved_nonce();
        if ctx.release_reservation() {
            let nonce = nonce.expect("released preheat reservation has a nonce");
            self.inflight_nonces.mark_released(nonce, "preheat_release");
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
                self.inflight_nonces.record_signed(
                    tx.nonce(),
                    tx.hash(),
                    tx.max_fee_per_gas(),
                    tx.max_priority_fee_per_gas(),
                );
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
            self.inflight_nonces.record_signed(
                tx.nonce(),
                tx.hash(),
                tx.max_fee_per_gas(),
                tx.max_priority_fee_per_gas(),
            );
        }
        result
    }

    fn record_broadcast_candidate(&self, tx: &Transaction) {
        self.inflight_nonces.record_signed(
            tx.nonce(),
            tx.hash(),
            tx.max_fee_per_gas(),
            tx.max_priority_fee_per_gas(),
        );
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

    async fn handle_preheat_broadcast_error(
        &self,
        ctx: &PreheatedContext,
        tx: &Transaction,
        error: &WalletError,
    ) {
        if is_definitive_precheck_rejection(error) && ctx.release_rejected_broadcast() {
            self.inflight_nonces
                .mark_released(tx.nonce(), "definitive_precheck_rejection");
        } else {
            self.recover_nonce_error(error).await;
        }
    }

    /// Send a pre-signed transaction
    ///
    /// Uses a timeout when acquiring the pending transaction permit to avoid
    /// blocking indefinitely when many transactions are in-flight. Once this
    /// method is called, an error leaves the nonce for chain reconciliation:
    /// a racing endpoint may have accepted the signed bytes without returning
    /// a successful response.
    pub async fn send_signed(&self, tx: &Transaction) -> WalletResult<B256> {
        self.record_broadcast_candidate(tx);
        let result = self.broadcast_signed_hash(tx).await;

        match &result {
            Ok(tx_hash) => {
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), *tx_hash);
            }
            Err(e) => {
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
        self.record_broadcast_candidate(tx);
        let result = self.broadcast_signed_result(tx, sign_ms).await;

        match &result {
            Ok(_) => {
                self.inflight_nonces
                    .mark_broadcast_accepted(tx.nonce(), tx.hash());
            }
            Err(e) => {
                self.recover_nonce_error(e).await;
            }
        }

        result
    }

    /// Verify a broadcast TX is visible in the mempool/chain.
    /// Poll `eth_getTransactionByHash` for `tx_hash` for up to `max_wait`, WITHOUT
    /// re-broadcasting or touching the nonce. Marks the in-flight ledger
    /// `MempoolSeen` when observed (preserving the propagation signal used by
    /// nonce-health gating). Returns true iff the tx was observed within the window.
    ///
    /// This is the safe variant for callers that have already handed `tx_hash` to a
    /// receipt poller and own their own retry/reclaim: unlike [`Self::verify_broadcast`]
    /// it never bumps the fee, never changes the tracked hash, and never releases the
    /// nonce — so a fire-and-forget lane's receipt poll keeps pointing at a hash that
    /// can still mine, and the caller's reclaim ownership is unaffected.
    pub async fn verify_broadcast_check_only(
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
        Ok(false)
    }

    /// Polls `eth_getTransactionByHash` for up to `max_wait`. If not found, re-broadcasts
    /// with bumped fees (the vanished tx was likely evicted as underpriced; an
    /// identical-gas re-send won't land). Same nonce ⇒ original and replacement are
    /// mutually exclusive (≤1 mines).
    ///
    /// Returns a [`VerifyOutcome`]: `in_mempool` is true if the original was found
    /// as-is, and `effective_hash` is the hash the caller should now track — the
    /// ORIGINAL when found, or the NEW bumped hash after a re-broadcast. Callers
    /// MUST rebind to `effective_hash`; the original hash will never appear on-chain
    /// once it has been replaced. A failed re-broadcast leaves the nonce for chain
    /// reconciliation because an RPC error cannot disprove acceptance.
    ///
    /// If your caller has already handed `tx_hash` to a receipt poller and owns its
    /// own retry/reclaim (e.g. a fire-and-forget lane), use
    /// [`Self::verify_broadcast_check_only`] instead — it never bumps the hash or
    /// touches the nonce.
    pub async fn verify_broadcast(
        &self,
        tx: &Transaction,
        tx_hash: B256,
        max_wait: Duration,
    ) -> WalletResult<VerifyOutcome> {
        let start = Instant::now();
        if self
            .verify_broadcast_check_only(tx, tx_hash, max_wait)
            .await?
        {
            return Ok(VerifyOutcome {
                in_mempool: true,
                effective_hash: tx_hash,
            });
        }

        // TX not found — re-broadcast with bumped fees (RBF, +12.5% floor).
        let resend = tx
            .bumped_resign(&self.signer, RBF_MIN_BUMP_BPS)
            .and_then(|r| r.ok());
        let send_tx = resend.as_ref().unwrap_or(tx);
        let send_hash = send_tx.hash();
        tracing::warn!(
            old_tx_hash = %tx_hash,
            new_tx_hash = %send_hash,
            nonce = tx.nonce(),
            bumped = resend.is_some(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "TX not in mempool after verification window, re-broadcasting with bumped fees"
        );
        let hex_tx = send_tx.to_hex();
        let result = if let Some(batch_client) = &self.batch_client {
            batch_client.broadcast_transaction(&hex_tx).await
        } else {
            self.rpc_client.send_raw_transaction(&hex_tx).await
        };

        if let Err(error) = result {
            tracing::warn!(%send_hash, error = %error,
                "re-broadcast failed; leaving nonce for chain reconciliation");
            return Err(error);
        };
        // Record the bumped tx's hash AND its (higher) fees so a later
        // replace_stalled_nonce bumps above the most expensive broadcast at this nonce.
        self.inflight_nonces.record_signed(
            send_tx.nonce(),
            send_hash,
            send_tx.max_fee_per_gas(),
            send_tx.max_priority_fee_per_gas(),
        );
        self.inflight_nonces
            .mark_rebroadcast_accepted(send_tx.nonce(), send_hash);
        tracing::info!(%send_hash, "re-broadcast succeeded");
        Ok(VerifyOutcome {
            in_mempool: false,
            effective_hash: send_hash,
        })
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

    /// Replace the head-of-line stuck nonce so the chain advances and the queue
    /// behind it unblocks. This is the recovery path that works WITH in-flight
    /// transactions present — the condition the [`recover_stalled`](Self::recover_stalled)
    /// rewind intentionally refuses. v0.1.40 ships the **cancel** form: a 0-value
    /// self-transfer at `chain_next` with a high tip; the original stranded tx (if
    /// any) and the cancel share the nonce, so at most one mines and the queue
    /// advances regardless.
    ///
    /// Safety: acts only when the chain agrees `chain_next` is next (pending-tag
    /// proof via `eth_getTransactionCount(latest)`), never targets a nonce below
    /// `chain_next` (already mined), aborts if a concurrent reservation/external
    /// consumption moved the local counter behind `chain_next`, and is idempotent
    /// within [`STALL_REPLACE_WINDOW`].
    ///
    /// `tip_bump_bps` is the desired tip bump over the fee basis; it is floored at
    /// [`RBF_MIN_BUMP_BPS`]. The basis is `max(current network price, the stranded
    /// tx's own fee)` so the cancel out-bids an over-paid stranded liquidation tx.
    ///
    /// Known limitations (by design for v0.1.40):
    /// - The pending-tag proof is a single `eth_getTransactionCount(latest)` read.
    ///   Under multi-RPC propagation lag a lagging endpoint could report `chain_next`
    ///   after the real tx mined elsewhere; the worst outcome is a harmless
    ///   nonce-too-low rejection of the cancel, never a double-spend (same nonce ⇒
    ///   ≤1 mines). Callers wanting a stronger gate should corroborate with a streak
    ///   and the in-flight ledger age before calling.
    /// - The cancel only acts when the lowest unresolved in-flight nonce equals
    ///   `chain_next`. A stall where `chain_next` is a released LOCAL gap (nothing
    ///   in flight there) is left to the [`recover_stalled`](Self::recover_stalled)
    ///   rewind path, which is the correct tool for the dropped-reservation case.
    /// - LEDGER CONTRACT: the stranded fee basis comes from the in-flight ledger,
    ///   which is authoritative ONLY for nonces broadcast through this wallet's send
    ///   methods ([`send`](Self::send) / [`send_signed`](Self::send_signed) /
    ///   [`send_with_preheat`](Self::send_with_preheat) / [`verify_broadcast`](Self::verify_broadcast)).
    ///   A tx broadcast at a managed nonce via a raw escape hatch ([`rpc`](Self::rpc),
    ///   `TransactionBroadcaster`) is NOT recorded, so the cancel could bump from a
    ///   stale (lower) basis. Broadcast managed nonces only through the send methods.
    pub async fn replace_stalled_nonce(
        &self,
        chain_next: u64,
        tip_bump_bps: u32,
    ) -> WalletResult<ReplaceOutcome> {
        // Idempotency: skip a repeat replacement of the same nonce within the window.
        if let Some((nonce, at)) = *self.last_stall_replace.lock() {
            if nonce == chain_next && at.elapsed() < STALL_REPLACE_WINDOW {
                return Ok(ReplaceOutcome::NotStalled);
            }
        }

        // Pending-tag safety proof: the chain must agree `chain_next` is next, i.e.
        // none of our in-flight txs at/above it are in the canonical pending set.
        let chain_latest = self.rpc_client.get_nonce_latest(self.address()).await?;
        if chain_latest != chain_next {
            return Ok(ReplaceOutcome::NotStalled);
        }

        // The HEAD itself must be the stuck in-flight tx. Requiring the lowest
        // unresolved nonce to equal `chain_next` (not merely be >= it) proves
        // `chain_next` is genuinely in-flight — NOT a released local gap that a
        // concurrent `sign()` could hand out, which would race our cancel. It also
        // gives us the stranded tx's own fee for the RBF-correct bump below.
        self.inflight_nonces.prune_consumed(chain_next);
        let candidate = self.lowest_unresolved_inflight(chain_next);
        let stranded = match candidate {
            Some(snap) if snap.nonce == chain_next => snap,
            _ => return Ok(ReplaceOutcome::NoCandidate),
        };

        // Network fees fetched BEFORE the final guard so there is no `.await` between
        // the guard and signing (closes the check→sign race window).
        let network_tip = self.get_priority_fee().await?;
        let network_gas = self.get_gas_price().await?;

        // Final guard (no await until broadcast). The local high-water must be
        // STRICTLY above the head: automatic `sign()` issues `nonce_manager.peek()`
        // then increments, so `current_nonce() == chain_next` means a concurrent
        // `sign()` could still be handed `chain_next` and collide with our cancel.
        // Requiring `current > chain_next` proves auto-sign cannot produce it (and
        // `chain_next` is in-flight, not a recycled gap, per the candidate gate above).
        if self.current_nonce() <= chain_next {
            return Ok(ReplaceOutcome::RaceAborted);
        }

        // Build a competitive cancel: 0-value self-transfer at `chain_next`, bumped
        // over max(network, stranded) so it can replace an over-paid stranded tx.
        let bump_bps = tip_bump_bps.max(RBF_MIN_BUMP_BPS);
        let (tip, max_fee) = compute_cancel_fees(
            network_tip,
            network_gas,
            stranded.max_priority_seen,
            stranded.max_fee_seen,
            bump_bps,
        );

        // Sign WITHOUT recording: only a successfully broadcast cancel should
        // contribute its fee to the ledger, so a failed broadcast can't pollute
        // `max_fee_seen` and over-escalate the next retry.
        let mut request = TransactionRequest::new()
            .to(self.address())
            .value(U256::ZERO)
            .gas_limit(21_000)
            .max_priority_fee_per_gas(tip)
            .max_fee_per_gas(max_fee);
        request.nonce = chain_next;
        request.chain_id = self.config.chain_id;
        let tx = request.build_and_sign(&self.signer)?;
        let tx_hash = tx.hash();
        let hex_tx = tx.to_hex();

        let result = if let Some(batch_client) = &self.batch_client {
            batch_client.broadcast_transaction(&hex_tx).await
        } else {
            self.rpc_client.send_raw_transaction(&hex_tx).await
        };

        match result {
            Ok(_) => {
                // Record only now that the cancel was accepted.
                self.inflight_nonces
                    .record_signed(chain_next, tx_hash, Some(max_fee), Some(tip));
                self.inflight_nonces
                    .mark_rebroadcast_accepted(chain_next, tx_hash);
                *self.last_stall_replace.lock() = Some((chain_next, Instant::now()));
                tracing::warn!(
                    nonce = chain_next,
                    %tx_hash,
                    tip_wei = %tip,
                    "replace_stalled_nonce: broadcast cancel self-transfer to unblock head-of-line queue"
                );
                Ok(ReplaceOutcome::Cancelled {
                    nonce: chain_next,
                    tx_hash,
                })
            }
            Err(error) => {
                tracing::warn!(nonce = chain_next, error = %error, "replace_stalled_nonce broadcast failed");
                Err(error)
            }
        }
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
                self.handle_preheat_broadcast_error(ctx, &tx, error).await;
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
                self.handle_preheat_broadcast_error(ctx, &tx, error).await;
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
                self.handle_preheat_broadcast_error(ctx, &tx, error).await;
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
    exclusive_broadcast: bool,
    config: WalletConfig,
    initial_nonce: Option<u64>,
}

/// Resolve the final broadcast RPC list: `primary_rpc` prepended unless
/// `exclusive` is set, in which case `broadcast_rpcs` is used as-is. Shared
/// by `FastWalletBuilder::build()`/`build_with_nonce()` so the two callers
/// can't drift on this logic.
fn resolve_broadcast_rpcs(primary_rpc: &str, broadcast_rpcs: Vec<String>, exclusive: bool) -> Vec<String> {
    if exclusive {
        broadcast_rpcs
    } else {
        let mut all_rpcs = vec![primary_rpc.to_string()];
        all_rpcs.extend(broadcast_rpcs);
        all_rpcs
    }
}

impl FastWalletBuilder {
    pub fn new(private_key: impl Into<String>, primary_rpc: impl Into<String>) -> Self {
        Self {
            private_key: private_key.into(),
            primary_rpc: primary_rpc.into(),
            gas_rpc_url: None,
            broadcast_rpcs: Vec::new(),
            exclusive_broadcast: false,
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
            exclusive_broadcast: false,
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
            exclusive_broadcast: false,
            config: WalletConfig::default(),
            initial_nonce: None,
        }
    }

    /// Add RPC endpoints for parallel broadcasting. `primary_rpc` is always
    /// included in the broadcast fan-out alongside these (the default,
    /// backward-compatible behavior) — see `broadcast_rpcs_exclusive` if
    /// `primary_rpc` must NOT receive the signed transaction.
    pub fn broadcast_rpcs(mut self, rpcs: Vec<String>) -> Self {
        self.broadcast_rpcs = rpcs;
        self.exclusive_broadcast = false;
        self
    }

    /// Add RPC endpoints for parallel broadcasting, WITHOUT including
    /// `primary_rpc` in the broadcast set.
    ///
    /// Use this when `primary_rpc` must not receive the signed transaction —
    /// e.g. `primary_rpc` is a public endpoint and `rpcs` is meant to be an
    /// exclusively private-routing broadcast set (Flashbots Protect, MEV
    /// Blocker, etc.) for MEV protection. `primary_rpc` is still used for
    /// everything else (nonce fetch, `eth_call`, receipt polling via
    /// `self.rpc()`) — only the broadcast fan-out excludes it.
    ///
    /// Contrast with `broadcast_rpcs`, which always prepends `primary_rpc`
    /// to the broadcast list.
    pub fn broadcast_rpcs_exclusive(mut self, rpcs: Vec<String>) -> Self {
        self.broadcast_rpcs = rpcs;
        self.exclusive_broadcast = true;
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
        // Cross-audit finding (both codex and an independent reviewer,
        // 2026-07-17): `broadcast_rpcs_exclusive(vec![])` used to silently
        // skip batch_client construction entirely (the `!is_empty()` guards
        // below never fire), leaving the wallet to fall back to
        // `self.rpc_client` (primary_rpc) for every broadcast — exactly the
        // public-endpoint leak `exclusive_broadcast` exists to prevent, with
        // no error raised. Fail loudly instead: an exclusive broadcast set
        // with zero endpoints can never honor its own exclusivity contract.
        if self.exclusive_broadcast && self.broadcast_rpcs.is_empty() {
            return Err(WalletError::InvalidConfig(
                "broadcast_rpcs_exclusive requires at least one RPC endpoint — an empty list would silently fall back to broadcasting via primary_rpc".into(),
            ));
        }
        let gas_rpc_url = self.gas_rpc_url;

        if let Some(nonce) = self.initial_nonce {
            let mut wallet = FastWallet::with_known_nonce(
                &self.private_key,
                &self.primary_rpc,
                nonce,
                self.config,
            )?;

            if !self.broadcast_rpcs.is_empty() {
                let all_rpcs =
                    resolve_broadcast_rpcs(&self.primary_rpc, self.broadcast_rpcs, self.exclusive_broadcast);
                wallet.batch_client = Some(Arc::new(BatchRpcClient::new(all_rpcs)?));
            }

            if let Some(url) = gas_rpc_url {
                wallet.gas_rpc_client = Some(Arc::new(RpcClient::new(&url)?));
            }

            Ok(wallet)
        } else if !self.broadcast_rpcs.is_empty() {
            let mut wallet = if self.exclusive_broadcast {
                let mut wallet =
                    FastWallet::new(&self.private_key, &self.primary_rpc, self.config).await?;
                wallet.batch_client = Some(Arc::new(BatchRpcClient::new(self.broadcast_rpcs)?));
                wallet
            } else {
                FastWallet::with_multiple_rpcs(
                    &self.private_key,
                    &self.primary_rpc,
                    self.broadcast_rpcs,
                    self.config,
                )
                .await?
            };

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
        // See the matching guard in `build()` — an empty exclusive broadcast
        // set must fail loudly, not silently fall back to primary_rpc.
        if self.exclusive_broadcast && self.broadcast_rpcs.is_empty() {
            return Err(WalletError::InvalidConfig(
                "broadcast_rpcs_exclusive requires at least one RPC endpoint — an empty list would silently fall back to broadcasting via primary_rpc".into(),
            ));
        }
        let mut wallet =
            FastWallet::with_known_nonce(&self.private_key, &self.primary_rpc, nonce, self.config)?;

        if !self.broadcast_rpcs.is_empty() {
            let all_rpcs =
                resolve_broadcast_rpcs(&self.primary_rpc, self.broadcast_rpcs, self.exclusive_broadcast);
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

    async fn rpc_send_error_server(code: i64, message: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf)
                .await
                .unwrap();
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":1,"error":{{"code":{code},"message":"{message}"}}}}"#
            );
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

    /// Default (backward-compatible) behavior: `primary_rpc` is always
    /// included in the broadcast fan-out alongside the configured
    /// `broadcast_rpcs`.
    #[test]
    fn resolve_broadcast_rpcs_default_includes_primary() {
        let rpcs = resolve_broadcast_rpcs(
            "https://primary.example.com",
            vec!["https://a.example.com".into(), "https://b.example.com".into()],
            false,
        );
        assert_eq!(
            rpcs,
            vec![
                "https://primary.example.com".to_string(),
                "https://a.example.com".to_string(),
                "https://b.example.com".to_string(),
            ]
        );
    }

    /// Exclusive mode: `primary_rpc` must NOT appear anywhere in the
    /// resolved broadcast list — this is the actual fix for the MEV-leak
    /// this feature exists to close (a public primary_rpc must never
    /// receive a tx meant to be routed exclusively through private
    /// endpoints like Flashbots Protect / MEV Blocker).
    #[test]
    fn resolve_broadcast_rpcs_exclusive_excludes_primary() {
        let primary = "https://primary.example.com";
        let rpcs = resolve_broadcast_rpcs(
            primary,
            vec!["https://relay.flashbots.net".into(), "https://rpc.mevblocker.io/fullprivacy".into()],
            true,
        );
        assert_eq!(
            rpcs,
            vec![
                "https://relay.flashbots.net".to_string(),
                "https://rpc.mevblocker.io/fullprivacy".to_string(),
            ]
        );
        assert!(!rpcs.iter().any(|url| url == primary), "primary_rpc leaked into an exclusive broadcast set");
    }

    /// `broadcast_rpcs_exclusive` builder method actually sets the flag the
    /// resolver reads — a regression here would silently re-introduce the
    /// leak even with the resolver itself correct.
    #[test]
    fn broadcast_rpcs_exclusive_sets_the_exclusive_flag() {
        let builder = FastWalletBuilder::new(TEST_PRIVATE_KEY, "https://primary.example.com")
            .broadcast_rpcs_exclusive(vec!["https://relay.flashbots.net".into()]);
        assert!(builder.exclusive_broadcast);

        // The non-exclusive method must leave it false, and switching back
        // via `broadcast_rpcs` after calling the exclusive variant must
        // reset the flag rather than leaving stale exclusive state.
        let builder = FastWalletBuilder::new(TEST_PRIVATE_KEY, "https://primary.example.com")
            .broadcast_rpcs_exclusive(vec!["https://relay.flashbots.net".into()])
            .broadcast_rpcs(vec!["https://a.example.com".into()]);
        assert!(!builder.exclusive_broadcast);
    }

    /// End-to-end via the sync builder path (`build_with_nonce`, no network
    /// access needed): confirms that a NON-EMPTY exclusive broadcast list
    /// constructs a batch client containing ONLY the exclusive endpoints —
    /// `endpoint_count() == 1`, not 2 — i.e. primary_rpc is genuinely never
    /// folded in for this case. (Round-2 audit correction: this does NOT
    /// discriminate old vs. new code — `resolve_broadcast_rpcs`'s exclusive
    /// branch already excluded primary_rpc correctly for non-empty lists in
    /// the original 105846e commit; the actual bug this session fixed was
    /// specific to an EMPTY exclusive list silently falling back to
    /// primary-only broadcast — see the two `_rejects_empty_exclusive_`
    /// tests below for that regression coverage instead.)
    #[test]
    fn build_with_nonce_exclusive_broadcast_constructs_batch_client() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .broadcast_rpcs_exclusive(vec!["http://localhost:8546".into()])
            .build_with_nonce(0)
            .unwrap();
        assert_eq!(wallet.batch_client.as_ref().unwrap().endpoint_count(), 1);
    }

    /// Cross-audit finding (both codex and an independent reviewer,
    /// 2026-07-17): an empty exclusive broadcast list must fail loudly, not
    /// silently degrade to broadcasting via primary_rpc.
    #[test]
    fn build_with_nonce_rejects_empty_exclusive_broadcast_list() {
        let result = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .broadcast_rpcs_exclusive(vec![])
            .build_with_nonce(0);
        assert!(matches!(result, Err(WalletError::InvalidConfig(_))));
    }

    /// Same guard, but on the async `build()` path (a separate function
    /// with its own duplicated check, not a delegate of `build_with_nonce`)
    /// — must not be exercised only on the sync path and assumed identical.
    #[tokio::test]
    async fn build_rejects_empty_exclusive_broadcast_list() {
        let result = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
            .broadcast_rpcs_exclusive(vec![])
            .initial_nonce(0)
            .build()
            .await;
        assert!(matches!(result, Err(WalletError::InvalidConfig(_))));
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
    async fn test_verify_broadcast_rebroadcast_failure_retains_nonce() {
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
        assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 1);
    }

    #[tokio::test]
    async fn definitive_insufficient_funds_preheat_rejection_reuses_nonce() {
        let url = rpc_send_error_server(
            -32003,
            "insufficient funds for gas * price + value",
        )
        .await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();
        let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());

        let result = wallet.send_with_preheat(&ctx, test_request()).await;

        assert!(result.is_err());
        assert_eq!(
            wallet.nonce_manager.reserve().nonce(),
            0,
            "definitive -32003 rejection must recycle the preheated nonce"
        );
    }

    #[tokio::test]
    async fn batch_definitive_insufficient_funds_rejections_reuse_nonce() {
        let primary = rpc_send_error_server(
            -32003,
            "insufficient funds for gas * price + value",
        )
        .await;
        let secondary = rpc_send_error_server(
            -32003,
            "insufficient funds for gas * price + value",
        )
        .await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary)
            .chain_id(1)
            .broadcast_rpcs(vec![secondary])
            .build_with_nonce(0)
            .unwrap();
        let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());

        let result = wallet.send_with_preheat(&ctx, test_request()).await;

        assert!(result.is_err());
        assert_eq!(
            wallet.nonce_manager.reserve().nonce(),
            0,
            "all-endpoint definitive rejection must recycle the preheated nonce"
        );
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

    // ============ compute_cancel_fees (fee relativity, item 3) ============

    #[test]
    fn compute_cancel_fees_bumps_over_network_when_network_higher() {
        // Stranded tx was cheap; network has since risen → bump over network.
        let (tip, max_fee) = compute_cancel_fees(
            U256::from(1_000u64), // network tip
            U256::from(2_000u64), // network gas
            Some(U256::from(100u64)),
            Some(U256::from(200u64)),
            RBF_MIN_BUMP_BPS,
        );
        assert_eq!(tip, U256::from(1_125u64)); // 1000 * 1.125
        assert_eq!(max_fee, U256::from(2_250u64)); // 2000 * 1.125
    }

    #[test]
    fn compute_cancel_fees_bumps_over_stranded_when_stranded_higher() {
        // The incident case: stranded liquidation was OVERPAID (tip 100 gwei) while
        // network has dropped to 1 gwei. The cancel MUST out-bid the stranded tx,
        // not the network, or RBF replacement fails and the queue stays stuck.
        let (tip, max_fee) = compute_cancel_fees(
            U256::from(1_000_000_000u64),         // network tip 1 gwei
            U256::from(2_000_000_000u64),         // network gas 2 gwei
            Some(U256::from(100_000_000_000u64)), // stranded tip 100 gwei
            Some(U256::from(120_000_000_000u64)), // stranded maxFee 120 gwei
            RBF_MIN_BUMP_BPS,
        );
        // Must bump over the STRANDED fee (100/120 gwei), not the 1/2 gwei network.
        assert_eq!(tip, U256::from(112_500_000_000u64)); // 100 gwei * 1.125
        assert_eq!(max_fee, U256::from(135_000_000_000u64)); // 120 gwei * 1.125
    }

    #[test]
    fn compute_cancel_fees_handles_missing_stranded_fees() {
        // Legacy stranded tx (no EIP-1559 fields) → fall back to network basis.
        let (tip, max_fee) = compute_cancel_fees(
            U256::from(1_000u64),
            U256::from(2_000u64),
            None,
            None,
            RBF_MIN_BUMP_BPS,
        );
        assert_eq!(tip, U256::from(1_125u64));
        assert_eq!(max_fee, U256::from(2_250u64));
    }

    #[test]
    fn compute_cancel_fees_max_fee_never_below_tip() {
        // Degenerate: network gas < tip basis → max_fee floored at tip.
        let (tip, max_fee) = compute_cancel_fees(
            U256::from(10_000u64),
            U256::from(1u64),
            None,
            None,
            RBF_MIN_BUMP_BPS,
        );
        assert!(max_fee >= tip);
    }

    // ============ replace_stalled_nonce ============

    /// Minimal method-routing mock JSON-RPC server. Each request gets its own
    /// connection (`Connection: close`) so it is robust to client pooling; routes
    /// by JSON-RPC `method`. `tx_found` controls `eth_getTransactionByHash`
    /// (false = accepted-but-never-mined, the strand condition). Returns the URL and
    /// a shared buffer capturing every `eth_sendRawTransaction` raw-tx hex so tests
    /// can assert what was actually broadcast (not just the returned outcome).
    async fn mock_rpc_server(
        latest_nonce: u64,
        tip_wei: u64,
        gas_price_wei: u64,
        tx_found: bool,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        mock_rpc_server_cfg(latest_nonce, tip_wei, gas_price_wei, tx_found, false).await
    }

    /// Like [`mock_rpc_server`] but `send_fails` makes `eth_sendRawTransaction` return
    /// a JSON-RPC error (the raw tx is still captured) — used to exercise the
    /// broadcast-failure path.
    async fn mock_rpc_server_cfg(
        latest_nonce: u64,
        tip_wei: u64,
        gas_price_wei: u64,
        tx_found: bool,
        send_fails: bool,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sent: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sent_srv = sent.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let sent_conn = sent_srv.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = match stream.read(&mut buf).await {
                        Ok(n) if n > 0 => n,
                        _ => return,
                    };
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let mut error_body = false;
                    let result = if req.contains("eth_getTransactionCount") {
                        format!("\"0x{latest_nonce:x}\"")
                    } else if req.contains("eth_maxPriorityFeePerGas") {
                        format!("\"0x{tip_wei:x}\"")
                    } else if req.contains("eth_gasPrice") {
                        format!("\"0x{gas_price_wei:x}\"")
                    } else if req.contains("eth_sendRawTransaction") {
                        // Capture the raw tx hex (params[0]) for test assertions.
                        if let Some(raw) = req
                            .split("\"params\":[\"")
                            .nth(1)
                            .and_then(|s| s.split('"').next())
                        {
                            sent_conn.lock().push(raw.to_string());
                        }
                        if send_fails {
                            error_body = true;
                            String::new()
                        } else {
                            format!("\"0x{}\"", "11".repeat(32))
                        }
                    } else if req.contains("eth_getTransactionByHash") {
                        if tx_found {
                            "{\"hash\":\"0x01\"}".to_string()
                        } else {
                            "null".to_string()
                        }
                    } else {
                        "null".to_string()
                    };
                    let body = if error_body {
                        "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32000,\"message\":\"send failed\"}}".to_string()
                    } else {
                        format!("{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{result}}}")
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (format!("http://{addr}"), sent)
    }

    /// Sign an EIP-1559 tx at `nonce` with the given fees, using the wallet's signer.
    fn signed_1559_at(wallet: &FastWallet, nonce: u64, tip: u64, max_fee: u64) -> Transaction {
        TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::ZERO)
            .gas_limit(21000)
            .max_priority_fee_per_gas(U256::from(tip))
            .max_fee_per_gas(U256::from(max_fee))
            .nonce(nonce)
            .chain_id(1)
            .build_and_sign(&wallet.signer)
            .unwrap()
    }

    #[tokio::test]
    async fn replace_stalled_nonce_broadcasts_eip1559_cancel_at_head() {
        // Strand: chain frozen at 50 (latest == 50), one in-flight nonce at 50.
        let (url, sent) = mock_rpc_server(50, 1_000_000_000, 20_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();
        // In-flight candidate at exactly nonce 50 (advances local high-water to 51).
        wallet.sign(test_request()).unwrap();
        assert!(wallet.current_nonce() >= 50);

        let outcome = wallet.replace_stalled_nonce(50, 0).await.unwrap();
        match outcome {
            ReplaceOutcome::Cancelled { nonce, .. } => assert_eq!(nonce, 50),
            other => panic!("expected Cancelled, got {other:?}"),
        }
        // Exactly one tx broadcast, and it is a typed EIP-1559 tx (0x02 prefix) —
        // i.e. an actual cancel was emitted, not just an outcome returned.
        let sent = sent.lock();
        assert_eq!(sent.len(), 1, "expected one broadcast cancel tx");
        assert!(
            sent[0].starts_with("0x02"),
            "cancel must be EIP-1559 typed, got {}",
            &sent[0][..6.min(sent[0].len())]
        );
        assert_eq!(wallet.last_stall_replace.lock().map(|(n, _)| n), Some(50));
    }

    #[tokio::test]
    async fn replace_stalled_nonce_outbids_overpaid_stranded_tx() {
        // The incident: stranded liquidation at nonce 50 paid 100 gwei tip; network
        // has since dropped to 1 gwei. The cancel must still be emitted (the fee math
        // is proven by compute_cancel_fees tests; here we prove the stranded fee is
        // read from the ledger and the EIP-1559 path is taken end-to-end).
        let (url, sent) = mock_rpc_server(50, 1_000_000_000, 2_000_000_000, false).await;
        // Local high-water at 51 (we moved past 50 locally), chain stuck at 50.
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(51)
            .unwrap();
        // Record a high-fee stranded tx at nonce 50 in the ledger.
        let stranded = signed_1559_at(&wallet, 50, 100_000_000_000, 120_000_000_000);
        wallet.inflight_nonces.record_signed(
            50,
            stranded.hash(),
            stranded.max_fee_per_gas(),
            stranded.max_priority_fee_per_gas(),
        );

        let snap = wallet.lowest_unresolved_inflight(50).unwrap();
        assert_eq!(snap.max_priority_seen, Some(U256::from(100_000_000_000u64)));

        let outcome = wallet.replace_stalled_nonce(50, 0).await.unwrap();
        assert!(matches!(
            outcome,
            ReplaceOutcome::Cancelled { nonce: 50, .. }
        ));
        // Decode the ACTUAL broadcast cancel and assert it out-bids the stranded
        // 100/120 gwei tx (bump 12.5% over the stranded fee, NOT the 1/2 gwei
        // network). A buggy impl that used network fees would emit ~1.125 gwei here.
        let raw = sent.lock()[0].clone();
        let (tip, max_fee) = decode_1559_fees(&raw);
        assert_eq!(
            tip,
            U256::from(112_500_000_000u64),
            "tip = 100 gwei * 1.125"
        );
        assert_eq!(
            max_fee,
            U256::from(135_000_000_000u64),
            "maxFee = 120 gwei * 1.125"
        );
    }

    #[tokio::test]
    async fn send_signed_retains_external_tx_fees() {
        // An externally-signed EIP-1559 tx broadcast via send_signed must have its
        // fees retained in the ledger (finding 1) so a later cancel can out-bid it.
        let (url, _sent) = mock_rpc_server(0, 1_000_000_000, 2_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(7)
            .unwrap();
        // Built directly (not via wallet.sign), so only send_signed can record fees.
        let tx = signed_1559_at(&wallet, 7, 100_000_000_000, 120_000_000_000);
        wallet.send_signed(&tx).await.unwrap();

        let snap = wallet.lowest_unresolved_inflight(7).unwrap();
        assert_eq!(snap.max_priority_seen, Some(U256::from(100_000_000_000u64)));
        assert_eq!(snap.max_fee_seen, Some(U256::from(120_000_000_000u64)));
    }

    #[tokio::test]
    async fn replace_stalled_nonce_race_aborts_when_local_equals_head() {
        // current_nonce() == chain_next must abort: auto-sign() could still be handed
        // chain_next and collide with the cancel (finding 2).
        let (url, sent) = mock_rpc_server(50, 1_000_000_000, 2_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();
        // In-flight at 50 via sign_with_nonce (does NOT advance the counter) → local
        // high-water stays at 50 == chain_next.
        wallet.sign_with_nonce(test_request(), 50).unwrap();
        assert_eq!(wallet.current_nonce(), 50);
        assert_eq!(
            wallet.replace_stalled_nonce(50, 0).await.unwrap(),
            ReplaceOutcome::RaceAborted
        );
        assert_eq!(sent.lock().len(), 0, "no cancel must be broadcast");
    }

    #[tokio::test]
    async fn replace_stalled_nonce_failed_broadcast_does_not_pollute_fees() {
        // If the cancel broadcast fails, its fee must NOT be retained (finding 5),
        // so a retry bumps over the stranded tx, not over the unaccepted cancel.
        let (url, sent) = mock_rpc_server_cfg(50, 1_000_000_000, 2_000_000_000, false, true).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(51)
            .unwrap();
        let stranded = signed_1559_at(&wallet, 50, 100_000_000_000, 120_000_000_000);
        wallet.inflight_nonces.record_signed(
            50,
            stranded.hash(),
            stranded.max_fee_per_gas(),
            stranded.max_priority_fee_per_gas(),
        );

        // Broadcast fails → Err, and the ledger fee for nonce 50 stays the stranded
        // tx's fee (not the higher, unaccepted cancel fee).
        assert!(wallet.replace_stalled_nonce(50, 0).await.is_err());
        assert_eq!(sent.lock().len(), 1, "one (failed) broadcast attempt");
        let snap = wallet.lowest_unresolved_inflight(50).unwrap();
        assert_eq!(
            snap.max_priority_seen,
            Some(U256::from(100_000_000_000u64)),
            "failed cancel fee must not pollute the retained stranded fee"
        );
    }

    /// Decode `(max_priority_fee_per_gas, max_fee_per_gas)` from an EIP-1559 raw-tx
    /// hex (`0x02 || rlp([chainId, nonce, maxPriority, maxFee, ...])`).
    fn decode_1559_fees(raw_hex: &str) -> (U256, U256) {
        use alloy::rlp::{Decodable, Header};
        let bytes = hex::decode(raw_hex.strip_prefix("0x").unwrap()).unwrap();
        assert_eq!(bytes[0], 0x02, "not an EIP-1559 typed tx");
        let mut buf: &[u8] = &bytes[1..];
        let header = Header::decode(&mut buf).unwrap();
        assert!(header.list, "EIP-1559 payload must be an RLP list");
        let _chain_id = u64::decode(&mut buf).unwrap();
        let _nonce = u64::decode(&mut buf).unwrap();
        let max_priority = U256::decode(&mut buf).unwrap();
        let max_fee = U256::decode(&mut buf).unwrap();
        (max_priority, max_fee)
    }

    #[tokio::test]
    async fn replace_stalled_nonce_refuses_when_chain_disagrees() {
        // Chain latest is 40, not the 50 the caller thinks is stuck → not a stall.
        let (url, _sent) = mock_rpc_server(40, 1_000_000_000, 20_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();
        assert_eq!(
            wallet.replace_stalled_nonce(50, 0).await.unwrap(),
            ReplaceOutcome::NotStalled
        );
    }

    #[tokio::test]
    async fn replace_stalled_nonce_no_candidate_when_ledger_empty() {
        let (url, _sent) = mock_rpc_server(50, 1_000_000_000, 20_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();
        // No in-flight tx → nothing to replace.
        assert_eq!(
            wallet.replace_stalled_nonce(50, 0).await.unwrap(),
            ReplaceOutcome::NoCandidate
        );
    }

    #[tokio::test]
    async fn replace_stalled_nonce_race_aborts_when_local_behind_head() {
        // Local counter at 42, but an in-flight tx exists at 50 and chain says 50 is
        // next: the local high-water is behind the head → unsafe, abort.
        let (url, _sent) = mock_rpc_server(50, 1_000_000_000, 20_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(42)
            .unwrap();
        // sign_with_nonce records an in-flight at 50 without moving the counter.
        wallet.sign_with_nonce(test_request(), 50).unwrap();
        assert_eq!(wallet.current_nonce(), 42);
        assert_eq!(
            wallet.replace_stalled_nonce(50, 0).await.unwrap(),
            ReplaceOutcome::RaceAborted
        );
    }

    #[tokio::test]
    async fn replace_stalled_nonce_idempotent_within_window() {
        // No RPC should be hit: the idempotency guard returns before any network call.
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://127.0.0.1:1")
            .chain_id(1)
            .build_with_nonce(50)
            .unwrap();
        *wallet.last_stall_replace.lock() = Some((50, Instant::now()));
        assert_eq!(
            wallet.replace_stalled_nonce(50, 0).await.unwrap(),
            ReplaceOutcome::NotStalled
        );
    }

    #[tokio::test]
    async fn verify_broadcast_rebroadcasts_bumped_with_new_effective_hash() {
        // tx never appears in mempool → verify_broadcast re-broadcasts a bumped tx
        // and returns its NEW hash as effective_hash (caller must rebind, item 7 fix).
        let (url, _sent) = mock_rpc_server(0, 1_000_000_000, 20_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();
        let tx = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::ZERO)
            .gas_limit(21000)
            .max_priority_fee_per_gas(U256::from(1_000_000_000u64))
            .max_fee_per_gas(U256::from(20_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build_and_sign(&wallet.signer)
            .unwrap();
        let outcome = wallet
            .verify_broadcast(&tx, tx.hash(), Duration::from_millis(50))
            .await
            .unwrap();
        assert!(!outcome.in_mempool, "tx was never in mempool");
        // effective_hash must be the NEW bumped tx, not the original.
        assert_ne!(
            outcome.effective_hash,
            tx.hash(),
            "rebroadcast bumped the fees, so the effective hash must change"
        );
    }

    #[tokio::test]
    async fn verify_broadcast_check_only_no_rebroadcast_no_release() {
        // tx never appears → check_only returns false WITHOUT re-broadcasting or
        // changing the nonce (verify_broadcast may bump but also retains ownership).
        // The caller owns reclaim and keeps tracking the original hash.
        let (url, sent) = mock_rpc_server(0, 1_000_000_000, 20_000_000_000, false).await;
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
            .chain_id(1)
            .build_with_nonce(0)
            .unwrap();
        let ctx = PreheatedContext::new(wallet.nonce_manager.reserve());
        let tx = wallet.sign_with_preheat(&ctx, test_request()).unwrap();
        wallet.commit_preheat(&ctx);

        let found = wallet
            .verify_broadcast_check_only(&tx, tx.hash(), Duration::from_millis(50))
            .await
            .unwrap();

        assert!(!found, "tx was never in mempool");
        assert!(sent.lock().is_empty(), "check_only must NOT re-broadcast");
        // nonce 0 stays consumed (not released) → next sign advances to nonce 1.
        assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 1);
    }
}

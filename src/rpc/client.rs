// RPC client construction, connection hints, and throttled warmup.
// Implements RpcClient using reqwest and the shared warm-state guard.
use super::*;

impl RpcClient {
    /// Create a new RPC client with optimized settings
    pub fn new(url: impl Into<String>) -> WalletResult<Self> {
        Self::with_request_timeout(url, Duration::from_secs(30))
    }

    pub(crate) fn with_request_timeout(
        url: impl Into<String>,
        request_timeout: Duration,
    ) -> WalletResult<Self> {
        crate::tls::ensure_provider();
        let client = Client::builder()
            // A redirect would resend the signed transaction to another URL, outside the
            // configured (possibly exclusive, private) endpoint set.
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(10)
            // Never evict an idle connection: the HTTP/2 pings below keep it
            // alive and drop it if the peer stops answering.
            .pool_idle_timeout(None)
            .timeout(request_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(15))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .http2_keep_alive_while_idle(true)
            .build()
            .map_err(|e| WalletError::NetworkError(e.to_string()))?;

        let url = url.into();
        Ok(Self {
            client,
            warm: WarmState::for_url(&url),
            url,
            request_id: AtomicU64::new(1),
            request_seen: AtomicBool::new(false),
        })
    }

    /// RPC endpoint URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Warm up the HTTP connection pool by sending a lightweight request
    ///
    /// This establishes TCP/TLS connections ahead of time, saving 5-20ms
    /// on subsequent requests. Use this before time-critical operations.
    /// Sends nothing while this endpoint answered within the warmup window
    /// (4 min for `https://`, 60 s otherwise) and no transport error has
    /// been seen since.
    pub async fn warmup(&self) -> WalletResult<()> {
        if self.should_skip_warmup() {
            return Ok(());
        }
        // eth_chainId is the lightest RPC call - just returns the chain ID.
        // Bound warmup with a short timeout so a blackholed / network-partitioned
        // endpoint cannot stall callers (e.g. preheat_full's `join!`) for the full
        // 30s reqwest timeout. Warmup is best-effort connection priming — a miss is
        // non-fatal (the real send still establishes the connection), so a slow
        // endpoint should be abandoned quickly rather than block the hot path.
        const WARMUP_TIMEOUT: Duration = Duration::from_millis(500);
        match tokio::time::timeout(
            WARMUP_TIMEOUT,
            self.request::<String>("eth_chainId", json!([])),
        )
        .await
        {
            Ok(result) => result.map(|_| ()),
            Err(_) => Err(WalletError::Timeout),
        }
    }

    /// Get next request ID (atomic, lock-free)
    #[inline]
    pub(super) fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub(super) fn mark_request_started(&self) {
        self.request_seen.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub(super) fn connection_reuse_hint(&self) -> bool {
        self.request_seen.swap(true, Ordering::Relaxed)
    }

    fn should_skip_warmup(&self) -> bool {
        !self.warm.probe_due()
    }
}

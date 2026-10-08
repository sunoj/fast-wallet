// Parallel transaction fan-out, complete all-fail verdict collection, one client per endpoint.
// Exports BatchRpcClient methods and normalized_url; depends on RpcClient, futures, verdicts.
use super::*;
use crate::error::{BroadcastFailure, EndpointVerdict};
use futures_util::future::BoxFuture;

pub const DEFAULT_BROADCAST_SEND_TIMEOUT: Duration = Duration::from_secs(6);

type EndpointFuture = BoxFuture<'static, (usize, String, WalletResult<(B256, bool)>, u64)>;

impl BatchRpcClient {
    /// Create a new batch client with multiple RPC endpoints
    pub fn new(urls: Vec<String>) -> WalletResult<Self> {
        let clients: WalletResult<Vec<_>> = urls
            .into_iter()
            .map(|url| RpcClient::new(url).map(Arc::new))
            .collect();

        Ok(Self {
            send_timeout: DEFAULT_BROADCAST_SEND_TIMEOUT,
            clients: clients?,
            current: AtomicU64::new(0),
        })
    }

    /// Batch over `urls` holding one client per endpoint: a URL equal to
    /// `primary`'s (by [`normalized_url`]) reuses that client and its pool, a
    /// repeated URL is dropped, and first-seen order is kept. Sending the same
    /// signed transaction twice to one endpoint adds nothing but a request.
    pub(crate) fn sharing_primary(
        primary: &Arc<RpcClient>,
        urls: Vec<String>,
        request_timeout: Duration,
    ) -> WalletResult<Self> {
        let mut clients: Vec<Arc<RpcClient>> = Vec::with_capacity(urls.len());
        for url in urls {
            let key = normalized_url(&url);
            if clients.iter().any(|c| normalized_url(c.url()) == key) {
                continue;
            }
            if normalized_url(primary.url()) == key {
                clients.push(primary.clone());
            } else {
                clients.push(Arc::new(RpcClient::with_request_timeout(
                    url,
                    request_timeout,
                )?));
            }
        }
        Ok(Self {
            send_timeout: DEFAULT_BROADCAST_SEND_TIMEOUT,
            clients,
            current: AtomicU64::new(0),
        })
    }

    /// Clients in broadcast order, one per endpoint.
    pub(crate) fn clients(&self) -> &[Arc<RpcClient>] {
        &self.clients
    }

    /// Bound only transaction sends; read RPCs retain their shared client timeout.
    pub fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Get the next client (round-robin)
    pub fn next_client(&self) -> Arc<RpcClient> {
        let idx = self.current.fetch_add(1, Ordering::Relaxed) as usize;
        self.clients[idx % self.clients.len()].clone()
    }

    /// Send transaction to all endpoints in parallel, return on first success
    pub async fn broadcast_transaction(&self, raw_tx: &str) -> WalletResult<B256> {
        let result = self.broadcast_transaction_detailed(raw_tx).await?;
        parse_b256_hex(&result.tx_hash)
    }

    /// Send transaction to all endpoints in parallel with first-success timing.
    pub async fn broadcast_transaction_detailed(&self, raw_tx: &str) -> WalletResult<SendResult> {
        if self.clients.is_empty() {
            return Err(WalletError::RpcError("No RPC endpoints".to_string()));
        }

        let fanout_started = Instant::now();
        let pending: Vec<_> = self
            .clients
            .iter()
            .enumerate()
            .map(|(endpoint_index, client)| {
                let client = client.clone();
                let url = client.url().to_string();
                let tx = raw_tx.to_string();
                let timeout = self.send_timeout;
                async move {
                    let started = Instant::now();
                    let result = match tokio::time::timeout(
                        timeout,
                        client.send_raw_transaction_with_connection_hint(&tx),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(WalletError::SendTimeout {
                            elapsed_ms: timeout.as_millis() as u64,
                        }),
                    };
                    (
                        endpoint_index,
                        url,
                        result,
                        started.elapsed().as_millis() as u64,
                    )
                }
                .boxed()
            })
            .collect();

        self.collect_broadcast(pending, fanout_started).await
    }

    async fn collect_broadcast(
        &self,
        mut pending: Vec<EndpointFuture>,
        fanout_started: Instant,
    ) -> WalletResult<SendResult> {
        let mut failures = Vec::new();
        let mut slowest_endpoint_ms = 0;
        while !pending.is_empty() {
            let (outcome, _, remaining) = select_all(pending).await;
            let (index, url, result, elapsed_ms) = outcome;
            slowest_endpoint_ms = slowest_endpoint_ms.max(elapsed_ms);
            match result {
                Ok((hash, reused)) => {
                    log_remaining(remaining, hash);
                    return Ok(success_result(
                        index,
                        &url,
                        hash,
                        reused,
                        elapsed_ms,
                        slowest_endpoint_ms,
                        fanout_started,
                        failures,
                    ));
                }
                Err(error) => {
                    failures.push(EndpointVerdict::new(&url, &error, elapsed_ms));
                    pending = remaining;
                }
            }
        }
        let failure = BroadcastFailure {
            endpoints: failures,
        };
        tracing::warn!(error = %failure, "all broadcast endpoints rejected");
        Err(WalletError::BroadcastFailed(failure))
    }

    /// Warm up HTTP connections to all endpoints
    ///
    /// Establishes TCP/TLS connections by sending lightweight requests.
    /// Returns the number of successfully warmed connections.
    pub async fn warmup(&self) -> usize {
        let futures: Vec<_> = self.clients.iter().map(|c| c.warmup()).collect();
        let results = join_all(futures).await;
        results.into_iter().filter(|r| r.is_ok()).count()
    }

    /// Get the number of endpoints
    pub fn endpoint_count(&self) -> usize {
        self.clients.len()
    }
}

/// Endpoint identity for deduplication: the URL exactly as a request parses it
/// (scheme and host case-folded, default port and empty path dropped), so only
/// two spellings of one request target compare equal. Nothing is trimmed first:
/// a trailing NBSP is part of the path, another endpoint. Text that does not
/// parse compares verbatim.
pub(crate) fn normalized_url(url: &str) -> String {
    reqwest::Url::parse(url).map_or_else(|_| url.to_string(), String::from)
}

fn log_remaining(remaining: Vec<EndpointFuture>, hash: B256) {
    if remaining.is_empty() {
        return;
    }
    tokio::spawn(async move {
        for (_, url, result, elapsed_ms) in join_all(remaining).await {
            let endpoint = endpoint_host(&url);
            match result {
                Ok(_) => tracing::debug!(
                    tx_hash = %hash, %endpoint, endpoint_ms = elapsed_ms,
                    "late broadcast endpoint accepted"
                ),
                Err(error) => tracing::warn!(
                    tx_hash = %hash, %endpoint, endpoint_ms = elapsed_ms,
                    error = %redact_urls(&error.to_string()),
                    "late broadcast endpoint rejected"
                ),
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn success_result(
    index: usize,
    url: &str,
    hash: B256,
    reused: bool,
    elapsed_ms: u64,
    slowest_endpoint_ms: u64,
    started: Instant,
    failures: Vec<EndpointVerdict>,
) -> SendResult {
    let failed_endpoints = failures.len();
    let mut per_endpoint_ms: Vec<_> = failures
        .into_iter()
        .map(|v| (v.host, Err(v.error)))
        .collect();
    per_endpoint_ms.push((endpoint_host(url).to_string(), Ok(elapsed_ms)));
    SendResult {
        tx_hash: format!("{hash:?}"),
        sign_ms: 0.0,
        semaphore_wait_ms: 0,
        broadcast_fanout_ms: started.elapsed().as_millis() as u64,
        fastest_endpoint_ms: elapsed_ms,
        fastest_endpoint_host: endpoint_host(url).to_string(),
        slowest_endpoint_ms,
        first_success_endpoint_index: index,
        failed_endpoints,
        connection_reused: reused,
        per_endpoint_ms,
    }
}

//! Multi-RPC transaction broadcasting for fastest inclusion
//!
//! This module provides advanced transaction broadcasting strategies:
//! - Parallel broadcasting to multiple RPC endpoints
//! - Support for private transaction relays (Flashbots, bloXroute, etc.)
//! - Race-based broadcasting (first success wins)
//! - Configurable retry strategies

use crate::error::{WalletError, WalletResult};
use crate::transaction::Transaction;
use alloy_primitives::B256;
use futures::future::{select_all, join_all};
use futures::FutureExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::timeout;

/// RPC endpoint configuration
#[derive(Debug, Clone)]
pub struct RpcEndpoint {
    /// Endpoint URL
    pub url: String,
    /// Optional authentication header (for private relays)
    pub auth_header: Option<(String, String)>,
    /// Whether this is a private relay (won't be visible in public mempool)
    pub is_private: bool,
    /// Priority (lower = higher priority)
    pub priority: u8,
    /// Custom timeout for this endpoint
    pub timeout: Option<Duration>,
}

impl RpcEndpoint {
    /// Create a new public RPC endpoint
    pub fn public(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_header: None,
            is_private: false,
            priority: 100,
            timeout: None,
        }
    }

    /// Create a private relay endpoint
    pub fn private(url: impl Into<String>, auth_key: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_header: Some(("X-Flashbots-Signature".to_string(), auth_key.into())),
            is_private: true,
            priority: 50, // Higher priority for private relays
            timeout: None,
        }
    }

    /// Create a Flashbots relay endpoint
    pub fn flashbots(auth_key: impl Into<String>) -> Self {
        Self {
            url: "https://relay.flashbots.net".to_string(),
            auth_header: Some(("X-Flashbots-Signature".to_string(), auth_key.into())),
            is_private: true,
            priority: 10, // Highest priority
            timeout: Some(Duration::from_secs(5)),
        }
    }

    /// Create a Blink Labs RPC endpoint
    ///
    /// Blink Labs provides low-latency RPC optimized for MEV and liquidation bots.
    /// Get an API key at: https://blinklabs.xyz
    ///
    /// # Example
    /// ```ignore
    /// let endpoint = RpcEndpoint::blink("your_api_key");
    /// ```
    pub fn blink(api_key: impl Into<String>) -> Self {
        Self {
            url: format!("https://eth.blinklabs.xyz/v1/{}", api_key.into()),
            auth_header: None,
            is_private: false,
            priority: 20, // High priority (lower than Flashbots, higher than public)
            timeout: Some(Duration::from_secs(3)), // Fast timeout for low-latency
        }
    }

    /// Create a Blink Labs RPC endpoint with custom chain
    ///
    /// Supported chains: eth, arb, base, opt, polygon
    pub fn blink_chain(chain: &str, api_key: impl Into<String>) -> Self {
        Self {
            url: format!("https://{}.blinklabs.xyz/v1/{}", chain, api_key.into()),
            auth_header: None,
            is_private: false,
            priority: 20,
            timeout: Some(Duration::from_secs(3)),
        }
    }

    // ========================================================================
    // Built-in Public RPC Endpoints
    // ========================================================================

    /// LlamaNodes RPC - High reliability public endpoint
    pub fn llama() -> Self {
        Self::public("https://eth.llamarpc.com").with_priority(30)
    }

    /// PublicNode RPC - Fast and privacy-first
    pub fn publicnode() -> Self {
        Self::public("https://ethereum-rpc.publicnode.com").with_priority(30)
    }

    /// 1RPC - Privacy-preserving RPC relay
    pub fn one_rpc() -> Self {
        Self::public("https://1rpc.io/eth").with_priority(35)
    }

    /// Cloudflare Ethereum Gateway
    pub fn cloudflare() -> Self {
        Self::public("https://cloudflare-eth.com").with_priority(40)
    }

    /// Ankr Public RPC
    pub fn ankr() -> Self {
        Self::public("https://rpc.ankr.com/eth").with_priority(40)
    }

    /// DRPC Public Endpoint
    pub fn drpc() -> Self {
        Self::public("https://eth.drpc.org").with_priority(35)
    }

    /// Blast API Public RPC
    pub fn blast() -> Self {
        Self::public("https://eth-mainnet.public.blastapi.io").with_priority(40)
    }

    /// BlockPI Public RPC
    pub fn blockpi() -> Self {
        Self::public("https://ethereum.blockpi.network/v1/rpc/public").with_priority(45)
    }

    /// OMNIA Tech Public RPC
    pub fn omnia() -> Self {
        Self::public("https://endpoints.omniatech.io/v1/eth/mainnet/public").with_priority(45)
    }

    /// Tenderly Public Gateway
    pub fn tenderly() -> Self {
        Self::public("https://gateway.tenderly.co/public/mainnet").with_priority(40)
    }

    /// Merkle RPC
    pub fn merkle() -> Self {
        Self::public("https://eth.merkle.io").with_priority(45)
    }

    /// MEV Blocker RPC - Protects against MEV attacks
    pub fn mev_blocker() -> Self {
        Self {
            url: "https://rpc.mevblocker.io".to_string(),
            auth_header: None,
            is_private: true, // MEV protected
            priority: 25,
            timeout: Some(Duration::from_secs(5)),
        }
    }

    /// MEV Blocker Fast Mode
    pub fn mev_blocker_fast() -> Self {
        Self {
            url: "https://rpc.mevblocker.io/fast".to_string(),
            auth_header: None,
            is_private: true,
            priority: 20,
            timeout: Some(Duration::from_secs(3)),
        }
    }

    /// Flashbots Protect RPC (no auth required for basic protection)
    pub fn flashbots_protect() -> Self {
        Self {
            url: "https://rpc.flashbots.net".to_string(),
            auth_header: None,
            is_private: true,
            priority: 15,
            timeout: Some(Duration::from_secs(5)),
        }
    }

    /// Flashbots Fast Mode
    pub fn flashbots_fast() -> Self {
        Self {
            url: "https://rpc.flashbots.net/fast".to_string(),
            auth_header: None,
            is_private: true,
            priority: 12,
            timeout: Some(Duration::from_secs(3)),
        }
    }

    /// bloXroute RPC (Virginia, US East)
    pub fn bloxroute_virginia() -> Self {
        Self::public("https://virginia.rpc.blxrbdn.com").with_priority(30)
    }

    /// bloXroute RPC (UK, Europe)
    pub fn bloxroute_uk() -> Self {
        Self::public("https://uk.rpc.blxrbdn.com").with_priority(30)
    }

    /// bloXroute RPC (Singapore, Asia)
    pub fn bloxroute_singapore() -> Self {
        Self::public("https://singapore.rpc.blxrbdn.com").with_priority(30)
    }

    /// SecureRPC
    pub fn securerpc() -> Self {
        Self::public("https://api.securerpc.com/v1").with_priority(40)
    }

    /// Builder0x69 RPC
    pub fn builder0x69() -> Self {
        Self::public("https://rpc.builder0x69.io").with_priority(35)
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Broadcasting strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BroadcastStrategy {
    /// Send to all endpoints in parallel, return first success
    RaceAll,
    /// Send to all endpoints in parallel, wait for all responses
    BroadcastAll,
    /// Send to endpoints by priority, stop on first success
    PriorityOrdered,
    /// Send only to private relays first, then public if all fail
    PrivateFirst,
}

/// Result of a broadcast operation
#[derive(Debug)]
pub struct BroadcastResult {
    /// Transaction hash (if any endpoint succeeded)
    pub tx_hash: Option<B256>,
    /// Number of successful submissions
    pub success_count: usize,
    /// Number of failed submissions
    pub failure_count: usize,
    /// Errors from failed submissions
    pub errors: Vec<(String, WalletError)>,
    /// Which endpoint succeeded first (if RaceAll strategy)
    pub first_success: Option<String>,
}

/// High-performance multi-RPC broadcaster
pub struct TransactionBroadcaster {
    client: Client,
    /// Endpoints sorted by priority
    pub endpoints: Vec<RpcEndpoint>,
    /// Current strategy
    pub strategy: BroadcastStrategy,
    default_timeout: Duration,
    request_id: AtomicU64,
}

impl TransactionBroadcaster {
    /// Create a new broadcaster with default settings
    pub fn new(endpoints: Vec<RpcEndpoint>) -> WalletResult<Self> {
        let client = Client::builder()
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| WalletError::NetworkError(e.to_string()))?;

        Ok(Self {
            client,
            endpoints,
            strategy: BroadcastStrategy::RaceAll,
            default_timeout: Duration::from_secs(5),
            request_id: AtomicU64::new(1),
        })
    }

    /// Set broadcasting strategy
    pub fn with_strategy(mut self, strategy: BroadcastStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set default timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Add an endpoint
    pub fn add_endpoint(&mut self, endpoint: RpcEndpoint) {
        self.endpoints.push(endpoint);
        // Sort by priority
        self.endpoints.sort_by_key(|e| e.priority);
    }

    /// Warm up HTTP connections to all endpoints
    ///
    /// Establishes TCP/TLS connections ahead of time by sending lightweight
    /// `eth_chainId` requests. This can save 5-20ms per endpoint on subsequent
    /// transaction broadcasts.
    ///
    /// Returns the number of successfully warmed connections.
    pub async fn warmup(&self) -> usize {
        let futures: Vec<_> = self
            .endpoints
            .iter()
            .map(|endpoint| self.warmup_endpoint(endpoint))
            .collect();

        let results = join_all(futures).await;
        results.into_iter().filter(|r| r.is_ok()).count()
    }

    /// Warm up a single endpoint
    async fn warmup_endpoint(&self, endpoint: &RpcEndpoint) -> WalletResult<()> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": self.next_id(),
        });

        let mut request = self.client.post(&endpoint.url).json(&request_body);

        if let Some((key, value)) = &endpoint.auth_header {
            request = request.header(key, value);
        }

        let timeout_duration = endpoint.timeout.unwrap_or(self.default_timeout);

        let response = timeout(timeout_duration, request.send())
            .await
            .map_err(|_| WalletError::Timeout)?
            .map_err(|e| WalletError::NetworkError(e.to_string()))?;

        // Just check that we got a valid response
        let _: RpcResponse = response
            .json()
            .await
            .map_err(|e| WalletError::RpcError(e.to_string()))?;

        Ok(())
    }

    /// Get next request ID
    fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send transaction to a single endpoint
    async fn send_to_endpoint(
        &self,
        endpoint: &RpcEndpoint,
        raw_tx: &str,
    ) -> WalletResult<B256> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [raw_tx],
            "id": self.next_id(),
        });

        let mut request = self.client.post(&endpoint.url).json(&request_body);

        // Add auth header if present
        if let Some((key, value)) = &endpoint.auth_header {
            request = request.header(key, value);
        }

        let timeout_duration = endpoint.timeout.unwrap_or(self.default_timeout);

        let response = timeout(timeout_duration, request.send())
            .await
            .map_err(|_| WalletError::Timeout)?
            .map_err(|e| WalletError::NetworkError(e.to_string()))?;

        let rpc_response: RpcResponse = response
            .json()
            .await
            .map_err(|e| WalletError::RpcError(e.to_string()))?;

        if let Some(error) = rpc_response.error {
            return Err(WalletError::RpcError(format!(
                "{}: {}",
                error.code, error.message
            )));
        }

        rpc_response
            .result
            .ok_or_else(|| WalletError::RpcError("Empty response".to_string()))
            .and_then(|s| parse_b256_hex(&s))
    }

    /// Broadcast transaction using configured strategy
    pub async fn broadcast(&self, tx: &Transaction) -> BroadcastResult {
        let raw_tx = tx.to_hex();

        match self.strategy {
            BroadcastStrategy::RaceAll => self.broadcast_race(&raw_tx).await,
            BroadcastStrategy::BroadcastAll => self.broadcast_all(&raw_tx).await,
            BroadcastStrategy::PriorityOrdered => self.broadcast_priority(&raw_tx).await,
            BroadcastStrategy::PrivateFirst => self.broadcast_private_first(&raw_tx).await,
        }
    }

    /// Broadcast raw transaction hex
    pub async fn broadcast_raw(&self, raw_tx: &str) -> BroadcastResult {
        match self.strategy {
            BroadcastStrategy::RaceAll => self.broadcast_race(raw_tx).await,
            BroadcastStrategy::BroadcastAll => self.broadcast_all(raw_tx).await,
            BroadcastStrategy::PriorityOrdered => self.broadcast_priority(raw_tx).await,
            BroadcastStrategy::PrivateFirst => self.broadcast_private_first(raw_tx).await,
        }
    }

    /// Race all endpoints, return on first success
    async fn broadcast_race(&self, raw_tx: &str) -> BroadcastResult {
        if self.endpoints.is_empty() {
            return BroadcastResult {
                tx_hash: None,
                success_count: 0,
                failure_count: 0,
                errors: vec![("none".to_string(), WalletError::RpcError("No endpoints configured".to_string()))],
                first_success: None,
            };
        }

        let futures: Vec<_> = self
            .endpoints
            .iter()
            .map(|endpoint| {
                let url = endpoint.url.clone();
                self.send_to_endpoint(endpoint, raw_tx)
                    .map(move |result| (url, result))
                    .boxed()
            })
            .collect();

        let mut errors = Vec::new();
        let mut pending = futures;

        while !pending.is_empty() {
            let (result, _index, remaining) = select_all(pending).await;
            pending = remaining;

            match result {
                (url, Ok(hash)) => {
                    return BroadcastResult {
                        tx_hash: Some(hash),
                        success_count: 1,
                        failure_count: errors.len(),
                        errors,
                        first_success: Some(url),
                    };
                }
                (url, Err(e)) => {
                    errors.push((url, e));
                }
            }
        }

        BroadcastResult {
            tx_hash: None,
            success_count: 0,
            failure_count: errors.len(),
            errors,
            first_success: None,
        }
    }

    /// Broadcast to all endpoints, wait for all responses
    async fn broadcast_all(&self, raw_tx: &str) -> BroadcastResult {
        let futures: Vec<_> = self
            .endpoints
            .iter()
            .map(|endpoint| {
                let url = endpoint.url.clone();
                async move { (url, self.send_to_endpoint(endpoint, raw_tx).await) }
            })
            .collect();

        let results = join_all(futures).await;

        let mut tx_hash = None;
        let mut success_count = 0;
        let mut errors = Vec::new();
        let mut first_success = None;

        for (url, result) in results {
            match result {
                Ok(hash) => {
                    if tx_hash.is_none() {
                        tx_hash = Some(hash);
                        first_success = Some(url);
                    }
                    success_count += 1;
                }
                Err(e) => {
                    errors.push((url, e));
                }
            }
        }

        BroadcastResult {
            tx_hash,
            success_count,
            failure_count: errors.len(),
            errors,
            first_success,
        }
    }

    /// Send by priority order, stop on first success
    async fn broadcast_priority(&self, raw_tx: &str) -> BroadcastResult {
        let mut errors = Vec::new();

        for endpoint in &self.endpoints {
            match self.send_to_endpoint(endpoint, raw_tx).await {
                Ok(hash) => {
                    return BroadcastResult {
                        tx_hash: Some(hash),
                        success_count: 1,
                        failure_count: errors.len(),
                        errors,
                        first_success: Some(endpoint.url.clone()),
                    };
                }
                Err(e) => {
                    errors.push((endpoint.url.clone(), e));
                }
            }
        }

        BroadcastResult {
            tx_hash: None,
            success_count: 0,
            failure_count: errors.len(),
            errors,
            first_success: None,
        }
    }

    /// Send to private relays first, then public endpoints
    async fn broadcast_private_first(&self, raw_tx: &str) -> BroadcastResult {
        let (private, public): (Vec<_>, Vec<_>) =
            self.endpoints.iter().partition(|e| e.is_private);

        // Try private relays first (in parallel)
        if !private.is_empty() {
            let futures: Vec<_> = private
                .iter()
                .map(|endpoint| {
                    let url = endpoint.url.clone();
                    self.send_to_endpoint(endpoint, raw_tx)
                        .map(move |result| (url, result))
                        .boxed()
                })
                .collect();

            let mut pending = futures;
            let mut private_errors = Vec::new();

            while !pending.is_empty() {
                let (result, _index, remaining) = select_all(pending).await;
                pending = remaining;

                match result {
                    (url, Ok(hash)) => {
                        // Also send to public endpoints in background (don't wait)
                        let _ = self.broadcast_to_subset(&public, raw_tx);

                        return BroadcastResult {
                            tx_hash: Some(hash),
                            success_count: 1,
                            failure_count: private_errors.len(),
                            errors: private_errors,
                            first_success: Some(url),
                        };
                    }
                    (url, Err(e)) => {
                        private_errors.push((url, e));
                    }
                }
            }
        }

        // Fall back to public endpoints
        self.broadcast_to_subset(&public, raw_tx).await
    }

    /// Broadcast to a subset of endpoints
    async fn broadcast_to_subset(&self, endpoints: &[&RpcEndpoint], raw_tx: &str) -> BroadcastResult {
        if endpoints.is_empty() {
            return BroadcastResult {
                tx_hash: None,
                success_count: 0,
                failure_count: 0,
                errors: vec![],
                first_success: None,
            };
        }

        let futures: Vec<_> = endpoints
            .iter()
            .map(|endpoint| {
                let url = endpoint.url.clone();
                self.send_to_endpoint(endpoint, raw_tx)
                    .map(move |result| (url, result))
                    .boxed()
            })
            .collect();

        let mut pending = futures;
        let mut errors = Vec::new();

        while !pending.is_empty() {
            let (result, _index, remaining) = select_all(pending).await;
            pending = remaining;

            match result {
                (url, Ok(hash)) => {
                    return BroadcastResult {
                        tx_hash: Some(hash),
                        success_count: 1,
                        failure_count: errors.len(),
                        errors,
                        first_success: Some(url),
                    };
                }
                (url, Err(e)) => {
                    errors.push((url, e));
                }
            }
        }

        BroadcastResult {
            tx_hash: None,
            success_count: 0,
            failure_count: errors.len(),
            errors,
            first_success: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: u64,
    result: Option<String>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

fn parse_b256_hex(s: &str) -> WalletResult<B256> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        return Err(WalletError::RpcError(format!(
            "Invalid hash length: {}",
            bytes.len()
        )));
    }
    Ok(B256::from_slice(&bytes))
}

/// Builder for TransactionBroadcaster
pub struct BroadcasterBuilder {
    endpoints: Vec<RpcEndpoint>,
    strategy: BroadcastStrategy,
    timeout: Duration,
}

impl BroadcasterBuilder {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
            strategy: BroadcastStrategy::RaceAll,
            timeout: Duration::from_secs(5),
        }
    }

    /// Add a public RPC endpoint
    pub fn add_public(mut self, url: impl Into<String>) -> Self {
        self.endpoints.push(RpcEndpoint::public(url));
        self
    }

    /// Add multiple public RPC endpoints
    pub fn add_publics(mut self, urls: Vec<String>) -> Self {
        for url in urls {
            self.endpoints.push(RpcEndpoint::public(url));
        }
        self
    }

    /// Add a Flashbots relay
    pub fn add_flashbots(mut self, auth_key: impl Into<String>) -> Self {
        self.endpoints.push(RpcEndpoint::flashbots(auth_key));
        self
    }

    /// Add a Blink Labs RPC endpoint (Ethereum mainnet)
    pub fn add_blink(mut self, api_key: impl Into<String>) -> Self {
        self.endpoints.push(RpcEndpoint::blink(api_key));
        self
    }

    /// Add a Blink Labs RPC endpoint for a specific chain
    pub fn add_blink_chain(mut self, chain: &str, api_key: impl Into<String>) -> Self {
        self.endpoints.push(RpcEndpoint::blink_chain(chain, api_key));
        self
    }

    // ========================================================================
    // Convenience Methods for Common Setups
    // ========================================================================

    /// Add all default public RPC endpoints (recommended for broadcast redundancy)
    ///
    /// Adds: LlamaNodes, PublicNode, 1RPC, DRPC, Ankr, Cloudflare
    pub fn add_default_public_rpcs(mut self) -> Self {
        self.endpoints.push(RpcEndpoint::llama());
        self.endpoints.push(RpcEndpoint::publicnode());
        self.endpoints.push(RpcEndpoint::one_rpc());
        self.endpoints.push(RpcEndpoint::drpc());
        self.endpoints.push(RpcEndpoint::ankr());
        self.endpoints.push(RpcEndpoint::cloudflare());
        self
    }

    /// Add all available public RPC endpoints for maximum broadcast coverage
    ///
    /// Includes all default RPCs plus: Blast, BlockPI, OMNIA, Tenderly, Merkle, SecureRPC
    pub fn add_all_public_rpcs(mut self) -> Self {
        // Core reliable endpoints
        self.endpoints.push(RpcEndpoint::llama());
        self.endpoints.push(RpcEndpoint::publicnode());
        self.endpoints.push(RpcEndpoint::one_rpc());
        self.endpoints.push(RpcEndpoint::drpc());
        self.endpoints.push(RpcEndpoint::ankr());
        self.endpoints.push(RpcEndpoint::cloudflare());
        // Additional endpoints
        self.endpoints.push(RpcEndpoint::blast());
        self.endpoints.push(RpcEndpoint::blockpi());
        self.endpoints.push(RpcEndpoint::omnia());
        self.endpoints.push(RpcEndpoint::tenderly());
        self.endpoints.push(RpcEndpoint::merkle());
        self.endpoints.push(RpcEndpoint::securerpc());
        self.endpoints.push(RpcEndpoint::builder0x69());
        self
    }

    /// Add MEV protection endpoints (protects against sandwich attacks and frontrunning)
    ///
    /// Adds: Flashbots Protect, Flashbots Fast, MEV Blocker, MEV Blocker Fast
    pub fn add_mev_protection(mut self) -> Self {
        self.endpoints.push(RpcEndpoint::flashbots_protect());
        self.endpoints.push(RpcEndpoint::flashbots_fast());
        self.endpoints.push(RpcEndpoint::mev_blocker());
        self.endpoints.push(RpcEndpoint::mev_blocker_fast());
        self
    }

    /// Add bloXroute regional endpoints for low-latency broadcasting
    ///
    /// Adds: Virginia (US East), UK (Europe), Singapore (Asia)
    pub fn add_regional_rpcs(mut self) -> Self {
        self.endpoints.push(RpcEndpoint::bloxroute_virginia());
        self.endpoints.push(RpcEndpoint::bloxroute_uk());
        self.endpoints.push(RpcEndpoint::bloxroute_singapore());
        self
    }

    /// Add a recommended set of endpoints for liquidation bots
    ///
    /// Includes: MEV protection + regional low-latency + default public RPCs
    pub fn add_liquidation_preset(self) -> Self {
        self.add_mev_protection()
            .add_regional_rpcs()
            .add_default_public_rpcs()
    }

    /// Add a minimal set of reliable endpoints
    ///
    /// Adds: LlamaNodes, PublicNode, 1RPC
    pub fn add_minimal_rpcs(mut self) -> Self {
        self.endpoints.push(RpcEndpoint::llama());
        self.endpoints.push(RpcEndpoint::publicnode());
        self.endpoints.push(RpcEndpoint::one_rpc());
        self
    }

    /// Add a custom endpoint
    pub fn add_endpoint(mut self, endpoint: RpcEndpoint) -> Self {
        self.endpoints.push(endpoint);
        self
    }

    /// Set broadcasting strategy
    pub fn strategy(mut self, strategy: BroadcastStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set default timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Build the broadcaster
    pub fn build(mut self) -> WalletResult<TransactionBroadcaster> {
        // Sort endpoints by priority
        self.endpoints.sort_by_key(|e| e.priority);

        TransactionBroadcaster::new(self.endpoints)
            .map(|b| b.with_strategy(self.strategy).with_timeout(self.timeout))
    }
}

impl Default for BroadcasterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_endpoint_creation() {
        let public = RpcEndpoint::public("https://eth.example.com");
        assert!(!public.is_private);
        assert_eq!(public.priority, 100);

        let flashbots = RpcEndpoint::flashbots("test_key");
        assert!(flashbots.is_private);
        assert_eq!(flashbots.priority, 10);

        let blink = RpcEndpoint::blink("test_api_key");
        assert!(!blink.is_private);
        assert_eq!(blink.priority, 20);
        assert!(blink.url.contains("blinklabs.xyz"));
    }

    #[test]
    fn test_builder() {
        let broadcaster = BroadcasterBuilder::new()
            .add_public("https://rpc1.example.com")
            .add_public("https://rpc2.example.com")
            .strategy(BroadcastStrategy::BroadcastAll)
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        assert_eq!(broadcaster.endpoints.len(), 2);
        assert_eq!(broadcaster.strategy, BroadcastStrategy::BroadcastAll);
    }

    #[test]
    fn test_endpoint_priority_sorting() {
        let broadcaster = BroadcasterBuilder::new()
            .add_endpoint(RpcEndpoint::public("low_priority").with_priority(200))
            .add_endpoint(RpcEndpoint::public("medium_priority").with_priority(100))
            .add_endpoint(RpcEndpoint::public("high_priority").with_priority(10))
            .build()
            .unwrap();

        assert_eq!(broadcaster.endpoints[0].priority, 10);
        assert_eq!(broadcaster.endpoints[1].priority, 100);
        assert_eq!(broadcaster.endpoints[2].priority, 200);
    }

    #[test]
    fn test_built_in_public_endpoints() {
        // Test that all built-in endpoints can be created
        let llama = RpcEndpoint::llama();
        assert!(llama.url.contains("llamarpc"));
        assert!(!llama.is_private);

        let publicnode = RpcEndpoint::publicnode();
        assert!(publicnode.url.contains("publicnode"));

        let mev_blocker = RpcEndpoint::mev_blocker();
        assert!(mev_blocker.is_private);
        assert!(mev_blocker.url.contains("mevblocker"));

        let flashbots = RpcEndpoint::flashbots_protect();
        assert!(flashbots.is_private);
        assert!(flashbots.url.contains("flashbots"));
    }

    #[test]
    fn test_convenience_methods() {
        // Test add_default_public_rpcs
        let broadcaster = BroadcasterBuilder::new()
            .add_default_public_rpcs()
            .build()
            .unwrap();
        assert_eq!(broadcaster.endpoints.len(), 6);

        // Test add_all_public_rpcs
        let broadcaster = BroadcasterBuilder::new()
            .add_all_public_rpcs()
            .build()
            .unwrap();
        assert_eq!(broadcaster.endpoints.len(), 13);

        // Test add_mev_protection
        let broadcaster = BroadcasterBuilder::new()
            .add_mev_protection()
            .build()
            .unwrap();
        assert_eq!(broadcaster.endpoints.len(), 4);
        assert!(broadcaster.endpoints.iter().all(|e| e.is_private));

        // Test add_regional_rpcs
        let broadcaster = BroadcasterBuilder::new()
            .add_regional_rpcs()
            .build()
            .unwrap();
        assert_eq!(broadcaster.endpoints.len(), 3);

        // Test add_liquidation_preset
        let broadcaster = BroadcasterBuilder::new()
            .add_liquidation_preset()
            .build()
            .unwrap();
        // MEV (4) + Regional (3) + Default (6) = 13
        assert_eq!(broadcaster.endpoints.len(), 13);

        // Test add_minimal_rpcs
        let broadcaster = BroadcasterBuilder::new()
            .add_minimal_rpcs()
            .build()
            .unwrap();
        assert_eq!(broadcaster.endpoints.len(), 3);
    }
}

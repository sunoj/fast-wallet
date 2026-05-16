//! High-performance JSON-RPC client for EVM chains
//!
//! This module provides an optimized RPC client with:
//! - Connection pooling
//! - Parallel request support
//! - Minimal serialization overhead

use crate::error::{WalletError, WalletResult};
use alloy::primitives::{Address, B256, U256};
use futures_util::future::{join_all, select_all};
use futures_util::FutureExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// JSON-RPC request
#[derive(Debug, Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    params: Value,
    id: u64,
}

/// JSON-RPC response
#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: u64,
    result: Option<T>,
    error: Option<RpcError>,
}

/// JSON-RPC error
#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

/// Extracted for unit-testing the data-preservation behavior.
fn format_rpc_error(error: &RpcError) -> String {
    let mut msg = format!("RPC error {}: {}", error.code, error.message);
    if let Some(data) = &error.data {
        let data_str = data
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| data.to_string());
        msg.push_str(&format!(" data={data_str}"));
    }
    msg
}

/// High-performance RPC client
pub struct RpcClient {
    client: Client,
    url: String,
    request_id: AtomicU64,
}

impl RpcClient {
    /// Create a new RPC client with optimized settings
    pub fn new(url: impl Into<String>) -> WalletResult<Self> {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(300))
            .timeout(Duration::from_secs(30))
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(15))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .http2_keep_alive_while_idle(true)
            .build()
            .map_err(|e| WalletError::NetworkError(e.to_string()))?;

        Ok(Self {
            client,
            url: url.into(),
            request_id: AtomicU64::new(1),
        })
    }

    /// Create with custom client configuration
    pub fn with_client(client: Client, url: impl Into<String>) -> Self {
        Self {
            client,
            url: url.into(),
            request_id: AtomicU64::new(1),
        }
    }

    /// RPC endpoint URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Warm up the HTTP connection pool by sending a lightweight request
    ///
    /// This establishes TCP/TLS connections ahead of time, saving 5-20ms
    /// on subsequent requests. Use this before time-critical operations.
    pub async fn warmup(&self) -> WalletResult<()> {
        // eth_chainId is the lightest RPC call - just returns the chain ID
        let _: String = self.request("eth_chainId", json!([])).await?;
        Ok(())
    }

    /// Get next request ID (atomic, lock-free)
    #[inline]
    fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Execute a raw JSON-RPC request
    async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Value,
    ) -> WalletResult<T> {
        let req = RpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id: self.next_id(),
        };

        let response = self
            .client
            .post(&self.url)
            .json(&req)
            .send()
            .await
            .map_err(|e| WalletError::NetworkError(e.to_string()))?;

        let rpc_response: RpcResponse<T> = response
            .json()
            .await
            .map_err(|e| WalletError::RpcError(e.to_string()))?;

        if let Some(error) = rpc_response.error {
            return Err(WalletError::RpcError(format_rpc_error(&error)));
        }

        rpc_response
            .result
            .ok_or_else(|| WalletError::RpcError("Empty response".to_string()))
    }

    /// Get the current nonce for an address
    pub async fn get_nonce(&self, address: Address) -> WalletResult<u64> {
        let result: String = self
            .request(
                "eth_getTransactionCount",
                json!([format!("{:?}", address), "pending"]),
            )
            .await?;

        parse_u64_hex(&result)
    }

    /// Get the current nonce at latest block (confirmed transactions only)
    pub async fn get_nonce_latest(&self, address: Address) -> WalletResult<u64> {
        let result: String = self
            .request(
                "eth_getTransactionCount",
                json!([format!("{:?}", address), "latest"]),
            )
            .await?;

        parse_u64_hex(&result)
    }

    /// Get the balance of an address
    pub async fn get_balance(&self, address: Address) -> WalletResult<U256> {
        let result: String = self
            .request(
                "eth_getBalance",
                json!([format!("{:?}", address), "latest"]),
            )
            .await?;

        parse_u256_hex(&result)
    }

    /// Get the current gas price
    pub async fn gas_price(&self) -> WalletResult<U256> {
        let result: String = self.request("eth_gasPrice", json!([])).await?;
        parse_u256_hex(&result)
    }

    /// Get EIP-1559 fee data
    pub async fn max_priority_fee(&self) -> WalletResult<U256> {
        let result: String = self.request("eth_maxPriorityFeePerGas", json!([])).await?;
        parse_u256_hex(&result)
    }

    /// Get the current block number
    pub async fn block_number(&self) -> WalletResult<u64> {
        let result: String = self.request("eth_blockNumber", json!([])).await?;
        parse_u64_hex(&result)
    }

    /// Get the chain ID
    pub async fn chain_id(&self) -> WalletResult<u64> {
        let result: String = self.request("eth_chainId", json!([])).await?;
        parse_u64_hex(&result)
    }

    /// Send a raw transaction
    ///
    /// Returns the transaction hash.
    /// Some RPC nodes return `null` result (no error) for accepted transactions.
    /// In that case we compute the tx hash from the raw transaction bytes.
    pub async fn send_raw_transaction(&self, raw_tx: &str) -> WalletResult<B256> {
        match self
            .request::<String>("eth_sendRawTransaction", json!([raw_tx]))
            .await
        {
            Ok(result) => parse_b256_hex(&result),
            Err(WalletError::RpcError(msg)) if msg == "Empty response" => {
                // RPC accepted the TX but returned null result — compute hash locally
                let hex_str = raw_tx.strip_prefix("0x").unwrap_or(raw_tx);
                let tx_bytes = hex::decode(hex_str)
                    .map_err(|e| WalletError::RpcError(format!("Failed to decode raw tx: {e}")))?;
                let hash = alloy::primitives::keccak256(&tx_bytes);
                tracing::warn!(
                    tx_hash = %hash,
                    "RPC returned null result for eth_sendRawTransaction — TX likely accepted"
                );
                Ok(hash)
            }
            Err(e) => Err(e),
        }
    }

    /// Send a raw transaction and return endpoint timing.
    pub async fn send_raw_transaction_detailed(&self, raw_tx: &str) -> WalletResult<SendResult> {
        let started = Instant::now();
        let tx_hash = self.send_raw_transaction(raw_tx).await?;
        let endpoint_ms = started.elapsed().as_millis() as u64;
        Ok(SendResult {
            tx_hash: format!("{tx_hash:?}"),
            sign_ms: 0.0,
            fastest_endpoint_ms: endpoint_ms,
            fastest_endpoint_url: self.url.clone(),
            slowest_endpoint_ms: endpoint_ms,
            failed_endpoints: 0,
            per_endpoint_ms: vec![(self.url.clone(), Ok(endpoint_ms))],
        })
    }

    /// Send raw transaction bytes (convenience method)
    pub async fn send_raw_transaction_bytes(&self, tx_bytes: &[u8]) -> WalletResult<B256> {
        let hex_tx = format!("0x{}", hex::encode(tx_bytes));
        self.send_raw_transaction(&hex_tx).await
    }

    /// Check if a TX exists in the mempool or on-chain via eth_getTransactionByHash.
    /// Returns true if found (mempool or mined), false if not.
    pub async fn tx_exists(&self, tx_hash: B256) -> WalletResult<bool> {
        let result: Option<Value> = self
            .request(
                "eth_getTransactionByHash",
                json!([format!("{:?}", tx_hash)]),
            )
            .await?;
        Ok(result.is_some())
    }

    /// Get transaction receipt
    pub async fn get_transaction_receipt(&self, tx_hash: B256) -> WalletResult<Option<Value>> {
        // Note: request::<Option<Value>>() treats JSON `null` result as "Empty
        // response" error because serde deserializes `null` as `None` for the
        // outer `Option` in `RpcResponse.result: Option<Option<Value>>`.
        // For eth_getTransactionReceipt, `null` is the normal response when the
        // TX is pending (not yet mined), so we must map it back to Ok(None).
        match self
            .request(
                "eth_getTransactionReceipt",
                json!([format!("{:?}", tx_hash)]),
            )
            .await
        {
            Ok(receipt) => Ok(Some(receipt)),
            Err(WalletError::RpcError(ref msg)) if msg == "Empty response" => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Wait for transaction receipt with timeout
    pub async fn wait_for_receipt(
        &self,
        tx_hash: B256,
        timeout: Duration,
        poll_interval: Duration,
    ) -> WalletResult<Value> {
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(WalletError::Timeout);
            }

            if let Some(receipt) = self.get_transaction_receipt(tx_hash).await? {
                return Ok(receipt);
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Estimate gas for a transaction
    pub async fn estimate_gas(
        &self,
        from: Address,
        to: Option<Address>,
        value: Option<U256>,
        data: Option<&[u8]>,
    ) -> WalletResult<u64> {
        let mut params = serde_json::Map::new();
        params.insert("from".to_string(), json!(format!("{:?}", from)));

        if let Some(to) = to {
            params.insert("to".to_string(), json!(format!("{:?}", to)));
        }

        if let Some(value) = value {
            params.insert("value".to_string(), json!(format!("{:#x}", value)));
        }

        if let Some(data) = data {
            params.insert(
                "data".to_string(),
                json!(format!("0x{}", hex::encode(data))),
            );
        }

        let result: String = self.request("eth_estimateGas", json!([params])).await?;
        parse_u64_hex(&result)
    }

    /// Call a contract (read-only)
    pub async fn call(
        &self,
        to: Address,
        data: &[u8],
        block: Option<&str>,
    ) -> WalletResult<Vec<u8>> {
        let params = json!({
            "to": format!("{:?}", to),
            "data": format!("0x{}", hex::encode(data)),
        });

        let block = block.unwrap_or("latest");
        let result: String = self.request("eth_call", json!([params, block])).await?;

        let hex_str = result.strip_prefix("0x").unwrap_or(&result);
        hex::decode(hex_str).map_err(|e| WalletError::RpcError(e.to_string()))
    }

    /// Call a contract with state overrides (eth_call 3rd param).
    /// `state_overrides` is a JSON object mapping addresses to their overrides.
    pub async fn call_with_overrides(
        &self,
        from: Address,
        to: Address,
        data: &[u8],
        state_overrides: Value,
    ) -> WalletResult<Vec<u8>> {
        let params = json!({
            "from": format!("{:?}", from),
            "to": format!("{:?}", to),
            "data": format!("0x{}", hex::encode(data)),
        });

        let result: String = self
            .request("eth_call", json!([params, "latest", state_overrides]))
            .await?;

        let hex_str = result.strip_prefix("0x").unwrap_or(&result);
        hex::decode(hex_str).map_err(|e| WalletError::RpcError(e.to_string()))
    }
}

/// Parse hex string to u64
fn parse_u64_hex(s: &str) -> WalletResult<u64> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(s, 16).map_err(|e| WalletError::RpcError(e.to_string()))
}

/// Parse hex string to U256
fn parse_u256_hex(s: &str) -> WalletResult<U256> {
    U256::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16)
        .map_err(|e| WalletError::RpcError(e.to_string()))
}

/// Parse hex string to B256
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

/// Batch RPC client for sending multiple requests in parallel
pub struct BatchRpcClient {
    clients: Vec<Arc<RpcClient>>,
    current: AtomicU64,
}

/// Result of a transaction broadcast with signing and endpoint timing.
#[derive(Debug, Clone)]
pub struct SendResult {
    pub tx_hash: String,
    pub sign_ms: f64,
    pub fastest_endpoint_ms: u64,
    pub fastest_endpoint_url: String,
    pub slowest_endpoint_ms: u64,
    pub failed_endpoints: usize,
    pub per_endpoint_ms: Vec<(String, Result<u64, String>)>,
}

impl BatchRpcClient {
    /// Create a new batch client with multiple RPC endpoints
    pub fn new(urls: Vec<String>) -> WalletResult<Self> {
        let clients: WalletResult<Vec<_>> = urls
            .into_iter()
            .map(|url| RpcClient::new(url).map(Arc::new))
            .collect();

        Ok(Self {
            clients: clients?,
            current: AtomicU64::new(0),
        })
    }

    /// Get the next client (round-robin)
    pub fn next_client(&self) -> Arc<RpcClient> {
        let idx = self.current.fetch_add(1, Ordering::Relaxed) as usize;
        self.clients[idx % self.clients.len()].clone()
    }

    /// Send transaction to all endpoints in parallel, return on first success
    pub async fn broadcast_transaction(&self, raw_tx: &str) -> WalletResult<B256> {
        if self.clients.is_empty() {
            return Err(WalletError::RpcError("No RPC endpoints".to_string()));
        }

        let mut pending: Vec<_> = self
            .clients
            .iter()
            .map(|client| {
                let client = client.clone();
                let tx = raw_tx.to_string();
                async move { client.send_raw_transaction(&tx).await }.boxed()
            })
            .collect();

        let mut last_err = None;
        while !pending.is_empty() {
            let (result, _index, remaining) = select_all(pending).await;
            match result {
                Ok(hash) => {
                    // First success — spawn remaining sends in background (fire-and-forget)
                    if !remaining.is_empty() {
                        tokio::spawn(async move {
                            join_all(remaining).await;
                        });
                    }
                    return Ok(hash);
                }
                Err(e) => {
                    last_err = Some(e);
                    pending = remaining;
                }
            }
        }

        Err(last_err.unwrap_or_else(|| WalletError::RpcError("No RPC endpoints".to_string())))
    }

    /// Send transaction to all endpoints in parallel with first-success timing.
    pub async fn broadcast_transaction_detailed(&self, raw_tx: &str) -> WalletResult<SendResult> {
        if self.clients.is_empty() {
            return Err(WalletError::RpcError("No RPC endpoints".to_string()));
        }

        let mut pending: Vec<_> = self
            .clients
            .iter()
            .map(|client| {
                let client = client.clone();
                let url = client.url().to_string();
                let tx = raw_tx.to_string();
                async move {
                    let started = Instant::now();
                    let result = client.send_raw_transaction(&tx).await;
                    (url, result, started.elapsed().as_millis() as u64)
                }
                .boxed()
            })
            .collect();

        let mut last_err = None;
        let mut per_endpoint_ms = Vec::new();
        let mut failed_endpoints = 0usize;
        let mut slowest_endpoint_ms = 0u64;

        while !pending.is_empty() {
            let ((url, result, endpoint_ms), _index, remaining) = select_all(pending).await;
            slowest_endpoint_ms = slowest_endpoint_ms.max(endpoint_ms);
            match result {
                Ok(hash) => {
                    per_endpoint_ms.push((url.clone(), Ok(endpoint_ms)));
                    if !remaining.is_empty() {
                        tokio::spawn(async move {
                            join_all(remaining).await;
                        });
                    }
                    return Ok(SendResult {
                        tx_hash: format!("{hash:?}"),
                        sign_ms: 0.0,
                        fastest_endpoint_ms: endpoint_ms,
                        fastest_endpoint_url: url,
                        slowest_endpoint_ms,
                        failed_endpoints,
                        per_endpoint_ms,
                    });
                }
                Err(e) => {
                    failed_endpoints += 1;
                    per_endpoint_ms.push((url, Err(e.to_string())));
                    last_err = Some(e);
                    pending = remaining;
                }
            }
        }

        Err(last_err.unwrap_or_else(|| WalletError::RpcError("No RPC endpoints".to_string())))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u64_hex() {
        assert_eq!(parse_u64_hex("0x0").unwrap(), 0);
        assert_eq!(parse_u64_hex("0x1").unwrap(), 1);
        assert_eq!(parse_u64_hex("0xff").unwrap(), 255);
        assert_eq!(parse_u64_hex("0x5208").unwrap(), 21000);
        assert_eq!(parse_u64_hex("5208").unwrap(), 21000);
    }

    #[test]
    fn test_parse_u256_hex() {
        let val = parse_u256_hex("0xde0b6b3a7640000").unwrap(); // 1 ETH in wei
        assert_eq!(val, U256::from(1_000_000_000_000_000_000u64));
    }

    #[test]
    fn test_parse_b256_hex() {
        let hash_str = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let hash = parse_b256_hex(hash_str).unwrap();
        assert_eq!(hash[31], 1);
    }

    #[test]
    fn test_rpc_client_creation() {
        let client = RpcClient::new("http://localhost:8545").unwrap();
        assert_eq!(client.url, "http://localhost:8545");
    }

    fn rpc_error_from_value(v: serde_json::Value) -> RpcError {
        let resp: RpcResponse<String> = serde_json::from_value(v).unwrap();
        resp.error.unwrap()
    }

    #[test]
    fn test_rpc_error_preserves_data_selector() {
        let error = rpc_error_from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": 3,
                "message": "execution reverted",
                "data": "0xddeb79ba"
            }
        }));
        let msg = format_rpc_error(&error);
        assert!(msg.contains("data=0xddeb79ba"), "got: {msg}");
    }

    #[test]
    fn test_rpc_error_omits_data_tail_when_missing() {
        let error = rpc_error_from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": 3, "message": "execution reverted" }
        }));
        let msg = format_rpc_error(&error);
        assert_eq!(msg, "RPC error 3: execution reverted");
        assert!(!msg.contains("data="), "got: {msg}");
    }

    #[test]
    fn test_rpc_error_omits_data_tail_when_null() {
        let error = rpc_error_from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": 3, "message": "execution reverted", "data": null }
        }));
        let msg = format_rpc_error(&error);
        assert_eq!(msg, "RPC error 3: execution reverted");
    }

    #[test]
    fn test_rpc_error_preserves_object_data() {
        let error = rpc_error_from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": -32000,
                "message": "execution reverted",
                "data": { "reason": "NotLiquidatable", "selector": "0xddeb79ba" }
            }
        }));
        let msg = format_rpc_error(&error);
        assert!(msg.contains("data="), "got: {msg}");
        assert!(msg.contains("\"selector\":\"0xddeb79ba\""), "got: {msg}");
    }
}

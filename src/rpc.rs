//! High-performance JSON-RPC client for EVM chains
//!
//! This module provides an optimized RPC client with:
//! - Connection pooling
//! - Parallel request support
//! - Minimal serialization overhead

use crate::error::{WalletError, WalletResult};
use crate::warm::WarmState;
use alloy::primitives::{Address, B256, U256};
use futures_util::future::{join_all, select_all};
use futures_util::FutureExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Consecutive receipt-fetch errors `wait_for_receipt` tolerates before it
/// returns the last one. A garbled body, reset or 5xx is usually transient,
/// but a dead endpoint must still fail well before the confirmation timeout.
pub const MAX_CONSECUTIVE_RECEIPT_ERRORS: u32 = 3;

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
    redact_urls(&msg)
}

/// High-performance RPC client
pub struct RpcClient {
    client: Client,
    url: String,
    request_id: AtomicU64,
    request_seen: AtomicBool,
    warm: WarmState,
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
pub mod observation;

pub struct BatchRpcClient {
    observer: std::sync::OnceLock<observation::EndpointObserver>,
    send_timeout: Duration,
    clients: Vec<Arc<RpcClient>>,
    current: AtomicU64,
}

/// Result of a transaction broadcast with signing and endpoint timing.
#[derive(Debug, Clone)]
pub struct SendResult {
    pub tx_hash: String,
    pub sign_ms: f64,
    pub semaphore_wait_ms: u64,
    pub broadcast_fanout_ms: u64,
    pub fastest_endpoint_ms: u64,
    /// Host of the endpoint that answered first. Host, never the URL:
    /// consumers log this field, and RPC URLs carry API keys.
    pub fastest_endpoint_host: String,
    pub slowest_endpoint_ms: u64,
    pub first_success_endpoint_index: usize,
    pub failed_endpoints: usize,
    pub connection_reused: bool,
    /// Per-endpoint verdict as `(host, ms | redacted error)`.
    pub per_endpoint_ms: Vec<(String, Result<u64, String>)>,
}

/// Host portion of an endpoint URL, for logs.
///
/// RPC URLs carry API keys in the path or the query string, so the host is the
/// only part that is ever safe to emit.
pub fn endpoint_host(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme.split(['/', '?', '#']).next().unwrap_or("");
    // `user:pass@host` — userinfo is a credential, keep only what follows it.
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host)
}

/// Replace HTTP(S) and WS(S) URLs, case-insensitively, with just their host.
///
/// `reqwest::Error::to_string()` embeds the request URL — API key and all
/// (`error sending request for url (https://host/v2/<key>)`), so error text
/// from a broadcast attempt can never be logged verbatim.
pub fn redact_urls(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let lowercase = message.to_ascii_lowercase();
    let mut rest = message;
    loop {
        let searchable = &lowercase[message.len() - rest.len()..];
        let Some(start) = ["http://", "https://", "ws://", "wss://"]
            .iter()
            .filter_map(|scheme| searchable.find(scheme))
            .min()
        else {
            break;
        };
        out.push_str(&rest[..start]);
        let tail = &rest[start..];
        // Consume to the next whitespace, never to a punctuation delimiter: a
        // key may legitimately contain `,`, `)` or a quote, and stopping early
        // would leave the remainder of it in the message. Losing a trailing
        // `)` from `url (https://...)` is the right trade.
        let end = tail.find(char::is_whitespace).unwrap_or(tail.len());
        out.push_str(endpoint_host(&tail[..end]));
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

mod batch;
pub(crate) use batch::normalized_url;
pub use batch::DEFAULT_BROADCAST_SEND_TIMEOUT;

mod client;
mod detail;
mod reads;
mod request;
mod send;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod warm_tests;

// First-success race whose losing sends run to their end, and the owned send behind it.
// Exports EndpointSend, post and first_success; late verdicts log host and redacted error.
use super::{format_rpc_error, parse_b256_hex, BroadcastResult, RpcEndpoint, RpcResponse};
use crate::error::{WalletError, WalletResult};
use crate::warm::WarmState;
use alloy::primitives::B256;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, warn};

type Verdicts = mpsc::UnboundedReceiver<(String, WalletResult<B256>, u64)>;

/// POST a JSON-RPC `body` to `endpoint` and read its answer. Unless the whole answer
/// arrives, `endpoint` loses its warm state: an error, the timeout or a body cut short
/// may each leave no usable pooled connection.
pub(super) async fn post(
    client: &Client,
    endpoint: &RpcEndpoint,
    warm: &WarmState,
    limit: Duration,
    body: &Value,
) -> WalletResult<RpcResponse> {
    let guard = warm.loss_guard();
    let mut request = client.post(&endpoint.url).json(body);
    if let Some((key, value)) = &endpoint.auth_header {
        request = request.header(key, value);
    }
    let response = tokio::time::timeout(limit, request.send())
        .await
        .map_err(|_| WalletError::Timeout)?
        .map_err(|e| WalletError::NetworkError(crate::redact_urls(&e.to_string())))?;
    let answer = response
        .json()
        .await
        .map_err(|e| WalletError::RpcError(crate::redact_urls(&e.to_string())))?;
    guard.disarm();
    Ok(answer)
}

/// One endpoint's `eth_sendRawTransaction`, owning its inputs so it can outlive the race.
pub(super) struct EndpointSend {
    pub client: Client,
    pub endpoint: RpcEndpoint,
    pub warm: Arc<WarmState>,
    pub limit: Duration,
    pub id: u64,
    pub raw_tx: String,
}

impl EndpointSend {
    pub(super) async fn run(self) -> WalletResult<B256> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [self.raw_tx],
            "id": self.id,
        });
        let answer = post(&self.client, &self.endpoint, &self.warm, self.limit, &body).await?;
        let host = crate::rpc::endpoint_host(&self.endpoint.url);
        if let Some(error) = answer.error {
            // Same exact match as RpcClient, which never matches an error carrying data.
            return match crate::rpc::raw_tx_hash(&self.raw_tx) {
                // The endpoint's pool already holds these signed bytes: a send, not a failure.
                Ok(hash)
                    if error.data.is_none()
                        && crate::error::is_already_known(&error.message, &hash) =>
                {
                    debug!(tx_hash = %hash, endpoint = %host, "endpoint already holds the transaction; counted as sent");
                    Ok(hash)
                }
                _ => Err(WalletError::RpcError(format_rpc_error(&error))),
            };
        }
        // Clean result only — a rejected send leaves the endpoint eligible for
        // the next warmup probe.
        self.warm.mark_success();
        match answer.result {
            Some(hash) => parse_b256_hex(&hash),
            None => {
                // RPC accepted the TX but returned null result — compute hash locally
                let hash = crate::rpc::raw_tx_hash(&self.raw_tx)?;
                warn!(
                    tx_hash = %hash,
                    endpoint = %host,
                    "RPC returned null result for eth_sendRawTransaction — TX likely accepted"
                );
                Ok(hash)
            }
        }
    }
}

/// Run every send on its own task and return on the first success, else with every
/// error. A send still running when this returns runs to its end and logs its verdict,
/// so a slow endpoint (a remote sequencer behind a fresh TLS handshake) still gets the
/// transaction directly when a nearer one answers first. The sends also outlive a
/// cancelled caller; each then logs its own verdict.
pub(super) async fn first_success(sends: Vec<(String, EndpointSend)>) -> BroadcastResult {
    let (report, mut verdicts) = mpsc::unbounded_channel();
    for (host, send) in sends {
        let report = report.clone();
        tokio::spawn(async move {
            let tx_hash = crate::rpc::raw_tx_hash(&send.raw_tx).unwrap_or_default();
            let started = Instant::now();
            let result = send.run().await;
            let verdict = (host, result, started.elapsed().as_millis() as u64);
            if let Err(mpsc::error::SendError((endpoint, result, endpoint_ms))) =
                report.send(verdict)
            {
                log_late(tx_hash, &endpoint, &result, endpoint_ms);
            }
        });
    }
    drop(report);
    let mut errors = Vec::new();
    while let Some((host, result, _)) = verdicts.recv().await {
        match result {
            Ok(hash) => {
                tokio::spawn(drain_late(verdicts, hash));
                return BroadcastResult {
                    tx_hash: Some(hash),
                    success_count: 1,
                    failure_count: errors.len(),
                    errors,
                    first_success: Some(host),
                };
            }
            Err(error) => errors.push((host, error)),
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

async fn drain_late(mut verdicts: Verdicts, hash: B256) {
    while let Some((endpoint, result, endpoint_ms)) = verdicts.recv().await {
        log_late(hash, &endpoint, &result, endpoint_ms);
    }
}

fn log_late(hash: B256, endpoint: &str, result: &WalletResult<B256>, endpoint_ms: u64) {
    match result {
        Ok(_) => debug!(
            tx_hash = %hash, %endpoint, endpoint_ms,
            "late broadcast endpoint accepted"
        ),
        Err(error) => warn!(
            tx_hash = %hash, %endpoint, endpoint_ms,
            error = %crate::redact_urls(&error.to_string()),
            "late broadcast endpoint rejected"
        ),
    }
}

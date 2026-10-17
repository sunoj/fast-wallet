// Raw transaction submission and endpoint timing.
// Implements RpcClient sends, preserving null-result hash fallback.
use super::*;

impl RpcClient {
    /// Send a raw transaction
    ///
    /// Returns the transaction hash.
    /// Some RPC nodes return `null` result (no error) for accepted transactions.
    /// In that case we compute the tx hash from the raw transaction bytes.
    pub async fn send_raw_transaction(&self, raw_tx: &str) -> WalletResult<B256> {
        self.send_raw_transaction_with_connection_hint(raw_tx)
            .await
            .map(|(hash, _)| hash)
    }

    pub(super) async fn send_raw_transaction_with_connection_hint(
        &self,
        raw_tx: &str,
    ) -> WalletResult<(B256, bool)> {
        self.send_with_detail(raw_tx, None).await
    }

    pub(super) async fn send_with_detail(
        &self,
        raw_tx: &str,
        detail: Option<&mut detail::ErrorDetail>,
    ) -> WalletResult<(B256, bool)> {
        // reqwest/hyper does not expose the exact connection-reused bit. This
        // is a passive pool hint: any prior request on this RpcClient means a
        // pooled connection was available for reuse, though hyper may still open
        // a fresh connection if the pool entry was closed.
        let connection_reused = self.connection_reuse_hint();
        match self
            .request_with_detail::<String>("eth_sendRawTransaction", json!([raw_tx]), detail)
            .await
        {
            Ok(result) => parse_b256_hex(&result).map(|hash| (hash, connection_reused)),
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
                Ok((hash, connection_reused))
            }
            Err(e) => Err(e),
        }
    }

    /// Send a raw transaction and return endpoint timing.
    pub async fn send_raw_transaction_detailed(&self, raw_tx: &str) -> WalletResult<SendResult> {
        let started = Instant::now();
        let (tx_hash, connection_reused) = self
            .send_raw_transaction_with_connection_hint(raw_tx)
            .await?;
        let endpoint_ms = started.elapsed().as_millis() as u64;
        Ok(SendResult {
            tx_hash: format!("{tx_hash:?}"),
            sign_ms: 0.0,
            semaphore_wait_ms: 0,
            broadcast_fanout_ms: endpoint_ms,
            fastest_endpoint_ms: endpoint_ms,
            fastest_endpoint_host: endpoint_host(&self.url).to_string(),
            slowest_endpoint_ms: endpoint_ms,
            first_success_endpoint_index: 0,
            failed_endpoints: 0,
            connection_reused,
            per_endpoint_ms: vec![(endpoint_host(&self.url).to_string(), Ok(endpoint_ms))],
        })
    }

    /// Send raw transaction bytes (convenience method)
    pub async fn send_raw_transaction_bytes(&self, tx_bytes: &[u8]) -> WalletResult<B256> {
        let hex_tx = format!("0x{}", hex::encode(tx_bytes));
        self.send_raw_transaction(&hex_tx).await
    }
}

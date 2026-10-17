// Read-only RPC methods and receipt polling.
// Implements RpcClient operations using shared request and hex parsing helpers.
use super::*;

impl RpcClient {
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
    ///
    /// A receipt-fetch error does not end the wait: it is logged and polling
    /// continues, up to [`MAX_CONSECUTIVE_RECEIPT_ERRORS`] in a row, after which
    /// the last error is returned. Any successful poll resets the count. The
    /// overall `timeout` still returns [`WalletError::Timeout`].
    pub async fn wait_for_receipt(
        &self,
        tx_hash: B256,
        timeout: Duration,
        poll_interval: Duration,
    ) -> WalletResult<Value> {
        let start = std::time::Instant::now();
        let mut consecutive_errors = 0;

        loop {
            if start.elapsed() > timeout {
                return Err(WalletError::Timeout);
            }

            match self.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => return Ok(receipt),
                Ok(None) => consecutive_errors = 0,
                Err(error) => {
                    consecutive_errors += 1;
                    if consecutive_errors >= MAX_CONSECUTIVE_RECEIPT_ERRORS {
                        return Err(error);
                    }
                    tracing::warn!(
                        tx_hash = %tx_hash,
                        error = %error,
                        consecutive_errors,
                        "receipt poll failed; retrying"
                    );
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Estimate gas for a transaction (uses the RPC's default block — usually `latest`).
    pub async fn estimate_gas(
        &self,
        from: Address,
        to: Option<Address>,
        value: Option<U256>,
        data: Option<&[u8]>,
    ) -> WalletResult<u64> {
        self.estimate_gas_at_block(from, to, value, data, None)
            .await
    }

    /// Estimate gas against a specific block tag.
    ///
    /// On Base flashblock-enabled RPCs, passing `Some("pending")` returns gas
    /// for the latest flashblock state, which captures intra-block oracle
    /// updates that `latest` would miss.
    pub async fn estimate_gas_at_block(
        &self,
        from: Address,
        to: Option<Address>,
        value: Option<U256>,
        data: Option<&[u8]>,
        block_tag: Option<&str>,
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

        let rpc_params = match block_tag {
            Some(tag) => json!([params, tag]),
            None => json!([params]),
        };
        let result: String = self.request("eth_estimateGas", rpc_params).await?;
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

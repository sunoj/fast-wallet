// JSON-RPC request execution and warm-state bookkeeping.
// Implements the shared request path with optional request-local error metadata.
use super::*;

impl RpcClient {
    /// Execute a raw JSON-RPC request
    pub(super) async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Value,
    ) -> WalletResult<T> {
        self.request_with_detail(method, params, None).await
    }

    // Redact where the error is BUILT, not where it is logged: a reqwest
    // error embeds the keyed request URL, and this error is returned to
    // callers that log it verbatim, wrap it in `format!("{e}")`, or hand it
    // to per-endpoint telemetry. Sanitising downstream means one missed
    // call site is a leaked API key.
    //
    // Unless the whole answer arrives, the pooled connection may be gone and
    // the next warmup must re-dial: a transport error, a body cut short, or
    // a caller that drops this future (its own timeout, a lost race) each
    // drop the armed guard.
    pub(super) async fn request_with_detail<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Value,
        mut detail: Option<&mut detail::ErrorDetail>,
    ) -> WalletResult<T> {
        self.mark_request_started();
        let req = RpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id: self.next_id(),
        };

        let guard = self.warm.loss_guard();
        let response = self
            .client
            .post(&self.url)
            .json(&req)
            .send()
            .await
            .map_err(|e| {
                if let Some(detail) = detail.as_deref_mut() {
                    detail.transport(&e);
                }
                WalletError::NetworkError(redact_urls(&e.to_string()))
            })?;

        let rpc_response = detail::decode_response(response, detail).await?;
        guard.disarm();

        if let Some(error) = rpc_response.error {
            return Err(WalletError::RpcError(format_rpc_error(&error)));
        }

        let result = rpc_response
            .result
            .ok_or_else(|| WalletError::RpcError("Empty response".to_string()))?;

        // Mark only after a fully successful response. A live host that answers
        // every call with a JSON-RPC error — or with neither result nor error —
        // is not warm in any useful sense, and marking it here would suppress
        // the next probe for a whole throttle window.
        self.warm.mark_success();
        Ok(result)
    }
}

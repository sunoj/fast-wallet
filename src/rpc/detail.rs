// Request-local, bounded error metadata for broadcast observations.
// Decodes the original RPC envelope separately from telemetry classification.
use super::*;

#[derive(Default)]
pub(super) struct ErrorDetail {
    pub http_status: Option<u16>,
    pub rpc_code: Option<i64>,
    pub cause: Option<&'static str>,
}

impl ErrorDetail {
    pub(super) fn transport(&mut self, error: &reqwest::Error) {
        self.cause = Some(if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect"
        } else if error.is_decode() {
            "decode"
        } else {
            "transport"
        });
    }

    fn envelope(&mut self, value: &Value) {
        if let Some(error) = value.get("error").filter(|error| error.is_object()) {
            self.rpc_code = error.get("code").and_then(Value::as_i64);
            self.cause.get_or_insert("rpc_error");
        }
    }
}

struct Captured<T> {
    response: Result<RpcResponse<T>, serde_json::Error>,
    value: Option<Value>,
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for Captured<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        Ok(Self {
            response: serde_json::from_str(raw.get()),
            value: serde_json::from_str(raw.get()).ok(),
        })
    }
}

pub(super) async fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    detail: Option<&mut ErrorDetail>,
) -> WalletResult<RpcResponse<T>> {
    let Some(detail) = detail else {
        return response
            .json()
            .await
            .map_err(|error| WalletError::RpcError(redact_urls(&error.to_string())));
    };
    detail.http_status = Some(response.status().as_u16());
    if !response.status().is_success() {
        detail.cause = Some("http_status");
    }
    let captured: Captured<T> = response.json().await.map_err(|error| {
        if detail.cause.is_none() || error.is_timeout() {
            detail.transport(&error);
        }
        WalletError::RpcError(redact_urls(&error.to_string()))
    })?;
    if let Some(value) = &captured.value {
        detail.envelope(value);
    }
    captured.response.map_err(|_| {
        detail.cause.get_or_insert("decode");
        // reqwest's JSON decoder exposes this same fixed Display text.
        WalletError::RpcError("error decoding response body".to_string())
    })
}

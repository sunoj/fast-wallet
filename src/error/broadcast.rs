// Structured broadcast failures that survive string-based caller wrappers.
// Exports endpoint verdicts and JSON display/parser; depends on serde and RPC redaction.
use super::WalletError;
use crate::rpc::{endpoint_host, redact_urls};
use serde::{Deserialize, Serialize};
use std::fmt;

const PREFIX: &str = "All broadcast endpoints failed: ";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointFailureClass {
    Transport,
    Rejection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointVerdict {
    pub host: String,
    pub error: String,
    pub elapsed_ms: u64,
    pub class: EndpointFailureClass,
}

impl EndpointVerdict {
    pub(crate) fn new(url: &str, error: &WalletError, elapsed_ms: u64) -> Self {
        let class = match error {
            WalletError::NetworkError(_) | WalletError::Timeout => EndpointFailureClass::Transport,
            _ => EndpointFailureClass::Rejection,
        };
        Self {
            host: endpoint_host(url).to_string(),
            error: redact_urls(&error.to_string()),
            elapsed_ms,
            class,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BroadcastFailure {
    pub endpoints: Vec<EndpointVerdict>,
}

impl BroadcastFailure {
    /// Decode the complete JSON payload after the marker, including wallet wrappers.
    pub fn from_error_text(text: &str) -> Result<Option<Self>, serde_json::Error> {
        match text.split_once(PREFIX) {
            Some((_, payload)) => serde_json::from_str(payload).map(Some),
            None => Ok(None),
        }
    }
}

impl fmt::Display for BroadcastFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{PREFIX}{payload}")
    }
}

impl WalletError {
    /// A low verdict wins disagreements: pending sync avoids rewinding to a stale node's latest.
    pub(crate) fn nonce_sync_block(&self) -> Option<&'static str> {
        let text = match self {
            Self::BroadcastFailed(failure) => failure
                .endpoints
                .iter()
                .filter(|v| v.class == EndpointFailureClass::Rejection)
                .map(|v| v.error.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
            _ => self.to_string(),
        }
        .to_ascii_lowercase();
        if text.contains("nonce too low") {
            Some("pending")
        } else if text.contains("nonce too high") {
            Some("latest")
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflicting_nonce_verdicts_sync_pending_in_either_order() {
        let high = EndpointVerdict::new(
            "https://high.test",
            &WalletError::RpcError("nonce too high".into()),
            1,
        );
        let low = EndpointVerdict::new(
            "https://low.test",
            &WalletError::RpcError("nonce too low".into()),
            2,
        );
        for endpoints in [vec![high.clone(), low.clone()], vec![low, high]] {
            let error = WalletError::BroadcastFailed(BroadcastFailure { endpoints });
            assert_eq!(error.nonce_sync_block(), Some("pending"));
        }
        assert_eq!(
            WalletError::RpcError("nonce too high".into()).nonce_sync_block(),
            Some("latest")
        );
        assert_eq!(WalletError::Timeout.nonce_sync_block(), None);
    }

    #[test]
    fn rpc_forwarder_timeout_is_not_a_transport_verdict() {
        let error = WalletError::RpcError("Post https://sequencer.test/key: timeout".into());
        let verdict = EndpointVerdict::new("https://user:pass@rpc.test/key", &error, 5005);
        assert_eq!(verdict.host, "rpc.test");
        assert_eq!(verdict.class, EndpointFailureClass::Rejection);
        assert!(!verdict.error.contains("/key"));
    }
}

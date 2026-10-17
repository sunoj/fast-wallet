// Client-side endpoint observations, without URLs, error bodies, or signed bytes.
// Exports EndpointObservation and its optional nonblocking sink for batch broadcasts.
use crate::error::{WalletError, WalletResult};
use alloy::primitives::B256;
use serde::Serialize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub type EndpointObserver = Arc<dyn Fn(EndpointObservation) + Send + Sync>;

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct EndpointObservation {
    #[serde(serialize_with = "serialize_hash")]
    pub tx_hash: B256,
    /// Index in the deduplicated configured route list; not upstream identity.
    pub endpoint_id: usize,
    pub is_first_result: Option<bool>,
    pub phase: &'static str,
    pub result: &'static str,
    /// HTTP response status, when headers were received.
    pub http_status: Option<u16>,
    /// Signed integer JSON-RPC code; absent or non-integer codes are unavailable.
    pub rpc_code: Option<i64>,
    /// Fixed error category; unavailable for dispatch and successful completion.
    pub cause: Option<&'static str>,
    pub started_utc_ms: Option<u64>,
    pub completed_utc_ms: Option<u64>,
    pub elapsed_us: Option<u64>,
    /// Passive prior-request hint; does not establish actual transport reuse.
    pub connection_reuse_hint: Option<bool>,
}

fn serialize_hash<S: serde::Serializer>(hash: &B256, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{hash:#x}"))
}

pub(super) struct Observation {
    sink: Option<EndpointObserver>,
    row: EndpointObservation,
    started: Instant,
    detached: Arc<AtomicBool>,
    first_result: Arc<AtomicBool>,
}

pub(super) fn utc_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| u64::try_from(time.as_millis()).ok())
}

pub(super) fn emit(sink: &Option<EndpointObserver>, row: EndpointObservation) {
    if let Some(sink) = sink {
        // An observability consumer must never unwind through the send path.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(row))).is_err() {
            tracing::warn!("endpoint observation sink panicked; send continues");
        }
    }
}

impl Observation {
    pub(super) fn start(
        sink: Option<EndpointObserver>,
        tx_hash: B256,
        endpoint_id: usize,
        detached: Arc<AtomicBool>,
        first_result: Arc<AtomicBool>,
    ) -> Self {
        let row = EndpointObservation {
            tx_hash,
            endpoint_id,
            is_first_result: None,
            phase: "dispatch",
            result: "pending",
            http_status: None,
            rpc_code: None,
            cause: None,
            started_utc_ms: utc_ms(),
            completed_utc_ms: None,
            elapsed_us: None,
            connection_reuse_hint: None,
        };
        emit(&sink, row.clone());
        Self {
            sink,
            row,
            started: Instant::now(),
            detached,
            first_result,
        }
    }

    pub(super) async fn send(
        self,
        client: &super::RpcClient,
        tx: &str,
        timeout: std::time::Duration,
    ) -> WalletResult<(B256, bool)> {
        let mut detail = super::detail::ErrorDetail::default();
        let result =
            match tokio::time::timeout(timeout, client.send_with_detail(tx, Some(&mut detail)))
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    detail.cause = Some("timeout");
                    Err(WalletError::SendTimeout {
                        elapsed_ms: timeout.as_millis() as u64,
                    })
                }
            };
        self.finish(&result, detail);
        result
    }

    pub(super) fn finish(
        mut self,
        result: &WalletResult<(B256, bool)>,
        detail: super::detail::ErrorDetail,
    ) {
        self.row.http_status = detail.http_status;
        self.row.rpc_code = detail.rpc_code;
        self.row.cause = if result.is_ok() {
            None
        } else {
            detail.cause.or(Some("decode"))
        };
        self.row.is_first_result = Some(!self.first_result.swap(true, Ordering::AcqRel));
        self.row.phase = if self.detached.load(Ordering::Acquire) {
            "detached_completion"
        } else {
            "completion_before_return"
        };
        self.row.result = match result {
            Ok(_) => "client_success",
            Err(WalletError::Timeout | WalletError::SendTimeout { .. }) => "timeout_uncertain",
            Err(WalletError::NetworkError(_)) => "transport_uncertain",
            Err(WalletError::RpcError(_)) => "rpc_error",
            Err(_) => "other_error",
        };
        self.row.completed_utc_ms = utc_ms();
        self.row.elapsed_us = Some(self.started.elapsed().as_micros() as u64);
        self.row.connection_reuse_hint = result.as_ref().ok().map(|(_, reused)| *reused);
        emit(&self.sink, self.row);
    }
}

pub(super) fn first_success(
    sink: Option<EndpointObserver>,
    tx_hash: B256,
    endpoint_id: usize,
    reused: bool,
    started: Instant,
) {
    emit(
        &sink,
        EndpointObservation {
            tx_hash,
            endpoint_id,
            is_first_result: None,
            phase: "first_success",
            result: "client_success",
            http_status: None,
            rpc_code: None,
            cause: None,
            started_utc_ms: None,
            completed_utc_ms: utc_ms(),
            elapsed_us: Some(started.elapsed().as_micros() as u64),
            connection_reuse_hint: Some(reused),
        },
    );
}

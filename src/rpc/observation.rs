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
pub struct EndpointObservation {
    pub tx_hash: B256,
    /// Index in the deduplicated configured route list; not upstream identity.
    pub endpoint_id: usize,
    pub phase: &'static str,
    pub result: &'static str,
    pub started_utc_ms: Option<u64>,
    pub completed_utc_ms: Option<u64>,
    pub elapsed_us: Option<u64>,
    pub connection_reused: Option<bool>,
}

pub(super) struct Observation {
    sink: Option<EndpointObserver>,
    row: EndpointObservation,
    started: Instant,
    detached: Arc<AtomicBool>,
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
    ) -> Self {
        let row = EndpointObservation {
            tx_hash,
            endpoint_id,
            phase: "dispatch",
            result: "pending",
            started_utc_ms: utc_ms(),
            completed_utc_ms: None,
            elapsed_us: None,
            connection_reused: None,
        };
        emit(&sink, row.clone());
        Self {
            sink,
            row,
            started: Instant::now(),
            detached,
        }
    }

    pub(super) fn finish(mut self, result: &WalletResult<(B256, bool)>) {
        self.row.phase = if self.detached.load(Ordering::Acquire) {
            "detached_completion"
        } else {
            "completion_before_return"
        };
        self.row.result = match result {
            Ok(_) => "rpc_ack",
            Err(WalletError::Timeout | WalletError::SendTimeout { .. }) => "timeout_uncertain",
            Err(WalletError::NetworkError(_)) => "transport_uncertain",
            Err(WalletError::RpcError(_)) => "rpc_error",
            Err(_) => "other_error",
        };
        self.row.completed_utc_ms = utc_ms();
        self.row.elapsed_us = Some(self.started.elapsed().as_micros() as u64);
        self.row.connection_reused = result.as_ref().ok().map(|(_, reused)| *reused);
        emit(&self.sink, self.row);
    }
}

// Warm-connection bookkeeping: whether an endpoint needs a connection-priming probe now.
// Exports WarmState (window chosen by URL scheme), LossGuard and both windows; reads tokio's
// clock so tests can drive it.
use parking_lot::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// Throttle window for an `https://` endpoint, which runs HTTP/2.
///
/// Pools never evict idle connections (`pool_idle_timeout(None)`) and HTTP/2
/// pings drop dead ones, so the window only bounds how long an idle connection
/// is trusted to survive without a request. Measured with
/// `examples/h2_idle_probe.rs` on 2026-10-06: QuickNode endpoints on three
/// chains (Arbitrum, Base, BSC) reused idle HTTP/2 connections after 1 and 5
/// minutes but dialled new ones after 10, 20 and 30 minutes despite the
/// pings (warm ~60 ms, cold ~175 ms), so they close idle connections between
/// 5 and 10 minutes. Four keyless public RPCs kept them for the full 30
/// minutes. 4 minutes stays under the shortest survival with margin. Probe
/// other hosts before relying on it: a host may close idle connections
/// regardless of pings.
pub(crate) const HTTPS_WARMUP_WINDOW: Duration = Duration::from_secs(4 * 60);

/// Throttle window for every other endpoint. Plain `http://` speaks HTTP/1.1:
/// no pings keep it open and a server's idle close goes unseen until the next
/// request. Such endpoints (a self-hosted node, a local proxy) bill nothing
/// for a probe, so they keep the short window.
pub(crate) const HTTP_WARMUP_WINDOW: Duration = Duration::from_secs(60);

/// Time of one endpoint's last clean answer; forgotten when its connection may be lost.
#[derive(Debug)]
pub(crate) struct WarmState {
    window: Duration,
    last_success: Mutex<Option<Instant>>,
}

impl WarmState {
    /// Warm state for `url`, parsed as a request parses it.
    pub(crate) fn for_url(url: &str) -> Self {
        let https = reqwest::Url::parse(url).is_ok_and(|u| u.scheme() == "https");
        Self {
            window: if https {
                HTTPS_WARMUP_WINDOW
            } else {
                HTTP_WARMUP_WINDOW
            },
            last_success: Mutex::new(None),
        }
    }

    /// A probe is due on first use, after a lost connection, or once the window lapses.
    pub(crate) fn probe_due(&self) -> bool {
        let fresh = |at: Instant| Instant::now().saturating_duration_since(at) < self.window;
        !self.last_success.lock().is_some_and(fresh)
    }

    pub(crate) fn mark_success(&self) {
        *self.last_success.lock() = Some(Instant::now());
    }

    /// The connection may be gone (transport error or abandoned send): probe next time.
    pub(crate) fn mark_lost(&self) {
        *self.last_success.lock() = None;
    }

    /// Marks this endpoint lost when dropped before [`LossGuard::disarm`].
    pub(crate) fn loss_guard(&self) -> LossGuard<'_> {
        LossGuard(Some(self))
    }
}

/// Held across one request: an error, a timeout, a body cut short or a send
/// dropped as a race loser each end it armed, and may each leave no usable
/// pooled connection behind.
pub(crate) struct LossGuard<'a>(Option<&'a WarmState>);

impl LossGuard<'_> {
    /// The whole answer arrived: the connection went back to the pool.
    pub(crate) fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for LossGuard<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.0 {
            state.mark_lost();
        }
    }
}

/// Let `idle` of tokio time pass at once, then run the clock in real time again,
/// so loopback I/O after it never races an auto-advancing paused clock.
#[cfg(test)]
pub(crate) async fn idle_for(idle: Duration) {
    tokio::time::pause();
    tokio::time::advance(idle).await;
    tokio::time::resume();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn probe_is_due_first_after_loss_and_after_the_window() {
        let state = WarmState::for_url("https://rpc.example/key");
        assert!(state.probe_due(), "first use probes");
        state.mark_success();
        assert!(!state.probe_due());
        tokio::time::advance(HTTPS_WARMUP_WINDOW - Duration::from_secs(1)).await;
        assert!(!state.probe_due(), "inside the window");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(state.probe_due(), "window lapsed");
        state.mark_success();
        state.mark_lost();
        assert!(state.probe_due(), "lost connection probes at once");
    }

    /// Probes in one hour of preheats every 10 s, each answered cleanly.
    async fn probes_in_an_hour(url: &str) -> usize {
        let state = WarmState::for_url(url);
        let mut probes = 0;
        for _ in 0..360 {
            if state.probe_due() {
                probes += 1;
                state.mark_success();
            }
            tokio::time::advance(Duration::from_secs(10)).await;
        }
        probes
    }

    #[tokio::test(start_paused = true)]
    async fn https_gets_the_measured_window_and_everything_else_sixty_seconds() {
        assert_eq!(probes_in_an_hour("https://rpc.example/key").await, 15);
        assert_eq!(probes_in_an_hour("HTTPS://RPC.EXAMPLE").await, 15);
        assert_eq!(probes_in_an_hour("http://127.0.0.1:4000").await, 60);
        assert_eq!(probes_in_an_hour("not a url").await, 60);
    }

    #[test]
    fn a_dropped_guard_marks_lost_and_a_disarmed_one_does_not() {
        let state = WarmState::for_url("https://rpc.example");
        state.mark_success();
        state.loss_guard().disarm();
        assert!(!state.probe_due(), "whole answer: still warm");
        drop(state.loss_guard());
        assert!(state.probe_due(), "abandoned request: probe next time");
    }
}

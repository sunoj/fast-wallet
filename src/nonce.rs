//! Local nonce management for high-performance transaction submission
//!
//! This module provides local nonce management to avoid RPC calls when sending
//! multiple transactions rapidly. It supports:
//! - Lock-free nonce acquisition for single-threaded hot paths
//! - Thread-safe nonce management for concurrent access
//! - Background synchronization with the blockchain
//! - Automatic nonce recovery on failures

use alloy::primitives::Address;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Fast nonce tracker for a single address
///
/// Uses atomic operations for lock-free nonce management in the hot path.
#[derive(Debug)]
pub struct NonceTracker {
    /// Current nonce (next nonce to use)
    current: AtomicU64,
    /// Pending nonces that have been allocated but not confirmed
    pending_count: AtomicU64,
    /// Last synced nonce from the chain
    synced_nonce: AtomicU64,
    /// Released middle nonces available for reuse.
    ///
    /// The hot path checks `gap_count` before taking this lock. The lock is only
    /// acquired when a gap exists, a nonce is released, or a chain sync prunes
    /// stale gaps.
    gaps: Mutex<BTreeSet<u64>>,
    /// Fast check that lets normal allocation avoid locking `gaps`.
    gap_count: AtomicU64,
}

impl NonceTracker {
    /// Create a new nonce tracker with initial nonce
    pub fn new(initial_nonce: u64) -> Self {
        Self {
            current: AtomicU64::new(initial_nonce),
            pending_count: AtomicU64::new(0),
            synced_nonce: AtomicU64::new(initial_nonce),
            gaps: Mutex::new(BTreeSet::new()),
            gap_count: AtomicU64::new(0),
        }
    }

    /// Get the next nonce and increment (lock-free)
    ///
    /// This is the hot path - must be as fast as possible.
    /// Uses AcqRel ordering which is sufficient for single-threaded hot path
    /// while still providing visibility guarantees.
    #[inline(always)]
    pub fn get_and_increment(&self) -> u64 {
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        self.reserve_next_nonce()
    }

    /// Reserve a nonce with an RAII guard.
    #[inline(always)]
    pub fn reserve(self: &Arc<Self>) -> ReservedNonce {
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        let nonce = self.reserve_next_nonce();
        ReservedNonce::new(Arc::clone(self), nonce)
    }

    /// Get the current nonce without incrementing
    #[inline(always)]
    pub fn peek(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Get the number of pending transactions
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Mark a nonce as confirmed (transaction included in block)
    #[inline]
    pub fn confirm(&self) {
        self.decrement_pending();
    }

    /// Release a nonce and make it available for reuse.
    #[inline]
    pub fn release(&self, used_nonce: u64) {
        self.decrement_pending();
        self.recycle_nonce(used_nonce);
    }

    /// Set the nonce from an external source without rewinding local state.
    #[inline]
    pub fn set(&self, nonce: u64) {
        self.sync_forward(nonce);
    }

    /// Reset pending count (used during sync when all pending are cleared)
    #[inline]
    pub fn reset_pending(&self) {
        self.pending_count.store(0, Ordering::Relaxed);
    }

    /// Get the last synced nonce
    #[inline]
    pub fn synced_nonce(&self) -> u64 {
        self.synced_nonce.load(Ordering::Acquire)
    }

    /// Get the number of recycled gaps waiting to be reused.
    #[inline]
    pub fn gap_count(&self) -> u64 {
        self.gap_count.load(Ordering::Relaxed)
    }

    /// Sync from chain without rewinding over locally reserved nonces.
    pub fn sync_forward(&self, chain_nonce: u64) -> bool {
        let mut gaps = self.gaps.lock();
        gaps.retain(|nonce| *nonce >= chain_nonce);
        self.gap_count.store(gaps.len() as u64, Ordering::Release);
        self.synced_nonce.fetch_max(chain_nonce, Ordering::AcqRel);

        loop {
            let current = self.current.load(Ordering::Acquire);
            if chain_nonce <= current {
                return false;
            }
            if self
                .current
                .compare_exchange(current, chain_nonce, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    #[inline(always)]
    fn reserve_next_nonce(&self) -> u64 {
        if self.gap_count.load(Ordering::Relaxed) > 0 {
            let mut gaps = self.gaps.lock();
            if let Some(nonce) = gaps.first().copied() {
                gaps.remove(&nonce);
                self.gap_count.fetch_sub(1, Ordering::AcqRel);
                return nonce;
            }
        }
        self.current.fetch_add(1, Ordering::AcqRel)
    }

    fn recycle_nonce(&self, used_nonce: u64) {
        let mut gaps = self.gaps.lock();
        let synced = self.synced_nonce.load(Ordering::Acquire);
        if used_nonce < synced {
            return;
        }

        if let Some(expected) = used_nonce.checked_add(1) {
            if self
                .current
                .compare_exchange(expected, used_nonce, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.collapse_top_gaps(&mut gaps);
                return;
            }
        }

        let current = self.current.load(Ordering::Acquire);
        if used_nonce < current {
            if used_nonce >= synced && used_nonce < current && gaps.insert(used_nonce) {
                self.gap_count.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn collapse_top_gaps(&self, gaps: &mut BTreeSet<u64>) {
        loop {
            let current = self.current.load(Ordering::Acquire);
            let Some(previous) = current.checked_sub(1) else {
                return;
            };
            if previous < self.synced_nonce.load(Ordering::Acquire) {
                return;
            }
            if !gaps.remove(&previous) {
                return;
            }
            if self
                .current
                .compare_exchange(current, previous, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.gap_count.fetch_sub(1, Ordering::AcqRel);
            } else {
                gaps.insert(previous);
                return;
            }
        }
    }

    fn decrement_pending(&self) {
        let _ = self.pending_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |pending| pending.checked_sub(1),
        );
    }
}

/// State for an RAII nonce reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReservedNonceState {
    Held,
    Broadcasting,
    Committed,
    Released,
}

/// RAII guard for a reserved nonce.
///
/// Dropping a held reservation releases the nonce back to the tracker. Once a
/// transaction is broadcasting, drop no longer releases because the signed
/// transaction may already be in an RPC or mempool.
pub struct ReservedNonce {
    tracker: Arc<NonceTracker>,
    nonce: u64,
    state: ReservedNonceState,
}

impl std::fmt::Debug for ReservedNonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReservedNonce")
            .field("nonce", &self.nonce)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl ReservedNonce {
    fn new(tracker: Arc<NonceTracker>, nonce: u64) -> Self {
        Self {
            tracker,
            nonce,
            state: ReservedNonceState::Held,
        }
    }

    /// Get the reserved nonce.
    #[inline]
    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Mark the reservation as having a signed transaction entering broadcast.
    pub fn mark_broadcasting(&mut self) -> bool {
        if self.state != ReservedNonceState::Held {
            return false;
        }
        self.state = ReservedNonceState::Broadcasting;
        true
    }

    /// Mark a held or broadcasting nonce as accepted by an RPC endpoint.
    pub fn commit(&mut self) -> bool {
        if self.state != ReservedNonceState::Held && self.state != ReservedNonceState::Broadcasting
        {
            return false;
        }
        self.tracker.confirm();
        self.state = ReservedNonceState::Committed;
        true
    }

    /// Release a held or broadcasting nonce back to the tracker.
    pub fn release(&mut self) -> bool {
        if self.state != ReservedNonceState::Held && self.state != ReservedNonceState::Broadcasting
        {
            return false;
        }
        self.tracker.release(self.nonce);
        self.state = ReservedNonceState::Released;
        true
    }
}

impl Drop for ReservedNonce {
    fn drop(&mut self) {
        match self.state {
            ReservedNonceState::Held => {
                self.tracker.release(self.nonce);
                self.state = ReservedNonceState::Released;
                tracing::warn!(
                    nonce = self.nonce,
                    "ReservedNonce dropped without commit/release; auto-released"
                );
            }
            ReservedNonceState::Broadcasting => {
                tracing::warn!(
                    nonce = self.nonce,
                    "ReservedNonce dropped during broadcast; left for chain sync"
                );
            }
            ReservedNonceState::Committed | ReservedNonceState::Released => {}
        }
    }
}

/// Multi-address nonce manager with async sync capabilities
pub struct NonceManager {
    /// Per-address nonce trackers
    trackers: DashMap<Address, Arc<NonceTracker>>,
    /// Notification for sync completion
    sync_notify: Notify,
}

impl NonceManager {
    /// Create a new nonce manager
    pub fn new() -> Self {
        Self {
            trackers: DashMap::new(),
            sync_notify: Notify::new(),
        }
    }

    /// Get or create a nonce tracker for an address
    pub fn get_tracker(&self, address: Address) -> Arc<NonceTracker> {
        self.trackers
            .entry(address)
            .or_insert_with(|| Arc::new(NonceTracker::new(0)))
            .clone()
    }

    /// Initialize nonce for an address
    pub fn init_nonce(&self, address: Address, nonce: u64) {
        self.trackers
            .entry(address)
            .or_insert_with(|| Arc::new(NonceTracker::new(nonce)));

        if let Some(tracker) = self.trackers.get(&address) {
            tracker.set(nonce);
        }
    }

    /// Get next nonce for an address (lock-free hot path)
    #[inline]
    pub fn get_nonce(&self, address: Address) -> u64 {
        self.get_tracker(address).get_and_increment()
    }

    /// Reserve next nonce for an address with RAII release on drop.
    #[inline]
    pub fn reserve_nonce(&self, address: Address) -> ReservedNonce {
        self.get_tracker(address).reserve()
    }

    /// Peek current nonce without incrementing
    #[inline]
    pub fn peek_nonce(&self, address: Address) -> u64 {
        self.get_tracker(address).peek()
    }

    /// Confirm a transaction was included
    #[inline]
    pub fn confirm_nonce(&self, address: Address) {
        if let Some(tracker) = self.trackers.get(&address) {
            tracker.confirm();
        }
    }

    /// Release a transaction nonce back to the recycling pool
    #[inline]
    pub fn release_nonce(&self, address: Address, nonce: u64) {
        if let Some(tracker) = self.trackers.get(&address) {
            tracker.release(nonce);
        }
    }

    /// Update nonce from chain (call after fetching from RPC)
    pub fn sync_nonce(&self, address: Address, chain_nonce: u64) {
        let tracker = self.get_tracker(address);
        if tracker.sync_forward(chain_nonce) {
            self.sync_notify.notify_waiters();
        }
    }

    /// Wait for sync notification
    pub async fn wait_for_sync(&self) {
        self.sync_notify.notified().await;
    }

    /// Get all addresses being tracked
    pub fn tracked_addresses(&self) -> Vec<Address> {
        self.trackers.iter().map(|r| *r.key()).collect()
    }

    /// Get pending transaction count for an address
    pub fn pending_count(&self, address: Address) -> u64 {
        self.trackers
            .get(&address)
            .map(|t| t.pending_count())
            .unwrap_or(0)
    }
}

impl Default for NonceManager {
    fn default() -> Self {
        Self::new()
    }
}

/// High-performance single-address nonce manager
///
/// When you only need to manage one address, this provides slightly
/// better performance by avoiding the hashmap lookup.
pub struct SingleAddressNonceManager {
    tracker: Arc<NonceTracker>,
    address: Address,
}

impl SingleAddressNonceManager {
    /// Create a new single-address nonce manager
    pub fn new(address: Address, initial_nonce: u64) -> Self {
        Self {
            tracker: Arc::new(NonceTracker::new(initial_nonce)),
            address,
        }
    }

    /// Get the address
    #[inline]
    pub fn address(&self) -> Address {
        self.address
    }

    /// Get next nonce (lock-free)
    #[inline]
    pub fn get_nonce(&self) -> u64 {
        self.tracker.get_and_increment()
    }

    /// Reserve next nonce with RAII release on drop.
    #[inline]
    pub fn reserve(&self) -> ReservedNonce {
        self.tracker.reserve()
    }

    /// Peek current nonce
    #[inline]
    pub fn peek(&self) -> u64 {
        self.tracker.peek()
    }

    /// Confirm transaction
    #[inline]
    pub fn confirm(&self) {
        self.tracker.confirm();
    }

    /// Release transaction nonce
    #[inline]
    pub fn release(&self, nonce: u64) {
        self.tracker.release(nonce);
    }

    /// Sync with chain nonce.
    ///
    /// Only advances the local tracker forward and prunes recycled gaps that
    /// the chain has already consumed. It never rewinds past in-flight
    /// pre-signed transactions whose nonces are not yet visible on chain.
    #[inline]
    pub fn sync(&self, chain_nonce: u64) {
        self.tracker.sync_forward(chain_nonce);
    }

    /// Get pending count
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.tracker.pending_count()
    }

    /// Get number of released middle nonces available for reuse.
    #[inline]
    pub fn gap_count(&self) -> u64 {
        self.tracker.gap_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockNonceRpc {
        next_nonce: AtomicU64,
    }

    impl MockNonceRpc {
        fn new(initial_nonce: u64) -> Self {
            Self {
                next_nonce: AtomicU64::new(initial_nonce),
            }
        }

        async fn get_nonce_latest(&self, _address: Address) -> u64 {
            self.next_nonce.fetch_add(1, Ordering::AcqRel)
        }
    }

    async fn sync_nonce_latest_once(
        manager: &SingleAddressNonceManager,
        rpc: &MockNonceRpc,
    ) -> u64 {
        let chain_nonce = rpc.get_nonce_latest(manager.address()).await;
        manager.sync(chain_nonce);
        chain_nonce
    }

    #[test]
    fn test_nonce_tracker_increment() {
        let tracker = NonceTracker::new(0);

        assert_eq!(tracker.get_and_increment(), 0);
        assert_eq!(tracker.get_and_increment(), 1);
        assert_eq!(tracker.get_and_increment(), 2);
        assert_eq!(tracker.peek(), 3);
    }

    #[test]
    fn test_nonce_tracker_pending() {
        let tracker = NonceTracker::new(0);

        tracker.get_and_increment();
        tracker.get_and_increment();
        assert_eq!(tracker.pending_count(), 2);

        tracker.confirm();
        assert_eq!(tracker.pending_count(), 1);

        tracker.release(0);
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_nonce_manager_multi_address() {
        let manager = NonceManager::new();
        let addr1 = Address::ZERO;
        let addr2 = Address::repeat_byte(1);

        manager.init_nonce(addr1, 10);
        manager.init_nonce(addr2, 20);

        assert_eq!(manager.get_nonce(addr1), 10);
        assert_eq!(manager.get_nonce(addr1), 11);
        assert_eq!(manager.get_nonce(addr2), 20);
        assert_eq!(manager.get_nonce(addr2), 21);
    }

    #[test]
    fn test_nonce_manager_sync() {
        let manager = NonceManager::new();
        let addr = Address::ZERO;

        manager.init_nonce(addr, 0);
        manager.get_nonce(addr); // 0
        manager.get_nonce(addr); // 1

        // Simulate chain confirmation
        manager.sync_nonce(addr, 5);
        assert_eq!(manager.peek_nonce(addr), 5);
    }

    #[test]
    fn test_single_address_nonce_manager() {
        let addr = Address::ZERO;
        let manager = SingleAddressNonceManager::new(addr, 100);

        assert_eq!(manager.get_nonce(), 100);
        assert_eq!(manager.get_nonce(), 101);
        assert_eq!(manager.pending_count(), 2);

        manager.confirm();
        assert_eq!(manager.pending_count(), 1);
    }

    /// Release of the most recently allocated nonce rewinds the top nonce.
    #[test]
    fn test_release_reclaims_last_nonce() {
        let tracker = NonceTracker::new(10);

        let n = tracker.get_and_increment(); // n=10, current=11
        assert_eq!(n, 10);
        assert_eq!(tracker.peek(), 11);

        tracker.release(10);
        assert_eq!(tracker.peek(), 10); // reclaimed
        assert_eq!(tracker.gap_count(), 0);

        // Next allocation reuses nonce 10
        assert_eq!(tracker.get_and_increment(), 10);
    }

    /// Middle nonce release is recycled by the next reservation.
    #[test]
    fn test_middle_nonce_release_reused_by_next_reserve() {
        let tracker = NonceTracker::new(10);

        let _n0 = tracker.get_and_increment(); // 10, current=11
        let _n1 = tracker.get_and_increment(); // 11, current=12

        tracker.release(10);
        assert_eq!(tracker.peek(), 12);
        assert_eq!(tracker.gap_count(), 1);
        assert_eq!(tracker.get_and_increment(), 10);
        assert_eq!(tracker.gap_count(), 0);
    }

    #[test]
    fn test_drop_without_commit_releases_nonce() {
        let tracker = Arc::new(NonceTracker::new(10));
        {
            let reservation = tracker.reserve();
            assert_eq!(reservation.nonce(), 10);
            assert_eq!(tracker.peek(), 11);
        }
        assert_eq!(tracker.peek(), 10);
        assert_eq!(tracker.reserve().nonce(), 10);
    }

    #[test]
    fn test_drop_during_broadcasting_does_not_release() {
        let tracker = Arc::new(NonceTracker::new(10));
        {
            let mut reservation = tracker.reserve();
            assert_eq!(reservation.nonce(), 10);
            assert!(reservation.mark_broadcasting());
        }
        assert_eq!(tracker.peek(), 11);
        assert_eq!(tracker.get_and_increment(), 11);
        assert_eq!(tracker.pending_count(), 2);
    }

    #[test]
    fn test_forward_sync_prunes_gaps() {
        let tracker = NonceTracker::new(10);

        let _n0 = tracker.get_and_increment(); // 10
        let _n1 = tracker.get_and_increment(); // 11
        tracker.release(10);
        assert_eq!(tracker.gap_count(), 1);

        assert!(!tracker.sync_forward(11));
        assert_eq!(tracker.peek(), 12);
        assert_eq!(tracker.gap_count(), 0);
        assert_eq!(tracker.get_and_increment(), 12);
    }

    #[test]
    fn test_concurrent_release_sync_never_rewinds_below_synced() {
        use std::sync::Barrier;
        use std::thread;

        for _ in 0..1000 {
            let tracker = Arc::new(NonceTracker::new(10));
            assert_eq!(tracker.get_and_increment(), 10);
            let barrier = Arc::new(Barrier::new(2));

            let release_tracker = Arc::clone(&tracker);
            let release_barrier = Arc::clone(&barrier);
            let release = thread::spawn(move || {
                release_barrier.wait();
                release_tracker.release(10);
            });

            let sync_tracker = Arc::clone(&tracker);
            let sync = thread::spawn(move || {
                barrier.wait();
                sync_tracker.sync_forward(11);
            });

            release.join().expect("release thread should finish");
            sync.join().expect("sync thread should finish");

            assert!(tracker.peek() >= tracker.synced_nonce());
            assert_eq!(tracker.get_and_increment(), 11);
        }
    }

    /// SingleAddressNonceManager: sync never rewinds past in-flight nonces.
    ///
    /// Regression for shared-key contention (uniswapx-filler#544): when an
    /// external tx advances on-chain nonce, sync() should advance the local
    /// tracker. When the local tracker is ahead (we have pre-signed tx not
    /// yet visible on chain), sync() must NOT rewind — that would re-issue
    /// nonces baked into in-flight signed transactions.
    #[test]
    fn test_single_address_sync_no_rewind() {
        let addr = Address::ZERO;
        let manager = SingleAddressNonceManager::new(addr, 100);

        // Allocate two pre-signed nonces (100, 101) — current is now 102
        assert_eq!(manager.get_nonce(), 100);
        assert_eq!(manager.get_nonce(), 101);
        assert_eq!(manager.peek(), 102);

        // Chain says nonce is still 100 (our 100, 101 not yet visible).
        // sync() must not rewind to 100 — that would re-issue 100 next.
        manager.sync(100);
        assert_eq!(manager.peek(), 102); // unchanged

        // Chain advances to 103 (external tx consumed 100/101/102). Sync
        // MUST advance the tracker, otherwise next get_nonce returns 102
        // which is already stale on-chain.
        manager.sync(103);
        assert_eq!(manager.peek(), 103);
        assert_eq!(manager.get_nonce(), 103);
    }

    #[tokio::test]
    async fn test_background_nonce_refresh_advances_local_tracker() {
        let addr = Address::ZERO;
        let manager = SingleAddressNonceManager::new(addr, 100);
        let rpc = MockNonceRpc::new(101);

        assert_eq!(sync_nonce_latest_once(&manager, &rpc).await, 101);
        assert_eq!(manager.peek(), 101);

        assert_eq!(sync_nonce_latest_once(&manager, &rpc).await, 102);
        assert_eq!(manager.peek(), 102);
    }

    /// SingleAddressNonceManager: release via manager API.
    #[test]
    fn test_single_address_release_and_reuse() {
        let addr = Address::ZERO;
        let manager = SingleAddressNonceManager::new(addr, 100);

        let n = manager.get_nonce(); // 100
        assert_eq!(n, 100);

        manager.release(100);
        assert_eq!(manager.peek(), 100);

        // Allocate again — should get 100 back
        assert_eq!(manager.get_nonce(), 100);
    }

    /// NonceManager: release_nonce recycles middle gaps.
    #[test]
    fn test_nonce_manager_release_recycles_middle_gap() {
        let manager = NonceManager::new();
        let addr = Address::ZERO;

        manager.init_nonce(addr, 10);
        let _n0 = manager.get_nonce(addr); // 10
        let _n1 = manager.get_nonce(addr); // 11

        manager.release_nonce(addr, 10);

        assert_eq!(manager.peek_nonce(addr), 12);
        assert_eq!(manager.get_nonce(addr), 10);
    }

    #[test]
    fn test_init_nonce_does_not_rewind_existing_tracker() {
        let manager = NonceManager::new();
        let addr = Address::ZERO;

        manager.init_nonce(addr, 10);
        assert_eq!(manager.get_nonce(addr), 10);
        manager.init_nonce(addr, 5);

        assert_eq!(manager.peek_nonce(addr), 11);
        assert_eq!(manager.get_nonce(addr), 11);
    }

    #[test]
    fn test_concurrent_multi_lane_release_reuses_all_gaps() {
        use std::collections::BTreeSet;
        use std::sync::Mutex as StdMutex;
        use std::thread;

        let tracker = Arc::new(NonceTracker::new(0));
        let released = Arc::new(StdMutex::new(BTreeSet::new()));
        let mut handles = Vec::new();

        for _ in 0..100 {
            let tracker = Arc::clone(&tracker);
            let released = Arc::clone(&released);
            handles.push(thread::spawn(move || {
                let mut reservation = tracker.reserve();
                let nonce = reservation.nonce();
                if nonce % 2 == 0 {
                    reservation.commit();
                } else {
                    reservation.release();
                    released.lock().expect("released set lock").insert(nonce);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("lane thread should finish");
        }

        let expected = released.lock().expect("released set lock").clone();
        let mut reused = BTreeSet::new();
        for _ in 0..expected.len() {
            let mut reservation = tracker.reserve();
            reused.insert(reservation.nonce());
            reservation.commit();
        }

        assert_eq!(reused, expected);
        assert_eq!(tracker.gap_count(), 0);
    }

    #[test]
    fn test_concurrent_nonce_access() {
        use std::thread;

        let tracker = Arc::new(NonceTracker::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let tracker = tracker.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    tracker.get_and_increment();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 10 threads * 100 increments = 1000
        assert_eq!(tracker.peek(), 1000);
    }
}

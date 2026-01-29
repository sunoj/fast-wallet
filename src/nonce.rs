//! Local nonce management for high-performance transaction submission
//!
//! This module provides local nonce management to avoid RPC calls when sending
//! multiple transactions rapidly. It supports:
//! - Lock-free nonce acquisition for single-threaded hot paths
//! - Thread-safe nonce management for concurrent access
//! - Background synchronization with the blockchain
//! - Automatic nonce recovery on failures

use alloy_primitives::Address;
use dashmap::DashMap;
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
    /// Flag indicating if we need to resync
    needs_sync: AtomicU64, // 0 = no, 1 = yes (using u64 for AtomicU64)
}

impl NonceTracker {
    /// Create a new nonce tracker with initial nonce
    pub fn new(initial_nonce: u64) -> Self {
        Self {
            current: AtomicU64::new(initial_nonce),
            pending_count: AtomicU64::new(0),
            synced_nonce: AtomicU64::new(initial_nonce),
            needs_sync: AtomicU64::new(0),
        }
    }

    /// Get the next nonce and increment (lock-free)
    ///
    /// This is the hot path - must be as fast as possible.
    #[inline]
    pub fn get_and_increment(&self) -> u64 {
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        self.current.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current nonce without incrementing
    #[inline]
    pub fn peek(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }

    /// Get the number of pending transactions
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Mark a nonce as confirmed (transaction included in block)
    #[inline]
    pub fn confirm(&self) {
        self.pending_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Mark a nonce as failed and potentially trigger resync
    #[inline]
    pub fn fail(&self, used_nonce: u64) {
        self.pending_count.fetch_sub(1, Ordering::Relaxed);

        // If the failed nonce is less than current, we might have a gap
        let current = self.current.load(Ordering::SeqCst);
        if used_nonce < current {
            self.needs_sync.store(1, Ordering::Release);
        }
    }

    /// Force set the nonce (used during sync)
    #[inline]
    pub fn set(&self, nonce: u64) {
        self.current.store(nonce, Ordering::SeqCst);
        self.synced_nonce.store(nonce, Ordering::Release);
        self.needs_sync.store(0, Ordering::Release);
    }

    /// Reset pending count (used during sync when all pending are cleared)
    #[inline]
    pub fn reset_pending(&self) {
        self.pending_count.store(0, Ordering::Relaxed);
    }

    /// Check if resync is needed
    #[inline]
    pub fn needs_sync(&self) -> bool {
        self.needs_sync.load(Ordering::Acquire) != 0
    }

    /// Get the last synced nonce
    #[inline]
    pub fn synced_nonce(&self) -> u64 {
        self.synced_nonce.load(Ordering::Acquire)
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

    /// Mark a transaction as failed
    #[inline]
    pub fn fail_nonce(&self, address: Address, nonce: u64) {
        if let Some(tracker) = self.trackers.get(&address) {
            tracker.fail(nonce);
        }
    }

    /// Update nonce from chain (call after fetching from RPC)
    pub fn sync_nonce(&self, address: Address, chain_nonce: u64) {
        let tracker = self.get_tracker(address);
        let current = tracker.peek();

        // Only update if chain nonce is higher (transactions confirmed)
        // or if we need a resync due to failures
        if chain_nonce > current || tracker.needs_sync() {
            tracker.set(chain_nonce);
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
    tracker: NonceTracker,
    address: Address,
}

impl SingleAddressNonceManager {
    /// Create a new single-address nonce manager
    pub fn new(address: Address, initial_nonce: u64) -> Self {
        Self {
            tracker: NonceTracker::new(initial_nonce),
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

    /// Fail transaction
    #[inline]
    pub fn fail(&self, nonce: u64) {
        self.tracker.fail(nonce);
    }

    /// Sync with chain nonce
    #[inline]
    pub fn sync(&self, chain_nonce: u64) {
        self.tracker.set(chain_nonce);
    }

    /// Check if sync needed
    #[inline]
    pub fn needs_sync(&self) -> bool {
        self.tracker.needs_sync()
    }

    /// Get pending count
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.tracker.pending_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        tracker.fail(0);
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

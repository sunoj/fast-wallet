//! In-flight nonce ledger for signed and broadcast transactions.
//! Exports snapshots used by nonce health checks and recovery decisions.
//! Deps: alloy hashes, parking_lot Mutex, and std collections/time.

use alloy::primitives::{B256, U256};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const MAX_TRACKED_NONCES: usize = 256;

/// Lifecycle state for the latest known activity on a nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightNonceStatus {
    Signed,
    BroadcastAccepted,
    MempoolSeen,
    RebroadcastAccepted,
    Committed,
    Released,
    Mined,
}

/// Read-only view of one nonce's in-flight state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InflightNonceSnapshot {
    pub nonce: u64,
    pub tx_hashes: Vec<B256>,
    pub status: InflightNonceStatus,
    pub age: Duration,
    pub accepted_broadcasts: u32,
    pub release_reason: Option<String>,
    /// Highest `max_fee_per_gas` broadcast for this nonce (EIP-1559 only). Lets a
    /// replacement bump relative to the stranded tx's own fee, not just network price.
    pub max_fee_seen: Option<U256>,
    /// Highest `max_priority_fee_per_gas` broadcast for this nonce (EIP-1559 only).
    pub max_priority_seen: Option<U256>,
}

#[derive(Debug)]
struct InflightNonceRecord {
    nonce: u64,
    tx_hashes: BTreeSet<B256>,
    status: InflightNonceStatus,
    first_seen: Instant,
    accepted_broadcasts: u32,
    release_reason: Option<String>,
    max_fee_seen: Option<U256>,
    max_priority_seen: Option<U256>,
}

/// Keep the larger of an existing tracked fee and a newly observed one.
fn merge_max(slot: &mut Option<U256>, observed: Option<U256>) {
    if let Some(value) = observed {
        *slot = Some(slot.map_or(value, |cur| cur.max(value)));
    }
}

impl InflightNonceRecord {
    fn new(
        nonce: u64,
        tx_hash: B256,
        max_fee: Option<U256>,
        max_priority: Option<U256>,
        now: Instant,
    ) -> Self {
        let mut tx_hashes = BTreeSet::new();
        tx_hashes.insert(tx_hash);
        Self {
            nonce,
            tx_hashes,
            status: InflightNonceStatus::Signed,
            first_seen: now,
            accepted_broadcasts: 0,
            release_reason: None,
            max_fee_seen: max_fee,
            max_priority_seen: max_priority,
        }
    }

    fn snapshot(&self, now: Instant) -> InflightNonceSnapshot {
        InflightNonceSnapshot {
            nonce: self.nonce,
            tx_hashes: self.tx_hashes.iter().copied().collect(),
            status: self.status,
            age: now.saturating_duration_since(self.first_seen),
            accepted_broadcasts: self.accepted_broadcasts,
            release_reason: self.release_reason.clone(),
            max_fee_seen: self.max_fee_seen,
            max_priority_seen: self.max_priority_seen,
        }
    }
}

/// Small bounded ledger keyed by nonce.
#[derive(Debug, Default)]
pub struct InflightNonceLedger {
    records: Mutex<BTreeMap<u64, InflightNonceRecord>>,
}

impl InflightNonceLedger {
    pub fn record_signed(
        &self,
        nonce: u64,
        tx_hash: B256,
        max_fee: Option<U256>,
        max_priority: Option<U256>,
    ) {
        let mut records = self.records.lock();
        let now = Instant::now();
        records
            .entry(nonce)
            .and_modify(|record| {
                record.tx_hashes.insert(tx_hash);
                merge_max(&mut record.max_fee_seen, max_fee);
                merge_max(&mut record.max_priority_seen, max_priority);
                if record.status == InflightNonceStatus::Released {
                    // A released nonce handed back out starts a NEW attempt, so
                    // its age restarts here. Age answers "how long has this
                    // nonce been unresolved", and callers gate a rewind on it —
                    // inheriting the previous attempt's age would let a
                    // freshly broadcast tx look stranded on its first check.
                    // Re-signing a nonce that is still Signed/BroadcastAccepted
                    // is a replacement for a tx that IS still stranded, so that
                    // case deliberately keeps the original clock.
                    record.status = InflightNonceStatus::Signed;
                    record.release_reason = None;
                    record.first_seen = now;
                    record.accepted_broadcasts = 0;
                }
            })
            .or_insert_with(|| {
                InflightNonceRecord::new(nonce, tx_hash, max_fee, max_priority, now)
            });
        prune_old_records(&mut records);
    }

    pub fn mark_broadcast_accepted(&self, nonce: u64, tx_hash: B256) {
        self.update(nonce, tx_hash, InflightNonceStatus::BroadcastAccepted, None);
    }

    pub fn mark_mempool_seen(&self, nonce: u64, tx_hash: B256) {
        self.update(nonce, tx_hash, InflightNonceStatus::MempoolSeen, None);
    }

    pub fn mark_rebroadcast_accepted(&self, nonce: u64, tx_hash: B256) {
        self.update(
            nonce,
            tx_hash,
            InflightNonceStatus::RebroadcastAccepted,
            None,
        );
    }

    pub fn mark_released(&self, nonce: u64, reason: impl Into<String>) {
        let reason = reason.into();
        let mut records = self.records.lock();
        let now = Instant::now();
        records
            .entry(nonce)
            .and_modify(|record| {
                record.status = InflightNonceStatus::Released;
                record.release_reason = Some(reason.clone());
            })
            .or_insert_with(|| {
                let mut record = InflightNonceRecord::new(nonce, B256::ZERO, None, None, now);
                record.tx_hashes.clear();
                record.status = InflightNonceStatus::Released;
                record.release_reason = Some(reason);
                record
            });
    }

    pub fn mark_committed(&self, nonce: u64) {
        let mut records = self.records.lock();
        let now = Instant::now();
        records
            .entry(nonce)
            .and_modify(|record| {
                record.status = InflightNonceStatus::Committed;
                record.release_reason = None;
            })
            .or_insert_with(|| {
                let mut record = InflightNonceRecord::new(nonce, B256::ZERO, None, None, now);
                record.tx_hashes.clear();
                record.status = InflightNonceStatus::Committed;
                record
            });
    }

    pub fn mark_mined(&self, nonce: u64, tx_hash: B256) {
        self.update(nonce, tx_hash, InflightNonceStatus::Mined, None);
    }

    pub fn prune_consumed(&self, chain_next: u64) {
        self.records.lock().retain(|nonce, _| *nonce >= chain_next);
    }

    pub fn lowest_unresolved_at_or_above(&self, chain_next: u64) -> Option<InflightNonceSnapshot> {
        let records = self.records.lock();
        let now = Instant::now();
        records
            .range(chain_next..)
            .find(|(_, record)| is_unresolved(record.status))
            .map(|(_, record)| record.snapshot(now))
    }

    pub fn unresolved_count(&self) -> usize {
        self.records
            .lock()
            .values()
            .filter(|record| is_unresolved(record.status))
            .count()
    }

    fn update(
        &self,
        nonce: u64,
        tx_hash: B256,
        status: InflightNonceStatus,
        release_reason: Option<String>,
    ) {
        let mut records = self.records.lock();
        let now = Instant::now();
        records
            .entry(nonce)
            .and_modify(|record| {
                record.tx_hashes.insert(tx_hash);
                record.status = status;
                record.accepted_broadcasts += u32::from(is_broadcast_acceptance(status));
                record.release_reason = release_reason.clone();
            })
            .or_insert_with(|| {
                let mut record = InflightNonceRecord::new(nonce, tx_hash, None, None, now);
                record.status = status;
                record.accepted_broadcasts = u32::from(is_broadcast_acceptance(status));
                record.release_reason = release_reason;
                record
            });
        prune_old_records(&mut records);
    }
}

fn is_unresolved(status: InflightNonceStatus) -> bool {
    !matches!(
        status,
        InflightNonceStatus::Released | InflightNonceStatus::Mined
    )
}

fn is_broadcast_acceptance(status: InflightNonceStatus) -> bool {
    matches!(
        status,
        InflightNonceStatus::BroadcastAccepted | InflightNonceStatus::RebroadcastAccepted
    )
}

fn prune_old_records(records: &mut BTreeMap<u64, InflightNonceRecord>) {
    while records.len() > MAX_TRACKED_NONCES {
        let Some(first_nonce) = records.keys().next().copied() else {
            return;
        };
        records.remove(&first_nonce);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_tracks_lowest_unresolved_nonce_with_replacements() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(10, B256::repeat_byte(1), None, None);
        ledger.mark_broadcast_accepted(10, B256::repeat_byte(1));
        ledger.record_signed(10, B256::repeat_byte(2), None, None);
        ledger.record_signed(11, B256::repeat_byte(3), None, None);

        let low = ledger.lowest_unresolved_at_or_above(10).unwrap();
        assert_eq!(low.nonce, 10);
        assert_eq!(low.tx_hashes.len(), 2);
        assert_eq!(low.status, InflightNonceStatus::BroadcastAccepted);
        assert_eq!(ledger.unresolved_count(), 2);

        ledger.mark_released(10, "replacement_abandoned");
        let low = ledger.lowest_unresolved_at_or_above(10).unwrap();
        assert_eq!(low.nonce, 11);
        assert_eq!(ledger.unresolved_count(), 1);
    }

    #[test]
    fn released_nonce_handed_out_again_restarts_its_age() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(30, B256::repeat_byte(6), None, None);
        ledger.mark_broadcast_accepted(30, B256::repeat_byte(6));
        std::thread::sleep(Duration::from_millis(25));
        assert!(ledger.lowest_unresolved_at_or_above(30).unwrap().age >= Duration::from_millis(25));

        ledger.mark_released(30, "candidate_dropped");
        ledger.record_signed(30, B256::repeat_byte(7), None, None);

        // Callers read this age as proof the transaction was dropped and rewind
        // on it. A new attempt at a recycled nonce inheriting the previous
        // attempt's clock would look stranded on its very first health check.
        let fresh = ledger.lowest_unresolved_at_or_above(30).unwrap();
        assert!(fresh.age < Duration::from_millis(25), "age must restart");
        assert_eq!(fresh.status, InflightNonceStatus::Signed);
    }

    #[test]
    fn replacement_at_a_still_unresolved_nonce_keeps_the_original_clock() {
        // The opposite case: re-signing a nonce that was never released is a
        // replacement for a tx that IS still stranded. Restarting the clock
        // there would let a bump loop keep the entry permanently young and the
        // rewind gate would never fire.
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(40, B256::repeat_byte(8), None, None);
        ledger.mark_broadcast_accepted(40, B256::repeat_byte(8));
        std::thread::sleep(Duration::from_millis(25));

        ledger.record_signed(40, B256::repeat_byte(9), None, None);

        let entry = ledger.lowest_unresolved_at_or_above(40).unwrap();
        assert!(entry.age >= Duration::from_millis(25), "clock must persist");
        assert_eq!(entry.status, InflightNonceStatus::BroadcastAccepted);
    }

    #[test]
    fn ledger_prunes_consumed_nonces() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(20, B256::repeat_byte(4), None, None);
        ledger.record_signed(21, B256::repeat_byte(5), None, None);

        ledger.prune_consumed(21);

        let low = ledger.lowest_unresolved_at_or_above(20).unwrap();
        assert_eq!(low.nonce, 21);
        assert_eq!(ledger.unresolved_count(), 1);
    }

    #[test]
    fn ledger_retains_highest_seen_fees_per_nonce() {
        let ledger = InflightNonceLedger::default();
        // First broadcast at a high fee, then a replacement at a lower fee: the
        // ledger must keep the HIGHEST so a cancel can still out-bid the stranded tx.
        ledger.record_signed(
            5,
            B256::repeat_byte(1),
            Some(U256::from(100u64)),
            Some(U256::from(10u64)),
        );
        ledger.record_signed(
            5,
            B256::repeat_byte(2),
            Some(U256::from(50u64)),
            Some(U256::from(5u64)),
        );
        let snap = ledger.lowest_unresolved_at_or_above(5).unwrap();
        assert_eq!(snap.max_fee_seen, Some(U256::from(100u64)));
        assert_eq!(snap.max_priority_seen, Some(U256::from(10u64)));
    }

    #[test]
    fn committed_nonce_remains_unresolved_until_mined_or_released() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(30, B256::repeat_byte(6), None, None);

        ledger.mark_committed(30);

        let low = ledger.lowest_unresolved_at_or_above(30).unwrap();
        assert_eq!(low.nonce, 30);
        assert_eq!(low.status, InflightNonceStatus::Committed);
        assert_eq!(ledger.unresolved_count(), 1);

        ledger.mark_mined(30, B256::repeat_byte(6));

        assert!(ledger.lowest_unresolved_at_or_above(30).is_none());
        assert_eq!(ledger.unresolved_count(), 0);
    }
}

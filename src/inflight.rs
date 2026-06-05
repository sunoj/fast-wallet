//! In-flight nonce ledger for signed and broadcast transactions.
//! Exports snapshots used by nonce health checks and recovery decisions.
//! Deps: alloy hashes, parking_lot Mutex, and std collections/time.

use alloy::primitives::B256;
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
}

#[derive(Debug)]
struct InflightNonceRecord {
    nonce: u64,
    tx_hashes: BTreeSet<B256>,
    status: InflightNonceStatus,
    first_seen: Instant,
    accepted_broadcasts: u32,
    release_reason: Option<String>,
}

impl InflightNonceRecord {
    fn new(nonce: u64, tx_hash: B256, now: Instant) -> Self {
        let mut tx_hashes = BTreeSet::new();
        tx_hashes.insert(tx_hash);
        Self {
            nonce,
            tx_hashes,
            status: InflightNonceStatus::Signed,
            first_seen: now,
            accepted_broadcasts: 0,
            release_reason: None,
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
        }
    }
}

/// Small bounded ledger keyed by nonce.
#[derive(Debug, Default)]
pub struct InflightNonceLedger {
    records: Mutex<BTreeMap<u64, InflightNonceRecord>>,
}

impl InflightNonceLedger {
    pub fn record_signed(&self, nonce: u64, tx_hash: B256) {
        let mut records = self.records.lock();
        let now = Instant::now();
        records
            .entry(nonce)
            .and_modify(|record| {
                record.tx_hashes.insert(tx_hash);
                if record.status == InflightNonceStatus::Released {
                    record.status = InflightNonceStatus::Signed;
                    record.release_reason = None;
                }
            })
            .or_insert_with(|| InflightNonceRecord::new(nonce, tx_hash, now));
        prune_old_records(&mut records);
    }

    pub fn mark_broadcast_accepted(&self, nonce: u64, tx_hash: B256) {
        self.update(nonce, tx_hash, InflightNonceStatus::BroadcastAccepted, None);
    }

    pub fn mark_mempool_seen(&self, nonce: u64, tx_hash: B256) {
        self.update(nonce, tx_hash, InflightNonceStatus::MempoolSeen, None);
    }

    pub fn mark_rebroadcast_accepted(&self, nonce: u64, tx_hash: B256) {
        self.update(nonce, tx_hash, InflightNonceStatus::RebroadcastAccepted, None);
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
                let mut record = InflightNonceRecord::new(nonce, B256::ZERO, now);
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
                let mut record = InflightNonceRecord::new(nonce, B256::ZERO, now);
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
                let mut record = InflightNonceRecord::new(nonce, tx_hash, now);
                record.status = status;
                record.accepted_broadcasts = u32::from(is_broadcast_acceptance(status));
                record.release_reason = release_reason;
                record
            });
        prune_old_records(&mut records);
    }
}

fn is_unresolved(status: InflightNonceStatus) -> bool {
    !matches!(status, InflightNonceStatus::Released | InflightNonceStatus::Mined)
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
        ledger.record_signed(10, B256::repeat_byte(1));
        ledger.mark_broadcast_accepted(10, B256::repeat_byte(1));
        ledger.record_signed(10, B256::repeat_byte(2));
        ledger.record_signed(11, B256::repeat_byte(3));

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
    fn ledger_prunes_consumed_nonces() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(20, B256::repeat_byte(4));
        ledger.record_signed(21, B256::repeat_byte(5));

        ledger.prune_consumed(21);

        let low = ledger.lowest_unresolved_at_or_above(20).unwrap();
        assert_eq!(low.nonce, 21);
        assert_eq!(ledger.unresolved_count(), 1);
    }

    #[test]
    fn committed_nonce_remains_unresolved_until_mined_or_released() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(30, B256::repeat_byte(6));

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

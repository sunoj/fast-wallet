//! Ledger gate for re-sending already-signed bytes.
//! Exports ResendRefusal and InflightNonceLedger::resend_eligible; pure, reads the ledger only.
//! Used by FastWallet::resend_signed before any network call.

use super::{is_broadcast_acceptance, is_unresolved, InflightNonceLedger, InflightNonceStatus};
use alloy::primitives::B256;
use std::time::Duration;

/// Why `FastWallet::resend_signed` refused to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResendRefusal {
    /// The ledger has no record for this nonce.
    NotTracked,
    /// The nonce is mined or released: the bytes can no longer land, or the nonce may be reused.
    Resolved,
    /// This hash was not signed at this nonce since the nonce was last handed out.
    UnknownHash,
    /// An unresolved nonce at or below this one is at least `max_age` old.
    TooOld,
    /// The wallet's allocator holds this nonce as free: released, dropped or rewound and not
    /// handed out again, or never handed out (`sign_alternatives` signs at the next nonce).
    NotAllocated,
}

impl InflightNonceLedger {
    /// Whether `hash` at `nonce` may be broadcast again.
    ///
    /// The age check covers every unresolved nonce at or below `nonce`, not only this one: a
    /// caller's stall recovery keys on the age of the lowest unresolved nonce and, when it
    /// rewinds, hands out every nonce from there up, this one included.
    pub fn resend_eligible(
        &self,
        nonce: u64,
        hash: B256,
        max_age: Duration,
    ) -> Result<(), ResendRefusal> {
        let records = self.records.lock();
        let record = records.get(&nonce).ok_or(ResendRefusal::NotTracked)?;
        if !is_unresolved(record.status) {
            return Err(ResendRefusal::Resolved);
        }
        if !record.current_hashes.contains(&hash) {
            return Err(ResendRefusal::UnknownHash);
        }
        let now = self.ledger_now();
        let too_old = records.range(..=nonce).any(|(_, record)| {
            is_unresolved(record.status)
                && now.saturating_duration_since(record.first_seen) >= max_age
        });
        if too_old {
            return Err(ResendRefusal::TooOld);
        }
        Ok(())
    }

    /// The wallet handed `nonce` out: hashes signed under an earlier allocation stop being
    /// current. Status, age and the hash history are left as they are for the other readers.
    pub(crate) fn start_allocation(&self, nonce: u64) {
        if let Some(record) = self.records.lock().get_mut(&nonce) {
            record.current_hashes.clear();
        }
    }

    /// Record an accepted copy, but only on the record the copy was cleared against: a receipt,
    /// release, reissue or prune during the send leaves the ledger as it is.
    pub(crate) fn mark_resend_accepted(&self, nonce: u64, hash: B256) {
        let mut records = self.records.lock();
        let Some(record) = records.get_mut(&nonce) else {
            return;
        };
        if is_unresolved(record.status) && record.current_hashes.contains(&hash) {
            let status = InflightNonceStatus::RebroadcastAccepted;
            record.status = status;
            record.accepted_broadcasts += u32::from(is_broadcast_acceptance(status));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AGE: Duration = Duration::from_secs(30);

    fn hash(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    #[test]
    fn a_young_unresolved_tracked_hash_is_eligible() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(10, hash(1), None, None);
        ledger.mark_broadcast_accepted(10, hash(1));
        assert_eq!(ledger.resend_eligible(10, hash(1), AGE), Ok(()));
    }

    #[test]
    fn untracked_unknown_and_resolved_are_refused() {
        let ledger = InflightNonceLedger::default();
        assert_eq!(
            ledger.resend_eligible(10, hash(1), AGE),
            Err(ResendRefusal::NotTracked)
        );
        ledger.record_signed(10, hash(1), None, None);
        assert_eq!(
            ledger.resend_eligible(10, hash(2), AGE),
            Err(ResendRefusal::UnknownHash)
        );
        ledger.mark_mined(10, hash(1));
        assert_eq!(
            ledger.resend_eligible(10, hash(1), AGE),
            Err(ResendRefusal::Resolved)
        );
        ledger.record_signed(11, hash(3), None, None);
        ledger.mark_released(11, "test");
        assert_eq!(
            ledger.resend_eligible(11, hash(3), AGE),
            Err(ResendRefusal::Resolved)
        );
    }

    #[test]
    fn an_old_nonce_at_or_below_refuses_and_one_above_does_not() {
        let ledger = InflightNonceLedger::default();
        for (nonce, byte) in [(9, 1), (10, 2), (11, 3)] {
            ledger.record_signed(nonce, hash(byte), None, None);
        }
        ledger.backdate_first_seen_for_tests(11, AGE);
        assert_eq!(
            ledger.resend_eligible(10, hash(2), AGE),
            Ok(()),
            "a higher nonce is not ours to wait on"
        );
        ledger.backdate_first_seen_for_tests(9, AGE);
        assert_eq!(
            ledger.resend_eligible(10, hash(2), AGE),
            Err(ResendRefusal::TooOld),
            "the head of the line is old"
        );
        ledger.mark_mined(9, hash(1));
        assert_eq!(ledger.resend_eligible(10, hash(2), AGE), Ok(()));
        ledger.backdate_first_seen_for_tests(10, AGE);
        assert_eq!(
            ledger.resend_eligible(10, hash(2), AGE),
            Err(ResendRefusal::TooOld)
        );
    }

    #[test]
    fn a_reissued_nonce_refuses_the_bytes_of_its_previous_owner() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(10, hash(1), None, None);
        ledger.mark_released(10, "definitive rejection");
        ledger.record_signed(10, hash(2), None, None);
        assert_eq!(
            ledger.resend_eligible(10, hash(1), AGE),
            Err(ResendRefusal::UnknownHash)
        );
        assert_eq!(ledger.resend_eligible(10, hash(2), AGE), Ok(()));
        ledger.record_signed(10, hash(3), None, None);
        assert_eq!(
            ledger.resend_eligible(10, hash(2), AGE),
            Ok(()),
            "an unreleased re-sign keeps both"
        );
    }

    #[test]
    fn an_accepted_copy_does_not_revive_a_resolved_or_missing_record() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(10, hash(1), None, None);
        ledger.mark_mined(10, hash(1));
        ledger.mark_resend_accepted(10, hash(1));
        assert_eq!(
            ledger.resend_eligible(10, hash(1), AGE),
            Err(ResendRefusal::Resolved)
        );
        ledger.mark_resend_accepted(11, hash(2));
        assert_eq!(
            ledger.resend_eligible(11, hash(2), AGE),
            Err(ResendRefusal::NotTracked)
        );
        ledger.record_signed(12, hash(3), None, None);
        ledger.mark_resend_accepted(12, hash(3));
        assert_eq!(ledger.resend_eligible(12, hash(3), AGE), Ok(()));
    }

    #[test]
    fn a_new_allocation_retires_the_old_hashes_and_nothing_else() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(10, hash(1), None, None);
        ledger.mark_broadcast_accepted(10, hash(1));
        ledger.backdate_first_seen_for_tests(10, AGE / 2);
        ledger.start_allocation(10);
        assert_eq!(
            ledger.resend_eligible(10, hash(1), AGE),
            Err(ResendRefusal::UnknownHash)
        );
        let snapshot = ledger
            .lowest_unresolved_at_or_above(10)
            .expect("still tracked");
        assert_eq!(snapshot.tx_hashes, vec![hash(1)], "history kept");
        assert_eq!(snapshot.status, InflightNonceStatus::BroadcastAccepted);
        assert!(snapshot.age >= AGE / 2, "age kept");

        ledger.record_signed(10, hash(2), None, None);
        ledger.record_signed(10, hash(3), None, None);
        assert_eq!(ledger.resend_eligible(10, hash(2), AGE), Ok(()));
        assert_eq!(
            ledger.resend_eligible(10, hash(3), AGE),
            Ok(()),
            "signatures within one allocation are all current"
        );

        ledger.start_allocation(11);
        assert_eq!(
            ledger.resend_eligible(11, hash(4), AGE),
            Err(ResendRefusal::NotTracked),
            "no record is created"
        );
    }

    #[test]
    fn a_re_sign_keeps_the_original_age() {
        let ledger = InflightNonceLedger::default();
        ledger.record_signed(10, hash(1), None, None);
        ledger.backdate_first_seen_for_tests(10, AGE);
        ledger.record_signed(10, hash(2), None, None);
        assert_eq!(
            ledger.resend_eligible(10, hash(2), AGE),
            Err(ResendRefusal::TooOld)
        );
    }
}

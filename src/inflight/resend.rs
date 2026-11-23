//! Ledger gate for re-sending already-signed bytes.
//! Exports ResendRefusal and InflightNonceLedger::resend_eligible; pure, reads the ledger only.
//! Used by FastWallet::resend_signed before any network call.

use super::{is_unresolved, InflightNonceLedger};
use alloy::primitives::B256;
use std::time::Duration;

/// Why `FastWallet::resend_signed` refused to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResendRefusal {
    /// The ledger has no record for this nonce.
    NotTracked,
    /// The nonce is mined or released: the bytes can no longer land, or the nonce may be reused.
    Resolved,
    /// The ledger never saw this hash at this nonce.
    UnknownHash,
    /// An unresolved nonce at or below this one is at least `max_age` old.
    TooOld,
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
        if !record.tx_hashes.contains(&hash) {
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

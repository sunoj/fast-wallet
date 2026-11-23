//! Re-send of a transaction's exact signed bytes, gated on the nonce allocator and the ledger.
//! Exports ResendOutcome and FastWallet::resend_signed, plus the two hand-out helpers that start
//! a nonce's allocation in the ledger. resend_signed never reserves, releases or resyncs a nonce.

use super::FastWallet;
use crate::error::WalletResult;
use crate::inflight::ResendRefusal;
use crate::nonce::ReservedNonce;
use crate::rpc::SendResult;
use crate::transaction::Transaction;
use std::time::Duration;

/// What `FastWallet::resend_signed` did.
#[derive(Debug, Clone)]
pub enum ResendOutcome {
    /// At least one endpoint accepted the copy.
    Sent(SendResult),
    /// The ledger gate refused; nothing was sent.
    Refused(ResendRefusal),
}

impl FastWallet {
    /// Broadcast `tx`'s exact signed bytes again, for a send whose first copy may have been
    /// dropped from a sequencer queue. Identical bytes carry one hash, so at most one copy is
    /// included.
    ///
    /// Refused without a network call unless the ledger holds `tx`'s nonce, unresolved, with
    /// this hash, every unresolved nonce at or below it is younger than `max_age`, and the
    /// wallet's allocator still holds the nonce as handed out. Pick `max_age` so a copy and its
    /// send timeout end before the caller's stall recovery may rewind or cancel by unresolved
    /// age. Only hashes signed since the wallet last handed the nonce out qualify, so a
    /// released, dropped or rewound nonce never carries its previous owner's bytes, before or
    /// after it is handed out again. Bytes signed at a nonce the wallet never handed out, as
    /// `sign_alternatives` does, are refused.
    /// Unlike `send_signed`, no outcome changes the nonce state: recycling the nonce of a
    /// rejected copy would hand out a nonce whose first copy may still be queued. An accepted
    /// copy is recorded as a rebroadcast only if its record is still the one it was cleared on.
    pub async fn resend_signed(
        &self,
        tx: &Transaction,
        max_age: Duration,
    ) -> WalletResult<ResendOutcome> {
        if let Err(refusal) = self
            .inflight_nonces
            .resend_eligible(tx.nonce(), tx.hash(), max_age)
        {
            return Ok(ResendOutcome::Refused(refusal));
        }
        if !self.nonce_manager.is_allocated(tx.nonce()) {
            return Ok(ResendOutcome::Refused(ResendRefusal::NotAllocated));
        }
        let result = self.broadcast_signed_result(tx, 0.0).await?;
        self.inflight_nonces
            .mark_resend_accepted(tx.nonce(), tx.hash());
        Ok(ResendOutcome::Sent(result))
    }

    /// Hand out the next nonce. Every automatic hand-out goes through here or
    /// `reserve_allocation`, so the ledger retires hashes signed under the nonce's last
    /// allocation.
    pub(super) fn allocate_nonce(&self) -> u64 {
        let nonce = self.nonce_manager.get_nonce();
        self.inflight_nonces.start_allocation(nonce);
        nonce
    }

    /// Reserve the next nonce; see `allocate_nonce`.
    pub(super) fn reserve_allocation(&self) -> ReservedNonce {
        let reservation = self.nonce_manager.reserve();
        self.inflight_nonces.start_allocation(reservation.nonce());
        reservation
    }
}

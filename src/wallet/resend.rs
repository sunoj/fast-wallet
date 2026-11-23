//! Re-send of a transaction's exact signed bytes, gated on the in-flight ledger.
//! Exports ResendOutcome and FastWallet::resend_signed; depends on the ledger gate and the
//! wallet's own broadcast path. Never reserves, releases or resyncs a nonce.

use super::FastWallet;
use crate::error::WalletResult;
use crate::inflight::ResendRefusal;
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
    /// this hash, and every unresolved nonce at or below it is younger than `max_age`. Pick
    /// `max_age` so a copy and its send timeout end before the caller's stall recovery may
    /// rewind or cancel by unresolved age. Only hashes signed since the nonce was last handed
    /// out qualify, so a released and reissued nonce never carries its previous owner's bytes.
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
        let result = self.broadcast_signed_result(tx, 0.0).await?;
        self.inflight_nonces
            .mark_resend_accepted(tx.nonce(), tx.hash());
        Ok(ResendOutcome::Sent(result))
    }
}

//! resend_signed against the nonce allocator: bytes whose nonce was released, dropped,
//! cancelled or rewound are refused while the nonce is free and after it is handed out again.
//! Bytes under a live reservation, including two signatures under one reservation, are sent.

#[path = "support/presigned_rpc.rs"]
mod rpc;

use alloy::primitives::U256;
use fast_wallet::{FastWallet, ResendOutcome, ResendRefusal, Transaction};
use rpc::{request, send, wallet, Reply, Rpc};
use std::time::Duration;

const MAX_AGE: Duration = Duration::from_secs(30);

/// How the first allocation of the nonce ended.
enum End {
    Release,
    Drop,
    CancelPreheat,
    Rewind,
}

/// Sign bytes at the next nonce, then end that allocation without telling the ledger.
async fn stale_bytes(wallet: &FastWallet, end: End) -> Transaction {
    match end {
        End::Release | End::Drop => {
            let mut reservation = wallet.reserve_nonce();
            let tx = wallet
                .sign_with_nonce(request(), reservation.nonce())
                .unwrap();
            if matches!(end, End::Release) {
                assert!(reservation.release());
            }
            tx
        }
        End::CancelPreheat => {
            let ctx = wallet.preheat(false).await.unwrap();
            let tx = wallet.sign_with_preheat(&ctx, request()).unwrap();
            wallet.cancel_preheat(ctx);
            tx
        }
        End::Rewind => {
            let tx = wallet.sign(request()).unwrap();
            assert!(wallet.recover_stalled(tx.nonce()));
            tx
        }
    }
}

/// A refusal is decided before any network call; reaching the loopback RPC times out here.
async fn refusal(wallet: &FastWallet, tx: &Transaction) -> ResendRefusal {
    let outcome = tokio::time::timeout(
        Duration::from_millis(500),
        wallet.resend_signed(tx, MAX_AGE),
    )
    .await
    .expect("refused without a network call")
    .unwrap();
    match outcome {
        ResendOutcome::Refused(refusal) => refusal,
        ResendOutcome::Sent(_) => panic!("sent"),
    }
}

async fn stale_bytes_stay_refused(end: End) {
    let rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    let stale = stale_bytes(&wallet, end).await;
    assert_eq!(
        refusal(&wallet, &stale).await,
        ResendRefusal::NotAllocated,
        "the nonce is free"
    );
    let next = wallet.sign(request().value(U256::from(1u64))).unwrap();
    assert_eq!(next.nonce(), stale.nonce(), "the nonce is handed out again");
    assert_eq!(
        refusal(&wallet, &stale).await,
        ResendRefusal::UnknownHash,
        "the new allocation does not carry the old bytes"
    );
}

#[tokio::test]
async fn bytes_of_a_released_reservation_are_refused() {
    stale_bytes_stay_refused(End::Release).await;
}

#[tokio::test]
async fn bytes_of_a_dropped_reservation_are_refused() {
    stale_bytes_stay_refused(End::Drop).await;
}

#[tokio::test]
async fn bytes_of_a_cancelled_preheat_are_refused() {
    stale_bytes_stay_refused(End::CancelPreheat).await;
}

#[tokio::test]
async fn bytes_of_a_rewound_nonce_are_refused() {
    stale_bytes_stay_refused(End::Rewind).await;
}

#[tokio::test]
async fn bytes_under_a_live_reservation_are_sent_and_two_signatures_share_it() {
    let mut rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    // The shape the filler leaves it in: reserved, marked broadcasting at once, signed, sent.
    let mut reservation = wallet.reserve_nonce();
    assert!(reservation.mark_broadcasting());
    let first = wallet
        .sign_with_nonce(request(), reservation.nonce())
        .unwrap();
    let second = wallet
        .sign_with_nonce(request().value(U256::from(1u64)), reservation.nonce())
        .unwrap();
    let sending = send(&wallet, &first, true);
    assert!(rpc.next_send().await.send(Reply::Accept).is_ok());
    assert!(sending.await.unwrap().is_ok());

    for tx in [first, second] {
        let copy = tokio::spawn({
            let (wallet, tx) = (wallet.clone(), tx.clone());
            async move { wallet.resend_signed(&tx, MAX_AGE).await }
        });
        assert!(rpc.next_send().await.send(Reply::Accept).is_ok());
        match copy.await.unwrap().unwrap() {
            ResendOutcome::Sent(result) => assert_eq!(result.tx_hash, format!("{}", tx.hash())),
            ResendOutcome::Refused(refusal) => panic!("refused: {refusal:?}"),
        }
    }
    assert!(reservation.commit());
}

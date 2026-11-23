//! resend_signed through real loopback RPC: identical bytes, ledger gate, no nonce recycling.
//! A definitive rejection of a copy keeps the nonce, where the same rejection of a first send
//! recycles it (tests/presigned_recycle.rs).

#[path = "support/presigned_rpc.rs"]
mod rpc;

use fast_wallet::{ResendOutcome, ResendRefusal};
use rpc::{request, send, wallet, Reply, Rpc, DEFINITIVE, NONCE};
use std::time::Duration;

const MAX_AGE: Duration = Duration::from_secs(30);

#[tokio::test]
async fn a_rejected_copy_keeps_the_nonce_and_an_accepted_one_is_the_same_hash() {
    let mut rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    let tx = wallet.sign(request()).unwrap();
    let sending = send(&wallet, &tx, true);
    assert!(rpc.next_send().await.send(Reply::Accept).is_ok());
    assert!(sending.await.unwrap().is_ok());

    let copy = tokio::spawn({
        let (wallet, tx) = (wallet.clone(), tx.clone());
        async move { wallet.resend_signed(&tx, MAX_AGE).await }
    });
    assert!(rpc.next_send().await.send(Reply::Error(DEFINITIVE)).is_ok());
    assert!(copy.await.unwrap().is_err());
    assert_eq!(wallet.inflight_count(), 1, "the nonce stays in flight");
    assert_eq!(
        wallet.sign(request()).unwrap().nonce(),
        NONCE + 1,
        "and is not handed out again"
    );

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

#[tokio::test]
async fn a_released_or_foreign_transaction_is_refused_without_sending() {
    let mut rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    let tx = wallet.sign(request()).unwrap();
    let sending = send(&wallet, &tx, false);
    assert!(rpc.next_send().await.send(Reply::Error(DEFINITIVE)).is_ok());
    assert!(sending.await.unwrap().is_err());
    assert!(matches!(
        wallet.resend_signed(&tx, MAX_AGE).await.unwrap(),
        ResendOutcome::Refused(ResendRefusal::Resolved)
    ));

    let other = rpc::wallet(&rpc, vec![]);
    let foreign = other.sign(request()).unwrap();
    let fresh = rpc::wallet(&rpc, vec![]);
    assert!(matches!(
        fresh.resend_signed(&foreign, MAX_AGE).await.unwrap(),
        ResendOutcome::Refused(ResendRefusal::NotTracked)
    ));
}

#[tokio::test]
async fn the_bytes_of_a_released_nonce_are_refused_after_it_is_reissued() {
    let mut rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    let first = wallet.sign(request()).unwrap();
    let sending = send(&wallet, &first, true);
    assert!(rpc.next_send().await.send(Reply::Error(DEFINITIVE)).is_ok());
    assert!(sending.await.unwrap().is_err());
    let second = wallet
        .sign(request().value(alloy::primitives::U256::from(1u64)))
        .unwrap();
    assert_eq!(
        second.nonce(),
        first.nonce(),
        "the released nonce is handed out again"
    );
    assert!(matches!(
        wallet.resend_signed(&first, MAX_AGE).await.unwrap(),
        ResendOutcome::Refused(ResendRefusal::UnknownHash)
    ));
}

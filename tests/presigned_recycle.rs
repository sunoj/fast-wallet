//! Presigned nonce recycling through real loopback RPC broadcasts.
//! Definitive rejection frees the gap; uncertain sends and rebroadcasts retain it.

#[path = "support/presigned_rpc.rs"]
mod rpc;

use rpc::{request, send, wallet, Reply, Rpc, AMBIGUOUS, DEFINITIVE, NONCE};
use std::time::Duration;

async fn rejected_send(detailed: bool, second_reply: Option<Reply>, recycled: bool) {
    let mut first = Rpc::start().await;
    let mut second = Rpc::start().await;
    let endpoints = if second_reply.is_some() {
        vec![first.url.clone(), second.url.clone()]
    } else {
        vec![]
    };
    let wallet = wallet(&first, endpoints);
    let tx = wallet.sign(request()).unwrap();
    assert_eq!(tx.nonce(), NONCE);
    let sending = send(&wallet, &tx, detailed);
    assert!(first
        .next_send()
        .await
        .send(Reply::Error(DEFINITIVE))
        .is_ok());
    if let Some(reply) = second_reply {
        assert!(second.next_send().await.send(reply).is_ok());
    }
    assert!(sending.await.unwrap().is_err());
    assert_eq!(wallet.inflight_count(), usize::from(!recycled));
    assert_eq!(
        wallet.sign(request()).unwrap().nonce(),
        NONCE + u64::from(!recycled)
    );
}

#[tokio::test]
async fn send_signed_definitive_single_recycles() {
    rejected_send(false, None, true).await;
}

#[tokio::test]
async fn send_signed_detailed_definitive_single_recycles() {
    rejected_send(true, None, true).await;
}

#[tokio::test]
async fn presigned_all_definitive_batch_recycles() {
    for detailed in [false, true] {
        rejected_send(detailed, Some(Reply::Error("intrinsic gas too high")), true).await;
    }
}

#[tokio::test]
async fn presigned_mixed_transport_retains() {
    for detailed in [false, true] {
        rejected_send(detailed, Some(Reply::Disconnect), false).await;
    }
}

#[tokio::test]
async fn presigned_mixed_ambiguous_retains() {
    for detailed in [false, true] {
        rejected_send(detailed, Some(Reply::Error(AMBIGUOUS)), false).await;
    }
}

#[tokio::test]
async fn verify_broadcast_definitive_rebroadcast_retains() {
    let mut rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    let tx = wallet.sign(request()).unwrap();
    let sending = send(&wallet, &tx, false);
    assert!(rpc.next_send().await.send(Reply::Accept).is_ok());
    assert!(sending.await.unwrap().is_ok());
    let verifying = tokio::spawn({
        let wallet = wallet.clone();
        async move {
            wallet
                .verify_broadcast(&tx, tx.hash(), Duration::ZERO)
                .await
        }
    });
    assert!(rpc.next_send().await.send(Reply::Error(DEFINITIVE)).is_ok());
    assert!(verifying.await.unwrap().is_err());
    assert_eq!(wallet.inflight_count(), 1);
    assert_eq!(wallet.sign(request()).unwrap().nonce(), NONCE + 1);
}

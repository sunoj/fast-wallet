//! Revert refusals through loopback JSON-RPC broadcasts using shared RPC support.
//! Covers batch and preheated nonce reuse, plus retention on a mixed timeout.

#[allow(dead_code)]
#[path = "support/presigned_rpc.rs"]
mod rpc;

use fast_wallet::{error::EndpointFailureClass, FastWalletBuilder, WalletError};
use rpc::{request, send, Reply, Rpc, KEY};
use std::sync::Arc;

async fn refused_send(preheated: bool, timeout: bool) {
    let mut first = Rpc::start().await;
    let mut second = Rpc::start().await;
    let wallet = Arc::new(
        FastWalletBuilder::new(KEY, &first.url)
            .chain_id(1)
            .broadcast_rpcs_exclusive(vec![first.url.clone(), second.url.clone()])
            .build_with_nonce(0)
            .unwrap(),
    );
    let sending = if preheated {
        let ctx = wallet.preheat(false).await.unwrap();
        tokio::spawn({
            let wallet = wallet.clone();
            async move {
                wallet
                    .send_with_preheat_detailed(&ctx, request())
                    .await
                    .map(|_| ())
            }
        })
    } else {
        let tx = wallet.sign(request()).unwrap();
        assert_eq!(tx.nonce(), 0);
        send(&wallet, &tx, true)
    };
    assert!(first.next_send().await.send(Reply::Revert).is_ok());
    let second_reply = second.next_send().await;
    let _pending_reply = if timeout {
        Some(second_reply)
    } else {
        assert!(second_reply.send(Reply::Revert).is_ok());
        None
    };
    let error = sending.await.unwrap().unwrap_err();
    assert_verdicts(&error, timeout);
    assert_eq!(error.is_definitive_rejection(), !timeout);
    assert_eq!(wallet.inflight_count(), usize::from(timeout));
    assert_eq!(wallet.sign(request()).unwrap().nonce(), u64::from(timeout));
}

fn assert_verdicts(error: &WalletError, timeout: bool) {
    let WalletError::BroadcastFailed(failure) = error else {
        panic!("expected per-endpoint failure, got {error}");
    };
    assert_eq!(failure.endpoints.len(), 2);
    let reverts = failure
        .endpoints
        .iter()
        .filter(|v| {
            v.class == EndpointFailureClass::Rejection
                && v.error.contains("RPC error 3: execution reverted")
                && v.error.contains("data=0x08c379a0")
        })
        .count();
    assert_eq!(reverts, if timeout { 1 } else { 2 });
    if timeout {
        assert!(failure.endpoints.iter().any(|v| {
            v.class == EndpointFailureClass::Transport && v.error.contains("timeout")
        }));
    }
}

#[tokio::test]
async fn signed_revert_refusal_recycles() {
    refused_send(false, false).await;
}

#[tokio::test]
async fn preheated_revert_refusal_recycles() {
    refused_send(true, false).await;
}

#[tokio::test]
async fn revert_refusal_and_timeout_retains() {
    refused_send(false, true).await;
}

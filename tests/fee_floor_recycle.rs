//! Fee-floor pre-check failures through real loopback RPC sends.
//! Covers nonce reuse, all-endpoint proof, and deny-first ambiguous outcomes.

#[allow(dead_code)]
#[path = "support/presigned_rpc.rs"]
mod rpc;

use rpc::{request, send, wallet, Reply, Rpc, NONCE};

const NITRO: &str = "max fee per gas less than block base fee: address 0x..., maxFeePerGas: 100000000 baseFee: 122718000";

async fn rejected_send(message: &'static str, batch: bool, ambiguous: Option<&'static str>) {
    let mut first = Rpc::start().await;
    let mut second = Rpc::start().await;
    let endpoints = if batch {
        vec![first.url.clone(), second.url.clone()]
    } else {
        vec![]
    };
    let wallet = wallet(&first, endpoints);
    let tx = wallet.sign(request()).unwrap();
    assert_eq!(tx.nonce(), NONCE);
    let sending = send(&wallet, &tx, true);
    let first_message = if batch {
        message
    } else {
        ambiguous.unwrap_or(message)
    };
    assert!(first
        .next_send()
        .await
        .send(Reply::Error(first_message))
        .is_ok());
    if batch {
        assert!(second
            .next_send()
            .await
            .send(Reply::Error(ambiguous.unwrap_or(message)))
            .is_ok());
    }
    assert!(sending.await.unwrap().is_err());
    assert_eq!(
        wallet.inflight_count(),
        usize::from(ambiguous.is_some()),
        "{message}"
    );
    assert_eq!(
        wallet.sign(request()).unwrap().nonce(),
        NONCE + u64::from(ambiguous.is_some()),
        "{message}"
    );
}

macro_rules! rejection_cases {
    ($single:ident, $batch:ident, $message:expr) => {
        #[tokio::test]
        async fn $single() {
            rejected_send($message, false, None).await;
        }
        #[tokio::test]
        async fn $batch() {
            rejected_send($message, true, None).await;
        }
    };
}

rejection_cases!(nitro_single_recycles, nitro_batch_recycles, NITRO);
rejection_cases!(
    geth_fee_cap_single_recycles,
    geth_fee_cap_batch_recycles,
    "fee cap less than block base fee"
);
rejection_cases!(
    max_fee_single_recycles,
    max_fee_batch_recycles,
    "max fee per gas too low"
);
rejection_cases!(
    gas_price_single_recycles,
    gas_price_batch_recycles,
    "gas price below minimum"
);
rejection_cases!(
    intrinsic_gas_single_recycles,
    intrinsic_gas_batch_recycles,
    "intrinsic gas too low"
);
rejection_cases!(
    configured_cap_single_recycles,
    configured_cap_batch_recycles,
    "tx fee (1.00 ether) exceeds the configured cap (0.50 ether)"
);

#[tokio::test]
async fn preheated_nitro_recycles() {
    for detailed in [false, true] {
        let mut rpc = Rpc::start().await;
        let wallet = wallet(&rpc, vec![]);
        let ctx = wallet.preheat(false).await.unwrap();
        let sending = tokio::spawn({
            let wallet = wallet.clone();
            async move {
                if detailed {
                    wallet
                        .send_with_preheat_detailed(&ctx, request())
                        .await
                        .map(|_| ())
                } else {
                    wallet.send_with_preheat(&ctx, request()).await.map(|_| ())
                }
            }
        });
        assert!(rpc.next_send().await.send(Reply::Error(NITRO)).is_ok());
        assert!(sending.await.unwrap().is_err());
        assert_eq!(wallet.inflight_count(), 0);
        assert_eq!(wallet.sign(request()).unwrap().nonce(), NONCE);
    }
}

#[tokio::test]
async fn nitro_and_timeout_batch_retains() {
    let mut first = Rpc::start().await;
    let mut second = Rpc::start().await;
    let wallet = wallet(&first, vec![first.url.clone(), second.url.clone()]);
    let tx = wallet.sign(request()).unwrap();
    let sending = send(&wallet, &tx, true);
    assert!(first.next_send().await.send(Reply::Error(NITRO)).is_ok());
    let _pending_timeout_reply = second.next_send().await;
    let error = sending.await.unwrap().unwrap_err();
    let fast_wallet::WalletError::BroadcastFailed(failure) = error else {
        panic!("expected per-endpoint failure, got {error}");
    };
    assert!(failure.endpoints.iter().any(|v| v.error.contains(NITRO)));
    assert!(failure.endpoints.iter().any(|v| {
        v.class == fast_wallet::error::EndpointFailureClass::Transport
            && v.error.contains("timeout")
    }));
    assert_eq!(wallet.inflight_count(), 1);
    assert_eq!(wallet.sign(request()).unwrap().nonce(), NONCE + 1);
}

#[tokio::test]
async fn underpriced_fee_floor_retains() {
    for batch in [false, true] {
        rejected_send(
            NITRO,
            batch,
            Some("transaction underpriced: gas price below minimum"),
        )
        .await;
    }
}

#[tokio::test]
async fn ambiguous_markers_with_fee_floor_retain() {
    for message in [
        "replacement transaction underpriced: gas price below minimum",
        "fee too low: gas price below minimum",
        "already known: gas price below minimum",
        "nonce too low: gas price below minimum",
        "nonce too high: gas price below minimum",
        "timeout: gas price below minimum",
        "connection: gas price below minimum",
        "network: gas price below minimum",
        "temporarily unavailable: gas price below minimum",
        "decoding response: gas price below minimum",
        "bad gateway: gas price below minimum",
        "upstream: gas price below minimum",
        "context deadline exceeded: gas price below minimum",
        "dial tcp: gas price below minimum",
        "Post \"https://sequencer.test\": EOF; gas price below minimum",
    ] {
        rejected_send(NITRO, false, Some(message)).await;
    }
}

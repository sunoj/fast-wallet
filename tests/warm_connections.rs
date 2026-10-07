// Loopback regressions for warm broadcast pools: warmups per URL over a simulated hour, a re-dial
// after a lost connection, and one client per endpoint URL (broadcast and gas) inside a wallet.
// Keep-alive HTTP/1.1 mocks count connections and calls; tokio's paused clock stands in for time.
#[allow(dead_code)] // each test binary uses part of the shared mock
mod support;

use alloy::primitives::{Address, U256};
use fast_wallet::rpc::{BatchRpcClient, RpcClient};
use fast_wallet::{FastWalletBuilder, RpcEndpoint, TransactionBroadcaster, TransactionRequest};
use std::sync::atomic::Ordering;
use std::time::Duration;
use support::{idle_for, Mock, DROP_NEXT_SEND, HANG_SENDS};

const TEST_PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn transfer() -> TransactionRequest {
    TransactionRequest::new()
        .to(Address::repeat_byte(1))
        .value(U256::from(1u64))
        .gas_limit(21_000)
        .gas_price(U256::from(1_000_000_000u64))
}

/// A wallet whose primary URL is repeated in its broadcast list, with a
/// preheat every 10 s for an hour, no other traffic. Before: one probe per
/// client per 60 s, the primary in three clients: 180 and 60 per hour. The
/// mocks are `http://`, so they keep the 60 s window; an `https://` URL gets
/// 4 minutes, 15 probes an hour (`src/warm.rs` tests).
#[tokio::test]
async fn an_hour_of_preheats_bills_one_warmup_per_url_per_window() {
    let primary = Mock::start().await;
    let other = Mock::start().await;
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary.url)
        .chain_id(1)
        .broadcast_rpcs(vec![primary.url.clone(), other.url.clone()])
        .build_with_nonce(0)
        .expect("wallet");
    for _ in 0..360 {
        wallet.warmup_connections().await;
        idle_for(Duration::from_secs(10)).await;
    }
    let per_url = (primary.count("eth_chainId"), other.count("eth_chainId"));
    assert_eq!(
        per_url,
        (60, 60),
        "eth_chainId per URL in one simulated hour"
    );
    assert_eq!(
        primary.connections(),
        1,
        "the primary's pooled connection never went idle-cold"
    );
}

#[tokio::test]
async fn an_idle_rpc_pool_keeps_its_connection() {
    let mock = Mock::start().await;
    let client = RpcClient::new(mock.url.clone()).expect("client");
    assert_eq!(client.chain_id().await.expect("chain id"), 1);
    idle_for(Duration::from_secs(20 * 60)).await;
    assert_eq!(client.chain_id().await.expect("chain id"), 1);
    assert_eq!(
        mock.connections(),
        1,
        "20 idle minutes kept the pooled connection"
    );
}

#[tokio::test]
async fn the_warmup_after_a_dropped_connection_redials() {
    let mock = Mock::start().await;
    let batch = BatchRpcClient::new(vec![mock.url.clone()]).expect("batch");
    assert_eq!(batch.warmup().await, 1);
    assert_eq!((mock.count("eth_chainId"), mock.connections()), (1, 1));

    mock.mode.store(DROP_NEXT_SEND, Ordering::SeqCst);
    assert!(
        batch.broadcast_transaction("0x01").await.is_err(),
        "peer closed mid-request"
    );

    assert_eq!(batch.warmup().await, 1);
    assert_eq!(
        mock.count("eth_chainId"),
        2,
        "a lost connection earns no throttle window"
    );
    assert_eq!(
        mock.connections(),
        2,
        "the probe re-established the connection"
    );
}

#[tokio::test]
async fn the_warmup_after_an_abandoned_send_redials() {
    let mock = Mock::start().await;
    let batch = BatchRpcClient::new(vec![mock.url.clone()])
        .expect("batch")
        .with_send_timeout(Duration::from_millis(100));
    assert_eq!(batch.warmup().await, 1);

    mock.mode.store(HANG_SENDS, Ordering::SeqCst);
    assert!(
        batch.broadcast_transaction("0x01").await.is_err(),
        "send timed out"
    );

    assert_eq!(batch.warmup().await, 1);
    assert_eq!(
        mock.count("eth_chainId"),
        2,
        "an abandoned send earns no throttle window"
    );
    assert_eq!(
        mock.connections(),
        2,
        "the probe dialled a fresh connection"
    );
}

#[tokio::test]
async fn the_broadcaster_keeps_an_idle_pool_and_redials_after_a_drop() {
    let mock = Mock::start().await;
    let endpoints = vec![RpcEndpoint::public(mock.url.clone())];
    let broadcaster = TransactionBroadcaster::new(endpoints).expect("broadcaster");
    assert_eq!(broadcaster.warmup().await, 1);
    idle_for(Duration::from_secs(20 * 60)).await;
    assert!(broadcaster.broadcast_raw("0x01").await.tx_hash.is_some());
    assert_eq!(
        mock.connections(),
        1,
        "20 idle minutes kept the pooled connection"
    );
    assert_eq!(broadcaster.warmup().await, 1);
    assert_eq!(
        mock.count("eth_chainId"),
        1,
        "the clean send restarted the window"
    );

    mock.mode.store(DROP_NEXT_SEND, Ordering::SeqCst);
    assert!(broadcaster.broadcast_raw("0x01").await.tx_hash.is_none());
    assert_eq!(broadcaster.warmup().await, 1);
    let traffic = (mock.count("eth_chainId"), mock.connections());
    assert_eq!(
        traffic,
        (2, 2),
        "the drop cost the window; the probe re-dialled"
    );
}

#[tokio::test]
async fn a_wallet_sends_once_per_endpoint_url_and_the_duplicate_added_nothing() {
    let primary = Mock::start().await;
    let other = Mock::start().await;
    let respelled = format!("{}/", primary.url.replace("http://", "HTTP://"));
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary.url)
        .chain_id(1)
        .broadcast_rpcs(vec![respelled.clone(), other.url.clone()])
        .build_with_nonce(0)
        .expect("wallet");
    wallet
        .sync_nonce()
        .await
        .expect("nonce read on the primary");
    let tx = wallet.sign(transfer()).expect("sign");
    let hash = wallet.send_signed(&tx).await.expect("send");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        primary.sent(),
        vec![tx.to_hex()],
        "one copy to the primary URL"
    );
    assert_eq!(
        other.sent(),
        vec![tx.to_hex()],
        "every other endpoint still gets its copy"
    );
    assert_eq!(
        primary.connections(),
        1,
        "read and send share the primary's one pool"
    );

    // What the duplicate entry used to buy: the same bytes to the same endpoint,
    // answered with the same hash.
    let duplicated = BatchRpcClient::new(vec![primary.url.clone(), respelled]).expect("batch");
    let duplicate_hash = duplicated
        .broadcast_transaction(&tx.to_hex())
        .await
        .expect("send");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(duplicate_hash, hash);
    assert_eq!(hash, tx.hash());
    assert_eq!(
        primary.sent(),
        vec![tx.to_hex(); 3],
        "two more identical copies"
    );
}

/// Identity is the URL exactly as a request parses it: a trailing NBSP is part
/// of the path (`/%C2%A0`), so it names another endpoint and keeps its send.
#[tokio::test]
async fn a_url_differing_by_a_trailing_nbsp_is_another_endpoint() {
    let primary = Mock::start().await;
    let nbsp = format!("{}/\u{a0}", primary.url);
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary.url)
        .chain_id(1)
        .broadcast_rpcs(vec![nbsp])
        .build_with_nonce(0)
        .expect("wallet");
    let tx = wallet.sign(transfer()).expect("sign");
    wallet.send_signed(&tx).await.expect("send");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        primary.sent(),
        vec![tx.to_hex(); 2],
        "one copy per endpoint"
    );
}

/// Gas price reads go through `gas_rpc_url`; a URL naming the primary or a
/// broadcast endpoint reuses that client's pool instead of dialling another.
#[tokio::test]
async fn a_gas_rpc_url_reuses_the_matching_client() {
    for gas_is_primary in [true, false] {
        let primary = Mock::start().await;
        let other = Mock::start().await;
        let (gas, gas_url) = if gas_is_primary {
            (&primary, primary.url.replace("http://", "HTTP://"))
        } else {
            (&other, other.url.clone())
        };
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary.url)
            .chain_id(1)
            .broadcast_rpcs(vec![other.url.clone()])
            .gas_rpc_url(gas_url)
            .build_with_nonce(0)
            .expect("wallet");
        assert_eq!(wallet.warmup_connections().await, (true, 2));
        wallet.get_gas_price().await.expect("gas price");
        let traffic = (gas.count("eth_gasPrice"), gas.connections());
        assert_eq!(traffic, (1, 1), "gas is primary: {gas_is_primary}");
    }

    let primary = Mock::start().await;
    let own = Mock::start().await;
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary.url)
        .chain_id(1)
        .gas_rpc_url(own.url.clone())
        .build_with_nonce(0)
        .expect("wallet");
    wallet.get_gas_price().await.expect("gas price");
    assert_eq!(
        (own.count("eth_gasPrice"), primary.count("eth_gasPrice")),
        (1, 0)
    );
}

// Loopback regressions for connections lost without a send() error: a body cut short after its
// headers, a send dropped as a race loser, a read its caller cancelled, and a server that closes
// idle HTTP/1.1 connections. Each shows whether the next warmup probes or trusts a dead connection.
#[allow(dead_code)] // each test binary uses part of the shared mock
mod support;

use alloy::primitives::Address;
use fast_wallet::rpc::{BatchRpcClient, RpcClient};
use fast_wallet::{BroadcastStrategy, RpcEndpoint, TransactionBroadcaster};
use std::sync::atomic::Ordering;
use std::time::Duration;
use support::{idle_for, Mock, CUT_NEXT_BODY, HANG_NEXT_READ, HANG_SENDS};

#[tokio::test]
async fn a_batch_send_whose_body_is_cut_short_costs_the_window() {
    let mock = Mock::start().await;
    let batch = BatchRpcClient::new(vec![mock.url.clone()]).expect("batch");
    assert_eq!(batch.warmup().await, 1);

    mock.mode.store(CUT_NEXT_BODY, Ordering::SeqCst);
    assert!(
        batch.broadcast_transaction("0x01").await.is_err(),
        "headers arrived, the body did not"
    );

    assert_eq!(batch.warmup().await, 1);
    let traffic = (mock.count("eth_chainId"), mock.connections());
    assert_eq!(traffic, (2, 2), "the probe re-dialled the cut connection");
}

#[tokio::test]
async fn a_broadcast_whose_body_is_cut_short_costs_the_window() {
    let mock = Mock::start().await;
    let endpoints = vec![RpcEndpoint::public(mock.url.clone())];
    let broadcaster = TransactionBroadcaster::new(endpoints).expect("broadcaster");
    assert_eq!(broadcaster.warmup().await, 1);

    mock.mode.store(CUT_NEXT_BODY, Ordering::SeqCst);
    let result = broadcaster.broadcast_raw("0x01").await;
    assert!(
        result.tx_hash.is_none(),
        "headers arrived, the body did not"
    );

    assert_eq!(broadcaster.warmup().await, 1);
    let traffic = (mock.count("eth_chainId"), mock.connections());
    assert_eq!(traffic, (2, 2), "the probe re-dialled the cut connection");
}

/// RaceAll and PrivateFirst return on the first accepted send and drop the
/// others mid-flight; on HTTP/1.1 that discards their connections.
#[tokio::test]
async fn a_send_dropped_as_a_race_loser_costs_the_window() {
    for strategy in [BroadcastStrategy::RaceAll, BroadcastStrategy::PrivateFirst] {
        let fast = Mock::start().await;
        let slow = Mock::start().await;
        let endpoints = vec![
            RpcEndpoint::private(fast.url.clone(), "key"),
            RpcEndpoint::private(slow.url.clone(), "key"),
        ];
        let broadcaster = TransactionBroadcaster::new(endpoints)
            .expect("broadcaster")
            .with_strategy(strategy);
        assert_eq!(broadcaster.warmup().await, 2);

        slow.mode.store(HANG_SENDS, Ordering::SeqCst);
        let result = broadcaster.broadcast_raw("0x01").await;
        assert!(result.tx_hash.is_some(), "{strategy:?}");

        assert_eq!(broadcaster.warmup().await, 2);
        let probes = (fast.count("eth_chainId"), slow.count("eth_chainId"));
        assert_eq!(
            probes,
            (1, 2),
            "{strategy:?}: the winner stays warm, the dropped loser probes again"
        );
    }
}

/// A caller that drops a read mid-flight (its own timeout, a lost `select!`)
/// discards the HTTP/1.1 connection, so the next warmup must re-dial rather
/// than leave the send that follows to dial cold.
#[tokio::test]
async fn a_read_cancelled_by_its_caller_costs_the_window() {
    let mock = Mock::start().await;
    let client = RpcClient::new(mock.url.clone()).expect("client");
    client.warmup().await.expect("first probe");

    mock.mode.store(HANG_NEXT_READ, Ordering::SeqCst);
    let in_flight = async {
        while mock.count("eth_getTransactionCount") == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        _ = client.get_nonce(Address::ZERO) => panic!("the mock never answers this read"),
        _ = in_flight => {}
    }

    client.warmup().await.expect("second probe");
    let traffic = (mock.count("eth_chainId"), mock.connections());
    assert_eq!(
        traffic,
        (2, 2),
        "the probe re-dialled the dropped connection"
    );
    assert!(client.send_raw_transaction("0x01").await.is_ok());
    assert_eq!(
        mock.connections(),
        2,
        "the send reused the probe's connection"
    );
}

/// Let `idle` of tokio time pass, then give the server's FIN real time to
/// reach the client's pool.
async fn idle_past_close(idle: Duration) {
    idle_for(idle).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// Pins the residual of a server that closes idle connections itself (here
/// HTTP/1.1, FIN after 10 s idle; HTTP/1.1 has no pings to keep it open). The
/// FIN empties the pool without an error, so the next send dials unless a
/// warmup probed first:
/// - idle past the window (90 s against the 60 s `http://` window): the warmup
///   probes and re-dials, and the send reuses that connection — a warm send;
/// - idle inside the window but past the close (30 s): the warmup skips and the
///   send dials — a cold send.
///
/// An `https://` URL gets the 4-minute window, so against a server that closes
/// idle connections after N s, every gap between N s and 4 min ends in a cold
/// send. QuickNode kept idle HTTP/2 connections for 5 min under pings (closed
/// by 10 min), the keyless hosts measured for 30 min; an `https://` host that
/// speaks HTTP/1.1, or that closes idle HTTP/2 connections within 4 min, pays
/// that cold send.
#[tokio::test]
async fn a_server_idle_close_costs_a_cold_send_only_inside_the_window() {
    let mock = Mock::closing_idle_after(Duration::from_secs(10)).await;
    let batch = BatchRpcClient::new(vec![mock.url.clone()]).expect("batch");
    assert_eq!(batch.warmup().await, 1);

    idle_past_close(Duration::from_secs(90)).await;
    assert_eq!(batch.warmup().await, 1);
    assert!(batch.broadcast_transaction("0x01").await.is_ok());
    let traffic = (mock.count("eth_chainId"), mock.connections());
    assert_eq!(
        traffic,
        (2, 2),
        "past the window: the probe re-dialled, the send reused it"
    );

    idle_past_close(Duration::from_secs(30)).await;
    assert_eq!(batch.warmup().await, 1);
    assert!(batch.broadcast_transaction("0x01").await.is_ok());
    let traffic = (mock.count("eth_chainId"), mock.connections());
    assert_eq!(
        traffic,
        (2, 3),
        "inside the window: no probe, the send dialled cold"
    );
}

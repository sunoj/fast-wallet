// A first-success race must let the losing endpoints' sends run to their end.
// The slow loopback endpoint checks, before it answers, whether its client is still waiting.
use fast_wallet::broadcast::{BroadcastStrategy, RpcEndpoint, TransactionBroadcaster};
use fast_wallet::rpc::BatchRpcClient;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const SLOW_MS: u64 = 150;

fn reply(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Answers every request at once with a send success.
async fn fast_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0; 4096];
                let _ = stream.read(&mut buf).await;
                let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{HASH}"}}"#);
                let _ = stream.write_all(reply(&body).as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

/// Holds the first request for `SLOW_MS`, then reports whether the client was still
/// connected (no EOF yet) and answers it.
async fn slow_endpoint() -> (String, oneshot::Receiver<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    let (report, waiting) = oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = [0; 4096];
        let _ = stream.read(&mut buf).await;
        tokio::time::sleep(Duration::from_millis(SLOW_MS)).await;
        let connected = match stream.try_read(&mut buf) {
            Err(error) => error.kind() == std::io::ErrorKind::WouldBlock,
            Ok(read) => read > 0,
        };
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{HASH}"}}"#);
        let _ = stream.write_all(reply(&body).as_bytes()).await;
        let _ = report.send(connected);
    });
    (format!("http://{addr}"), waiting)
}

async fn assert_still_waited_for(waiting: oneshot::Receiver<bool>) {
    let connected = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .expect("slow endpoint got the request")
        .expect("slow endpoint report");
    assert!(connected, "the losing send was dropped before its endpoint answered");
}

#[tokio::test]
async fn broadcaster_race_lets_the_slow_endpoint_answer() {
    let (slow, waiting) = slow_endpoint().await;
    let broadcaster = TransactionBroadcaster::new(vec![
        RpcEndpoint::public(slow),
        RpcEndpoint::public(fast_endpoint().await),
    ])
    .expect("broadcaster")
    .with_strategy(BroadcastStrategy::RaceAll);
    let result = broadcaster.broadcast_raw("0x01").await;
    assert!(result.tx_hash.is_some(), "{:?}", result.errors);
    assert_still_waited_for(waiting).await;
}

#[tokio::test]
async fn broadcaster_private_first_lets_the_slow_relay_answer() {
    let (slow, waiting) = slow_endpoint().await;
    let broadcaster = TransactionBroadcaster::new(vec![
        RpcEndpoint::private(slow, "k"),
        RpcEndpoint::private(fast_endpoint().await, "k"),
    ])
    .expect("broadcaster")
    .with_strategy(BroadcastStrategy::PrivateFirst);
    let result = broadcaster.broadcast_raw("0x01").await;
    assert!(result.tx_hash.is_some(), "{:?}", result.errors);
    assert_still_waited_for(waiting).await;
}

#[tokio::test]
async fn private_first_success_still_skips_public_endpoints() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let public = format!("http://{}", listener.local_addr().expect("address"));
    let broadcaster = TransactionBroadcaster::new(vec![
        RpcEndpoint::private(fast_endpoint().await, "k"),
        RpcEndpoint::public(public),
    ])
    .expect("broadcaster")
    .with_strategy(BroadcastStrategy::PrivateFirst);
    let result = broadcaster.broadcast_raw("0x01").await;
    assert!(result.tx_hash.is_some(), "{:?}", result.errors);
    let contacted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
    assert!(contacted.is_err(), "a private success must not reach public endpoints");
}

#[tokio::test]
async fn batch_client_lets_the_slow_endpoint_answer() {
    let (slow, waiting) = slow_endpoint().await;
    let client = BatchRpcClient::new(vec![slow, fast_endpoint().await]).expect("client");
    let result = client
        .broadcast_transaction_detailed("0x01")
        .await
        .expect("first success");
    assert_eq!(result.first_success_endpoint_index, 1);
    assert_still_waited_for(waiting).await;
}

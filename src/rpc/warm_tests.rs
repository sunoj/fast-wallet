// Loopback tests for broadcast timing and warmup throttling.
// Uses the shared RPC client and Tokio HTTP test servers.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn tx_rpc_server(delay_ms: u64, hash: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let _ = stream.read(&mut buf).await.unwrap();
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{hash}"}}"#);
        let response = format!(
            concat!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
                "content-length: {}\r\nconnection: close\r\n\r\n{}"
            ),
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn broadcast_transaction_detailed_returns_winning_endpoint_index() {
    let slow = tx_rpc_server(
        80,
        "0x1111111111111111111111111111111111111111111111111111111111111111",
    )
    .await;
    let fast = tx_rpc_server(
        5,
        "0x2222222222222222222222222222222222222222222222222222222222222222",
    )
    .await;
    let client = BatchRpcClient::new(vec![slow, fast.clone()]).unwrap();

    let result = client
        .broadcast_transaction_detailed("0x01")
        .await
        .expect("broadcast should succeed");

    assert_eq!(result.first_success_endpoint_index, 1);
    assert_eq!(result.fastest_endpoint_host, endpoint_host(&fast));
    assert_eq!(
        result.tx_hash,
        "0x2222222222222222222222222222222222222222222222222222222222222222"
    );
}

#[tokio::test]
async fn warmup_bails_out_fast_on_blackholed_endpoint() {
    // Accept + read the request but never reply, holding the connection open
    // far past the warmup timeout — simulates a blackholed / partitioned RPC.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let _ = stream.read(&mut buf).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
        let _ = &mut stream;
    });

    let client = RpcClient::new(format!("http://{addr}")).unwrap();
    let start = Instant::now();
    let result = client.warmup().await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "warmup must fail on a blackholed endpoint");
    assert!(
        elapsed < Duration::from_secs(2),
        "warmup must bail via the 500ms timeout, not the 30s reqwest timeout (took {elapsed:?})"
    );
}

async fn counting_rpc_server(result: &'static str) -> (String, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicU64::new(0));
    let hits_clone = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            hits_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf).await;
            let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{result}"}}"#);
            let response = format!(
                concat!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
                    "content-length: {}\r\nconnection: close\r\n\r\n{}"
                ),
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    (format!("http://{addr}"), hits)
}

/// Serves HTTP 200 with a well-formed JSON-RPC *error* body — a host that is
/// reachable and answering but not actually serving.
async fn counting_rpc_error_server() -> (String, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicU64::new(0));
    let hits_clone = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            hits_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf).await;
            let body =
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Network mismatch"}}"#;
            let response = format!(
                concat!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
                    "content-length: {}\r\nconnection: close\r\n\r\n{}"
                ),
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    (format!("http://{addr}"), hits)
}

/// Cross-audit finding (2026-08-12): marking success before inspecting
/// `rpc_response.error` let a live-but-broken endpoint buy a full throttle
/// window, so warmup went quiet for 60s exactly when it should have probed.
#[tokio::test]
async fn jsonrpc_error_does_not_suppress_next_warmup() {
    let (url, hits) = counting_rpc_error_server().await;
    let client = RpcClient::new(url).unwrap();

    assert!(client.warmup().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    assert!(client.warmup().await.is_err());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "a JSON-RPC error must not earn a throttle window"
    );
}

/// Round-2 re-audit residual: a body carrying neither `result` nor `error`
/// is still an endpoint that is not serving, so it must not earn a window.
#[tokio::test]
async fn empty_response_does_not_suppress_next_warmup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicU64::new(0));
    let hits_clone = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            hits_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf).await;
            let body = r#"{"jsonrpc":"2.0","id":1}"#;
            let response = format!(
                concat!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n",
                    "content-length: {}\r\nconnection: close\r\n\r\n{}"
                ),
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    let client = RpcClient::new(format!("http://{addr}")).unwrap();

    assert!(client.warmup().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    assert!(client.warmup().await.is_err());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "a body with neither result nor error must not earn a throttle window"
    );
}

/// Same guard on the request path: a failed call must not mark the endpoint
/// warm and silence the warmup that follows it.
#[tokio::test]
async fn failed_rpc_request_does_not_suppress_warmup() {
    let (url, hits) = counting_rpc_error_server().await;
    let client = RpcClient::new(url).unwrap();

    assert!(client.chain_id().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    assert!(client.warmup().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn warmup_skips_probe_within_throttle_window() {
    let (url, hits) = counting_rpc_server("0x1").await;
    let client = RpcClient::new(url).unwrap();

    client.warmup().await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    client.warmup().await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn warmup_probes_again_after_throttle_window() {
    let (url, hits) = counting_rpc_server("0x1").await;
    let client = RpcClient::new(url).unwrap();

    client.warmup().await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    crate::warm::idle_for(crate::warm::HTTP_WARMUP_WINDOW).await;
    client.warmup().await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn successful_rpc_request_suppresses_warmup_probe() {
    let (url, hits) = counting_rpc_server("0x1").await;
    let client = RpcClient::new(url).unwrap();

    client.chain_id().await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    client.warmup().await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_request_does_not_suppress_warmup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicU64::new(0));
    let hits_clone = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            hits_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf).await;
        }
    });

    let client = RpcClient::new(format!("http://{addr}")).unwrap();
    assert!(client.warmup().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert!(client.warmup().await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn batch_warmup_throttled_skip_counts_as_success() {
    let (url, hits) = counting_rpc_server("0x1").await;
    let batch = BatchRpcClient::new(vec![url]).unwrap();

    assert_eq!(batch.warmup().await, 1);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(batch.warmup().await, 1);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

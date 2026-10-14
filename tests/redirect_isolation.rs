// A send endpoint's HTTP redirect must never carry the signed transaction to another URL.
// Loopback servers: the first answers 307 to the second, which counts every connection.
use fast_wallet::rpc::BatchRpcClient;
use fast_wallet::{RpcEndpoint, TransactionBroadcaster};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn counting_target() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}/", listener.local_addr().expect("address"));
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0; 4096];
            let _ = stream.read(&mut buf).await;
            let body = r#"{"jsonrpc":"2.0","id":1,"result":"0x01"}"#;
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(reply.as_bytes()).await;
        }
    });
    (url, hits)
}

async fn redirecting_endpoint(location: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}/", listener.local_addr().expect("address"));
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0; 4096];
            let _ = stream.read(&mut buf).await;
            let reply = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(reply.as_bytes()).await;
        }
    });
    url
}

#[tokio::test]
async fn batch_send_does_not_follow_a_redirect() {
    for endpoints in [1, 2] {
        let (target, hits) = counting_target().await;
        let mut urls = Vec::new();
        for _ in 0..endpoints {
            urls.push(redirecting_endpoint(target.clone()).await);
        }
        let client = BatchRpcClient::new(urls).expect("client");
        assert!(client.broadcast_transaction("0x01").await.is_err());
        assert!(client.broadcast_transaction_detailed("0x01").await.is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 0, "redirect followed ({endpoints} endpoints)");
    }
}

#[tokio::test]
async fn broadcaster_does_not_follow_a_redirect() {
    let (target, hits) = counting_target().await;
    let relay = redirecting_endpoint(target).await;
    let broadcaster = TransactionBroadcaster::new(vec![RpcEndpoint::public(relay)]).expect("broadcaster");
    let _ = broadcaster.broadcast_raw("0x01").await;
    assert_eq!(hits.load(Ordering::SeqCst), 0, "redirect followed");
}

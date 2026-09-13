// Broadcast result and provider-error redaction through every broadcast strategy.
// Depends on fast-wallet and local HTTP fixtures; sends only synthetic raw transactions.

use fast_wallet::broadcast::{BroadcastStrategy, RpcEndpoint, TransactionBroadcaster};
use std::io::{Read, Write};
use std::net::TcpListener;

fn strategies() -> [BroadcastStrategy; 4] {
    [
        BroadcastStrategy::RaceAll,
        BroadcastStrategy::BroadcastAll,
        BroadcastStrategy::PriorityOrdered,
        BroadcastStrategy::PrivateFirst,
    ]
}

fn server(body: &'static str) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let task = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0; 4096];
        stream.read(&mut buffer).unwrap();
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    (format!("http://{address}/v2/SECRETKEY"), task)
}

#[tokio::test]
async fn successful_broadcasts_return_only_endpoint_hosts() {
    for strategy in strategies() {
        let (url, task) = server(r#"{"jsonrpc":"2.0","id":1,"result":null}"#);
        let expected = fast_wallet::endpoint_host(&url).to_string();
        let broadcaster = TransactionBroadcaster::new(vec![RpcEndpoint::public(url)])
            .unwrap()
            .with_strategy(strategy);
        let result = broadcaster.broadcast_raw("0x00").await;
        task.join().unwrap();
        assert_eq!(result.first_success, Some(expected));
        assert!(!format!("{result:?}").contains("SECRETKEY"));
    }
}

#[tokio::test]
async fn failed_broadcasts_redact_server_echoed_urls_and_endpoint_identity() {
    for strategy in strategies() {
        let (url, task) = server(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"provider https://x.example/v2/SECRETKEY"}}"#,
        );
        let expected = fast_wallet::endpoint_host(&url).to_string();
        let broadcaster = TransactionBroadcaster::new(vec![RpcEndpoint::public(url)])
            .unwrap()
            .with_strategy(strategy);
        let result = broadcaster.broadcast_raw("0x00").await;
        task.join().unwrap();
        assert_eq!(result.errors[0].0, expected);
        assert!(result.errors[0].1.to_string().contains("x.example"));
        assert!(!format!("{result:?}").contains("SECRETKEY"));
    }
}

#[tokio::test]
async fn transport_failures_never_expose_request_paths() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let url = format!("http://{address}/v2/SECRETKEY");
    let broadcaster = TransactionBroadcaster::new(vec![RpcEndpoint::public(url)]).unwrap();
    let result = broadcaster.broadcast_raw("0x00").await;
    assert_eq!(result.failure_count, 1);
    assert!(!format!("{result:?}").contains("SECRETKEY"));
}

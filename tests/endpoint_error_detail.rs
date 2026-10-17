// Loopback coverage for bounded broadcast error metadata and detached timeouts.
// Compares observed sends with the same unobserved RPC responses.
use fast_wallet::error::WalletError;
use fast_wallet::rpc::{observation::EndpointObservation, BatchRpcClient, RpcClient};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};

const HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

async fn server(success_ready: Arc<Notify>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let ready = success_ready.clone();
            tokio::spawn(async move {
                let mut input = [0; 4096];
                let size = socket.read(&mut input).await.unwrap();
                let request = String::from_utf8_lossy(&input[..size]);
                let path = request.split_whitespace().nth(1).unwrap();
                if path == "/timeout" {
                    return std::future::pending::<()>().await;
                }
                if path == "/success" {
                    ready.notified().await;
                }
                let (status, body) = reply(path);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://{address}")
}

fn reply(path: &str) -> (&'static str, String) {
    let body = match path {
        "/status" => return ("403 Forbidden", "execution reverted".into()),
        "/decode" => return ("200 OK", "invalid JSON".into()),
        "/missing" => r#"{"error":{"message":"execution reverted"}}"#.to_string(),
        "/string" => r#"{"error":{"code":"denied","message":"private response text"}}"#.to_string(),
        "/numeric" => r#"{"error":{"code":-32000,"message":"private response text"}}"#.to_string(),
        _ => format!(r#"{{"result":"{HASH}"}}"#),
    };
    let body = format!(r#"{{"jsonrpc":"2.0","id":1,{}}}"#, &body[1..body.len() - 1]);
    ("200 OK", body)
}

fn observer(
    client: &BatchRpcClient,
    ready: Arc<Notify>,
) -> mpsc::UnboundedReceiver<EndpointObservation> {
    let (sender, receiver) = mpsc::unbounded_channel();
    let failures = std::sync::atomic::AtomicUsize::new(0);
    assert!(client.set_observer(Arc::new(move |row| {
        if row.phase == "completion_before_return"
            && row.result != "client_success"
            && failures.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
        {
            ready.notify_one();
        }
        sender.send(row).unwrap();
    })));
    receiver
}

async fn completions(
    receiver: &mut mpsc::UnboundedReceiver<EndpointObservation>,
    count: usize,
) -> Vec<EndpointObservation> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut rows = Vec::new();
        while rows.len() < count {
            let row = receiver.recv().await.unwrap();
            if row.is_first_result.is_some() {
                rows.push(row);
            } else if row.phase == "dispatch" {
                assert_eq!(
                    (row.http_status, row.rpc_code, row.cause),
                    (None, None, None)
                );
            }
        }
        rows.sort_by_key(|row| row.endpoint_id);
        rows
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn errors_and_detached_timeout_keep_first_success_send_result() {
    let ready = Arc::new(Notify::new());
    let url = server(ready.clone()).await;
    let client = BatchRpcClient::new(
        ["status", "missing", "timeout", "success"]
            .map(|path| format!("{url}/{path}"))
            .to_vec(),
    )
    .unwrap()
    .with_send_timeout(Duration::from_millis(500));
    let mut receiver = observer(&client, ready);
    let result = client.broadcast_transaction_detailed("0x01").await.unwrap();
    assert_eq!(result.tx_hash, HASH);
    assert_eq!(result.first_success_endpoint_index, 3);
    assert_eq!(result.failed_endpoints, 2);
    assert!(!result.connection_reused);
    assert_eq!(result.per_endpoint_ms.len(), 3);
    let rows = completions(&mut receiver, 4).await;
    let expected = [
        (Some(403), None, Some("http_status")),
        (Some(200), None, Some("rpc_error")),
        (None, None, Some("timeout")),
        (Some(200), None, None),
    ];
    for (row, expected) in rows.iter().zip(expected) {
        assert_eq!((row.http_status, row.rpc_code, row.cause), expected);
    }
    assert_eq!(rows[2].phase, "detached_completion");
    assert_eq!(rows[2].result, "timeout_uncertain");
    let encoded = serde_json::to_string(&rows).unwrap();
    for forbidden in ["127.0.0.1", "private response text", "denied", "0x01"] {
        assert!(!encoded.contains(forbidden));
    }
}

#[tokio::test]
async fn malformed_codes_are_rpc_causes_without_changing_returned_errors() {
    let ready = Arc::new(Notify::new());
    let url = server(ready.clone()).await;
    for (path, status, code, cause) in [
        ("missing", 200, None, "rpc_error"),
        ("string", 200, None, "rpc_error"),
        ("numeric", 200, Some(-32000), "rpc_error"),
        ("status", 403, None, "http_status"),
        ("decode", 200, None, "decode"),
    ] {
        let client = BatchRpcClient::new(vec![format!("{url}/{path}")]).unwrap();
        let mut receiver = observer(&client, ready.clone());
        let baseline = client
            .broadcast_transaction_detailed("0x01")
            .await
            .unwrap_err();
        let row = completions(&mut receiver, 1).await.pop().unwrap();
        let plain = BatchRpcClient::new(vec![format!("{url}/{path}")])
            .unwrap()
            .broadcast_transaction_detailed("0x01")
            .await
            .unwrap_err();
        assert!(!baseline.is_definitive_rejection());
        assert!(!plain.is_definitive_rejection());
        let batch_error = endpoint_error(&baseline).to_string();
        assert_eq!(without_timing(baseline), without_timing(plain));
        assert_eq!(
            (row.http_status, row.rpc_code, row.cause),
            (Some(status), code, Some(cause))
        );
        let direct = RpcClient::new(format!("{url}/{path}"))
            .unwrap()
            .send_raw_transaction("0x01")
            .await
            .unwrap_err();
        assert!(!direct.is_definitive_rejection());
        assert_eq!(batch_error, direct.to_string());
    }
}

fn endpoint_error(error: &WalletError) -> &str {
    let WalletError::BroadcastFailed(failure) = error else {
        panic!("expected structured broadcast failure");
    };
    assert_eq!(failure.endpoints.len(), 1);
    &failure.endpoints[0].error
}

fn without_timing(error: WalletError) -> String {
    let WalletError::BroadcastFailed(mut failure) = error else {
        panic!("expected structured broadcast failure");
    };
    for endpoint in &mut failure.endpoints {
        endpoint.elapsed_ms = 0;
    }
    failure.to_string()
}

#[tokio::test]
async fn observed_and_plain_success_return_the_same_fields() {
    let ready = Arc::new(Notify::new());
    let url = server(ready.clone()).await;
    let client = BatchRpcClient::new(vec![format!("{url}/success")]).unwrap();
    let mut receiver = observer(&client, ready.clone());
    ready.notify_one();
    let observed = client.broadcast_transaction_detailed("0x01").await.unwrap();
    ready.notify_one();
    let plain = BatchRpcClient::new(vec![format!("{url}/success")])
        .unwrap()
        .broadcast_transaction_detailed("0x01")
        .await
        .unwrap();
    assert_eq!(observed.tx_hash, plain.tx_hash);
    assert_eq!(
        observed.first_success_endpoint_index,
        plain.first_success_endpoint_index
    );
    assert_eq!(observed.failed_endpoints, plain.failed_endpoints);
    assert_eq!(observed.fastest_endpoint_host, plain.fastest_endpoint_host);
    assert_eq!(observed.connection_reused, plain.connection_reused);
    let row = completions(&mut receiver, 1).await.pop().unwrap();
    assert_eq!(
        (row.http_status, row.rpc_code, row.cause),
        (Some(200), None, None)
    );
}

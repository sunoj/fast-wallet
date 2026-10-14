// Fake HTTP integration coverage for route identity and detached endpoint evidence.
// Uses only loopback RPC; checks telemetry failure cannot change send outcomes.
use fast_wallet::rpc::{observation::EndpointObservation, BatchRpcClient};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const RETURNED_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const HASH_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"result":"0x1111111111111111111111111111111111111111111111111111111111111111"}"#;

async fn server(delay: u64, error: bool) -> String {
    response_server(delay, error, HASH_BODY).await
}

async fn response_server(delay: u64, error: bool, success_body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut input = [0; 4096];
                let size = socket.read(&mut input).await.unwrap();
                let fast = String::from_utf8_lossy(&input[..size]).contains("/fast");
                tokio::time::sleep(Duration::from_millis(if fast { 1 } else { delay })).await;
                let body = if error && !fast {
                    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"node error with secret"}}"#
                } else {
                    success_body
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://{address}")
}

fn observe(client: &BatchRpcClient) -> Arc<Mutex<Vec<EndpointObservation>>> {
    let rows = Arc::new(Mutex::new(Vec::new()));
    let sink = rows.clone();
    assert!(client.set_observer(Arc::new(move |row| sink.lock().unwrap().push(row))));
    rows
}

#[tokio::test]
async fn duplicate_host_paths_keep_ids_first_ack_and_detached_error() {
    let url = server(60, true).await;
    let client = BatchRpcClient::new(vec![format!("{url}/slow"), format!("{url}/fast")]).unwrap();
    let rows = observe(&client);
    let result = client.broadcast_transaction_detailed("0x01").await.unwrap();
    assert_eq!(result.first_success_endpoint_index, 1);
    tokio::time::sleep(Duration::from_millis(120)).await;
    let rows = rows.lock().unwrap();
    let mut ids: Vec<_> = rows
        .iter()
        .filter(|r| r.phase == "dispatch")
        .map(|r| r.endpoint_id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
    assert!(rows
        .iter()
        .any(|r| r.phase == "first_success" && r.endpoint_id == 1));
    assert_eq!(
        rows.iter()
            .filter(|r| r.is_first_result == Some(true))
            .count(),
        1
    );
    let late = rows
        .iter()
        .find(|r| r.phase == "detached_completion")
        .unwrap();
    assert_eq!((late.endpoint_id, late.result), (0, "rpc_error"));
    assert!(
        late.started_utc_ms.is_some()
            && late.completed_utc_ms.is_some()
            && late.elapsed_us.is_some()
    );
    let serialized = serde_json::to_string(&*rows).unwrap();
    for forbidden in ["127.0.0.1", "/slow", "/fast", "secret", "0x01"] {
        assert!(!serialized.contains(forbidden));
    }
}

#[tokio::test]
async fn all_timeout_is_uncertain_and_dispatch_has_unavailable_completion() {
    let url = server(100, false).await;
    let client = BatchRpcClient::new(vec![format!("{url}/a"), format!("{url}/b")])
        .unwrap()
        .with_send_timeout(Duration::from_millis(10));
    let rows = observe(&client);
    assert!(client.broadcast_transaction_detailed("0x01").await.is_err());
    let rows = rows.lock().unwrap();
    assert_eq!(
        rows.iter()
            .filter(|r| r.result == "timeout_uncertain")
            .count(),
        2
    );
    assert!(!rows.iter().any(|r| r.phase == "first_success"));
    for row in rows.iter().filter(|r| r.phase == "dispatch") {
        assert_eq!(row.elapsed_us, None);
        assert_eq!(row.completed_utc_ms, None);
    }
}

#[tokio::test]
async fn failed_observer_does_not_change_successful_send() {
    let url = server(1, false).await;
    let client = BatchRpcClient::new(vec![url]).unwrap();
    client.set_observer(Arc::new(|_| panic!("broken telemetry consumer")));
    assert!(client.broadcast_transaction_detailed("0x01").await.is_ok());
}

#[tokio::test]
async fn detached_ack_retains_its_own_completion_and_hash() {
    let url = server(60, false).await;
    let client = BatchRpcClient::new(vec![format!("{url}/slow"), format!("{url}/fast")]).unwrap();
    let rows = observe(&client);
    let result = client.broadcast_transaction_detailed("0x01").await.unwrap();
    assert_eq!(result.first_success_endpoint_index, 1);
    tokio::time::sleep(Duration::from_millis(120)).await;
    let rows = rows.lock().unwrap();
    let late = rows
        .iter()
        .find(|r| r.phase == "detached_completion")
        .unwrap();
    assert_eq!(late.result, "client_success");
    assert_eq!(late.tx_hash, alloy::primitives::keccak256([1]));
    assert_eq!(late.is_first_result, Some(false));
}

async fn assert_client_success(body: &'static str, expected_hash: &str) {
    let url = response_server(1, false, body).await;
    let client = BatchRpcClient::new(vec![url]).unwrap();
    let rows = observe(&client);
    let result = client.broadcast_transaction_detailed("0x01").await.unwrap();
    assert_eq!(result.tx_hash, expected_hash);
    assert_eq!(result.first_success_endpoint_index, 0);
    assert_eq!(result.failed_endpoints, 0);
    assert!(!result.connection_reused);
    let rows = rows.lock().unwrap();
    assert_eq!(rows.len(), 3);
    for phase in ["completion_before_return", "first_success"] {
        let row = rows.iter().find(|row| row.phase == phase).unwrap();
        assert_eq!(row.result, "client_success");
        assert_eq!(row.tx_hash, alloy::primitives::keccak256([1]));
    }
}

#[tokio::test]
async fn returned_hash_keeps_send_result_and_client_success_label() {
    assert_client_success(HASH_BODY, RETURNED_HASH).await;
}

#[tokio::test]
async fn null_result_keeps_local_hash_and_client_success_label() {
    let local_hash = format!("{:#x}", alloy::primitives::keccak256([1]));
    assert_client_success(r#"{"jsonrpc":"2.0","id":1,"result":null}"#, &local_hash).await;
}

#[tokio::test]
async fn missing_result_and_error_keeps_local_hash_and_client_success_label() {
    let local_hash = format!("{:#x}", alloy::primitives::keccak256([1]));
    assert_client_success(r#"{"jsonrpc":"2.0","id":1}"#, &local_hash).await;
}

#[tokio::test]
async fn closed_connections_report_only_a_passive_reuse_hint() {
    let url = server(1, false).await;
    let client = BatchRpcClient::new(vec![url]).unwrap();
    let rows = observe(&client);
    for expected_hint in [false, true] {
        let result = client.broadcast_transaction_detailed("0x01").await.unwrap();
        assert_eq!(result.tx_hash, RETURNED_HASH);
        assert_eq!(result.connection_reused, expected_hint);
        let mut rows = rows.lock().unwrap();
        assert_eq!(rows.len(), 3);
        for row in rows.iter() {
            let serialized = serde_json::to_value(row).unwrap();
            assert!(serialized.get("connection_reused").is_none());
            assert!(serialized.get("connection_reuse_hint").is_some());
            let hint = (row.phase != "dispatch").then_some(expected_hint);
            assert_eq!(serialized["connection_reuse_hint"], serde_json::json!(hint));
        }
        rows.clear();
    }
}

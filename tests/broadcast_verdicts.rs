// Real HTTP fan-out regressions for complete, redacted endpoint failure verdicts.
// Exercises both BatchRpcClient APIs using isolated loopback servers and tracing capture.
use fast_wallet::error::{BroadcastFailure, EndpointFailureClass};
use fast_wallet::rpc::{endpoint_host, BatchRpcClient};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn rejecting_endpoint(delay_ms: u64, message: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = [0; 4096];
        stream.read(&mut buf).await.expect("read");
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let body = serde_json::json!({"jsonrpc":"2.0", "id":1,
            "error":{"code":-32000, "message":message}})
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.expect("reply");
    });
    format!("http://{addr}/SYNTHETIC_REDACTION_TOKEN")
}

async fn assert_complete_failure(detailed: bool) {
    let first = rejecting_endpoint(5, "insufficient funds https://rpc.example/SYNTHETIC_KEY").await;
    let last = rejecting_endpoint(40, "nonce too low").await;
    let client = BatchRpcClient::new(vec![first.clone(), last.clone()]).expect("client");
    let error = if detailed {
        client
            .broadcast_transaction_detailed("0x01")
            .await
            .expect_err("all fail")
    } else {
        client
            .broadcast_transaction("0x01")
            .await
            .expect_err("all fail")
    };
    let text = format!("wallet error: send_presigned failed: {error}");
    let failure = BroadcastFailure::from_error_text(&text)
        .expect("valid JSON")
        .expect("verdicts");
    assert_eq!(failure.endpoints.len(), 2, "{text}");
    for (url, message, minimum_ms) in [
        (&first, "insufficient funds", 5),
        (&last, "nonce too low", 40),
    ] {
        let verdict = failure
            .endpoints
            .iter()
            .find(|v| v.host == endpoint_host(url))
            .expect("host");
        assert!(verdict.error.contains(message));
        assert!(verdict.elapsed_ms >= minimum_ms);
        assert_eq!(verdict.class, EndpointFailureClass::Rejection);
    }
    assert!(!text.contains("SYNTHETIC"), "{text}");
}

#[tokio::test]
async fn detailed_all_fail_returns_and_logs_every_endpoint() {
    let capture = LogCapture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    assert_complete_failure(true).await;
    let logs = String::from_utf8(capture.0.lock().expect("log lock").clone()).expect("UTF-8");
    assert!(logs.contains("all broadcast endpoints rejected"), "{logs}");
    assert!(
        logs.contains("insufficient funds") && logs.contains("nonce too low"),
        "{logs}"
    );
    assert!(logs.contains("elapsed_ms"), "{logs}");
    assert!(!logs.contains("SYNTHETIC"), "{logs}");
}

#[tokio::test]
async fn simple_all_fail_returns_every_endpoint() {
    assert_complete_failure(false).await;
}

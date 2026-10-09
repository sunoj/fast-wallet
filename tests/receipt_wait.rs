// Loopback regressions for `RpcClient::wait_for_receipt` across transient receipt-fetch errors.
// A scripted JSON-RPC mock answers each poll in order; no external RPC is involved.
use alloy::primitives::B256;
use fast_wallet::error::WalletError;
use fast_wallet::rpc::RpcClient;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const POLL: Duration = Duration::from_millis(50);

#[derive(Clone, Copy)]
enum Reply {
    /// HTTP 200 whose body is not JSON: `error decoding response body`.
    Garbled,
    /// `result: null`, the normal answer for a pending transaction.
    Pending,
    Receipt,
}

struct Script {
    url: String,
    polls: Arc<Mutex<usize>>,
}

/// Serve `replies` in order, then `Pending` forever.
async fn script(replies: Vec<Reply>) -> Script {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    let queue = Arc::new(Mutex::new(VecDeque::from(replies)));
    let polls = Arc::new(Mutex::new(0));
    let counter = polls.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve(stream, queue.clone(), counter.clone()));
        }
    });
    Script { url, polls }
}

async fn serve(stream: TcpStream, queue: Arc<Mutex<VecDeque<Reply>>>, polls: Arc<Mutex<usize>>) {
    let mut stream = BufReader::new(stream);
    while let Some(body) = read_request(&mut stream).await {
        let request: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        *polls.lock() += 1;
        let reply = queue.lock().pop_front().unwrap_or(Reply::Pending);
        let body = match reply {
            Reply::Garbled => "<html>bad gateway</html>".to_string(),
            Reply::Pending => format!(
                r#"{{"jsonrpc":"2.0","id":{},"result":null}}"#,
                request["id"]
            ),
            Reply::Receipt => format!(
                r#"{{"jsonrpc":"2.0","id":{},"result":{{"status":"0x1"}}}}"#,
                request["id"]
            ),
        };
        let reply = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        if stream.get_mut().write_all(reply.as_bytes()).await.is_err() {
            return;
        }
    }
}

async fn read_request(stream: &mut BufReader<TcpStream>) -> Option<Vec<u8>> {
    let mut length = 0;
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let line = line.trim_end().to_ascii_lowercase();
        if line.is_empty() {
            break;
        }
        if let Some(value) = line.strip_prefix("content-length:") {
            length = value.trim().parse().ok()?;
        }
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.ok()?;
    Some(body)
}

async fn wait(mock: &Script, timeout: Duration) -> Result<serde_json::Value, WalletError> {
    let client = RpcClient::new(mock.url.clone()).expect("client");
    client
        .wait_for_receipt(B256::repeat_byte(0x11), timeout, POLL)
        .await
}

#[tokio::test]
async fn one_decode_error_then_receipt_succeeds() {
    let mock = script(vec![Reply::Garbled, Reply::Receipt]).await;
    let receipt = wait(&mock, Duration::from_secs(5)).await.expect("receipt");
    assert_eq!(receipt["status"], "0x1");
    assert_eq!(*mock.polls.lock(), 2);
}

#[tokio::test]
async fn pending_poll_resets_the_error_count() {
    use Reply::*;
    let mock = script(vec![Garbled, Garbled, Pending, Garbled, Garbled, Receipt]).await;
    let receipt = wait(&mock, Duration::from_secs(5)).await.expect("receipt");
    assert_eq!(receipt["status"], "0x1");
    assert_eq!(*mock.polls.lock(), 6);
}

#[tokio::test]
async fn three_consecutive_errors_return_the_last_error() {
    let mock = script(vec![Reply::Garbled; 3]).await;
    let start = Instant::now();
    let error = wait(&mock, Duration::from_secs(5))
        .await
        .expect_err("dead endpoint");
    let elapsed = start.elapsed();
    assert!(
        matches!(&error, WalletError::RpcError(msg) if msg.contains("decoding")),
        "unexpected error: {error}"
    );
    assert_eq!(*mock.polls.lock(), 3);
    assert!(elapsed >= 2 * POLL, "returned after {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "returned after {elapsed:?}"
    );
}

#[tokio::test]
async fn pending_until_deadline_times_out() {
    let mock = script(Vec::new()).await;
    let error = wait(&mock, Duration::from_millis(200))
        .await
        .expect_err("never mined");
    assert!(
        matches!(error, WalletError::Timeout),
        "unexpected error: {error}"
    );
    assert!(*mock.polls.lock() >= 2);
}

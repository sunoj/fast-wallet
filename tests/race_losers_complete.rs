// A first-success race must let the losing endpoints' sends run to their end.
// The slow endpoint answers only after the race returned, if its client is still waiting.
use fast_wallet::broadcast::{BroadcastStrategy, RpcEndpoint, TransactionBroadcaster};
use fast_wallet::rpc::BatchRpcClient;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::time::timeout;

const HASH_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"result":"0x1111111111111111111111111111111111111111111111111111111111111111"}"#;
const REJECT_BODY: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"insufficient funds"}}"#;
/// Time a dropped client has to show its EOF once the race has returned.
const GRACE: Duration = Duration::from_millis(250);
const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    /// Debug events of this test's (current-thread) runtime, including spawned tasks.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Self::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }

    async fn wait_for(&self, needle: &str) {
        let deadline = Instant::now() + WATCHDOG;
        loop {
            let text = String::from_utf8_lossy(&self.0.lock().expect("log lock")).into_owned();
            if text.contains(needle) {
                return;
            }
            assert!(Instant::now() < deadline, "no {needle:?} in:\n{text}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn read_request(stream: &mut BufReader<TcpStream>) -> bool {
    let mut length = 0;
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
            return false;
        }
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    stream.read_exact(&mut vec![0; length]).await.is_ok()
}

async fn answer(stream: &mut BufReader<TcpStream>, body: &str) -> bool {
    let reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.get_mut().write_all(reply.as_bytes()).await.is_ok()
}

/// Answers each whole request with `body`, after `gate` is notified when one is given.
async fn quick_endpoint(body: &'static str, gate: Option<Arc<Notify>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let gate = gate.clone();
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                if read_request(&mut stream).await {
                    if let Some(gate) = gate {
                        gate.notified().await;
                    }
                    answer(&mut stream, body).await;
                }
            });
        }
    });
    format!("http://{addr}")
}

/// Takes one whole request and notifies `arrived`, then holds it until released.
struct Slow {
    url: String,
    release: oneshot::Sender<()>,
    answered: oneshot::Receiver<bool>,
}

impl Slow {
    async fn start(arrived: Arc<Notify>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let (release, released) = oneshot::channel();
        let (report, answered) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut stream = BufReader::new(stream);
            assert!(read_request(&mut stream).await, "whole request");
            arrived.notify_one();
            let _ = released.await;
            let mut probe = [0; 1];
            let waiting = timeout(GRACE, stream.read(&mut probe)).await.is_err();
            let _ = report.send(waiting && answer(&mut stream, HASH_BODY).await);
        });
        Self {
            url,
            release,
            answered,
        }
    }

    /// Call once the race has returned: the held send must still be waiting, and its
    /// client must read the late answer.
    async fn answers_after_return(self, logs: &Logs) {
        self.release
            .send(())
            .expect("slow endpoint holds its request");
        let answered = timeout(WATCHDOG, self.answered).await.expect("watchdog");
        assert!(
            answered.expect("slow endpoint report"),
            "the losing send was dropped before its endpoint answered"
        );
        logs.wait_for("late broadcast endpoint accepted").await;
    }
}

async fn assert_broadcaster_loser_finishes(strategy: BroadcastStrategy, private: bool) {
    let (logs, _guard) = Logs::capture();
    let arrived = Arc::new(Notify::new());
    let slow = Slow::start(arrived.clone()).await;
    let fast = quick_endpoint(HASH_BODY, Some(arrived)).await;
    let endpoint = |url: String| {
        if private {
            RpcEndpoint::private(url, "k")
        } else {
            RpcEndpoint::public(url)
        }
    };
    let mut endpoints = vec![endpoint(slow.url.clone()), endpoint(fast)];
    if strategy == BroadcastStrategy::PrivateFirst && !private {
        let rejecting = quick_endpoint(REJECT_BODY, None).await;
        endpoints.push(RpcEndpoint::private(rejecting, "k"));
    }
    let broadcaster = TransactionBroadcaster::new(endpoints)
        .expect("broadcaster")
        .with_strategy(strategy);
    let result = timeout(WATCHDOG, broadcaster.broadcast_raw("0x01"))
        .await
        .expect("race returned while the slow endpoint held its answer");
    assert!(result.tx_hash.is_some(), "{:?}", result.errors);
    slow.answers_after_return(&logs).await;
}

#[tokio::test]
async fn broadcaster_race_lets_the_slow_endpoint_answer() {
    assert_broadcaster_loser_finishes(BroadcastStrategy::RaceAll, false).await;
}

#[tokio::test]
async fn broadcaster_private_first_lets_the_slow_relay_answer() {
    assert_broadcaster_loser_finishes(BroadcastStrategy::PrivateFirst, true).await;
}

#[tokio::test]
async fn broadcaster_public_fallback_lets_the_slow_endpoint_answer() {
    assert_broadcaster_loser_finishes(BroadcastStrategy::PrivateFirst, false).await;
}

#[tokio::test]
async fn private_first_success_still_skips_public_endpoints() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let public = format!("http://{}", listener.local_addr().expect("address"));
    let broadcaster = TransactionBroadcaster::new(vec![
        RpcEndpoint::private(quick_endpoint(HASH_BODY, None).await, "k"),
        RpcEndpoint::public(public),
    ])
    .expect("broadcaster")
    .with_strategy(BroadcastStrategy::PrivateFirst);
    let result = broadcaster.broadcast_raw("0x01").await;
    assert!(result.tx_hash.is_some(), "{:?}", result.errors);
    let contacted = timeout(Duration::from_millis(200), listener.accept()).await;
    assert!(
        contacted.is_err(),
        "a private success must not reach public endpoints"
    );
}

#[tokio::test]
async fn batch_client_lets_the_slow_endpoint_answer() {
    let (logs, _guard) = Logs::capture();
    let arrived = Arc::new(Notify::new());
    let slow = Slow::start(arrived.clone()).await;
    let fast = quick_endpoint(HASH_BODY, Some(arrived)).await;
    let client = BatchRpcClient::new(vec![slow.url.clone(), fast]).expect("client");
    let result = timeout(WATCHDOG, client.broadcast_transaction_detailed("0x01"))
        .await
        .expect("race returned while the slow endpoint held its answer")
        .expect("first success");
    assert_eq!(result.first_success_endpoint_index, 1);
    slow.answers_after_return(&logs).await;
}

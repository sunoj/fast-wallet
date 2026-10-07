// Keep-alive HTTP/1.1 JSON-RPC mock for the warm-connection tests, plus a tokio clock helper.
// Exports Mock (connection and call counters, send-failure modes, optional idle close) and
// idle_for; loopback only.
use alloy::primitives::keccak256;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub const ANSWER: u8 = 0;
pub const DROP_NEXT_SEND: u8 = 1;
pub const HANG_SENDS: u8 = 2;
/// Answer the next send with its headers and half a body, then close.
pub const CUT_NEXT_BODY: u8 = 3;
/// Leave the next read (any method but a send or `eth_chainId`) unanswered.
pub const HANG_NEXT_READ: u8 = 4;

/// JSON-RPC endpoint on loopback that keeps connections alive and records traffic.
#[derive(Clone)]
pub struct Mock {
    pub url: String,
    connections: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<(String, String)>>>,
    pub mode: Arc<AtomicU8>,
    idle_close: Option<Duration>,
}

impl Mock {
    pub async fn start() -> Self {
        Self::start_with(None).await
    }

    /// A server that closes (FIN) a connection idle for `idle` of tokio time.
    pub async fn closing_idle_after(idle: Duration) -> Self {
        Self::start_with(Some(idle)).await
    }

    async fn start_with(idle_close: Option<Duration>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let mock = Mock {
            url: format!("http://{}", listener.local_addr().expect("address")),
            connections: Arc::default(),
            calls: Arc::default(),
            mode: Arc::new(AtomicU8::new(ANSWER)),
            idle_close,
        };
        let server = mock.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                server.connections.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(server.clone().serve(stream));
            }
        });
        mock
    }

    async fn serve(self, stream: TcpStream) {
        let mut stream = BufReader::new(stream);
        while let Some(body) = self.next_request(&mut stream).await {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("json body");
            let method = request["method"].as_str().unwrap_or_default().to_string();
            let param = request["params"][0]
                .as_str()
                .unwrap_or_default()
                .to_string();
            self.calls.lock().push((method.clone(), param.clone()));
            let result = match method.as_str() {
                "eth_sendRawTransaction" => match self.mode.swap(ANSWER, Ordering::SeqCst) {
                    DROP_NEXT_SEND => return,
                    HANG_SENDS => {
                        self.mode.store(HANG_SENDS, Ordering::SeqCst);
                        return std::future::pending().await;
                    }
                    CUT_NEXT_BODY => {
                        let head = "HTTP/1.1 200 OK\r\ncontent-length: 64\r\n\r\n{\"jsonrpc\"";
                        let _ = stream.get_mut().write_all(head.as_bytes()).await;
                        return;
                    }
                    _ => format!("{}", keccak256(hex::decode(&param[2..]).expect("hex"))),
                },
                "eth_chainId" => "0x1".to_string(),
                _ if self.take_mode(HANG_NEXT_READ) => return std::future::pending().await,
                _ => "0x0".to_string(),
            };
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":{},"result":"{result}"}}"#,
                request["id"]
            );
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            if stream.get_mut().write_all(reply.as_bytes()).await.is_err() {
                return;
            }
        }
    }

    async fn next_request(&self, stream: &mut BufReader<TcpStream>) -> Option<Vec<u8>> {
        match self.idle_close {
            Some(idle) => tokio::time::timeout(idle, read_request(stream))
                .await
                .ok()?,
            None => read_request(stream).await,
        }
    }

    fn take_mode(&self, mode: u8) -> bool {
        let swap = self
            .mode
            .compare_exchange(mode, ANSWER, Ordering::SeqCst, Ordering::SeqCst);
        swap.is_ok()
    }

    pub fn count(&self, method: &str) -> usize {
        self.calls
            .lock()
            .iter()
            .filter(|(m, _)| m == method)
            .count()
    }

    pub fn sent(&self) -> Vec<String> {
        let calls = self.calls.lock();
        let sends = calls.iter().filter(|(m, _)| m == "eth_sendRawTransaction");
        sends.map(|(_, raw)| raw.clone()).collect()
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

/// One request body, or `None` once the client closes the connection.
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

/// Let `idle` of tokio time pass at once, then resume real time for loopback I/O.
pub async fn idle_for(idle: Duration) {
    tokio::time::pause();
    tokio::time::advance(idle).await;
    tokio::time::resume();
}

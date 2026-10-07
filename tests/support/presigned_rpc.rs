//! Controlled loopback JSON-RPC for presigned rejection regressions.
//! Sends wait for a test-provided response to exercise single and batch outcomes.
//! Exports RPC, wallet, request, and send helpers using synthetic fixtures.

use alloy::primitives::{keccak256, Address, U256};
use fast_wallet::{FastWallet, FastWalletBuilder, Transaction, TransactionRequest, WalletError};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const NONCE: u64 = 42;
pub const KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const DEFINITIVE: &str = "insufficient funds for gas * price + value";
pub const AMBIGUOUS: &str = "replacement transaction underpriced";

pub enum Reply {
    Error(&'static str),
    Disconnect,
    Accept,
}

pub struct Rpc {
    pub url: String,
    calls: mpsc::UnboundedReceiver<oneshot::Sender<Reply>>,
    task: JoinHandle<()>,
}

impl Rpc {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, calls) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, sender.clone()));
            }
        });
        Self { url, calls, task }
    }

    pub async fn next_send(&mut self) -> oneshot::Sender<Reply> {
        tokio::time::timeout(Duration::from_secs(3), self.calls.recv())
            .await
            .expect("RPC send deadline")
            .expect("RPC send")
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(stream: TcpStream, sends: mpsc::UnboundedSender<oneshot::Sender<Reply>>) {
    let mut stream = BufReader::new(stream);
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    let id = request["id"].clone();
    let body = match request["method"].as_str().unwrap() {
        "eth_sendRawTransaction" => {
            let (sender, receiver) = oneshot::channel();
            if sends.send(sender).is_err() {
                return;
            }
            match receiver.await {
                Ok(Reply::Error(message)) => serde_json::json!({"jsonrpc":"2.0", "id":id,
                    "error":{"code":-32000, "message":message}}),
                Ok(Reply::Accept) => {
                    let raw = request["params"][0].as_str().unwrap();
                    serde_json::json!({"jsonrpc":"2.0", "id":id,
                        "result":format!("{}", keccak256(hex::decode(&raw[2..]).unwrap()))})
                }
                _ => return,
            }
        }
        "eth_getTransactionCount" => serde_json::json!({"jsonrpc":"2.0", "id":id,
            "result":format!("0x{:x}", NONCE)}),
        "eth_getTransactionByHash" => serde_json::json!({"jsonrpc":"2.0", "id":id, "result":null}),
        _ => serde_json::json!({"jsonrpc":"2.0", "id":id, "result":"0x1"}),
    }
    .to_string();
    let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    let _ = stream.get_mut().write_all(reply.as_bytes()).await;
}

async fn read_request(stream: &mut BufReader<TcpStream>) -> Option<serde_json::Value> {
    let mut length = 0;
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        if line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().ok()?;
        }
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.ok()?;
    serde_json::from_slice(&body).ok()
}

pub fn wallet(rpc: &Rpc, batch: Vec<String>) -> Arc<FastWallet> {
    let mut builder = FastWalletBuilder::new(KEY, &rpc.url).chain_id(1);
    if !batch.is_empty() {
        builder = builder.broadcast_rpcs_exclusive(batch);
    }
    Arc::new(builder.build_with_nonce(NONCE).unwrap())
}

pub fn request() -> TransactionRequest {
    TransactionRequest::new()
        .to(Address::repeat_byte(7))
        .gas_limit(21_000)
        .max_fee_per_gas(U256::from(100u64))
        .max_priority_fee_per_gas(U256::from(10u64))
}

pub fn send(
    wallet: &Arc<FastWallet>,
    tx: &Transaction,
    detailed: bool,
) -> JoinHandle<Result<(), WalletError>> {
    let (wallet, tx) = (wallet.clone(), tx.clone());
    tokio::spawn(async move {
        if detailed {
            wallet.send_signed_detailed(&tx, 0.0).await.map(|_| ())
        } else {
            wallet.send_signed(&tx).await.map(|_| ())
        }
    })
}

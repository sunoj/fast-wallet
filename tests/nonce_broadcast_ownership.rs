//! Regression for nonce ownership after an ambiguous broadcast failure.
//! Exercises concurrent `FastWallet` reservations and the real `send_signed` path.
//! Deps: a local JSON-RPC stub returning Base's replacement-underpriced error.

use alloy::primitives::{Address, Bytes, U256};
use fast_wallet::{FastWallet, FastWalletBuilder, ReservedNonce, TransactionRequest};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};

const INITIAL_NONCE: u64 = 9_169;
const TEST_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn underpriced_rpc() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock RPC");
    let url = format!("http://{}", listener.local_addr().expect("mock RPC address"));
    let task = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept wallet request");
        read_http_request(&mut stream);
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"replacement transaction underpriced"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(), body
        );
        stream.write_all(response.as_bytes()).expect("write RPC response");
    });
    (url, task)
}

fn read_http_request(stream: &mut impl Read) {
    let mut request = Vec::new();
    let mut chunk = [0u8; 2_048];
    loop {
        let read = stream.read(&mut chunk).expect("read wallet request");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    assert!(request.windows(4).any(|window| window == b"\r\n\r\n"));
}

fn reserve_concurrently(wallet: &Arc<FastWallet>) -> (ReservedNonce, ReservedNonce) {
    let barrier = Arc::new(Barrier::new(3));
    let reserve = |wallet: Arc<FastWallet>, barrier: Arc<Barrier>| {
        thread::spawn(move || {
            barrier.wait();
            wallet.reserve_nonce()
        })
    };
    let first = reserve(wallet.clone(), barrier.clone());
    let second = reserve(wallet.clone(), barrier.clone());
    barrier.wait();
    (first.join().expect("first reservation"), second.join().expect("second reservation"))
}

fn sign_probe(wallet: &FastWallet, nonce: u64, marker: u8) -> fast_wallet::Transaction {
    let request = TransactionRequest::new()
        .to(Address::repeat_byte(marker))
        .data(Bytes::from(vec![marker]))
        .gas_limit(2_000_000)
        .max_fee_per_gas(U256::from(20_000_000u64))
        .max_priority_fee_per_gas(U256::from(10_000_000u64));
    wallet.sign_with_nonce(request, nonce).expect("sign probe")
}

#[tokio::test]
async fn failed_broadcast_never_recycles_nonce_into_concurrent_stream() {
    let (rpc_url, rpc_task) = underpriced_rpc();
    let wallet = Arc::new(
        FastWalletBuilder::new(TEST_PRIVATE_KEY, rpc_url)
            .chain_id(8_453)
            .build_with_nonce(INITIAL_NONCE)
            .expect("build wallet"),
    );
    let (a, b) = reserve_concurrently(&wallet);
    let (mut failed, concurrent) = if a.nonce() < b.nonce() { (a, b) } else { (b, a) };
    assert_eq!((failed.nonce(), concurrent.nonce()), (INITIAL_NONCE, INITIAL_NONCE + 1));

    let failed_tx = sign_probe(&wallet, failed.nonce(), 0xaa);
    let concurrent_tx = sign_probe(&wallet, concurrent.nonce(), 0xbb);
    assert!(failed.mark_broadcasting(), "signed nonce must enter broadcast ownership");
    let error = wallet.send_signed(&failed_tx).await.expect_err("mock rejects broadcast");
    assert!(error.to_string().contains("replacement transaction underpriced"));
    assert!(!failed.release(), "a broadcasting reservation cannot be manually recycled");
    rpc_task.join().expect("mock RPC task");

    let later = wallet.reserve_nonce();
    let later_tx = sign_probe(&wallet, later.nonce(), 0xcc);
    assert_ne!(later_tx.hash(), failed_tx.hash(), "later probe must be a different transaction");
    assert_eq!(concurrent_tx.nonce(), INITIAL_NONCE + 1);
    assert_eq!(
        later.nonce(),
        INITIAL_NONCE + 2,
        "a nonce handed to send_signed may be on the wire and must not enter gaps"
    );
}

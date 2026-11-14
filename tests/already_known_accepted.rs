//! An endpoint answering "already known" holds these exact signed bytes: the send succeeded.
//! Covers single and batch wallet sends, early return past a hung endpoint, and exact matching.

#[allow(dead_code)]
#[path = "support/presigned_rpc.rs"]
mod rpc;

use alloy::primitives::{keccak256, B256};
use fast_wallet::broadcast::{RpcEndpoint, TransactionBroadcaster};
use fast_wallet::rpc::{observation::EndpointObservation, BatchRpcClient, RpcClient};
use fast_wallet::{FastWallet, Transaction, WalletError};
use rpc::{request, wallet, Reply, Rpc, NONCE};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Well under the 6 s per-endpoint send timeout a hung endpoint would hold the call for.
const EARLY: Duration = Duration::from_secs(2);

fn send_hash(
    wallet: &Arc<FastWallet>,
    tx: &Transaction,
    detailed: bool,
) -> JoinHandle<Result<B256, WalletError>> {
    let (wallet, tx) = (wallet.clone(), tx.clone());
    tokio::spawn(async move {
        if detailed {
            let result = wallet.send_signed_detailed(&tx, 0.0).await?;
            Ok(result.tx_hash.parse().expect("hash"))
        } else {
            wallet.send_signed(&tx).await
        }
    })
}

async fn finish(sending: JoinHandle<Result<B256, WalletError>>) -> Result<B256, WalletError> {
    tokio::time::timeout(EARLY, sending)
        .await
        .expect("send returned before the hung endpoint's timeout")
        .expect("send task")
}

fn assert_accepted(wallet: &FastWallet, tx: &Transaction, result: Result<B256, WalletError>) {
    assert_eq!(result.expect("already known is a send"), tx.hash());
    assert_eq!(wallet.inflight_count(), 1);
    assert_eq!(wallet.sign(request()).unwrap().nonce(), NONCE + 1);
}

#[tokio::test]
async fn batch_already_known_returns_without_waiting_for_a_hung_endpoint() {
    for detailed in [false, true] {
        let mut known = Rpc::start().await;
        let mut hung = Rpc::start().await;
        let mut also_hung = Rpc::start().await;
        let urls = vec![known.url.clone(), hung.url.clone(), also_hung.url.clone()];
        let wallet = wallet(&known, urls);
        let tx = wallet.sign(request()).unwrap();
        let sending = send_hash(&wallet, &tx, detailed);
        // Held unanswered until the iteration ends: both endpoints hang.
        let _held = (hung.next_send().await, also_hung.next_send().await);
        let reply = known.next_send().await;
        assert!(reply.send(Reply::Error("already known")).is_ok());
        assert_accepted(&wallet, &tx, finish(sending).await);
    }
}

/// Sends one signed tx through a single-endpoint wallet answered with `message(tx hash)`.
async fn single_send(
    message: fn(B256) -> String,
) -> (Arc<FastWallet>, Transaction, Result<B256, WalletError>) {
    let mut rpc = Rpc::start().await;
    let wallet = wallet(&rpc, vec![]);
    let tx = wallet.sign(request()).unwrap();
    let message: &'static str = Box::leak(message(tx.hash()).into_boxed_str());
    let sending = send_hash(&wallet, &tx, false);
    assert!(rpc.next_send().await.send(Reply::Error(message)).is_ok());
    let outcome = finish(sending).await;
    (wallet, tx, outcome)
}

#[tokio::test]
async fn single_endpoint_known_transaction_phrases_are_sends() {
    let phrases: [fn(B256) -> String; 5] = [
        |_| "already known".into(),
        |_| "Already Known".into(),
        |_| "Known transaction".into(),
        |hash| format!("known transaction: {}", hex::encode(hash)),
        |hash| format!("known transaction: {hash:#x}"),
    ];
    for phrase in phrases {
        let (wallet, tx, outcome) = single_send(phrase).await;
        assert_accepted(&wallet, &tx, outcome);
    }
}

#[tokio::test]
async fn messages_that_only_contain_the_phrase_stay_errors() {
    let messages: [fn(B256) -> String; 7] = [
        |_| "unknown transaction".into(),
        |_| "execution reverted: already known".into(),
        |_| "already known: gas price below minimum".into(),
        |_| "transaction already known to be invalid".into(),
        |_| format!("known transaction: {}", hex::encode(B256::repeat_byte(9))),
        |hash| format!("known transaction: {}; nonce too low", hex::encode(hash)),
        |hash| format!("known transaction: {}", &hex::encode(hash)[..62]),
    ];
    for message in messages {
        let (wallet, tx, outcome) = single_send(message).await;
        let error = outcome.expect_err("a partial match must stay an error");
        assert!(error.to_string().contains(&message(tx.hash())), "{error}");
        assert_eq!(
            wallet.inflight_count(),
            1,
            "{error}: the nonce stays consumed"
        );
    }
}

/// One loopback endpoint answering a single request with `body`.
async fn answer_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = [0; 4096];
        let _ = stream.read(&mut buf).await;
        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(reply.as_bytes()).await;
    });
    url
}

const KNOWN: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"already known"}}"#;
const KNOWN_WITH_DATA: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"already known","data":"0x01"}}"#;

#[tokio::test]
async fn an_already_known_error_carrying_data_stays_an_error() {
    let client = RpcClient::new(answer_once(KNOWN_WITH_DATA).await).unwrap();
    let error = client.send_raw_transaction("0x01").await.expect_err("data");
    assert!(error.to_string().contains("data=0x01"), "{error}");
    let url = answer_once(KNOWN_WITH_DATA).await;
    let broadcaster = TransactionBroadcaster::new(vec![RpcEndpoint::public(url)]).unwrap();
    let result = broadcaster.broadcast_raw("0x01").await;
    assert_eq!((result.tx_hash, result.failure_count), (None, 1));
}

#[tokio::test]
async fn already_known_observation_is_a_success_that_keeps_its_rpc_code() {
    let client = BatchRpcClient::new(vec![answer_once(KNOWN).await]).unwrap();
    let rows = Arc::new(Mutex::new(Vec::new()));
    let sink = rows.clone();
    assert!(
        client.set_observer(Arc::new(move |row: EndpointObservation| {
            sink.lock().unwrap().push(row)
        }))
    );
    let result = client.broadcast_transaction_detailed("0x01").await;
    assert_eq!(
        result.expect("send").tx_hash,
        format!("{:#x}", keccak256([1]))
    );
    let rows = rows.lock().unwrap();
    let done = rows
        .iter()
        .find(|row| row.phase == "completion_before_return")
        .expect("completion row");
    assert_eq!(done.result, "client_success");
    assert_eq!((done.rpc_code, done.cause), (Some(-32000), None));
}

#[tokio::test]
async fn broadcaster_already_known_reports_the_local_hash() {
    let url = answer_once(KNOWN).await;
    let broadcaster = TransactionBroadcaster::new(vec![RpcEndpoint::public(url)]).unwrap();
    let result = broadcaster.broadcast_raw("0x01").await;
    assert_eq!(result.tx_hash, Some(keccak256([1])), "{:?}", result.errors);
    assert_eq!(result.failure_count, 0);
}

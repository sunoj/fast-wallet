// Loopback regressions for the send deadline, early success, and nonce reuse after timeout.
// Uses public wallet and batch APIs; no external RPC or real transactions are involved.
use alloy::primitives::{Address, U256};
use fast_wallet::error::{EndpointFailureClass, WalletError};
use fast_wallet::rpc::{endpoint_host, BatchRpcClient};
use fast_wallet::{FastWalletBuilder, TransactionRequest, WalletConfig};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const DEADLINE: Duration = Duration::from_millis(100);
const TEST_PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

async fn endpoint(body: Option<&'static str>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("address");
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buf = [0; 4096];
        stream.read(&mut buf).await.expect("request");
        if let Some(body) = body {
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).await.expect("reply");
        } else {
            let mut remaining = Vec::new();
            stream
                .read_to_end(&mut remaining)
                .await
                .expect("client disconnect");
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn stalled_endpoint_returns_timeout_verdict_near_configured_bound() {
    let rejects = endpoint(Some(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Post sequencer: deadline exceeded"}}"#)).await;
    let hangs = endpoint(None).await;
    let client = BatchRpcClient::new(vec![rejects, hangs.clone()])
        .expect("client")
        .with_send_timeout(DEADLINE);
    let start = Instant::now();
    let error = client
        .broadcast_transaction_detailed("0x01")
        .await
        .expect_err("all fail");
    let WalletError::BroadcastFailed(failure) = error else {
        panic!("missing verdicts: {error}")
    };
    assert_eq!(failure.endpoints.len(), 2);
    let timeout = failure
        .endpoints
        .iter()
        .find(|v| v.host == endpoint_host(&hangs))
        .expect("hung endpoint");
    assert_eq!(timeout.class, EndpointFailureClass::Transport);
    assert_eq!(timeout.error, "timeout after 100 ms");
    assert!(timeout.elapsed_ms >= 100);
    assert!(start.elapsed() >= DEADLINE && start.elapsed() < Duration::from_secs(2));
    assert_eq!(failure.endpoints[0].class, EndpointFailureClass::Rejection);
}

#[tokio::test]
async fn first_success_does_not_wait_for_a_stalled_endpoint() {
    let accepts = endpoint(Some(r#"{"jsonrpc":"2.0","id":1,"result":"0x1111111111111111111111111111111111111111111111111111111111111111"}"#)).await;
    let hangs = endpoint(None).await;
    let client = BatchRpcClient::new(vec![hangs, accepts])
        .expect("client")
        .with_send_timeout(Duration::from_secs(2));
    let result = tokio::time::timeout(Duration::from_secs(1), client.broadcast_transaction("0x01"))
        .await
        .expect("returns before deadline")
        .expect("accepted");
    assert_eq!(result, alloy::primitives::B256::repeat_byte(0x11));
}

fn request() -> TransactionRequest {
    TransactionRequest::new()
        .to(Address::repeat_byte(1))
        .value(U256::ZERO)
        .gas_limit(21000)
        .gas_price(U256::from(1))
}

#[tokio::test]
async fn timed_out_presigned_send_retains_nonce_after_broadcasting_context_was_dropped() {
    let primary = endpoint(None).await;
    let second = endpoint(None).await;
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary)
        .broadcast_rpcs(vec![second])
        .config(WalletConfig {
            broadcast_send_timeout: DEADLINE,
            ..WalletConfig::default()
        })
        .build_with_nonce(100)
        .expect("wallet");
    let ctx = wallet.preheat_warmup_only(false).await.expect("preheat");
    let tx = wallet.sign_with_preheat(&ctx, request()).expect("signed");
    wallet
        .mark_preheat_broadcasting(&ctx)
        .expect("broadcasting");
    drop(ctx);
    assert_eq!(
        wallet.effective_next_nonce(),
        101,
        "drop retains Broadcasting reservation"
    );
    let start = Instant::now();
    let error = wallet
        .send_signed_detailed(&tx, 0.0)
        .await
        .expect_err("timeout");
    assert!(matches!(error, WalletError::BroadcastFailed(_)));
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(
        wallet.effective_next_nonce(),
        101,
        "timeout may hide acceptance, so the nonce must remain consumed"
    );
    assert_eq!(wallet.sign(request()).expect("next nonce").nonce(), 101);
}

#[test]
fn default_allows_forwarder_verdicts_before_ending_thirty_second_tail() {
    assert_eq!(
        WalletConfig::default().broadcast_send_timeout,
        Duration::from_secs(6)
    );
}

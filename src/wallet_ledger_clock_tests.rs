// Ledger clock and request-timeout regressions using real wallet construction.
// Covers wallet isolation, age gates, replacement cooldown and RPC endpoint plumbing.
// Deps: parent wallet tests, loopback sockets and std time.

#[test]
fn default_rpc_request_timeout_is_thirty_seconds() {
    assert_eq!(
        WalletConfig::default().rpc_request_timeout,
        Duration::from_secs(30)
    );
}

#[cfg(feature = "test-util")]
#[test]
fn ledger_clock_advance_is_per_wallet_and_covers_stall_window() {
    let first = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .build_with_nonce(0)
        .unwrap();
    let second = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .build_with_nonce(0)
        .unwrap();
    first.sign(test_request()).unwrap();
    second.sign(test_request()).unwrap();
    assert!(!nonce_stalled(
        1,
        0,
        first.lowest_unresolved_inflight(0).as_ref()
    ));
    *first.last_stall_replace.lock() = Some((0, first.inflight_nonces.ledger_now()));
    first.advance_ledger_clock(Duration::from_secs(31));
    assert!(first.lowest_unresolved_inflight(0).unwrap().age >= Duration::from_secs(30));
    assert!(second.lowest_unresolved_inflight(0).unwrap().age < Duration::from_secs(30));
    assert!(nonce_stalled(
        1,
        0,
        first.lowest_unresolved_inflight(0).as_ref()
    ));
    assert!(!nonce_stalled(
        1,
        0,
        second.lowest_unresolved_inflight(0).as_ref()
    ));
    assert!(
        first
            .inflight_nonces
            .ledger_now()
            .saturating_duration_since(first.last_stall_replace.lock().unwrap().1)
            >= STALL_REPLACE_WINDOW
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn ledger_clock_expires_real_cancel_cooldown() {
    let (url, sent) = mock_rpc_server(0, 1_000_000_000, 20_000_000_000, false).await;
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url)
        .build_with_nonce(0)
        .unwrap();
    wallet.sign(test_request()).unwrap();
    assert_eq!(
        wallet.replace_stalled_nonce(0, 0).await.unwrap(),
        ReplaceOutcome::NotStalled
    );
    wallet.advance_ledger_clock(Duration::from_secs(30));
    assert!(matches!(
        wallet.replace_stalled_nonce(0, 0).await.unwrap(),
        ReplaceOutcome::Cancelled { .. }
    ));
    assert_eq!(
        wallet.replace_stalled_nonce(0, 0).await.unwrap(),
        ReplaceOutcome::NotStalled
    );
    assert_eq!(sent.lock().len(), 1);
    wallet.advance_ledger_clock(STALL_REPLACE_WINDOW);
    assert!(matches!(
        wallet.replace_stalled_nonce(0, 0).await.unwrap(),
        ReplaceOutcome::Cancelled { .. }
    ));
    assert_eq!(sent.lock().len(), 2);
}

#[tokio::test]
async fn rpc_request_timeout_reaches_primary_broadcast_and_dedicated_clients() {
    let primary = blackholed_rpc().await;
    let secondary = blackholed_rpc().await;
    let dedicated = blackholed_rpc().await;
    let timeout = Duration::from_millis(150);
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary)
        .broadcast_rpcs(vec![secondary])
        .rpc_request_timeout(timeout)
        .build_with_nonce(0)
        .unwrap();
    let mut clients = wallet.batch_client.as_ref().unwrap().clients().to_vec();
    clients.push(wallet.client_for(&dedicated).unwrap());
    assert_eq!(clients.len(), 3);
    for client in clients {
        let started = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(2), client.get_nonce(Address::ZERO))
            .await
            .expect("configured request deadline must precede the outer bound");
        assert!(result.is_err());
        assert!(started.elapsed() >= timeout);
    }
}

async fn blackholed_rpc() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0u8; 4096];
        tokio::io::AsyncReadExt::read(&mut socket, &mut bytes)
            .await
            .unwrap();
        std::future::pending::<()>().await;
        drop(socket);
    });
    format!("http://{address}")
}

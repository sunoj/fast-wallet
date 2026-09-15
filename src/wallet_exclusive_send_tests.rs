// Exclusive-send regression tests using the wallet's JSON-RPC mocks.
// Covers builder variants, read isolation, send/cancel routing, and conflicts.
mod exclusive_send_tests {
    use super::*;

    #[tokio::test]
    async fn exclusive_send_and_cancel_never_reach_primary() {
        assert_exclusive_send_and_cancel(0).await;
    }

    #[tokio::test]
    async fn exclusive_send_and_cancel_with_initial_nonce_never_reach_primary() {
        assert_exclusive_send_and_cancel(1).await;
    }

    #[tokio::test]
    async fn exclusive_send_and_cancel_with_known_nonce_never_reach_primary() {
        assert_exclusive_send_and_cancel(2).await;
    }

    async fn assert_exclusive_send_and_cancel(mode: u8) {
        let methods = Arc::new(Mutex::new(Vec::new()));
        let (primary, primary_sent) = mock_rpc_server_counted(
            50,
            1_000_000_000,
            20_000_000_000,
            false,
            true,
            methods.clone(),
        )
        .await;
        let (exclusive, exclusive_sent) =
            mock_rpc_server(50, 1_000_000_000, 20_000_000_000, false).await;
        let builder = FastWalletBuilder::new(TEST_PRIVATE_KEY, &primary)
            .chain_id(1)
            .broadcast_rpcs_exclusive(vec![exclusive]);
        let wallet = match mode {
            0 => builder.build().await.unwrap(),
            1 => builder.initial_nonce(50).build().await.unwrap(),
            _ => builder.build_with_nonce(50).unwrap(),
        };
        assert!(wallet.batch_client.is_some());
        assert_eq!(wallet.sync_nonce().await.unwrap(), 50);
        assert_eq!(wallet.get_balance().await.unwrap(), U256::from(256));
        let tx = wallet.sign(test_request()).unwrap();
        wallet.send_signed(&tx).await.unwrap();
        wallet.inflight_nonces.backdate_first_seen_for_tests(
            50,
            Duration::from_secs(60),
        );
        assert!(matches!(
            wallet.replace_stalled_nonce(50, 0).await.unwrap(),
            ReplaceOutcome::Cancelled { nonce: 50, .. }
        ));
        assert_eq!(exclusive_sent.lock().len(), 2, "send and cancel accepted");
        assert_eq!(
            primary_sent.lock().len(),
            0,
            "primary must never receive sends"
        );
        let methods = methods.lock();
        assert!(methods.iter().any(|m| m == "eth_getTransactionCount"));
        assert!(methods.iter().any(|m| m == "eth_getBalance"));
        assert!(!methods.iter().any(|m| m == "eth_sendRawTransaction"));
    }

    #[tokio::test]
    async fn exclusive_mode_replaces_previous_broadcast_list() {
        for mode in 0..3 {
            let builder = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://127.0.0.1:1")
                .initial_nonce(0)
                .broadcast_rpcs(vec!["http://broadcast".into()])
                .broadcast_rpcs_exclusive(vec!["https://bsc.blockrazor.xyz/<rpc_id>".into()]);
            let result = match mode {
                0 => builder.build().await,
                1 => builder.initial_nonce(0).build().await,
                _ => builder.build_with_nonce(0),
            };
            let wallet = result.unwrap();
            let batch = wallet.batch_client.as_ref().unwrap();
            assert_eq!(batch.endpoint_count(), 1);
            assert_eq!(batch.next_client().url(), "https://bsc.blockrazor.xyz/<rpc_id>");
        }
    }

    #[test]
    fn empty_exclusive_list_is_rejected_unless_mode_is_reset() {
        let error = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://primary")
            .broadcast_rpcs_exclusive(Vec::new())
            .build_with_nonce(0)
            .unwrap_err();
        assert!(matches!(error, WalletError::InvalidConfig(_)));
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://primary")
            .broadcast_rpcs_exclusive(Vec::new())
            .broadcast_rpcs(vec!["http://broadcast".into()])
            .build_with_nonce(0)
            .unwrap();
        assert!(wallet.batch_client.is_some());
    }
}

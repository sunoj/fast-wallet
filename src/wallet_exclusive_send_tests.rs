// Exclusive-send regression tests using the wallet's JSON-RPC mocks.
// Covers builder variants, read isolation, send/cancel routing, and conflicts.
mod exclusive_send_tests {
    use super::*;

    #[tokio::test]
    async fn exclusive_send_and_cancel_never_reach_primary() {
        for mode in 0..3 {
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
                .send_rpcs_exclusive(vec![exclusive]);
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
    }

    #[tokio::test]
    async fn conflicting_send_modes_fail_before_network_access() {
        for mode in 0..3 {
            let builder = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://127.0.0.1:1")
                .broadcast_rpcs(vec!["http://broadcast".into()])
                .send_rpcs_exclusive(vec!["https://bsc.blockrazor.xyz/<rpc_id>".into()]);
            let result = match mode {
                0 => builder.build().await,
                1 => builder.initial_nonce(0).build().await,
                _ => builder.build_with_nonce(0),
            };
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("mutually exclusive"));
        }
    }

    #[test]
    fn empty_exclusive_list_preserves_default_send_mode() {
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://primary")
            .send_rpcs_exclusive(Vec::new())
            .build_with_nonce(0)
            .unwrap();
        assert!(wallet.batch_client.is_none());
        let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://primary")
            .send_rpcs_exclusive(Vec::new())
            .broadcast_rpcs(vec!["http://broadcast".into()])
            .build_with_nonce(0)
            .unwrap();
        assert!(wallet.batch_client.is_some());
    }
}

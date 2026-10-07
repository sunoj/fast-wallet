// Admission regressions through the wallet send paths and loopback RPCs.
// Uses the wallet test fixtures to observe RPC calls and nonce ownership.

mod admission {
    use super::*;

    async fn fixture(batch: bool) -> (FastWallet, Arc<Mutex<Vec<String>>>) {
        let methods = Arc::new(Mutex::new(Vec::new()));
        let (url, _) = mock_rpc_server_counted(50, 1, 20, false, false, methods.clone()).await;
        let mut builder = FastWalletBuilder::new(TEST_PRIVATE_KEY, &url).chain_id(1);
        if batch {
            builder = builder.broadcast_rpcs_exclusive(vec![url]);
        }
        (builder.build_with_nonce(50).unwrap(), methods)
    }

    fn refusal() -> WalletResult<()> {
        Err(WalletError::RpcError(
            "nonce too low: next nonce 99, tx nonce 50".into(),
        ))
    }

    fn assert_refused(error: WalletError) {
        assert!(
            matches!(&error, WalletError::BroadcastRefused(source) if matches!(source.as_ref(), WalletError::RpcError(_)))
        );
        assert!(error
            .to_string()
            .starts_with("Broadcast refused locally (never sent):"));
        assert!(error.is_definitive_rejection());
        assert_eq!(error.authoritative_nonce(), None);
        assert_eq!(error.nonce_sync_block(), None);
    }

    async fn assert_reused(wallet: &FastWallet, methods: &Mutex<Vec<String>>) {
        assert!(methods.lock().is_empty(), "refusal must not call any RPC");
        assert_eq!(wallet.pending_count(), 0);
        assert_eq!(wallet.inflight_count(), 0);
        assert!(wallet.lowest_unresolved_inflight(50).is_none());
        let next = wallet.sign(test_request()).unwrap();
        assert_eq!(next.nonce(), 50);
        wallet.send_signed_detailed(&next, 0.0).await.unwrap();
        assert_eq!(*methods.lock(), ["eth_sendRawTransaction"]);
        let record = wallet.lowest_unresolved_inflight(50).unwrap();
        assert_eq!(record.accepted_broadcasts, 1);
        assert_eq!(record.status, crate::InflightNonceStatus::BroadcastAccepted);
    }

    #[tokio::test]
    async fn signed_refusal_recycles_without_rpc_or_candidate() {
        for batch in [false, true] {
            for tracked in [false, true] {
                let (wallet, methods) = fixture(batch).await;
                let tx = if tracked {
                    wallet.sign(test_request()).unwrap()
                } else {
                    let nonce = wallet.nonce_manager.get_nonce();
                    signed_1559_at(&wallet, nonce, 1, 20)
                };
                let error = wallet
                    .send_signed_detailed_guarded(&tx, 0.0, |_| {
                        assert_eq!(wallet.inflight_count(), usize::from(tracked));
                        refusal()
                    })
                    .await
                    .unwrap_err();
                assert_reused(&wallet, &methods).await;
                assert_refused(error);
            }
        }
    }

    #[tokio::test]
    async fn hash_send_refusal_recycles_without_rpc_or_candidate() {
        for batch in [false, true] {
            for signed in [false, true] {
                let (wallet, methods) = fixture(batch).await;
                let mut checked = false;
                let hook = |_: &Transaction| {
                    checked = true;
                    refusal()
                };
                let error = if signed {
                    let tx = wallet.sign(test_request()).unwrap();
                    wallet.send_signed_guarded(&tx, hook).await
                } else {
                    wallet.send_guarded(test_request(), hook).await
                }
                .unwrap_err();
                assert!(checked);
                assert_reused(&wallet, &methods).await;
                assert_refused(error);
            }
        }
    }

    #[tokio::test]
    async fn preheat_refusal_recycles_before_and_after_context_drop() {
        for batch in [false, true] {
            for warmup_only in [false, true] {
                let (wallet, methods) = fixture(batch).await;
                let ctx = if warmup_only {
                    wallet.preheat_warmup_only(false).await.unwrap()
                } else {
                    wallet.preheat(false).await.unwrap()
                };
                let error = wallet
                    .send_with_preheat_detailed_guarded(&ctx, test_request(), |_| refusal())
                    .await
                    .unwrap_err();
                assert_reused(&wallet, &methods).await;
                drop(ctx);
                assert_eq!(wallet.pending_count(), 1, "drop must not release twice");
                assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 51);
                assert_refused(error);
            }
        }
    }

    #[tokio::test]
    async fn hash_preheat_refusal_recycles_without_rpc() {
        for batch in [false, true] {
            let (wallet, methods) = fixture(batch).await;
            let ctx = wallet.preheat(false).await.unwrap();
            let mut checked = false;
            let error = wallet
                .send_with_preheat_guarded(&ctx, test_request(), |_| {
                    checked = true;
                    refusal()
                })
                .await
                .unwrap_err();
            assert!(checked);
            assert_reused(&wallet, &methods).await;
            drop(ctx);
            assert_eq!(wallet.pending_count(), 1, "drop must not release twice");
            assert_refused(error);
        }
    }

    #[tokio::test]
    async fn preheat_refusal_is_checked_after_pending_slot_wait() {
        let (wallet, methods) = fixture(false).await;
        let ctx = wallet.preheat(false).await.unwrap();
        let permit = wallet
            .pending_semaphore
            .acquire_many(wallet.config.max_pending_txs as u32)
            .await
            .unwrap();
        let checked = AtomicBool::new(false);
        let mut send =
            Box::pin(
                wallet.send_with_preheat_detailed_guarded(&ctx, test_request(), |_| {
                    checked.store(true, Ordering::Relaxed);
                    refusal()
                }),
            );
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut send)
            .await
            .is_err());
        assert!(!checked.load(Ordering::Relaxed));
        drop(permit);
        let error = send.await.unwrap_err();
        assert!(checked.load(Ordering::Relaxed));
        assert_reused(&wallet, &methods).await;
        assert_refused(error);
    }

    #[tokio::test]
    async fn allowed_hooks_match_unguarded_sends() {
        for batch in [false, true] {
            for preheated in [false, true] {
                for guarded in [false, true] {
                    let (wallet, methods) = fixture(batch).await;
                    let result = if preheated {
                        let ctx = wallet.preheat(false).await.unwrap();
                        if guarded {
                            wallet
                                .send_with_preheat_detailed_guarded(
                                    &ctx,
                                    test_request(),
                                    |_| Ok(()),
                                )
                                .await
                        } else {
                            wallet
                                .send_with_preheat_detailed(&ctx, test_request())
                                .await
                        }
                    } else {
                        let tx = wallet.sign(test_request()).unwrap();
                        if guarded {
                            wallet
                                .send_signed_detailed_guarded(&tx, 2.0, |_| Ok(()))
                                .await
                        } else {
                            wallet.send_signed_detailed(&tx, 2.0).await
                        }
                    };
                    let result = result.unwrap();
                    if !preheated {
                        assert_eq!(result.sign_ms, 2.0);
                    }
                    assert_eq!(*methods.lock(), ["eth_sendRawTransaction"]);
                    assert_eq!(wallet.pending_count(), u64::from(!preheated));
                    let record = wallet.lowest_unresolved_inflight(50).unwrap();
                    assert_eq!(record.accepted_broadcasts, 1);
                    assert_eq!(record.status, crate::InflightNonceStatus::BroadcastAccepted);
                    assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 51);
                }
            }
        }
    }

    fn assert_cancel_unchanged(wallet: &FastWallet, before: &crate::InflightNonceSnapshot) {
        let after = wallet.lowest_unresolved_inflight(50).unwrap();
        assert_eq!(after.tx_hashes, before.tx_hashes);
        assert_eq!(after.status, before.status);
        assert_eq!(after.accepted_broadcasts, before.accepted_broadcasts);
        assert_eq!(after.max_fee_seen, before.max_fee_seen);
        assert_eq!(after.max_priority_seen, before.max_priority_seen);
        assert_eq!(wallet.pending_count(), 1);
        assert!(wallet.last_stall_replace.lock().is_none());
    }

    #[tokio::test]
    async fn cancel_refusal_preserves_original_and_allows_retry() {
        for batch in [false, true] {
            let (wallet, methods) = fixture(batch).await;
            let original = wallet.sign(test_request()).unwrap();
            wallet.send_signed(&original).await.unwrap();
            wallet
                .inflight_nonces
                .backdate_first_seen_for_tests(50, Duration::from_secs(60));
            let before = wallet.lowest_unresolved_inflight(50).unwrap();
            methods.lock().clear();
            let mut checked = false;
            let error = wallet
                .replace_stalled_nonce_guarded(50, 0, |_| {
                    checked = true;
                    refusal()
                })
                .await
                .unwrap_err();
            assert!(checked);
            assert!(!methods
                .lock()
                .iter()
                .any(|method| method == "eth_sendRawTransaction"));
            assert_cancel_unchanged(&wallet, &before);
            assert_eq!(wallet.sign(test_request()).unwrap().nonce(), 51);
            let outcome = wallet
                .replace_stalled_nonce_guarded(50, 0, |_| Ok(()))
                .await
                .unwrap();
            assert!(matches!(
                outcome,
                ReplaceOutcome::Cancelled { nonce: 50, .. }
            ));
            assert_eq!(
                methods
                    .lock()
                    .iter()
                    .filter(|method| *method == "eth_sendRawTransaction")
                    .count(),
                1
            );
            assert_refused(error);
        }
    }
}

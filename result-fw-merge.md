# fast-wallet 0.2.7 maintenance merge

## Scope and decisions

Merge `origin/feat/exclusive-send-rpcs` (`15e8df0`) into the main-based
`merge/0.1-line-into-main` branch (`d1608b7`), using a two-parent merge.
No rebase, push, or tag. The untagged maintenance versions 0.1.45–0.1.47 land here.

| Paired fix | Resolution and reason |
| --- | --- |
| Exclusive send | Main's `broadcast_rpcs_exclusive` wins. Remove the duplicate `send_rpcs_exclusive` API and helper. All three builder paths use `resolve_broadcast_rpcs`, create a batch client, and apply the configured endpoint timeout. Primary reads remain separate. Keep `mock_rpc_server_counted`; port the send-and-cancel regression into three separately executed builder tests. |
| Nonce stalls | Both sides survive: maintenance's age-based detection and ledger clock reset on reuse; main's cancellation freshness guard, definitive-rejection recycling, and authoritative nonce-too-low parser. `nonce.rs` already contains main's changes and needs no edit. |
| Broadcast verdicts | Maintenance's structured `BroadcastFailed` payload and six-second endpoint deadline win the representation. Main's safety rule wins nonce ownership: classify each endpoint separately, require a nonempty set of definitive rejections, and reject recycling on any transport or ambiguous verdict. Existing public main error variants and their tests remain, but the batch sender emits one structured representation. Late verdict logging and next-hop transport classification survive. |
| URL redaction | Maintenance wins: `endpoint_host(&str) -> &str` and `redact_urls(&str) -> String` remain public, `SendResult` contains hosts, and RPC/broadcaster errors are redacted at construction. |
| Dependencies | Main wins: reqwest 0.13 with `rustls-no-provider`, ring provider installation, and KeySource/signer behavior remain. Version is 0.2.7. Cargo regenerates the lockfile remotely from main's dependency lock; no manual lockfile merge. |

## Test reconciliation

All 151 main test functions remain. The maintenance inventory had 143 functions;
these deliberate behavior changes account for renamed or superseded tests:

- `test_signer_clone`: main deliberately removed `FastSigner::Clone` to avoid copying
  private-key material. Retain main's signer/credential lifecycle and deterministic
  signing tests; do not restore cloning or migrate consumer signers.
- `test_verify_broadcast_rebroadcast_failure_releases_nonce`: main's existing
  `test_verify_broadcast_rebroadcast_failure_retains_nonce` wins because a failed
  broadcast can hide acceptance.
- `timed_out_presigned_send_releases_nonce_after_broadcasting_context_was_dropped`:
  retain the HTTP timeout regression as `...retains_nonce...`, asserting nonce 101
  remains next after a timeout at nonce 100. Main's ambiguity safety wins.
- `conflicting_send_modes_fail_before_network_access`: ported to
  `exclusive_mode_replaces_previous_broadcast_list`, matching main's last-setter
  semantics for the single broadcast list.
- `empty_exclusive_list_preserves_default_send_mode`: ported to
  `empty_exclusive_list_is_rejected_unless_mode_is_reset`, matching main's rejection
  of an empty exclusive set and explicit reset via `broadcast_rpcs`.
- The decisive cancellation tests backdate the ledger by 60 seconds so they test a
  stranded transaction while preserving main's 30-second freshness guard.
- The verdict logging test installs its capture as the integration binary's global
  subscriber. Thread-local capture missed events during parallel tests; all existing
  log-content and redaction assertions remain.

No new ignored tests. The existing ignored scope is two external-network integration
cases and three illustrative doctests. Existing oversized source files are retained;
new source/test files remain below 300 lines. No formatter or local Cargo was run.

## Verification transport

All actual compilation and tests run on `grok-bot-twitter` through `aid build` or
`aid test` and the task's installed Cargo/rbox shim. The inherited target is never
replaced by this task; the installed shim selects its warm shared remote target.
A temporary adapter supplies `--no-fail-fast` (unsupported by the installed
`aid test` CLI), forwards `RUSTC_WRAPPER=""` and `CARGO_INCREMENTAL=0` through rbox's
clean environment, and retains Cargo.lock in the remote job's temporary artifacts.

An initial chief attempt was refused by rbox's disk admission check (9.8 GiB free,
10 GiB required). An initial twitter attempt had incomplete sources because unresolved
Git index entries cannot be represented by rbox's patch transfer; staging the resolved
entries fixed that. The first complete suite then found the log-capture issue above
(177 passed, 1 failed); the corrected suite is recorded below.

## Verification results

### fast-wallet

- `aid build check -p fast-wallet`: succeeded, 0 errors, 0 warnings.
- `aid test -p fast-wallet` → `cargo test --no-fail-fast --message-format=json -p fast-wallet`:
  **178 passed, 0 failed, 5 pre-existing ignored**.
- Remote compile job: `915887248e5e4a75aa247c70ec6944ed`.
- Remote full-suite job: `6bd9a5c6cc104dc2a4645c0c1c1b62e0`.
- Raw evidence: `/home/builder/.rbox/jobs/<job-id>/log` on grok-bot-twitter.

```text
     Running unittests src/lib.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/fast_wallet-4c2374e54765e4da)
test result: ok. 151 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.97s
     Running unittests src/main.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/fast_wallet-54dcb8b420cd7df0)
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
     Running tests/broadcast_redaction.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/broadcast_redaction-a9ce9a17cd036e34)
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.13s
     Running tests/broadcast_timeout.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/broadcast_timeout-b5b9f0a3cf01119e)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.21s
     Running tests/broadcast_verdicts.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/broadcast_verdicts-b6f70d3d66331dd1)
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.11s
     Running tests/integration_test.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/integration_test-b01942638f07c5ba)
test result: ok. 15 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.69s
     Running tests/nonce_broadcast_ownership.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/nonce_broadcast_ownership-1924af33d4dafd7c)
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s
     Running tests/url_redaction.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/url_redaction-993d03b0c64e87f4)
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
   Doc-tests fast_wallet
test result: ok. 1 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 0.05s
```

### Routing mutation

Temporarily changed only the exclusive arm of `resolve_broadcast_rpcs` to prepend
`primary_rpc`. `aid test -p fast-wallet exclusive_send_and_cancel` executed all three
builder tests and failed each at the primary-send assertion: **2 actual, 0 expected**.
Remote mutation job: `dde3e2f986354fc3ae03318b5f7c83e3`.

```text
     Running unittests src/lib.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/fast_wallet-4c2374e54765e4da)
test wallet::tests::exclusive_send_tests::exclusive_send_and_cancel_with_initial_nonce_never_reach_primary ... FAILED
test wallet::tests::exclusive_send_tests::exclusive_send_and_cancel_with_known_nonce_never_reach_primary ... FAILED
test wallet::tests::exclusive_send_tests::exclusive_send_and_cancel_never_reach_primary ... FAILED
assertion `left == right` failed: primary must never receive sends
  left: 2
 right: 0
test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 148 filtered out; finished in 0.11s
```

The mutation was reverted byte-for-byte to the staged implementation. Restored
remote job `6b5e9bc92e1b4e5791df2520b7c843d1` passed all three tests:

```text
     Running unittests src/lib.rs (/home/builder/.rbox/target/fast-wallet/debug/deps/fast_wallet-4c2374e54765e4da)
test wallet::tests::exclusive_send_tests::exclusive_send_and_cancel_with_initial_nonce_never_reach_primary ... ok
test wallet::tests::exclusive_send_tests::exclusive_send_and_cancel_with_known_nonce_never_reach_primary ... ok
test wallet::tests::exclusive_send_tests::exclusive_send_and_cancel_never_reach_primary ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 148 filtered out; finished in 0.08s
```


### Consumer

Pending verification of the committed merge.


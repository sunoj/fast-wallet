# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Started at 0.2.1; earlier releases are recorded only in git tags and commit messages.

## [0.2.11] - 2026-10-07

### Added

- Admission hooks: `send_guarded`, `send_signed_guarded`, `send_signed_detailed_guarded`,
  `send_with_preheat_guarded`, `send_with_preheat_detailed_guarded` and
  `replace_stalled_nonce_guarded` take a synchronous `before_broadcast` hook that runs after
  the pending-slot wait and immediately before the raw bytes go to any endpoint. The hook must
  be cheap, non-blocking and must not panic (a panic leaves the nonce reserved). Unguarded
  methods behave as before.
- `WalletError::BroadcastRefused`, returned when the hook refuses; nothing was sent and
  `is_definitive_rejection()` is true. `WalletError` is not `#[non_exhaustive]`, so this is a
  source-breaking addition for exhaustive matches.

### Changed

- Release rule on refusal: a refused send or preheated send releases its nonce and marks the
  ledger record `admission_refused`, unless the in-flight ledger holds an accepted broadcast at
  that nonce (a replacement); then the nonce and its ledger record are left untouched. A refused
  cancel from `replace_stalled_nonce_guarded` reserves no nonce and releases nothing; callers
  must not release the stalled nonce on that error either.
- The `verify_broadcast` fee-bump rebroadcast runs no hook and has no guarded equivalent: a
  definitive endpoint rejection of a replacement sent through `send_signed*_guarded` still
  recycles the nonce. Use `replace_stalled_nonce_guarded` for replacements.

## [0.2.10] - 2026-10-07

### Added

- `WalletError::is_definitive_rejection()`. `send_signed` and `send_signed_detailed`
  release the nonce exactly when the returned error satisfies it; release recycles only a
  nonce this wallet reserved that is still above the synced nonce. On `false` the reservation
  is retained and the caller settles it with a bounded receipt check plus reconciliation:
  `false` does not mean any endpoint received the bytes (a permit timeout or connect failure
  also returns it). It is the same predicate the send paths use, so callers no longer need to
  infer the outcome from error variants. A variant-only mapping
  misreads a single-endpoint `RpcError` whose text is definitive, and an aggregate
  `BroadcastFailed` whose every verdict is a definitive rejection.

## [0.2.9] - 2026-10-07

### Fixed

- Node fee-floor, intrinsic-gas-too-low, and configured fee-cap pre-check rejections now recycle nonces when every endpoint definitively rejects the transaction.
- `send_signed` and `send_signed_detailed` now recycle the nonce when every endpoint
  definitively rejects the transaction during pre-check. Mixed, ambiguous, transport,
  and timeout errors retain the nonce and use the existing nonce recovery; rejection
  of a `verify_broadcast` rebroadcast also retains it because an earlier send may be live.
- Recycling matches the preheated path's plain release: a nonce sync between send and
  error can make the release stale. The tracker reuses the gap and nonce-too-low recovery
  resyncs. This is narrower than presigned sends' release-on-every-error policy in 0.2.6
  and earlier; it does not add reservation identity checks.
- Recycling assumes a first broadcast of the nonce. A definitively rejected replacement
  at a nonce whose original was accepted would recycle a live nonce; send replacements
  through `replace_stalled_nonce`.

## [0.2.8] - 2026-10-07

### Added

- `examples/h2_idle_probe.rs`: measures whether a pooled connection survives idle gaps
  (reused vs newly dialled, warm vs cold latency) for URLs from arguments or `PROBE_URLS`.

### Changed

- `RpcClient` and `TransactionBroadcaster` pools no longer evict idle connections
  (`pool_idle_timeout(None)`); the unchanged HTTP/2 keep-alive pings (10 s interval,
  20 s timeout, while idle) still drop a connection whose peer stops answering.
- The warmup throttle window runs on tokio's clock and depends on the URL scheme:
  4 minutes for `https://` endpoints (HTTP/2 with keep-alive pings), 60 s as before for
  every other URL (plain `http://` speaks HTTP/1.1, has no pings, and an idle close by
  the server goes unseen). The `https://` window sits below the shortest idle survival
  measured with `examples/h2_idle_probe.rs`: QuickNode closes idle HTTP/2 connections
  between 5 and 10 minutes despite pings (four keyless public RPCs kept them for 30
  minutes); a warmup per `https://` endpoint now runs at most 15 times an hour instead
  of 60. The endpoint's warm state is cleared, so the next warmup probes again, whenever
  a request ends without its whole answer: a transport error, a response body cut short
  after its headers, an `RpcClient` request its caller drops (the caller's own timeout,
  a lost race, a batch send abandoned at its timeout), or a `TransactionBroadcaster`
  send dropped as a `RaceAll` / `PrivateFirst` race loser.
- A wallet holds one `RpcClient` per endpoint URL: a broadcast entry for the primary URL
  reuses the primary's client, a URL repeated in the broadcast list is sent once, and a
  gas RPC URL reuses a matching client. URLs match when they parse to the same request
  target (scheme and host case, default port, empty path). Nothing is trimmed beyond what
  URL parsing itself strips (leading and trailing ASCII spaces and control characters), so
  a URL with a trailing NBSP is a distinct endpoint.
- Deduplication shifts broadcast endpoint indexes and counts: a dropped duplicate no
  longer occupies a slot, so `SendResult::first_success_endpoint_index`,
  `BatchRpcClient::endpoint_count()`, the `batch_count` returned by
  `FastWallet::warmup_connections()` and the entries of `SendResult::per_endpoint_ms`
  count each endpoint once. Callers that map an endpoint index to a label will see later
  indexes shift down by one per removed duplicate.

## [0.2.7] - 2026-09-15

### Added

- Public `endpoint_host` and `redact_urls` helpers and structured per-endpoint broadcast
  verdicts, including next-hop transport classification and error-text parsing.

### Changed

- Merge the 0.1.x maintenance line, including untagged 0.1.45–0.1.47 changes.
- Keep `broadcast_rpcs_exclusive` as the exclusive-send API; maintenance consumers must
  rename `send_rpcs_exclusive`. Empty exclusive lists remain invalid.
- Broadcast results expose endpoint hosts instead of credential-bearing URLs.

### Fixed

- Bound each batch broadcast endpoint to six seconds (configurable), preserve every
  failure verdict, and recycle nonces only when every endpoint definitively rejects.
- Detect aged nonce stalls even at a gap of one and reset ledger age on nonce reuse,
  while retaining the head-cancel freshness guard and authoritative nonce parsing.
- Redact HTTP(S) and WS(S) URLs case-insensitively at RPC error construction.

## [0.2.1] - 2026-07-26

### Changed

- **`reqwest` 0.12 → 0.13, and the ring `CryptoProvider` is now installed by this library.**

  reqwest 0.13 offers only two rustls options and **removed `rustls-tls-webpki-roots`**:

  | feature | provider | roots |
  |---|---|---|
  | `rustls` | `aws-lc-rs` → `aws-lc-sys` (C/asm, **hostile to cross-compilation**) | platform verifier |
  | `rustls-no-provider` | none — the process must install one or `Client::build()` **panics** | platform verifier |

  We take `rustls-no-provider` to stay cross-compile clean (macOS → Linux) and install `ring` in
  the new `tls` module. `ensure_provider()` is `Once`-guarded, idempotent and thread-safe, and is
  called before every client this crate builds: `TransactionBroadcaster::new` and `RpcClient::new`.

  **The library installs it, not the caller**, deliberately: both constructors build a
  `reqwest::Client` internally, so requiring a caller-side install would make this bump panic at
  construction for every existing consumer.

### Breaking

- **Root certificates now come from the OS trust store** (`rustls-platform-verifier`) rather than a
  bundled `webpki-roots` set, because reqwest 0.13 deleted that feature. **Hosts must have a CA
  bundle** (e.g. `ca-certificates`). Verify before deploying — this crate signs and broadcasts
  transactions, so a TLS failure is a missed send.
- **This crate no longer enables a provider-selecting reqwest feature.** A consumer that leaned on
  our old `rustls-tls` (which implied `__rustls-ring`) for *its own* clients must now enable one
  itself or call `fast_wallet::tls::ensure_provider()`.

### Verification

- 125 tests pass, 0 fail.
- Built from a clean clone of the release branch (an earlier commit had omitted `src/tls.rs` —
  `git commit -a` does not stage new files).

### Note for consumers on the 0.1.x line

`smart-router` pins `v0.1.42`. This release sits on top of `v0.2.0`, so adopting it also takes
`v0.1.43` (`broadcast_rpcs_exclusive`) and `v0.2.0` (`KeySource::Credential` + zeroize hardening).
The change here was **not** backported onto 0.1.x: doing so would have meant releasing from a base
7 commits behind `main` and silently reverting that upstream work.

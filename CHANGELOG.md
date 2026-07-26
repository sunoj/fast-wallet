# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Started at 0.2.1; earlier releases are recorded only in git tags and commit messages.

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

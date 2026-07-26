//! Process-global rustls `CryptoProvider` installation.
//!
//! Exports: [`ensure_provider`].
//!
//! This crate builds `reqwest` with `rustls-no-provider`, so the library
//! installs a provider itself rather than requiring every caller to.

/// Installs the ring-backed rustls `CryptoProvider` as the process default,
/// exactly once. Idempotent, thread-safe, cheap to call repeatedly.
///
/// # Why the library does this instead of the caller
///
/// reqwest 0.13 offers only `rustls` (which selects `aws-lc-rs`, pulling
/// `aws-lc-sys` -- a C/asm build hostile to cross-compiling macOS -> Linux) or
/// `rustls-no-provider` (no provider, and `Client::builder().build()` **panics**
/// until the process installs one).
///
/// We take `rustls-no-provider` to stay cross-compile clean and install `ring`
/// here. Doing it in the library rather than the caller keeps the reqwest 0.13
/// move non-breaking: `TransactionBroadcaster::new` and `RpcClient::new` build
/// clients internally, so otherwise every existing consumer would begin
/// panicking at construction.
///
/// reqwest 0.13 also **removed** `rustls-tls-webpki-roots`; both remaining
/// options use `rustls-platform-verifier` (the OS trust store), so hosts need a
/// CA bundle present.
pub fn ensure_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Errs only when a provider is already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::ensure_provider;

    #[test]
    fn is_idempotent_and_concurrency_safe() {
        let handles: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(ensure_provider))
            .collect();
        for h in handles {
            h.join().expect("ensure_provider must not panic");
        }
        ensure_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    /// The regression this module prevents: a client must build without the
    /// caller having installed anything.
    #[test]
    fn client_builds_without_caller_installing_a_provider() {
        ensure_provider();
        reqwest::Client::builder()
            .build()
            .expect("client must build");
    }
}

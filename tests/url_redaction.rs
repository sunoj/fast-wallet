// Verify provider URL redaction covers mixed-case HTTP and WebSocket schemes.
// Depends on the public fast-wallet redactor; no network access is needed.

#[test]
fn mixed_case_http_and_websocket_urls_never_expose_credentials() {
    for scheme in ["http", "HTTP", "hTtPs", "HTTPS", "ws", "WS", "wSs", "WSS"] {
        let message = format!(
            "provider {scheme}://user:SECRETKEY@rpc.example/v2/SECRETKEY?key=SECRETKEY failed"
        );
        assert_eq!(
            fast_wallet::redact_urls(&message),
            "provider rpc.example failed"
        );
    }
    assert_eq!(
        fast_wallet::redact_urls(
            "é WSS://rpc.example/SECRETKEY then HtTpS://other.example/SECRETKEY done"
        ),
        "é rpc.example then other.example done"
    );
}

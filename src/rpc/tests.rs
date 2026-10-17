// RPC parsing, redaction, and error-format regression tests.
// Exercises the parent module without external services.
use super::*;
#[test]
fn endpoint_host_drops_the_key_bearing_parts() {
    assert_eq!(
        endpoint_host("https://base-mainnet.infura.io/v3/deadbeefdeadbeef"),
        "base-mainnet.infura.io"
    );
    assert_eq!(
        endpoint_host("https://sleek-vineyard.base-mainnet.quiknode.pro/abc123/"),
        "sleek-vineyard.base-mainnet.quiknode.pro"
    );
    assert_eq!(
        endpoint_host("http://162.250.127.74:8545"),
        "162.250.127.74:8545"
    );
    assert_eq!(
        endpoint_host("https://user:secret@rpc.example.com/x"),
        "rpc.example.com"
    );
    assert_eq!(
        endpoint_host("https://host.example.com/?key=secret"),
        "host.example.com"
    );
}

#[test]
fn redact_urls_strips_keys_out_of_error_text() {
    // The exact shape reqwest produces, which is what reaches per_endpoint.
    let raw = "error sending request for url (https://base-mainnet.g.alchemy.com/v2/SECRETKEY)";
    let clean = redact_urls(raw);
    assert!(!clean.contains("SECRETKEY"), "{clean}");
    assert!(!clean.contains("/v2/"), "{clean}");
    assert!(clean.contains("base-mainnet.g.alchemy.com"), "{clean}");

    // Several URLs in one message, and text after the last one.
    let many = redact_urls("https://a.io/k1 then https://b.io/k2 done");
    assert_eq!(many, "a.io then b.io done");

    // Punctuation inside the key must not end the redaction early.
    let punctuated = redact_urls("failed for url (https://host.io/k,SECRET) retrying");
    assert!(!punctuated.contains("SECRET"), "{punctuated}");
    assert_eq!(punctuated, "failed for url (host.io retrying");

    // Nothing to redact must round-trip untouched.
    assert_eq!(
        redact_urls("replacement transaction underpriced"),
        "replacement transaction underpriced"
    );
}

#[test]
fn test_parse_u64_hex() {
    assert_eq!(parse_u64_hex("0x0").unwrap(), 0);
    assert_eq!(parse_u64_hex("0x1").unwrap(), 1);
    assert_eq!(parse_u64_hex("0xff").unwrap(), 255);
    assert_eq!(parse_u64_hex("0x5208").unwrap(), 21000);
    assert_eq!(parse_u64_hex("5208").unwrap(), 21000);
}

#[test]
fn test_parse_u256_hex() {
    let val = parse_u256_hex("0xde0b6b3a7640000").unwrap(); // 1 ETH in wei
    assert_eq!(val, U256::from(1_000_000_000_000_000_000u64));
}

#[test]
fn test_parse_b256_hex() {
    let hash_str = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let hash = parse_b256_hex(hash_str).unwrap();
    assert_eq!(hash[31], 1);
}

#[test]
fn test_rpc_client_creation() {
    let client = RpcClient::new("http://localhost:8545").unwrap();
    assert_eq!(client.url, "http://localhost:8545");
}

fn rpc_error_from_value(v: serde_json::Value) -> RpcError {
    let resp: RpcResponse<String> = serde_json::from_value(v).unwrap();
    resp.error.unwrap()
}

#[test]
fn test_rpc_error_preserves_data_selector() {
    let error = rpc_error_from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": 3,
            "message": "execution reverted",
            "data": "0xddeb79ba"
        }
    }));
    let msg = format_rpc_error(&error);
    assert!(msg.contains("data=0xddeb79ba"), "got: {msg}");
}

#[test]
fn test_rpc_error_omits_data_tail_when_missing() {
    let error = rpc_error_from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": { "code": 3, "message": "execution reverted" }
    }));
    let msg = format_rpc_error(&error);
    assert_eq!(msg, "RPC error 3: execution reverted");
    assert!(!msg.contains("data="), "got: {msg}");
}

#[test]
fn test_rpc_error_omits_data_tail_when_null() {
    let error = rpc_error_from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": { "code": 3, "message": "execution reverted", "data": null }
    }));
    let msg = format_rpc_error(&error);
    assert_eq!(msg, "RPC error 3: execution reverted");
}

#[test]
fn test_rpc_error_preserves_object_data() {
    let error = rpc_error_from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": -32000,
            "message": "execution reverted",
            "data": { "reason": "NotLiquidatable", "selector": "0xddeb79ba" }
        }
    }));
    let msg = format_rpc_error(&error);
    assert!(msg.contains("data="), "got: {msg}");
    assert!(msg.contains("\"selector\":\"0xddeb79ba\""), "got: {msg}");
}

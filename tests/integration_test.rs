//! Integration tests for fast-wallet
//!
//! These tests verify the complete flow from signing to encoding.
//! Network tests are skipped by default (use --ignored to run them).

use fast_wallet::{
    crypto::keccak256,
    nonce::{NonceManager, NonceTracker, SingleAddressNonceManager},
    signer::FastSigner,
    transaction::TransactionRequest,
    wallet::FastWalletBuilder,
    BroadcasterBuilder, BroadcastStrategy, RpcEndpoint,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use std::sync::Arc;
use std::thread;

const TEST_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const TEST_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

// ============ Signer Tests ============

#[test]
fn test_signer_address_derivation() {
    let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
    let expected = TEST_ADDRESS.parse::<Address>().unwrap();
    assert_eq!(signer.address(), expected);
}

#[test]
fn test_signer_deterministic() {
    let signer1 = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
    let signer2 = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

    let hash = keccak256(b"test message");
    let sig1 = signer1.sign_hash(&hash, 1).unwrap();
    let sig2 = signer2.sign_hash(&hash, 1).unwrap();

    assert_eq!(sig1.r, sig2.r);
    assert_eq!(sig1.s, sig2.s);
    assert_eq!(sig1.v, sig2.v);
}

// ============ Transaction Tests ============

#[test]
fn test_legacy_transaction_roundtrip() {
    let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

    let tx = TransactionRequest::new()
        .to(Address::repeat_byte(0xaa))
        .value(U256::from(1_000_000_000_000_000_000u64)) // 1 ETH
        .gas_limit(21000)
        .gas_price(U256::from(20_000_000_000u64)) // 20 gwei
        .nonce(42)
        .chain_id(1)
        .build_and_sign(&signer)
        .unwrap();

    assert_eq!(tx.nonce(), 42);
    assert!(!tx.encoded().is_empty());
    assert_ne!(tx.hash(), B256::ZERO);
}

#[test]
fn test_eip1559_transaction_roundtrip() {
    let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

    let tx = TransactionRequest::new()
        .to(Address::repeat_byte(0xbb))
        .value(U256::from(500_000_000_000_000_000u64)) // 0.5 ETH
        .gas_limit(50000)
        .max_fee_per_gas(U256::from(100_000_000_000u64)) // 100 gwei
        .max_priority_fee_per_gas(U256::from(2_000_000_000u64)) // 2 gwei
        .nonce(100)
        .chain_id(1)
        .build_and_sign(&signer)
        .unwrap();

    assert_eq!(tx.nonce(), 100);
    // EIP-1559 transactions start with 0x02
    assert_eq!(tx.encoded()[0], 0x02);
}

#[test]
fn test_contract_call_encoding() {
    let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

    // Example: approve(address spender, uint256 amount)
    // Function selector: 0x095ea7b3
    let mut data = vec![0x09, 0x5e, 0xa7, 0xb3];
    // Spender address (padded to 32 bytes)
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(Address::repeat_byte(0xcc).as_slice());
    // Amount (max uint256)
    data.extend_from_slice(&[0xff; 32]);

    let tx = TransactionRequest::new()
        .to(Address::repeat_byte(0xdd)) // Token contract
        .data(Bytes::from(data))
        .gas_limit(100000)
        .gas_price(U256::from(30_000_000_000u64))
        .nonce(0)
        .chain_id(1)
        .build_and_sign(&signer)
        .unwrap();

    assert!(!tx.encoded().is_empty());
}

// ============ Nonce Management Tests ============

#[test]
fn test_nonce_tracker_thread_safety() {
    let tracker = Arc::new(NonceTracker::new(0));
    let mut handles = vec![];

    // Spawn 10 threads, each incrementing 1000 times
    for _ in 0..10 {
        let tracker = tracker.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                tracker.get_and_increment();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Total should be exactly 10,000
    assert_eq!(tracker.peek(), 10000);
}

#[test]
fn test_nonce_manager_multi_address() {
    let manager = NonceManager::new();

    let addr1 = Address::repeat_byte(0x01);
    let addr2 = Address::repeat_byte(0x02);
    let addr3 = Address::repeat_byte(0x03);

    manager.init_nonce(addr1, 100);
    manager.init_nonce(addr2, 200);
    manager.init_nonce(addr3, 300);

    assert_eq!(manager.get_nonce(addr1), 100);
    assert_eq!(manager.get_nonce(addr2), 200);
    assert_eq!(manager.get_nonce(addr3), 300);

    assert_eq!(manager.get_nonce(addr1), 101);
    assert_eq!(manager.get_nonce(addr2), 201);
    assert_eq!(manager.get_nonce(addr3), 301);
}

#[test]
fn test_nonce_sync_recovery() {
    let manager = SingleAddressNonceManager::new(Address::ZERO, 0);

    // Simulate sending 5 transactions
    for _ in 0..5 {
        manager.get_nonce();
    }
    assert_eq!(manager.peek(), 5);
    assert_eq!(manager.pending_count(), 5);

    // Simulate chain confirmed nonce is 3 (2 transactions confirmed)
    manager.sync(3);
    assert_eq!(manager.peek(), 3);

    // Now nonce should continue from 3
    assert_eq!(manager.get_nonce(), 3);
}

// ============ Wallet Integration Tests ============

#[test]
fn test_wallet_sequential_transactions() {
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .chain_id(1)
        .build_with_nonce(0)
        .unwrap();

    let mut transactions = Vec::new();

    for i in 0..10 {
        let tx = wallet
            .sign_legacy_transaction(
                Address::repeat_byte(i as u8),
                U256::from(1_000_000_000_000_000_000u64),
                Bytes::new(),
                21000,
                U256::from(20_000_000_000u64),
            )
            .unwrap();

        transactions.push(tx);
    }

    // Verify nonces are sequential
    for (i, tx) in transactions.iter().enumerate() {
        assert_eq!(tx.nonce(), i as u64);
    }

    // Verify all transaction hashes are unique
    let hashes: std::collections::HashSet<_> = transactions.iter().map(|tx| tx.hash()).collect();
    assert_eq!(hashes.len(), 10);
}

#[test]
fn test_wallet_mixed_transaction_types() {
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .chain_id(1)
        .build_with_nonce(100)
        .unwrap();

    // Legacy transaction
    let legacy = wallet
        .sign_legacy_transaction(
            Address::repeat_byte(1),
            U256::from(1_000_000_000_000_000_000u64),
            Bytes::new(),
            21000,
            U256::from(20_000_000_000u64),
        )
        .unwrap();

    // EIP-1559 transaction
    let eip1559 = wallet
        .sign_eip1559_transaction(
            Address::repeat_byte(2),
            U256::from(1_000_000_000_000_000_000u64),
            Bytes::new(),
            21000,
            U256::from(50_000_000_000u64),
            U256::from(2_000_000_000u64),
        )
        .unwrap();

    assert_eq!(legacy.nonce(), 100);
    assert_eq!(eip1559.nonce(), 101);

    // Legacy starts with RLP list header (>= 0xc0)
    assert!(legacy.encoded()[0] >= 0xc0);
    // EIP-1559 starts with type byte 0x02
    assert_eq!(eip1559.encoded()[0], 0x02);
}

// ============ Broadcaster Tests ============

#[test]
fn test_broadcaster_builder() {
    let broadcaster = BroadcasterBuilder::new()
        .add_public("https://eth.example.com/rpc1")
        .add_public("https://eth.example.com/rpc2")
        .add_endpoint(RpcEndpoint::private(
            "https://relay.flashbots.net",
            "test_auth_key",
        ))
        .strategy(BroadcastStrategy::RaceAll)
        .build()
        .unwrap();

    // Verify endpoints are sorted by priority
    // Flashbots (priority 50) should come before public (priority 100)
    assert!(broadcaster.endpoints[0].is_private);
}

#[test]
fn test_endpoint_configuration() {
    let public = RpcEndpoint::public("https://rpc.example.com");
    assert!(!public.is_private);
    assert_eq!(public.priority, 100);
    assert!(public.auth_header.is_none());

    let private = RpcEndpoint::private("https://private.example.com", "secret_key");
    assert!(private.is_private);
    assert_eq!(private.priority, 50);
    assert!(private.auth_header.is_some());

    let flashbots = RpcEndpoint::flashbots("my_auth_key");
    assert!(flashbots.is_private);
    assert_eq!(flashbots.priority, 10);
    assert_eq!(flashbots.url, "https://relay.flashbots.net");
}

// ============ Crypto Tests ============

#[test]
fn test_keccak256_known_vectors() {
    // Empty input
    let empty = keccak256(&[]);
    assert_eq!(
        format!("{:x}", empty),
        "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
    );

    // "hello"
    let hello = keccak256(b"hello");
    assert_eq!(
        format!("{:x}", hello),
        "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"
    );

    // "hello world"
    let hello_world = keccak256(b"hello world");
    assert_eq!(
        format!("{:x}", hello_world),
        "47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad"
    );
}

// ============ Performance Smoke Tests ============

#[test]
fn test_signing_performance_baseline() {
    use std::time::Instant;

    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .chain_id(1)
        .build_with_nonce(0)
        .unwrap();

    let iterations = 1000;
    let start = Instant::now();

    for _ in 0..iterations {
        let _ = wallet
            .sign_legacy_transaction(
                Address::repeat_byte(1),
                U256::from(1_000_000_000_000_000_000u64),
                Bytes::new(),
                21000,
                U256::from(20_000_000_000u64),
            )
            .unwrap();
    }

    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();

    // Ensure we can sign at least 1000 transactions per second
    // This is a conservative baseline - actual performance should be much higher
    assert!(
        ops_per_sec > 1000.0,
        "Signing performance too low: {} ops/sec",
        ops_per_sec
    );

    println!(
        "Signing performance: {:.0} transactions/second ({:.2} µs/tx)",
        ops_per_sec,
        elapsed.as_micros() as f64 / iterations as f64
    );
}

#[test]
fn test_nonce_acquisition_performance() {
    use std::time::Instant;

    let manager = SingleAddressNonceManager::new(Address::ZERO, 0);
    let iterations = 100_000;

    let start = Instant::now();
    for _ in 0..iterations {
        manager.get_nonce();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();

    // Nonce acquisition should be extremely fast (millions per second)
    assert!(
        ops_per_sec > 1_000_000.0,
        "Nonce acquisition too slow: {} ops/sec",
        ops_per_sec
    );

    println!(
        "Nonce acquisition: {:.0} ops/second ({:.0} ns/op)",
        ops_per_sec,
        elapsed.as_nanos() as f64 / iterations as f64
    );
}

// ============ Network Tests (Ignored by Default) ============

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_mainnet_chain_id() {
    use fast_wallet::rpc::RpcClient;

    let client = RpcClient::new("https://eth.llamarpc.com").unwrap();
    let chain_id = client.chain_id().await.unwrap();
    assert_eq!(chain_id, 1);
}

#[tokio::test]
#[ignore = "Requires network access"]
async fn test_wallet_nonce_fetch() {
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "https://eth.llamarpc.com")
        .chain_id(1)
        .build()
        .await
        .unwrap();

    // Nonce should be fetched from chain
    let nonce = wallet.current_nonce();
    println!("Current nonce for test address: {}", nonce);
}

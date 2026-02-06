//! Performance benchmarks for the fast-wallet library
//!
//! Run with: cargo bench
//!
//! These benchmarks measure the critical hot paths:
//! - Transaction signing speed
//! - Nonce acquisition speed
//! - RLP encoding speed
//! - Hash computation speed

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fast_wallet::{
    crypto::keccak256,
    nonce::{NonceManager, NonceTracker, SingleAddressNonceManager},
    signer::FastSigner,
    transaction::{Eip1559Transaction, LegacyTransaction, Transaction, TransactionRequest},
    wallet::FastWalletBuilder,
};
use alloy::primitives::{Address, Bytes, B256, U256};

const TEST_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

/// Benchmark keccak256 hash computation
fn bench_keccak256(c: &mut Criterion) {
    let mut group = c.benchmark_group("keccak256");

    for size in [32, 64, 128, 256, 512, 1024].iter() {
        let data = vec![0xabu8; *size];
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| keccak256(black_box(&data)))
        });
    }

    group.finish();
}

/// Benchmark signer creation
fn bench_signer_creation(c: &mut Criterion) {
    c.bench_function("signer_from_hex", |b| {
        b.iter(|| FastSigner::from_hex(black_box(TEST_PRIVATE_KEY)).unwrap())
    });
}

/// Benchmark hash signing
fn bench_sign_hash(c: &mut Criterion) {
    let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();
    let hash = B256::repeat_byte(0xab);

    let mut group = c.benchmark_group("signing");

    group.bench_function("sign_hash_legacy", |b| {
        b.iter(|| signer.sign_hash(black_box(&hash), 1).unwrap())
    });

    group.bench_function("sign_hash_typed", |b| {
        b.iter(|| signer.sign_hash_typed(black_box(&hash)).unwrap())
    });

    group.finish();
}

/// Benchmark transaction encoding
fn bench_transaction_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("encoding");

    // Legacy transaction
    let legacy_tx = LegacyTransaction {
        nonce: 0,
        gas_price: U256::from(20_000_000_000u64),
        gas_limit: 21000,
        to: Some(Address::repeat_byte(1)),
        value: U256::from(1_000_000_000_000_000_000u64),
        data: Bytes::new(),
        chain_id: 1,
    };

    group.bench_function("legacy_encode_for_signing", |b| {
        b.iter(|| black_box(&legacy_tx).encode_for_signing())
    });

    // EIP-1559 transaction
    let eip1559_tx = Eip1559Transaction {
        chain_id: 1,
        nonce: 0,
        max_priority_fee_per_gas: U256::from(1_000_000_000u64),
        max_fee_per_gas: U256::from(20_000_000_000u64),
        gas_limit: 21000,
        to: Some(Address::repeat_byte(1)),
        value: U256::from(1_000_000_000_000_000_000u64),
        data: Bytes::new(),
        access_list: vec![],
    };

    group.bench_function("eip1559_encode_for_signing", |b| {
        b.iter(|| black_box(&eip1559_tx).encode_for_signing())
    });

    // With data payload
    let tx_with_data = LegacyTransaction {
        nonce: 0,
        gas_price: U256::from(20_000_000_000u64),
        gas_limit: 100000,
        to: Some(Address::repeat_byte(1)),
        value: U256::ZERO,
        data: Bytes::from(vec![0xabu8; 256]),
        chain_id: 1,
    };

    group.bench_function("legacy_encode_with_256b_data", |b| {
        b.iter(|| black_box(&tx_with_data).encode_for_signing())
    });

    group.finish();
}

/// Benchmark full transaction signing (encode + hash + sign)
fn bench_full_transaction_sign(c: &mut Criterion) {
    let signer = FastSigner::from_hex(TEST_PRIVATE_KEY).unwrap();

    let mut group = c.benchmark_group("full_sign");

    // Legacy transaction
    group.bench_function("legacy_full_sign", |b| {
        let typed_tx = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build()
            .unwrap();

        b.iter(|| Transaction::sign(black_box(typed_tx.clone()), &signer).unwrap())
    });

    // EIP-1559 transaction
    group.bench_function("eip1559_full_sign", |b| {
        let typed_tx = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .max_fee_per_gas(U256::from(20_000_000_000u64))
            .max_priority_fee_per_gas(U256::from(1_000_000_000u64))
            .nonce(0)
            .chain_id(1)
            .build()
            .unwrap();

        b.iter(|| Transaction::sign(black_box(typed_tx.clone()), &signer).unwrap())
    });

    group.finish();
}

/// Benchmark wallet sign (includes nonce management)
fn bench_wallet_sign(c: &mut Criterion) {
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .chain_id(1)
        .build_with_nonce(0)
        .unwrap();

    let mut group = c.benchmark_group("wallet");

    group.bench_function("sign_legacy", |b| {
        b.iter(|| {
            wallet
                .sign_legacy(
                    Address::repeat_byte(1),
                    U256::from(1_000_000_000_000_000_000u64),
                    Bytes::new(),
                    21000,
                    U256::from(20_000_000_000u64),
                )
                .unwrap()
        })
    });

    group.bench_function("sign_eip1559", |b| {
        b.iter(|| {
            wallet
                .sign_eip1559(
                    Address::repeat_byte(1),
                    U256::from(1_000_000_000_000_000_000u64),
                    Bytes::new(),
                    21000,
                    U256::from(50_000_000_000u64),
                    U256::from(2_000_000_000u64),
                )
                .unwrap()
        })
    });

    group.finish();
}

/// Benchmark nonce management
fn bench_nonce_management(c: &mut Criterion) {
    let mut group = c.benchmark_group("nonce");

    // Single-threaded nonce tracker
    group.bench_function("tracker_get_and_increment", |b| {
        let tracker = NonceTracker::new(0);
        b.iter(|| tracker.get_and_increment())
    });

    // Single address nonce manager
    group.bench_function("single_address_get_nonce", |b| {
        let manager = SingleAddressNonceManager::new(Address::ZERO, 0);
        b.iter(|| manager.get_nonce())
    });

    // Multi-address nonce manager
    group.bench_function("multi_address_get_nonce", |b| {
        let manager = NonceManager::new();
        let addr = Address::ZERO;
        manager.init_nonce(addr, 0);
        b.iter(|| manager.get_nonce(black_box(addr)))
    });

    group.finish();
}

/// Benchmark concurrent nonce access
fn bench_concurrent_nonce(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("concurrent_nonce");

    // Note: This benchmark measures contention overhead
    group.bench_function("tracker_4_threads_1000_ops", |b| {
        b.iter(|| {
            let tracker = Arc::new(NonceTracker::new(0));
            let mut handles = vec![];

            for _ in 0..4 {
                let tracker = tracker.clone();
                handles.push(thread::spawn(move || {
                    for _ in 0..250 {
                        tracker.get_and_increment();
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    group.finish();
}

/// Benchmark transaction throughput (how many we can sign per second)
fn bench_throughput(c: &mut Criterion) {
    let wallet = FastWalletBuilder::new(TEST_PRIVATE_KEY, "http://localhost:8545")
        .chain_id(1)
        .build_with_nonce(0)
        .unwrap();

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("transactions_per_second", |b| {
        b.iter(|| {
            let request = TransactionRequest::new()
                .to(Address::repeat_byte(1))
                .value(U256::from(1_000_000_000_000_000_000u64))
                .gas_limit(21000)
                .gas_price(U256::from(20_000_000_000u64));

            wallet.sign(black_box(request)).unwrap()
        })
    });

    group.finish();
}

/// Benchmark hex encoding/decoding
fn bench_hex(c: &mut Criterion) {
    use fast_wallet::crypto::fast_hex;

    let mut group = c.benchmark_group("hex");

    let data = vec![0xabu8; 32]; // 32 bytes like a hash

    group.bench_function("encode_32_bytes", |b| {
        b.iter(|| fast_hex::encode(black_box(&data)))
    });

    group.bench_function("encode_prefixed_32_bytes", |b| {
        b.iter(|| fast_hex::encode_prefixed(black_box(&data)))
    });

    let hex_str = "abababababababababababababababababababababababababababababababab";
    group.bench_function("decode_32_bytes", |b| {
        b.iter(|| fast_hex::decode(black_box(hex_str)).unwrap())
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_keccak256,
    bench_signer_creation,
    bench_sign_hash,
    bench_transaction_encoding,
    bench_full_transaction_sign,
    bench_wallet_sign,
    bench_nonce_management,
    bench_concurrent_nonce,
    bench_throughput,
    bench_hex,
);

criterion_main!(benches);

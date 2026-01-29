//! RPC Performance benchmarks using local Anvil node
//!
//! Prerequisites:
//!   1. Install Foundry: curl -L https://foundry.paradigm.xyz | bash && foundryup
//!   2. Start Anvil: anvil --block-time 1
//!
//! Run with: cargo bench --bench rpc_benchmark
//!
//! These benchmarks measure RPC-dependent operations:
//! - Gas price fetching
//! - Transaction sending
//! - Nonce syncing
//! - Preheat performance
//! - Gas coalescing effectiveness

use criterion::{criterion_group, criterion_main, Criterion};
use fast_wallet::{FastWalletBuilder, TransactionRequest};
use alloy_primitives::{Address, U256};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

const ANVIL_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const ANVIL_RPC_URL: &str = "http://127.0.0.1:8545";

/// Check if Anvil is running
fn check_anvil(rt: &Runtime) -> bool {
    rt.block_on(async {
        let client = reqwest::Client::new();
        client
            .post(ANVIL_RPC_URL)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_chainId",
                "params": [],
                "id": 1
            }))
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .is_ok()
    })
}

/// Benchmark wallet creation with nonce fetch
fn bench_wallet_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        eprintln!("⚠️  Anvil not running, skipping RPC benchmarks");
        eprintln!("   Start with: anvil --block-time 1");
        return;
    }

    let mut group = c.benchmark_group("rpc_wallet");
    group.sample_size(20); // Fewer samples for RPC tests

    group.bench_function("create_with_nonce_fetch", |b| {
        b.to_async(&rt).iter(|| async {
            FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
                .chain_id(31337)
                .build()
                .await
                .unwrap()
        })
    });

    group.bench_function("create_with_known_nonce", |b| {
        b.iter(|| {
            FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
                .chain_id(31337)
                .build_with_nonce(0)
                .unwrap()
        })
    });

    group.finish();
}

/// Benchmark gas price fetching
fn bench_gas_price(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        return;
    }

    let wallet = rt.block_on(async {
        FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
            .chain_id(31337)
            .build()
            .await
            .unwrap()
    });

    let mut group = c.benchmark_group("rpc_gas");
    group.sample_size(50);

    // First call (cache miss)
    group.bench_function("gas_price_cold", |b| {
        b.to_async(&rt).iter(|| async {
            // Clear cache by waiting
            tokio::time::sleep(Duration::from_secs(1)).await;
            wallet.get_gas_price().await.unwrap()
        })
    });

    // Cached call
    group.bench_function("gas_price_cached", |b| {
        // Warm up cache
        rt.block_on(wallet.get_gas_price()).unwrap();

        b.to_async(&rt).iter(|| async {
            wallet.get_gas_price().await.unwrap()
        })
    });

    // Priority fee
    group.bench_function("priority_fee_cached", |b| {
        rt.block_on(wallet.get_priority_fee()).ok();

        b.to_async(&rt).iter(|| async {
            wallet.get_priority_fee().await.ok()
        })
    });

    // Both in parallel
    group.bench_function("gas_prices_parallel", |b| {
        b.to_async(&rt).iter(|| async {
            tokio::join!(
                wallet.get_gas_price(),
                wallet.get_priority_fee()
            )
        })
    });

    group.finish();
}

/// Benchmark preheat performance
fn bench_preheat(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        return;
    }

    let wallet = rt.block_on(async {
        FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
            .chain_id(31337)
            .build()
            .await
            .unwrap()
    });

    let mut group = c.benchmark_group("rpc_preheat");
    group.sample_size(30);

    // Preheat without gas fetch (nonce only)
    group.bench_function("preheat_nonce_only", |b| {
        b.to_async(&rt).iter(|| async {
            let ctx = wallet.preheat(false).await.unwrap();
            wallet.cancel_preheat(ctx);
        })
    });

    // Preheat with gas fetch
    group.bench_function("preheat_with_gas", |b| {
        b.to_async(&rt).iter(|| async {
            let ctx = wallet.preheat(true).await.unwrap();
            wallet.cancel_preheat(ctx);
        })
    });

    // Batch preheat
    group.bench_function("preheat_batch_5", |b| {
        b.to_async(&rt).iter(|| async {
            let contexts = wallet.preheat_batch(5, true).await.unwrap();
            for ctx in contexts {
                wallet.cancel_preheat(ctx);
            }
        })
    });

    group.finish();
}

/// Benchmark gas coalescing (concurrent requests)
fn bench_gas_coalescing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        return;
    }

    let wallet = Arc::new(rt.block_on(async {
        FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
            .chain_id(31337)
            .build()
            .await
            .unwrap()
    }));

    let mut group = c.benchmark_group("rpc_coalescing");
    group.sample_size(20);

    // Sequential gas fetches (baseline)
    group.bench_function("sequential_4_gas_fetches", |b| {
        b.to_async(&rt).iter(|| {
            let w = wallet.clone();
            async move {
                // Clear cache
                tokio::time::sleep(Duration::from_millis(600)).await;
                for _ in 0..4 {
                    w.get_gas_price().await.unwrap();
                    // Small delay to ensure sequential
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        })
    });

    // Concurrent gas fetches (should coalesce)
    group.bench_function("concurrent_4_gas_fetches", |b| {
        b.to_async(&rt).iter(|| {
            let w = wallet.clone();
            async move {
                // Clear cache
                tokio::time::sleep(Duration::from_millis(600)).await;
                let futures: Vec<_> = (0..4)
                    .map(|_| {
                        let w = w.clone();
                        async move { w.get_gas_price().await }
                    })
                    .collect();
                futures::future::join_all(futures).await
            }
        })
    });

    // Concurrent preheats
    group.bench_function("concurrent_4_preheats", |b| {
        b.to_async(&rt).iter(|| {
            let w = wallet.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(600)).await;
                let futures: Vec<_> = (0..4)
                    .map(|_| {
                        let w = w.clone();
                        async move {
                            let ctx = w.preheat(true).await?;
                            w.cancel_preheat(ctx);
                            Ok::<_, fast_wallet::WalletError>(())
                        }
                    })
                    .collect();
                futures::future::join_all(futures).await
            }
        })
    });

    group.finish();
}

/// Benchmark transaction sending
fn bench_send_transaction(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        return;
    }

    let mut group = c.benchmark_group("rpc_send");
    group.sample_size(10); // Fewer samples as these actually send txs

    // Sign + Send
    group.bench_function("sign_and_send", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
                .chain_id(31337)
                .build()
                .await
                .unwrap();

            wallet.warmup().await.unwrap();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let request = TransactionRequest::new()
                    .to(Address::repeat_byte(0xaa))
                    .value(U256::from(1_000_000_000_000u64))
                    .gas_limit(21000)
                    .gas_price(U256::from(1_000_000_000u64));

                wallet.send(request).await.unwrap();
            }
            start.elapsed()
        })
    });

    // Preheated send
    group.bench_function("preheated_send", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
                .chain_id(31337)
                .build()
                .await
                .unwrap();

            wallet.warmup().await.unwrap();

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let ctx = wallet.preheat(true).await.unwrap();

                let request = TransactionRequest::new()
                    .to(Address::repeat_byte(0xaa))
                    .value(U256::from(1_000_000_000_000u64))
                    .gas_limit(21000);

                wallet.send_with_preheat(&ctx, request).await.unwrap();
            }
            start.elapsed()
        })
    });

    group.finish();
}

/// Benchmark warmup
fn bench_warmup(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        return;
    }

    let mut group = c.benchmark_group("rpc_warmup");
    group.sample_size(10);

    group.bench_function("full_warmup", |b| {
        b.to_async(&rt).iter(|| async {
            let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
                .chain_id(31337)
                .build()
                .await
                .unwrap();

            wallet.warmup().await.unwrap()
        })
    });

    group.finish();
}

/// Benchmark nonce sync
fn bench_nonce_sync(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    if !check_anvil(&rt) {
        return;
    }

    let wallet = rt.block_on(async {
        FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
            .chain_id(31337)
            .build()
            .await
            .unwrap()
    });

    let mut group = c.benchmark_group("rpc_nonce");
    group.sample_size(30);

    group.bench_function("sync_nonce", |b| {
        b.to_async(&rt).iter(|| async {
            wallet.sync_nonce().await.unwrap()
        })
    });

    group.finish();
}

criterion_group!(
    name = rpc_benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(2));
    targets =
        bench_wallet_creation,
        bench_gas_price,
        bench_preheat,
        bench_gas_coalescing,
        bench_send_transaction,
        bench_warmup,
        bench_nonce_sync,
);

criterion_main!(rpc_benches);

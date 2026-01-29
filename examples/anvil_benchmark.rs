//! Realistic benchmark using Foundry Anvil
//!
//! This benchmark tests actual transaction submission performance using Anvil.
//!
//! Prerequisites:
//! - Install Foundry: curl -L https://foundry.paradigm.xyz | bash && foundryup
//!
//! Run:
//! 1. Start Anvil: anvil --block-time 1
//! 2. Run benchmark: cargo run --example anvil_benchmark --release
//!
//! For mainnet fork testing:
//! anvil --fork-url https://eth.llamarpc.com --block-time 1

use fast_wallet::{FastWalletBuilder, TransactionRequest};
use alloy_primitives::{Address, U256};
use std::time::{Duration, Instant};

// Anvil default private keys (DO NOT USE IN PRODUCTION)
const ANVIL_PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const ANVIL_RPC_URL: &str = "http://127.0.0.1:8545";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Fast Wallet - Anvil Benchmark ===\n");

    // Check if Anvil is running
    let client = reqwest::Client::new();
    let check = client
        .post(ANVIL_RPC_URL)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        }))
        .send()
        .await;

    if check.is_err() {
        println!("ERROR: Anvil is not running!");
        println!("Please start Anvil first:");
        println!("  anvil --block-time 1");
        println!("\nFor mainnet fork testing:");
        println!("  anvil --fork-url https://eth.llamarpc.com --block-time 1");
        return Ok(());
    }

    println!("Connected to Anvil at {}\n", ANVIL_RPC_URL);

    // ==================== Benchmark 1: Wallet Creation ====================
    println!("--- Benchmark 1: Wallet Creation ---");

    let start = Instant::now();
    let iterations = 100;

    for _ in 0..iterations {
        let _ = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
            .chain_id(31337)
            .build_with_nonce(0)?;
    }

    let elapsed = start.elapsed();
    println!(
        "Wallet creation (sync, known nonce): {:.2} µs/op ({} iterations)",
        elapsed.as_micros() as f64 / iterations as f64,
        iterations
    );

    // Async wallet creation with nonce fetch
    let start = Instant::now();
    let iterations = 10;

    for _ in 0..iterations {
        let _ = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
            .chain_id(31337)
            .build()
            .await?;
    }

    let elapsed = start.elapsed();
    println!(
        "Wallet creation (async, fetch nonce): {:.2} ms/op ({} iterations)",
        elapsed.as_millis() as f64 / iterations as f64,
        iterations
    );

    // ==================== Benchmark 2: Warmup Performance ====================
    println!("\n--- Benchmark 2: Warmup Performance ---");

    let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
        .chain_id(31337)
        .build()
        .await?;

    let start = Instant::now();
    wallet.warmup().await?;
    let warmup_time = start.elapsed();

    println!("Warmup time: {:.2} ms", warmup_time.as_millis());
    println!("Wallet warmed up: {}", wallet.is_warmed_up());

    // ==================== Benchmark 3: Transaction Signing ====================
    println!("\n--- Benchmark 3: Transaction Signing (CPU only) ---");

    let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
        .chain_id(31337)
        .build_with_nonce(0)?;

    let iterations = 10000;
    let start = Instant::now();

    for _ in 0..iterations {
        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000u64)) // 0.001 ETH
            .gas_limit(21000)
            .gas_price(U256::from(1_000_000_000u64)); // 1 gwei

        let _ = wallet.sign(request)?;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();

    println!(
        "Signing speed: {:.0} tx/sec ({:.2} µs/tx)",
        ops_per_sec,
        elapsed.as_micros() as f64 / iterations as f64
    );

    // ==================== Benchmark 4: Preheat Performance ====================
    println!("\n--- Benchmark 4: Preheat Performance ---");

    let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
        .chain_id(31337)
        .build()
        .await?;

    // Preheat without gas fetch
    let start = Instant::now();
    let iterations = 1000;

    for _ in 0..iterations {
        let ctx = wallet.preheat(false).await?;
        wallet.cancel_preheat(ctx);
    }

    let elapsed = start.elapsed();
    println!(
        "Preheat (no gas fetch): {:.2} µs/op",
        elapsed.as_micros() as f64 / iterations as f64
    );

    // Preheat with gas fetch
    let start = Instant::now();
    let iterations = 50;

    for _ in 0..iterations {
        let ctx = wallet.preheat(true).await?;
        wallet.cancel_preheat(ctx);
    }

    let elapsed = start.elapsed();
    println!(
        "Preheat (with gas fetch): {:.2} ms/op",
        elapsed.as_millis() as f64 / iterations as f64
    );

    // ==================== Benchmark 5: End-to-End Transaction ====================
    println!("\n--- Benchmark 5: End-to-End Transaction (Sign + Send) ---");

    let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
        .chain_id(31337)
        .build()
        .await?;

    // Warmup first
    wallet.warmup().await?;

    let recipient = Address::repeat_byte(0xaa);
    let iterations = 20;
    let mut times = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let start = Instant::now();

        let request = TransactionRequest::new()
            .to(recipient)
            .value(U256::from(1_000_000_000_000u64)) // 0.000001 ETH
            .gas_limit(21000)
            .gas_price(U256::from(1_000_000_000u64)); // 1 gwei

        let tx_hash = wallet.send(request).await?;
        let elapsed = start.elapsed();
        times.push(elapsed);

        if i == 0 {
            println!("First tx hash: {:?}", tx_hash);
        }
    }

    let total: Duration = times.iter().sum();
    let avg = total / iterations as u32;
    let min = times.iter().min().unwrap();
    let max = times.iter().max().unwrap();

    println!(
        "End-to-end (sign + send): avg {:.2} ms, min {:.2} ms, max {:.2} ms",
        avg.as_millis(),
        min.as_secs_f64() * 1000.0,
        max.as_secs_f64() * 1000.0
    );

    // ==================== Benchmark 6: Preheated Transaction ====================
    println!("\n--- Benchmark 6: Preheated Transaction ---");

    let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
        .chain_id(31337)
        .build()
        .await?;

    wallet.warmup().await?;

    let iterations = 20;
    let mut times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        // Preheat (simulating opportunity detection)
        let ctx = wallet.preheat(true).await?;

        // Simulate some computation time
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Now send with preheated context
        let start = Instant::now();

        let request = TransactionRequest::new()
            .to(recipient)
            .value(U256::from(1_000_000_000_000u64))
            .gas_limit(21000);
        // Gas prices come from preheated context

        let _ = wallet.send_with_preheat(&ctx, request).await?;
        let elapsed = start.elapsed();
        times.push(elapsed);
    }

    let total: Duration = times.iter().sum();
    let avg = total / iterations as u32;
    let min = times.iter().min().unwrap();
    let max = times.iter().max().unwrap();

    println!(
        "Preheated send: avg {:.2} ms, min {:.2} ms, max {:.2} ms",
        avg.as_millis(),
        min.as_secs_f64() * 1000.0,
        max.as_secs_f64() * 1000.0
    );

    // ==================== Benchmark 7: Batch Transactions ====================
    println!("\n--- Benchmark 7: Batch Transaction Sending ---");

    let wallet = FastWalletBuilder::new(ANVIL_PRIVATE_KEY, ANVIL_RPC_URL)
        .chain_id(31337)
        .build()
        .await?;

    wallet.warmup().await?;

    let batch_size = 10;
    let requests: Vec<_> = (0..batch_size)
        .map(|i| {
            TransactionRequest::new()
                .to(Address::repeat_byte(i as u8))
                .value(U256::from(1_000_000_000_000u64))
                .gas_limit(21000)
                .gas_price(U256::from(1_000_000_000u64))
        })
        .collect();

    let start = Instant::now();
    let results = wallet.send_batch(requests).await;
    let elapsed = start.elapsed();

    let success_count = results.iter().filter(|r| r.is_ok()).count();

    println!(
        "Batch {} txs: {:.2} ms total ({:.2} ms/tx), {} succeeded",
        batch_size,
        elapsed.as_millis(),
        elapsed.as_millis() as f64 / batch_size as f64,
        success_count
    );

    // ==================== Summary ====================
    println!("\n=== Benchmark Complete ===");
    println!("All transactions submitted successfully!");

    Ok(())
}

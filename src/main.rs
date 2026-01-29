//! Fast Wallet CLI - Example usage
//!
//! This is a simple demonstration of the fast-wallet library capabilities.

use alloy_primitives::{Address, U256};
use fast_wallet::TransactionRequest;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("fast_wallet=info".parse()?)
                .add_directive("info".parse()?),
        )
        .init();

    info!("Fast Wallet - High-performance EVM wallet for liquidation bots");

    // Example usage (won't actually connect without valid RPC)
    println!("\nExample usage:");
    println!("==============\n");

    println!("1. Create a wallet with known nonce (fastest startup):");
    println!("   ```rust");
    println!("   use fast_wallet::{{FastWallet, FastWalletBuilder}};");
    println!();
    println!("   let wallet = FastWalletBuilder::new(PRIVATE_KEY, RPC_URL)");
    println!("       .chain_id(1)");
    println!("       .initial_nonce(100)  // Skip RPC call");
    println!("       .build_with_nonce(100)?;");
    println!("   ```\n");

    println!("2. Sign and send a transaction:");
    println!("   ```rust");
    println!("   let tx = wallet.sign_legacy_transaction(");
    println!("       to_address,");
    println!("       U256::from(1_000_000_000_000_000_000u64), // 1 ETH");
    println!("       Bytes::new(),");
    println!("       21000,  // gas limit");
    println!("       U256::from(20_000_000_000u64),  // 20 gwei");
    println!("   )?;");
    println!();
    println!("   let tx_hash = wallet.send_signed(&tx).await?;");
    println!("   ```\n");

    println!("3. Send multiple transactions in parallel:");
    println!("   ```rust");
    println!("   let requests = vec![tx_request1, tx_request2, tx_request3];");
    println!("   let results = wallet.send_batch(requests).await;");
    println!("   ```\n");

    println!("4. Use multiple RPC endpoints for broadcasting:");
    println!("   ```rust");
    println!("   let wallet = FastWalletBuilder::new(PRIVATE_KEY, PRIMARY_RPC)");
    println!("       .broadcast_rpcs(vec![RPC1, RPC2, RPC3])");
    println!("       .build().await?;");
    println!("   ```\n");

    // Run benchmarks if requested
    if std::env::args().any(|arg| arg == "--bench") {
        run_signing_benchmark();
    }

    Ok(())
}

/// Run a quick signing benchmark
fn run_signing_benchmark() {
    use fast_wallet::{FastSigner, FastWalletBuilder};
    use std::time::Instant;

    println!("\nRunning signing benchmark...\n");

    let private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    // Benchmark signer creation
    let start = Instant::now();
    let iterations = 1000;

    for _ in 0..iterations {
        let _ = FastSigner::from_hex(private_key).unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Signer creation: {} iterations in {:?} ({:.2} μs/op)",
        iterations,
        elapsed,
        elapsed.as_micros() as f64 / iterations as f64
    );

    // Benchmark transaction signing
    let wallet = FastWalletBuilder::new(private_key, "http://localhost:8545")
        .chain_id(1)
        .build_with_nonce(0)
        .unwrap();

    let start = Instant::now();
    let iterations = 10000;

    for _ in 0..iterations {
        let request = TransactionRequest::new()
            .to(Address::repeat_byte(1))
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64));

        let _ = wallet.sign_transaction(request).unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Transaction signing: {} iterations in {:?} ({:.2} μs/op, {:.0} tx/sec)",
        iterations,
        elapsed,
        elapsed.as_micros() as f64 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Benchmark hash computation
    use fast_wallet::crypto::keccak256;
    let data = vec![0u8; 256];

    let start = Instant::now();
    let iterations = 100000;

    for _ in 0..iterations {
        let _ = keccak256(&data);
    }

    let elapsed = start.elapsed();
    println!(
        "Keccak256 (256 bytes): {} iterations in {:?} ({:.2} ns/op)",
        iterations,
        elapsed,
        elapsed.as_nanos() as f64 / iterations as f64
    );

    println!("\nBenchmark complete!");
}

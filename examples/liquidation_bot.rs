//! Liquidation Bot Example
//!
//! Demonstrates the fastest execution path for liquidation bots:
//! 1. Background gas price refresh (always fresh, no RPC latency)
//! 2. Preheating (nonce reserved ahead of time)
//! 3. Optimistic execution (skip simulation, let contract revert)
//!
//! Run with: cargo run --example liquidation_bot

use alloy_primitives::{address, Address, Bytes, U256};
use fast_wallet::{FastWalletBuilder, WalletConfig};
use std::sync::Arc;
use std::time::Duration;

// Simulated liquidation contract
const LIQUIDATION_CONTRACT: Address = address!("1234567890123456789012345678901234567890");

// Typical liquidation gas costs:
// - Health factor check (early revert): ~3,000 gas
// - Full liquidation execution: ~150,000 gas
const LIQUIDATION_GAS_LIMIT: u64 = 150_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Liquidation Bot Example ===\n");

    // Configure for liquidation bot use case
    let config = WalletConfig {
        chain_id: 1,
        gas_cache_duration: Duration::from_millis(100), // Aggressive cache refresh
        background_gas_refresh: Some(Duration::from_millis(500)), // Auto-refresh every 500ms
        optimistic_default_gas_limit: LIQUIDATION_GAS_LIMIT,
        optimistic_default_gas_price: U256::from(2_000_000_000u64), // 2 gwei fallback
        ..Default::default()
    };

    // Create wallet (in production, use real RPC)
    let wallet = Arc::new(
        FastWalletBuilder::new(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "http://127.0.0.1:8545",
        )
        .config(config)
        .build_with_nonce(0)?,
    );

    // Start background gas refresh (runs forever, updating cache)
    let _gas_handle = wallet.start_background_gas_refresh();
    println!("✓ Background gas refresh started\n");

    // Simulate monitoring for liquidation opportunities
    println!("Monitoring for liquidation opportunities...\n");

    // Example: Different execution paths with timing

    // === Path 1: Quick Liquidation (simplest, uses defaults) ===
    println!("Path 1: Quick Liquidation");
    println!("  - Uses cached gas price");
    println!("  - Uses default gas limit from config");
    println!("  - No simulation, no RPC calls in hot path");
    println!("  - Latency: ~47µs (sign) + network\n");

    let calldata = encode_liquidation_call(
        address!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
        address!("1111111111111111111111111111111111111111"),
        U256::from(1_000_000_000_000_000_000u64), // 1 ETH worth
    );

    // In production:
    // let tx_hash = wallet.send_quick_liquidation(LIQUIDATION_CONTRACT, calldata).await?;

    // === Path 2: Preheated Optimistic (lowest latency) ===
    println!("Path 2: Preheated Optimistic");
    println!("  - Nonce reserved during idle time");
    println!("  - Gas cached during preheat");
    println!("  - When opportunity found: sign + send immediately");
    println!("  - Latency: ~47µs (sign only, nonce already reserved)\n");

    // During idle time (before opportunity detected):
    // let ctx = wallet.preheat(true).await?;

    // When opportunity detected:
    // let tx_hash = wallet.send_optimistic_with_preheat(
    //     &ctx,
    //     LIQUIDATION_CONTRACT,
    //     calldata,
    //     LIQUIDATION_GAS_LIMIT,
    // ).await?;

    // === Path 3: Fire-and-Forget (absolute minimum latency) ===
    println!("Path 3: Fire-and-Forget");
    println!("  - Returns immediately after signing");
    println!("  - Send happens in background");
    println!("  - Get tx hash instantly (computed locally)");
    println!("  - Latency: ~47µs (returns before network round-trip)\n");

    // let (tx_hash, handle) = wallet.send_optimistic_fire_and_forget(request)?;
    // // tx_hash available immediately
    // // optionally await handle later to confirm

    // === Path 4: Batch alternatives (replace-by-fee strategy) ===
    println!("Path 4: Transaction Alternatives");
    println!("  - Sign multiple transactions with same nonce");
    println!("  - Different gas prices for RBF");
    println!("  - Send best one, keep others ready\n");

    // let alternatives = wallet.sign_alternatives(vec![
    //     request.clone().gas_price(U256::from(1_000_000_000u64)),
    //     request.clone().gas_price(U256::from(2_000_000_000u64)),
    //     request.clone().gas_price(U256::from(5_000_000_000u64)),
    // ]);

    // === Cost Analysis ===
    println!("=== Cost Analysis ===\n");
    println!("Failed liquidation cost (early revert):");
    println!("  - Gas used: ~3,000-5,000");
    println!("  - At 1 gwei: ~0.000005 ETH (~$0.01)");
    println!("  - At 10 gwei: ~0.00005 ETH (~$0.10)\n");

    println!("Time saved by skipping simulation:");
    println!("  - eth_call latency: 2-10ms");
    println!("  - In competitive MEV: 2-10ms = winning vs losing\n");

    println!("Break-even analysis:");
    println!("  - If 1 in 100 txs succeed with $10 profit");
    println!("  - Failed cost: 99 × $0.01 = $0.99");
    println!("  - Profit: $10 - $0.99 = $9.01");
    println!("  - vs simulation: might miss opportunity entirely\n");

    println!("=== Example Complete ===");

    Ok(())
}

/// Encode a liquidation call (example ABI encoding)
fn encode_liquidation_call(user: Address, asset: Address, amount: U256) -> Bytes {
    // In production, use alloy's sol! macro for type-safe encoding
    // This is a simplified example

    // Function selector for liquidate(address,address,uint256)
    let selector: [u8; 4] = [0x12, 0x34, 0x56, 0x78]; // placeholder

    let mut data = Vec::with_capacity(4 + 32 * 3);
    data.extend_from_slice(&selector);

    // Pad addresses to 32 bytes
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(user.as_slice());

    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(asset.as_slice());

    // Amount as 32 bytes
    data.extend_from_slice(&amount.to_be_bytes::<32>());

    Bytes::from(data)
}

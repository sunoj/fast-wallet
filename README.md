# Fast Wallet

High-performance EVM wallet optimized for liquidation bots and MEV applications.

## Features

- **~24,100 tx/sec** signing throughput
- **~11ns** lock-free nonce acquisition
- **~40µs** optimistic send latency (CPU only)
- **Optimistic execution**: Skip simulation for lowest latency
- **Fire-and-forget**: Return immediately, send in background
- **Gas coalescing**: Multiple requests share single RPC call
- **Background gas refresh**: Always-fresh gas prices
- **Preheating API**: Pre-allocate resources for fastest execution

## Quick Start

```toml
[dependencies]
fast-wallet = { git = "https://github.com/user/fast-wallet" }
tokio = { version = "1.35", features = ["full"] }
alloy-primitives = "0.8"
```

### Basic Usage

```rust
use fast_wallet::{FastWalletBuilder, TransactionRequest};
use alloy_primitives::{Address, U256};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wallet = FastWalletBuilder::new(
        "0xYOUR_PRIVATE_KEY",
        "https://eth-mainnet.g.alchemy.com/v2/KEY"
    )
    .chain_id(1)
    .build()
    .await?;

    // Warmup (recommended)
    wallet.warmup().await?;

    // Send transaction
    let tx_hash = wallet.send(
        TransactionRequest::new()
            .to("0x...".parse()?)
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
    ).await?;

    Ok(())
}
```

## Liquidation Bot Usage

### Optimistic Execution (Fastest Path)

Skip simulation and let the contract revert on failure:

```rust
use std::sync::Arc;

let wallet = Arc::new(
    FastWalletBuilder::new(private_key, rpc_url)
        .chain_id(1)
        .background_gas_refresh(Duration::from_millis(500))
        .build()
        .await?
);

// Start background gas refresh
wallet.start_background_gas_refresh();

// When liquidation opportunity detected:
let tx_hash = wallet.send_quick_liquidation(
    liquidation_contract,
    encoded_calldata,
).await?;
```

### Preheated Optimistic (Lowest Latency)

```rust
// During idle time - reserve nonce and fetch gas
let ctx = wallet.preheat(true).await?;

// When opportunity detected - sign and send immediately
let tx_hash = wallet.send_optimistic_with_preheat(
    &ctx,
    contract_address,
    calldata,
    gas_limit,
).await?;
```

### Fire-and-Forget (Absolute Minimum Latency)

```rust
// Returns immediately after signing (~45µs)
let (tx_hash, handle) = wallet.send_optimistic_fire_and_forget(request)?;

// tx_hash available immediately
// Optionally await handle later to confirm
```

### Transaction Alternatives (Replace-by-Fee)

```rust
// Sign multiple transactions with same nonce, different gas prices
let alternatives = wallet.sign_alternatives(vec![
    request.clone().gas_price(U256::from(1_000_000_000u64)),
    request.clone().gas_price(U256::from(2_000_000_000u64)),
    request.clone().gas_price(U256::from(5_000_000_000u64)),
]);
```

## API Reference

### Wallet Creation

```rust
// Async (fetches nonce)
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .build()
    .await?;

// Sync with known nonce (faster startup)
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .build_with_nonce(0)?;

// With multiple RPC endpoints
let wallet = FastWalletBuilder::new(private_key, primary_rpc)
    .chain_id(1)
    .broadcast_rpcs(vec![rpc1, rpc2, rpc3])
    .build()
    .await?;
```

### Send Methods

| Method | Use Case | Latency |
|--------|----------|---------|
| `send()` | Standard send | 12-60ms |
| `send_optimistic()` | Skip simulation | 10-50ms |
| `send_quick_liquidation()` | Liquidation with defaults | 10-50ms |
| `send_optimistic_with_preheat()` | Preheated + optimistic | ~45µs + network |
| `send_optimistic_fire_and_forget()` | Return immediately | ~45µs |

### Configuration

```rust
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .max_pending_txs(100)
    .use_eip1559(true)
    .gas_cache_duration(Duration::from_millis(500))
    .background_gas_refresh(Duration::from_millis(500))
    .optimistic_default_gas_limit(150_000)
    .optimistic_default_gas_price(U256::from(1_000_000_000u64))
    .build()
    .await?;
```

## Benchmarks

Run CPU benchmarks:

```bash
cargo bench --bench transaction_benchmark
```

Run RPC benchmarks (requires Anvil):

```bash
anvil --block-time 1
cargo bench --bench rpc_benchmark
```

See [BENCHMARKS.md](BENCHMARKS.md) for detailed performance data.

## License

MIT

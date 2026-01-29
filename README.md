# Fast Wallet

High-performance EVM wallet optimized for liquidation bots and MEV applications.

## Features

- **~20,000 tx/sec** signing throughput
- **~13ns** lock-free nonce acquisition
- **~50µs** optimistic send latency (CPU only)
- **Connection warmup**: Save 5-20ms with pre-established TCP/TLS connections
- **20+ built-in RPC presets**: Public, MEV-protected, and regional endpoints
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

### Connection Warmup (Save 5-20ms)

Preestablish TCP/TLS connections before time-critical operations:

```rust
// Simple warmup
wallet.warmup().await?;

// Full preheat: connections + nonce + gas prices
let ctx = wallet.preheat_full().await?;
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

// Using Blink Labs RPC (low-latency, MEV-optimized)
let wallet = FastWalletBuilder::with_blink(private_key, "your_blink_api_key")
    .chain_id(1)
    .build()
    .await?;

// Sync with known nonce (faster startup)
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .build_with_nonce(0)?;

// With multiple RPC endpoints (including Blink)
let wallet = FastWalletBuilder::new(private_key, primary_rpc)
    .chain_id(1)
    .add_blink_broadcast("your_blink_api_key")
    .broadcast_rpcs(vec![rpc1, rpc2])
    .build()
    .await?;

// Using built-in public RPC presets
use fast_wallet::{BroadcasterBuilder, BroadcastStrategy};

let broadcaster = BroadcasterBuilder::new()
    .add_liquidation_preset()  // MEV protection + regional + public RPCs
    .strategy(BroadcastStrategy::RaceAll)
    .build()?;
```

### Send Methods

| Method | Use Case | Latency |
|--------|----------|---------|
| `send()` | Standard send | 12-60ms |
| `send_optimistic()` | Skip simulation | 10-50ms |
| `send_quick_liquidation()` | Liquidation with defaults | 10-50ms |
| `send_optimistic_with_preheat()` | Preheated + optimistic | ~45µs + network |
| `send_optimistic_fire_and_forget()` | Return immediately | ~45µs |

### Built-in RPC Presets

```rust
use fast_wallet::{BroadcasterBuilder, RpcEndpoint};

// Individual endpoints
let llama = RpcEndpoint::llama();
let mev_blocker = RpcEndpoint::mev_blocker();
let flashbots = RpcEndpoint::flashbots_protect();

// Convenience presets
let broadcaster = BroadcasterBuilder::new()
    .add_default_public_rpcs()   // LlamaNodes, PublicNode, 1RPC, DRPC, Ankr, Cloudflare
    .add_mev_protection()         // Flashbots Protect, MEV Blocker
    .add_regional_rpcs()          // bloXroute Virginia/UK/Singapore
    .build()?;

// Or use the liquidation preset (includes all of the above)
let broadcaster = BroadcasterBuilder::new()
    .add_liquidation_preset()
    .build()?;
```

**Available Presets:**
| Method | Endpoints |
|--------|-----------|
| `add_default_public_rpcs()` | LlamaNodes, PublicNode, 1RPC, DRPC, Ankr, Cloudflare |
| `add_all_public_rpcs()` | All public + Blast, BlockPI, OMNIA, Tenderly, Merkle |
| `add_mev_protection()` | Flashbots Protect/Fast, MEV Blocker/Fast |
| `add_regional_rpcs()` | bloXroute Virginia, UK, Singapore |
| `add_liquidation_preset()` | MEV protection + Regional + Default public |
| `add_minimal_rpcs()` | LlamaNodes, PublicNode, 1RPC |

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

# Fast Wallet

High-performance EVM wallet optimized for liquidation bots and MEV applications.

## Features

- **~31,500 tx/sec** signing throughput (with `secp256k1-ffi`)
- **~13ns** lock-free nonce acquisition
- **~30µs** signing latency (with `secp256k1-ffi`)
- **Dual signing backends**: Pure Rust (k256) or Bitcoin Core's C library (1.5x faster)
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
fast-wallet = { git = "https://github.com/sunoj/fast-wallet" }
tokio = { version = "1.35", features = ["full"] }
alloy = { version = "1.0", features = ["rlp"] }

# For maximum performance (requires C compiler):
# fast-wallet = { git = "https://github.com/sunoj/fast-wallet", features = ["secp256k1-ffi"] }
```

### Signing Backend Comparison

| Backend | Throughput | Signing Latency | Notes |
|---------|------------|-----------------|-------|
| `k256` (default) | ~21,000 tx/sec | ~46 µs | Pure Rust, no dependencies |
| `secp256k1-ffi` | **~31,500 tx/sec** | **~30 µs** | Bitcoin Core's C library, 50% faster |

Enable the faster backend with:
```toml
fast-wallet = { version = "0.1", features = ["secp256k1-ffi"] }
```

### Basic Usage

```rust
use fast_wallet::{FastWalletBuilder, TransactionRequest};
use alloy::primitives::{Address, U256};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wallet = FastWalletBuilder::new(
        "0xYOUR_PRIVATE_KEY",
        "https://eth-mainnet.g.alchemy.com/v2/KEY"
    )
    .chain_id(1)
    .build()
    .await?;

    // Startup self-check: logs address, balance, nonce
    let status = wallet.self_check().await?;
    println!("{}", status); // Address: 0x... | Balance: 1.234 ETH | Nonce: 42 | Chain: 1

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
let ctx = wallet.preheat_full(true).await?;

// Same parallel warmup but skip gas RPC — for callers that supply
// explicit gas prices to sign_with_preheat.
let ctx = wallet.preheat_full(false).await?;
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

// With multiple RPC endpoints (primary is always included in broadcast)
let wallet = FastWalletBuilder::new(private_key, primary_rpc)
    .chain_id(1)
    .add_blink_broadcast("your_blink_api_key")
    .broadcast_rpcs(vec![rpc2, rpc3])
    .build()
    .await?;
// Broadcasts to: primary_rpc + rpc2 + rpc3 in parallel

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

### Wallet Self-Check & Status Query

```rust
// Startup self-check: fetches balance + nonce in parallel, logs via tracing
let status = wallet.self_check().await?;
// INFO ========== Wallet Self-Check ==========
// INFO   Address:  0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
// INFO   Balance:  1.500000 ETH (1500000000000000000 wei)
// INFO   Nonce:    42
// INFO   Chain ID: 1
// INFO =======================================

// Access status fields programmatically
println!("Address: {:?}", status.address);
println!("Balance: {} wei ({:.6} ETH)", status.balance, status.balance_eth());
println!("Nonce:   {}", status.nonce);

// Query balance/nonce on demand
let balance = wallet.get_balance().await?;
let nonce = wallet.get_nonce().await?;           // pending (includes mempool)
let nonce_confirmed = wallet.get_nonce_latest().await?;  // confirmed only
```

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

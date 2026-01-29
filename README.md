# Fast Wallet

High-performance EVM wallet optimized for liquidation bots and MEV applications.

## Features

- **Extreme Speed**: ~20,000 tx/sec signing, ~13ns nonce acquisition
- **Auto Nonce Management**: No need to track nonces manually
- **Preheating API**: Pre-allocate resources while calculating parameters
- **Multi-RPC Broadcasting**: Send to multiple endpoints for faster inclusion
- **Lock-free Operations**: Atomic nonce management without locks
- **Gas Price Caching**: Reduces RPC calls during high-frequency trading

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
fast-wallet = { path = "." }
tokio = { version = "1.35", features = ["full"] }
alloy-primitives = "0.8"
```

### Basic Usage

```rust
use fast_wallet::{FastWalletBuilder, TransactionRequest};
use alloy_primitives::{Address, U256};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create wallet (fetches initial nonce from chain)
    let wallet = FastWalletBuilder::new(
        "0xYOUR_PRIVATE_KEY",
        "https://eth-mainnet.alchemyapi.io/v2/YOUR_KEY"
    )
    .chain_id(1)
    .build()
    .await?;

    // Warmup connections (recommended)
    wallet.warmup().await?;

    // Sign and send - nonce is managed automatically
    let tx_hash = wallet.send(
        TransactionRequest::new()
            .to("0x...".parse()?)
            .value(U256::from(1_000_000_000_000_000_000u64)) // 1 ETH
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
    ).await?;

    println!("Transaction sent: {:?}", tx_hash);
    Ok(())
}
```

## API Reference

### Wallet Creation

```rust
// Async creation (fetches nonce from chain)
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .build()
    .await?;

// Sync creation with known nonce (faster startup)
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .build_with_nonce(0)?;

// With multiple RPC endpoints for broadcasting
let wallet = FastWalletBuilder::new(private_key, primary_rpc)
    .chain_id(1)
    .broadcast_rpcs(vec![
        "https://rpc.flashbots.net".to_string(),
        "https://eth.llamarpc.com".to_string(),
    ])
    .build()
    .await?;
```

### Warmup API

Call `warmup()` during initialization to pre-establish connections:

```rust
// Warmup syncs nonce, fetches gas prices, and warms HTTP connection pool
wallet.warmup().await?;

// Check warmup status
if wallet.is_warmed_up() {
    println!("Ready for high-speed transactions");
}
```

### Preheating API (Lowest Latency)

Use preheating when you detect a potential opportunity but are still calculating parameters:

```rust
// Preheat: reserves nonce and optionally fetches gas prices
let ctx = wallet.preheat(true).await?;  // true = also fetch gas

// ... calculate transaction parameters ...

// Sign using preheated context (fastest path)
let tx = wallet.sign_with_preheat(&ctx,
    TransactionRequest::new()
        .to(target_address)
        .data(calldata)
        .gas_limit(estimated_gas)
)?;

// Send the transaction
let hash = wallet.send_signed(&tx).await?;
```

If you decide not to send:

```rust
// Cancel to release the reserved nonce
wallet.cancel_preheat(ctx);
```

Batch preheating for multiple transactions:

```rust
// Reserve 5 nonces at once
let contexts = wallet.preheat_batch(5, true).await?;

for ctx in contexts {
    let tx = wallet.sign_with_preheat(&ctx, request.clone())?;
    // ...
}
```

### Transaction Signing

```rust
// Auto-nonce signing (simplest API)
let tx = wallet.sign(
    TransactionRequest::new()
        .to(address)
        .value(value)
        .gas_limit(21000)
        .gas_price(gas_price)
)?;

// Legacy transaction
let tx = wallet.sign_legacy(to, value, data, gas_limit, gas_price)?;

// EIP-1559 transaction
let tx = wallet.sign_eip1559(
    to,
    value,
    data,
    gas_limit,
    max_fee_per_gas,
    max_priority_fee_per_gas
)?;
```

### Transaction Sending

```rust
// Sign and send in one call
let hash = wallet.send(request).await?;

// Send pre-signed transaction
let tx = wallet.sign(request)?;
let hash = wallet.send_signed(&tx).await?;

// ETH transfer (convenience method)
let hash = wallet.send_eth(to, value, Some(gas_price)).await?;

// Contract call (convenience method)
let hash = wallet.send_contract_call(contract, calldata, gas_limit, None).await?;

// Batch send (parallel, fastest for multiple txs)
let results = wallet.send_batch(vec![req1, req2, req3]).await;
```

### Gas Price Management

```rust
// Get cached gas price (or fetch if expired)
let gas_price = wallet.get_gas_price().await?;

// Get priority fee (EIP-1559)
let priority_fee = wallet.get_priority_fee().await?;

// Force refresh
let (gas_price, priority_fee) = wallet.refresh_gas_prices().await?;
```

### Nonce Management

Nonce is managed automatically, but you can access it if needed:

```rust
// Current nonce (without incrementing)
let nonce = wallet.current_nonce();

// Pending transaction count
let pending = wallet.pending_count();

// Sync from chain (after transaction failures)
wallet.sync_nonce().await?;
```

## Transaction Types

### TransactionRequest Builder

```rust
let request = TransactionRequest::new()
    .to(address)                              // Recipient
    .value(U256::from(1_ether))               // Value in wei
    .data(Bytes::from(calldata))              // Contract calldata
    .gas_limit(100_000)                       // Gas limit
    .gas_price(U256::from(20_gwei))           // Legacy gas price
    .max_fee_per_gas(U256::from(50_gwei))     // EIP-1559 max fee
    .max_priority_fee_per_gas(U256::from(2_gwei)); // EIP-1559 priority fee
```

### Transaction Types

- **Legacy**: Uses `gas_price`
- **EIP-1559**: Uses `max_fee_per_gas` and `max_priority_fee_per_gas`
- **EIP-2930**: Legacy with access list

The library automatically selects the transaction type based on which gas parameters you provide.

## Multi-RPC Broadcasting

For MEV and liquidation bots, sending to multiple RPCs can improve inclusion speed:

```rust
let wallet = FastWalletBuilder::new(private_key, primary_rpc)
    .chain_id(1)
    .broadcast_rpcs(vec![
        "https://rpc.flashbots.net".to_string(),
        "https://eth.llamarpc.com".to_string(),
        "https://rpc.ankr.com/eth".to_string(),
    ])
    .build()
    .await?;

// Transactions will be broadcast to all RPCs in parallel
```

### Advanced Broadcasting

```rust
use fast_wallet::{BroadcasterBuilder, BroadcastStrategy, RpcEndpoint};

let broadcaster = BroadcasterBuilder::new()
    .add_endpoint(RpcEndpoint::flashbots("your_auth_key"))  // Priority 10
    .add_endpoint(RpcEndpoint::private("https://relay.example.com", "key"))  // Priority 50
    .add_public("https://eth.llamarpc.com")  // Priority 100
    .strategy(BroadcastStrategy::RaceAll)  // First success wins
    .build()?;
```

Broadcast strategies:
- `RaceAll`: Return on first success (fastest)
- `BroadcastAll`: Wait for all responses
- `PriorityOrdered`: Try endpoints in priority order
- `PrivateFirst`: Try private relays first, then public

## Performance Optimization

### For Liquidation Bots

```rust
// 1. Create wallet with known nonce (skip initial RPC call)
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .build_with_nonce(known_nonce)?;

// 2. Warmup during idle time
wallet.warmup().await?;

// 3. When opportunity detected, preheat immediately
let ctx = wallet.preheat(true).await?;

// 4. Calculate parameters (tx is preheated in parallel)
let params = calculate_liquidation_params().await;

// 5. Sign with preheated context (fastest)
let tx = wallet.sign_with_preheat(&ctx,
    TransactionRequest::new()
        .to(params.target)
        .data(params.calldata)
        .gas_limit(params.gas)
)?;

// 6. Send
let hash = wallet.send_signed(&tx).await?;
```

### Signing Performance

The library achieves ~20,000 transactions/second for signing:

```rust
// Synchronous signing is CPU-bound and very fast
for _ in 0..1000 {
    let tx = wallet.sign(request.clone())?;  // ~50µs each
}
```

### Nonce Acquisition

Lock-free atomic operations achieve ~13ns per nonce:

```rust
// Nonces are acquired atomically without locks
let nonce = wallet.nonce_manager.get_nonce();  // ~13ns
```

## Testing with Foundry Anvil

Run realistic benchmarks using Foundry's Anvil:

```bash
# Start Anvil
anvil --block-time 1

# Run benchmark
cargo run --example anvil_benchmark --release
```

For mainnet fork testing:

```bash
anvil --fork-url https://eth.llamarpc.com --block-time 1
```

## Configuration

```rust
let wallet = FastWalletBuilder::new(private_key, rpc_url)
    .chain_id(1)
    .max_pending_txs(100)              // Max concurrent pending txs
    .use_eip1559(true)                  // Default to EIP-1559
    .confirmation_timeout(Duration::from_secs(120))
    .gas_cache_duration(Duration::from_millis(500))
    .build()
    .await?;
```

## Error Handling

```rust
use fast_wallet::{WalletError, WalletResult};

match wallet.send(request).await {
    Ok(hash) => println!("Sent: {:?}", hash),
    Err(WalletError::NonceError(msg)) => {
        // Nonce issue - sync and retry
        wallet.sync_nonce().await?;
    }
    Err(WalletError::RpcError(msg)) => {
        // RPC issue
    }
    Err(e) => {
        // Other errors
    }
}
```

## Types Re-exported

```rust
use fast_wallet::types::{Address, Bytes, B256, U256};
```

## License

MIT

# Release v0.1.1

High-performance EVM wallet for liquidation bots and MEV applications.

## What's New in v0.1.1

**Performance Optimizations (+18% throughput)**
- Use `alloy_primitives::keccak256` for optimized hashing (-18% latency)
- Reduce B256 copies in signature extraction (-15% latency)
- Thread-local buffer reuse for transaction encoding (-21% latency)
- Add `#[inline]` attributes to hot path functions
- Use `Ordering::AcqRel/Acquire` instead of `SeqCst` for atomics (-16% latency)

**Benchmarks**
- Signing throughput: ~20,400 → **~24,000 tx/sec** (+17.6%)
- Nonce acquisition: 13.3 ns → **11.2 ns** (-15.8%)
- EIP-1559 encoding: 61.6 ns → **49.0 ns** (-20.5%)

## Download

### Binary (Linux x86_64)
```
releases/fast-wallet-v0.1.1-linux-x86_64
```

### Source Code
```
releases/fast-wallet-v0.1.1-source.tar.gz
```

## SHA256 Checksums
```
5cd64de462316e5707631f5f98379ac221915d472d455194aae26eb1aa47d4c2  fast-wallet-v0.1.1-linux-x86_64
c1d4069f408dfd0afccda243bf2d6ee5edf33806795d2e204523330aa5f2d091  fast-wallet-v0.1.1-source.tar.gz
```

## Features

- **Extreme Speed**: ~24,000 tx/sec signing, ~11ns nonce acquisition
- **Auto Nonce Management**: No need to track nonces manually
- **Preheating API**: Pre-allocate resources for lowest latency
- **Multi-RPC Broadcasting**: Send to multiple endpoints for faster inclusion
- **EIP-1559 & Legacy**: Full transaction type support
- **Gas Price Caching**: Reduces RPC calls

## Installation

### From Binary
```bash
# Download and make executable
chmod +x fast-wallet-v0.1.1-linux-x86_64
./fast-wallet-v0.1.1-linux-x86_64
```

### From Source
```bash
# Extract and build
tar -xzf fast-wallet-v0.1.1-source.tar.gz
cd fast-wallet
cargo build --release
```

### As Library
```toml
[dependencies]
fast-wallet = { git = "https://github.com/sunoj/fast-wallet", tag = "v0.1.1" }
```

## Quick Start

```rust
use fast_wallet::{FastWalletBuilder, TransactionRequest};
use alloy_primitives::{Address, U256};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wallet = FastWalletBuilder::new(
        "0xYOUR_PRIVATE_KEY",
        "https://eth.llamarpc.com"
    )
    .chain_id(1)
    .build()
    .await?;

    // Warmup
    wallet.warmup().await?;

    // Send transaction (nonce auto-managed)
    let hash = wallet.send(
        TransactionRequest::new()
            .to("0x...".parse()?)
            .value(U256::from(1_000_000_000_000_000_000u64))
            .gas_limit(21000)
            .gas_price(U256::from(20_000_000_000u64))
    ).await?;

    Ok(())
}
```

## Changelog

### v0.1.1 (Performance Release)
- Use alloy_primitives::keccak256 for optimized hashing
- Reduce B256 copies in signature extraction
- Thread-local buffer reuse for transaction encoding
- Add #[inline] attributes to hot path functions
- Optimize atomic ordering (AcqRel instead of SeqCst)
- Add EIP-1559 signing benchmark
- Add BENCHMARKS.md for performance tracking

### v0.1.0 (Initial Release)
- Core wallet implementation with k256 ECDSA signing
- Lock-free atomic nonce management
- Preheating API for transaction preparation
- Multi-RPC broadcasting support
- Legacy, EIP-2930, EIP-1559 transaction encoding
- Gas price caching
- Comprehensive test suite

---

## Previous Releases

### v0.1.0
```
releases/fast-wallet-v0.1.0-linux-x86_64
releases/fast-wallet-v0.1.0-source.tar.gz
```

SHA256:
```
8b03f477c87f263cccfa06b4cd3aa4ec078528f423070e7a2333ea6b9d3d97d5  fast-wallet-v0.1.0-linux-x86_64
b1c0980c505d7dde2dff1fe21be101610c18fc28e0adc21dbc2fabe7640618a1  fast-wallet-v0.1.0-source.tar.gz
```

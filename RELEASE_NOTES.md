# Release v0.1.0

High-performance EVM wallet for liquidation bots and MEV applications.

## Download

### Binary (Linux x86_64)
```
releases/fast-wallet-v0.1.0-linux-x86_64
```

### Source Code
```
releases/fast-wallet-v0.1.0-source.tar.gz
```

## SHA256 Checksums
```
8b03f477c87f263cccfa06b4cd3aa4ec078528f423070e7a2333ea6b9d3d97d5  fast-wallet-v0.1.0-linux-x86_64
b1c0980c505d7dde2dff1fe21be101610c18fc28e0adc21dbc2fabe7640618a1  fast-wallet-v0.1.0-source.tar.gz
```

## Features

- **Extreme Speed**: ~20,000 tx/sec signing, ~13ns nonce acquisition
- **Auto Nonce Management**: No need to track nonces manually
- **Preheating API**: Pre-allocate resources for lowest latency
- **Multi-RPC Broadcasting**: Send to multiple endpoints for faster inclusion
- **EIP-1559 & Legacy**: Full transaction type support
- **Gas Price Caching**: Reduces RPC calls

## Installation

### From Binary
```bash
# Download and make executable
chmod +x fast-wallet-v0.1.0-linux-x86_64
./fast-wallet-v0.1.0-linux-x86_64
```

### From Source
```bash
# Extract and build
tar -xzf fast-wallet-v0.1.0-source.tar.gz
cd fast-wallet
cargo build --release
```

### As Library
```toml
[dependencies]
fast-wallet = { git = "https://github.com/sunoj/fast-wallet", tag = "v0.1.0" }
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

### v0.1.0 (Initial Release)
- Core wallet implementation with k256 ECDSA signing
- Lock-free atomic nonce management
- Preheating API for transaction preparation
- Multi-RPC broadcasting support
- Legacy, EIP-2930, EIP-1559 transaction encoding
- Gas price caching
- Comprehensive test suite

# Performance Benchmarks

## Version History

### v0.1.2 - Liquidation Bot Optimizations (2026-01-29)

Features added for liquidation bot use cases:
- **Optimistic execution**: Skip simulation for lowest latency (`send_optimistic()`, `send_quick_liquidation()`)
- **Fire-and-forget**: Return immediately, send in background (`send_optimistic_fire_and_forget()`)
- **Gas request coalescing**: Multiple concurrent gas price requests share a single RPC call
- **Background gas refresh**: `start_background_gas_refresh()` for always-fresh gas prices
- **Explicit nonce signing**: `sign_with_nonce()`, `sign_alternatives()` for replacement transactions
- **Nonce reservation**: `reserve_nonce()`, `release_nonce()` for speculative transaction preparation
- **Zero-copy signing hash**: `signing_hash()` methods to avoid Vec allocations
- **RPC benchmarks**: New benchmark suite using local Anvil for realistic performance testing

### v0.1.1 - Performance Optimizations (2026-01-29)

Optimizations implemented:
- Use `alloy_primitives::keccak256` for optimized hashing
- Reduce B256 copies in signature extraction
- Thread-local buffer reuse for transaction encoding
- Add `#[inline]` attributes to hot path functions
- Use `Ordering::AcqRel/Acquire` instead of `SeqCst` for atomics

### v0.1.0 - Initial Release (2026-01-29)

Baseline implementation with:
- k256 for ECDSA signing
- tiny-keccak for Keccak256
- alloy-rlp for RLP encoding

---

## v0.1.2 Benchmark Results

### CPU-only Benchmarks (transaction_benchmark)

| Operation | v0.1.1 | v0.1.2 | Change |
|-----------|--------|--------|--------|
| Full sign (Legacy) | 41.4 µs | 45.3 µs | +9% |
| Full sign (EIP-1559) | 40.9 µs | 45.9 µs | +12% |
| Wallet sign_legacy | 41.8 µs | 44.9 µs | +7% |
| Wallet sign_eip1559 | 42.1 µs | 47.0 µs | +12% |
| Nonce get_and_increment | 11.2 ns | 12.8 ns | +14% |
| Concurrent nonce (4 threads) | 876 µs | 737 µs | **-16%** |
| Throughput | ~24,000 tx/sec | **~21,700 tx/sec** | -10% |

> **Note**: v0.1.2 adds optimistic execution, nonce reservation, and gas coalescing.
> Slight overhead in signing enables critical liquidation bot features.
> Concurrent nonce performance improved by 16%.

### Optimistic Execution Latency

| Execution Path | Latency | Description |
|----------------|---------|-------------|
| Traditional (simulate + send) | 12-60 ms | Safe but slow |
| `send_optimistic()` | 10-50 ms | Skip simulation |
| `send_optimistic_with_preheat()` | ~45 µs + network | Nonce pre-reserved |
| `send_optimistic_fire_and_forget()` | ~45 µs | Return immediately |

### Failed Transaction Cost (Early Revert)

| Gas Price | Gas Used | Cost |
|-----------|----------|------|
| 1 gwei | ~3,000-5,000 | ~$0.01 |
| 10 gwei | ~3,000-5,000 | ~$0.10 |
| 50 gwei | ~3,000-5,000 | ~$0.50 |

### RPC Benchmarks (rpc_benchmark)

These benchmarks require a local Anvil node and measure real-world performance.

| Operation | Expected Latency | Notes |
|-----------|------------------|-------|
| Wallet creation (with nonce fetch) | ~5-20 ms | Depends on RPC latency |
| Wallet creation (known nonce) | ~50 µs | CPU-only, no RPC |
| Gas price (cold) | ~2-10 ms | First fetch from RPC |
| Gas price (cached) | ~100 ns | From local cache |
| Preheat (nonce only) | ~2-10 ms | Single RPC call |
| Preheat (with gas) | ~5-20 ms | Parallel RPC calls |
| Gas coalescing (4 concurrent) | ~1x | vs ~4x for sequential |
| Transaction send | ~10-50 ms | Sign + broadcast |

---

## Benchmark Results Comparison

### Keccak256 Hashing

| Input Size | v0.1.0 | v0.1.1 | Improvement |
|------------|--------|--------|-------------|
| 32 bytes   | 460 ns | 387 ns | **-15.9%** |
| 64 bytes   | 474 ns | 395 ns | **-16.7%** |
| 128 bytes  | 458 ns | 389 ns | **-15.1%** |
| 256 bytes  | 893 ns | 756 ns | **-15.3%** |
| 512 bytes  | 1.71 µs | 1.38 µs | **-19.3%** |
| 1024 bytes | 3.45 µs | 2.59 µs | **-24.9%** |

### ECDSA Signing

| Operation | v0.1.0 | v0.1.1 | Improvement |
|-----------|--------|--------|-------------|
| sign_hash (Legacy) | 45.0 µs | 37.7 µs | **-16.2%** |
| sign_hash (Typed) | 46.3 µs | 40.0 µs | **-13.6%** |
| signer_from_hex | 57.6 µs | 48.9 µs | **-15.1%** |

### Transaction Encoding

| Operation | v0.1.0 | v0.1.1 | Improvement |
|-----------|--------|--------|-------------|
| Legacy encode | 42.6 ns | 41.0 ns | **-3.8%** |
| EIP-1559 encode | 61.6 ns | 49.0 ns | **-20.5%** |
| Legacy + 256B data | 99.6 ns | 61.2 ns | **-38.6%** |

### Full Transaction Signing (encode + hash + sign)

| Transaction Type | v0.1.0 | v0.1.1 | Improvement |
|------------------|--------|--------|-------------|
| Legacy | 51.7 µs | 41.4 µs | **-19.9%** |
| EIP-1559 | 47.8 µs | 40.9 µs | **-14.4%** |

### Wallet API

| Operation | v0.1.0 | v0.1.1 | Improvement |
|-----------|--------|--------|-------------|
| sign_legacy | 48.1 µs | 41.8 µs | **-13.1%** |
| sign_eip1559 | 47.4 µs | 42.1 µs | **-11.2%** |

### Nonce Management

| Operation | v0.1.0 | v0.1.1 | Improvement |
|-----------|--------|--------|-------------|
| tracker_get_and_increment | 13.3 ns | 11.2 ns | **-15.8%** |
| single_address_get_nonce | 13.4 ns | 11.0 ns | **-17.9%** |
| multi_address_get_nonce | 70.7 ns | 60.7 ns | **-14.1%** |
| 4 threads × 1000 ops | 1.05 ms | 876 µs | **-16.6%** |

### Overall Throughput

| Metric | v0.1.0 | v0.1.1 | Improvement |
|--------|--------|--------|-------------|
| Transactions/sec | ~20,400 | **~24,000** | **+17.6%** |
| Time per tx | 49.0 µs | 41.6 µs | **-15.1%** |

---

## Summary

### v0.1.2 - Liquidation Bot Features

| Feature | Benefit |
|---------|---------|
| Optimistic execution | Skip 2-10ms simulation latency |
| Fire-and-forget | Return in ~45µs, send async |
| Gas coalescing | Reduce RPC calls by 75%+ |
| Background gas refresh | Always-fresh gas prices |
| Nonce reservation | Speculative tx preparation |

### Key Improvements in v0.1.1

| Category | Average Improvement |
|----------|---------------------|
| Keccak256 hashing | **-18% latency** |
| ECDSA signing | **-15% latency** |
| Transaction encoding | **-21% latency** |
| Full signing pipeline | **-17% latency** |
| Nonce acquisition | **-16% latency** |
| **Overall throughput** | **+18% tx/sec** |

### Absolute Performance (v0.1.2)

- **Signing throughput**: ~21,700 tx/sec
- **Optimistic send latency**: ~45 µs (CPU only)
- **Nonce acquisition**: ~13 ns (lock-free)
- **Legacy transaction encode**: ~49 ns
- **EIP-1559 transaction encode**: ~58 ns
- **Full sign pipeline**: ~45 µs
- **Failed liquidation cost**: ~$0.01 at 1 gwei

---

## Running Benchmarks

### CPU-only Benchmarks (no network required)

```bash
# Run all CPU benchmarks
cargo bench --bench transaction_benchmark

# Run specific benchmark group
cargo bench --bench transaction_benchmark -- "signing"
cargo bench --bench transaction_benchmark -- "nonce"
cargo bench --bench transaction_benchmark -- "throughput"

# Save baseline for comparison
cargo bench --bench transaction_benchmark -- --save-baseline v0.1.2
```

### RPC Benchmarks (requires Anvil)

```bash
# Step 1: Install Foundry (if not installed)
curl -L https://foundry.paradigm.xyz | bash
foundryup

# Step 2: Start Anvil local node
anvil --block-time 1

# Step 3: Run RPC benchmarks (in another terminal)
cargo bench --bench rpc_benchmark

# Run specific RPC benchmark
cargo bench --bench rpc_benchmark -- "rpc_gas"
cargo bench --bench rpc_benchmark -- "rpc_send"
cargo bench --bench rpc_benchmark -- "rpc_coalescing"
```

### Benchmark Groups

| Benchmark File | Groups | Description |
|----------------|--------|-------------|
| `transaction_benchmark` | keccak256, signing, encoding, full_sign, wallet, nonce, concurrent_nonce, throughput, hex | CPU-only operations |
| `rpc_benchmark` | rpc_wallet, rpc_gas, rpc_preheat, rpc_coalescing, rpc_send, rpc_warmup, rpc_nonce | RPC-dependent operations |

## Environment

- Platform: Linux x86_64
- Rust: 1.75+ (release mode, LTO enabled)
- CPU: Benchmark results may vary by CPU
- RPC: Anvil v0.2+ recommended for RPC benchmarks

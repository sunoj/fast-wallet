# Performance Benchmarks

## Version History

### v0.1.7 - Concurrency & Correctness Audit Fixes (2026-02-06)

**Critical Bug Fixes:**
- **Gas coalescing race condition**: Replaced broadcast-channel pattern with `tokio::sync::Mutex` double-checked locking to eliminate TOCTOU race
- **Nonce leak on cancel_preheat**: Added CAS-based nonce reclamation in `NonceTracker::fail()` — reclaims the nonce when possible, flags resync otherwise
- **Semaphore deadlock**: Added `tokio::time::timeout` to `pending_semaphore.acquire()` (configurable via `pending_acquire_timeout`, default 100ms)
- **Blocking lock in async context**: Replaced `parking_lot::Mutex` gas coalescer with `tokio::sync::Mutex` to avoid blocking the tokio runtime
- **RwLock guard lifetime**: Fixed gas cache reads to drop `RwLock` guard before awaiting, preventing potential deadlocks

**Safety Improvements:**
- Added `max_gas_cache_age` (default 30s) as hard cap on gas price staleness
- Added `debug_assert!` for ECDSA recovery_id validation in both k256 and secp256k1-ffi backends
- `WalletConfig` gains `max_gas_cache_age` and `pending_acquire_timeout` fields

**Dependency Cleanup:**
- Removed unused: `sha3`, `rand`, `anyhow`, `bytes`
- Removed unused dev-deps: `wiremock`, `tokio-test`, `instant`
- Trimmed redundant tokio features (`full` already includes `sync`, `time`, `rt-multi-thread`)

### v0.1.6 - secp256k1-ffi Backend (50% Faster Signing) (2026-01-30)

**New secp256k1-ffi Feature:**
- Optional Bitcoin Core's C library backend for maximum signing performance
- Uses precomputed multiplication tables from libsecp256k1
- **50% faster** than pure Rust k256 implementation

**Benchmark Results:**
| Backend | Throughput | Signing Latency | Improvement |
|---------|------------|-----------------|-------------|
| k256 (default) | ~21,000 tx/sec | ~46 µs | - |
| secp256k1-ffi | **~31,500 tx/sec** | **~30 µs** | **+50%** |

Enable with: `features = ["secp256k1-ffi"]`

### v0.1.5 - Built-in Public RPC Presets & Connection Warmup (2026-01-29)

**Built-in RPC Endpoints:**
- **20+ public RPC endpoints**: LlamaNodes, PublicNode, 1RPC, DRPC, Ankr, Cloudflare, Blast, BlockPI, OMNIA, Tenderly, Merkle, SecureRPC, Builder0x69
- **MEV protection**: Flashbots Protect/Fast, MEV Blocker/Fast
- **Regional endpoints**: bloXroute Virginia, UK, Singapore
- **Convenience methods**: `add_default_public_rpcs()`, `add_all_public_rpcs()`, `add_mev_protection()`, `add_regional_rpcs()`, `add_liquidation_preset()`, `add_minimal_rpcs()`

**HTTP Connection Warmup:**
- `warmup()` / `warmup_connections()` - Pre-establish TCP/TLS connections
- `preheat_full()` - Full preheat (connections + nonce + gas)
- **Latency savings**: 5-20ms per endpoint on first request

**Benchmark Results (k256):**
| Metric | Value |
|--------|-------|
| Throughput | ~19,500-20,300 tx/sec |
| Nonce acquisition | ~13.2 ns |
| Full sign (Legacy) | ~50 µs |
| Connection warmup savings | 5-20 ms |

### v0.1.4 - Blink Labs RPC Integration (2026-01-29)

- **Blink Labs RPC**: Built-in support for low-latency MEV-optimized RPC
- New APIs: `RpcEndpoint::blink()`, `FastWalletBuilder::with_blink()`
- Multi-chain support: eth, arb, base, opt, polygon
- Performance: ~21,000-24,000 tx/sec (environment-dependent)

### v0.1.3 - Code Cleanup & Performance Recovery (2026-01-29)

- Removed unused imports, fields, and methods
- Removed redundant documentation files
- Zero compiler warnings
- **Performance recovered**: ~24,100 tx/sec (was ~21,700 in v0.1.2)

### v0.1.2 - Liquidation Bot Optimizations (2026-01-29)

Features added for liquidation bot use cases:
- **Optimistic execution**: Skip simulation for lowest latency (`send_optimistic()`, `send_quick_liquidation()`)
- **Fire-and-forget**: Return immediately, send in background (`send_optimistic_fire_and_forget()`)
- **Gas request coalescing**: Multiple concurrent gas price requests share a single RPC call
- **Background gas refresh**: `start_background_gas_refresh()` for always-fresh gas prices
- **Explicit nonce signing**: `sign_with_nonce()`, `sign_alternatives()` for replacement transactions
- **Nonce reservation**: `reserve_nonce()`, `release_nonce()` for speculative transaction preparation
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

## v0.1.3 Benchmark Results

### CPU-only Benchmarks (transaction_benchmark)

| Operation | v0.1.2 | v0.1.3 | Change |
|-----------|--------|--------|--------|
| Full sign (Legacy) | 45.3 µs | 40.0 µs | **-12%** |
| Full sign (EIP-1559) | 45.9 µs | 40.4 µs | **-12%** |
| Wallet sign_legacy | 44.9 µs | 41.2 µs | **-8%** |
| Wallet sign_eip1559 | 47.0 µs | 41.0 µs | **-13%** |
| Nonce get_and_increment | 12.8 ns | 11.1 ns | **-13%** |
| Throughput | ~21,700 tx/sec | **~24,100 tx/sec** | **+11%** |

> **Note**: v0.1.3 removes dead code, recovering performance lost in v0.1.2.

### Optimistic Execution Latency

| Execution Path | Latency | Description |
|----------------|---------|-------------|
| Traditional (simulate + send) | 12-60 ms | Safe but slow |
| `send_optimistic()` | 10-50 ms | Skip simulation |
| `send_optimistic_with_preheat()` | ~40 µs + network | Nonce pre-reserved |
| `send_optimistic_fire_and_forget()` | ~40 µs | Return immediately |

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

### Connection Warmup Impact

| Scenario | Latency | Description |
|----------|---------|-------------|
| Cold first request | 5-20 ms | TCP/TLS handshake required |
| Warm cached request | 0.5-2 ms | Connection reused from pool |
| Cold liquidation flow | 15-60 ms | New connection + send |
| Warm liquidation flow | 10-50 ms | Existing connection |
| Preheated liquidation | 5-20 ms | All resources ready |

**Savings from warmup:** 5-20ms per endpoint (TCP + TLS handshake avoided)

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

### Absolute Performance (v0.1.3)

- **Signing throughput**: ~24,100 tx/sec
- **Optimistic send latency**: ~40 µs (CPU only)
- **Nonce acquisition**: ~11 ns (lock-free)
- **Legacy transaction encode**: ~38 ns
- **EIP-1559 transaction encode**: ~47 ns
- **Full sign pipeline**: ~40 µs
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
| `rpc_benchmark` | rpc_wallet, rpc_gas, rpc_preheat, rpc_coalescing, rpc_send, rpc_warmup, rpc_nonce, connection_warmup, liquidation_flow, warmup_connections | RPC-dependent operations |

### Connection Warmup Benchmarks (requires Anvil)

```bash
# Compare cold vs warm connection latency
cargo bench --bench rpc_benchmark -- "connection_warmup"

# Test full liquidation flow with different warmup strategies
cargo bench --bench rpc_benchmark -- "liquidation_flow"

# Measure warmup_connections and preheat_full timing
cargo bench --bench rpc_benchmark -- "warmup_connections"
```

## Environment

- Platform: Linux x86_64
- Rust: 1.75+ (release mode, LTO enabled)
- CPU: Benchmark results may vary by CPU
- RPC: Anvil v0.2+ recommended for RPC benchmarks

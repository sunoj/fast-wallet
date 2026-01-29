# Performance Benchmarks

## Version History

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

### Key Improvements in v0.1.1

| Category | Average Improvement |
|----------|---------------------|
| Keccak256 hashing | **-18% latency** |
| ECDSA signing | **-15% latency** |
| Transaction encoding | **-21% latency** |
| Full signing pipeline | **-17% latency** |
| Nonce acquisition | **-16% latency** |
| **Overall throughput** | **+18% tx/sec** |

### Absolute Performance (v0.1.1)

- **Signing throughput**: ~24,000 tx/sec
- **Nonce acquisition**: ~11 ns (lock-free)
- **Legacy transaction encode**: ~41 ns
- **EIP-1559 transaction encode**: ~49 ns
- **Full sign pipeline**: ~41 µs

---

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- "signing"

# Generate HTML report
cargo bench -- --save-baseline v0.1.1
```

## Environment

- Platform: Linux x86_64
- Rust: 1.75+ (release mode, LTO enabled)
- CPU: Benchmark results may vary by CPU

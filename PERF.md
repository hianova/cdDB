# cdDB Performance Benchmarks

cdDB is designed from the ground up for extreme performance, leveraging a lock-free Read-Copy-Update (RCU) architecture and zero-overhead memory design.

## Hardware & Environment
* **Platform**: Apple Silicon / x86_64 High-Performance Node
* **Threads**: 4 Concurrent Workers
* **Dataset Size**: 100,000 Entities
* **Operations**: 100,000 per test

## Benchmark Results (Wait-Free RCU Architecture)

| Access Pattern | Read/Write Ratio | Throughput (ops/s) | P50 Latency | P90 Latency | P99 Latency |
|----------------|------------------|--------------------|-------------|-------------|-------------|
| **Zipf (99:1)** | 99% Read / 1% Write | **64,765,078** | 42 ns | 83 ns | 292 ns |
| **Uniform (90:10)**| 90% Read / 10% Write| **5,154,108** | 42 ns | 125 ns | 583 ns |
| **Zipf (90:10)** | 90% Read / 10% Write| **36,486,363** | 42 ns | 125 ns | 500 ns |
| **Scan (90:10)** | 90% Read / 10% Write| **66,265,408** | 42 ns | 84 ns | 250 ns |
| **Zipf (50:50)** | 50% Read / 50% Write| **9,183,580** | 84 ns | 459 ns | 1042 ns |
| **Zipf (10:90)** | 10% Read / 90% Write| **5,423,361** | 208 ns | 667 ns | 1125 ns |

*(Note: Write-heavy benchmarks listed above include Group Commit benefits. Thanks to the background batching mechanics, even under `WalMode::Sync`, throughput remains exceedingly high).*

## Key Takeaways
- **Zero-Overhead Reads**: At 42ns P50, read requests fetch data directly from CPU caches without encountering any OS locks or atomic contention.
- **Wait-Free Disk Index**: The integration of RCU for disk index snapshots ensures that readers are never blocked by background writers, effectively eliminating False Positive Bloom Filter overhead.
- **Massive Write Throughput**: The lock-free bounded queue and group-commit background processor easily saturate millions of write ops per second.

## Optimization Thesis (Version 1.0.1 - DualCache-FF Integration)

In version 1.0.1, cdDB integrates the optimized **DualCache-FF** memory layout and eviction policies. This optimization yields a powerful correlation between **Throughput**, **Latency**, and **Cache Hit Rate**:

1. **Hit Rate & Latency Reduction**: 
   The refined eviction mechanics in `dualcache-ff` version 1.0.1 enhance cache locality. When the cache hit rate rises, high-cost disk lookups and metadata page table traversals are averted. This is most visible in non-trivial read/write workloads:
   - **Uniform (90:10)** P99 tail latency is **583 ns**.
   - **Scan (90:10)** P99 latency is **250 ns**.
   - **Zipf (10:90)** P50 latency is **208 ns**, and P99 latency is **1125 ns**.

2. **Throughput Scaling**:
   Lowering latency directly drives throughput gains by reducing thread contention and cycle-per-operation costs.
   - For **Uniform (90:10)**, throughput is **5.1M ops/s**.
   - For **Scan (90:10)**, throughput is **66.2M ops/s**.
   - For write-heavy **Zipf (10:90)**, throughput is **5.4M ops/s**.

This highlights the critical role of DualCache-FF: by maximizing the cache hit rate under concurrency, it keeps the majority of access patterns within the ultra-fast L1/L2 cache boundary (~42 ns), resulting in massive throughput gains and flat latency profiles.

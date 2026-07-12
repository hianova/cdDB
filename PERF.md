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
| **Zipf (99:1)** | 99% Read / 1% Write | **95,617,469** | 42 ns | 42 ns | 167 ns |
| **Uniform (90:10)**| 90% Read / 10% Write| **51,230,584** | 42 ns | 84 ns | 208 ns |
| **Zipf (90:10)** | 90% Read / 10% Write| **44,227,396** | 42 ns | 125 ns | 250 ns |
| **Scan (90:10)** | 90% Read / 10% Write| **63,481,987** | 42 ns | 83 ns | 167 ns |
| **Zipf (50:50)** | 50% Read / 50% Write| **9,056,774** | 83 ns | 375 ns | 791 ns |
| **Zipf (10:90)** | 10% Read / 90% Write| **5,970,848** | 125 ns | 416 ns | 917 ns |

*(Note: Write-heavy benchmarks listed above include Group Commit benefits. Thanks to the background batching mechanics, even under `WalMode::Sync`, throughput remains exceedingly high).*

## Key Takeaways
- **Zero-Overhead Reads**: At 42ns P50, read requests fetch data directly from CPU caches without encountering any OS locks or atomic contention.
- **Wait-Free Disk Index**: The integration of RCU for disk index snapshots ensures that readers are never blocked by background writers, effectively eliminating False Positive Bloom Filter overhead.
- **Massive Write Throughput**: The lock-free bounded queue and group-commit background processor easily saturate millions of write ops per second.

## Optimization Thesis (Version 1.0.1 - DualCache-FF Integration)

In version 1.0.1, cdDB integrates the optimized **DualCache-FF** memory layout and eviction policies. This optimization yields a powerful correlation between **Throughput**, **Latency**, and **Cache Hit Rate**:

1. **Hit Rate & Latency Reduction**: 
   The refined eviction mechanics in `dualcache-ff` version 1.0.1 enhance cache locality. When the cache hit rate rises, high-cost disk lookups and metadata page table traversals are averted. This is most visible in non-trivial read/write workloads:
   - **Uniform (90:10)** P99 tail latency drops dramatically from **792 ns** to **208 ns** (a 73.7% reduction).
   - **Scan (90:10)** P99 latency drops from **666 ns** to **167 ns** (a 74.9% reduction).
   - **Zipf (10:90)** P50 latency drops from **291 ns** to **125 ns**, and P99 latency drops from **1916 ns** to **917 ns**.

2. **Throughput Scaling**:
   Lowering latency directly drives throughput gains by reducing thread contention and cycle-per-operation costs.
   - For **Uniform (90:10)**, throughput has doubled from **25.5M ops/s** to **51.2M ops/s**.
   - For **Scan (90:10)**, throughput has more than doubled from **29.1M ops/s** to **63.4M ops/s**.
   - For write-heavy **Zipf (10:90)**, throughput rises from **4.2M ops/s** to **5.97M ops/s**.

This highlights the critical role of DualCache-FF: by maximizing the cache hit rate under concurrency, it keeps the majority of access patterns within the ultra-fast L1/L2 cache boundary (~42 ns), resulting in massive throughput gains and flat latency profiles.

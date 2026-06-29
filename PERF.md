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
| **Zipf (99:1)** | 99% Read / 1% Write | **99,941,734** | 42 ns | 42 ns | 209 ns |
| **Uniform (90:10)**| 90% Read / 10% Write| **33,370,876** | 42 ns | 125 ns | 625 ns |
| **Zipf (90:10)** | 90% Read / 10% Write| **47,129,973** | 42 ns | 84 ns | 333 ns |
| **Scan (90:10)** | 90% Read / 10% Write| **26,118,471** | 42 ns | 84 ns | 666 ns |
| **Zipf (50:50)** | 50% Read / 50% Write| **10,724,148** | 83 ns | 208 ns | 708 ns |
| **Zipf (10:90)** | 10% Read / 90% Write| **6,153,878** | 125 ns | 416 ns | 834 ns |

*(Note: Write-heavy benchmarks listed above include Group Commit benefits. Thanks to the background batching mechanics, even under `WalMode::Sync`, throughput remains exceedingly high).*

## Key Takeaways
- **Zero-Overhead Reads**: At 42ns P50, read requests fetch data directly from CPU caches without encountering any OS locks or atomic contention.
- **Wait-Free Disk Index**: The integration of RCU for disk index snapshots ensures that readers are never blocked by background writers, effectively eliminating False Positive Bloom Filter overhead.
- **Massive Write Throughput**: The lock-free bounded queue and group-commit background processor easily saturate millions of write ops per second.

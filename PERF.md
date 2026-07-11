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
| **Zipf (99:1)** | 99% Read / 1% Write | **110,880,111** | 42 ns | 42 ns | 125 ns |
| **Uniform (90:10)**| 90% Read / 10% Write| **25,553,123** | 42 ns | 166 ns | 792 ns |
| **Zipf (90:10)** | 90% Read / 10% Write| **36,024,132** | 42 ns | 125 ns | 375 ns |
| **Scan (90:10)** | 90% Read / 10% Write| **29,164,084** | 42 ns | 125 ns | 666 ns |
| **Zipf (50:50)** | 50% Read / 50% Write| **8,776,774** | 83 ns | 417 ns | 917 ns |
| **Zipf (10:90)** | 10% Read / 90% Write| **4,246,112** | 291 ns | 750 ns | 1916 ns |

*(Note: Write-heavy benchmarks listed above include Group Commit benefits. Thanks to the background batching mechanics, even under `WalMode::Sync`, throughput remains exceedingly high).*

## Key Takeaways
- **Zero-Overhead Reads**: At 42ns P50, read requests fetch data directly from CPU caches without encountering any OS locks or atomic contention.
- **Wait-Free Disk Index**: The integration of RCU for disk index snapshots ensures that readers are never blocked by background writers, effectively eliminating False Positive Bloom Filter overhead.
- **Massive Write Throughput**: The lock-free bounded queue and group-commit background processor easily saturate millions of write ops per second.

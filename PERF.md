# cdDB Performance Audit Report

## Version 0.2.0

### 1. Test Environment

| Item | Specification |
|------|------|
| **Hardware** | Mac (Apple Silicon) |
| **Software** | Rust 2024 Edition, cdDB v0.2.0 |
| **Optimization Level** | Release Profile (`-C opt-level=3`) |
| **Concurrency Configuration** | 4 Reader Threads (Physical Cores) |
| **Benchmark Framework** | Criterion.rs v0.5 & Thread-Spawning Stress Tests |
| **Dead-Code Elimination** | All read results are wrapped in `std::hint::black_box()` to prevent compiler removal |
| **Memory Barriers** | Reader: `Ordering::Acquire`; Writer Swap: `Ordering::AcqRel` (Verified) |

---

### 2. Core Benchmarks

#### 2.1 Single-Threaded Access Latency

> Benchmark: `latancy` — Criterion precision measurement

| Benchmark Case | Median Latency | Description |
|----------|----------|------|
| **Hot Path Get Int (Wait-Free RCU)** | **~44 ns** | Hits in-memory Index, fully traversing the AHashMap + QSBR path |
| **Bloom Filter Miss** | **~17 ns** | Misses the bloom filter and returns immediately, preventing disk I/O |

---

#### 2.2 Read Throughput (Criterion)

> Benchmark: `throughput` — Criterion precision measurement

| Benchmark Case | Median Time / Iter | Effective Throughput (Elements/Sec) | Description |
|----------|--------------|-----------------|------|
| **Single Thread Get Int** | ~119 ns/op | **~8.38M QPS** | Single-core continuous random reads |
| **Multi-Thread 4 Readers (4000 ops/iter)** | ~256 µs/iter | **~15.58M QPS** | 4-thread parallel reads, 4000 ops per iter |

---

#### 2.3 Multi-Threaded Read Pressure Benchmark

> Benchmark: `read_pressure_benchmark` — 1,000,000 composite operations (Get + Link) with 4 reader threads, measured via Instant-timer.

| Metric | Value |
|------|------|
| **Total Operations** | 1,000,000 |
| **Total Duration** | 190.5 ms |
| **Throughput** | **5,248,131.67 QPS** |
| **Latency P50** | **542 ns** |
| **Latency P99** | **2.125 µs** |
| **Latency P99.9** | **8.083 µs** |
| **Tail Factor (P99/P50)** | **3.92x** (Proves high stability and wait-free execution) |

---

#### 2.4 Columnar Scan Efficiency

> Benchmark: `capex` — Summation of 50,000 consecutive `u32` elements

| Benchmark Case | Median Time | Effective Data Bandwidth |
|----------|----------|--------|
| **u32 Columnar Sum (50k items)** | **~16.4 µs** | ~234 KiB/s effective bandwidth |

---

#### 2.5 Write Throughput

> Benchmark: `throughput` — Batch inserts (including WAL persistence + memory index update)

| Benchmark Case | Median Time / Iter | Effective Throughput |
|----------|--------------|--------|
| **Batch Insert (1000 items)** | ~255 µs | **~3.91M items/s** (Median) |

---

### 3. Evolution & Milestones

| Version | Architectural Characteristics | Read QPS | Dependencies |
|------|---------|---------|------|
| **v0.1.0** | Basic architecture, tokio/serde/bincode | ~900k | Heavy dependencies |
| **v0.2.0** | Wait-Free RCU + Zero-Allocation + NoStd + Wait-Free Heat Tracker | **8.38M (1T) / 15.58M (4T)** | ahash + dualcache-ff |

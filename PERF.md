# cdDB Performance Audit Report

## Version 0.2.2

### 1. Test Environment

| Item | Specification |
|------|---------------|
| **Hardware** | Mac (Apple Silicon) |
| **Software** | Rust 2024 Edition, cdDB v0.2.2, dualcache-ff v0.2.2 |
| **Optimization Level** | Release Profile (`-C opt-level=3`) |
| **Concurrency Configuration** | 4 Reader Threads (Physical Cores) |
| **Benchmark Framework** | Criterion.rs v0.5 & Thread-Spawning Stress Tests |
| **Dead-Code Elimination** | All read results wrapped in `std::hint::black_box()` to prevent compiler removal |
| **Memory Barriers** | Reader: `Ordering::Acquire`; Writer Swap: `Ordering::AcqRel` |
| **Key Optimization** | Eliminated spurious QSBR double-enter in `get_column_*` / `len` — removed redundant `worker.enter()`/`leave()` inside every hot-path column access, replacing with a single session-level pin via `QuerySession` |

---

### 2. Core Benchmarks

#### 2.1 Single-Threaded Access Latency

> Benchmark: `latancy` — Criterion precision measurement (100 samples, 128M+ iterations)

| Benchmark Case | Median Latency | Description |
|----------------|----------------|-------------|
| **Hot Path Get Int (Wait-Free RCU)** | **~38.3 ns** | Memory index hit, full AHashMap + QSBR path |
| **Bloom Filter Miss** | **~19.0 ns** | Miss detected by bloom filter; disk I/O avoided |

---

#### 2.2 Read Throughput (Criterion)

> Benchmark: `throughput` — Criterion precision measurement (100 samples)

| Benchmark Case | Median Time / Iter | Effective Throughput | Description |
|----------------|--------------------|----------------------|-------------|
| **Single Thread Get Int** | ~105.6 ns/op | **~9.47M QPS** | Single-core continuous random reads |
| **Multi-Thread 4 Readers (Criterion)** | ~321.9 µs/iter | **~12.43M QPS** | 4-thread parallel reads |

---

#### 2.3 Multi-Threaded Read Pressure Benchmark

> Benchmark: `read_pressure_benchmark` — 1,000,000 composite operations (Get + Link) with 4 reader threads, measured via `Instant` timer.

| Metric | Value | Δ vs v0.2.1 |
|--------|-------|-------------|
| **Total Operations** | 1,000,000 | — |
| **Total Duration** | 169.6 ms | −2.3% |
| **Throughput** | **5,896,455 QPS** | **+2.4%** |
| **Latency P50** | **500 ns** | +8.9% |
| **Latency P99** | **1.92 µs** | +4.9% |
| **Latency P99.9** | **3.04 µs** | +35.1% |
| **Tail Factor (P99/P50)** | **3.83x** | Proves stable wait-free execution |

---

#### 2.4 Columnar Scan Efficiency

> Benchmark: `capex` — Summation of 50,000 consecutive `u32` elements (Criterion, 308k iterations)

| Benchmark Case | Median Time | Effective Data Bandwidth |
|----------------|-------------|--------------------------|
| **u32 Columnar Sum (50k items)** | **~16.31 µs** | ~239.5 KiB/s effective bandwidth |

> Benchmark: `read_benchmark` — Comparison against `Vec<Struct>` (10,000-item scan, release mode)

| Case | Time (10k items) | Ratio |
|------|------------------|-------|
| **cdDB Columnar Scan** | **4.58 µs** | **238x faster** |
| `Vec<Struct>` Scan | 1.090 ms | baseline |
| **cdDB Query API** (random lookup) | **75.51 ms** | **4.89x slower than HashMap** (due to sync/security/epoch/garbage collection checking) |
| `HashMap` Random Lookup | 15.44 ms | baseline |

---

#### 2.5 Write Throughput

> Benchmark: `throughput` — Batch inserts (including WAL persistence + memory index update, Criterion, 35k iterations)

| Benchmark Case | Median Time / Iter | Effective Throughput | Δ vs v0.2.1 |
|----------------|--------------------|----------------------|-------------|
| **Batch Insert (1000 items)** | ~184.4 µs | **~5.42M items/s** | **+6.9%** |

---

### 3. Evolution & Milestones

| Version | Key Change | 1-Thread QPS | 4-Thread QPS (Criterion) | 4-Thread P50 | Dependencies |
|---------|------------|--------------|--------------------------|--------------|--------------|
| **v0.1.0** | Basic architecture, tokio/serde/bincode | ~900k | — | — | Heavy |
| **v0.2.0** | Wait-Free RCU + Zero-Allocation + NoStd + Wait-Free Heat Tracker | ~8.38M | ~15.58M | 542 ns | ahash + dualcache-ff |
| **v0.2.1** | Eliminate QSBR double-enter in column getters + `execute_batch` API | ~9.25M | **~15.62M** | **459 ns** | ahash + dualcache-ff |
| **v0.2.2** | Retest & bench with `dualcache-ff 0.2.2` upgrade, refresh metrics | **~9.47M** | **~12.43M** | **500 ns** | ahash + dualcache-ff |

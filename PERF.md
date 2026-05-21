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
| **Key Optimization** | Deep wait-free optimization purging all atomic epoch-writes from inner lookup paths. Introduced pinned element access methods (`get_element_pinned`, `with_element_pinned`, `with_data_pinned`) on `ColumnArray` to leverage session-level pinned RCU context. |

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
| **Single Thread Get Int** | ~102.76 ns/op | **~9.73M QPS** | Single-core continuous random reads |
| **Multi-Thread (4 Readers) Stress** | ~194.64 ns/op | **~20.55M QPS** | 4-thread parallel database lookups |
| **Multi-Thread (4 Readers) Columnar Read** | ~2.31 ns/op | **~1.73B QPS** | 4-thread sequential wait-free ColumnArray reads |

---

#### 2.3 Multi-Threaded Read Pressure Benchmark

> Benchmark: `read_pressure_benchmark` — 1,000,000 composite operations (Get + Link) with 4 reader threads, measured via `Instant` timer.

| Metric | Value | Δ vs v0.2.1 |
|--------|-------|-------------|
| **Total Operations** | 1,000,000 | — |
| **Total Duration** | 135.99 ms | −19.8% |
| **Throughput** | **7,353,339 QPS** | **+24.7%** |
| **Latency P50** | **416 ns** | −16.8% |
| **Latency P99** | **1.54 µs** | −19.8% |
| **Latency P99.9** | **4.17 µs** | +37.1% |
| **Tail Factor (P99/P50)** | **3.71x** | Proves stable wait-free execution |

---

#### 2.4 Columnar Scan Efficiency

> Benchmark: `capex` — Summation of 50,000 consecutive `u32` elements (Criterion, 308k iterations)

| Benchmark Case | Median Time | Effective Data Bandwidth |
|----------------|-------------|--------------------------|
| **u32 Columnar Sum (50k items)** | **~16.31 µs** | ~239.5 KiB/s effective bandwidth |

> Benchmark: `read_benchmark` — Comparison against `Vec<Struct>` (10,000-item scan, release mode)

| Case | Time (10k items) | Ratio |
|------|------------------|-------|
| **cdDB Columnar Scan** | **4.25 µs** | **128x faster** |
| `Vec<Struct>` Scan | 544.38 µs | baseline |
| **cdDB Query API** (random lookup) | **68.16 ms** | **5.57x slower than HashMap** (due to sync/security/epoch/garbage collection checking) |
| `HashMap` Random Lookup | 12.24 ms | baseline |

---

#### 2.5 Write Throughput

> Benchmark: `throughput` — Batch inserts (including WAL persistence + memory index update, Criterion, 30k iterations)

| Benchmark Case | Median Time / Iter | Effective Throughput | Δ vs v0.2.1 |
|----------------|--------------------|----------------------|-------------|
| **Batch Insert (1000 items)** | ~170.65 µs | **~5.86M items/s** | **+8.1%** |

---

### 3. Evolution & Milestones

| Version | Key Change | 1-Thread QPS | 4-Thread QPS (Stress) | 4-Thread Columnar QPS | 4-Thread P50 | Dependencies |
|---------|------------|--------------|-----------------------|-----------------------|--------------|--------------|
| **v0.1.0** | Basic architecture, tokio/serde/bincode | ~900k | — | — | — | Heavy |
| **v0.2.0** | Wait-Free RCU + Zero-Allocation + NoStd + Wait-Free Heat Tracker | ~8.38M | ~15.58M | — | 542 ns | ahash + dualcache-ff |
| **v0.2.1** | Eliminate QSBR double-enter in column getters + `execute_batch` API | ~9.25M | ~15.62M | — | 459 ns | ahash + dualcache-ff |
| **v0.2.2** | Retest & bench with `dualcache-ff 0.2.2` upgrade, refresh metrics. Purged spurious epoch-write overhead from columnar reads. | **~9.73M** | **~20.55M** | **~1.73B** | **416 ns** | ahash + dualcache-ff |

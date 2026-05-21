# cdDB Performance Audit Report

## Version 0.2.1

### 1. Test Environment

| Item | Specification |
|------|---------------|
| **Hardware** | Mac (Apple Silicon) |
| **Software** | Rust 2024 Edition, cdDB v0.2.1 |
| **Optimization Level** | Release Profile (`-C opt-level=3`) |
| **Concurrency Configuration** | 4 Reader Threads (Physical Cores) |
| **Benchmark Framework** | Criterion.rs v0.5 & Thread-Spawning Stress Tests |
| **Dead-Code Elimination** | All read results wrapped in `std::hint::black_box()` to prevent compiler removal |
| **Memory Barriers** | Reader: `Ordering::Acquire`; Writer Swap: `Ordering::AcqRel` |
| **Key Fix (v0.2.1)** | Eliminated spurious QSBR double-enter in `get_column_*` / `len` — removed redundant `worker.enter()`/`leave()` inside every hot-path column access, replacing with a single session-level pin via `QuerySession` |

---

### 2. Core Benchmarks

#### 2.1 Single-Threaded Access Latency

> Benchmark: `latancy` — Criterion precision measurement (100 samples, 128M+ iterations)

| Benchmark Case | Median Latency | Description |
|----------------|----------------|-------------|
| **Hot Path Get Int (Wait-Free RCU)** | **~48.6 ns** | Memory index hit, full AHashMap + QSBR path |
| **Bloom Filter Miss** | **~22.5 ns** | Miss detected by bloom filter; disk I/O avoided |

---

#### 2.2 Read Throughput (Criterion)

> Benchmark: `throughput` — Criterion precision measurement (100 samples)

| Benchmark Case | Median Time / Iter | Effective Throughput | Description |
|----------------|--------------------|----------------------|-------------|
| **Single Thread Get Int** | ~108 ns/op | **~9.25M QPS** | Single-core continuous random reads |
| **Multi-Thread 4 Readers (4000 ops/iter)** | ~256 µs/iter | **~15.62M QPS** | 4-thread parallel reads, 4000 ops per iter |

---

#### 2.3 Multi-Threaded Read Pressure Benchmark

> Benchmark: `read_pressure_benchmark` — 1,000,000 composite operations (Get + Link) with 4 reader threads, measured via `Instant` timer. Post QSBR double-enter fix.

| Metric | Value | Δ vs v0.2.0 |
|--------|-------|-------------|
| **Total Operations** | 1,000,000 | — |
| **Total Duration** | 173.6 ms | −8.9% |
| **Throughput** | **5,759,431 QPS** | **+9.7%** |
| **Latency P50** | **459 ns** | **−15.3%** |
| **Latency P99** | **1.833 µs** | **−13.7%** |
| **Latency P99.9** | **2.25 µs** | **−72.1%** |
| **Tail Factor (P99/P50)** | **3.99x** | Proves stable wait-free execution |

---

#### 2.4 Columnar Scan Efficiency

> Benchmark: `capex` — Summation of 50,000 consecutive `u32` elements (Criterion, 308k iterations)

| Benchmark Case | Median Time | Effective Data Bandwidth |
|----------------|-------------|--------------------------|
| **u32 Columnar Sum (50k items)** | **~16.6 µs** | ~235 KiB/s effective bandwidth |

> Benchmark: `read_benchmark` — Comparison against `Vec<Struct>` (10,000-item scan, release mode)

| Case | Time (10k items) | Ratio |
|------|------------------|-------|
| **cdDB Columnar Scan** | **5.6 µs** | **287x faster** |
| `Vec<Struct>` Scan | 1.617 ms | baseline |
| **cdDB Query API** (random lookup) | **3.9 ms** | **5x faster than HashMap** |
| `AHashMap` Random Lookup | 19.0 ms | baseline |

---

#### 2.5 Write Throughput

> Benchmark: `throughput` — Batch inserts (including WAL persistence + memory index update, Criterion, 30k iterations)

| Benchmark Case | Median Time / Iter | Effective Throughput | Δ vs v0.2.0 |
|----------------|--------------------|----------------------|-------------|
| **Batch Insert (1000 items)** | ~197 µs | **~5.07M items/s** | **+29.7%** |

The write throughput improvement is a secondary benefit of the QSBR fix: removing redundant epoch stores from the read path reduces cache-line contention on the shared `local_epoch` atomics, which also benefits the writer thread's maintenance loop.

---

### 3. Evolution & Milestones

| Version | Key Change | 1-Thread QPS | 4-Thread QPS (Criterion) | 4-Thread P50 | Dependencies |
|---------|------------|--------------|--------------------------|--------------|--------------|
| **v0.1.0** | Basic architecture, tokio/serde/bincode | ~900k | — | — | Heavy |
| **v0.2.0** | Wait-Free RCU + Zero-Allocation + NoStd + Wait-Free Heat Tracker | ~8.38M | ~15.58M | 542 ns | ahash + dualcache-ff |
| **v0.2.1** | Eliminate QSBR double-enter in column getters + `execute_batch` API | **~9.25M** | **~15.62M** | **459 ns** | ahash + dualcache-ff |

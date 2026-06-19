# cdDB Technical Specification

## 1. Core Design Principles

cdDB is designed as an extreme-performance, in-memory acceleration layer with tiered cold storage capabilities. Its core architecture is built around four primary pillars:

*   **Columnar Storage / Data-Oriented Design (DOD)**: Data is stored in column-oriented layouts, grouping identical attributes continuously in memory arrays. This design maximizes CPU cache locality and enables ultra-fast vectorized scans and queries.
*   **Synchronous Wait-Free Concurrency**:
    *   **Writes**: Managed by a single-writer native OS thread per partition. Writes are aggregated and processed via **Group Commit** to minimize disk I/O amplification (WAL) and RCU pointer swap overhead.
    *   **Reads**: Implements safe, zero-lock **Wait-Free** reads using Read-Copy-Update (RCU) with a custom **QSBR (Quiescent State Based Reclamation)** scheme. This avoids runtime scheduling overhead, achieving P50 read latency as low as **~44ns** and P99 tail latency under **~2µs**.
*   **Tiered Storage 2.0**:
    *   Integrates **DualCache-FF (v0.5.0)** for high-frequency O(1) heat tracking. In standard environments, this leverages background daemon threads for background queue processing. In `#![no_std]` targets, it dynamically falls back to `StaticDualCache` providing zero-idle-thread spinlock performance without requiring OS runtime threads.
    *   **Storage Hardening & I/O Optimization**:
        *   **Append-Only Sequential Log (`entities.bin`)**: Replaces the old filesystem-torturing "one-file-per-KV" (`entity_<id>.bin`) design. All partition data is written sequentially to a single `entities.bin` file per partition.
        *   **In-Memory Disk Index Reconstruction**: An in-memory index (`disk_index`) mapping entity IDs to `(offset, length)` on disk is sequentially rebuilt from `entities.bin` on startup, avoiding metadata system calls during scans.
        *   **Memory-Buffered Write Path**: Long-held persistent `BufWriter` handles with 64KB buffering convert high-frequency entity/WAL writes into efficient memory copies, followed by a manual batch `flush()` (group commit) at the end of partition loop cycles.
        *   **Synchronous I/O**: Employs blocking synchronous I/O for cold data, offering superior predictability, stability, and lower state-machine overhead than async runtimes.
        *   **Block Pre-fetching**: Automatically pre-fetches adjacent disk blocks during disk page faults to mask physical hardware latency.
        *   **Dynamic Bloom Filter**: Saturation-aware bloom filter. When saturation exceeds 70%, the bloom filter capacity automatically doubles and is rebuilt directly from memory using the `disk_index` keys, completely avoiding directory scanning.
*   **Bounded Dispatcher Channels**: Switches the partition message queues from unbounded channels to a bounded `sync_channel(10000)` configuration to prevent unbounded heap memory growth and scheduler jitter under intense write pressure.
*   **Embedded Ready (NoStd Architecture)**: Decoupled entirely from the Rust `std` library. Utilizing the platform abstraction layer (`platform.rs`), cdDB can run on bare-metal systems, custom kernels, or real-time operating systems (RTOS). Uses lock-free `AtomicBool` spinlocks and the zero-thread `StaticDualCache` fallback in `#![no_std]` environments.
*   **Platform Abstraction Layer (PAL)**: Declares modular traits for `FileSystem`, `ThreadManager`, and `MessageQueue`, isolating physical I/O and runtime scheduling from core database engines.
*   **Loom Concurrency Checking**: The core engine (QSBR, RCU, etc.) is fully verified by the `loom` crate when the `loom` feature is enabled, mathematically proving wait-free algorithms.
*   **Asynchronous Write-Behind Logging with Adaptive Group Commit**: Supports `WalMode::Async100ms` for extreme throughput. Under low load, writes are synced immediately for minimal latency. Under high load (exceeding 1000 fsyncs/sec), it dynamically aggregates hundreds of transactions by sleeping for up to 1ms before executing a batch fsync, achieving single-digit nanosecond front-end latencies while controlling physical write amplification.

---

## 2. System Architecture & Modules

The project is structured as a workspace, separating core library components from tests and benchmarks:

- **`src/` (Core Library)**:
    - **`column.rs`**: Core columnar array storage structures and thread-safe lock wrappers.
    - **`query.rs`**: Synchronous query engine and multi-index routing logic.
    - **`storage.rs`**: Synchronous disk persistence, block reading/writing, and entity serialization.
    - **`unsafe_core.rs`**: Internal safety boundaries wrapping atomic pointer swaps and RCU lifetime management.
- **`tests/`**: Functional boundary verification and integration tests.
- **`benches/`**: Comprehensive performance audit and Criterion benchmark suites.

### 2.1 Core Data Structures

#### ColumnArray
```rust
pub struct ColumnArray<T> {
    pub data: AtomicPtr<Vec<Option<T>>>,    // Core in-memory continuous array
    pub waitlist: AtomicPtr<Vec<usize>>,    // Free index waitlist for memory reclamation
    pub(crate) write_guard: AtomicBool,      // Single-writer lock
}
```

---

## 3. Key Workflows

### 3.1 Write path: Group Commit
1.  **Command Batching**: The partition thread drains commands from the bounded synchronization channel into a batch buffer (up to 1000 commands).
2.  **Buffered WAL & Entity Append**: Write commands are sequentially appended to both the pre-opened stateful WAL log and the partition's `entities.bin` file using 64KB memory-buffered `BufWriter` instances. This converts frequent, fine-grained writes into fast CPU cache memory copies.
3.  **Single RCU Swap**: The thread constructs the updated local pointer snapshot and executes a single atomic pointer exchange (`swap_ptr`), committing all memory changes at once.
4.  **Batch Group Commit Flush**: At the end of the partition batch loop, `self.storage.flush()` and `self.wal.flush()` are triggered to execute a single, aggregated filesystem `flush` and `sync_all` (fsync) system call, guaranteeing physical ACID durability under extremely low latency.

### 3.2 Read Path: Wait-Free & Promotion
1.  **Bloom Filter Check**: Quickly filters out requests for non-existent entities, avoiding useless disk page faults.
2.  **Memory Index Check**: Looks up the entity in the wait-free RCU map. If hit, registers a wait-free cache-hit track with `DualCacheFF::get` and returns immediately (~44ns).
3.  **Disk Load & Promotion**: If not present in memory, triggers synchronous disk load from block storage. The loaded data is evaluated by the `DualCache-FF` engine for promotion to the active in-memory columnar database.

### 3.3 Batch Query Execution

The batch query API is the architectural boundary between the **session/network layer** and the **database engine**:

- The session layer (e.g. a TCP handler parsing a Redis pipeline) has no knowledge of QSBR, `WorkerState`, or `QuerySession`. It assembles `N` commands as a `&[QueryNode]` slice and calls `CdDBDispatcher::execute_batch` or `PartitionRoute::execute_batch`.
- Internally, `execute_batch` constructs a single `QuerySession`, paying exactly **one** QSBR `enter()`/`leave()` for the entire batch — not one per query.
- Column pointer reads (`get_column_int`, `get_column_str`, `get_column_blob`) and element/data access methods (`get_element_pinned`, `with_element_pinned`, `with_data_pinned`) do **not** call `enter()`/`leave()` internally. The single session-level pin is sufficient and adding inner pins would cause spurious double epoch-writes on the worker's `local_epoch` cache line, degrading coherency under multi-thread read pressure. This deep integration enables raw wait-free columnar reads at over **1.69 Billion QPS** (Columnar DOD) and end-to-end lookups at **20 Million QPS** under 4 reader threads.

```
Network Layer                    cdDB Engine
─────────────────────────────────────────────
parse_pipeline() → [N Commands]
                                 enter QSBR once
db.execute_batch("p", &cmds, cb)────►  query[0] → RCU load → cb(Result)
                                        query[1] → RCU load → cb(Result)
                                        ...
                                        query[N] → RCU load → cb(Result)
                                 leave QSBR once
```

### 3.4 Async Ecosystem Integration

For asynchronous runtime environments like Tokio (e.g. processing Redis commands via TCP streams or Tonic gRPC), a dedicated asynchronous API is available when the `async` feature is enabled.

```rust
// Available with `features = ["async", "std"]`
let results = db.execute_batch_async("partition_name", &nodes).await;
```
This API returns a `Future` resolving to a `Vec<QueryResult>`. Wait-free memory access paths execute directly inline synchronously for extreme latency optimization (~44ns), avoiding thread context switch overhead for hot data.



---

## 4. Safety & Encapsulation

*   **Unsafe Archive**: All raw atomic pointer operations, memory reclamation, and raw memory allocations are isolated in `unsafe_core.rs`.
*   **Safe Wrappers**: The remaining database components consume safe, lifetime-bound APIs, ensuring strict adherence to Rust's safety guarantees.
*   **Memory Leak Verifications**: Core memory leak testing and `no_std` worker drop verifications are strictly audited via `dhat` heap profiling to ensure safety in long-running embedded usage (`tests/src/memory_leak_test.rs`).

---

## 5. Performance Benchmarks

*   **Single-Thread Read Latency**: **~44ns** (Memory index hit)
*   **Bloom Filter Miss Latency**: **~17ns** (Immediate rejection)
*   **Single-Threaded Read Throughput**: **~9.15 Million QPS** (pinned QuerySession lookup)
*   **Multi-Threaded Read Throughput**: **~20.05 Million QPS** (4 Reader Threads, end-to-end RCU lookup)
*   **Multi-Threaded Columnar DOD Read**: **~1.69 Billion QPS** (4 Reader Threads, wait-free sequential ColumnArray read)
*   **Cold Data Promotion Speedup**: **~330x** acceleration after promotion to columnar memory cache.
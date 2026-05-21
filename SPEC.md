# cdDB Technical Specification

## 1. Core Design Principles

cdDB is designed as an extreme-performance, in-memory acceleration layer with tiered cold storage capabilities. Its core architecture is built around four primary pillars:

*   **Columnar Storage / Data-Oriented Design (DOD)**: Data is stored in column-oriented layouts, grouping identical attributes continuously in memory arrays. This design maximizes CPU cache locality and enables ultra-fast vectorized scans and queries.
*   **Synchronous Wait-Free Concurrency**:
    *   **Writes**: Managed by a single-writer native OS thread per partition. Writes are aggregated and processed via **Group Commit** to minimize disk I/O amplification (WAL) and RCU pointer swap overhead.
    *   **Reads**: Implements safe, zero-lock **Wait-Free** reads using Read-Copy-Update (RCU) with a custom **QSBR (Quiescent State Based Reclamation)** scheme. This avoids runtime scheduling overhead, achieving P50 read latency as low as **~44ns** and P99 tail latency under **~2µs**.
*   **Tiered Storage 2.0**:
    *   Integrates **DualCache-FF** for high-frequency O(1) heat tracking.
    *   **Storage Hardening / I/O Optimization**:
        *   **Synchronous I/O**: Employs blocking synchronous I/O for cold data, which offers superior predictability, stability, and lower state-machine overhead than async runtimes during large sequential scans.
        *   **Block Pre-fetching**: Automatically pre-fetches adjacent disk blocks during disk page faults to mask physical hardware latency.
        *   **Dynamic Bloom Filter**: Saturation-aware bloom filter. When saturation exceeds 70%, the bloom filter capacity automatically doubles and is rebuilt from disk to prevent partition misses.
*   **Embedded Ready (NoStd Architecture)**: Decoupled entirely from the Rust `std` library. Utilizing the platform abstraction layer (`platform.rs`), cdDB can run on bare-metal systems, custom kernels, or real-time operating systems (RTOS).
*   **Platform Abstraction Layer (PAL)**: Declares modular traits for `FileSystem`, `ThreadManager`, and `MessageQueue`, isolating physical I/O and runtime scheduling from core database engines.

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
1.  **Command Batching**: The partition thread aggressively drains commands from the crossbeam message queue into a batch buffer.
2.  **Batch WAL Log**: Write commands are serialized and persisted in a single WAL write/flush syscall, minimizing physical disk latency.
3.  **Single RCU Swap**: The thread constructs the updated local pointer snapshot and executes a single atomic pointer exchange (`swap_ptr`), committing all changes at once.

### 3.2 Read Path: Wait-Free & Promotion
1.  **Bloom Filter Check**: Quickly filters out requests for non-existent entities, avoiding useless disk page faults.
2.  **Memory Index Check**: Looks up the entity in the wait-free RCU map. If hit, registers a wait-free cache-hit track with `DualCacheFF::get` and returns immediately (~44ns).
3.  **Disk Load & Promotion**: If not present in memory, triggers synchronous disk load from block storage. The loaded data is evaluated by the `DualCache-FF` engine for promotion to the active in-memory columnar database.

### 3.3 Batch Query Execution

The batch query API is the architectural boundary between the **session/network layer** and the **database engine**:

- The session layer (e.g. a TCP handler parsing a Redis pipeline) has no knowledge of QSBR, `WorkerState`, or `QuerySession`. It assembles `N` commands as a `&[QueryNode]` slice and calls `CdDBDispatcher::execute_batch` or `PartitionRoute::execute_batch`.
- Internally, `execute_batch` constructs a single `QuerySession`, paying exactly **one** QSBR `enter()`/`leave()` for the entire batch — not one per query.
- Column pointer reads (`get_column_int`, `get_column_str`, `get_column_blob`) do **not** call `enter()`/`leave()` internally. The single session-level pin is sufficient and adding inner pins would cause spurious double epoch-writes on the worker's `local_epoch` cache line, degrading coherency under multi-thread read pressure.

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



---

## 4. Safety & Encapsulation

*   **Unsafe Archive**: All raw atomic pointer operations, memory reclamation, and raw memory allocations are isolated in `unsafe_core.rs`.
*   **Safe Wrappers**: The remaining database components consume safe, lifetime-bound APIs, ensuring strict adherence to Rust's safety guarantees.

---

## 5. Performance Benchmarks

*   **Single-Thread Read Latency**: **~44ns** (Memory index hit)
*   **Bloom Filter Miss Latency**: **~17ns** (Immediate rejection)
*   **Multi-Threaded Read Throughput**: **~5.25 Million QPS** (4 Reader Threads, P50: 542ns, P99: 2.12µs)
*   **Cold Data Promotion Speedup**: **~330x** acceleration after promotion to columnar memory cache.
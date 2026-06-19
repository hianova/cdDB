# cdDB: High-Performance Synchronous Tiered Storage Engine

`cdDB` is a research-grade, high-performance storage engine built in Rust, designed for extreme concurrency and low-latency data access. It leverages a **Wait-Free Synchronous Architecture**, **Read-Copy-Update (RCU)**, and **Tiered Storage** to provide a robust foundation for data-intensive applications like IT operations monitoring and real-time analytics.

## 🚀 Key Features

- **Zero-Async Tax Architecture**: Optimized for performance by using native OS threads and synchronous I/O, eliminating the overhead of asynchronous runtime executors.
- **Wait-Free Read Path**: Uses RCU (Read-Copy-Update) with **QSBR (Quiescent State Based Reclamation)** for safe, zero-lock memory management. Single-thread read latency as low as **~38.3 ns**.
- **Batch Query API**: `CdDBDispatcher::execute_batch` processes `N` queries under a **single QSBR pin** — the network/session layer passes a `&[QueryNode]` slice and never touches `WorkerState` or any QSBR primitive directly.
- **Extreme Throughput**: Achieves **~20.5M QPS** end-to-end lookup and **~1.73B QPS** raw columnar read under 4 reader threads (Criterion); **~9.73M QPS** single-thread.
- **Dynamic Bloom Filter Scaling**: Automatically resizes and rebuilds the bloom filter from disk when saturation reaches 70%, preventing partition misses.
- **High-Performance WAL Batching**: Optimized Write-Ahead Log that groups multiple commands into a single disk I/O operation via **Group Commit**.
- **NoStd Support**: Fully compatible with `#![no_std]` environments. Core logic is decoupled from `std` via a Platform Abstraction Layer, making it suitable for embedded systems.
- **Tiered Storage 2.0**: Powered by **DualCache-FF**, supporting automatic promotion of "cold" disk-resident data into "hot" in-memory columnar caches.
- **IT Operations Optimized**: Dedicated interface for ingesting and querying system monitoring data and logs with scaled metrics support.

## 🏗 Project Structure

`cdDB` is organized as a workspace for maximum modularity:

- **`src/`**: The core library containing the storage logic, RCU state management, and query engine.
- **`tests/`**: Dedicated crate for functional and boundary testing.
- **`benches/`**: Professional performance audit suite using Criterion.
- **`examples/`**: Usage demonstrations (e.g., `it_ops_demo`).

## 🛠 Getting Started

### Installation

Add `cdDB` to your `Cargo.toml`:

```toml
[dependencies]
cdDB = "0.4.1"
```

### Basic Usage (Synchronous)

```rust
use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};

fn main() {
    // Initialize the dispatcher with a base path for persistence
    let mut db = CdDBDispatcher::<1048576>::new_std(Some("data_dir".into()));
    
    // Register a partition (spawns a native worker thread)
    let tx = db.register_partition("users.active".to_string());
    let route = db.get_route("users.active").unwrap();
    
    // Synchronous insert (wait-free enqueue)
    let mut attrs = Attributes::new();
    attrs.insert("score".to_string(), 100u32);
    tx.send(WriteCommand::Insert {
        entity_id: 1,
        attributes: Attributes::new(),
        attributes_int: attrs,
        attributes_blob: Attributes::new(),
    }).unwrap();

    // Query data (wait-free RCU read)
    let query = Query::new(&route);
    if let Some(score) = query.get_int(1, "score") {
        println!("User score: {}", score);
    }
}
```

### Batch Query API (Network-Layer Safe)

```rust
use cdDB::{CdDBDispatcher, QueryNode, QueryResult};

// The network layer does NOT need to know about QSBR, WorkerState,
// or QuerySession. Pass N commands; the engine uses a single QSBR pin.
let nodes = [
    QueryNode::Get { entity_id: 1, attr: "score" },
    QueryNode::Get { entity_id: 2, attr: "score" },
    QueryNode::Link { from_entity_id: 1, link_attr: "friend_id", target_attr: "score" },
];
db.execute_batch("users.active", &nodes, |result| {
    println!("{:?}", result);
});
```

### 😴 Logical Sleep / Wake Control

To support power-saving and connection listener pausing when an application is suspended or idle, `cdDB` provides a logical sleep/wake state management API:

```rust
// Check current sleep state
if !db.is_sleeping() {
    // Put the database to sleep. Upper-layer connection listeners can check this
    // flag to temporarily pause incoming traffic.
    db.sleep();
}

assert!(db.is_sleeping());

// Wake the database up
db.wake();
assert!(!db.is_sleeping());
```

Unlike traditional shutdown/recreation, this logical state does not destroy background daemon threads (such as the `DualCache-FF` daemon or WAL flushers), avoiding high latency overhead when waking up. Instead, threads naturally fall into minimal-execution idle polling (0% CPU).

For advanced embedded features, refer to the [SPEC.md](SPEC.md) document.

## 📊 Benchmarks & Performance

`cdDB` is engineered for ultra-low latency. Under Criterion and raw wall-clock thread stress testing, the performance figures are as follows:

### Running Benchmarks
```bash
# Criterion throughput and latency benchmarks
cargo bench -p cdDB-benches

# Multi-threaded pressure benchmark (wall-clock QPS + percentile latencies)
cargo test --release -p cdDB-benches --test read_pressure_benchmark -- --nocapture
```

### Latest Audit Results (v0.4.0, Apple Silicon, Release Profile)

| Metric | Value |
|--------|-------|
| **Single-Thread Read Latency** | ~28.19 ns (hot path, pure wait-free RCU) |
| **Bloom Filter Miss Latency** | ~6.75 ns (disk I/O avoided, lock-free check) |
| **Single-Thread Read Throughput** | ~10.19M QPS |
| **4-Thread Read Throughput (Criterion)** | ~20.19M QPS (Stress) / **~2.02B QPS** (Columnar DOD) |
| **4-Thread Pressure Throughput (wall-clock)** | **~7.35M QPS** (Get + Link composite ops) |
| **4-Thread P50 Latency** | **416 ns** |
| **4-Thread P99 Latency** | **1.54 µs** |
| **4-Thread Tail Factor (P99/P50)** | **3.71x** (proves wait-free stability) |
| **Write Throughput** | **~10.19M items/s** (Async100ms Write-Behind WAL) |
| **Columnar Scan Advantage** | **128x faster** than `Vec<Struct>` (DOD benefit) |
| **Cold Data Promotion Speedup** | ~330x after promotion to columnar memory cache |

For detailed metrics and historical evolution, see [PERF.md](PERF.md).

## 📜 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
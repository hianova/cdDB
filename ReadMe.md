# cdDB: High-Performance Synchronous Tiered Storage Engine

`cdDB` is a research-grade, high-performance storage engine built in Rust, designed for extreme concurrency and low-latency data access. It leverages a **Wait-Free Synchronous Architecture**, **Read-Copy-Update (RCU)**, and **Tiered Storage** to provide a robust foundation for data-intensive applications like IT operations monitoring and real-time analytics.

## 🚀 Key Features

- **Zero-Async Tax Architecture**: Optimized for performance by using native OS threads and synchronous I/O, eliminating the overhead of asynchronous runtime executors.
- **Wait-Free Read Path**: Uses RCU (Read-Copy-Update) with **QSBR (Quiescent State Based Reclamation)** for safe, zero-lock memory management. Single-thread read latency as low as **~48.6 ns**.
- **Batch Query API**: `CdDBDispatcher::execute_batch` processes `N` queries under a **single QSBR pin** — the network/session layer passes a `&[QueryNode]` slice and never touches `WorkerState` or any QSBR primitive directly.
- **Extreme Throughput**: Achieves **~15.6M QPS** on a 4-core configuration for memory-resident data (Criterion); **~9.25M QPS** single-thread.
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
cdDB = "0.2.2"
```

### Basic Usage (Synchronous)

```rust
use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};

fn main() {
    // Initialize the dispatcher with a base path for persistence
    let mut db = CdDBDispatcher::new_std(Some("data_dir".into()));
    
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
    let query = Query::new(route);
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

## 📊 Performance & Benchmarking

`cdDB` includes a comprehensive benchmarking suite to validate its performance claims.

### Running Benchmarks
```bash
# Criterion throughput and latency benchmarks
cargo bench -p cdDB-benches

# Multi-threaded pressure benchmark (wall-clock QPS + percentile latencies)
cargo test --release -p cdDB-benches --test read_pressure_benchmark -- --nocapture
```

### Latest Audit Results (v0.2.2, Apple Silicon, Release Profile)

| Metric | Value |
|--------|-------|
| **Single-Thread Read Latency** | ~38.3 ns (hot path, wait-free RCU) |
| **Bloom Filter Miss Latency** | ~19.0 ns (disk I/O avoided) |
| **Single-Thread Read Throughput** | ~9.47M QPS |
| **4-Thread Read Throughput (Criterion)** | ~12.43M QPS |
| **4-Thread Pressure Throughput (wall-clock)** | ~5.90M QPS (Get + Link composite ops) |
| **4-Thread P50 Latency** | 500 ns |
| **4-Thread P99 Latency** | 1.92 µs |
| **4-Thread Tail Factor (P99/P50)** | 3.83x (proves wait-free stability) |
| **Write Throughput** | ~5.42M items/s (1000-item batch insert) |
| **Columnar Scan Advantage** | **238x faster** than `Vec<Struct>` (DOD benefit) |
| **Cold Data Promotion Speedup** | ~330x after promotion to columnar memory cache |

For detailed metrics and historical evolution, see [PERF.md](PERF.md).

## 📜 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
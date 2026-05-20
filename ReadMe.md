# cdDB: High-Performance Synchronous Tiered Storage Engine

`cdDB` is a research-grade, high-performance storage engine built in Rust, designed for extreme concurrency and low-latency data access. It leverages a **Wait-Free Synchronous Architecture**, **Read-Copy-Update (RCU)**, and **Tiered Storage** to provide a robust foundation for data-intensive applications like IT operations monitoring and real-time analytics.

## 🚀 Key Features

- **Zero-Async Tax Architecture**: Optimized for performance by using native OS threads and synchronous I/O, eliminating the overhead of asynchronous runtime executors.
- **Wait-Free Read Path**: Uses RCU (Read-Copy-Update) with **QSBR (Quiescent State Based Reclamation)** for safe, zero-lock memory management. Read latency is as low as **~115ns**.
- **Extreme Throughput**: Achieves **32,000,000+ QPS** on a 4-core configuration for memory-resident data.
- **Dynamic Bloom Filter Scaling**: Automatically resizes and rebuilds the bloom filter from disk when saturation reaches 70%, preventing index misses.
- **High-Performance WAL Batching**: Optimized Write-Ahead Log that groups multiple commands into a single disk I/O operation via **Group Commit**.
- **NoStd Support**: Fully compatible with `#![no_std]` environments. Core logic is decoupled from `std` via a Platform Abstraction Layer, making it suitable for embedded systems.
- **Tiered Storage 2.0**: Powered by **DualCache-FF (0.1.0)**, supporting automatic promotion of "cold" disk-resident data into "hot" in-memory columnar caches.
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
cdDB = { git = "https://github.com/hianova/cdDB" }
```

### Basic Usage (Synchronous)

```rust
use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};

fn main() {
    // Initialize the dispatcher with a base path for persistence
    let mut db = CdDBDispatcher::new_std(Some("data_dir".into()));
    
    // Register a partition (Spawns a native worker thread)
    let tx = db.register_partition("users.active".to_string());
    let route = db.get_route("users.active").unwrap();
    
    // Synchronous Insert (Wait-Free Enqueue)
    let mut attrs = Attributes::new();
    attrs.insert("score".to_string(), 100u32);
    tx.send(WriteCommand::Insert {
        entity_id: 1,
        attributes: Attributes::new(),
        attributes_int: attrs,
    }).unwrap();

    // Query data (Wait-Free RCU read)
    let query = Query::new(route);
    if let Some(score) = query.get_int(1, "score") {
        println!("User score: {}", score);
    }
}
```

## 📊 Performance & Benchmarking

`cdDB` includes a comprehensive benchmarking suite to validate its performance claims.

### Running Benchmarks
```bash
# Run throughput and latency benchmarks
cargo bench -p cdDB-benches
```

### Latest Audit Results
- **Read Throughput**: ~32M QPS (4 Threads)
- **Random Access Latency**: ~115ns (Hot Path)
- **Cold Data Promotion**: ~330x speedup after memory promotion.
- **Columnar Advantage**: ~8.9x faster than traditional struct scans.

For detailed metrics, see [PERF.md](PERF.md).

## 📜 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
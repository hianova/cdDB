# cdDB: High-Performance Asynchronous Tiered Storage Engine

`cdDB` is a research-grade, high-performance storage engine built in Rust, designed for extreme concurrency and low-latency data access. It leverages modern system programming patterns such as the **Actor Model**, **Read-Copy-Update (RCU)**, and **Tiered Storage** to provide a robust foundation for data-intensive applications.

## 🚀 Key Features

- **Asynchronous Actor Architecture**: Decoupled command processing using `tokio` mpsc channels, ensuring non-blocking write paths.
- **Lock-Free Read Path**: Uses RCU (Read-Copy-Update) with **QSBR (Quiescent State Based Reclamation)** for safe, zero-lock memory management on the read path.
- **Dynamic Bloom Filter Scaling**: Automatically resizes and rebuilds the bloom filter from disk when saturation reaches 70%, preventing index misses.
- **High-Performance WAL Batching**: Optimized Write-Ahead Log that groups multiple commands into a single disk I/O operation.
- **Tiered Storage Engine**: Powered by **DualCache-FF (0.1.0)**, supporting automatic promotion of "cold" disk-resident data into "hot" in-memory columnar caches.
- **Read-Block Pre-fetching**: Intelligent I/O optimization that fetches subsequent data blocks to hide disk latency during sequential scans.
- **Unsafe Encapsulation**: Strictly audited `unsafe` code centralized in a dedicated core module for maximum reliability.

## 🏗 Architecture

`cdDB` is logically split into focused modules:

1.  **Dispatcher (`dispatcher.rs`)**: Central entry point for partition routing and worker registration.
2.  **Partition (`partition.rs`)**: The core actor handling the RCU state, WAL persistence, and data promotion.
3.  **Query (`query.rs`)**: High-speed query engine supporting point lookups, multi-vector links, and range scans.
4.  **Column (`column.rs`)**: Low-level Data-Oriented Design (DOD) structures for high-cache-locality storage.
5.  **Storage (`storage.rs`)**: Asynchronous disk I/O layer managing persistent entity blocks.
6.  **Unsafe Core (`unsafe_core.rs`)**: The safety boundary containing all manual pointer management and atomic operations.

## 🛠 Getting Started

### Installation

Add `cdDB` to your `Cargo.toml`:

```toml
[dependencies]
cdDB = { path = "../path/to/cdDB" }
```

### Basic Usage

```rust
use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};

#[tokio::main]
async fn main() {
    // Initialize the dispatcher with a base path for persistence
    let mut db = CdDBDispatcher::new(Some("data_dir".into()));
    
    // Register a partition
    let tx = db.register_partition("users.active".to_string());
    let route = db.get_route("users.active").unwrap();
    
    // Asynchronous Insert
    let mut attrs = Attributes::new();
    attrs.insert("score".to_string(), 100u32);
    tx.send(WriteCommand::Insert {
        entity_id: 1,
        attributes: Attributes::new(),
        attributes_int: attrs,
    }).await.unwrap();

    // Query data (Wait-Free RCU read)
    let query = Query::new(route);
    if let Some(score) = query.get_int(1, "score").await {
        println!("User score: {}", score);
    }
}
```

## 📊 Benchmarking

`cdDB` includes specialized benchmarks to validate its tiered storage performance.

### Running the Cold Data Benchmark
Measures the efficiency gain when data is promoted from Disk to Memory.
```bash
cargo test --test cold_data_benchmark -- --nocapture
```
**Latest Results:** ~28x speedup on promoted data scans.

### Running the Read Throughput Benchmark
Validates Columnar vs Struct scan performance.
```bash
cargo test --test read_benchmark -- --nocapture
```
**Latest Results:** ~7x speedup using Columnar Layout.

## 📜 License

PolyForm Noncommercial License 1.0
# cdDB: High-Performance Asynchronous Tiered Storage Engine

`cdDB` is a research-grade, high-performance storage engine built in Rust, designed for extreme concurrency and low-latency data access. It leverages modern system programming patterns such as the **Actor Model**, **Read-Copy-Update (RCU)**, and **Tiered Storage** to provide a robust foundation for data-intensive applications.

## 🚀 Key Features

- **Asynchronous Actor Architecture**: Decoupled command processing using `tokio` mpsc channels, ensuring non-blocking write paths.
- **Lock-Free Read Path**: Uses RCU (Read-Copy-Update) with **QSBR (Quiescent State Based Reclamation)** for safe, zero-lock memory management on the read path.
- **Tiered Storage Engine**: Automatic promotion of "cold" disk-resident data into "hot" in-memory columnar caches based on access patterns.
- **Columnar Layout**: Optimized for range scans and analytical queries, reducing I/O and memory pressure.
- **Write-Ahead Log (WAL)**: Robust persistence and recovery mechanism using `bincode` serialization for high-speed durability.
- **Block Fetching**: Intelligent I/O optimization that fetches neighboring entities to hide disk latency during cold scans.

## 🏗 Architecture

`cdDB` is composed of several core layers:

1.  **Dispatcher (`CdDBDispatcher`)**: The central entry point for managing partitions and routing commands.
2.  **Partition Actor**: Each data partition runs its own asynchronous loop, processing writes, deletes, and promotion requests.
3.  **Storage Layer (`AsyncStorage`)**: Manages the physical persistence of entities on disk, supporting block-based reads and synchronous writes for durability.
4.  **Query Layer**: Provides a clean, asynchronous API for point lookups and range aggregations.

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

    // Query data
    let query = Query::new(route);
    if let Some(score) = query.get_int(1, "score").await {
        println!("User score: {}", score);
    }
}
```

## 📊 Benchmarking

`cdDB` includes specialized benchmarks to validate its tiered storage performance.

### Running the Cold Data Benchmark
This benchmark measures the speed gain when data is promoted from the "Cold" (disk) layer to the "Hot" (memory) layer.

```bash
cargo test --test cold_data_benchmark -- --nocapture
```

**Expected Performance Results:**
- **Cold Disk Load**: ~40ms per 1,000 items.
- **Memory Hit (Promoted)**: ~2ms per 1,000 items.
- **Efficiency**: ~20x faster once data is cached in memory.

### Running the Read Throughput Benchmark
```bash
cargo test --test read_benchmark -- --nocapture
```

## 🧪 Testing

The test suite ensures memory safety and consistency across the asynchronous boundaries.

```bash
# Run all tests
cargo test

# Run with address sanitizer (optional, requires nightly)
RUSTFLAGS="-Z sanitizer=address" cargo test
```

## 📜 License

This project is licensed under the MIT License.

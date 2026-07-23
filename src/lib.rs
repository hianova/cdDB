#![doc = " # cdDB: High-Performance Synchronous Tiered Storage Engine"]
#![doc = ""]
#![doc = " `cdDB` is a high-performance tiered storage engine built in Rust, designed for extreme"]
#![doc = " concurrency, ultra-low latency data access, and micro-latency analytics."]
#![doc = ""]
#![doc = " ## Key Architectural Features"]
#![doc = ""]
#![doc = " - **Zero-Async Tax**: Utilizes synchronous, native OS threads and blocking I/O to avoid"]
#![doc = "   asynchronous runtime scheduler overhead."]
#![doc = " - **Wait-Free Read Path**: Employs Read-Copy-Update (RCU) with Quiescent State Based"]
#![doc = "   Reclamation (QSBR) for thread-safe, lock-free reads. A single QSBR pin covers an entire"]
#![doc = "   [`QuerySession`], so processing 1,000 queries inside one session pays exactly **one**"]
#![doc = "   enter/leave overhead — not 1,000."]
#![doc = " - **Batch Query API**: [`CdDBDispatcher::execute_batch`] and [`PartitionRoute::execute_batch`]"]
#![doc = "   are the architectural boundary between the network/session layer and the database engine."]
#![doc = "   The caller passes a `&[QueryNode]` slice; the engine processes them under a single QSBR pin"]
#![doc = "   and delivers results via a callback. The caller never touches [`WorkerState`],"]
#![doc = "   [`QuerySession`], or any QSBR primitive directly."]
#![doc = " - **Columnar Storage (DOD)**: Data is stored in column-oriented [`ColumnArray`] structures,"]
#![doc = "   grouping identical attributes contiguously in memory for maximal CPU cache locality and"]
#![doc = "   vectorized scan throughput (~1.5 Billion QPS under 4 reader threads)."]
#![doc = " - **Tiered Storage 2.0**: Powered by `DualCache-FF` for O(1) wait-free heat tracking and"]
#![doc = "   automatic promotion of cold disk-resident data into hot in-memory columnar caches (~330x"]
#![doc = "   speedup after promotion)."]
#![doc = " - **NoStd Support**: Decoupled from `std` by default, making it fully compatible with"]
#![doc = "   embedded systems and custom kernels via the [`FileSystem`] / `Executor` Platform"]
#![doc = "   Abstraction Layer."]
#![doc = " - **Write-Ahead Log (WAL)**: Supports [`WalMode::Sync`] (immediate fsync) and"]
#![doc = "   [`WalMode::Async100ms`] (adaptive group commit) for configurable durability vs. throughput."]
#![doc = ""]
#![doc = " ## Module Overview"]
#![doc = ""]
#![doc = " | Module | Description |"]
#![doc = " |--------|-------------|"]
#![doc = " | [`column`] | Core [`ColumnData`] and [`ColumnArray`] wait-free columnar storage |"]
#![doc = " | [`query`] | [`Query`], [`QuerySession`], [`QueryNode`], [`QueryResult`] — the query engine |"]
#![doc = " | [`dispatcher`] | [`CdDBDispatcher`], [`PartitionRoute`], [`UserWriter`] — top-level API |"]
#![doc = " | [`commands`] | [`WriteCommand`], [`Attributes`], [`ITOpsRecord`] — write primitives |"]
#![doc = " | [`partition`] | [`Partition`] background thread and [`MultiVectorPointer`] |"]
#![doc = " | [`qsbr`] | [`WorkerState`], [`QsbrManager`] — QSBR epoch-based memory reclamation |"]
#![doc = " | [`wal`] | [`WalProvider`], [`StdWal`], [`NoopWal`], [`WalMode`] |"]
#![doc = " | [`storage`] | [`Storage`], [`EntityData`] — append-only disk persistence |"]
#![doc = " | [`bloom`] | `SimpleBloom<N>` — const-generic lock-free bloom filter |"]
#![doc = " | [`queue`] | `BoundedQueue<T>` — MPSC wait-free bounded ring buffer |"]
#![doc = " | [`platform`] | [`FileSystem`], `Executor`, `Backoff` — Platform Abstraction Layer |"]
#![doc = " | [`unsafe_core`] | `load_ref`, `swap_ptr`, `GarbageEntry` — unsafe RCU primitives |"]
#![doc = ""]
#![doc = " ## Feature Flags"]
#![doc = ""]
#![doc = " | Feature | Default | Description |"]
#![doc = " |---------|---------|-------------|"]
#![doc = " | `std` | ✓ | Enables `StdFileSystem`, `StdExecutor`, `StdWal`, memory-mapped I/O |"]
#![doc = " | `dualcache-ff` | ✓ | Enables the `DualCache-FF` tiered cache engine |"]
#![doc = " | `async` | ✗ | Enables `execute_batch_async` (requires Tokio) |"]
#![doc = ""]
#![doc = " ## Quick Start"]
#![doc = ""]
#![doc = " ### Basic Setup (Global Static Database)"]
#![doc = ""]
#![doc = " Using the `cddb_init!` macro to generate a globally accessible, thread-safe `CdDBDispatcher`:"]
#![doc = ""]
#![doc = " ```rust,ignore"]
#![doc = " use cdDB::{cddb_init, WriteCommand, Attributes, QueryNode, QueryResult};"]
#![doc = " use std::thread;"]
#![doc = ""]
#![doc = " // 1. Create a global static dispatcher and register partitions."]
#![doc = " cddb_init!("]
#![doc = "     pub static GLOBAL_DB: 1024 = \"./data\","]
#![doc = "     partitions = [\"users\", \"orders\"]"]
#![doc = " );"]
#![doc = ""]
#![doc = " fn main() {"]
#![doc = "     // The macro returns a tuple of `(CdDBDispatcher, BTreeMap<&'static str, Arc<UserWriter>>)`"]
#![doc = "     let (db, writers) = &*GLOBAL_DB;"]
#![doc = "     let tx = writers.get(\"users\").unwrap();"]
#![doc = " ```"]
#![doc = ""]
#![doc = " ### Manual Setup"]
#![doc = ""]
#![doc = " Alternatively, initialize manually without the macro:"]
#![doc = ""]
#![doc = " ```rust,ignore"]
#![doc = " use cdDB::{CdDBDispatcher, WriteCommand, Attributes, QueryNode, QueryResult};"]
#![doc = " use std::thread;"]
#![doc = ""]
#![doc = " // 1. Create the dispatcher with a persistence directory."]
#![doc = " let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(\"./data\".into()));"]
#![doc = ""]
#![doc = " // 2. Register a partition — spawns a background worker thread."]
#![doc = " let tx = db.register_partition(\"users\".to_string());"]
#![doc = ""]
#![doc = " // 3. Insert an entity (wait-free enqueue to the partition thread)."]
#![doc = " let mut attrs_int = Attributes::new();"]
#![doc = " attrs_int.insert(\"score\".to_string(), 100u32);"]
#![doc = " tx.send(WriteCommand::Insert {"]
#![doc = "     entity_id: 1,"]
#![doc = "     attributes: Attributes::new(),"]
#![doc = "     attributes_int: attrs_int,"]
#![doc = "     attributes_blob: Attributes::new(),"]
#![doc = " }).unwrap();"]
#![doc = ""]
#![doc = " // 4. Wait for the partition to process the write."]
#![doc = " let route = db.get_route(\"users\").unwrap();"]
#![doc = " let worker = route.register_worker();"]
#![doc = " while route.len(&worker) < 1 { thread::yield_now(); }"]
#![doc = ""]
#![doc = " // 5. Batch query — the network layer passes N commands under one QSBR pin."]
#![doc = " let nodes = ["]
#![doc = "     QueryNode::Get { entity_id: 1, attr: \"score\" },"]
#![doc = "     QueryNode::Scan { attr: \"score\" },"]
#![doc = " ];"]
#![doc = " db.execute_batch(\"users\", &nodes, |result| {"]
#![doc = "     println!(\"{:?}\", result);  // Int(100), then IntList([100])"]
#![doc = " });"]
#![doc = " ```"]
#![allow(clippy::large_enum_variant)]
#![no_std]
#![allow(non_snake_case)]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;
#[cfg(all(feature = "std", feature = "dualcache-ff"))]
pub mod cache;
pub mod core;
#[cfg(feature = "std")]
pub mod engine;
pub mod io;
#[cfg(all(feature = "std", feature = "dualcache-ff"))]
pub use cache::HitCache;
#[macro_export]
macro_rules! covopt_param {
    ($ name : expr , $ default : expr , $ range : expr) => {{
        #[cfg(feature = "covopt")]
        {
            if let Ok(val_str) = ::std::env::var(concat!("COVOPT_", $name)) {
                if let Ok(val) = val_str.parse() {
                    val
                } else {
                    $default
                }
            } else {
                $default
            }
        }
        #[cfg(not(feature = "covopt"))]
        {
            $default
        }
    }};
}
pub use crate::core::AHashMap;
pub use core::column::MultiVectorPointer;
pub use core::column::{ColumnArray, Columns};
pub use core::commands::{
    Attributes, ITOpsIngest, ITOpsRecord, LogLevel, PartitionCommand, WriteCommand,
};
pub use core::qsbr::{QsbrManager, WorkerState};
pub use core::query::PartitionRoute;
pub use core::query::{AggregateOp, CdDbQuery, Query, QueryNode, QueryResult, QuerySession};
#[cfg(feature = "std")]
pub use engine::dispatcher::{CdDBDispatcher, UserWriter};
#[cfg(feature = "std")]
pub use engine::facade::{CdDBBlobStore, CdDBPartition, CdDBStore, CdDBStrStore};
#[cfg(feature = "std")]
pub use engine::partition::Partition;
#[cfg(feature = "std")]
pub use engine::simple_kv::SimpleKvStore;
#[cfg(feature = "std")]
pub use engine::reader::PartitionReader;
#[cfg(feature = "std")]
pub use engine::typed_cache::TypedCdDbCache;
pub use io::platform::FileSystem;
pub use io::storage::{EntityData, Storage};
pub use io::wal::{
    DurabilityMode, FlushConfig, FlushConfigBuilder, NoopWal, StdWal, WalMode, WalProvider,
};
pub use io::audit::{AuditService, DatabaseMetadata};
#[doc = " Convenience macro to initialize a static global CdDB database and register partitions."]
#[doc = " Returns a tuple of `(CdDBDispatcher<N>, BTreeMap<&'static str, Arc<UserWriter>>)`."]
#[doc = ""]
#[doc = " # Example"]
#[doc = " ```rust,ignore"]
#[doc = " cddb_init!("]
#[doc = "     pub static GLOBAL_DB: 1024 = \"./db_data\","]
#[doc = "     partitions = [\"users\", \"orders\"]"]
#[doc = " );"]
#[doc = " ```"]
#[cfg(feature = "std")]
#[macro_export]
macro_rules ! cddb_init { ($ vis : vis static $ name : ident : $ n : tt = $ path : expr , partitions = [$ ($ part : expr) ,* $ (,) ?]) => { $ vis static $ name : std :: sync :: LazyLock < ($ crate :: CdDBDispatcher <$ n >, std :: collections :: BTreeMap <&'static str , alloc :: sync :: Arc <$ crate :: UserWriter >>) > = std :: sync :: LazyLock :: new (|| { let mut db = $ crate :: CdDBDispatcher ::<$ n >:: new_std (Some ($ path . into ()) , $ crate :: CacheConfig :: default ()) ; let mut tx_map = std :: collections :: BTreeMap :: new () ; $ (let tx = alloc :: sync :: Arc :: new (db . register_partition (alloc :: string :: String :: from ($ part))) ; tx_map . insert ($ part , tx) ;) * (db , tx_map) }) ; } ; }
#[doc = " Configuration for the cache subsystem."]
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct CacheConfig {
    pub daemon_mode: bool,
    pub cata_tuning: bool,
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            daemon_mode: true,
            cata_tuning: false,
        }
    }
}
#[cfg(feature = "dualcache-ff")]
pub use dualcache_ff;
#[cfg(all(feature = "dualcache-ff", feature = "std"))]
pub use dualcache_ff::DualCacheFF;
#[cfg(any(not(feature = "dualcache-ff"), not(feature = "std")))]
pub use no_std_tool::dualcache_stub::DualCacheFF;
#[cfg(all(test, feature = "std"))]
mod macro_tests {
    cddb_init ! (pub static TEST_DB : 16 = "/tmp/cddb_macro_test" , partitions = ["test1" , "test2"]);
    #[test]
    fn test_cddb_init_macro() {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let (_db, writers) = &*TEST_DB;
                assert!(writers.contains_key("test1"));
                assert!(writers.contains_key("test2"));
            })
            .unwrap()
            .join()
            .unwrap();
    }
}

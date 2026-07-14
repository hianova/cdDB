//! # cdDB: High-Performance Synchronous Tiered Storage Engine
//!
//! `cdDB` is a high-performance tiered storage engine built in Rust, designed for extreme
//! concurrency, ultra-low latency data access, and micro-latency analytics.
//!
//! ## Key Architectural Features
//!
//! - **Zero-Async Tax**: Utilizes synchronous, native OS threads and blocking I/O to avoid
//!   asynchronous runtime scheduler overhead.
//! - **Wait-Free Read Path**: Employs Read-Copy-Update (RCU) with Quiescent State Based
//!   Reclamation (QSBR) for thread-safe, lock-free reads. A single QSBR pin covers an entire
//!   [`QuerySession`], so processing 1,000 queries inside one session pays exactly **one**
//!   enter/leave overhead — not 1,000.
//! - **Batch Query API**: [`CdDBDispatcher::execute_batch`] and [`PartitionRoute::execute_batch`]
//!   are the architectural boundary between the network/session layer and the database engine.
//!   The caller passes a `&[QueryNode]` slice; the engine processes them under a single QSBR pin
//!   and delivers results via a callback. The caller never touches [`WorkerState`],
//!   [`QuerySession`], or any QSBR primitive directly.
//! - **Columnar Storage (DOD)**: Data is stored in column-oriented [`ColumnArray`] structures,
//!   grouping identical attributes contiguously in memory for maximal CPU cache locality and
//!   vectorized scan throughput (~1.5 Billion QPS under 4 reader threads).
//! - **Tiered Storage 2.0**: Powered by `DualCache-FF` for O(1) wait-free heat tracking and
//!   automatic promotion of cold disk-resident data into hot in-memory columnar caches (~330x
//!   speedup after promotion).
//! - **NoStd Support**: Decoupled from `std` by default, making it fully compatible with
//!   embedded systems and custom kernels via the [`FileSystem`] / `Executor` Platform
//!   Abstraction Layer.
//! - **Write-Ahead Log (WAL)**: Supports [`WalMode::Sync`] (immediate fsync) and
//!   [`WalMode::Async100ms`] (adaptive group commit) for configurable durability vs. throughput.
//!
//! ## Module Overview
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`column`] | Core [`ColumnData`] and [`ColumnArray`] wait-free columnar storage |
//! | [`query`] | [`Query`], [`QuerySession`], [`QueryNode`], [`QueryResult`] — the query engine |
//! | [`dispatcher`] | [`CdDBDispatcher`], [`PartitionRoute`], [`UserWriter`] — top-level API |
//! | [`commands`] | [`WriteCommand`], [`Attributes`], [`ITOpsRecord`] — write primitives |
//! | [`partition`] | [`Partition`] background thread and [`MultiVectorPointer`] |
//! | [`qsbr`] | [`WorkerState`], [`QsbrManager`] — QSBR epoch-based memory reclamation |
//! | [`wal`] | [`WalProvider`], [`StdWal`], [`NoopWal`], [`WalMode`] |
//! | [`storage`] | [`Storage`], [`EntityData`] — append-only disk persistence |
//! | [`bloom`] | `SimpleBloom<N>` — const-generic lock-free bloom filter |
//! | [`queue`] | `BoundedQueue<T>` — MPSC wait-free bounded ring buffer |
//! | [`platform`] | [`FileSystem`], `Executor`, `Backoff` — Platform Abstraction Layer |
//! | [`unsafe_core`] | `load_ref`, `swap_ptr`, `GarbageEntry` — unsafe RCU primitives |
//!
//! ## Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `std` | ✓ | Enables `StdFileSystem`, `StdExecutor`, `StdWal`, memory-mapped I/O |
//! | `dualcache-ff` | ✓ | Enables the `DualCache-FF` tiered cache engine |
//! | `async` | ✗ | Enables `execute_batch_async` (requires Tokio) |
//!
//! ## Quick Start
//!
//! ### Basic Setup (Global Static Database)
//!
//! Using the `cddb_init!` macro to generate a globally accessible, thread-safe `CdDBDispatcher`:
//!
//! ```rust,ignore
//! use cdDB::{cddb_init, WriteCommand, Attributes, QueryNode, QueryResult};
//! use std::thread;
//!
//! // 1. Create a global static dispatcher and register partitions.
//! cddb_init!(
//!     pub static GLOBAL_DB: 1024 = "./data",
//!     partitions = ["users", "orders"]
//! );
//! 
//! fn main() {
//!     // The macro returns a tuple of `(CdDBDispatcher, BTreeMap<&'static str, Arc<UserWriter>>)`
//!     let (db, writers) = &*GLOBAL_DB;
//!     let tx = writers.get("users").unwrap();
//! ```
//!
//! ### Manual Setup
//! 
//! Alternatively, initialize manually without the macro:
//! 
//! ```rust,ignore
//! use cdDB::{CdDBDispatcher, WriteCommand, Attributes, QueryNode, QueryResult};
//! use std::thread;
//!
//! // 1. Create the dispatcher with a persistence directory.
//! let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some("./data".into()));
//!
//! // 2. Register a partition — spawns a background worker thread.
//! let tx = db.register_partition("users".to_string());
//!
//! // 3. Insert an entity (wait-free enqueue to the partition thread).
//! let mut attrs_int = Attributes::new();
//! attrs_int.insert("score".to_string(), 100u32);
//! tx.send(WriteCommand::Insert {
//!     entity_id: 1,
//!     attributes: Attributes::new(),
//!     attributes_int: attrs_int,
//!     attributes_blob: Attributes::new(),
//! }).unwrap();
//!
//! // 4. Wait for the partition to process the write.
//! let route = db.get_route("users").unwrap();
//! let worker = route.register_worker();
//! while route.len(&worker) < 1 { thread::yield_now(); }
//!
//! // 5. Batch query — the network layer passes N commands under one QSBR pin.
//! let nodes = [
//!     QueryNode::Get { entity_id: 1, attr: "score" },
//!     QueryNode::Scan { attr: "score" },
//! ];
//! db.execute_batch("users", &nodes, |result| {
//!     println!("{:?}", result);  // Int(100), then IntList([100])
//! });
//! ```

#![no_std]
#![allow(non_snake_case)]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub mod core;
#[cfg(feature = "std")]
pub mod engine;
pub mod io;
#[cfg(all(feature = "std", feature = "dualcache-ff"))]
pub mod cache;

#[cfg(feature = "std")]
pub mod ml;
#[cfg(feature = "std")]
pub mod agent;

#[cfg(all(feature = "std", feature = "dualcache-ff"))]
pub use cache::HitCache;

// Re-export public types for API compatibility
pub use core::column::{ColumnArray, Columns};
pub use core::commands::{
    Attributes, ITOpsIngest, ITOpsRecord, LogLevel, PartitionCommand, WriteCommand,
};
pub use core::qsbr::{QsbrManager, WorkerState};
pub use core::query::{AggregateOp, CdDbQuery, Query, QueryNode, QueryResult, QuerySession};

#[cfg(feature = "std")]
pub use engine::dispatcher::{CdDBDispatcher, UserWriter};
#[cfg(feature = "std")]
pub use engine::partition::Partition;

pub use core::column::MultiVectorPointer;
pub use core::query::PartitionRoute;
#[cfg(feature = "std")]
pub use engine::facade::{CdDBBlobStore, CdDBPartition, CdDBStore, CdDBStrStore};
#[cfg(feature = "std")]
pub use engine::simple_kv::SimpleKvStore;

pub use io::platform::FileSystem;
pub use io::storage::{EntityData, Storage};
pub use io::wal::{
    DurabilityMode, FlushConfig, FlushConfigBuilder, NoopWal, StdWal, WalMode, WalProvider,
};

pub use crate::core::AHashMap;

/// Convenience macro to initialize a static global CdDB database and register partitions.
/// Returns a tuple of `(CdDBDispatcher<N>, BTreeMap<&'static str, Arc<UserWriter>>)`.
///
/// # Example
/// ```rust,ignore
/// cddb_init!(
///     pub static GLOBAL_DB: 1024 = "./db_data",
///     partitions = ["users", "orders"]
/// );
/// ```
#[cfg(feature = "std")]
#[macro_export]
macro_rules! cddb_init {
    (
        $vis:vis static $name:ident : $n:tt = $path:expr, partitions = [ $( $part:expr ),* $(,)? ]
    ) => {
        $vis static $name: std::sync::LazyLock<(
            $crate::CdDBDispatcher<$n>, 
            std::collections::BTreeMap<&'static str, alloc::sync::Arc<$crate::UserWriter>>
        )> = std::sync::LazyLock::new(|| {
            let mut db = $crate::CdDBDispatcher::<$n>::new_std(
                Some($path.into()), 
                $crate::CacheConfig::default()
            );
            let mut tx_map = std::collections::BTreeMap::new();
            $(
                let tx = alloc::sync::Arc::new(db.register_partition(alloc::string::String::from($part)));
                tx_map.insert($part, tx);
            )*
            (db, tx_map)
        });
    };
}

/// Configuration for the cache subsystem.
#[derive(Debug, Clone, Copy)]
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


#[cfg(any(not(feature = "dualcache-ff"), not(feature = "std")))]
mod dualcache_stub {
    #[derive(Clone, Debug)]
    pub struct DualCacheFF<
        K,
        V,
        P,
        const C2: usize,
        const C1: usize,
        const C0: usize,
        const TC: usize,
        const P4: usize = 0,
        const P5: usize = 0,
        const P6: usize = 0,
    > {
        _marker: core::marker::PhantomData<(K, V, P)>,
    }

    unsafe impl<
        K,
        V,
        P,
        const C2: usize,
        const C1: usize,
        const C0: usize,
        const TC: usize,
        const P4: usize,
        const P5: usize,
        const P6: usize,
    > Send for DualCacheFF<K, V, P, C2, C1, C0, TC, P4, P5, P6>
    {
    }
    unsafe impl<
        K,
        V,
        P,
        const C2: usize,
        const C1: usize,
        const C0: usize,
        const TC: usize,
        const P4: usize,
        const P5: usize,
        const P6: usize,
    > Sync for DualCacheFF<K, V, P, C2, C1, C0, TC, P4, P5, P6>
    {
    }

    impl<
        K,
        V,
        P,
        const C2: usize,
        const C1: usize,
        const C0: usize,
        const TC: usize,
        const P4: usize,
        const P5: usize,
        const P6: usize,
    > DualCacheFF<K, V, P, C2, C1, C0, TC, P4, P5, P6>
    {
        pub fn new(_config: crate::CacheConfig) -> Self {
            Self {
                _marker: core::marker::PhantomData,
            }
        }
    }
}

#[cfg(all(feature = "dualcache-ff", feature = "std"))]
pub use dualcache_ff::DualCacheFF;

#[cfg(feature = "dualcache-ff")]
pub use dualcache_ff;

#[cfg(any(not(feature = "dualcache-ff"), not(feature = "std")))]
pub use dualcache_stub::DualCacheFF;

#[cfg(all(test, feature = "std"))]
mod macro_tests {
    cddb_init!(
        pub static TEST_DB: 16 = "/tmp/cddb_macro_test",
        partitions = ["test1", "test2"]
    );

    #[test]
    #[ignore]
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

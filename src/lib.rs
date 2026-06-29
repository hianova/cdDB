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

pub use io::platform::FileSystem;
pub use io::storage::{EntityData, Storage};
pub use io::wal::{
    DurabilityMode, FlushConfig, FlushConfigBuilder, NoopWal, StdWal, WalMode, WalProvider,
};

pub use crate::core::AHashMap;

#[cfg(not(feature = "dualcache-ff"))]
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
    > {
        _marker: core::marker::PhantomData<(K, V, P)>,
    }

    unsafe impl<K, V, P, const C2: usize, const C1: usize, const C0: usize, const TC: usize> Send
        for DualCacheFF<K, V, P, C2, C1, C0, TC>
    {
    }
    unsafe impl<K, V, P, const C2: usize, const C1: usize, const C0: usize, const TC: usize> Sync
        for DualCacheFF<K, V, P, C2, C1, C0, TC>
    {
    }

    impl<K, V, P, const C2: usize, const C1: usize, const C0: usize, const TC: usize>
        DualCacheFF<K, V, P, C2, C1, C0, TC>
    {
        pub fn new(_config: ()) -> Self {
            Self {
                _marker: core::marker::PhantomData,
            }
        }
    }
}

#[cfg(feature = "dualcache-ff")]
#[cfg(feature = "std")]
pub use dualcache_ff::DualCacheFF;

#[cfg(feature = "dualcache-ff")]
#[cfg(not(feature = "std"))]
pub use dualcache_ff::StaticDualCache as DualCacheFF;

#[cfg(not(feature = "dualcache-ff"))]
pub use dualcache_stub::DualCacheFF;

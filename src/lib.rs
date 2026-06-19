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
//!   `QuerySession`, so processing 1,000 queries inside one session pays exactly **one**
//!   enter/leave overhead — not 1,000.
//! - **Batch Query API**: `CdDBDispatcher::execute_batch` and `PartitionRoute::execute_batch`
//!   are the architectural boundary between the network/session layer and the database engine.
//!   The caller passes an array of `QueryNode` commands; the engine processes them under a
//!   single QSBR pin and delivers results via a callback. The caller never touches `WorkerState`,
//!   `QuerySession`, or any QSBR primitive directly.
//! - **NoStd Support**: Decoupled from `std` by default, making it fully compatible with
//!   embedded systems and custom kernels via a Platform Abstraction Layer.
//! - **Tiered Storage 2.0**: Powered by `DualCache-FF` for O(1) wait-free heat tracking and
//!   automatic promotion of cold disk-resident data into hot in-memory columnar caches.
//!
//! ## Batch Query Example
//!
//! ```rust,ignore
//! use cdDB::{CdDBDispatcher, WriteCommand, Attributes, QueryNode, QueryResult};
//!
//! let mut db = CdDBDispatcher::new_std(None);
//! let _tx = db.register_partition("users".to_string());
//!
//! // The network layer: does NOT know about QSBR, WorkerState, or QuerySession.
//! // It simply assembles a slice of commands and calls execute_batch.
//! let nodes = [
//!     QueryNode::Get { entity_id: 1, attr: "score" },
//!     QueryNode::Get { entity_id: 2, attr: "score" },
//! ];
//! db.execute_batch("users", &nodes, |result| {
//!     // process result — entire batch runs under one QSBR pin
//!     println!("{:?}", result);
//! });
//! ```

#![no_std]
#![allow(non_snake_case)]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub mod platform;
pub mod sync;
pub mod qsbr;
mod storage;
pub mod unsafe_core;
pub mod queue;
pub mod column;
pub mod commands;
mod partition;
mod query;
mod dispatcher;
mod bloom;
pub mod wal;
#[cfg(not(feature = "dualcache-ff"))]
mod dualcache_stub {
    #[derive(Clone, Debug)]
    pub struct DualCacheFF<K, V> {
        _marker: core::marker::PhantomData<(K, V)>,
    }

    unsafe impl<K, V> Send for DualCacheFF<K, V> {}
    unsafe impl<K, V> Sync for DualCacheFF<K, V> {}

    #[derive(Clone, Debug)]
    pub struct Config;

    impl Config {
        pub fn with_memory_budget(_budget: usize, _percent: usize) -> Self {
            Self
        }
    }

    impl<K, V> DualCacheFF<K, V> {
        pub fn new(_config: Config) -> Self {
            Self {
                _marker: core::marker::PhantomData,
            }
        }

        pub fn new_headless(_config: Config) -> (Self, ()) {
            (Self::new(_config), ())
        }

        pub fn insert(&self, _key: K, _value: V) {}
        pub fn remove(&self, _key: &K) -> Option<V> {
            None
        }
        pub fn get(&self, _key: &K) -> Option<&V> {
            None
        }
    }
}

// Re-export public types for API compatibility
pub use column::{Columns, ColumnArray};
pub use commands::{Attributes, WriteCommand, PartitionCommand};
pub use partition::{MultiVectorPointer, Partition};
pub use query::{QueryNode, AggregateOp, CdDbQuery, QueryResult, Query, QuerySession};
pub use dispatcher::{CdDBDispatcher, PartitionRoute};
#[cfg(feature = "std")]
pub use dispatcher::UserWriter;
pub use qsbr::{QsbrManager, WorkerState};
pub use storage::{Storage, EntityData};
pub use commands::{ITOpsRecord, LogLevel, ITOpsIngest};
pub use wal::{WalProvider, StdWal, NoopWal, WalMode};
pub use platform::FileSystem;

#[cfg(feature = "dualcache-ff")]
#[cfg(feature = "std")]
pub use dualcache_ff::{DualCacheFF, Config};

#[cfg(feature = "dualcache-ff")]
#[cfg(not(feature = "std"))]
pub use dualcache_ff::{StaticDualCache as DualCacheFF, Config};

#[cfg(not(feature = "dualcache-ff"))]
pub use dualcache_stub::{DualCacheFF, Config};

pub type AHashMap<K, V> = hashbrown::HashMap<K, V>;

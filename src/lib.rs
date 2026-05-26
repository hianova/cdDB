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
pub mod qsbr;
mod storage;
mod unsafe_core;
mod column;
pub mod commands;
mod partition;
mod query;
mod dispatcher;
mod ops;
mod bloom;
mod wal;

#[cfg(not(feature = "dualcache-ff"))]
mod dualcache_stub;

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
pub use ops::{ITOpsRecord, LogLevel, ITOpsIngest};
pub use wal::{WalProvider, StdWal, NoopWal};
pub use platform::FileSystem;

#[cfg(feature = "dualcache-ff")]
pub use dualcache_ff::{DualCacheFF, Config};
#[cfg(not(feature = "dualcache-ff"))]
pub use dualcache_stub::{DualCacheFF, Config};

pub type AHashMap<K, V> = hashbrown::HashMap<K, V>;

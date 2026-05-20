//! # cdDB: High-Performance Synchronous Tiered Storage Engine
//!
//! `cdDB` is a high-performance tiered storage engine built in Rust, designed for extreme concurrency,
//! ultra-low latency data access, and micro-latency analytics.
//!
//! ## Key Architectural Features
//! - **Zero-Async Tax**: Utilizes synchronous, native OS threads and blocking I/O to avoid asynchronous runtime scheduler overhead.
//! - **Wait-Free Read Path**: Employs Read-Copy-Update (RCU) with Quiescent State Based Reclamation (QSBR) for thread-safe lock-free reads.
//! - **NoStd Support**: Decoupled from `std` by default, making it fully compatible with embedded systems and custom kernels.
//! - **Tiered Storage 2.0**: Powered by `DualCache-FF` for O(1) heat tracking and automatic promotion of cold data to memory.

#![no_std]
#![allow(non_snake_case)]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

mod platform;
mod qsbr;
mod storage;
mod unsafe_core;
mod column;
mod commands;
mod partition;
mod query;
mod dispatcher;
mod ops;
mod bloom;
mod wal;

// Re-export public types for API compatibility
pub use column::{Columns, ColumnArray};
pub use commands::{Attributes, WriteCommand, PartitionCommand};
pub use partition::{MultiVectorPointer, Partition};
pub use query::{QueryNode, AggregateOp, CdDbQuery, QueryResult, Query};
pub use dispatcher::{CdDBDispatcher, PartitionRoute};
#[cfg(feature = "std")]
pub use dispatcher::UserWriter;
pub use qsbr::{QsbrManager, WorkerState};
pub use storage::{Storage, EntityData};
pub use ops::{ITOpsRecord, LogLevel, ITOpsIngest};
pub use wal::{WalProvider, StdWal, NoopWal};

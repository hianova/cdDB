#![no_std]
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

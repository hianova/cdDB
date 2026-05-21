use ahash::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use alloc::format;
use crate::query::{Query, QueryNode, QueryResult};
use core::sync::atomic::AtomicPtr;

#[cfg(feature = "std")]
use std::sync::{Mutex, mpsc::Sender};
#[cfg(not(feature = "std"))]
use spin::Mutex;

use crate::bloom::SimpleBloom;
use dualcache_ff::DualCacheFF;

use crate::column::{Columns, ColumnArray};
use crate::commands::{PartitionCommand, WriteCommand};
use crate::partition::{MultiVectorPointer, Partition};
use crate::qsbr::WorkerState;
use crate::storage::Storage;
use crate::unsafe_core::new_atomic_ptr;
use crate::platform::{FileSystem, ThreadManager};
use crate::wal::{WalProvider, StdWal, NoopWal};

/// 4. cdDB 全域入口與調度器 (Dispatcher)
pub struct CdDBDispatcher {
    pub route_table: AHashMap<String, PartitionRoute>,
    pub base_path: Option<String>,
    pub workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
    pub fs: Arc<dyn FileSystem>,
    pub thread_manager: Arc<dyn ThreadManager>,
}

impl CdDBDispatcher {
    pub fn new(
        base_path: Option<String>,
        fs: Arc<dyn FileSystem>,
        thread_manager: Arc<dyn ThreadManager>,
    ) -> Self {
        Self {
            route_table: AHashMap::new(),
            base_path,
            workers: Arc::new(Mutex::new(Vec::new())),
            fs,
            thread_manager,
        }
    }

    #[cfg(feature = "std")]
    pub fn new_std(base_path: Option<String>) -> Self {
        Self::new(
            base_path,
            Arc::new(crate::platform::StdFileSystem),
            Arc::new(crate::platform::StdThreadManager),
        )
    }

    #[cfg(feature = "std")]
    pub fn register_partition(&mut self, path: String) -> UserWriter {
        self.register_partition_with_wal(path, None)
    }

    #[cfg(feature = "std")]
    pub fn register_partition_with_budget(
        &mut self,
        path: String,
        _budget_bytes: usize,
    ) -> UserWriter {
        self.register_partition_with_wal(path, None)
    }

    #[cfg(feature = "std")]
    pub fn register_partition_with_wal(
        &mut self,
        path: String,
        wal_path: Option<String>,
    ) -> UserWriter {
        let wal: Arc<dyn WalProvider> = if let Some(p) = wal_path {
            Arc::new(StdWal::new(p, self.fs.clone()))
        } else {
            Arc::new(NoopWal)
        };
        self.register_partition_with_wal_provider(path, wal)
    }

    #[cfg(feature = "std")]
    pub fn register_partition_with_wal_provider(
        &mut self,
        path: String,
        wal: Arc<dyn WalProvider>,
    ) -> UserWriter {
        let (tx, rx) = std::sync::mpsc::channel();
        let writer_tx_out = tx.clone();
        
        let (storage_path, shared_pointers, bloom_filter, columns, hot_index, workers) = self.init_partition_state(&path);
        
        let route = PartitionRoute {
            writer_tx: tx,
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&hot_index),
            bloom_filter: Arc::clone(&bloom_filter),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::clone(&wal),
        };
        
        self.route_table.insert(path.clone(), route);
 
        self.spawn_partition_thread(
            rx,
            columns,
            wal,
            workers,
            storage_path,
            shared_pointers,
            bloom_filter,
        );
 
        UserWriter(writer_tx_out)
    }

    #[cfg(not(feature = "std"))]
    pub fn register_partition_no_std(
        &mut self,
        path: String,
        writer_tx: Arc<dyn crate::platform::MessageSender>,
        _writer_rx: alloc::boxed::Box<dyn crate::platform::MessageQueue>,
    ) {
        let (storage_path, shared_pointers, bloom_filter, columns, hot_index, workers) = self.init_partition_state(&path);
        
        let route = PartitionRoute {
            writer_tx,
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&hot_index),
            bloom_filter: Arc::clone(&bloom_filter),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::new(NoopWal),
        };
        
        self.route_table.insert(path, route);
        
        // In no_std, the user must manage the thread/loop for the Partition
    }

    fn init_partition_state(&self, path: &str) -> (String, Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>, Arc<Mutex<SimpleBloom>>, Arc<AtomicPtr<Columns>>, Arc<DualCacheFF<usize, ()>>, Arc<Mutex<Vec<Arc<WorkerState>>>>) {
        let storage_path = self
            .base_path
            .as_ref()
            .map(|base| format!("{}/{}.data", base, path.replace('.', "/")))
            .unwrap_or_else(|| format!("data/{}", path));

        let _ = self.fs.create_dir_all(&storage_path);

        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::new()));
        let bloom_filter = Arc::new(Mutex::new(SimpleBloom::new(1024 * 1024)));
        let columns = Arc::new(new_atomic_ptr(Columns::new()));
        let hot_index = Arc::new(DualCacheFF::new(dualcache_ff::Config::with_memory_budget(100, 60)));
        let workers = Arc::new(Mutex::new(Vec::new()));
        
        (storage_path, shared_pointers, bloom_filter, columns, hot_index, workers)
    }

    #[cfg(feature = "std")]
    fn spawn_partition_thread(
        &self,
        rx: std::sync::mpsc::Receiver<PartitionCommand>,
        columns: Arc<AtomicPtr<Columns>>,
        wal: Arc<dyn WalProvider>,
        workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
        storage_path: String,
        shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        bloom_filter: Arc<Mutex<SimpleBloom>>,
    ) {
        let fs_rt = self.fs.clone();
        let wal_rt = wal.clone();
        
        self.thread_manager.spawn(alloc::boxed::Box::new(move || {
            let mut partition = Partition::new(
                alloc::boxed::Box::new(crate::platform::StdMessageQueue { rx: Mutex::new(rx) }),
                columns,
                wal_rt.clone(),
                workers,
                storage_path,
                fs_rt,
                shared_pointers,
                bloom_filter,
            );

            partition.replay_wal();
            partition.run();
        }));
    }

    pub fn get_route(&self, path: &str) -> Option<&PartitionRoute> {
        self.route_table.get(path)
    }

    /// Execute a batch of query nodes against a named partition under a single
    /// QSBR pin. The network / session layer does not need to know about QSBR,
    /// `Query`, or `WorkerState` — simply pass the slice and a result callback.
    ///
    /// This is the architectural boundary described in the cdDB design spec:
    /// the caller (e.g. a TCP stream handler parsing a Redis pipeline) hands
    /// `N` commands as an array and pays exactly **one** QSBR enter/leave.
    #[cfg(feature = "std")]
    pub fn execute_batch<'b, F>(
        &self,
        partition: &str,
        nodes: &[QueryNode<'b>],
        mut cb: F,
    ) where
        F: FnMut(QueryResult),
    {
        if let Some(route) = self.route_table.get(partition) {
            let q = Query::new(route);
            q.execute_with_cb(nodes, &mut cb);
        }
    }
}

#[cfg(feature = "std")]
impl Default for CdDBDispatcher {
    fn default() -> Self {
        Self::new_std(None)
    }
}

#[cfg(feature = "std")]
pub struct UserWriter(Sender<PartitionCommand>);
#[cfg(feature = "std")]
impl UserWriter {
    pub fn send(&self, cmd: WriteCommand) -> Result<(), std::sync::mpsc::SendError<PartitionCommand>> {
        self.0.send(PartitionCommand::Write(cmd))
    }
}

#[derive(Clone)]
pub struct PartitionRoute {
    #[cfg(feature = "std")]
    pub writer_tx: Sender<PartitionCommand>,
    #[cfg(not(feature = "std"))]
    pub writer_tx: Arc<dyn crate::platform::MessageSender>,
    pub columns: Arc<AtomicPtr<Columns>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub hot_index: Arc<DualCacheFF<usize, ()>>,
    pub bloom_filter: Arc<Mutex<SimpleBloom>>,
    pub storage: Arc<Storage>,
    pub workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
    pub wal: Arc<dyn WalProvider>,
}

impl PartitionRoute {
    pub fn get_snapshot(&self) -> AHashMap<usize, MultiVectorPointer> {
        crate::unsafe_core::load_clone(&self.shared_pointers)
    }

    pub fn register_worker(&self) -> Arc<WorkerState> {
        let worker = Arc::new(WorkerState::new());
        #[cfg(feature = "std")]
        let mut workers = self.workers.lock().unwrap();
        #[cfg(not(feature = "std"))]
        let mut workers = self.workers.lock();
        workers.push(Arc::clone(&worker));
        worker
    }

    /// Look up a string column by name.
    ///
    /// **Caller contract**: this must be invoked while the calling thread is
    /// already within a QSBR-pinned region (i.e. inside a `QuerySession`, or
    /// after a manual `worker.enter()` call). The method itself does **not**
    /// call `enter()`/`leave()` — doing so inside an already-pinned session
    /// would cause spurious double epoch-writes on the worker's `local_epoch`
    /// cache line, degrading coherency under multi-thread read pressure.
    pub fn get_column_str(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<String>>> {
        let cols = crate::unsafe_core::load_ref(&self.columns);
        cols.str_cols.get(name).cloned()
    }

    /// Look up an integer column by name.
    ///
    /// See `get_column_str` for the caller QSBR contract.
    pub fn get_column_int(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<u32>>> {
        let cols = crate::unsafe_core::load_ref(&self.columns);
        cols.int_cols.get(name).cloned()
    }

    /// Look up a blob column by name.
    ///
    /// See `get_column_str` for the caller QSBR contract.
    pub fn get_column_blob(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<Vec<u8>>>> {
        let cols = crate::unsafe_core::load_ref(&self.columns);
        cols.blob_cols.get(name).cloned()
    }

    /// Return the number of entities currently resident in memory.
    ///
    /// See `get_column_str` for the caller QSBR contract.
    pub fn len(&self, _worker: &WorkerState) -> usize {
        let snap = crate::unsafe_core::load_ref(&self.shared_pointers);
        snap.len()
    }

    /// Execute a batch of query nodes under a single QSBR pin.
    ///
    /// This is the primary API for callers that process multiple queries
    /// at once (e.g. a network session handling a Redis pipeline). The
    /// caller does not need to know about `WorkerState` or QSBR epochs.
    pub fn execute_batch<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult),
    {
        let q = Query::new(self);
        q.execute_with_cb(nodes, cb);
    }
}

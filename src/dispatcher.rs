use crate::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use alloc::format;
use crate::query::{Query, QueryNode, QueryResult};
use crate::sync::atomic::AtomicPtr;

#[cfg(feature = "std")]
use crate::queue::BoundedQueue;

use crate::bloom::SimpleBloom;
use crate::{DualCacheFF, Config};

use crate::column::{Columns, ColumnArray};
use crate::partition::MultiVectorPointer;
#[cfg(feature = "std")]
use crate::partition::Partition;
use crate::qsbr::{WorkerState, WorkerNode};
use crate::storage::Storage;
use crate::unsafe_core::new_atomic_ptr;
use crate::platform::{FileSystem, Executor};
use crate::wal::{WalProvider, NoopWal};
#[cfg(feature = "std")]
use crate::wal::StdWal;
#[cfg(feature = "std")]
use crate::commands::{PartitionCommand, WriteCommand};

/// 4. cdDB 全域入口與調度器 (Dispatcher)
pub struct CdDBDispatcher<const N: usize> {
    pub route_table: AHashMap<String, Arc<PartitionRoute<N>>>,
    pub base_path: Option<String>,
    pub workers: Arc<AtomicPtr<WorkerNode>>,
    pub fs: Arc<dyn FileSystem>,
    pub executor: Arc<dyn Executor>,
    pub global_cache: Arc<DualCacheFF<(u32, usize), ()>>,
    pub next_partition_id: u32,
}

impl<const N: usize> CdDBDispatcher<N> {
    pub fn new(
        base_path: Option<String>,
        fs: Arc<dyn FileSystem>,
        executor: Arc<dyn Executor>,
    ) -> Self {
        Self {
            route_table: AHashMap::default(),
            base_path,
            workers: Arc::new(crate::sync::atomic::AtomicPtr::new(core::ptr::null_mut())),
            fs,
            executor,
            global_cache: Arc::new(DualCacheFF::new(Config::with_memory_budget(100, 60))),
            next_partition_id: 0,
        }
    }

    #[cfg(feature = "std")]
    pub fn new_std(base_path: Option<String>) -> Self {
        Self::new(
            base_path,
            Arc::new(crate::platform::StdFileSystem),
            Arc::new(crate::platform::StdExecutor),
        )
    }

    #[cfg(feature = "std")]
    pub fn register_partition(&mut self, path: String) -> UserWriter {
        self.register_partition_with_wal(path, None, crate::wal::WalMode::Sync)
    }

    #[cfg(feature = "std")]
    pub fn register_partition_with_budget(
        &mut self,
        path: String,
        _budget_bytes: usize,
    ) -> UserWriter {
        self.register_partition_with_wal(path, None, crate::wal::WalMode::Sync)
    }

    #[cfg(feature = "std")]
    pub fn register_partition_with_wal(
        &mut self,
        path: String,
        wal_path: Option<String>,
        wal_mode: crate::wal::WalMode,
    ) -> UserWriter {
        let wal: Arc<dyn WalProvider> = if let Some(p) = wal_path {
            Arc::new(StdWal::new(p, self.fs.clone(), wal_mode))
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
        let queue = Arc::new(BoundedQueue::new(262144));
        let writer_tx_out = queue.clone();
        
        let partition_id = self.next_partition_id;
        self.next_partition_id += 1;
        let (storage_path, shared_pointers, bloom, columns, workers) = self.init_partition_state(&path);
        
        let route = Arc::new(PartitionRoute {
            name: path.clone(),
            partition_id,
            writer_tx: queue.clone(),
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&self.global_cache),
            bloom_filter: Arc::clone(&bloom),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::clone(&wal),
        });
        
        self.route_table.insert(path.clone(), route);
 
        self.spawn_partition_thread(
            queue,
            columns,
            wal,
            workers,
            storage_path,
            shared_pointers,
            bloom,
            partition_id,
            self.global_cache.clone(),
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
        let partition_id = self.next_partition_id;
        self.next_partition_id += 1;
        let (storage_path, shared_pointers, bloom, columns, workers) = self.init_partition_state(&path);
        
        let route = Arc::new(PartitionRoute {
            name: path.clone(),
            partition_id,
            writer_tx,
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&self.global_cache),
            bloom_filter: Arc::clone(&bloom),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::new(NoopWal),
        });
        
        self.route_table.insert(path, route);
        
        // In no_std, the user must manage the thread/loop for the Partition
    }

    fn init_partition_state(&self, path: &str) -> (String, Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>, Arc<AtomicPtr<SimpleBloom<N>>>, Arc<AtomicPtr<Columns<N>>>, Arc<AtomicPtr<WorkerNode>>) {
        let storage_path = self
            .base_path
            .as_ref()
            .map(|base| format!("{}/{}.data", base, path.replace('.', "/")))
            .unwrap_or_else(|| format!("data/{}", path));

        let _ = self.fs.create_dir_all(&storage_path);

        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::default()));
        let bloom = Arc::new(new_atomic_ptr(SimpleBloom::<N>::new()));
        let columns = Arc::new(new_atomic_ptr(Columns::<N>::new()));
        let workers = Arc::new(crate::sync::atomic::AtomicPtr::new(core::ptr::null_mut()));
        
        (storage_path, shared_pointers, bloom, columns, workers)
    }

    #[cfg(feature = "std")]
    fn spawn_partition_thread(
        &self,
        rx: Arc<BoundedQueue<PartitionCommand>>,
        columns: Arc<AtomicPtr<Columns<N>>>,
        wal: Arc<dyn WalProvider>,
        workers: Arc<AtomicPtr<WorkerNode>>,
        storage_path: String,
        shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,
        partition_id: u32,
        hot_index: Arc<DualCacheFF<(u32, usize), ()>>,
    ) {
        let fs_rt = self.fs.clone();
        let wal_rt = wal.clone();
        
        self.executor.spawn_task(alloc::boxed::Box::new(move || {
            let mut partition = Partition::new(
                alloc::boxed::Box::new(crate::platform::StdMessageQueue { rx }),
                columns,
                wal_rt.clone(),
                workers,
                storage_path,
                fs_rt,
                shared_pointers,
                bloom_filter,
                hot_index,
                partition_id,
            );

            partition.replay_wal();
            partition.run();
        }));
    }

    pub fn get_route(&self, partition_name: &str) -> Option<Arc<PartitionRoute<N>>> {
        self.route_table.get(partition_name).cloned()
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

    /// Execute a batch of query nodes asynchronously.
    #[cfg(all(feature = "std", feature = "async"))]
    pub async fn execute_batch_async<'b, F, R>(
        &self,
        partition: String,
        nodes: Vec<QueryNode<'static>>,
        mut cb: F,
    ) -> Option<R>
    where
        F: FnMut(QueryResult) -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Some(route) = self.route_table.get(&partition).cloned() {
            
            tokio::task::spawn_blocking(move || {
                let q = Query::new(&route);
                let mut last_res = None;
                q.execute_with_cb(&nodes, |res| {
                    last_res = Some(cb(res));
                });
                last_res
            }).await.unwrap()
        } else {
            None
        }
    }
}

#[cfg(feature = "std")]
impl<const N: usize> Default for CdDBDispatcher<N> {
    fn default() -> Self {
        Self::new_std(None)
    }
}

#[cfg(feature = "std")]
pub struct UserWriter(Arc<BoundedQueue<PartitionCommand>>);
#[cfg(feature = "std")]
impl UserWriter {
    pub fn send(&self, cmd: WriteCommand) -> Result<(), &'static str> {
        let mut cmd = PartitionCommand::Write(cmd);
        let mut backoff = crate::platform::Backoff::new();
        loop {
            match self.0.push(cmd) {
                Ok(()) => return Ok(()),
                Err(c) => {
                    cmd = c;
                    if backoff.is_completed() {
                        std::thread::yield_now();
                    } else {
                        backoff.snooze();
                    }
                }
            }
        }
    }
    pub fn try_send(&self, cmd: WriteCommand) -> Result<(), &'static str> {
        self.0.push(PartitionCommand::Write(cmd)).map_err(|_| "Full")
    }
}

#[cfg(feature = "std")]
impl Drop for UserWriter {
    fn drop(&mut self) {
        // Send shutdown command on drop to gracefully terminate the background partition thread
        let mut retries = 0;
        while self.0.push(PartitionCommand::Shutdown).is_err() && retries < 100 {
            retries += 1;
        }
    }
}

#[derive(Clone)]
pub struct PartitionRoute<const N: usize> {
    pub name: String,
    pub partition_id: u32,
    #[cfg(feature = "std")]
    pub writer_tx: Arc<BoundedQueue<PartitionCommand>>,
    #[cfg(not(feature = "std"))]
    pub writer_tx: Arc<dyn crate::platform::MessageSender>,
    pub columns: Arc<AtomicPtr<Columns<N>>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub hot_index: Arc<DualCacheFF<(u32, usize), ()>>,
    pub bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,
    pub storage: Arc<Storage>,
    pub workers: Arc<AtomicPtr<WorkerNode>>,
    pub wal: Arc<dyn WalProvider>,
}

impl<const N: usize> PartitionRoute<N> {
    pub fn get_snapshot(&self) -> AHashMap<usize, MultiVectorPointer> {
        crate::unsafe_core::load_clone(&self.shared_pointers)
    }

    pub fn register_worker(&self) -> Arc<WorkerState> {
        let worker = Arc::new(WorkerState::new());
        let new_node = alloc::boxed::Box::into_raw(alloc::boxed::Box::new(crate::qsbr::WorkerNode {
            worker: Arc::clone(&worker),
            next: crate::sync::atomic::AtomicPtr::new(core::ptr::null_mut()),
        }));
        loop {
            let head = self.workers.load(crate::sync::atomic::Ordering::Acquire);
            unsafe { crate::unsafe_core::link_node(new_node, |n| &n.next, head); }
            if self.workers.compare_exchange(
                head,
                new_node,
                crate::sync::atomic::Ordering::Release,
                crate::sync::atomic::Ordering::Relaxed,
            ).is_ok() {
                break;
            }
        }
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
    ) -> Option<Arc<ColumnArray<String, N>>> {
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
    ) -> Option<Arc<ColumnArray<u32, N>>> {
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
    ) -> Option<Arc<ColumnArray<Vec<u8>, N>>> {
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

    /// Trigger a synchronous WAL flush to durable storage
    pub fn flush_wal(&self) -> Result<(), String> {
        self.wal.checkpoint()
    }
}

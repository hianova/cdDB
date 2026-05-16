use ahash::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use alloc::format;
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
        let (tx, rx) = std::sync::mpsc::channel();
        let writer_tx_out = tx.clone();
        
        let (storage_path, shared_pointers, bloom_filter, columns, hot_index, workers) = self.init_partition_state(&path, wal_path.clone());
        
        let route = PartitionRoute {
            writer_tx: tx,
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&hot_index),
            bloom_filter: Arc::clone(&bloom_filter),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
        };
        
        self.route_table.insert(path.clone(), route);

        self.spawn_partition_thread(
            rx,
            columns,
            wal_path,
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
        let (storage_path, shared_pointers, bloom_filter, columns, hot_index, workers) = self.init_partition_state(&path, None);
        
        let route = PartitionRoute {
            writer_tx,
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&hot_index),
            bloom_filter: Arc::clone(&bloom_filter),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
        };
        
        self.route_table.insert(path, route);
        
        // In no_std, the user must manage the thread/loop for the Partition
    }

    fn init_partition_state(&self, path: &str, _wal_path: Option<String>) -> (String, Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>, Arc<Mutex<SimpleBloom>>, Arc<AtomicPtr<Columns>>, Arc<DualCacheFF<usize, ()>>, Arc<Mutex<Vec<Arc<WorkerState>>>>) {
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
        wal_path: Option<String>,
        workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
        storage_path: String,
        shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        bloom_filter: Arc<Mutex<SimpleBloom>>,
    ) {
        let fs_rt = self.fs.clone();
        let wal_path_rt = wal_path.clone();
        
        self.thread_manager.spawn(alloc::boxed::Box::new(move || {
            let mut partition = Partition::new(
                alloc::boxed::Box::new(crate::platform::StdMessageQueue { rx: Mutex::new(rx) }),
                columns,
                wal_path_rt.clone(),
                workers,
                storage_path,
                fs_rt,
                shared_pointers,
                bloom_filter,
            );

            if let Some(ref path) = wal_path_rt {
                partition.replay_wal(path);
            }
            partition.run();
        }));
    }

    pub fn get_route(&self, path: &str) -> Option<&PartitionRoute> {
        self.route_table.get(path)
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

    pub fn get_column_str(
        &self,
        name: &str,
        worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<String>>> {
        worker.enter();
        let cols = crate::unsafe_core::load_ref(&self.columns);
        let col = cols.str_cols.get(name).cloned();
        worker.leave();
        col
    }

    pub fn get_column_int(
        &self,
        name: &str,
        worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<u32>>> {
        worker.enter();
        let cols = crate::unsafe_core::load_ref(&self.columns);
        let col = cols.int_cols.get(name).cloned();
        worker.leave();
        col
    }

    pub fn get_column_blob(
        &self,
        name: &str,
        worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<Vec<u8>>>> {
        worker.enter();
        let cols = crate::unsafe_core::load_ref(&self.columns);
        let col = cols.blob_cols.get(name).cloned();
        worker.leave();
        col
    }

    pub fn len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let snap = crate::unsafe_core::load_ref(&self.shared_pointers);
        let count = snap.len();
        worker.leave();
        count
    }
}

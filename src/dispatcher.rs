use ahash::AHashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicPtr;
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Sender};
use tokio::fs::OpenOptions;
use fastbloom::BloomFilter;
use dualcache_ff::{Config, DualCacheFF};

use crate::column::{Columns, ColumnArray};
use crate::commands::{PartitionCommand, WriteCommand};
use crate::partition::{MultiVectorPointer, Partition};
use crate::qsbr::{QsbrManager, WorkerState};
use crate::storage::AsyncStorage;
use crate::unsafe_core::new_atomic_ptr;

/// 4. cdDB 全域入口與調度器 (Dispatcher)
pub struct CdDBDispatcher {
    pub route_table: AHashMap<String, PartitionRoute>,
    pub base_path: Option<PathBuf>,
    pub workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
}

impl CdDBDispatcher {
    pub fn new(base_path: Option<PathBuf>) -> Self {
        Self {
            route_table: AHashMap::new(),
            base_path,
            workers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn register_partition(&mut self, path: String) -> UserWriter {
        self.register_partition_with_budget(path, 100 * 1024 * 1024)
    }

    pub fn register_partition_with_budget(
        &mut self,
        path: String,
        _budget_bytes: usize,
    ) -> UserWriter {
        let (tx, rx) = mpsc::channel(10000);
        let cols = Arc::new(new_atomic_ptr(Columns {
            str_cols: AHashMap::new(),
            int_cols: AHashMap::new(),
        }));

        let wal_path = self
            .base_path
            .as_ref()
            .map(|base| base.join(format!("{}.wal", path.replace('.', "/"))));
        let storage_path = self
            .base_path
            .as_ref()
            .map(|base| base.join(format!("{}.data", path.replace('.', "/"))))
            .unwrap_or_else(|| PathBuf::from("data").join(&path));


        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::new()));
        let bloom_filter = Arc::new(Mutex::new(BloomFilter::with_num_bits(1024 * 1024).hashes(4)));
        let storage = Arc::new(AsyncStorage::new(storage_path));
        let hot_index = Arc::new(DualCacheFF::new(Config::with_memory_budget(100, 60)));

        let route = PartitionRoute {
            writer_tx: tx.clone(),
            columns: Arc::clone(&cols),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&hot_index),
            bloom_filter: Arc::clone(&bloom_filter),
            storage: Arc::clone(&storage),
            workers: Arc::clone(&self.workers),
        };

        self.route_table.insert(path.clone(), route);

        let workers_rt = Arc::clone(&self.workers);
        let cols_rt = Arc::clone(&cols);
        let shared_pointers_rt = Arc::clone(&shared_pointers);
        let storage_rt = Arc::clone(&storage);
        let bloom_filter_rt = Arc::clone(&bloom_filter);
        let hot_index_rt = Arc::clone(&hot_index);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut partition = Partition {
                    columns: cols_rt,
                    shared_pointers: shared_pointers_rt,
                    writer_rx: rx,
                    wal_file: None,
                    qsbr: QsbrManager::new(workers_rt),
                    storage: storage_rt,
                    hot_index: (*hot_index_rt).clone(),
                    bloom_filter: bloom_filter_rt,
                    bloom_count: 0,
                    bloom_bits: 1024 * 1024,
                };

                if let Some(path) = wal_path {
                    if let Some(parent) = path.parent() { let _ = tokio::fs::create_dir_all(parent).await; }
                    partition.wal_file = Some(OpenOptions::new().create(true).append(true).open(&path).await.unwrap());
                    partition.replay_wal(&path).await;
                }
                partition.run().await;
            });
        });

        UserWriter(tx)
    }

    pub fn get_route(&self, path: &str) -> Option<&PartitionRoute> {
        self.route_table.get(path)
    }
}

impl Default for CdDBDispatcher {
    fn default() -> Self {
        Self::new(None)
    }
}

pub struct UserWriter(Sender<PartitionCommand>);
impl UserWriter {
    pub async fn send(&self, cmd: WriteCommand) -> Result<(), tokio::sync::mpsc::error::SendError<PartitionCommand>> {
        self.0.send(PartitionCommand::Write(cmd)).await
    }
}

#[derive(Clone)]
pub struct PartitionRoute {
    pub writer_tx: Sender<PartitionCommand>,
    pub columns: Arc<AtomicPtr<Columns>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub hot_index: Arc<DualCacheFF<usize, ()>>,
    pub bloom_filter: Arc<Mutex<BloomFilter>>,
    pub storage: Arc<AsyncStorage>,
    pub workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
}

impl PartitionRoute {
    pub fn register_worker(&self) -> Arc<WorkerState> {
        let worker = Arc::new(WorkerState::new());
        let mut workers = self.workers.lock().unwrap();
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

    pub fn len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let snap = crate::unsafe_core::load_ref(&self.shared_pointers);
        let count = snap.len();
        worker.leave();
        count
    }
}

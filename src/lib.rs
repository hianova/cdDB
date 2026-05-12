use ahash::AHashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use dualcache_ff::{Config, DualCacheFF};
use fastbloom::BloomFilter;
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::path::PathBuf;
use std::sync::atomic::AtomicPtr;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

mod qsbr;
mod storage;
mod unsafe_core;

use qsbr::{QsbrManager, WorkerState};
use storage::{AsyncStorage, EntityData};
use unsafe_core::{load_clone, load_ref, new_atomic_ptr, swap_ptr};

/// 0. 核心列集合 (Columns Snapshot)
#[derive(Clone)]
pub struct Columns {
    pub str_cols: AHashMap<String, Arc<ColumnArray<String>>>,
    pub int_cols: AHashMap<String, Arc<ColumnArray<u32>>>,
}

/// 1. 最底層的連續資料陣列 (Column / DOD 結構)
///
/// 使用自定義 QSBR 實現 Wait-Free 讀取
pub struct ColumnArray<T> {
    pub data: AtomicPtr<Vec<Option<T>>>,
    pub waitlist: AtomicPtr<Vec<usize>>,
    pub(crate) write_guard: AtomicBool,
}

impl<T> ColumnArray<T> {
    pub fn new() -> Self {
        Self {
            data: new_atomic_ptr(Vec::new()),
            waitlist: new_atomic_ptr(Vec::new()),
            write_guard: AtomicBool::new(false),
        }
    }

    pub fn acquire_lock(&self) {
        if self.write_guard.swap(true, Ordering::SeqCst) {
            panic!(
                "Data Race Detected: Multiple writers attempted to access the same ColumnArray!"
            );
        }
    }

    pub fn release_lock(&self) {
        self.write_guard.store(false, Ordering::SeqCst);
    }

    pub fn get_element(&self, idx: usize, worker: &WorkerState) -> Option<T>
    where
        T: Clone,
    {
        worker.enter();
        let data = unsafe { load_ref(&self.data) };
        let val = data.get(idx).and_then(|v| v.clone());
        worker.leave();
        val
    }

    pub fn get_data_snapshot(&self, worker: &WorkerState) -> Vec<Option<T>>
    where
        T: Clone,
    {
        worker.enter();
        let data = unsafe { load_clone(&self.data) };
        worker.leave();
        data
    }

    pub fn get_waitlist_snapshot(&self, worker: &WorkerState) -> Vec<usize> {
        worker.enter();
        let wl = unsafe { load_clone(&self.waitlist) };
        worker.leave();
        wl
    }

    pub fn data_len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let data = unsafe { load_ref(&self.data) };
        let len = data.len();
        worker.leave();
        len
    }

    /// Optimized: Enter QSBR once for multiple reads
    pub fn with_data<F, R>(&self, worker: &WorkerState, f: F) -> R
    where
        F: FnOnce(&Vec<Option<T>>) -> R,
    {
        worker.enter();
        let data = unsafe { load_ref(&self.data) };
        let res = f(data);
        worker.leave();
        res
    }
}

/// 2. 多向量指針快照 (RCU Snapshot)
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MultiVectorPointer {
    pub entity_id: usize,
    pub attribute_indices: AHashMap<String, usize>,
}

/// 寫入指令屬性封裝
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Attributes<V>(AHashMap<String, V>);

impl<V> Attributes<V> {
    pub fn new() -> Self {
        Self(AHashMap::new())
    }

    pub fn insert(&mut self, key: String, value: V) {
        self.0.insert(key, value);
    }

    pub fn inner(&self) -> &AHashMap<String, V> {
        &self.0
    }
}

impl<V> From<AHashMap<String, V>> for Attributes<V> {
    fn from(map: AHashMap<String, V>) -> Self {
        Self(map)
    }
}

impl<V> IntoIterator for Attributes<V> {
    type Item = (String, V);
    type IntoIter = std::collections::hash_map::IntoIter<String, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// 寫入指令列舉 (持久化用)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteCommand {
    Insert {
        entity_id: usize,
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
    },
    BatchInsert(Vec<(usize, Attributes<String>, Attributes<u32>)>),
    Delete {
        entity_id: usize,
    },
}

/// 內部指令列舉 (非同步溝通用)
#[derive(Debug)]
pub enum PartitionCommand {
    Write(WriteCommand),
    InternalLoad {
        entity_id: usize,
        response_tx: oneshot::Sender<Option<MultiVectorPointer>>,
    },
}

/// 4. 查詢接口 (Query Engine)
/// 實踐 SPEC 中提到的「極速多重指標跳轉」

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryNode {
    /// 精準取值
    Get { entity_id: usize, attr: String },
    /// 跨陣列跳轉 (目前限於同分區內)
    Link {
        from_entity_id: usize,
        link_attr: String,
        target_attr: String,
    },
    /// 範圍取值
    Range {
        entity_id: usize, // 起始 ID
        attr: String,
        len: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdDbQuery {
    pub nodes: Vec<QueryNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    Str(String),
    Int(u32),
    IntRange(Vec<u32>),
    None,
}

pub struct Query<'a> {
    route: &'a PartitionRoute,
    worker: Arc<WorkerState>,
}

impl<'a> Query<'a> {
    pub fn new(route: &'a PartitionRoute) -> Self {
        let worker = route.register_worker();
        Self { route, worker }
    }

    pub async fn execute(&self, query: CdDbQuery) -> Vec<QueryResult> {
        let mut results = Vec::new();
        for node in query.nodes {
            match node {
                QueryNode::Get { entity_id, attr } => {
                    if let Some(v) = self.get_int(entity_id, &attr).await {
                        results.push(QueryResult::Int(v));
                    } else if let Some(v) = self.get_str(entity_id, &attr).await {
                        results.push(QueryResult::Str(v));
                    } else {
                        results.push(QueryResult::None);
                    }
                }
                QueryNode::Link {
                    from_entity_id,
                    link_attr,
                    target_attr,
                } => {
                    if let Some(target_id) = self.get_int(from_entity_id, &link_attr).await {
                        let target_id = target_id as usize;
                        if let Some(v) = self.get_int(target_id, &target_attr).await {
                            results.push(QueryResult::Int(v));
                        } else if let Some(v) = self.get_str(target_id, &target_attr).await {
                            results.push(QueryResult::Str(v));
                        } else {
                            results.push(QueryResult::None);
                        }
                    } else {
                        results.push(QueryResult::None);
                    }
                }
                QueryNode::Range {
                    entity_id,
                    attr,
                    len,
                } => {
                    // Range scan usually happens on hot data or specific blocks
                    if let Some(ptr) = self.get_pointer(entity_id).await {
                        if let Some(&start_idx) = ptr.attribute_indices.get(&attr) {
                            if let Some(col) = self.route.get_column_int(&attr, &self.worker) {
                                let range_vals = col.with_data(&self.worker, |data| {
                                    data.iter()
                                        .skip(start_idx)
                                        .take(len)
                                        .flatten()
                                        .cloned()
                                        .collect()
                                });
                                results.push(QueryResult::IntRange(range_vals));
                                continue;
                            }
                        }
                    }
                    results.push(QueryResult::None);
                }
            }
        }
        results
    }

    async fn get_pointer(&self, entity_id: usize) -> Option<MultiVectorPointer> {
        // 1. Bloom Filter Check
        {
            let bloom = self.route.bloom_filter.lock().unwrap();
            if !bloom.contains(&entity_id) {
                return None;
            }
        }

        // 2. Full Memory Index Check (Wait-Free RCU)
        {
            self.worker.enter();
            let snap = unsafe { load_ref(&self.route.shared_pointers) };
            let ptr = snap.get(&entity_id).cloned();
            self.worker.leave();
            if let Some(p) = ptr {
                self.route.hot_index.insert(entity_id, ()); // Track hit
                return Some(p);
            }
        }

        // 3. Page Fault (Async Disk Load)
        let (tx, rx) = oneshot::channel();
        let _ = self.route.writer_tx.send(PartitionCommand::InternalLoad {
            entity_id,
            response_tx: tx,
        }).await;
        
        rx.await.unwrap_or(None)
    }

    pub async fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        if let Some(ptr) = self.get_pointer(entity_id).await {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_str(attr, &self.worker)
                    .and_then(|col| col.get_element(idx, &self.worker));
            }
        }
        None
    }

    pub async fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        if let Some(ptr) = self.get_pointer(entity_id).await {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_int(attr, &self.worker)
                    .and_then(|col| col.get_element(idx, &self.worker));
            }
        }
        None
    }

    pub async fn async_sum_int_range(&self, attr: &str, entity_ids: Vec<usize>) -> u64 {
        use futures::future::join_all;
        let futures: Vec<_> = entity_ids
            .into_iter()
            .map(|id| self.get_int(id, attr))
            .collect();
        let results = join_all(futures).await;
        results.into_iter().flatten().map(|v| v as u64).sum()
    }

    pub fn seed_bloom_filter(&self, entity_id: usize) {
        let mut bloom = self.route.bloom_filter.lock().unwrap();
        bloom.insert(&entity_id);
    }

    pub fn entities(&self) -> Vec<usize> {
        // Since we use Bloom Filter and DualCacheFF, we don't have a simple list of all entities in memory.
        // In a real system, we'd query the persistent index.
        Vec::new()
    }

    pub fn sum_int_range(&self, attr: &str, start_idx: usize, len: usize) -> Option<u64> {
        self.route.get_column_int(attr, &self.worker).map(|col| {
            col.with_data(&self.worker, |data| {
                data.iter()
                    .skip(start_idx)
                    .take(len)
                    .flatten()
                    .map(|&v| v as u64)
                    .sum()
            })
        })
    }
}

/// 3. 分區/群組 (Partition / Group)
pub struct Partition {
    pub columns: Arc<AtomicPtr<Columns>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub writer_rx: Receiver<PartitionCommand>,
    pub qsbr: QsbrManager,

    // 持久層與快照
    pub storage: Arc<AsyncStorage>,
    pub hot_index: DualCacheFF<usize, ()>, // Just for heat tracking
    pub bloom_filter: Arc<Mutex<BloomFilter>>,

    // WAL 支援
    pub wal_file: Option<File>,
}

impl Partition {
    pub async fn new(
        writer_rx: Receiver<PartitionCommand>,
        columns: Arc<AtomicPtr<Columns>>,
        wal_path: Option<PathBuf>,
        workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
        storage_path: PathBuf,
    ) -> Self {
        let wal_file = if let Some(path) = wal_path {
            if let Some(parent) = path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            Some(OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await
                .expect("Failed to open WAL file"))
        } else {
            None
        };

        // Initialize DualCache-FF with a 100MB budget (example)
        let cache_config = Config::with_memory_budget(100, 60);
        let hot_index = DualCacheFF::new(cache_config);

        let storage = Arc::new(AsyncStorage::new(storage_path));
        let bloom_filter = Arc::new(Mutex::new(
            BloomFilter::with_num_bits(1024 * 1024).hashes(4),
        ));
        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::new()));

        Self {
            columns,
            shared_pointers,
            writer_rx,
            wal_file,
            qsbr: QsbrManager::new(workers),
            storage,
            hot_index,
            bloom_filter,
        }
    }

    pub fn set_budget(&mut self, _bytes: usize) {
        // Managed by DualCache-FF
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.writer_rx.recv().await {
            let mut commands = vec![cmd];
            for _ in 0..1000 {
                if let Ok(next) = self.writer_rx.try_recv() {
                    commands.push(next);
                } else {
                    break;
                }
            }

            if let Some(ref mut file) = self.wal_file {
                for cmd in &commands {
                    if let PartitionCommand::Write(wcmd) = cmd {
                        let bytes = bincode::serialize(wcmd).expect("Failed to serialize command");
                        let len = bytes.len() as u64;
                        file.write_all(&len.to_le_bytes()).await.expect("Failed to write len to WAL");
                        file.write_all(&bytes).await.expect("Failed to write cmd to WAL");
                    }
                }
                file.flush().await.expect("Failed to flush WAL");
            }

            self.apply_batch_commands(commands).await;
            self.qsbr.maintenance();
        }
    }

    async fn apply_batch_commands(&mut self, commands: Vec<PartitionCommand>) {
        let mut next_pointers = unsafe { load_clone(&self.shared_pointers) };

        for cmd in commands {
            match cmd {
                PartitionCommand::Write(wcmd) => match wcmd {
                    WriteCommand::Insert { entity_id, attributes, attributes_int } => {
                        self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int).await;
                    }
                    WriteCommand::BatchInsert(batch) => {
                        for (entity_id, attributes, attributes_int) in batch {
                            self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int).await;
                        }
                    }
                    WriteCommand::Delete { entity_id } => {
                        next_pointers.remove(&entity_id);
                        self.hot_index.remove(&entity_id);
                    }
                },
                PartitionCommand::InternalLoad { entity_id, response_tx } => {
                    if let Some(ptr) = next_pointers.get(&entity_id) {
                        let _ = response_tx.send(Some(ptr.clone()));
                        continue;
                    }
                    let block = self.storage.read_block(entity_id, 32).await;
                    let mut target = None;
                    for data in block {
                        let ptr = self.process_promote(&mut next_pointers, data);
                        if ptr.entity_id == entity_id { target = Some(ptr); }
                    }
                    let _ = response_tx.send(target);
                }
            }
        }

        let old = swap_ptr(&self.shared_pointers, next_pointers);
        self.qsbr.defer_free(old);
    }

    async fn process_insert(
        &mut self,
        next_pointers: &mut AHashMap<usize, MultiVectorPointer>,
        entity_id: usize,
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
    ) {
        let mut new_indices = AHashMap::new();

        {
            let mut bloom = self.bloom_filter.lock().unwrap();
            bloom.insert(&entity_id);
        }

        for (name, val) in attributes.clone() {
            let col = self.get_or_create_column_str(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name, idx);
            col.release_lock();
        }
        for (name, val) in attributes_int.clone() {
            let col = self.get_or_create_column_int(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name, idx);
            col.release_lock();
        }

        let ptr = MultiVectorPointer {
            entity_id,
            attribute_indices: new_indices,
        };

        next_pointers.insert(entity_id, ptr);
        self.hot_index.insert(entity_id, ());

        let entity_data = EntityData {
            entity_id,
            attributes,
            attributes_int,
        };
        let _ = self.storage.write_entity(&entity_data).await;
    }

    fn process_promote(&mut self, next: &mut AHashMap<usize, MultiVectorPointer>, data: EntityData) -> MultiVectorPointer {
        let mut new_indices = AHashMap::new();
        {
            let mut bloom = self.bloom_filter.lock().unwrap();
            bloom.insert(&data.entity_id);
        }
        for (name, val) in data.attributes {
            let col = self.get_or_create_column_str(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name, idx);
            col.release_lock();
        }
        for (name, val) in data.attributes_int {
            let col = self.get_or_create_column_int(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name, idx);
            col.release_lock();
        }
        let ptr = MultiVectorPointer { entity_id: data.entity_id, attribute_indices: new_indices };
        next.insert(data.entity_id, ptr.clone());
        self.hot_index.insert(data.entity_id, ());
        ptr
    }

    async fn apply_command(&mut self, cmd: PartitionCommand) {
        self.apply_batch_commands(vec![cmd]).await;
    }

    pub async fn replay_wal(&mut self, path: &PathBuf) {
        if !path.exists() { return; }
        let mut file = File::open(path).await.expect("Failed to open WAL");
        loop {
            let mut len_bytes = [0u8; 8];
            if file.read_exact(&mut len_bytes).await.is_err() { break; }
            let len = u64::from_le_bytes(len_bytes) as usize;
            let mut buf = vec![0u8; len];
            if file.read_exact(&mut buf).await.is_err() { break; }
            let cmd: WriteCommand = bincode::deserialize(&buf).expect("Failed to deserialize WAL cmd");
            self.apply_command(PartitionCommand::Write(cmd)).await;
        }
    }

    fn get_or_create_column_str(&mut self, name: &str) -> Arc<ColumnArray<String>> {
        let cols = unsafe { load_ref(&self.columns) };
        if let Some(col) = cols.str_cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(ColumnArray::new());
            next_cols.str_cols.insert(name.to_string(), col.clone());
            let old = swap_ptr(&self.columns, next_cols);
            self.qsbr.defer_free(old);
            col
        }
    }

    fn get_or_create_column_int(&mut self, name: &str) -> Arc<ColumnArray<u32>> {
        let cols = unsafe { load_ref(&self.columns) };
        if let Some(col) = cols.int_cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(ColumnArray::new());
            next_cols.int_cols.insert(name.to_string(), col.clone());
            let old = swap_ptr(&self.columns, next_cols);
            self.qsbr.defer_free(old);
            col
        }
    }

    fn insert_into_column<T: Clone>(&mut self, col: &ColumnArray<T>, val: T) -> usize {
        let mut wl = unsafe { load_clone(&col.waitlist) };
        let mut data = unsafe { load_clone(&col.data) };

        let idx;
        if let Some(i) = wl.pop() {
            data[i] = Some(val);
            idx = i;
        } else {
            idx = data.len();
            data.push(Some(val));
        }

        let old_wl = swap_ptr(&col.waitlist, wl);
        let old_data = swap_ptr(&col.data, data);
        self.qsbr.defer_free(old_wl);
        self.qsbr.defer_free(old_data);
        idx
    }



    // set_budget was duplicate
}

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
        let cols = unsafe { load_ref(&self.columns) };
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
        let cols = unsafe { load_ref(&self.columns) };
        let col = cols.int_cols.get(name).cloned();
        worker.leave();
        col
    }

    pub fn len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let snap = unsafe { load_ref(&self.shared_pointers) };
        let count = snap.len();
        worker.leave();
        count
    }
}

impl Default for CdDBDispatcher {
    fn default() -> Self {
        Self::new(None)
    }
}

impl<T> Default for ColumnArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

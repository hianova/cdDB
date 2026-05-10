use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use ahash::AHashMap;
use crossbeam::channel::{Sender, Receiver, unbounded};
use parking_lot::RwLock;

/// 1. 最底層的連續資料陣列 (Column / DOD 結構)
pub struct ColumnArray<T> {
    pub data: RwLock<Vec<Option<T>>>, // Readers need to read this
    pub waitlist: RwLock<Vec<usize>>, // Only writer should touch this, but for simplicity...
    pub write_guard: AtomicBool, 
}

impl<T> ColumnArray<T> {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(Vec::new()),
            waitlist: RwLock::new(Vec::new()),
            write_guard: AtomicBool::new(false),
        }
    }

    pub fn acquire_lock(&self) {
        if self.write_guard.swap(true, Ordering::SeqCst) {
            panic!("Data Race Detected: Multiple writers attempted to access the same ColumnArray!");
        }
    }

    pub fn release_lock(&self) {
        self.write_guard.store(false, Ordering::SeqCst);
    }
}

/// 2. 多向量指針快照 (RCU Snapshot)
#[derive(Clone, Debug, Default)]
pub struct MultiVectorPointer {
    pub entity_id: usize,
    pub attribute_indices: AHashMap<String, usize>, 
}

/// 寫入指令列舉
pub enum WriteCommand {
    Insert { 
        entity_id: usize, 
        attributes: AHashMap<String, String>,
        attributes_int: AHashMap<String, u32>,
    },
    BatchInsert(Vec<(usize, AHashMap<String, String>, AHashMap<String, u32>)>),
    Delete { entity_id: usize },
}

/// 3. 分區/群組 (Partition / Group)
pub struct Partition {
    // These must be shared with readers
    pub columns_str: Arc<RwLock<AHashMap<String, Arc<ColumnArray<String>>>>>, 
    pub columns_int: Arc<RwLock<AHashMap<String, Arc<ColumnArray<u32>>>>>, 

    pub shared_pointers: Arc<RwLock<Arc<AHashMap<usize, MultiVectorPointer>>>>,
    writer_rx: Receiver<WriteCommand>, 
}

impl Partition {
    pub fn new(
        writer_rx: Receiver<WriteCommand>, 
        shared_pointers: Arc<RwLock<Arc<AHashMap<usize, MultiVectorPointer>>>>,
        columns_str: Arc<RwLock<AHashMap<String, Arc<ColumnArray<String>>>>>,
        columns_int: Arc<RwLock<AHashMap<String, Arc<ColumnArray<u32>>>>>
    ) -> Self {
        Self {
            columns_str,
            columns_int,
            shared_pointers,
            writer_rx,
        }
    }

    pub fn run(mut self) {
        while let Ok(cmd) = self.writer_rx.recv() {
            match cmd {
                WriteCommand::Insert { entity_id, attributes, attributes_int } => {
                    self.process_insert(entity_id, attributes, attributes_int);
                }
                WriteCommand::BatchInsert(batch) => {
                    let mut next_pointers = (**self.shared_pointers.read()).clone();
                    for (entity_id, attributes, attributes_int) in batch {
                        let mut new_indices = AHashMap::new();

                        for (name, val) in attributes {
                            let col = {
                                let mut cols = self.columns_str.write();
                                cols.entry(name.clone()).or_insert_with(|| Arc::new(ColumnArray::new())).clone()
                            };
                            col.acquire_lock();
                            let idx = self.insert_into_column(&col, val);
                            new_indices.insert(name, idx);
                            col.release_lock();
                        }

                        for (name, val) in attributes_int {
                            let col = {
                                let mut cols = self.columns_int.write();
                                cols.entry(name.clone()).or_insert_with(|| Arc::new(ColumnArray::new())).clone()
                            };
                            col.acquire_lock();
                            let idx = self.insert_into_column(&col, val);
                            new_indices.insert(name, idx);
                            col.release_lock();
                        }
                        next_pointers.insert(entity_id, MultiVectorPointer {
                            entity_id,
                            attribute_indices: new_indices,
                        });
                    }
                    let mut lock = self.shared_pointers.write();
                    *lock = Arc::new(next_pointers);
                }
                WriteCommand::Delete { entity_id } => {
                    let current = {
                        let lock = self.shared_pointers.read();
                        Arc::clone(&lock)
                    };
                    
                    if let Some(ptr) = current.get(&entity_id) {
                        let mut next = (*current).clone();
                        next.remove(&entity_id);
                        
                        for (name, &idx) in &ptr.attribute_indices {
                            if let Some(col) = self.columns_str.read().get(name) {
                                col.acquire_lock();
                                col.data.write()[idx] = None;
                                col.waitlist.write().push(idx);
                                col.release_lock();
                            } else if let Some(col) = self.columns_int.read().get(name) {
                                col.acquire_lock();
                                col.data.write()[idx] = None;
                                col.waitlist.write().push(idx);
                                col.release_lock();
                            }
                        }
                        
                        let mut lock = self.shared_pointers.write();
                        *lock = Arc::new(next);
                    }
                }
            }
        }
    }

    fn process_insert(&mut self, entity_id: usize, attributes: AHashMap<String, String>, attributes_int: AHashMap<String, u32>) {
        let mut new_indices = AHashMap::new();

        for (name, val) in attributes {
            let col = {
                let mut cols = self.columns_str.write();
                cols.entry(name.clone()).or_insert_with(|| Arc::new(ColumnArray::new())).clone()
            };
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name, idx);
            col.release_lock();
        }

        for (name, val) in attributes_int {
            let col = {
                let mut cols = self.columns_int.write();
                cols.entry(name.clone()).or_insert_with(|| Arc::new(ColumnArray::new())).clone()
            };
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name, idx);
            col.release_lock();
        }

        // RCU Publish
        let current = {
            let lock = self.shared_pointers.read();
            Arc::clone(&lock)
        };
        let mut next = (*current).clone();
        next.insert(entity_id, MultiVectorPointer {
            entity_id,
            attribute_indices: new_indices,
        });
        
        let mut lock = self.shared_pointers.write();
        *lock = Arc::new(next);
    }

    fn insert_into_column<T>(&self, col: &ColumnArray<T>, val: T) -> usize {
        let mut wl = col.waitlist.write();
        if let Some(i) = wl.pop() {
            let mut data = col.data.write();
            data[i] = Some(val);
            i
        } else {
            let mut data = col.data.write();
            let i = data.len();
            data.push(Some(val));
            i
        }
    }
}

/// 4. cdDB 全域入口與調度器 (Dispatcher)
pub struct CdDBDispatcher {
    pub route_table: AHashMap<String, PartitionRoute>, 
}

#[derive(Clone)]
pub struct PartitionRoute {
    pub writer_tx: Sender<WriteCommand>,
    pub reader_snapshot_root: Arc<RwLock<Arc<AHashMap<usize, MultiVectorPointer>>>>,
    pub columns_str: Arc<RwLock<AHashMap<String, Arc<ColumnArray<String>>>>>,
    pub columns_int: Arc<RwLock<AHashMap<String, Arc<ColumnArray<u32>>>>>,
}

impl PartitionRoute {
    pub fn get_snapshot(&self) -> Arc<AHashMap<usize, MultiVectorPointer>> {
        let lock = self.reader_snapshot_root.read();
        Arc::clone(&lock)
    }

    pub fn get_column_str(&self, name: &str) -> Option<Arc<ColumnArray<String>>> {
        self.columns_str.read().get(name).cloned()
    }

    pub fn get_column_int(&self, name: &str) -> Option<Arc<ColumnArray<u32>>> {
        self.columns_int.read().get(name).cloned()
    }
}

impl CdDBDispatcher {
    pub fn new() -> Self {
        Self {
            route_table: AHashMap::new(),
        }
    }

    pub fn register_partition(&mut self, path: String) -> Sender<WriteCommand> {
        let (tx, rx) = unbounded();
        let shared = Arc::new(RwLock::new(Arc::new(AHashMap::new())));
        let cols_str = Arc::new(RwLock::new(AHashMap::new()));
        let cols_int = Arc::new(RwLock::new(AHashMap::new()));
        
        let route = PartitionRoute {
            writer_tx: tx.clone(),
            reader_snapshot_root: Arc::clone(&shared),
            columns_str: Arc::clone(&cols_str),
            columns_int: Arc::clone(&cols_int),
        };
        
        self.route_table.insert(path, route);
        
        let partition = Partition::new(rx, shared, cols_str, cols_int);
        std::thread::spawn(move || {
            partition.run();
        });
        
        tx
    }

    pub fn get_route(&self, path: &str) -> Option<&PartitionRoute> {
        self.route_table.get(path)
    }
}

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use ahash::AHashMap;
use crossbeam::channel::{Sender, Receiver, unbounded};
use crossbeam::epoch::{self, Atomic, Owned};
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, Read};
use std::path::PathBuf;

/// 1. 最底層的連續資料陣列 (Column / DOD 結構)
///
/// 使用 crossbeam::epoch 實現 Wait-Free 讀取
pub struct ColumnArray<T> {
    pub data: Atomic<Vec<Option<T>>>, 
    pub waitlist: Atomic<Vec<usize>>, 
    pub(crate) write_guard: AtomicBool, 
}

impl<T> ColumnArray<T> {
    pub fn new() -> Self {
        Self {
            data: Atomic::new(Vec::new()),
            waitlist: Atomic::new(Vec::new()),
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

    pub fn get_element(&self, idx: usize) -> Option<T> where T: Clone {
        let guard = &epoch::pin();
        let data = unsafe { self.data.load(Ordering::Acquire, guard).as_ref().unwrap() };
        data.get(idx).and_then(|v| v.clone())
    }

    pub fn get_data_snapshot(&self) -> Vec<Option<T>> where T: Clone {
        let guard = &epoch::pin();
        let data = self.data.load(Ordering::Acquire, guard);
        unsafe { data.as_ref().unwrap().clone() }
    }

    pub fn get_waitlist_snapshot(&self) -> Vec<usize> {
        let guard = &epoch::pin();
        let wl = self.waitlist.load(Ordering::Acquire, guard);
        unsafe { wl.as_ref().unwrap().clone() }
    }

    pub fn data_len(&self) -> usize {
        let guard = &epoch::pin();
        let data = unsafe { self.data.load(Ordering::Acquire, guard).as_ref().unwrap() };
        data.len()
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

/// 寫入指令列舉
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteCommand {
    Insert { 
        entity_id: usize, 
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
    },
    BatchInsert(Vec<(usize, Attributes<String>, Attributes<u32>)>),
    Delete { entity_id: usize },
}

/// 4. 查詢接口 (Query Engine)
/// 實踐 SPEC 中提到的「極速多重指標跳轉」
pub struct Query<'a> {
    route: &'a PartitionRoute,
    snapshot: AHashMap<usize, MultiVectorPointer>,
}

impl<'a> Query<'a> {
    pub fn new(route: &'a PartitionRoute) -> Self {
        Self {
            route,
            snapshot: route.get_snapshot(),
        }
    }

    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        self.snapshot.get(&entity_id)
            .and_then(|ptr| ptr.attribute_indices.get(attr))
            .and_then(|&idx| self.route.get_column_str(attr).and_then(|col| col.get_element(idx)))
    }

    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        self.snapshot.get(&entity_id)
            .and_then(|ptr| ptr.attribute_indices.get(attr))
            .and_then(|&idx| self.route.get_column_int(attr).and_then(|col| col.get_element(idx)))
    }

    pub fn entities(&self) -> Vec<usize> {
        self.snapshot.keys().cloned().collect()
    }
}

/// 3. 分區/群組 (Partition / Group)
pub struct Partition {
    // 使用 Arc<Atomic<...>> 以便與 PartitionRoute 共享並支援 Wait-Free 讀取
    pub columns_str: Arc<Atomic<AHashMap<String, Arc<ColumnArray<String>>>>>, 
    pub columns_int: Arc<Atomic<AHashMap<String, Arc<ColumnArray<u32>>>>>, 

    pub shared_pointers: Arc<Atomic<AHashMap<usize, MultiVectorPointer>>>,
    writer_rx: Receiver<WriteCommand>, 

    // WAL 支援
    wal_file: Option<File>,
}

impl Partition {
    pub fn new(
        writer_rx: Receiver<WriteCommand>, 
        shared_pointers: Arc<Atomic<AHashMap<usize, MultiVectorPointer>>>,
        columns_str: Arc<Atomic<AHashMap<String, Arc<ColumnArray<String>>>>>,
        columns_int: Arc<Atomic<AHashMap<String, Arc<ColumnArray<u32>>>>>,
        wal_path: Option<PathBuf>,
    ) -> Self {
        let wal_file = wal_path.map(|path| {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("Failed to open WAL file")
        });

        Self {
            columns_str,
            columns_int,
            shared_pointers,
            writer_rx,
            wal_file,
        }
    }

    pub fn run(mut self) {
        while let Ok(cmd) = self.writer_rx.recv() {
            // 1. Write to WAL
            if let Some(ref mut file) = self.wal_file {
                let bytes = bincode::serialize(&cmd).expect("Failed to serialize command");
                let len = bytes.len() as u64;
                file.write_all(&len.to_le_bytes()).expect("Failed to write len to WAL");
                file.write_all(&bytes).expect("Failed to write cmd to WAL");
                file.flush().expect("Failed to flush WAL");
            }

            // 2. Apply to memory
            let guard = &epoch::pin();
            self.apply_command(cmd, guard);
        }
    }

    fn apply_command(&mut self, cmd: WriteCommand, guard: &epoch::Guard) {
        match cmd {
            WriteCommand::Insert { entity_id, attributes, attributes_int } => {
                self.process_insert(entity_id, attributes, attributes_int, guard);
            }
            WriteCommand::BatchInsert(batch) => {
                let mut next_pointers = unsafe {
                    self.shared_pointers.load(Ordering::Acquire, guard).as_ref().unwrap().clone()
                };
                
                for (entity_id, attributes, attributes_int) in batch {
                    let mut new_indices = AHashMap::new();

                    for (name, val) in attributes {
                        let col = self.get_or_create_column_str(&name, guard);
                        col.acquire_lock();
                        let idx = self.insert_into_column(&col, val, guard);
                        new_indices.insert(name, idx);
                        col.release_lock();
                    }

                    for (name, val) in attributes_int {
                        let col = self.get_or_create_column_int(&name, guard);
                        col.acquire_lock();
                        let idx = self.insert_into_column(&col, val, guard);
                        new_indices.insert(name, idx);
                        col.release_lock();
                    }
                    next_pointers.insert(entity_id, MultiVectorPointer {
                        entity_id,
                        attribute_indices: new_indices,
                    });
                }
                
                let old = self.shared_pointers.swap(Owned::new(next_pointers), Ordering::AcqRel, guard);
                unsafe { guard.defer_destroy(old); }
            }
            WriteCommand::Delete { entity_id } => {
                let current = unsafe {
                    self.shared_pointers.load(Ordering::Acquire, guard).as_ref().unwrap()
                };
                
                if let Some(ptr) = current.get(&entity_id) {
                    let mut next = current.clone();
                    next.remove(&entity_id);
                    
                    for (name, &idx) in &ptr.attribute_indices {
                        let cols_str = unsafe { self.columns_str.load(Ordering::Acquire, guard).as_ref().unwrap() };
                        let cols_int = unsafe { self.columns_int.load(Ordering::Acquire, guard).as_ref().unwrap() };

                        if let Some(col) = cols_str.get(name) {
                            col.acquire_lock();
                            self.update_column_element(col, idx, None, guard);
                            col.release_lock();
                        } else if let Some(col) = cols_int.get(name) {
                            col.acquire_lock();
                            self.update_column_element(col, idx, None, guard);
                            col.release_lock();
                        }
                    }
                    
                    let old = self.shared_pointers.swap(Owned::new(next), Ordering::AcqRel, guard);
                    unsafe { guard.defer_destroy(old); }
                }
            }
        }
    }

    pub fn replay_wal(&mut self, path: &PathBuf) {
        if !path.exists() { return; }
        let file = File::open(path).expect("Failed to open WAL for replay");
        let mut reader = BufReader::new(file);
        
        loop {
            let mut len_bytes = [0u8; 8];
            if reader.read_exact(&mut len_bytes).is_err() { break; }
            let len = u64::from_le_bytes(len_bytes) as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer).expect("Failed to read command from WAL");
            
            let cmd: WriteCommand = bincode::deserialize(&buffer).expect("Failed to deserialize command from WAL");
            let guard = &epoch::pin();
            self.apply_command(cmd, guard);
        }
    }

    fn get_or_create_column_str(&self, name: &str, guard: &epoch::Guard) -> Arc<ColumnArray<String>> {
        let cols = unsafe { self.columns_str.load(Ordering::Acquire, guard).as_ref().unwrap() };
        if let Some(col) = cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(ColumnArray::new());
            next_cols.insert(name.to_string(), col.clone());
            let old = self.columns_str.swap(Owned::new(next_cols), Ordering::AcqRel, guard);
            unsafe { guard.defer_destroy(old); }
            col
        }
    }

    fn get_or_create_column_int(&self, name: &str, guard: &epoch::Guard) -> Arc<ColumnArray<u32>> {
        let cols = unsafe { self.columns_int.load(Ordering::Acquire, guard).as_ref().unwrap() };
        if let Some(col) = cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(ColumnArray::new());
            next_cols.insert(name.to_string(), col.clone());
            let old = self.columns_int.swap(Owned::new(next_cols), Ordering::AcqRel, guard);
            unsafe { guard.defer_destroy(old); }
            col
        }
    }

    fn process_insert(&mut self, entity_id: usize, attributes: Attributes<String>, attributes_int: Attributes<u32>, guard: &epoch::Guard) {
        let mut new_indices = AHashMap::new();

        for (name, val) in attributes {
            let col = self.get_or_create_column_str(&name, guard);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val, guard);
            new_indices.insert(name, idx);
            col.release_lock();
        }

        for (name, val) in attributes_int {
            let col = self.get_or_create_column_int(&name, guard);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val, guard);
            new_indices.insert(name, idx);
            col.release_lock();
        }

        let current = unsafe { self.shared_pointers.load(Ordering::Acquire, guard).as_ref().unwrap() };
        let mut next = current.clone();
        next.insert(entity_id, MultiVectorPointer {
            entity_id,
            attribute_indices: new_indices,
        });
        
        let old = self.shared_pointers.swap(Owned::new(next), Ordering::AcqRel, guard);
        unsafe { guard.defer_destroy(old); }
    }

    fn insert_into_column<T: Clone>(&self, col: &ColumnArray<T>, val: T, guard: &epoch::Guard) -> usize {
        let mut wl = unsafe { col.waitlist.load(Ordering::Acquire, guard).as_ref().unwrap().clone() };
        let mut data = unsafe { col.data.load(Ordering::Acquire, guard).as_ref().unwrap().clone() };
        
        let idx;
        if let Some(i) = wl.pop() {
            data[i] = Some(val);
            idx = i;
        } else {
            idx = data.len();
            data.push(Some(val));
        }
        
        let old_wl = col.waitlist.swap(Owned::new(wl), Ordering::AcqRel, guard);
        let old_data = col.data.swap(Owned::new(data), Ordering::AcqRel, guard);
        unsafe {
            guard.defer_destroy(old_wl);
            guard.defer_destroy(old_data);
        }
        idx
    }

    fn update_column_element<T: Clone>(&self, col: &ColumnArray<T>, idx: usize, val: Option<T>, guard: &epoch::Guard) {
        let is_val_none = val.is_none();
        let mut data = unsafe { col.data.load(Ordering::Acquire, guard).as_ref().unwrap().clone() };
        data[idx] = val;
        let old_data = col.data.swap(Owned::new(data), Ordering::AcqRel, guard);
        
        if is_val_none {
            let mut wl = unsafe { col.waitlist.load(Ordering::Acquire, guard).as_ref().unwrap().clone() };
            wl.push(idx);
            let old_wl = col.waitlist.swap(Owned::new(wl), Ordering::AcqRel, guard);
            unsafe { guard.defer_destroy(old_wl); }
        }
        
        unsafe { guard.defer_destroy(old_data); }
    }
}

/// 4. cdDB 全域入口與調度器 (Dispatcher)
pub struct CdDBDispatcher {
    pub route_table: AHashMap<String, PartitionRoute>, 
    pub base_path: Option<PathBuf>,
}

impl CdDBDispatcher {
    pub fn new(base_path: Option<PathBuf>) -> Self {
        Self {
            route_table: AHashMap::new(),
            base_path,
        }
    }

    pub fn register_partition(&mut self, path: String) -> Sender<WriteCommand> {
        let (tx, rx) = unbounded();
        let shared = Arc::new(Atomic::new(AHashMap::new()));
        let cols_str = Arc::new(Atomic::new(AHashMap::new()));
        let cols_int = Arc::new(Atomic::new(AHashMap::new()));
        
        let wal_path = self.base_path.as_ref().map(|base| {
            base.join(format!("{}.wal", path.replace('.', "/")))
        });

        let route = PartitionRoute {
            writer_tx: tx.clone(),
            reader_snapshot_root: Arc::clone(&shared),
            columns_str: Arc::clone(&cols_str),
            columns_int: Arc::clone(&cols_int),
        };
        
        self.route_table.insert(path.clone(), route);
        
        let mut partition = Partition::new(rx, shared, cols_str, cols_int, wal_path.clone());
        
        // Replay WAL if it exists
        if let Some(wp) = wal_path {
            partition.replay_wal(&wp);
        }

        std::thread::spawn(move || {
            partition.run();
        });
        
        tx
    }

    pub fn get_route(&self, path: &str) -> Option<&PartitionRoute> {
        self.route_table.get(path)
    }
}

#[derive(Clone)]
pub struct PartitionRoute {
    pub writer_tx: Sender<WriteCommand>,
    pub reader_snapshot_root: Arc<Atomic<AHashMap<usize, MultiVectorPointer>>>,
    pub columns_str: Arc<Atomic<AHashMap<String, Arc<ColumnArray<String>>>>>,
    pub columns_int: Arc<Atomic<AHashMap<String, Arc<ColumnArray<u32>>>>>,
}

impl PartitionRoute {
    pub fn get_snapshot(&self) -> AHashMap<usize, MultiVectorPointer> {
        let guard = &epoch::pin();
        let snapshot = self.reader_snapshot_root.load(Ordering::Acquire, guard);
        unsafe { snapshot.as_ref().unwrap().clone() }
    }

    pub fn get_column_str(&self, name: &str) -> Option<Arc<ColumnArray<String>>> {
        let guard = &epoch::pin();
        let cols = self.columns_str.load(Ordering::Acquire, guard);
        unsafe { cols.as_ref().unwrap().get(name).cloned() }
    }

    pub fn get_column_int(&self, name: &str) -> Option<Arc<ColumnArray<u32>>> {
        let guard = &epoch::pin();
        let cols = self.columns_int.load(Ordering::Acquire, guard);
        unsafe { cols.as_ref().unwrap().get(name).cloned() }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_wal_recovery() {
        let base_path = std::env::current_dir().unwrap().join("test_data_wal");
        if base_path.exists() {
            let _ = std::fs::remove_dir_all(&base_path);
        }
        
        let path = "test.wal_recovery".to_string();

        // 1. Initial write
        {
            let mut db = CdDBDispatcher::new(Some(base_path.clone()));
            let tx = db.register_partition(path.clone());
            
            let mut attrs_int = AHashMap::new();
            attrs_int.insert("val".to_string(), 123);
            
            tx.send(WriteCommand::Insert {
                entity_id: 1,
                attributes: AHashMap::new().into(),
                attributes_int: attrs_int.into(),
            }).unwrap();
            
            // Wait for processing
            std::thread::sleep(std::time::Duration::from_millis(300));
        }

        // 2. Recovery
        {
            let mut db = CdDBDispatcher::new(Some(base_path.clone()));
            db.register_partition(path);
            
            let route = db.get_route("test.wal_recovery").unwrap();
            let query = Query::new(route);
            assert_eq!(query.get_int(1, "val"), Some(123));
        }
        
        let _ = std::fs::remove_dir_all(&base_path);
    }
}

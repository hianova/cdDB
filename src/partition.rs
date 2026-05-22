use crate::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::vec;
use alloc::string::{String, ToString};
use core::sync::atomic::AtomicPtr;
use crate::platform::FileSystem;

#[cfg(feature = "std")]
use std::sync::Mutex;
#[cfg(not(feature = "std"))]
use spin::Mutex;

use crate::{DualCacheFF, Config};
use crate::bloom::SimpleBloom;

use crate::column::Columns;
use crate::commands::{Attributes, PartitionCommand, WriteCommand};
use crate::qsbr::{QsbrManager, WorkerState};
use crate::storage::{Storage, EntityData};
use crate::unsafe_core::{load_clone, swap_ptr};
use crate::wal::WalProvider;

/// 2. 多向量指針快照 (RCU Snapshot)
#[derive(Clone, Debug, Default)]
pub struct MultiVectorPointer {
    pub entity_id: usize,
    pub attribute_indices: AHashMap<String, usize>,
}

/// 3. 分區/群組 (Partition / Group)
pub struct Partition {
    pub columns: Arc<AtomicPtr<Columns>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub writer_rx: alloc::boxed::Box<dyn crate::platform::MessageQueue>,
    pub qsbr: QsbrManager,

    // 持久層與快照
    pub storage: Arc<Storage>,
    pub hot_index: DualCacheFF<usize, ()>, // Just for heat tracking
    pub bloom_filter: Arc<Mutex<SimpleBloom>>,

    // WAL 支援
    pub wal: Arc<dyn WalProvider>,
    pub bloom_count: usize,
    pub bloom_bits: usize,
    pub fs: Arc<dyn FileSystem>,
}

impl Partition {
    pub fn new(
        writer_rx: alloc::boxed::Box<dyn crate::platform::MessageQueue>,
        columns: Arc<AtomicPtr<Columns>>,
        wal: Arc<dyn WalProvider>,
        workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
        storage_path: String,
        fs: Arc<dyn FileSystem>,
        shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        bloom_filter: Arc<Mutex<SimpleBloom>>,
    ) -> Self {
        let storage = Arc::new(Storage::new(storage_path, fs.clone()));
        let bloom_bits = 1024 * 1024;

        Self {
            columns,
            shared_pointers,
            writer_rx,
            wal,
            qsbr: QsbrManager::new(workers),
            storage,
            hot_index: DualCacheFF::new(Config::with_memory_budget(100, 60)),
            bloom_filter,
            bloom_count: 0,
            bloom_bits,
            fs,
        }
    }

    fn update_bloom(&mut self, entity_id: usize) {
        {
            #[cfg(feature = "std")]
            let mut bloom = self.bloom_filter.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let mut bloom = self.bloom_filter.lock();
            bloom.insert(&entity_id);
            self.bloom_count += 1;
        }

        if self.bloom_count > (self.bloom_bits * 7 / 10) {
            self.rebuild_bloom_filter();
        }
    }

    fn rebuild_bloom_filter(&mut self) {
        let _old_bits = self.bloom_bits;
        self.bloom_bits *= 2;
        let mut new_bloom = SimpleBloom::new(self.bloom_bits);
        let mut count = 0;

        {
            #[cfg(feature = "std")]
            let index = self.storage.disk_index.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let index = self.storage.disk_index.lock();

            for &id in index.keys() {
                new_bloom.insert(&id);
                count += 1;
            }
        }

        {
            #[cfg(feature = "std")]
            let mut bloom = self.bloom_filter.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let mut bloom = self.bloom_filter.lock();
            *bloom = new_bloom;
        }
        self.bloom_count = count;
    }

    pub fn run(mut self) {
        loop {
            let cmd_res = self.writer_rx.recv();
            #[cfg(feature = "std")]
            let cmd = match cmd_res {
                Ok(c) => c,
                Err(_) => break,
            };
            #[cfg(not(feature = "std"))]
            let cmd = match cmd_res {
                Ok(c) => c,
                Err(_) => break,
            };

            let mut commands = vec![cmd];
            for _ in 0..1000 {
                if let Ok(next) = self.writer_rx.try_recv() {
                    commands.push(next);
                } else {
                    break;
                }
            }

            let mut batch_refs = Vec::new();
            for cmd in &commands {
                if let PartitionCommand::Write(wcmd) = cmd {
                    batch_refs.push(wcmd);
                }
            }
            if !batch_refs.is_empty() {
                let _ = self.wal.append_batch(&batch_refs);
            }

            self.apply_batch_commands(commands);
            let _ = self.storage.flush();
            self.qsbr.maintenance();
        }
    }

    fn apply_batch_commands(&mut self, commands: Vec<PartitionCommand>) {
        let mut next_pointers = load_clone(&self.shared_pointers);

        for cmd in commands {
            match cmd {
                PartitionCommand::Write(wcmd) => match wcmd {
                    WriteCommand::Insert { entity_id, attributes, attributes_int, attributes_blob } => {
                        self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int, attributes_blob);
                    }
                    WriteCommand::BatchInsert(batch) => {
                        for (entity_id, attributes, attributes_int, attributes_blob) in batch {
                            self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int, attributes_blob);
                        }
                    }
                    WriteCommand::Delete { entity_id } => {
                        next_pointers.remove(&entity_id);
                        self.hot_index.remove(&entity_id);
                    }
                },
                PartitionCommand::InternalLoad { entity_id, response_tx } => {
                    if let Some(ptr) = next_pointers.get(&entity_id) {
                        // Ensure it's committed to shared_pointers before waking the reader
                        let old = swap_ptr(&self.shared_pointers, next_pointers.clone());
                        self.qsbr.defer_free(old);
                        let _ = response_tx.send(Some(ptr.clone()));
                        continue;
                    }
                    let block = self.storage.read_block(entity_id, 32);
                    let mut target = None;
                    for data in block {
                        let ptr = self.process_promote(&mut next_pointers, data);
                        if ptr.entity_id == entity_id { target = Some(ptr); }
                    }
                    
                    // Commit the promoted pointers to shared_pointers immediately
                    let old = swap_ptr(&self.shared_pointers, next_pointers.clone());
                    self.qsbr.defer_free(old);
                    
                    let _ = response_tx.send(target);
                }
            }
        }

        let old = swap_ptr(&self.shared_pointers, next_pointers);
        self.qsbr.defer_free(old);
    }

    fn process_insert(
        &mut self,
        next_pointers: &mut AHashMap<usize, MultiVectorPointer>,
        entity_id: usize,
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
        attributes_blob: Attributes<Vec<u8>>,
    ) {
        let mut new_indices = AHashMap::default();

        self.update_bloom(entity_id);

        for (name, val) in attributes.clone() {
            let col = self.get_or_create_column_str(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name.to_string(), idx);
            col.release_lock();
        }
        for (name, val) in attributes_int.clone() {
            let col = self.get_or_create_column_int(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name.to_string(), idx);
            col.release_lock();
        }
        for (name, val) in attributes_blob.clone() {
            let col = self.get_or_create_column_blob(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name.to_string(), idx);
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
            attributes_blob,
        };
        let _ = self.storage.write_entity(&entity_data);
    }

    fn process_promote(&mut self, next: &mut AHashMap<usize, MultiVectorPointer>, data: EntityData) -> MultiVectorPointer {
        let mut new_indices = AHashMap::default();
        self.update_bloom(data.entity_id);
        for (name, val) in data.attributes {
            let col = self.get_or_create_column_str(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name.to_string(), idx);
            col.release_lock();
        }
        for (name, val) in data.attributes_int {
            let col = self.get_or_create_column_int(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name.to_string(), idx);
            col.release_lock();
        }
        for (name, val) in data.attributes_blob {
            let col = self.get_or_create_column_blob(&name);
            col.acquire_lock();
            let idx = self.insert_into_column(&col, val);
            new_indices.insert(name.to_string(), idx);
            col.release_lock();
        }
        let ptr = MultiVectorPointer { entity_id: data.entity_id, attribute_indices: new_indices };
        next.insert(data.entity_id, ptr.clone());
        self.hot_index.insert(data.entity_id, ());
        ptr
    }

    fn apply_command(&mut self, cmd: PartitionCommand) {
        self.apply_batch_commands(vec![cmd]);
    }

    #[allow(dead_code)]
    fn log_wal(&mut self, cmd: &WriteCommand) {
        let _ = self.wal.append(cmd);
    }

    pub fn replay_wal(&mut self) {
        if let Ok(bytes) = self.wal.read_all() {
            let mut pos = 0;
            while pos + 4 <= bytes.len() {
                let len = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + len <= bytes.len() {
                    if let Some(cmd) = WriteCommand::decode(&bytes[pos..pos+len]) {
                        self.apply_command(PartitionCommand::Write(cmd));
                    }
                    pos += len;
                } else {
                    break;
                }
            }
        }
    }

    fn get_or_create_column_str(&mut self, name: &str) -> Arc<crate::column::ColumnArray<String>> {
        let cols = crate::unsafe_core::load_ref(&self.columns);
        if let Some(col) = cols.str_cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(crate::column::ColumnArray::new());
            next_cols.str_cols.insert(name.to_string(), col.clone());
            let old = swap_ptr(&self.columns, next_cols);
            self.qsbr.defer_free(old);
            col
        }
    }

    fn get_or_create_column_int(&mut self, name: &str) -> Arc<crate::column::ColumnArray<u32>> {
        let cols = crate::unsafe_core::load_ref(&self.columns);
        if let Some(col) = cols.int_cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(crate::column::ColumnArray::new());
            next_cols.int_cols.insert(name.to_string(), col.clone());
            let old = swap_ptr(&self.columns, next_cols);
            self.qsbr.defer_free(old);
            col
        }
    }

    fn get_or_create_column_blob(&mut self, name: &str) -> Arc<crate::column::ColumnArray<Vec<u8>>> {
        let cols = crate::unsafe_core::load_ref(&self.columns);
        if let Some(col) = cols.blob_cols.get(name) {
            col.clone()
        } else {
            let mut next_cols = cols.clone();
            let col = Arc::new(crate::column::ColumnArray::new());
            next_cols.blob_cols.insert(name.to_string(), col.clone());
            let old = swap_ptr(&self.columns, next_cols);
            self.qsbr.defer_free(old);
            col
        }
    }

    fn insert_into_column<T: Clone>(&mut self, col: &crate::column::ColumnArray<T>, val: T) -> usize {
        let mut wl = load_clone(&col.waitlist);
        let mut data = load_clone(&col.data);

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
}

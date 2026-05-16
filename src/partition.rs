use ahash::AHashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicPtr;
use std::path::PathBuf;
use std::fs::{File, OpenOptions, self};
use std::io::{Read, Write};
use serde::{Deserialize, Serialize};
use crossbeam_channel::Receiver;
use dualcache_ff::DualCacheFF;
use fastbloom::BloomFilter;

use crate::column::Columns;
use crate::commands::{Attributes, PartitionCommand, WriteCommand};
use crate::qsbr::{QsbrManager, WorkerState};
use crate::storage::{Storage, EntityData};
use crate::unsafe_core::{load_clone, new_atomic_ptr, swap_ptr};

/// 2. 多向量指針快照 (RCU Snapshot)
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MultiVectorPointer {
    pub entity_id: usize,
    pub attribute_indices: AHashMap<String, usize>,
}

/// 3. 分區/群組 (Partition / Group)
pub struct Partition {
    pub columns: Arc<AtomicPtr<Columns>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub writer_rx: Receiver<PartitionCommand>,
    pub qsbr: QsbrManager,

    // 持久層與快照
    pub storage: Arc<Storage>,
    pub hot_index: DualCacheFF<usize, ()>, // Just for heat tracking
    pub bloom_filter: Arc<Mutex<BloomFilter>>,

    // WAL 支援
    pub wal_file: Option<File>,
    pub bloom_count: usize,
    pub bloom_bits: usize,
}

impl Partition {
    pub fn new(
        writer_rx: Receiver<PartitionCommand>,
        columns: Arc<AtomicPtr<Columns>>,
        wal_path: Option<PathBuf>,
        workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
        storage_path: PathBuf,
    ) -> Self {
        let wal_file = if let Some(path) = wal_path {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            Some(OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("Failed to open WAL file"))
        } else {
            None
        };

        let storage = Arc::new(Storage::new(storage_path));
        let bloom_bits = 1024 * 1024;
        let bloom_filter = Arc::new(Mutex::new(
            BloomFilter::with_num_bits(bloom_bits).hashes(4),
        ));
        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::new()));

        Self {
            columns,
            shared_pointers,
            writer_rx,
            wal_file,
            qsbr: QsbrManager::new(workers),
            storage,
            hot_index: DualCacheFF::new(dualcache_ff::Config::with_memory_budget(100, 60)),
            bloom_filter,
            bloom_count: 0,
            bloom_bits,
        }
    }

    fn update_bloom(&mut self, entity_id: usize) {
        {
            let mut bloom = self.bloom_filter.lock().unwrap();
            bloom.insert(&entity_id);
            self.bloom_count += 1;
        }

        if self.bloom_count > (self.bloom_bits * 7 / 10) {
            self.rebuild_bloom_filter();
        }
    }

    fn rebuild_bloom_filter(&mut self) {
        let old_bits = self.bloom_bits;
        self.bloom_bits *= 2;
        println!("[cdDB] Resizing Bloom Filter: {} -> {} bits", old_bits, self.bloom_bits);
        let mut new_bloom = BloomFilter::with_num_bits(self.bloom_bits).hashes(4);
        let mut count = 0;

        if let Ok(entries) = fs::read_dir(&self.storage.base_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().into_string().unwrap_or_default();
                if name.starts_with("entity_") && name.ends_with(".bin") {
                    if let Ok(id) = name[7..name.len() - 4].parse::<usize>() {
                        new_bloom.insert(&id);
                        count += 1;
                    }
                }
            }
        }

        let mut bloom = self.bloom_filter.lock().unwrap();
        *bloom = new_bloom;
        self.bloom_count = count;
    }

    pub fn run(mut self) {
        while let Ok(cmd) = self.writer_rx.recv() {
            let mut commands = vec![cmd];
            // Batch processing: drain up to 1000 more commands
            for _ in 0..1000 {
                if let Ok(next) = self.writer_rx.try_recv() {
                    commands.push(next);
                } else {
                    break;
                }
            }

            if let Some(ref mut file) = self.wal_file {
                let mut batch_bytes = Vec::new();
                for cmd in &commands {
                    if let PartitionCommand::Write(wcmd) = cmd {
                        let bytes = bincode::serialize(wcmd).expect("Failed to serialize command");
                        let len = bytes.len() as u64;
                        batch_bytes.extend_from_slice(&len.to_le_bytes());
                        batch_bytes.extend_from_slice(&bytes);
                    }
                }
                if !batch_bytes.is_empty() {
                    file.write_all(&batch_bytes).expect("Failed to write batch to WAL");
                    file.flush().expect("Failed to flush WAL");
                }
            }

            self.apply_batch_commands(commands);
            self.qsbr.maintenance();
        }
    }

    fn apply_batch_commands(&mut self, commands: Vec<PartitionCommand>) {
        let mut next_pointers = load_clone(&self.shared_pointers);

        for cmd in commands {
            match cmd {
                PartitionCommand::Write(wcmd) => match wcmd {
                    WriteCommand::Insert { entity_id, attributes, attributes_int } => {
                        self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int);
                    }
                    WriteCommand::BatchInsert(batch) => {
                        for (entity_id, attributes, attributes_int) in batch {
                            self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int);
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
                    let block = self.storage.read_block(entity_id, 32);
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

    fn process_insert(
        &mut self,
        next_pointers: &mut AHashMap<usize, MultiVectorPointer>,
        entity_id: usize,
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
    ) {
        let mut new_indices = AHashMap::new();

        self.update_bloom(entity_id);

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
        let _ = self.storage.write_entity(&entity_data);
    }

    fn process_promote(&mut self, next: &mut AHashMap<usize, MultiVectorPointer>, data: EntityData) -> MultiVectorPointer {
        let mut new_indices = AHashMap::new();
        self.update_bloom(data.entity_id);
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

    fn apply_command(&mut self, cmd: PartitionCommand) {
        self.apply_batch_commands(vec![cmd]);
    }

    pub fn replay_wal(&mut self, path: &PathBuf) {
        if !path.exists() { return; }
        let mut file = File::open(path).expect("Failed to open WAL");
        loop {
            let mut len_bytes = [0u8; 8];
            if file.read_exact(&mut len_bytes).is_err() { break; }
            let len = u64::from_le_bytes(len_bytes) as usize;
            let mut buf = vec![0u8; len];
            if file.read_exact(&mut buf).is_err() { break; }
            let cmd: WriteCommand = bincode::deserialize(&buf).expect("Failed to deserialize WAL cmd");
            self.apply_command(PartitionCommand::Write(cmd));
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

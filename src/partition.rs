use crate::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::vec;
use alloc::string::{String, ToString};
use crate::sync::atomic::AtomicPtr;
use crate::platform::FileSystem;


use crate::DualCacheFF;
use crate::bloom::SimpleBloom;

use crate::column::Columns;
use crate::commands::{Attributes, PartitionCommand, WriteCommand};
use crate::qsbr::{QsbrManager, WorkerNode};
use crate::storage::{Storage, EntityData};
use crate::unsafe_core::{load_clone, swap_ptr};
use crate::wal::WalProvider;

/// An RCU snapshot pointer for a single entity, containing the entity ID and a map of
/// attribute names to their column array indices.
///
/// A new `MultiVectorPointer` is published atomically (via [`swap_ptr`]) every time an
/// entity is inserted, updated, or deleted, so readers always see a consistent snapshot
/// without holding any locks.
#[derive(Clone, Debug, Default)]
pub struct MultiVectorPointer {
    /// The unique numeric identifier of the entity this snapshot belongs to.
    pub entity_id: usize,
    /// Maps each attribute name to the index of its value inside the corresponding
    /// [`ColumnArray`](crate::column::ColumnArray).
    pub attribute_indices: AHashMap<String, usize>,
}

/// Represents a single database partition — its in-memory columnar store, persistent
/// storage, write-ahead log, and the command channel that drives it.
///
/// Each partition runs on its own native OS thread. The [`run()`](Partition::run) method
/// is the thread's main loop: it drains incoming [`PartitionCommand`]s, executes batched
/// writes (Group Commit), and performs QSBR maintenance to safely reclaim old RCU
/// snapshots.
pub struct Partition<const N: usize> {
    /// Backing columns array.
    pub columns: Arc<AtomicPtr<Columns<N>>>,
    /// Thread-safe pointers to vector snapshots.
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    /// Receiver channel for incoming commands.
    pub writer_rx: alloc::boxed::Box<dyn crate::platform::MessageQueue>,
    /// Quiescent State Based Reclamation (QSBR) manager.
    pub qsbr: QsbrManager,

    // 持久層與快照
    /// Storage layer instance for this partition.
    pub storage: Arc<Storage>,
    /// Heat tracking cache for hot entities.
    pub hot_index: Arc<DualCacheFF<(u32, usize), ()>>, // Just for heat tracking
    /// The unique numeric ID of this partition.
    pub partition_id: u32,
    /// Thread-safe pointer to the Bloom filter.
    pub bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,

    // WAL 支援
    /// Write-Ahead Log provider.
    pub wal: Arc<dyn WalProvider>,
    /// Count of items in the Bloom filter.
    pub bloom_count: usize,
    /// Total number of bits in the Bloom filter.
    pub bloom_bits: usize,
    /// File system abstraction.
    pub fs: Arc<dyn FileSystem>,
}

impl<const N: usize> Partition<N> {
    /// Creates a new `Partition` worker instance.
    ///
    /// # Arguments
    ///
    /// * `writer_rx` — The command channel this partition will drain in its event loop.
    /// * `columns`   — Shared atomic pointer to the columnar data store.
    /// * `wal`       — Write-Ahead Log provider used to persist commands before applying them.
    /// * `workers`   — QSBR worker list shared across all partition threads.
    /// * `storage_path` — File-system path for the on-disk storage of this partition.
    /// * `fs`        — File-system abstraction (allows injection of in-memory FS for tests).
    /// * `shared_pointers` — Atomic pointer to the global entity-pointer map (RCU-managed).
    /// * `bloom_filter`    — Atomic pointer to the Bloom filter for fast existence checks.
    /// * `hot_index`       — Dual-cache heat tracker shared with the query path.
    /// * `partition_id`    — Numeric identifier of this partition within the database.
    pub fn new(
        writer_rx: alloc::boxed::Box<dyn crate::platform::MessageQueue>,
        columns: Arc<AtomicPtr<Columns<N>>>,
        wal: Arc<dyn WalProvider>,
        workers: Arc<AtomicPtr<WorkerNode>>,
        storage_path: String,
        fs: Arc<dyn FileSystem>,
        shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,
        hot_index: Arc<DualCacheFF<(u32, usize), ()>>,
        partition_id: u32,
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
            hot_index,
            partition_id,
            bloom_filter,
            bloom_count: 0,
            bloom_bits,
            fs,
        }
    }

    /// Inserts `entity_id` into the Bloom filter and triggers a rebuild when the filter
    /// exceeds 70 % occupancy.
    fn update_bloom(&mut self, entity_id: usize) {
        {
            let bloom = crate::unsafe_core::load_ref(&self.bloom_filter);
            bloom.insert(&entity_id);
            self.bloom_count += 1;
        }

        if self.bloom_count > (self.bloom_bits * 7 / 10) {
            self.rebuild_bloom_filter();
        }
    }

    /// Doubles the Bloom filter's bit-count and repopulates it from the current disk index,
    /// then atomically swaps in the new filter via RCU.
    fn rebuild_bloom_filter(&mut self) {
        let _old_bits = self.bloom_bits;
        self.bloom_bits *= 2;
        let new_bloom = SimpleBloom::<N>::new();
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
            crate::unsafe_core::swap_ptr(&self.bloom_filter, new_bloom);
        }
        self.bloom_count = count;
    }

    /// Runs the partition's main event loop on the calling thread (intended to be the
    /// partition's dedicated OS thread).
    ///
    /// The loop:
    /// 1. Blocks on the command channel, waiting for the next [`PartitionCommand`].
    /// 2. Drains up to 1 000 additional pending commands without blocking (Group Commit
    ///    batching — amortises WAL and flush overhead across many writes).
    /// 3. Appends all [`WriteCommand`]s in the batch to the WAL before applying them.
    /// 4. Applies the entire batch via [`apply_batch_commands`](Self::apply_batch_commands).
    /// 5. Flushes the storage layer.
    /// 6. Runs one round of QSBR maintenance to reclaim deferred RCU garbage.
    ///
    /// The loop exits when a [`PartitionCommand::Shutdown`] is received or the channel
    /// is closed.
    ///
    /// Under `std`, a background compaction thread is also spawned that triggers
    /// storage compaction every 300 seconds.
    pub fn run(mut self) {
        #[cfg(feature = "std")]
        {
            let storage_clone = Arc::downgrade(&self.storage);
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(300));
                    if let Some(s) = storage_clone.upgrade() {
                        let _ = s.compact();
                    } else {
                        break;
                    }
                }
            });
        }

        loop {
            let cmd_res = self.writer_rx.recv();
            let cmd = match cmd_res {
                Ok(crate::commands::PartitionCommand::Shutdown) => break,
                Ok(c) => c,
                Err(_) => break,
            };

            let mut commands = vec![cmd];
            for _ in 0..1000 {
                if let Ok(next) = self.writer_rx.try_recv() {
                    if let crate::commands::PartitionCommand::Shutdown = next {
                        commands.push(next);
                        break;
                    }
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

    /// Applies a batch of [`PartitionCommand`]s to the in-memory columnar store and
    /// atomically publishes the updated [`MultiVectorPointer`] map via RCU swap.
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
                        next_pointers.insert(entity_id, MultiVectorPointer {
                            entity_id,
                            attribute_indices: AHashMap::default(),
                        });
                        self.hot_index.remove(&(self.partition_id, entity_id));
                    }
                    WriteCommand::InsertFast { entity_id, epoch, record_type, payload } => {
                        let attributes = Attributes::new();
                        let mut attributes_int = Attributes::new();
                        let mut attributes_blob = Attributes::new();
                        attributes_int.insert("epoch".to_string(), epoch);
                        attributes_int.insert("type".to_string(), record_type);
                        attributes_blob.insert("payload".to_string(), payload.as_ref().clone());
                        self.process_insert(&mut next_pointers, entity_id, attributes, attributes_int, attributes_blob);
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
                crate::commands::PartitionCommand::Shutdown => {}
            }
        }

        let old = swap_ptr(&self.shared_pointers, next_pointers);
        self.qsbr.defer_free(old);
    }

    /// Inserts or updates a single entity's attributes into the columnar store, updates
    /// the Bloom filter and hot-index, and persists the entity to storage.
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
        self.hot_index.insert((self.partition_id, entity_id), ());

        let entity_data = EntityData {
            entity_id,
            attributes,
            attributes_int,
            attributes_blob,
        };
        let _ = self.storage.write_entity(&entity_data);
    }

    /// Promotes a cold entity loaded from disk into the in-memory columnar store,
    /// returning its new [`MultiVectorPointer`].
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
        self.hot_index.insert((self.partition_id, data.entity_id), ());
        ptr
    }

    /// Convenience wrapper — applies a single [`PartitionCommand`] as a one-element batch.
    fn apply_command(&mut self, cmd: PartitionCommand) {
        self.apply_batch_commands(vec![cmd]);
    }

    /// Appends a single [`WriteCommand`] to the WAL (kept for direct-call use-cases;
    /// the hot path uses [`WalProvider::append_batch`] instead).
    #[allow(dead_code)]
    fn log_wal(&mut self, cmd: &WriteCommand) {
        let _ = self.wal.append(cmd);
    }

    /// Replays all WAL entries on startup to restore the in-memory columnar state after
    /// a crash or restart.
    ///
    /// Reads the entire WAL byte stream, decodes each length-prefixed [`WriteCommand`]
    /// record in order, and re-applies them through [`apply_command`](Self::apply_command).
    /// Truncated or corrupt trailing records are silently skipped.
    ///
    /// # Errors
    ///
    /// If [`WalProvider::read_all`] returns an error the method returns early without
    /// applying any commands.
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

    /// Returns the existing string [`ColumnArray`](crate::column::ColumnArray) for `name`,
    /// or creates and atomically publishes a new one via RCU swap.
    fn get_or_create_column_str(&mut self, name: &str) -> Arc<crate::column::ColumnArray<String, N>> {
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

    /// Returns the existing integer [`ColumnArray`](crate::column::ColumnArray) for `name`,
    /// or creates and atomically publishes a new one via RCU swap.
    fn get_or_create_column_int(&mut self, name: &str) -> Arc<crate::column::ColumnArray<u32, N>> {
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

    /// Returns the existing blob [`ColumnArray`](crate::column::ColumnArray) for `name`,
    /// or creates and atomically publishes a new one via RCU swap.
    fn get_or_create_column_blob(&mut self, name: &str) -> Arc<crate::column::ColumnArray<Vec<u8>, N>> {
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

    /// Appends `val` to `col`, reusing a slot from the waitlist (free-list) when one is
    /// available. Atomically publishes both the updated waitlist and data array via RCU
    /// swap and returns the index at which `val` was stored.
    fn insert_into_column<T: Default + Clone>(&mut self, col: &crate::column::ColumnArray<T, N>, val: T) -> usize {
        let mut wl = load_clone(&col.waitlist);
        let mut data = load_clone(&col.data);

        let idx;
        if let Some(i) = wl.pop() {
            data.set(i, val);
            idx = i;
        } else {
            idx = data.len();
            data.push(val);
        }

        let old_wl = swap_ptr(&col.waitlist, wl);
        let old_data = swap_ptr(&col.data, data);
        self.qsbr.defer_free(old_wl);
        self.qsbr.defer_free(old_data);
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::NoopWal;
    use crate::unsafe_core::new_atomic_ptr;
    use crate::partition::WorkerNode;
    use crate::bloom::SimpleBloom;
    
    #[test]
    fn test_partition_apply_commands() {
        let columns = Arc::new(new_atomic_ptr(crate::column::Columns::<512>::new()));
        let wal = Arc::new(NoopWal);
        let workers = Arc::new(new_atomic_ptr(WorkerNode {
            worker: Arc::new(crate::qsbr::WorkerState::default()),
            next: core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()),
        }));
        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::default()));
        let bloom_filter = Arc::new(new_atomic_ptr(SimpleBloom::<512>::new()));
        let hot_index = Arc::new(DualCacheFF::new(crate::Config::with_memory_budget(1, 1)));
        
        let rx = Arc::new(crate::queue::BoundedQueue::new(16));
        let mut partition = Partition::new(
            alloc::boxed::Box::new(crate::platform::StdMessageQueue { rx }),
            columns.clone(),
            wal,
            workers,
            "test_part".to_string(),
            Arc::new(crate::platform::StdFileSystem),
            shared_pointers,
            bloom_filter,
            hot_index,
            1,
        );

        // Test logging wal
        let cmd = WriteCommand::Delete { entity_id: 10 };
        partition.log_wal(&cmd);

        // Test apply command write delete
        partition.apply_command(PartitionCommand::Write(WriteCommand::Delete { entity_id: 10 }));

        // Test insert to trigger column creation and process_promote
        let mut attrs = crate::AHashMap::default();
        attrs.insert("s".to_string(), crate::commands::ColumnValue::Str("a".to_string()));
        attrs.insert("i".to_string(), crate::commands::ColumnValue::Int(1));
        attrs.insert("b".to_string(), crate::commands::ColumnValue::Blob(vec![1]));
        partition.apply_command(PartitionCommand::Write(crate::commands::WriteCommand::insert(20, attrs)));
        
        // Test rebuild bloom filter manually
        partition.rebuild_bloom_filter();

        // Test process_promote
        let mut ptr_map = AHashMap::default();
        let mut attrs_str = crate::commands::Attributes::new();
        attrs_str.insert("s2".to_string(), "b".to_string());
        let mut attrs_int = crate::commands::Attributes::new();
        attrs_int.insert("i2".to_string(), 2);
        let mut attrs_blob = crate::commands::Attributes::new();
        attrs_blob.insert("b2".to_string(), vec![2]);
        
        let entity_data = crate::storage::EntityData {
            entity_id: 30,
            attributes: attrs_str,
            attributes_int: attrs_int,
            attributes_blob: attrs_blob,
        };
        partition.process_promote(&mut ptr_map, entity_data);

        // Test replay wal (NoopWal returns empty so it shouldn't panic)
        partition.replay_wal();
        
        // Test applying batch with multiple commands
        let batch = PartitionCommand::Write(WriteCommand::BatchInsert(vec![(
            40,
            crate::commands::Attributes::new(),
            crate::commands::Attributes::new(),
            crate::commands::Attributes::new(),
        )]));
        partition.apply_batch_commands(vec![batch]);
    }
}


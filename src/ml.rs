use crate::cache::HitCache;
use crate::{
    core::commands::Attributes,
    core::commands::WriteCommand,
    core::query::{QueryNode, QueryResult},
    engine::dispatcher::CdDBDispatcher,
    engine::dispatcher::UserWriter,
};
use std::boxed::Box;
use std::string::ToString;
use std::sync::Arc;
use std::vec::Vec;

/// A Tiered Tensor Cache leveraging cdDB HitCache and L3 disk persistence.
/// Provides O(1) lock-free read access and fast disk eviction for tensors.
pub struct TieredTensorCache {
    pub cache: HitCache<u64, Arc<[i32]>, 1024, 2048, 4096, 7168>,
    db: Box<CdDBDispatcher<1024>>,
    kv_writer: UserWriter,
    pub block_size: usize,
    pub hidden_dim: usize,
    session_id: u32,
}

impl TieredTensorCache {
    pub fn new(session_id: u32, hidden_dim: usize, block_size: usize) -> Self {
        let cache = HitCache::new();

        let mut db = std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(|| {
                Box::new(CdDBDispatcher::<1024>::new_std(
                    None,
                    crate::CacheConfig::default(),
                ))
            })
            .unwrap()
            .join()
            .unwrap();

        let kv_writer = db.register_partition("tensor_storage".to_string());

        Self {
            cache,
            db,
            kv_writer,
            block_size,
            hidden_dim,
            session_id,
        }
    }

    /// Helper to compute DualCache Hash Key
    fn block_key(&self, block_idx: usize) -> u64 {
        ((self.session_id as u64) << 32) | (block_idx as u64)
    }

    /// Pin System Prompt permanently in T1.
    pub fn insert_system_prompt(&self, block_idx: usize, tensor_data: Vec<i32>) {
        let key = self.block_key(block_idx);
        let payload: Arc<[i32]> = tensor_data.into_boxed_slice().into();

        self.cache.insert(key, payload.clone());
        self.persist_to_disk(block_idx, payload);
    }

    /// Insert normal tensor block.
    pub fn insert_block(&self, block_idx: usize, tensor_data: Vec<i32>) {
        let key = self.block_key(block_idx);
        let payload: Arc<[i32]> = tensor_data.into_boxed_slice().into();

        self.cache.insert(key, payload.clone());
        self.persist_to_disk(block_idx, payload);
    }

    fn persist_to_disk(&self, block_idx: usize, payload: Arc<[i32]>) {
        // Transmute Vec<i32> to Vec<u8> for storage
        let byte_len = payload.len() * 4;
        let bytes = unsafe { std::slice::from_raw_parts(payload.as_ptr() as *const u8, byte_len) };

        let mut attributes_blob = Attributes::new();
        attributes_blob.insert("tensor".to_string(), bytes.to_vec());

        self.kv_writer
            .send(WriteCommand::Insert {
                entity_id: self.block_key(block_idx) as usize,
                attributes: Attributes::new(),
                attributes_int: Attributes::new(),
                attributes_blob,
            })
            .unwrap();
    }

    /// Fetch block. If missed, read from cdDB Disk.
    pub fn fetch_block(&self, block_idx: usize) -> Option<Arc<[i32]>> {
        let key = self.block_key(block_idx);

        if let Some(block) = self.cache.get(&key) {
            return Some(block);
        }

        let route = self.db.get_route("tensor_storage")?;

        let mut loaded = None;
        let nodes = [QueryNode::Get {
            entity_id: key as usize,
            attr: "tensor",
        }];
        route.execute_batch(&nodes, |res| {
            if let QueryResult::Blob(b) = res {
                let floats_len = b.len() / 4;
                let mut vec_f32 = Vec::with_capacity(floats_len);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        b.as_ptr() as *const i32,
                        vec_f32.as_mut_ptr(),
                        floats_len,
                    );
                    vec_f32.set_len(floats_len);
                }
                let slice: Arc<[i32]> = vec_f32.into_boxed_slice().into();
                loaded = Some(slice);
            }
        });

        if let Some(payload) = loaded {
            self.cache.insert(key, payload.clone());
            return Some(payload);
        }

        None
    }

    /// Cache Invalidation: Purge blocks from cdDB persistent storage.
    pub fn invalidate_blocks(&self, start_block: usize, end_block: usize) {
        for block_idx in start_block..=end_block {
            let key = self.block_key(block_idx);

            let _ = self.kv_writer.send(WriteCommand::Delete {
                entity_id: key as usize,
            });
        }
    }
}

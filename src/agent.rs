use crate::cache::HitCache;
use crate::{
    core::commands::Attributes, core::commands::WriteCommand, engine::dispatcher::CdDBDispatcher,
    engine::dispatcher::UserWriter,
};
use std::string::{String, ToString};
use std::sync::{Arc, OnceLock};
use std::vec::Vec;

/// The Memory & State Mesh
/// Bridges the mmap model loader, DualCacheFF routing, and cdDB disk persistence.
pub struct MemoryMesh {
    /// O(1) Wait-Free routing state machine mapping Intent Hash -> Success State.
    pub cache: HitCache<u64, Arc<String>, 1024, 2048, 4096, 7168>,
    /// High-performance synchronous persistent storage engine.
    _db: CdDBDispatcher<1024>,
    workflows_writer: UserWriter,
    temporal_writer: UserWriter,
}

static GLOBAL_MESH: OnceLock<MemoryMesh> = OnceLock::new();

impl MemoryMesh {
    pub fn global() -> &'static MemoryMesh {
        GLOBAL_MESH.get_or_init(|| {
            // MemoryMesh initialization requires a massive stack due to DualCacheFF/CdDBDispatcher size.
            // We spawn a temporary thread with 128MB stack to initialize it safely.
            std::thread::Builder::new()
                .stack_size(128 * 1024 * 1024)
                .spawn(MemoryMesh::new)
                .unwrap()
                .join()
                .unwrap()
        })
    }

    pub fn new() -> Self {
        let cache = HitCache::new();

        // Initialize cdDB for persisting long-text state and workflows
        let mut db = std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(|| CdDBDispatcher::<1024>::new_std(None, crate::CacheConfig::default()))
            .unwrap()
            .join()
            .unwrap();

        let workflows_writer = db.register_partition("workflows".to_string());
        let temporal_writer = db.register_partition("temporal_log".to_string());

        Self {
            cache,
            _db: db,
            workflows_writer,
            temporal_writer,
        }
    }

    /// Logs a successful workflow intent hash to the fast-path cache.
    pub fn cache_intent_success(&self, intent_hash: u64, result: String) {
        self.cache.insert(intent_hash, Arc::new(result));
    }

    /// Persists a complex workflow or long-text memory into the cdDB WAL and SSD.
    pub fn persist_workflow(&self, entity_id: u32, workflow_json: &str) {
        let mut attributes = Attributes::new();
        attributes.insert("workflow_data".to_string(), workflow_json.to_string());

        let cmd = WriteCommand::Insert {
            entity_id: entity_id as usize,
            attributes,
            attributes_int: Attributes::new(),
            attributes_blob: Attributes::new(),
        };

        let _ = self.workflows_writer.send(cmd);
    }

    /// Exposes a way to retrieve stored workflows or metadata from the cdDB partition.
    pub fn get_workflow(&self, entity_id: u32) -> Option<String> {
        let mut result = None;
        let route = self._db.get_route("workflows")?;

        let nodes = [crate::core::query::QueryNode::Get {
            entity_id: entity_id as usize,
            attr: "workflow_data",
        }];

        route.execute_batch(&nodes, |res| {
            if let crate::core::query::QueryResult::Str(s) = res {
                result = Some(s.clone());
            }
        });

        result
    }

    /// Exposes the inner wait-free DualCache lookup for O(1) route verification.
    pub fn get_cached_intent(&self, intent_hash: u64) -> Option<Arc<String>> {
        self.cache.get(&intent_hash)
    }

    /// Persists a temporal snapshot (e.g. an epoch) of a ChaosState or Workflow.
    pub fn persist_temporal_state(&self, workflow_id: u32, epoch: u32, state_payload: Vec<u8>) {
        let entity_id = ((workflow_id as usize) << 32) | (epoch as usize);

        let cmd = WriteCommand::InsertFast {
            entity_id,
            epoch,
            record_type: 1, // 1 for ChaosState snapshot
            payload: std::sync::Arc::new(state_payload),
        };

        let _ = self.temporal_writer.send(cmd);
    }

    /// Retrieves a temporal snapshot of a ChaosState or Workflow at a specific epoch.
    pub fn get_temporal_state(&self, workflow_id: u32, epoch: u32) -> Option<Vec<u8>> {
        let entity_id = ((workflow_id as usize) << 32) | (epoch as usize);

        let mut result_payload = None;
        let route = self._db.get_route("temporal_log")?;

        let nodes = [crate::core::query::QueryNode::Get {
            entity_id,
            attr: "payload",
        }];

        route.execute_batch(&nodes, |res| {
            if let crate::core::query::QueryResult::Blob(b) = res {
                result_payload = Some(b);
            }
        });

        result_payload
    }
}

impl Default for MemoryMesh {
    fn default() -> Self {
        Self::new()
    }
}

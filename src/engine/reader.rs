use alloc::sync::Arc;
use crate::core::query::{PartitionRoute, Query};

/// Safe abstraction to execute queries without exposing RCU/QSBR primitives.
/// Used directly by callers like ServerGo to read partition data cleanly.
pub struct PartitionReader<const N: usize> {
    route: Arc<PartitionRoute<N>>,
}

impl<const N: usize> PartitionReader<N> {
    /// Create a new `PartitionReader` wrapping a `PartitionRoute`.
    pub fn new(route: Arc<PartitionRoute<N>>) -> Self {
        Self { route }
    }

    /// Retrieves an integer attribute safely using an internal query session.
    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        let query = Query::new(&self.route);
        let session = query.session();
        session.get_int(entity_id, attr)
    }

    /// Retrieves a blob attribute safely using an internal query session.
    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<alloc::vec::Vec<u8>> {
        let query = Query::new(&self.route);
        let session = query.session();
        session.get_blob(entity_id, attr)
    }

    /// Retrieves a string attribute safely using an internal query session.
    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<alloc::string::String> {
        let query = Query::new(&self.route);
        let session = query.session();
        session.get_str(entity_id, attr)
    }
}

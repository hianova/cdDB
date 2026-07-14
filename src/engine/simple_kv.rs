#[cfg(feature = "std")]
use alloc::string::{String, ToString};
#[cfg(feature = "std")]
use alloc::sync::Arc;
#[cfg(feature = "std")]
use alloc::vec::Vec;

#[cfg(feature = "std")]
use crate::{
    CacheConfig, CdDBDispatcher, WalMode,
    core::commands::{Attributes, WriteCommand},
    core::query::{QueryNode, QueryResult},
};

/// A highly simplified, beginner-friendly Key-Value store facade over cdDB.
///
/// This encapsulates all dispatcher, worker, and partition boilerplate, allowing
/// standard simple `put` and `get` semantics for rapid development.
#[cfg(feature = "std")]
pub struct SimpleKvStore {
    dispatcher: Arc<CdDBDispatcher<1024>>,
    writer: crate::engine::dispatcher::UserWriter,
    partition_name: String,
}

#[cfg(feature = "std")]
impl SimpleKvStore {
    /// Opens or creates a simple KV store at the specified directory path.
    /// Uses default CacheConfig (with Daemon Mode enabled).
    pub fn open(path: &str) -> Self {
        let mut dispatcher =
            CdDBDispatcher::<1024>::new_std(Some(path.to_string()), CacheConfig::default());

        let partition_name = "default".to_string();

        // Register a partition with WAL persistence enabled.
        let writer = dispatcher.register_partition_with_wal(
            partition_name.clone(),
            None,
            WalMode::Async100ms,
        );

        Self {
            dispatcher: Arc::new(dispatcher),
            writer,
            partition_name,
        }
    }

    /// Stores a binary blob associated with a numeric key.
    pub fn put(&self, key: usize, value: Vec<u8>) -> Result<(), &'static str> {
        let mut attributes_blob = Attributes::new();
        attributes_blob.insert("data".to_string(), value);

        let cmd = WriteCommand::Insert {
            entity_id: key,
            attributes: Attributes::new(),
            attributes_int: Attributes::new(),
            attributes_blob,
        };

        self.writer.send(cmd)
    }

    /// Retrieves a binary blob associated with a numeric key, if it exists.
    pub fn get(&self, key: usize) -> Option<Vec<u8>> {
        let node = QueryNode::Get {
            entity_id: key,
            attr: "data",
        };

        let mut res = None;
        self.dispatcher
            .execute_batch(&self.partition_name, &[node], |result| {
                if let QueryResult::Blob(b) = result {
                    res = Some(b);
                }
            });

        res
    }

    /// Deletes a key from the store.
    pub fn delete(&self, key: usize) -> Result<(), &'static str> {
        let cmd = WriteCommand::Delete { entity_id: key };
        self.writer.send(cmd)
    }
}

#[cfg(test)]
#[cfg(feature = "std")]
mod tests {
    use super::*;
    use alloc::vec;
    use std::time::Duration;

    #[test]
    #[ignore]
    fn test_simple_kv_store() {
        let store = SimpleKvStore::open("/tmp/cddb_test_simple_kv");

        store.put(42, vec![1, 2, 3]).unwrap();

        // Let background workers process
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(store.get(42), Some(vec![1, 2, 3]));

        store.delete(42).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(store.get(42), None);
    }
    #[test]
    #[ignore]
    fn test_stack_overflow() {
        let _d = CdDBDispatcher::<1024>::new_std(None, crate::CacheConfig::default());
    }
}

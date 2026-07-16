use alloc::vec::Vec;
use alloc::sync::Arc;

#[cfg(feature = "std")]
use alloc::string::{String, ToString};

/// A high-level unified persistence trait for cdDB tiered storage backend.
/// Exposes standard CRUD operations.
pub trait CdDBStore {
    type Value;

    /// Write a key-value attribute associated with a given entity.
    fn put(&self, entity_id: usize, key: &str, value: Self::Value) -> Result<(), &'static str>;

    /// Retrieve the key-value attribute associated with a given entity.
    fn get(&self, entity_id: usize, key: &str) -> Option<Self::Value>;

    /// Delete all attributes associated with a given entity.
    fn delete(&self, entity_id: usize) -> Result<(), &'static str>;
}

/// A high-level facade representing a single registered partition.
/// Provides synchronous read/write methods to hide execution channel boilerplate.
#[cfg(feature = "std")]
pub struct CdDBPartition<'a, const N: usize> {
    pub name: String,
    pub writer: crate::engine::dispatcher::UserWriter,
    pub dispatcher: &'a crate::CdDBDispatcher<N>,
}

#[cfg(feature = "std")]
impl<'a, const N: usize> CdDBPartition<'a, N> {
    /// Write a string attribute to this partition.
    pub fn write_str(&self, entity_id: usize, attr: &str, value: &str) -> Result<(), &'static str> {
        let mut attributes = crate::core::commands::Attributes::new();
        attributes.insert(attr.to_string(), value.to_string());
        let cmd = crate::core::commands::WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int: crate::core::commands::Attributes::new(),
            attributes_blob: crate::core::commands::Attributes::new(),
        };
        self.writer.send(cmd)
    }

    /// Write a binary blob attribute to this partition.
    pub fn write_blob(
        &self,
        entity_id: usize,
        attr: &str,
        data: Vec<u8>,
    ) -> Result<(), &'static str> {
        let mut attributes_blob = crate::core::commands::Attributes::new();
        attributes_blob.insert(attr.to_string(), data);
        let cmd = crate::core::commands::WriteCommand::Insert {
            entity_id,
            attributes: crate::core::commands::Attributes::new(),
            attributes_int: crate::core::commands::Attributes::new(),
            attributes_blob,
        };
        self.writer.send(cmd)
    }

    /// Write a fast epoch-based snapshot payload.
    pub fn write_epoch_snapshot(
        &self,
        entity_id: usize,
        epoch: u32,
        record_type: u32,
        payload: Vec<u8>,
    ) -> Result<(), &'static str> {
        let cmd = crate::core::commands::WriteCommand::InsertFast {
            entity_id,
            epoch,
            record_type,
            payload: Arc::new(payload),
        };
        self.writer.send(cmd)
    }

    /// Read a string attribute.
    pub fn read_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        let node = crate::core::query::QueryNode::Get { entity_id, attr };
        let mut res = None;
        self.dispatcher
            .execute_batch(&self.name, &[node], |result| {
                if let crate::core::query::QueryResult::Str(s) = result {
                    res = Some(s);
                }
            });
        res
    }

    /// Read a binary blob attribute.
    pub fn read_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        let node = crate::core::query::QueryNode::Get { entity_id, attr };
        let mut res = None;
        self.dispatcher
            .execute_batch(&self.name, &[node], |result| {
                if let crate::core::query::QueryResult::Blob(b) = result {
                    res = Some(b);
                }
            });
        res
    }

    /// Delete all attributes of an entity.
    pub fn delete(&self, entity_id: usize) -> Result<(), &'static str> {
        let cmd = crate::core::commands::WriteCommand::Delete { entity_id };
        self.writer.send(cmd)
    }
}

/// A standard string attribute store wrapping `CdDBPartition`.
#[cfg(feature = "std")]
pub struct CdDBStrStore<'a, const N: usize> {
    partition: CdDBPartition<'a, N>,
}

#[cfg(feature = "std")]
impl<'a, const N: usize> CdDBStrStore<'a, N> {
    pub fn new(partition: CdDBPartition<'a, N>) -> Self {
        Self { partition }
    }
}

#[cfg(feature = "std")]
impl<'a, const N: usize> CdDBStore for CdDBStrStore<'a, N> {
    type Value = String;

    fn put(&self, entity_id: usize, key: &str, value: Self::Value) -> Result<(), &'static str> {
        self.partition.write_str(entity_id, key, &value)
    }

    fn get(&self, entity_id: usize, key: &str) -> Option<Self::Value> {
        self.partition.read_str(entity_id, key)
    }

    fn delete(&self, entity_id: usize) -> Result<(), &'static str> {
        self.partition.delete(entity_id)
    }
}

/// A standard binary blob attribute store wrapping `CdDBPartition`.
#[cfg(feature = "std")]
pub struct CdDBBlobStore<'a, const N: usize> {
    partition: CdDBPartition<'a, N>,
}

#[cfg(feature = "std")]
impl<'a, const N: usize> CdDBBlobStore<'a, N> {
    pub fn new(partition: CdDBPartition<'a, N>) -> Self {
        Self { partition }
    }
}

#[cfg(feature = "std")]
impl<'a, const N: usize> CdDBStore for CdDBBlobStore<'a, N> {
    type Value = Vec<u8>;

    fn put(&self, entity_id: usize, key: &str, value: Self::Value) -> Result<(), &'static str> {
        self.partition.write_blob(entity_id, key, value)
    }

    fn get(&self, entity_id: usize, key: &str) -> Option<Self::Value> {
        self.partition.read_blob(entity_id, key)
    }

    fn delete(&self, entity_id: usize) -> Result<(), &'static str> {
        self.partition.delete(entity_id)
    }
}

#[cfg(test)]
#[cfg(feature = "std")]
mod tests {
    use super::*;
    use crate::engine::dispatcher::CdDBDispatcher;
    use crate::io::platform::{StdExecutor, StdFileSystem};
    use alloc::vec;
    use std::time::Duration;

    fn make_test_dispatcher() -> CdDBDispatcher<1024> {
        CdDBDispatcher::<1024>::new(
            None,
            Arc::new(StdFileSystem),
            Arc::new(StdExecutor),
            crate::CacheConfig::default(),
        )
    }

    #[test]
    #[ignore]
    fn test_facade_str_store() {
        let mut dispatcher = make_test_dispatcher();
        let writer = dispatcher
            .register_partition_with_budget("/tmp/cddb_test_facade_str".to_string(), 1024);
        let partition = CdDBPartition {
            name: "/tmp/cddb_test_facade_str".to_string(),
            writer,
            dispatcher: &dispatcher,
        };
        let store = CdDBStrStore::new(partition);

        store.put(1, "name", "alice".to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(store.get(1, "name"), Some("alice".to_string()));

        store.delete(1).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(store.get(1, "name"), None);
    }

    #[test]
    #[ignore]
    fn test_facade_blob_store() {
        let mut dispatcher = make_test_dispatcher();
        let writer = dispatcher
            .register_partition_with_budget("/tmp/cddb_test_facade_blob".to_string(), 1024);
        let partition = CdDBPartition {
            name: "/tmp/cddb_test_facade_blob".to_string(),
            writer,
            dispatcher: &dispatcher,
        };
        let store = CdDBBlobStore::new(partition);

        store.put(2, "data", vec![1, 2, 3]).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(store.get(2, "data"), Some(vec![1, 2, 3]));

        store.delete(2).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(store.get(2, "data"), None);
    }

    #[test]
    #[ignore]
    fn test_facade_fast_insert() {
        let mut dispatcher = make_test_dispatcher();
        let writer = dispatcher
            .register_partition_with_budget("/tmp/cddb_test_facade_fast".to_string(), 1024);
        let partition = CdDBPartition {
            name: "/tmp/cddb_test_facade_fast".to_string(),
            writer,
            dispatcher: &dispatcher,
        };

        partition
            .write_epoch_snapshot(3, 100, 1, vec![9, 9, 9])
            .unwrap();
        std::thread::sleep(Duration::from_millis(50));
    }
}

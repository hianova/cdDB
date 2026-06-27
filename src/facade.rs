use alloc::sync::Arc;
use crate::{DualCacheFF, Config};
#[cfg(feature = "std")]
use alloc::string::{String, ToString};
#[cfg(feature = "std")]
use alloc::vec::Vec;

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

pub struct CdDBManagedCache<K, V>
where
    K: core::hash::Hash + Eq + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub inner: Arc<DualCacheFF<K, V>>,
}

impl<K, V> CdDBManagedCache<K, V>
where
    K: core::hash::Hash + Eq + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    /// Create a new managed cache with the given budget configuration.
    pub fn new(config: Config) -> Self {
        Self {
            inner: Arc::new(DualCacheFF::new(config)),
        }
    }

    /// Insert a key-value pair asynchronously (telemetry-buffered).
    #[inline(always)]
    pub fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    /// Read a value from the cache.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key)
    }

    /// Remove a key from the cache.
    #[inline(always)]
    pub fn remove(&self, key: &K) {
        self.inner.remove(key);
    }

    /// Pre-warm/pin a key to the Hot Tier (T1).
    pub fn pin_to_t1(&self, key: K, value: V) {
        #[cfg(all(feature = "dualcache-ff", feature = "std"))]
        {
            let session = self.inner.begin_cold_start_session();
            session.warmup_batch([(key, value)]);
        }
        #[cfg(all(feature = "dualcache-ff", not(feature = "std")))]
        {
            self.inner.insert_t1(key, value);
        }
        #[cfg(not(feature = "dualcache-ff"))]
        {
            let _ = (key, value);
        }
    }

    /// Returns the health status of the Daemon.
    pub fn daemon_health(&self) -> crate::DaemonStatus {
        self.inner.daemon_health()
    }

    /// Shutdown the daemon gracefully.
    pub fn shutdown_gracefully(&self, timeout: Option<core::time::Duration>) {
        self.inner.shutdown_gracefully(timeout);
    }

    /// Suspend background daemon polling.
    pub fn suspend(&self) {
        self.inner.suspend();
    }

    /// Resume background daemon polling.
    pub fn resume(&self) {
        self.inner.resume();
    }
}

impl<K, V> Drop for CdDBManagedCache<K, V>
where
    K: core::hash::Hash + Eq + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
{
    fn drop(&mut self) {
        // Shutdown background daemon gracefully when dropping the facade
        self.inner.shutdown_gracefully(Some(core::time::Duration::from_millis(100)));
    }
}

/// A high-level facade representing a single registered partition.
/// Provides synchronous read/write methods to hide execution channel boilerplate.
#[cfg(feature = "std")]
pub struct CdDBPartition<'a, const N: usize> {
    pub name: String,
    pub writer: crate::dispatcher::UserWriter,
    pub dispatcher: &'a crate::CdDBDispatcher<N>,
}

#[cfg(feature = "std")]
impl<'a, const N: usize> CdDBPartition<'a, N> {
    /// Write a string attribute to this partition.
    pub fn write_str(&self, entity_id: usize, attr: &str, value: &str) -> Result<(), &'static str> {
        let mut attributes = crate::commands::Attributes::new();
        attributes.insert(attr.to_string(), value.to_string());
        let cmd = crate::commands::WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int: crate::commands::Attributes::new(),
            attributes_blob: crate::commands::Attributes::new(),
        };
        self.writer.send(cmd)
    }

    /// Write a binary blob attribute to this partition.
    pub fn write_blob(&self, entity_id: usize, attr: &str, data: Vec<u8>) -> Result<(), &'static str> {
        let mut attributes_blob = crate::commands::Attributes::new();
        attributes_blob.insert(attr.to_string(), data);
        let cmd = crate::commands::WriteCommand::Insert {
            entity_id,
            attributes: crate::commands::Attributes::new(),
            attributes_int: crate::commands::Attributes::new(),
            attributes_blob,
        };
        self.writer.send(cmd)
    }

    /// Write a fast epoch-based snapshot payload.
    pub fn write_epoch_snapshot(&self, entity_id: usize, epoch: u32, record_type: u32, payload: Vec<u8>) -> Result<(), &'static str> {
        let cmd = crate::commands::WriteCommand::InsertFast {
            entity_id,
            epoch,
            record_type,
            payload: Arc::new(payload),
        };
        self.writer.send(cmd)
    }

    /// Read a string attribute.
    pub fn read_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        let node = crate::query::QueryNode::Get {
            entity_id,
            attr,
        };
        let mut res = None;
        self.dispatcher.execute_batch(&self.name, &[node], |result| {
            if let crate::query::QueryResult::Str(s) = result {
                res = Some(s);
            }
        });
        res
    }

    /// Read a binary blob attribute.
    pub fn read_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        let node = crate::query::QueryNode::Get {
            entity_id,
            attr,
        };
        let mut res = None;
        self.dispatcher.execute_batch(&self.name, &[node], |result| {
            if let crate::query::QueryResult::Blob(b) = result {
                res = Some(b);
            }
        });
        res
    }

    /// Delete all attributes of an entity.
    pub fn delete(&self, entity_id: usize) -> Result<(), &'static str> {
        let cmd = crate::commands::WriteCommand::Delete { entity_id };
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

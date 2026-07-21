use core::marker::PhantomData;
use alloc::string::String;
use crate::engine::dispatcher::CdDBDispatcher;
use crate::core::commands::{Attributes, WriteCommand};
use crate::core::query::{QueryNode, QueryResult};

/// Generic Typed Cache Interface over `cdDB`.
/// Requires `serde::Serialize` and `serde::de::DeserializeOwned` for `K` and `V`.
#[cfg(feature = "std")]
pub struct TypedCdDbCache<K, V, const N: usize> {
    dispatcher: CdDBDispatcher<N>,
    partition_name: String,
    _marker: PhantomData<(K, V)>,
}

#[cfg(feature = "std")]
impl<K, V, const N: usize> TypedCdDbCache<K, V, N>
where
    K: serde::Serialize + serde::de::DeserializeOwned + core::hash::Hash + core::cmp::Eq,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn new(dispatcher: CdDBDispatcher<N>, partition_name: String) -> Self {
        Self {
            dispatcher,
            partition_name,
            _marker: PhantomData,
        }
    }

    /// Retrieve an entity and deserialize it.
    /// Assumes the entity id can be derived from the key (e.g. by hashing)
    /// and the serialized value is stored in the "data" blob attribute.
    pub fn get(&self, key: &K) -> Option<V> {
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let entity_id = hasher.finish() as usize;

        let nodes = [QueryNode::Get {
            entity_id,
            attr: "data",
        }];
        
        let mut result_val: Option<V> = None;
        self.dispatcher.execute_batch(&self.partition_name, &nodes, |res| {
            if let QueryResult::Blob(data) = res
                && let Ok(v) = bincode::deserialize(&data) {
                    result_val = Some(v);
                }
        });

        result_val
    }

    /// Serialize and insert a value into the cache.
    pub fn insert(&mut self, key: K, value: V) -> Result<(), &'static str> {
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let entity_id = hasher.finish() as usize;

        let serialized_data = bincode::serialize(&value).map_err(|_| "Serialization error")?;
        
        let mut attributes_blob = Attributes::new();
        attributes_blob.insert(String::from("data"), serialized_data);

        let cmd = WriteCommand::Insert {
            entity_id,
            attributes: Attributes::new(),
            attributes_int: Attributes::new(),
            attributes_blob,
        };

        let tx = self.dispatcher.register_partition(self.partition_name.clone());
        tx.send(cmd).map_err(|_| "Failed to send insert command to partition")?;
        
        Ok(())
    }
}

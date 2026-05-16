use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use crossbeam_channel::Sender;
use crate::partition::MultiVectorPointer;

/// 寫入指令屬性封裝
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Attributes<V>(AHashMap<String, V>);

impl<V> Attributes<V> {
    pub fn new() -> Self {
        Self(AHashMap::new())
    }

    pub fn insert(&mut self, key: String, value: V) {
        self.0.insert(key, value);
    }

    pub fn inner(&self) -> &AHashMap<String, V> {
        &self.0
    }
}

impl<V> From<AHashMap<String, V>> for Attributes<V> {
    fn from(map: AHashMap<String, V>) -> Self {
        Self(map)
    }
}

impl<V> IntoIterator for Attributes<V> {
    type Item = (String, V);
    type IntoIter = std::collections::hash_map::IntoIter<String, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// 寫入指令列舉 (持久化用)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteCommand {
    Insert {
        entity_id: usize,
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
    },
    BatchInsert(Vec<(usize, Attributes<String>, Attributes<u32>)>),
    Delete {
        entity_id: usize,
    },
}

/// 內部指令列舉 (同步溝通用)
#[derive(Debug)]
pub enum PartitionCommand {
    Write(WriteCommand),
    InternalLoad {
        entity_id: usize,
        response_tx: Sender<Option<MultiVectorPointer>>,
    },
}

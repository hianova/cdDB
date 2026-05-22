use crate::AHashMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use crate::partition::MultiVectorPointer;

/// cdDB 支援的資料類型
#[derive(Clone, Debug)]
pub enum ColumnValue {
    Str(String),
    Int(u32),
    Blob(Vec<u8>),
}

/// 寫入指令屬性封裝
#[derive(Clone, Debug, Default)]
pub struct Attributes<V>(AHashMap<String, V>);

impl<V> Attributes<V> {
    pub fn new() -> Self {
        Self(AHashMap::default())
    }

    pub fn insert(&mut self, key: String, value: V) {
        self.0.insert(key, value);
    }

    pub fn inner(&self) -> &AHashMap<String, V> {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn encode_to(&self, buf: &mut Vec<u8>, val_encoder: fn(&V, &mut Vec<u8>)) {
        buf.extend_from_slice(&(self.0.len() as u32).to_le_bytes());
        for (k, v) in &self.0 {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k.as_bytes());
            val_encoder(v, buf);
        }
    }

    pub fn decode_from(
        buf: &[u8],
        pos: &mut usize,
        val_decoder: fn(&[u8], &mut usize) -> V,
    ) -> Option<Self> {
        let count = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
        *pos += 4;
        let mut map = AHashMap::with_capacity_and_hasher(count, Default::default());
        for _ in 0..count {
            let k_len = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
            *pos += 4;
            let k = core::str::from_utf8(buf.get(*pos..*pos + k_len)?).ok()?.to_string();
            *pos += k_len;
            let v = val_decoder(buf, pos);
            map.insert(k, v);
        }
        Some(Self(map))
    }
}

impl<V> From<AHashMap<String, V>> for Attributes<V> {
    fn from(map: AHashMap<String, V>) -> Self {
        Self(map)
    }
}

impl<V> IntoIterator for Attributes<V> {
    type Item = (String, V);
    type IntoIter = <crate::AHashMap<String, V> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// 寫入指令列舉 (持久化用)
#[derive(Clone, Debug)]
pub enum WriteCommand {
    Insert {
        entity_id: usize,
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
        attributes_blob: Attributes<Vec<u8>>,
    },
    BatchInsert(Vec<(usize, Attributes<String>, Attributes<u32>, Attributes<Vec<u8>>)>),
    Delete {
        entity_id: usize,
    },
}

impl WriteCommand {
    pub fn insert(
        entity_id: usize,
        typed_attrs: AHashMap<String, ColumnValue>,
    ) -> Self {
        let mut attributes = Attributes::new();
        let mut attributes_int = Attributes::new();
        let mut attributes_blob = Attributes::new();

        for (k, v) in typed_attrs {
            match v {
                ColumnValue::Str(s) => attributes.insert(k, s),
                ColumnValue::Int(i) => attributes_int.insert(k, i),
                ColumnValue::Blob(b) => attributes_blob.insert(k, b),
            }
        }

        WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int,
            attributes_blob,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            WriteCommand::Insert {
                entity_id,
                attributes,
                attributes_int,
                attributes_blob,
            } => {
                buf.push(0); // Type ID
                buf.extend_from_slice(&(*entity_id as u64).to_le_bytes());
                attributes.encode_to(&mut buf, |v: &String, b: &mut Vec<u8>| {
                    b.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    b.extend_from_slice(v.as_bytes());
                });
                attributes_int.encode_to(&mut buf, |v: &u32, b: &mut Vec<u8>| {
                    b.extend_from_slice(&(*v as u32).to_le_bytes());
                });
                attributes_blob.encode_to(&mut buf, |v: &Vec<u8>, b: &mut Vec<u8>| {
                    b.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    b.extend_from_slice(v);
                });
            }
            WriteCommand::BatchInsert(items) => {
                buf.push(1);
                buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for (id, attrs, attrs_int, attrs_blob) in items {
                    buf.extend_from_slice(&(*id as u64).to_le_bytes());
                    attrs.encode_to(&mut buf, |v: &String, b: &mut Vec<u8>| {
                        b.extend_from_slice(&(v.len() as u32).to_le_bytes());
                        b.extend_from_slice(v.as_bytes());
                    });
                    attrs_int.encode_to(&mut buf, |v: &u32, b: &mut Vec<u8>| {
                        b.extend_from_slice(&(*v as u32).to_le_bytes());
                    });
                    attrs_blob.encode_to(&mut buf, |v: &Vec<u8>, b: &mut Vec<u8>| {
                        b.extend_from_slice(&(v.len() as u32).to_le_bytes());
                        b.extend_from_slice(v);
                    });
                }
            }
            WriteCommand::Delete { entity_id } => {
                buf.push(2);
                buf.extend_from_slice(&(*entity_id as u64).to_le_bytes());
            }
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut pos = 0;
        let type_id = *buf.get(pos)?;
        pos += 1;
        match type_id {
            0 => {
                let entity_id = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
                pos += 8;
                let attributes = Attributes::<String>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                    let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
                    *p += 4;
                    let s = core::str::from_utf8(b.get(*p..*p + len).unwrap()).unwrap().to_string();
                    *p += len;
                    s
                })?;
                let attributes_int = Attributes::<u32>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                    let v = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap());
                    *p += 4;
                    v
                })?;
                let attributes_blob = Attributes::<Vec<u8>>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                    let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
                    *p += 4;
                    let v = b.get(*p..*p + len).unwrap().to_vec();
                    *p += len;
                    v
                })?;
                Some(WriteCommand::Insert {
                    entity_id,
                    attributes,
                    attributes_int,
                    attributes_blob,
                })
            }
            1 => {
                let count = u32::from_le_bytes(buf.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
                    pos += 8;
                    let attrs = Attributes::<String>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                        let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
                        *p += 4;
                        let s = core::str::from_utf8(b.get(*p..*p + len).unwrap()).unwrap().to_string();
                        *p += len;
                        s
                    })?;
                    let attrs_int = Attributes::<u32>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                        let v = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap());
                        *p += 4;
                        v
                    })?;
                    let attrs_blob = Attributes::<Vec<u8>>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                        let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
                        *p += 4;
                        let v = b.get(*p..*p + len).unwrap().to_vec();
                        *p += len;
                        v
                    })?;
                    items.push((id, attrs, attrs_int, attrs_blob));
                }
                Some(WriteCommand::BatchInsert(items))
            }
            2 => {
                let entity_id = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
                Some(WriteCommand::Delete { entity_id })
            }
            _ => None,
        }
    }
}

pub trait ResponseSender<T>: Send + Sync {
    fn send(&self, val: T) -> Result<(), String>;
}

#[cfg(feature = "std")]
impl<T: Send + 'static> ResponseSender<T> for std::sync::mpsc::SyncSender<T> {
    fn send(&self, val: T) -> Result<(), String> {
        self.send(val).map_err(|e| e.to_string())
    }
}

/// 內部指令列舉 (同步溝通用)
pub enum PartitionCommand {
    Write(WriteCommand),
    InternalLoad {
        entity_id: usize,
        response_tx: alloc::boxed::Box<dyn ResponseSender<Option<MultiVectorPointer>>>,
    },
}

impl core::fmt::Debug for PartitionCommand {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PartitionCommand::Write(w) => f.debug_tuple("Write").field(w).finish(),
            PartitionCommand::InternalLoad { entity_id, .. } => f.debug_struct("InternalLoad")
                .field("entity_id", entity_id)
                .finish(),
        }
    }
}

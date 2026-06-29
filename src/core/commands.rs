use crate::AHashMap;
use crate::core::column::MultiVectorPointer;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

/// A dynamically typed value that can be stored in a cdDB column.
///
/// cdDB columns are schema-flexible: each attribute key can hold one of three
/// concrete value kinds.  The variant chosen at write time determines which
/// [`Attributes`] bucket the value is placed into and how it is encoded on
/// disk.
#[derive(Clone, Debug)]
pub enum ColumnValue {
    /// A UTF-8 string value.
    Str(String),
    /// An unsigned 32-bit integer value.
    Int(u32),
    /// An arbitrary binary blob.
    Blob(Vec<u8>),
}

/// A typed attribute map used to carry named key-value pairs for write commands.
///
/// `Attributes<V>` is a thin newtype wrapper around [`AHashMap<String, V>`].
/// Three concrete instantiations are used throughout cdDB:
///
/// | Type parameter | Stored values |
/// |---|---|
/// | `String`  | UTF-8 string attributes |
/// | `u32`     | Unsigned integer attributes |
/// | `Vec<u8>` | Binary blob attributes |
///
/// The separation allows each attribute kind to be encoded and decoded
/// independently without boxing or runtime type dispatch.
#[derive(Clone, Debug, Default)]
pub struct Attributes<V>(AHashMap<String, V>);

impl<V> Attributes<V> {
    /// Creates a new, empty attribute map.
    pub fn new() -> Self {
        Self(AHashMap::default())
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map already contains an entry for `key`, the old value is
    /// silently replaced.
    pub fn insert(&mut self, key: String, value: V) {
        self.0.insert(key, value);
    }

    /// Returns a reference to the value associated with `key`, or `None` if
    /// no such key exists.
    pub fn get(&self, key: &str) -> Option<&V> {
        self.0.get(key)
    }

    /// Returns a reference to the underlying [`AHashMap`].
    pub fn inner(&self) -> &AHashMap<String, V> {
        &self.0
    }

    /// Returns the number of key-value pairs in the map.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Serializes the attribute map into `buf` using a caller-supplied value
    /// encoder.
    ///
    /// The binary layout is:
    /// ```text
    /// [entry_count: u32 LE]
    /// for each entry:
    ///   [key_len: u32 LE][key_bytes: UTF-8][value: encoded by val_encoder]
    /// ```
    ///
    /// `val_encoder` receives a reference to the value and the output buffer;
    /// it is responsible for appending the encoded value bytes.
    pub fn encode_to(&self, buf: &mut Vec<u8>, val_encoder: fn(&V, &mut Vec<u8>)) {
        buf.extend_from_slice(&(self.0.len() as u32).to_le_bytes());
        for (k, v) in &self.0 {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k.as_bytes());
            val_encoder(v, buf);
        }
    }

    /// Deserializes an attribute map from `buf` starting at `*pos`, advancing
    /// `*pos` past the consumed bytes.
    ///
    /// `val_decoder` is responsible for reading a single value from `buf`
    /// starting at the cursor position and advancing the cursor accordingly.
    ///
    /// # Returns
    ///
    /// Returns `Some(Self)` on success, or `None` if the buffer is too short,
    /// the entry count is inconsistent, or any key is not valid UTF-8.
    pub fn decode_from(
        buf: &[u8],
        pos: &mut usize,
        val_decoder: fn(&[u8], &mut usize) -> V,
    ) -> Option<Self> {
        let count = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
        *pos += 4;
        let mut map = AHashMap::with_capacity(count);
        for _ in 0..count {
            let k_len = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
            *pos += 4;
            let k = core::str::from_utf8(buf.get(*pos..*pos + k_len)?)
                .ok()?
                .to_string();
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

/// A write command that can be applied to a cdDB partition and persisted to
/// the Write-Ahead Log (WAL).
///
/// Each variant maps to a distinct WAL type-ID byte (`0`–`3`) and is
/// round-trippable through [`WriteCommand::encode`] / [`WriteCommand::decode`].
#[derive(Clone, Debug)]
pub enum WriteCommand {
    /// Insert a standard entity with dynamically typed attributes.
    Insert {
        /// The entity ID to insert.
        entity_id: usize,
        /// String attributes.
        attributes: Attributes<String>,
        /// Integer attributes.
        attributes_int: Attributes<u32>,
        /// Blob attributes.
        attributes_blob: Attributes<Vec<u8>>,
    },
    /// Batch insert multiple standard entities in a single WAL record.
    ///
    /// Each tuple contains `(entity_id, string_attrs, int_attrs, blob_attrs)`.
    BatchInsert(
        Vec<(
            usize,
            Attributes<String>,
            Attributes<u32>,
            Attributes<Vec<u8>>,
        )>,
    ),
    /// Delete an entity.
    Delete {
        /// The entity ID to delete.
        entity_id: usize,
    },
    /// Fast insertion path, skipping dynamic attributes parsing.
    InsertFast {
        /// The entity ID to insert.
        entity_id: usize,
        /// Fast insertion epoch tracking.
        epoch: u32,
        /// The specific type of the record.
        record_type: u32,
        /// The raw payload bytes.
        payload: alloc::sync::Arc<Vec<u8>>,
    },
}

impl WriteCommand {
    /// Constructs a [`WriteCommand::Insert`] from a heterogeneous attribute map.
    ///
    /// Iterates over `typed_attrs` and routes each entry to the appropriate
    /// typed bucket (`attributes`, `attributes_int`, or `attributes_blob`)
    /// based on its [`ColumnValue`] variant.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use cddb::commands::{WriteCommand, ColumnValue};
    /// use cddb::AHashMap;
    ///
    /// let mut attrs = AHashMap::default();
    /// attrs.insert("name".to_string(), ColumnValue::Str("Alice".to_string()));
    /// attrs.insert("age".to_string(), ColumnValue::Int(30));
    ///
    /// let cmd = WriteCommand::insert(42, attrs);
    /// ```
    pub fn insert(entity_id: usize, typed_attrs: AHashMap<String, ColumnValue>) -> Self {
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

    /// Encodes the command into a byte buffer suitable for WAL persistence.
    ///
    /// The first byte is a type discriminant:
    ///
    /// | Byte | Variant |
    /// |---|---|
    /// | `0` | [`WriteCommand::Insert`] |
    /// | `1` | [`WriteCommand::BatchInsert`] |
    /// | `2` | [`WriteCommand::Delete`] |
    /// | `3` | [`WriteCommand::InsertFast`] |
    ///
    /// All multi-byte integers are little-endian.  The returned buffer can be
    /// passed directly to [`WriteCommand::decode`] to recover the original
    /// command.
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
                    b.extend_from_slice(&(*v).to_le_bytes());
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
                        b.extend_from_slice(&(*v).to_le_bytes());
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
            WriteCommand::InsertFast {
                entity_id,
                epoch,
                record_type,
                payload,
            } => {
                buf.push(3);
                buf.extend_from_slice(&(*entity_id as u64).to_le_bytes());
                buf.extend_from_slice(&epoch.to_le_bytes());
                buf.extend_from_slice(&record_type.to_le_bytes());
                buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(payload.as_slice());
            }
        }
        buf
    }

    /// Decodes a [`WriteCommand`] from a byte buffer previously produced by
    /// [`WriteCommand::encode`].
    ///
    /// # Returns
    ///
    /// Returns `Some(cmd)` when decoding succeeds, or `None` if:
    /// - `buf` is empty or too short for the declared payload,
    /// - the type-discriminant byte is not in the range `0`–`3`, or
    /// - any UTF-8 key cannot be decoded.
    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut pos = 0;
        let type_id = *buf.get(pos)?;
        pos += 1;
        match type_id {
            0 => {
                let entity_id =
                    u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
                pos += 8;
                let attributes =
                    Attributes::<String>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                        let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap())
                            as usize;
                        *p += 4;
                        let s = core::str::from_utf8(b.get(*p..*p + len).unwrap())
                            .unwrap()
                            .to_string();
                        *p += len;
                        s
                    })?;
                let attributes_int =
                    Attributes::<u32>::decode_from(buf, &mut pos, |b: &[u8], p: &mut usize| {
                        let v = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap());
                        *p += 4;
                        v
                    })?;
                let attributes_blob = Attributes::<Vec<u8>>::decode_from(
                    buf,
                    &mut pos,
                    |b: &[u8], p: &mut usize| {
                        let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap())
                            as usize;
                        *p += 4;
                        let v = b.get(*p..*p + len).unwrap().to_vec();
                        *p += len;
                        v
                    },
                )?;
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
                    let attrs = Attributes::<String>::decode_from(
                        buf,
                        &mut pos,
                        |b: &[u8], p: &mut usize| {
                            let len =
                                u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap())
                                    as usize;
                            *p += 4;
                            let s = core::str::from_utf8(b.get(*p..*p + len).unwrap())
                                .unwrap()
                                .to_string();
                            *p += len;
                            s
                        },
                    )?;
                    let attrs_int = Attributes::<u32>::decode_from(
                        buf,
                        &mut pos,
                        |b: &[u8], p: &mut usize| {
                            let v =
                                u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap());
                            *p += 4;
                            v
                        },
                    )?;
                    let attrs_blob = Attributes::<Vec<u8>>::decode_from(
                        buf,
                        &mut pos,
                        |b: &[u8], p: &mut usize| {
                            let len =
                                u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap())
                                    as usize;
                            *p += 4;
                            let v = b.get(*p..*p + len).unwrap().to_vec();
                            *p += len;
                            v
                        },
                    )?;
                    items.push((id, attrs, attrs_int, attrs_blob));
                }
                Some(WriteCommand::BatchInsert(items))
            }
            2 => {
                let entity_id =
                    u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
                Some(WriteCommand::Delete { entity_id })
            }
            3 => {
                let entity_id =
                    u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
                pos += 8;
                let epoch = u32::from_le_bytes(buf.get(pos..pos + 4)?.try_into().ok()?);
                pos += 4;
                let record_type = u32::from_le_bytes(buf.get(pos..pos + 4)?.try_into().ok()?);
                pos += 4;
                let len = u32::from_le_bytes(buf.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let payload = alloc::sync::Arc::new(buf.get(pos..pos + len)?.to_vec());
                Some(WriteCommand::InsertFast {
                    entity_id,
                    epoch,
                    record_type,
                    payload,
                })
            }
            _ => None,
        }
    }
}

/// Generic trait for sending responses back from background operations.
pub trait ResponseSender<T>: Send + Sync {
    /// Send the value through the response channel.
    fn send(&self, val: T) -> Result<(), String>;
}

#[cfg(feature = "std")]
impl<T: Send + 'static> ResponseSender<T> for std::sync::mpsc::SyncSender<T> {
    fn send(&self, val: T) -> Result<(), String> {
        self.send(val).map_err(|e| e.to_string())
    }
}

/// Internal command enum used for synchronous communication with partition background threads.
pub enum PartitionCommand {
    /// A regular write command (Insert/Delete).
    Write(WriteCommand),
    /// Load an entity synchronously from disk into memory.
    InternalLoad {
        /// The ID of the entity to load.
        entity_id: usize,
        /// The callback channel for the response.
        response_tx: alloc::boxed::Box<dyn ResponseSender<Option<MultiVectorPointer>>>,
    },
    /// Shutdown the partition thread cleanly.
    Shutdown,
}

impl core::fmt::Debug for PartitionCommand {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PartitionCommand::Write(w) => f.debug_tuple("Write").field(w).finish(),
            PartitionCommand::InternalLoad { entity_id, .. } => f
                .debug_struct("InternalLoad")
                .field("entity_id", entity_id)
                .finish(),
            PartitionCommand::Shutdown => f.debug_tuple("Shutdown").finish(),
        }
    }
}

/// IT Operations Log Levels
#[derive(Debug, Clone)]
pub enum LogLevel {
    /// Information level.
    Info,
    /// Warning level.
    Warn,
    /// Error level.
    Error,
    /// Fatal level.
    Fatal,
    /// Debug level.
    Debug,
}

/// A structured record for IT Operations (Monitoring, Logging, etc.)
#[derive(Debug, Clone)]
pub struct ITOpsRecord {
    /// Unix timestamp of the event.
    pub timestamp: u64,
    /// Name of the service generating the log.
    pub service: String,
    /// Node identifier.
    pub node: String,
    /// Severity level of the log.
    pub level: LogLevel,
    /// Log message content.
    pub message: String,
    /// CPU usage snapshot (0.0 to 1.0).
    pub cpu_usage: f32, // 0.0 - 1.0
    /// Memory usage snapshot (0.0 to 1.0).
    pub mem_usage: f32, // 0.0 - 1.0
    /// API response time in milliseconds.
    pub response_time_ms: u32,
}

impl ITOpsRecord {
    /// Converts the structured record into a pair of cdDB-compatible attribute
    /// maps ready for use in a [`WriteCommand::Insert`].
    ///
    /// String attributes returned: `service`, `node`, `level`, `message`.
    ///
    /// Integer (`u32`) attributes returned:
    /// - `timestamp` — the Unix timestamp, truncated to `u32::MAX` via `%`.
    /// - `cpu_milli` — `cpu_usage × 1000` cast to `u32` (e.g. `0.75` → `750`).
    /// - `mem_milli` — `mem_usage × 1000` cast to `u32` (e.g. `0.60` → `600`).
    /// - `response_time` — `response_time_ms` as-is.
    ///
    /// The `× 1000` scaling preserves three decimal places of precision for
    /// floating-point usage ratios within the integer attribute store.
    pub fn to_cd_db_params(&self) -> (Attributes<String>, Attributes<u32>) {
        let mut attrs = AHashMap::default();
        attrs.insert("service".to_string(), self.service.clone());
        attrs.insert("node".to_string(), self.node.clone());
        attrs.insert("level".to_string(), format!("{:?}", self.level));
        attrs.insert("message".to_string(), self.message.clone());

        let mut attrs_int = AHashMap::default();
        attrs_int.insert(
            "timestamp".to_string(),
            (self.timestamp % (u32::MAX as u64)) as u32,
        );
        attrs_int.insert("cpu_milli".to_string(), (self.cpu_usage * 1000.0) as u32);
        attrs_int.insert("mem_milli".to_string(), (self.mem_usage * 1000.0) as u32);
        attrs_int.insert("response_time".to_string(), self.response_time_ms);

        (attrs.into(), attrs_int.into())
    }
}

/// Extension trait for easier ITOps data ingestion
pub trait ITOpsIngest {
    /// Converts and inserts an operations record as a WriteCommand.
    fn insert_ops_record(
        &self,
        entity_id: usize,
        record: ITOpsRecord,
    ) -> crate::core::commands::WriteCommand;
}

impl ITOpsIngest for ITOpsRecord {
    fn insert_ops_record(
        &self,
        entity_id: usize,
        record: ITOpsRecord,
    ) -> crate::core::commands::WriteCommand {
        let (attributes, attributes_int) = record.to_cd_db_params();
        crate::core::commands::WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int,
            attributes_blob: crate::core::commands::Attributes::<Vec<u8>>::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_write_command_encode_decode() {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), 42);

        let cmd = WriteCommand::Insert {
            entity_id: 1,
            attributes: Attributes::new(),
            attributes_int: attrs_int,
            attributes_blob: Attributes::new(),
        };

        let bytes = cmd.encode();
        let decoded = WriteCommand::decode(&bytes).unwrap();

        if let WriteCommand::Insert {
            entity_id,
            attributes_int,
            ..
        } = decoded
        {
            assert_eq!(entity_id, 1);
            assert_eq!(attributes_int.get("val"), Some(&42));
        } else {
            panic!("Decode failed");
        }
    }

    #[test]
    fn test_attributes() {
        let mut attrs = Attributes::new();
        attrs.insert("key".to_string(), "val".to_string());
        assert!(!attrs.is_empty());
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs.get("key"), Some(&"val".to_string()));
        let mut buf = Vec::new();
        attrs.encode_to(&mut buf, |v, b| {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            b.extend_from_slice(v.as_bytes());
        });
        let mut pos = 0;
        let dec = Attributes::<String>::decode_from(&buf, &mut pos, |b, p| {
            let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
            *p += 4;
            let s = core::str::from_utf8(b.get(*p..*p + len).unwrap())
                .unwrap()
                .to_string();
            *p += len;
            s
        })
        .unwrap();
        assert_eq!(dec.get("key"), Some(&"val".to_string()));
    }

    #[test]
    fn test_it_ops() {
        let record = ITOpsRecord {
            timestamp: 1000,
            service: "web".to_string(),
            node: "n1".to_string(),
            level: LogLevel::Info,
            message: "ok".to_string(),
            cpu_usage: 0.5,
            mem_usage: 0.6,
            response_time_ms: 200,
        };
        let cmd = record.insert_ops_record(10, record.clone());
        if let WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int,
            ..
        } = cmd
        {
            assert_eq!(entity_id, 10);
            assert_eq!(attributes.get("service").unwrap(), "web");
            assert_eq!(attributes_int.get("cpu_milli").unwrap(), &500);
        } else {
            panic!("Wrong command type");
        }
    }

    #[test]
    fn test_write_command_insert_helper() {
        use crate::core::commands::ColumnValue;
        let mut attrs = crate::AHashMap::default();
        attrs.insert("s".to_string(), ColumnValue::Str("foo".to_string()));
        attrs.insert("i".to_string(), ColumnValue::Int(42));
        attrs.insert("b".to_string(), ColumnValue::Blob(vec![1, 2]));

        let cmd = WriteCommand::insert(99, attrs);
        if let WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int,
            attributes_blob,
        } = cmd
        {
            assert_eq!(entity_id, 99);
            assert_eq!(attributes.get("s"), Some(&"foo".to_string()));
            assert_eq!(attributes_int.get("i"), Some(&42));
            assert_eq!(attributes_blob.get("b"), Some(&vec![1, 2]));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_decode_invalid() {
        assert!(WriteCommand::decode(&[]).is_none());
        assert!(WriteCommand::decode(&[4]).is_none()); // Invalid variant
    }

    #[test]
    fn test_partition_command_debug() {
        let cmd = PartitionCommand::Shutdown;
        let s = format!("{:?}", cmd);
        assert_eq!(s, "Shutdown");

        let (tx, _rx) = std::sync::mpsc::sync_channel(1);
        let internal_load = PartitionCommand::InternalLoad {
            entity_id: 42,
            response_tx: alloc::boxed::Box::new(tx),
        };
        let s = format!("{:?}", internal_load);
        assert!(s.contains("InternalLoad"));
        assert!(s.contains("entity_id: 42"));
    }

    #[test]
    fn test_batch_and_fast_insert() {
        let fast = WriteCommand::InsertFast {
            entity_id: 1,
            epoch: 2,
            record_type: 3,
            payload: alloc::sync::Arc::new(vec![1, 2, 3]),
        };
        let enc_fast = fast.encode();
        let dec_fast = WriteCommand::decode(&enc_fast).unwrap();
        if let WriteCommand::InsertFast {
            entity_id, epoch, ..
        } = dec_fast
        {
            assert_eq!(entity_id, 1);
            assert_eq!(epoch, 2);
        } else {
            panic!("Failed fast insert");
        }

        let mut attrs_str = Attributes::new();
        attrs_str.insert("k".to_string(), "v".to_string());
        let mut attrs_int = Attributes::new();
        attrs_int.insert("num".to_string(), 100);
        let mut attrs_blob = Attributes::new();
        attrs_blob.insert("data".to_string(), vec![255]);

        let batch = WriteCommand::BatchInsert(vec![(5, attrs_str, attrs_int, attrs_blob)]);
        let enc_batch = batch.encode();
        let dec_batch = WriteCommand::decode(&enc_batch).unwrap();
        if let WriteCommand::BatchInsert(items) = dec_batch {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].0, 5);
            assert_eq!(items[0].1.get("k"), Some(&"v".to_string()));
            assert_eq!(items[0].2.get("num"), Some(&100));
            assert_eq!(items[0].3.get("data"), Some(&vec![255]));
        } else {
            panic!("Failed batch insert");
        }
    }

    #[test]
    fn test_write_command_extra_variants() {
        // Test LogLevel Debug format
        assert_eq!(format!("{:?}", LogLevel::Info), "Info");
        assert_eq!(format!("{:?}", LogLevel::Warn), "Warn");
        assert_eq!(format!("{:?}", LogLevel::Error), "Error");
        assert_eq!(format!("{:?}", LogLevel::Fatal), "Fatal");
        assert_eq!(format!("{:?}", LogLevel::Debug), "Debug");

        // Test WriteCommand::Delete encode/decode
        let del = WriteCommand::Delete { entity_id: 123 };
        let enc_del = del.encode();
        let dec_del = WriteCommand::decode(&enc_del).unwrap();
        if let WriteCommand::Delete { entity_id } = dec_del {
            assert_eq!(entity_id, 123);
        } else {
            panic!("Delete decode failed");
        }

        // Test PartitionCommand formatting
        assert_eq!(
            format!(
                "{:?}",
                PartitionCommand::Write(WriteCommand::Delete { entity_id: 1 })
            ),
            "Write(Delete { entity_id: 1 })"
        );

        // Test Attributes::into_iter
        let mut attrs = Attributes::new();
        attrs.insert("x".to_string(), "y".to_string());
        let mut count = 0;
        for (k, v) in attrs {
            assert_eq!(k, "x");
            assert_eq!(v, "y");
            count += 1;
        }
        assert_eq!(count, 1);

        // Test From<AHashMap> for Attributes
        let mut map = crate::AHashMap::default();
        map.insert("a".to_string(), 1);
        let attrs_from = Attributes::from(map);
        assert_eq!(attrs_from.get("a"), Some(&1));
    }
}

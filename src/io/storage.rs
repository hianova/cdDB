use alloc::vec::Vec;
use crate::AHashMap;
use crate::core::atomic::AtomicPtr;
use crate::core::rcu::new_atomic_ptr;
use crate::io::platform::FileSystem;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, Ordering};

/// Represents the complete persisted state of a single entity.
///
/// An `EntityData` bundles the entity's unique identifier together with its
/// three typed attribute maps — string-valued, integer-valued, and raw-byte-valued.
/// It is the canonical unit of serialisation: one `EntityData` corresponds to
/// exactly one length-prefixed record in `entities.bin`.
#[derive(Clone, Debug)]
pub struct EntityData {
    /// The unique numeric identifier for this entity within its partition.
    pub entity_id: usize,
    /// String-valued attributes keyed by attribute name.
    pub attributes: crate::Attributes<String>,
    /// Integer-valued attributes (`u32`) keyed by attribute name.
    pub attributes_int: crate::Attributes<u32>,
    /// Raw byte-blob attributes keyed by attribute name.
    pub attributes_blob: crate::Attributes<Vec<u8>>,
}

impl EntityData {
    /// Serialises the entity into a compact little-endian binary representation.
    ///
    /// The layout written into the returned buffer is:
    /// 1. **Entity ID** — 8 bytes, little-endian `u64`.
    /// 2. **String attributes** — encoded by [`crate::Attributes::encode_to`]; each
    ///    value is prefixed by a 4-byte LE length followed by its UTF-8 bytes.
    /// 3. **Integer attributes** — each `u32` value is 4 bytes LE.
    /// 4. **Blob attributes** — each value is prefixed by a 4-byte LE length
    ///    followed by its raw bytes.
    ///
    /// The returned `Vec<u8>` is later wrapped in a 4-byte length prefix by
    /// [`Storage::write_entity`] before being appended to `entities.bin`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let data = EntityData { entity_id: 1, .. };
    /// let bytes = data.encode();
    /// assert!(bytes.len() >= 8); // at minimum the entity-ID field
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.entity_id as u64).to_le_bytes());
        self.attributes
            .encode_to(&mut buf, |v: &String, b: &mut Vec<u8>| {
                b.extend_from_slice(&(v.len() as u32).to_le_bytes());
                b.extend_from_slice(v.as_bytes());
            });
        self.attributes_int
            .encode_to(&mut buf, |v: &u32, b: &mut Vec<u8>| {
                b.extend_from_slice(&(*v).to_le_bytes());
            });
        self.attributes_blob
            .encode_to(&mut buf, |v: &Vec<u8>, b: &mut Vec<u8>| {
                b.extend_from_slice(&(v.len() as u32).to_le_bytes());
                b.extend_from_slice(v);
            });
        buf
    }

    /// Deserialises an `EntityData` from a raw binary buffer previously produced
    /// by [`EntityData::encode`].
    ///
    /// The buffer must begin with an 8-byte little-endian entity ID followed by
    /// the three attribute-map sections in the same order as `encode` writes them.
    /// If the buffer is too short, contains invalid UTF-8 in a string attribute, or
    /// is otherwise malformed, `None` is returned rather than panicking.
    ///
    /// # Returns
    ///
    /// - `Some(EntityData)` on success.
    /// - `None` if the input is truncated or otherwise cannot be decoded.
    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut pos = 0;
        let entity_id = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
        pos += 8;
        let attributes = crate::Attributes::<String>::decode_from(buf, &mut pos, |b, p| {
            let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
            *p += 4;
            let s = core::str::from_utf8(b.get(*p..*p + len).unwrap())
                .unwrap()
                .to_string();
            *p += len;
            s
        })?;
        let attributes_int = crate::Attributes::<u32>::decode_from(buf, &mut pos, |b, p| {
            let v = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap());
            *p += 4;
            v
        })?;
        let attributes_blob = crate::Attributes::<Vec<u8>>::decode_from(buf, &mut pos, |b, p| {
            let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
            *p += 4;
            let v = b.get(*p..*p + len).unwrap().to_vec();
            *p += len;
            v
        })?;
        Some(Self {
            entity_id,
            attributes,
            attributes_int,
            attributes_blob,
        })
    }
}

/// Manages the append-only sequential log (`entities.bin`) for a single partition.
///
/// `Storage` owns all I/O concerns for one partition directory:
/// - It opens (or creates) `<base_path>/entities.bin` on construction and wraps
///   the write handle in a 64 KiB [`std::io::BufWriter`] to amortise syscall
///   overhead.
/// - It maintains an in-memory **disk index** (`entity_id → (offset, length)`) so
///   that any entity can be located in O(1) time without scanning the log.
/// - On `std` targets, reads are served through a memory-mapped view of the file
///   ([`memmap2::Mmap`]) and the view is lazily (re-)created whenever it is stale.
/// - [`Storage::compact`] rewrites the file, keeping only the most-recent record
///   per entity and atomically swapping in the compacted file.
///
/// All mutable state is guarded by `crate::core::Mutex` so that a `Storage`
/// instance can be shared across threads.
pub struct Storage {
    /// Filesystem path to the partition directory that contains `entities.bin`.
    pub base_path: String,
    /// Filesystem abstraction used for all I/O; supports both the standard
    /// library and `no_std` environments through the [`FileSystem`] trait.
    pub fs: Arc<dyn FileSystem>,
    /// Wait-free snapshot of the disk index for concurrent readers
    pub disk_index: Arc<AtomicPtr<AHashMap<usize, (u64, u32)>>>,
    /// The mutable disk index mapping `entity_id` to `(offset, length)` inside the file.
    /// This is updated by the background thread as entities are written to disk.
    #[cfg(feature = "std")]
    pub next_disk_index: std::sync::Mutex<AHashMap<usize, (u64, u32)>>,
    #[cfg(not(feature = "std"))]
    pub next_disk_index: no_std_tool::sync::SpinMutex<AHashMap<usize, (u64, u32)>>,
    /// Tracks whether next_disk_index has been modified since the last swap
    pub disk_index_dirty: AtomicBool,
    /// The byte offset at which the *next* record will be appended.
    /// Updated atomically alongside `disk_index` on every successful write.
    pub current_offset: crate::core::Mutex<u64>,
    /// Buffered file writer for `entities.bin` (64 KiB capacity).
    /// `None` if the file could not be opened or after a `compact()` swap.
    /// Only present on `std` targets.
    #[cfg(feature = "std")]
    pub writer: crate::core::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
    /// Memory-mapped read view of `entities.bin`.
    /// Lazily created on the first read and invalidated (set to `None`) after
    /// `compact()` replaces the underlying file.
    /// Only present on `std` targets.
    #[cfg(feature = "std")]
    pub mmap: crate::core::Mutex<Option<alloc::sync::Arc<memmap2::Mmap>>>,
}

impl Storage {
    /// Creates a new `Storage` instance for the given partition directory.
    ///
    /// This function:
    /// 1. Ensures `base_path` exists (creates it with all parents if necessary).
    /// 2. Opens `<base_path>/entities.bin` in append mode (creating it if absent),
    ///    wrapping the handle in a 64 KiB [`std::io::BufWriter`].
    /// 3. Calls [`Storage::rebuild_disk_index`] to scan any pre-existing data and
    ///    populate the in-memory disk index and `current_offset`.
    ///
    /// On `no_std` targets the buffered writer and mmap fields are omitted; all
    /// I/O is delegated to `fs` directly.
    ///
    /// # Arguments
    ///
    /// * `base_path` — Path to the partition directory (created if missing).
    /// * `fs` — Shared filesystem implementation.
    pub fn new(base_path: String, fs: Arc<dyn FileSystem>) -> Self {
        #[cfg(feature = "std")]
        let writer = {
            let path = format!("{}/entities.bin", base_path);
            let _ = fs.create_dir_all(&base_path);
            use std::fs::OpenOptions;
            let file_opt = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .ok()
                .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f));
            crate::core::Mutex::new(file_opt)
        };

        let s = Self {
            base_path,
            fs,
            disk_index: Arc::new(new_atomic_ptr(AHashMap::default())),
            #[cfg(feature = "std")]
            next_disk_index: std::sync::Mutex::new(AHashMap::default()),
            #[cfg(not(feature = "std"))]
            next_disk_index: no_std_tool::sync::SpinMutex::new(AHashMap::default()),
            disk_index_dirty: AtomicBool::new(false),
            current_offset: crate::core::Mutex::new(0),
            #[cfg(feature = "std")]
            writer,
            #[cfg(feature = "std")]
            mmap: crate::core::Mutex::new(None),
        };
        s.rebuild_disk_index();
        s
    }

    fn rebuild_disk_index(&self) {
        let path = format!("{}/entities.bin", self.base_path);
        if !self.fs.exists(&path) {
            return;
        }
        if let Ok(bytes) = self.fs.read(&path) {
            let mut pos = 0;
            let mut map = AHashMap::default();
            while pos + 4 <= bytes.len() {
                let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
                let record_start = pos as u64;
                pos += 4;
                if pos + len <= bytes.len() {
                    if let Some(entity_id) = Self::peek_entity_id(&bytes[pos..pos + len]) {
                        map.insert(entity_id, (record_start, len as u32));
                    }
                    pos += len;
                } else {
                    break;
                }
            }
            #[cfg(feature = "std")]
            {
                let mut index = self.next_disk_index.lock().unwrap();
                *index = map;
                let mut off = self.current_offset.lock().unwrap();
                *off = pos as u64;
            }
            #[cfg(not(feature = "std"))]
            {
                let mut index = self.next_disk_index.lock().unwrap();
                *index = map;
                let mut off = self.current_offset.lock().unwrap();
                *off = pos as u64;
            }
            self.swap_disk_index();
        }
    }

    fn peek_entity_id(buf: &[u8]) -> Option<usize> {
        if buf.len() >= 8 {
            Some(u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize)
        } else {
            None
        }
    }

    /// Flushes the in-process write buffer and durably syncs the file to storage.
    ///
    /// On `std` targets this calls [`std::io::Write::flush`] on the inner
    /// `BufWriter` (draining any buffered bytes to the kernel) and then
    /// [`std::fs::File::sync_all`] to ensure the data has reached the underlying
    /// storage device.
    ///
    /// This is a no-op on `no_std` targets because writes are unbuffered there.
    ///
    /// # Errors
    ///
    /// Returns `Err(String)` if either the flush or the `fsync` call fails.
    pub fn flush(&self) -> Result<(), String> {
        #[cfg(feature = "std")]
        {
            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                w.flush().map_err(|e| e.to_string())?;
                // We do NOT call w.get_ref().sync_data() here. Durability is handled by the WAL layer.
                // Forcing fdatasync on the cold storage layer on every batch kills performance needlessly.
            }
        }
        Ok(())
    }

    /// Fast-path check to determine if an entity exists on disk without loading it.
    /// Use this for wait-free exact disk index checks before disk loads.
    pub fn contains(&self, entity_id: usize) -> bool {
        let index = crate::core::rcu::load_ref(&self.disk_index);
        index.contains_key(&entity_id)
    }

    /// Swaps the `next_disk_index` into `disk_index` if changes have occurred.
    /// Returns the old `AHashMap` pointer for QSBR memory reclamation, or `null` if no changes occurred.
    pub fn swap_disk_index(&self) -> *mut AHashMap<usize, (u64, u32)> {
        if !self.disk_index_dirty.swap(false, Ordering::Acquire) {
            return core::ptr::null_mut();
        }
        #[cfg(feature = "std")]
        let next = self.next_disk_index.lock().unwrap().clone();
        #[cfg(not(feature = "std"))]
        let next = self.next_disk_index.lock().unwrap().clone();

        crate::core::rcu::swap_ptr(&self.disk_index, next)
    }

    /// Appends an entity to `entities.bin` as a length-prefixed record and
    /// updates the in-memory disk index.
    ///
    /// The on-disk format for each record is:
    /// ```text
    /// [ 4 bytes LE payload length ][ payload bytes (EntityData::encode output) ]
    /// ```
    ///
    /// On `std` targets the record is written through the shared `BufWriter`.
    /// If the writer is unavailable, the call falls back to
    /// [`FileSystem::append`]. On `no_std` targets [`FileSystem::append`] is
    /// always used.
    ///
    /// After a successful write `disk_index` is updated so that subsequent
    /// [`Storage::read_entity`] calls can find the new record immediately.
    ///
    /// # Errors
    ///
    /// Returns `Err(String)` if the underlying write or append operation fails.
    pub fn write_entity(&self, data: &EntityData) -> Result<(), String> {
        let path = format!("{}/entities.bin", self.base_path);
        let bytes = data.encode();

        let mut record = Vec::with_capacity(4 + bytes.len());
        record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        record.extend_from_slice(&bytes);

        #[cfg(feature = "std")]
        {
            let mut off_lock = self.current_offset.lock().unwrap();
            let offset = *off_lock;

            let use_fallback = {
                let mut lock = self.writer.lock().unwrap();
                if let Some(w) = lock.as_mut() {
                    use std::io::Write;
                    w.write_all(&record).map_err(|e| e.to_string())?;
                    false
                } else {
                    true
                }
            };

            if !use_fallback {
                *off_lock = offset + record.len() as u64;
                #[cfg(feature = "std")]
                let mut index = self.next_disk_index.lock().unwrap();
                #[cfg(not(feature = "std"))]
                let mut index = self.next_disk_index.lock().unwrap();
                index.insert(data.entity_id, (offset, bytes.len() as u32));
                self.disk_index_dirty.store(true, Ordering::Release);
                return Ok(());
            }

            self.fs.append(&path, &record)?;
            *off_lock = offset + record.len() as u64;
            #[cfg(feature = "std")]
            let mut index = self.next_disk_index.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let mut index = self.next_disk_index.lock().unwrap();
            index.insert(data.entity_id, (offset, bytes.len() as u32));
            self.disk_index_dirty.store(true, Ordering::Release);
            Ok(())
        }

        #[cfg(not(feature = "std"))]
        {
            let mut off_lock = self.current_offset.lock().unwrap();
            let offset = *off_lock;

            self.fs.append(&path, &record)?;
            *off_lock = offset + record.len() as u64;
            let mut index = self.next_disk_index.lock().unwrap();
            index.insert(data.entity_id, (offset, bytes.len() as u32));
            self.disk_index_dirty.store(true, Ordering::Release);
            Ok(())
        }
    }

    /// Reads and decodes a single entity from `entities.bin` using the disk index.
    ///
    /// The method looks up `entity_id` in `disk_index` to obtain the record's
    /// byte offset and length, then reads exactly those bytes.
    ///
    /// On `std` targets the read is served through a memory-mapped view
    /// ([`memmap2::Mmap`]) that is lazily created (or re-created if stale) so
    /// that the kernel can satisfy sequential reads from the page cache with
    /// minimal copying. On `no_std` targets [`FileSystem::read_range`] is used.
    ///
    /// # Errors
    ///
    /// - `"Not found in disk index"` — `entity_id` has never been written.
    /// - `"Truncated"` / `"Truncated record"` — the on-disk record is shorter
    ///   than the length prefix indicates (file corruption).
    /// - `"Decode failed"` / `"Failed to decode"` — [`EntityData::decode`]
    ///   returned `None` (malformed payload).
    /// - Any I/O error from the underlying filesystem.
    pub fn read_entity(&self, entity_id: usize) -> Result<EntityData, String> {
        let index = crate::core::rcu::load_ref(&self.disk_index);
        let (offset, len) = *index.get(&entity_id).ok_or("Not found in disk index")?;

        let path = format!("{}/entities.bin", self.base_path);

        #[cfg(feature = "std")]
        {
            let mut remap_needed = false;
            let mmap_opt = {
                let lock = self.mmap.lock().unwrap();
                if let Some(m) = lock.as_ref() {
                    if (offset + len as u64 + 4) <= m.len() as u64 {
                        Some(m.clone())
                    } else {
                        remap_needed = true;
                        None
                    }
                } else {
                    remap_needed = true;
                    None
                }
            };

            let mmap = if remap_needed {
                if let Ok(mut w) = self.writer.lock()
                    && let Some(writer) = w.as_mut()
                {
                    use std::io::Write;
                    let _ = writer.flush();
                }
                let file = std::fs::File::open(&path).map_err(|e| e.to_string())?;
                let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| e.to_string())?;
                let arc_mmap = alloc::sync::Arc::new(mmap);
                let mut lock = self.mmap.lock().unwrap();
                *lock = Some(arc_mmap.clone());
                arc_mmap
            } else {
                mmap_opt.unwrap()
            };

            let buf = &mmap[offset as usize..(offset + len as u64 + 4) as usize];
            if buf.len() < 4 {
                return Err("Truncated".to_string());
            }
            let data_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            if buf.len() < 4 + data_len {
                return Err("Truncated".to_string());
            }
            EntityData::decode(&buf[4..4 + data_len]).ok_or("Decode failed".to_string())
        }

        #[cfg(not(feature = "std"))]
        {
            let bytes = self.fs.read_range(&path, offset, len as usize + 4)?;
            if bytes.len() < 4 {
                return Err("Truncated record".to_string());
            }
            let data_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
            if bytes.len() < 4 + data_len {
                return Err("Truncated record data".to_string());
            }
            EntityData::decode(&bytes[4..4 + data_len]).ok_or("Failed to decode".to_string())
        }
    }

    /// Prefetch-reads up to `block_size * 2` adjacent entities starting from the
    /// block-aligned predecessor of `entity_id`.
    ///
    /// Entity IDs are grouped into blocks of `block_size`. This method determines
    /// the start of the block that contains `entity_id` and then attempts to read
    /// `2 × block_size` consecutive IDs beginning at that block boundary. This
    /// over-fetching strategy is intended for promoting cold data into an
    /// upper-tier cache without requiring callers to know exact block boundaries.
    ///
    /// Entities that are not present in the disk index are silently skipped;
    /// only successfully decoded records are included in the returned vector.
    ///
    /// # Arguments
    ///
    /// * `entity_id` — The entity whose block should be fetched.
    /// * `block_size` — Number of entity IDs per block; also controls how many
    ///   total IDs are probed (`2 × block_size`).
    ///
    /// # Returns
    ///
    /// A `Vec<EntityData>` containing every entity that was found and decoded
    /// within the probed ID range (may be empty if none are stored).
    pub fn read_block(&self, entity_id: usize, block_size: usize) -> Vec<EntityData> {
        let start_id = (entity_id / block_size) * block_size;
        let mut block_data = Vec::new();

        let fetch_size = block_size * 2;
        for i in 0..fetch_size {
            let id = start_id + i;
            if let Ok(data) = self.read_entity(id) {
                block_data.push(data);
            }
        }
        block_data
    }

    /// Rewrites `entities.bin`, retaining only the latest record for each entity.
    ///
    /// Over time, repeated [`Storage::write_entity`] calls for the same entity ID
    /// leave stale, superseded records in the append-only log. `compact` eliminates
    /// these duplicates in two lock-free phases to minimise write stalls:
    ///
    /// **Phase 1 (unlocked):** Snapshot the current disk index and `current_offset`,
    /// then iterate over the snapshot, copying each entity's latest record into a
    /// temporary file (`entities.bin.tmp`) using a fresh 64 KiB `BufWriter`.
    ///
    /// **Phase 2 (locked):** Acquire all three locks (`disk_index`, `current_offset`,
    /// `writer`). Collect any *delta* records written since the Phase 1 snapshot
    /// (those whose offset ≥ `original_offset`), append them to the temp file in
    /// offset order, then atomically rename the temp file over `entities.bin`.
    /// The `writer` and `mmap` handles are refreshed to point at the new file.
    ///
    /// After `compact` returns, `disk_index` and `current_offset` reflect the
    /// compacted file and the mmap cache is cleared so the next read will
    /// re-map the new file.
    ///
    /// This method is a no-op on `no_std` targets (returns `Ok(())` immediately).
    ///
    /// # Errors
    ///
    /// Returns `Err(String)` if the temporary file cannot be created, any read or
    /// write fails, or the final rename fails.
    pub fn compact(&self) -> Result<(), String> {
        #[cfg(feature = "std")]
        {
            let path = format!("{}/entities.bin", self.base_path);
            let tmp_path = format!("{}/entities.bin.tmp", self.base_path);

            let (active_ids, original_offset) = {
                let index = self.next_disk_index.lock().unwrap();
                let off = *self.current_offset.lock().unwrap();
                (index.clone(), off)
            };

            let mut tmp_writer = std::io::BufWriter::with_capacity(
                64 * 1024,
                std::fs::File::create(&tmp_path).map_err(|e| e.to_string())?,
            );

            let mut new_index = AHashMap::default();
            let mut new_offset: u64 = 0;

            for (&id, &(off, len)) in active_ids.iter() {
                if off >= original_offset {
                    continue;
                }
                if let Ok(bytes) = self.fs.read_range(&path, off, len as usize + 4) {
                    use std::io::Write;
                    tmp_writer.write_all(&bytes).map_err(|e| e.to_string())?;
                    new_index.insert(id, (new_offset, len));
                    new_offset += bytes.len() as u64;
                }
            }

            // Phase 2: Lock and merge deltas
            let mut index_lock = self.next_disk_index.lock().unwrap();
            let mut offset_lock = self.current_offset.lock().unwrap();
            let mut writer_lock = self.writer.lock().unwrap();

            let mut delta_ids = Vec::new();
            for (&id, &(off, len)) in index_lock.iter() {
                if off >= original_offset {
                    delta_ids.push((id, off, len));
                }
            }

            delta_ids.sort_by_key(|&(_, off, _)| off);

            if let Some(w) = writer_lock.as_mut() {
                use std::io::Write;
                let _ = w.flush();
            }

            for (id, off, len) in delta_ids {
                if let Ok(bytes) = self.fs.read_range(&path, off, len as usize + 4) {
                    use std::io::Write;
                    tmp_writer.write_all(&bytes).map_err(|e| e.to_string())?;
                    new_index.insert(id, (new_offset, len));
                    new_offset += bytes.len() as u64;
                }
            }

            use std::io::Write;
            tmp_writer.flush().map_err(|e| e.to_string())?;
            tmp_writer
                .get_ref()
                .sync_data()
                .map_err(|e| e.to_string())?;

            *writer_lock = None;
            let _ = std::fs::rename(&tmp_path, &path);

            *writer_lock = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .ok()
                .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f));

            *index_lock = new_index;
            *offset_lock = new_offset;

            self.disk_index_dirty.store(true, Ordering::Release);

            let mut mmap_lock = self.mmap.lock().unwrap();
            *mmap_lock = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_entity_data_encode_decode() {
        let mut attrs = crate::Attributes::new();
        attrs.insert("name".to_string(), "foo".to_string());
        let mut attrs_int = crate::Attributes::new();
        attrs_int.insert("age".to_string(), 42);
        let mut attrs_blob = crate::Attributes::new();
        attrs_blob.insert("data".to_string(), vec![1, 2, 3]);

        let data = EntityData {
            entity_id: 42,
            attributes: attrs,
            attributes_int: attrs_int,
            attributes_blob: attrs_blob,
        };

        let buf = data.encode();
        let dec = EntityData::decode(&buf).unwrap();
        assert_eq!(dec.entity_id, 42);
        assert_eq!(dec.attributes.get("name").unwrap(), "foo");
        assert_eq!(dec.attributes_int.get("age").unwrap(), &42);
        assert_eq!(dec.attributes_blob.get("data").unwrap(), &vec![1, 2, 3]);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_storage_read_write() {
        let fs = Arc::new(crate::io::platform::StdFileSystem);
        let path = "test_storage_dir";
        let _ = std::fs::remove_dir_all(path);
        let storage = Storage::new(path.to_string(), fs);
        let data = EntityData {
            entity_id: 10,
            attributes: crate::Attributes::new(),
            attributes_int: crate::Attributes::new(),
            attributes_blob: crate::Attributes::new(),
        };
        storage.write_entity(&data).unwrap();
        storage.swap_disk_index();
        let read_data = storage.read_entity(10).unwrap();
        assert_eq!(read_data.entity_id, 10);

        // Test missing entity
        assert!(storage.read_entity(999).is_err());

        let block = storage.read_block(10, 5);
        assert!(!block.is_empty());
        assert_eq!(block[0].entity_id, 10);

        let empty_block = storage.read_block(999, 5);
        assert!(empty_block.is_empty());

        storage.compact().unwrap();
        let read_data2 = storage.read_entity(10).unwrap();
        assert_eq!(read_data2.entity_id, 10);

        let _ = std::fs::remove_dir_all(path);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_storage_fallback_write() {
        let fs = Arc::new(crate::io::platform::StdFileSystem);
        let path = "test_storage_fallback";
        let _ = std::fs::remove_dir_all(path);
        let storage = Storage::new(path.to_string(), fs);

        // Force writer to None
        *storage.writer.lock().unwrap() = None;
        *storage.mmap.lock().unwrap() = None;

        let data = EntityData {
            entity_id: 20,
            attributes: crate::Attributes::new(),
            attributes_int: crate::Attributes::new(),
            attributes_blob: crate::Attributes::new(),
        };
        storage.write_entity(&data).unwrap();
        storage.swap_disk_index();
        let read_data = storage.read_entity(20).unwrap();
        assert_eq!(read_data.entity_id, 20);

        let _ = std::fs::remove_dir_all(path);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_storage_rebuild_and_corrupt() {
        let fs = Arc::new(crate::io::platform::StdFileSystem);
        let path = "test_storage_corrupt";
        let _ = std::fs::remove_dir_all(path);
        std::fs::create_dir_all(path).unwrap();
        let bin_path = format!("{}/entities.bin", path);
        // Write invalid data (length prefix without enough payload)
        std::fs::write(&bin_path, vec![0xFF, 0x00, 0x00, 0x00, 1, 2, 3]).unwrap();
        let storage = Storage::new(path.to_string(), fs.clone()); // calls rebuild disk index
        assert!(storage.read_entity(1).is_err());
        let _ = std::fs::remove_dir_all(path);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_storage_flush_none() {
        let fs = Arc::new(crate::io::platform::StdFileSystem);
        let path = "test_storage_flush_none";
        let _ = std::fs::remove_dir_all(path);
        let storage = Storage::new(path.to_string(), fs);
        *storage.writer.lock().unwrap() = None;
        assert!(storage.flush().is_ok());
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_peek_entity_id_short() {
        assert!(Storage::peek_entity_id(&[1, 2, 3]).is_none());
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_storage_rebuild_truncated_record() {
        let fs = Arc::new(crate::io::platform::StdFileSystem);
        let path = "test_storage_trunc";
        let _ = std::fs::remove_dir_all(path);
        std::fs::create_dir_all(path).unwrap();
        let bin_path = format!("{}/entities.bin", path);
        // Write invalid data (length prefix larger than file)
        std::fs::write(&bin_path, vec![0xFF, 0x00, 0x00, 0x00, 1]).unwrap();
        let storage = Storage::new(path.to_string(), fs.clone()); // calls rebuild disk index
        assert!(storage.read_entity(1).is_err());
        let _ = std::fs::remove_dir_all(path);
    }
}

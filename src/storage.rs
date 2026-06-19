use alloc::string::{String, ToString};
use alloc::vec::Vec;
use alloc::format;
use crate::platform::FileSystem;
use crate::AHashMap;
use alloc::sync::Arc;

#[derive(Clone, Debug)]
pub struct EntityData {
    pub entity_id: usize,
    pub attributes: crate::Attributes<String>,
    pub attributes_int: crate::Attributes<u32>,
    pub attributes_blob: crate::Attributes<Vec<u8>>,
}

impl EntityData {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.entity_id as u64).to_le_bytes());
        self.attributes.encode_to(&mut buf, |v: &String, b: &mut Vec<u8>| {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            b.extend_from_slice(v.as_bytes());
        });
        self.attributes_int.encode_to(&mut buf, |v: &u32, b: &mut Vec<u8>| {
            b.extend_from_slice(&(*v).to_le_bytes());
        });
        self.attributes_blob.encode_to(&mut buf, |v: &Vec<u8>, b: &mut Vec<u8>| {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            b.extend_from_slice(v);
        });
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut pos = 0;
        let entity_id = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
        pos += 8;
        let attributes = crate::Attributes::<String>::decode_from(buf, &mut pos, |b, p| {
            let len = u32::from_le_bytes(b.get(*p..*p + 4).unwrap().try_into().unwrap()) as usize;
            *p += 4;
            let s = core::str::from_utf8(b.get(*p..*p + len).unwrap()).unwrap().to_string();
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

pub struct Storage {
    pub base_path: String,
    pub fs: Arc<dyn FileSystem>,
    pub disk_index: crate::sync::Mutex<AHashMap<usize, (u64, u32)>>,
    pub current_offset: crate::sync::Mutex<u64>,
    #[cfg(feature = "std")]
    pub writer: crate::sync::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
    #[cfg(feature = "std")]
    pub mmap: crate::sync::Mutex<Option<alloc::sync::Arc<memmap2::Mmap>>>,
}

impl Storage {
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
            crate::sync::Mutex::new(file_opt)
        };

        let storage = Self {
            base_path,
            fs,
            disk_index: crate::sync::Mutex::new(AHashMap::default()),
            current_offset: crate::sync::Mutex::new(0),
            #[cfg(feature = "std")]
            writer,
            #[cfg(feature = "std")]
            mmap: crate::sync::Mutex::new(None),
        };
        storage.rebuild_disk_index();
        storage
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
                let len = u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap()) as usize;
                let record_start = pos as u64;
                pos += 4;
                if pos + len <= bytes.len() {
                    if let Some(entity_id) = Self::peek_entity_id(&bytes[pos..pos+len]) {
                        map.insert(entity_id, (record_start, len as u32));
                    }
                    pos += len;
                } else {
                    break;
                }
            }
            #[cfg(feature = "std")]
            {
                let mut index = self.disk_index.lock().unwrap();
                *index = map;
                let mut off = self.current_offset.lock().unwrap();
                *off = pos as u64;
            }
            #[cfg(not(feature = "std"))]
            {
                let mut index = self.disk_index.lock();
                *index = map;
                let mut off = self.current_offset.lock();
                *off = pos as u64;
            }
        }
    }

    fn peek_entity_id(buf: &[u8]) -> Option<usize> {
        if buf.len() >= 8 {
            Some(u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize)
        } else {
            None
        }
    }

    pub fn flush(&self) -> Result<(), String> {
        #[cfg(feature = "std")]
        {
            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                w.flush().map_err(|e| e.to_string())?;
                w.get_ref().sync_all().map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    /// 將實體寫入持久層
    pub fn write_entity(&self, data: &EntityData) -> Result<(), String> {
        let path = format!("{}/entities.bin", self.base_path);
        let bytes = data.encode();
        
        let mut record = Vec::with_capacity(4 + bytes.len());
        record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        record.extend_from_slice(&bytes);

        #[cfg(feature = "std")]
        {
            let mut index = self.disk_index.lock().unwrap();
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
                index.insert(data.entity_id, (offset, bytes.len() as u32));
                return Ok(());
            }

            self.fs.append(&path, &record)?;
            *off_lock = offset + record.len() as u64;
            index.insert(data.entity_id, (offset, bytes.len() as u32));
            Ok(())
        }

        #[cfg(not(feature = "std"))]
        {
            let mut index = self.disk_index.lock();
            let mut off_lock = self.current_offset.lock();
            let offset = *off_lock;

            self.fs.append(&path, &record)?;
            *off_lock = offset + record.len() as u64;
            index.insert(data.entity_id, (offset, bytes.len() as u32));
            Ok(())
        }
    }

    /// 讀取實體
    pub fn read_entity(&self, entity_id: usize) -> Result<EntityData, String> {
        let (offset, len) = {
            #[cfg(feature = "std")]
            let index = self.disk_index.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let index = self.disk_index.lock();
            *index.get(&entity_id).ok_or("Not found in disk index")?
        };
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
                    && let Some(writer) = w.as_mut() {
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
            EntityData::decode(&buf[4..4+data_len]).ok_or("Decode failed".to_string())
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
            EntityData::decode(&bytes[4..4+data_len]).ok_or("Failed to decode".to_string())
        }
    }

    /// 塊狀讀取
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

    /// Background Compaction
    pub fn compact(&self) -> Result<(), String> {
        #[cfg(feature = "std")]
        {
            let path = format!("{}/entities.bin", self.base_path);
            let tmp_path = format!("{}/entities.bin.tmp", self.base_path);
            
            let (active_ids, original_offset) = {
                let index = self.disk_index.lock().unwrap();
                let off = *self.current_offset.lock().unwrap();
                (index.clone(), off)
            };

            let mut tmp_writer = std::io::BufWriter::with_capacity(
                64 * 1024,
                std::fs::File::create(&tmp_path).map_err(|e| e.to_string())?
            );
            
            let mut new_index = AHashMap::default();
            let mut new_offset: u64 = 0;

            for (&id, &(off, len)) in active_ids.iter() {
                if off >= original_offset { continue; }
                if let Ok(bytes) = self.fs.read_range(&path, off, len as usize + 4) {
                    use std::io::Write;
                    tmp_writer.write_all(&bytes).map_err(|e| e.to_string())?;
                    new_index.insert(id, (new_offset, len));
                    new_offset += bytes.len() as u64;
                }
            }

            // Phase 2: Lock and merge deltas
            let mut index_lock = self.disk_index.lock().unwrap();
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
            tmp_writer.get_ref().sync_all().map_err(|e| e.to_string())?;

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
            
            let mut mmap_lock = self.mmap.lock().unwrap();
            *mmap_lock = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_data_encode_decode() {
        let mut attrs = crate::Attributes::new();
        attrs.insert("name".to_string(), "foo".to_string());
        
        let data = EntityData {
            entity_id: 42,
            attributes: attrs,
            attributes_int: crate::Attributes::new(),
            attributes_blob: crate::Attributes::new(),
        };
        
        let buf = data.encode();
        let dec = EntityData::decode(&buf).unwrap();
        assert_eq!(dec.entity_id, 42);
        assert_eq!(dec.attributes.get("name").unwrap(), "foo");
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_storage_read_write() {
        let fs = Arc::new(crate::platform::StdFileSystem);
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
        let read_data = storage.read_entity(10).unwrap();
        assert_eq!(read_data.entity_id, 10);
        
        let block = storage.read_block(10, 5);
        assert!(!block.is_empty());
        assert_eq!(block[0].entity_id, 10);
        
        storage.compact().unwrap();
        let read_data2 = storage.read_entity(10).unwrap();
        assert_eq!(read_data2.entity_id, 10);
        
        let _ = std::fs::remove_dir_all(path);
    }
}

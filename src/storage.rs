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
            b.extend_from_slice(&(*v as u32).to_le_bytes());
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
    pub disk_index: crate::platform::Mutex<AHashMap<usize, (u64, u32)>>,
    pub current_offset: crate::platform::Mutex<u64>,
    #[cfg(feature = "std")]
    pub writer: crate::platform::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
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
                .write(true)
                .open(&path)
                .ok()
                .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f));
            crate::platform::Mutex::new(file_opt)
        };

        let storage = Self {
            base_path,
            fs,
            disk_index: crate::platform::Mutex::new(AHashMap::default()),
            current_offset: crate::platform::Mutex::new(0),
            #[cfg(feature = "std")]
            writer,
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
            return Ok(());
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
        let path = format!("{}/entities.bin", self.base_path);
        let (offset, len) = {
            #[cfg(feature = "std")]
            let index = self.disk_index.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let index = self.disk_index.lock();
            index.get(&entity_id).copied()
        }.ok_or_else(|| "Entity not found on disk".to_string())?;

        let bytes = self.fs.read_range(&path, offset + 4, len as usize)?;
        EntityData::decode(&bytes)
            .ok_or_else(|| "Decode Failed".to_string())
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
}

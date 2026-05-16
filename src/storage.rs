use alloc::string::{String, ToString};
use alloc::vec::Vec;
use alloc::format;
use crate::platform::FileSystem;

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
        self.attributes.encode_to(&mut buf, |v, b| {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            b.extend_from_slice(v.as_bytes());
        });
        self.attributes_int.encode_to(&mut buf, |v, b| {
            b.extend_from_slice(&(*v as u32).to_le_bytes());
        });
        self.attributes_blob.encode_to(&mut buf, |v, b| {
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


use alloc::sync::Arc;

pub struct Storage {
    pub base_path: String,
    pub fs: Arc<dyn FileSystem>,
}

impl Storage {
    pub fn new(base_path: String, fs: Arc<dyn FileSystem>) -> Self {
        Self { base_path, fs }
    }

    /// 將實體寫入持久層
    pub fn write_entity(&self, data: &EntityData) -> Result<(), String> {
        let path = format!("{}/entity_{}.bin", self.base_path, data.entity_id);
        let bytes = data.encode();
        self.fs.write(&path, &bytes)
    }

    /// 讀取實體
    pub fn read_entity(&self, entity_id: usize) -> Result<EntityData, String> {
        let path = format!("{}/entity_{}.bin", self.base_path, entity_id);
        let bytes = self.fs.read(&path)?;
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

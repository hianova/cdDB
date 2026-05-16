use std::path::PathBuf;
use std::fs::{File, self};
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct EntityData {
    pub entity_id: usize,
    pub attributes: crate::Attributes<String>,
    pub attributes_int: crate::Attributes<u32>,
}

/// Helper for hex encoding/decoding binary payloads
pub struct PayloadCodec;

impl PayloadCodec {
    pub fn encode(data: &[u8]) -> String {
        hex::encode(data)
    }

    pub fn decode(hex_str: &str) -> Result<Vec<u8>, hex::FromHexError> {
        hex::decode(hex_str)
    }
}

pub struct Storage {
    pub base_path: PathBuf,
}

impl Storage {
    pub fn new(base_path: PathBuf) -> Self {
        let _ = fs::create_dir_all(&base_path);
        Self { base_path }
    }

    /// 將實體寫入持久層 (同步實現，避免 Tokio 執行緒池開銷)
    pub fn write_entity(&self, data: &EntityData) -> std::io::Result<()> {
        let path = self.base_path.join(format!("entity_{}.bin", data.entity_id));
        let mut file = File::create(path)?;
        let bytes = bincode::serialize(data).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }

    /// 讀取實體 (同步實現)
    pub fn read_entity(&self, entity_id: usize) -> std::io::Result<EntityData> {
        let path = self.base_path.join(format!("entity_{}.bin", entity_id));
        let mut file = File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let data: EntityData = bincode::deserialize(&bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(data)
    }

    /// 塊狀讀取 (同步實現)
    pub fn read_block(&self, entity_id: usize, block_size: usize) -> Vec<EntityData> {
        let start_id = (entity_id / block_size) * block_size;
        let mut block_data = Vec::new();
        
        // 同步讀取鄰近實體
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

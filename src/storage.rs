use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};



#[derive(Clone, Serialize, Deserialize)]
pub struct EntityData {
    pub entity_id: usize,
    pub attributes: crate::Attributes<String>,
    pub attributes_int: crate::Attributes<u32>,
}

pub struct AsyncStorage {
    base_path: PathBuf,
}

impl AsyncStorage {
    pub fn new(base_path: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&base_path);
        Self { base_path }
    }

    /// 將實體寫入持久層 (簡單實現：每個 Entity 一個位置，實務中會用 SSTable)
    pub async fn write_entity(&self, data: &EntityData) -> tokio::io::Result<()> {
        let path = self.base_path.join(format!("entity_{}.bin", data.entity_id));
        let mut file = File::create(path).await?;
        let bytes = bincode::serialize(data).map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::Other, e))?;
        file.write_all(&bytes).await?;
        Ok(())
    }

    /// 讀取實體 (觸發 Page Fault 時調用)
    pub async fn read_entity(&self, entity_id: usize) -> tokio::io::Result<EntityData> {
        let path = self.base_path.join(format!("entity_{}.bin", entity_id));
        let mut file = File::open(path).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        let data: EntityData = bincode::deserialize(&bytes).map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::Other, e))?;
        Ok(data)
    }

    /// 塊狀讀取 (Block Fetching)：同時讀取鄰近的實體
    pub async fn read_block(&self, entity_id: usize, block_size: usize) -> Vec<EntityData> {
        let start_id = (entity_id / block_size) * block_size;
        let mut block_data = Vec::new();
        
        // 並行讀取鄰近實體 (模擬 Block Fetch)
        let mut handles = vec![];
        for i in 0..block_size {
            let id = start_id + i;
            let path = self.base_path.join(format!("entity_{}.bin", id));
            handles.push(tokio::spawn(async move {
                if let Ok(mut file) = File::open(path).await {
                    let mut bytes = Vec::new();
                    if file.read_to_end(&mut bytes).await.is_ok() {
                        return bincode::deserialize::<EntityData>(&bytes).ok();
                    }
                }
                None
            }));
        }

        for h in handles {
            if let Ok(Some(data)) = h.await {
                block_data.push(data);
            }
        }
        block_data
    }
}

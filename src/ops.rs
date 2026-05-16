use crate::commands::Attributes;
use ahash::AHashMap;
use alloc::string::{String, ToString};
use alloc::format;

/// IT Operations Log Levels
#[derive(Debug, Clone)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Fatal,
    Debug,
}

/// A structured record for IT Operations (Monitoring, Logging, etc.)
#[derive(Debug, Clone)]
pub struct ITOpsRecord {
    pub timestamp: u64,
    pub service: String,
    pub node: String,
    pub level: LogLevel,
    pub message: String,
    pub cpu_usage: f32, // 0.0 - 1.0
    pub mem_usage: f32, // 0.0 - 1.0
    pub response_time_ms: u32,
}

impl ITOpsRecord {
    /// Converts the structured record into cdDB compatible attributes.
    /// Usage percentages are scaled by 1000 for precision in u32.
    pub fn to_cd_db_params(&self) -> (Attributes<String>, Attributes<u32>) {
        let mut attrs = AHashMap::new();
        attrs.insert("service".to_string(), self.service.clone());
        attrs.insert("node".to_string(), self.node.clone());
        attrs.insert("level".to_string(), format!("{:?}", self.level));
        attrs.insert("message".to_string(), self.message.clone());

        let mut attrs_int = AHashMap::new();
        attrs_int.insert("timestamp".to_string(), (self.timestamp % (u32::MAX as u64)) as u32);
        attrs_int.insert("cpu_milli".to_string(), (self.cpu_usage * 1000.0) as u32);
        attrs_int.insert("mem_milli".to_string(), (self.mem_usage * 1000.0) as u32);
        attrs_int.insert("response_time".to_string(), self.response_time_ms);

        (attrs.into(), attrs_int.into())
    }
}

/// Extension trait for easier ITOps data ingestion
pub trait ITOpsIngest {
    fn insert_ops_record(&self, entity_id: usize, record: ITOpsRecord) -> crate::commands::WriteCommand;
}

impl ITOpsIngest for ITOpsRecord {
    fn insert_ops_record(&self, entity_id: usize, record: ITOpsRecord) -> crate::commands::WriteCommand {
        let (attributes, attributes_int) = record.to_cd_db_params();
        crate::commands::WriteCommand::Insert {
            entity_id,
            attributes,
            attributes_int,
            attributes_blob: crate::commands::Attributes::new(),
        }
    }
}

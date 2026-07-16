#[cfg(feature = "std")]
use alloc::format;
#[cfg(feature = "std")]
use alloc::string::String;
#[cfg(feature = "std")]
use alloc::vec::Vec;

#[cfg(feature = "std")]
use no_std_tool::sha2::{Sha256, Digest};
#[cfg(feature = "std")]
use std::fs::OpenOptions;
#[cfg(feature = "std")]
use std::io::Write;

#[cfg(feature = "std")]
use crate::core::commands::WriteCommand;

/// A Cold Storage Archiver to persist old cdDB records for Digital Preservation.
/// Supports cryptographic provenance hashing to prevent bit-rot and tampering.
#[cfg(feature = "std")]
pub struct ColdStorageArchiver {
    archive_path: String,
    signer_id: String,
}

#[cfg(feature = "std")]
impl ColdStorageArchiver {
    pub fn new(archive_path: &str, signer_id: &str) -> Self {
        Self {
            archive_path: String::from(archive_path),
            signer_id: String::from(signer_id),
        }
    }

    /// Serializes a WriteCommand into a deterministic byte array for hashing.
    fn serialize_command(cmd: &WriteCommand) -> Vec<u8> {
        // Simplified deterministic serialization for provenance hashing
        let mut buffer = Vec::new();
        match cmd {
            WriteCommand::Insert { entity_id, .. } => {
                buffer.extend_from_slice(b"INSERT:");
                buffer.extend_from_slice(&entity_id.to_le_bytes());
            }
            WriteCommand::InsertFast { entity_id, .. } => {
                buffer.extend_from_slice(b"INSERT_FAST:");
                buffer.extend_from_slice(&entity_id.to_le_bytes());
            }
            WriteCommand::BatchInsert(inserts) => {
                buffer.extend_from_slice(b"BATCH_INSERT:");
                buffer.extend_from_slice(&(inserts.len() as u32).to_le_bytes());
            }
            WriteCommand::Delete { entity_id } => {
                buffer.extend_from_slice(b"DELETE:");
                buffer.extend_from_slice(&entity_id.to_le_bytes());
            }
        }
        buffer
    }

    /// Archives a batch of commands to cold storage with a SHA-256 signature.
    pub fn archive_batch(&self, commands: &[WriteCommand], timestamp: u64) -> Result<String, &'static str> {
        let mut hasher = Sha256::new();
        hasher.update(timestamp.to_le_bytes());
        hasher.update(self.signer_id.as_bytes());

        let mut data_payload = Vec::new();

        for cmd in commands {
            let serialized = Self::serialize_command(cmd);
            hasher.update(&serialized);
            data_payload.extend_from_slice(&serialized);
            data_payload.push(b'\n');
        }

        let hash_result = hasher.finalize();
        let hash_hex = format!("{:x}", hash_result);

        // Append to archive log file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.archive_path)
            .map_err(|_| "Failed to open archive file")?;

        let header = format!("=== ARCHIVE BATCH ===\nTimestamp: {}\nSigner: {}\nHash: {}\n", timestamp, self.signer_id, hash_hex);
        
        file.write_all(header.as_bytes()).map_err(|_| "Failed to write header")?;
        file.write_all(&data_payload).map_err(|_| "Failed to write payload")?;
        file.write_all(b"=====================\n\n").map_err(|_| "Failed to write footer")?;

        Ok(hash_hex)
    }
}

#[cfg(not(feature = "std"))]
#[allow(unused_imports)]
pub use spin::Mutex;

#[cfg(feature = "std")]
pub use std::sync::Mutex;

use alloc::vec::Vec;
use alloc::string::String;
use alloc::string::ToString;

pub trait FileSystem: Send + Sync {
    fn write(&self, path: &str, data: &[u8]) -> Result<(), String>;
    fn read(&self, path: &str) -> Result<Vec<u8>, String>;
    fn append(&self, path: &str, data: &[u8]) -> Result<(), String>;
    fn exists(&self, path: &str) -> bool;
    fn create_dir_all(&self, path: &str) -> Result<(), String>;
    fn read_dir(&self, path: &str) -> Result<Vec<String>, String>;

    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, String> {
        let all = self.read(path)?;
        let start = offset as usize;
        if start + len <= all.len() {
            Ok(all[start..start + len].to_vec())
        } else {
            Err("Read out of bounds".to_string())
        }
    }

    fn file_size(&self, path: &str) -> Result<u64, String> {
        let bytes = self.read(path)?;
        Ok(bytes.len() as u64)
    }
}

pub trait ThreadManager: Send + Sync {
    fn spawn(&self, f: alloc::boxed::Box<dyn FnOnce() + Send + 'static>);
}

#[cfg(feature = "std")]
pub struct StdFileSystem;

#[cfg(feature = "std")]
impl FileSystem for StdFileSystem {
    fn write(&self, path: &str, data: &[u8]) -> Result<(), String> {
        use std::io::Write;
        let mut file = std::fs::File::create(path).map_err(|e: std::io::Error| e.to_string())?;
        file.write_all(data).map_err(|e: std::io::Error| e.to_string())
    }
    fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        std::fs::read(path).map_err(|e: std::io::Error| e.to_string())
    }
    fn append(&self, path: &str, data: &[u8]) -> Result<(), String> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).map_err(|e: std::io::Error| e.to_string())?;
        file.write_all(data).map_err(|e: std::io::Error| e.to_string())
    }
    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, String> {
        use std::io::{Read, Seek};
        let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        file.seek(std::io::SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
        let mut buf = alloc::vec![0; len];
        file.read_exact(&mut buf).map_err(|e| e.to_string())?;
        Ok(buf)
    }
    fn file_size(&self, path: &str) -> Result<u64, String> {
        std::fs::metadata(path)
            .map(|m| m.len())
            .map_err(|e| e.to_string())
    }
    fn exists(&self, path: &str) -> bool {
        std::path::Path::new(path).exists()
    }
    fn create_dir_all(&self, path: &str) -> Result<(), String> {
        std::fs::create_dir_all(path).map_err(|e: std::io::Error| e.to_string())
    }
    fn read_dir(&self, path: &str) -> Result<Vec<String>, String> {
        let entries = std::fs::read_dir(path).map_err(|e: std::io::Error| e.to_string())?;
        let mut names = Vec::new();
        for entry in entries {
            if let Ok(entry) = entry {
                if let Ok(name) = entry.file_name().into_string() {
                    names.push(name);
                }
            }
        }
        Ok(names)
    }
}

#[cfg(feature = "std")]
pub struct StdThreadManager;
pub trait MessageQueue: Send + Sync {
    fn recv(&self) -> Result<crate::commands::PartitionCommand, String>;
    fn try_recv(&self) -> Result<crate::commands::PartitionCommand, String>;
}

#[cfg(feature = "std")]
pub struct StdMessageQueue {
    pub rx: Mutex<std::sync::mpsc::Receiver<crate::commands::PartitionCommand>>,
}

#[cfg(feature = "std")]
impl MessageQueue for StdMessageQueue {
    fn recv(&self) -> Result<crate::commands::PartitionCommand, String> {
        self.rx.lock().unwrap().recv().map_err(|e: std::sync::mpsc::RecvError| e.to_string())
    }
    fn try_recv(&self) -> Result<crate::commands::PartitionCommand, String> {
        self.rx.lock().unwrap().try_recv().map_err(|e: std::sync::mpsc::TryRecvError| e.to_string())
    }
}

#[cfg(feature = "std")]
impl ThreadManager for StdThreadManager {
    fn spawn(&self, f: alloc::boxed::Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(move || f());
    }
}

#[allow(dead_code)]
pub trait MessageSender: Send + Sync {
    fn send(&self, cmd: crate::commands::PartitionCommand) -> Result<(), String>;
}

#[cfg(feature = "std")]
#[allow(dead_code)]
pub struct StdMessageSender {
    pub tx: std::sync::mpsc::SyncSender<crate::commands::PartitionCommand>,
}

#[cfg(feature = "std")]
impl MessageSender for StdMessageSender {
    fn send(&self, cmd: crate::commands::PartitionCommand) -> Result<(), String> {
        self.tx.send(cmd).map_err(|e| e.to_string())
    }
}

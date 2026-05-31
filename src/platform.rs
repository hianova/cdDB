#[cfg(not(feature = "std"))]
pub use crate::unsafe_core::no_std_sync::Mutex;

#[cfg(feature = "std")]
pub use std::sync::Mutex;

#[cfg(not(feature = "loom"))]
pub mod atomic {
    pub use core::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool, Ordering};
    #[cfg(target_has_atomic = "64")]
    pub use core::sync::atomic::AtomicU64;
}

#[cfg(feature = "loom")]
pub mod atomic {
    pub use loom::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool, Ordering};
}


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

pub trait Executor: Send + Sync {
    fn spawn_task(&self, f: alloc::boxed::Box<dyn FnOnce() + Send + 'static>);
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
pub struct StdExecutor;
pub trait MessageQueue: Send + Sync {
    fn recv(&self) -> Result<crate::commands::PartitionCommand, String>;
    fn try_recv(&self) -> Result<crate::commands::PartitionCommand, String>;
}

#[cfg(feature = "std")]
pub struct StdMessageQueue {
    pub rx: alloc::sync::Arc<crate::queue::BoundedQueue<crate::commands::PartitionCommand>>,
}

pub struct Backoff {
    spins: u32,
}

impl Backoff {
    pub fn new() -> Self {
        Self { spins: 0 }
    }
    
    pub fn snooze(&mut self) {
        core::hint::spin_loop();
        self.spins += 1;
    }
    
    pub fn is_completed(&self) -> bool {
        self.spins > 100
    }
}

#[cfg(feature = "std")]
impl MessageQueue for StdMessageQueue {
    fn recv(&self) -> Result<crate::commands::PartitionCommand, String> {
        let mut backoff = Backoff::new();
        loop {
            if let Some(cmd) = self.rx.pop() {
                return Ok(cmd);
            }
            if backoff.is_completed() {
                std::thread::yield_now();
            } else {
                backoff.snooze();
            }
        }
    }
    fn try_recv(&self) -> Result<crate::commands::PartitionCommand, String> {
        self.rx.pop().ok_or_else(|| "Empty".to_string())
    }
}

#[cfg(feature = "std")]
impl Executor for StdExecutor {
    fn spawn_task(&self, f: alloc::boxed::Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(move || f());
    }
}

/// Pluggable Thread-Local Storage (TLS) abstraction for `no_std` environments.
pub trait ThreadLocalProvider<T>: Send + Sync {
    fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&T>) -> R;
    
    fn set(&self, val: T);
}

#[allow(dead_code)]
pub trait MessageSender: Send + Sync {
    fn send(&self, cmd: crate::commands::PartitionCommand) -> Result<(), String>;
}

#[cfg(feature = "std")]
#[allow(dead_code)]
pub struct StdMessageSender {
    pub tx: alloc::sync::Arc<crate::queue::BoundedQueue<crate::commands::PartitionCommand>>,
}

#[cfg(feature = "std")]
impl MessageSender for StdMessageSender {
    fn send(&self, mut cmd: crate::commands::PartitionCommand) -> Result<(), String> {
        let mut backoff = Backoff::new();
        loop {
            match self.tx.push(cmd) {
                Ok(()) => return Ok(()),
                Err(c) => {
                    cmd = c;
                    if backoff.is_completed() {
                        std::thread::yield_now();
                    } else {
                        backoff.snooze();
                    }
                }
            }
        }
    }
}

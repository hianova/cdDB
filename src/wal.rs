use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
#[cfg(feature = "std")]
use alloc::string::ToString;
use crate::commands::WriteCommand;
use crate::platform::FileSystem;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[derive(Default)]
pub enum WalMode {
    #[default]
    Sync,
    Async100ms,
}


pub trait WalProvider: Send + Sync {
    fn append(&self, cmd: &WriteCommand) -> Result<(), String>;
    fn append_batch(&self, commands: &[&WriteCommand]) -> Result<(), String>;
    fn checkpoint(&self) -> Result<(), String>;
    fn read_all(&self) -> Result<Vec<u8>, String>;
}

pub struct NoopWal;
impl WalProvider for NoopWal {
    fn append(&self, _cmd: &WriteCommand) -> Result<(), String> { Ok(()) }
    fn append_batch(&self, _commands: &[&WriteCommand]) -> Result<(), String> { Ok(()) }
    fn checkpoint(&self) -> Result<(), String> { Ok(()) }
    fn read_all(&self) -> Result<Vec<u8>, String> { Ok(Vec::new()) }
}

pub struct StdWal {
    pub path: String,
    pub fs: Arc<dyn FileSystem>,
    pub mode: WalMode,
    #[cfg(feature = "std")]
    pub writer: Arc<crate::sync::Mutex<Option<std::io::BufWriter<std::fs::File>>>>,
    #[cfg(feature = "std")]
    pub async_buffer: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    #[cfg(feature = "std")]
    pub condvar: Arc<std::sync::Condvar>,
}

impl StdWal {
    pub fn new(path: String, fs: Arc<dyn FileSystem>, mode: WalMode) -> Self {
        #[cfg(feature = "std")]
        {
            use std::fs::OpenOptions;
            let file_opt = OpenOptions::new()
                .create(true)
                .append(true)
                
                .open(&path)
                .ok()
                .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f));
            
            let writer = Arc::new(crate::sync::Mutex::new(file_opt));
            let async_buffer = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::with_capacity(10000)));
            let condvar = Arc::new(std::sync::Condvar::new());

            if mode == WalMode::Async100ms {
                let bg_writer = Arc::clone(&writer);
                let bg_buffer = Arc::clone(&async_buffer);
                let bg_condvar = Arc::clone(&condvar);
                std::thread::spawn(move || {
                    let mut last_fsync = std::time::Instant::now();
                    loop {
                        let mut local_buf = Vec::<Vec<u8>>::new();
                        {
                            let mut lock = bg_buffer.lock().unwrap();
                            while lock.is_empty() {
                                lock = bg_condvar.wait(lock).unwrap();
                            }
                            core::mem::swap(&mut local_buf, &mut *lock);
                        }
                        if !local_buf.is_empty() {
                            // Adaptive Group Commit:
                            // Under low load, write and fsync immediately (minimal latency).
                            // Under high load (i.e. fsync rate would exceed 1000/sec), we sleep for the remaining part of 1ms.
                            // This aggregates hundreds/thousands of transactions into a single fsync.
                            let elapsed = last_fsync.elapsed();
                            let target_interval = std::time::Duration::from_millis(1);
                            if elapsed < target_interval {
                                std::thread::sleep(target_interval - elapsed);
                                // After sleep, check if more items arrived to batch them
                                {
                                    let mut lock = bg_buffer.lock().unwrap();
                                    if !lock.is_empty() {
                                        let mut extra_buf = Vec::<Vec<u8>>::new();
                                        core::mem::swap(&mut extra_buf, &mut *lock);
                                        local_buf.extend(extra_buf);
                                    }
                                }
                            }
                            
                            let mut w_lock = bg_writer.lock().unwrap();
                            if let Some(w) = w_lock.as_mut() {
                                use std::io::Write;
                                for buf in local_buf {
                                    let _ = w.write_all(&buf);
                                }
                                let _ = w.flush();
                                let _ = w.get_ref().sync_all();
                                last_fsync = std::time::Instant::now();
                            }
                        }
                    }
                });
            }

            Self {
                path,
                fs,
                mode,
                writer,
                async_buffer,
                condvar,
            }
        }
        #[cfg(not(feature = "std"))]
        Self { path, fs, mode }
    }
}

impl WalProvider for StdWal {
    fn append(&self, cmd: &WriteCommand) -> Result<(), String> {
        let bytes = cmd.encode();
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);

        #[cfg(feature = "std")]
        {
            if self.mode == WalMode::Async100ms {
                let mut lock = self.async_buffer.lock().unwrap();
                lock.push(buf);
                self.condvar.notify_one();
                return Ok(());
            }

            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                w.write_all(&buf).map_err(|e| e.to_string())?;
                w.flush().map_err(|e| e.to_string())?;
                w.get_ref().sync_all().map_err(|e| e.to_string())?;
                return Ok(());
            }
        }
        self.fs.append(&self.path, &buf)
    }

    fn append_batch(&self, commands: &[&WriteCommand]) -> Result<(), String> {
        let mut buf = Vec::new();
        for cmd in commands {
            let bytes = cmd.encode();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&bytes);
        }
        if !buf.is_empty() {
            #[cfg(feature = "std")]
            {
                if self.mode == WalMode::Async100ms {
                    let mut lock = self.async_buffer.lock().unwrap();
                    lock.push(buf);
                    self.condvar.notify_one();
                    return Ok(());
                }

                let mut lock = self.writer.lock().unwrap();
                if let Some(w) = lock.as_mut() {
                    use std::io::Write;
                    w.write_all(&buf).map_err(|e| e.to_string())?;
                    w.flush().map_err(|e| e.to_string())?;
                    w.get_ref().sync_all().map_err(|e| e.to_string())?;
                    return Ok(());
                }
            }
            self.fs.append(&self.path, &buf)?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), String> {
        #[cfg(feature = "std")]
        {
            if self.mode == WalMode::Async100ms {
                // For async, we can optionally wait for buffer to drain or just flush writer.
                // To be safe, we just flush the writer directly.
            }
            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                w.flush().map_err(|e| e.to_string())?;
                w.get_ref().sync_all().map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<u8>, String> {
        #[cfg(feature = "std")]
        {
            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                let _ = w.flush();
            }
        }
        self.fs.read(&self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::StdFileSystem;

    #[test]
    fn test_noop_wal() {
        let wal = NoopWal;
        let cmd = WriteCommand::Delete { entity_id: 1 };
        assert!(wal.append(&cmd).is_ok());
        assert!(wal.append_batch(&[&cmd]).is_ok());
        assert!(wal.checkpoint().is_ok());
        assert_eq!(wal.read_all().unwrap(), Vec::<u8>::new());
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_std_wal_sync() {
        let fs = Arc::new(StdFileSystem);
        let path = "test_wal_sync.log".to_string();
        let _ = std::fs::remove_file(&path);
        let wal = StdWal::new(path.clone(), fs, WalMode::Sync);
        let cmd = WriteCommand::Delete { entity_id: 2 };
        wal.append(&cmd).unwrap();
        wal.checkpoint().unwrap();
        let data = wal.read_all().unwrap();
        assert!(!data.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_std_wal_async() {
        let fs = Arc::new(StdFileSystem);
        let path = "test_wal_async.log".to_string();
        let _ = std::fs::remove_file(&path);
        let wal = StdWal::new(path.clone(), fs, WalMode::Async100ms);
        let cmd = WriteCommand::Delete { entity_id: 3 };
        wal.append_batch(&[&cmd, &cmd]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150));
        wal.checkpoint().unwrap();
        let data = wal.read_all().unwrap();
        assert!(!data.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}

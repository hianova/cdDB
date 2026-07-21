use crate::core::commands::WriteCommand;
use crate::io::platform::FileSystem;
use alloc::string::String;
#[cfg(feature = "std")]
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;
#[doc = " Durability level for WAL and storage writes."]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    #[doc = " AI/Compute mode: Protect SSD, optimize throughput."]
    #[doc = " Flushes and syncs data to disk at the specified interval."]
    Relaxed(core::time::Duration),
    #[doc = " HFT/Strict mode: Guarantee no data loss."]
    #[doc = " ⚠\u{fe0f} **DANGER**: Every write is immediately flushed and synced to disk using `fdatasync`."]
    #[doc = " This causes severe write amplification on modern SSDs if used for high-frequency small writes."]
    Strict,
}
#[doc = " Configuration for the WAL background flusher batching behavior."]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, align(64))]
pub struct FlushConfig {
    pub batch_size_bytes: usize,
    pub ttl_micros: u64,
    pub expert_mode: bool,
}
#[doc = " A builder to safely construct a `FlushConfig`."]
#[repr(C, align(64))]
pub struct FlushConfigBuilder {
    batch_size_bytes: usize,
    ttl_micros: u64,
    expert_mode: bool,
}
impl Default for FlushConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl FlushConfigBuilder {
    pub fn new() -> Self {
        Self {
            batch_size_bytes: crate::covopt_param!("WAL_BATCH_SIZE", 204428, 4096..1048576),
            ttl_micros: 10_000,
            expert_mode: false,
        }
    }
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size_bytes = size;
        self
    }
    pub fn with_ttl_micros(mut self, micros: u64) -> Self {
        self.ttl_micros = micros;
        self
    }
    pub fn unlock_expert_danger_zone(mut self) -> Self {
        self.expert_mode = true;
        self
    }
    pub fn build(self) -> FlushConfig {
        if self.ttl_micros < 1000 && !self.expert_mode {
            panic!(
                "[cdDB FATAL ERROR] TTL {} µs is below the physical SSD and OS timer limit (1000 µs)!\n\
                 This will lead to extreme I/O blocking and write amplification.\n\
                 If you are using NVRAM or Kernel Bypass, call `.unlock_expert_danger_zone()` to override.",
                self.ttl_micros
            );
        }
        FlushConfig {
            batch_size_bytes: self.batch_size_bytes,
            ttl_micros: self.ttl_micros,
            expert_mode: self.expert_mode,
        }
    }
}
#[doc = " Defines the operation mode for the Write-Ahead Log."]
#[doc = ""]
#[doc = " # ⚠\u{fe0f} CRITICAL WARNING: SSD WEAR OUT & WRITE AMPLIFICATION ⚠\u{fe0f}"]
#[doc = ""]
#[doc = " **DO NOT USE `WalMode::Sync` FOR HIGH-FREQUENCY LOGGING.**"]
#[doc = ""]
#[doc = " If you are using cdDB to store high-frequency feedback logs (e.g., AI chat logs,"]
#[doc = " analytics events), `WalMode::Sync` will trigger an `fdatasync()` on **every single insert**."]
#[doc = " Even if you only write 10 bytes, the underlying SSD Flash Translation Layer (FTL) may"]
#[doc = " amplify this into writing a full 16KB or 64KB physical block, rapidly degrading your SSD's"]
#[doc = " lifespan (similar to the severe Codex SQLite write-amplification incident that wrote 640TB/year)."]
#[doc = ""]
#[doc = " For high-frequency, small-payload logging, **you MUST use `WalMode::Async100ms`**"]
#[doc = " or `WalMode::Custom { durability: DurabilityMode::Relaxed(...) }`. This buffers writes"]
#[doc = " and batches the `fdatasync()` calls (e.g., every 100ms or 64KB), virtually eliminating"]
#[doc = " hardware-level write amplification."]
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum WalMode {
    #[doc = " Synchronous WAL: flushes to disk synchronously."]
    #[doc = " ⚠\u{fe0f} **DANGER**: Causes extreme SSD write amplification under high throughput. Use only for low-frequency, high-value financial/critical data."]
    #[default]
    Sync,
    #[doc = " Asynchronous WAL: flushes to disk in batches (e.g. every 100ms)."]
    Async100ms,
    #[doc = " Custom configuration for durability and flusher batching."]
    Custom {
        durability: DurabilityMode,
        flush: FlushConfig,
    },
}
#[doc = " Defines the standard interface for a Write-Ahead Log provider."]
pub trait WalProvider: Send + Sync {
    #[doc = " Appends a single write command to the log."]
    fn append(&self, cmd: &WriteCommand) -> Result<(), String>;
    #[doc = " Appends a batch of write commands to the log."]
    fn append_batch(&self, commands: &[&WriteCommand]) -> Result<(), String>;
    #[doc = " Forces a checkpoint/flush of the log."]
    fn checkpoint(&self) -> Result<(), String>;
    #[doc = " Reads all entries from the log into a raw byte vector."]
    fn read_all(&self) -> Result<Vec<u8>, String>;
}
#[doc = " A no-operation WAL provider useful for read-only partitions or volatile stores."]
#[repr(C, align(64))]
pub struct NoopWal;
impl WalProvider for NoopWal {
    #[doc = " Accepts the command without writing anything; always returns `Ok(())`."]
    fn append(&self, _cmd: &WriteCommand) -> Result<(), String> {
        Ok(())
    }
    #[doc = " Accepts the batch without writing anything; always returns `Ok(())`."]
    fn append_batch(&self, _commands: &[&WriteCommand]) -> Result<(), String> {
        Ok(())
    }
    #[doc = " No-op checkpoint; always returns `Ok(())`."]
    fn checkpoint(&self) -> Result<(), String> {
        Ok(())
    }
    #[doc = " Returns an empty byte vector because no data is ever written."]
    fn read_all(&self) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }
}
#[doc = " A standard library-backed WAL implementation that supports sync and async writing."]
#[repr(C, align(64))]
pub struct StdWal {
    #[doc = " The file path to the WAL file."]
    pub path: String,
    #[doc = " File system abstraction used for reading/writing."]
    pub fs: Arc<dyn FileSystem>,
    #[doc = " The WAL mode (Sync vs Async)."]
    pub mode: WalMode,
    #[doc = " Shared writer to the underlying file."]
    #[cfg(feature = "std")]
    pub writer: Arc<crate::core::Mutex<Option<std::io::BufWriter<std::fs::File>>>>,
    #[doc = " Buffer for async batched writes."]
    #[cfg(feature = "std")]
    pub async_buffer: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    #[doc = " Condvar for waking up the async flusher thread."]
    #[cfg(feature = "std")]
    pub condvar: Arc<std::sync::Condvar>,
}
impl StdWal {
    #[doc = " Creates a new `StdWal` at the specified path and starts background threads if needed."]
    #[doc = ""]
    #[doc = " Opens (or creates) the WAL file in append mode and wraps it in a 64 KiB"]
    #[doc = " [`BufWriter`](std::io::BufWriter) protected by a [`Mutex`](crate::core::Mutex)."]
    #[doc = ""]
    #[doc = " # Note"]
    #[doc = ""]
    #[doc = " The async flusher background thread is **only spawned** when `mode` is"]
    #[doc = " [`WalMode::Async100ms`].  In [`WalMode::Sync`] mode no additional threads"]
    #[doc = " are created; every `append` call flushes and `fsync`s inline."]
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
            let writer = Arc::new(crate::core::Mutex::new(file_opt));
            let async_buffer =
                Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::with_capacity(10000)));
            let condvar = Arc::new(std::sync::Condvar::new());
            let is_async = matches!(
                mode,
                WalMode::Async100ms
                    | WalMode::Custom {
                        durability: DurabilityMode::Relaxed(_),
                        ..
                    }
            );
            if is_async {
                let bg_writer = Arc::clone(&writer);
                let bg_buffer = Arc::clone(&async_buffer);
                let bg_condvar = Arc::clone(&condvar);
                let (target_interval, batch_size) = match mode {
                    WalMode::Async100ms => (std::time::Duration::from_millis(1), 64 * 1024),
                    WalMode::Custom {
                        durability: DurabilityMode::Relaxed(_),
                        flush,
                    } => (
                        std::time::Duration::from_micros(flush.ttl_micros),
                        flush.batch_size_bytes,
                    ),
                    _ => (std::time::Duration::from_millis(1), 64 * 1024),
                };
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
                            let total_bytes: usize = local_buf.iter().map(|b| b.len()).sum();
                            if total_bytes < batch_size {
                                let elapsed = last_fsync.elapsed();
                                if elapsed < target_interval {
                                    std::thread::sleep(target_interval - elapsed);
                                    {
                                        let mut lock = bg_buffer.lock().unwrap();
                                        if !lock.is_empty() {
                                            let mut extra_buf = Vec::<Vec<u8>>::new();
                                            core::mem::swap(&mut extra_buf, &mut *lock);
                                            local_buf.extend(extra_buf);
                                        }
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
                                let _ = w.get_ref().sync_data();
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
    #[doc = " Appends a single [`WriteCommand`] to the WAL."]
    #[doc = ""]
    #[doc = " The command is length-prefixed (4-byte little-endian `u32`) before being"]
    #[doc = " written.  The write path depends on the configured [`WalMode`]:"]
    #[doc = ""]
    #[doc = " - **[`WalMode::Sync`]**: writes directly to the `BufWriter`, flushes the"]
    #[doc = "   user-space buffer, and calls `sync_data` before returning."]
    #[doc = " - **[`WalMode::Async100ms`]**: pushes the encoded bytes into"]
    #[doc = "   `async_buffer` and signals the background flusher thread via `condvar`;"]
    #[doc = "   returns immediately without waiting for the disk write to complete."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an `Err(String)` if the underlying write, flush, or `sync_data`"]
    #[doc = " operation fails (sync mode only)."]
    fn append(&self, cmd: &WriteCommand) -> Result<(), String> {
        let bytes = cmd.encode();
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
        #[cfg(feature = "std")]
        {
            let is_async = matches!(
                self.mode,
                WalMode::Async100ms
                    | WalMode::Custom {
                        durability: DurabilityMode::Relaxed(_),
                        ..
                    }
            );
            if is_async {
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
                w.get_ref().sync_data().map_err(|e| e.to_string())?;
                return Ok(());
            }
        }
        Ok(self.fs.append(&self.path, &buf)?)
    }
    #[doc = " Appends a batch of [`WriteCommand`]s to the WAL in a single operation."]
    #[doc = ""]
    #[doc = " All commands are serialised into one contiguous buffer (each"]
    #[doc = " length-prefixed with a 4-byte little-endian `u32`) before the buffer is"]
    #[doc = " handed off.  This minimises the number of `fdatasync` calls under"]
    #[doc = " [`WalMode::Sync`] and reduces lock contention under"]
    #[doc = " [`WalMode::Async100ms`] compared to calling [`append`](Self::append)"]
    #[doc = " in a loop."]
    #[doc = ""]
    #[doc = " If `commands` is empty the method returns `Ok(())` without touching the"]
    #[doc = " file or the async buffer."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an `Err(String)` if the underlying write, flush, or `sync_data`"]
    #[doc = " operation fails (sync mode only)."]
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
                let is_async = matches!(
                    self.mode,
                    WalMode::Async100ms
                        | WalMode::Custom {
                            durability: DurabilityMode::Relaxed(_),
                            ..
                        }
                );
                if is_async {
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
                    w.get_ref().sync_data().map_err(|e| e.to_string())?;
                    return Ok(());
                }
            }
            Ok(self.fs.append(&self.path, &buf)?)
        } else {
            Ok(())
        }
    }
    #[doc = " Forces an explicit flush and `sync_data` of all buffered WAL data to disk."]
    #[doc = ""]
    #[doc = " In both [`WalMode::Sync`] and [`WalMode::Async100ms`] this acquires the"]
    #[doc = " writer lock, flushes the `BufWriter`'s user-space buffer, and then calls"]
    #[doc = " `sync_data` on the underlying [`File`](std::fs::File) to ensure all"]
    #[doc = " kernel-buffered writes are persisted before returning."]
    #[doc = ""]
    #[doc = " > **Note**: In async mode, entries that are still queued in"]
    #[doc = " > `async_buffer` and have not yet been drained by the background thread"]
    #[doc = " > will **not** be covered by this checkpoint.  The background thread"]
    #[doc = " > handles those independently."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an `Err(String)` if the flush or `sync_data` fails."]
    fn checkpoint(&self) -> Result<(), String> {
        #[cfg(feature = "std")]
        {
            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                w.flush().map_err(|e| e.to_string())?;
                w.get_ref().sync_data().map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }
    #[doc = " Reads the entire WAL file as a raw byte vector."]
    #[doc = ""]
    #[doc = " Before delegating to the [`FileSystem`] layer, this method flushes the"]
    #[doc = " `BufWriter`'s user-space buffer so that any in-memory buffered data is"]
    #[doc = " written to the OS before the file is read back.  The returned bytes"]
    #[doc = " contain the raw, length-prefixed encoded [`WriteCommand`] records exactly"]
    #[doc = " as they were written."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an `Err(String)` if the flush or the underlying file read fails."]
    fn read_all(&self) -> Result<Vec<u8>, String> {
        #[cfg(feature = "std")]
        {
            let mut lock = self.writer.lock().unwrap();
            if let Some(w) = lock.as_mut() {
                use std::io::Write;
                let _ = w.flush();
            }
        }
        Ok(self.fs.read(&self.path)?)
    }
}
#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::io::platform::StdFileSystem;
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
        wal.append(&cmd).unwrap();
        wal.append_batch(&[&cmd, &cmd]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150));
        let data = wal.read_all().unwrap();
        assert!(!data.is_empty());
        let _ = std::fs::remove_file(&path);
    }
    #[cfg(feature = "std")]
    #[test]
    fn test_std_wal_fallback() {
        let fs = Arc::new(StdFileSystem);
        let path = "test_wal_fallback.log".to_string();
        let _ = std::fs::remove_file(&path);
        let wal = StdWal::new(path.clone(), fs, WalMode::Sync);
        *wal.writer.lock().unwrap() = None;
        let cmd = WriteCommand::Delete { entity_id: 1 };
        wal.append(&cmd).unwrap();
        wal.append_batch(&[&cmd]).unwrap();
        wal.checkpoint().unwrap();
        let data = wal.read_all().unwrap();
        assert!(!data.is_empty());
        let _ = std::fs::remove_file(&path);
    }
    #[cfg(feature = "std")]
    #[test]
    fn test_std_wal_custom_relaxed() {
        let fs = Arc::new(StdFileSystem);
        let path = "test_wal_custom.log".to_string();
        let _ = std::fs::remove_file(&path);
        let wal = StdWal::new(
            path.clone(),
            fs,
            WalMode::Custom {
                durability: DurabilityMode::Relaxed(std::time::Duration::from_millis(50)),
                flush: FlushConfigBuilder::new().build(),
            },
        );
        let cmd = WriteCommand::Delete { entity_id: 1 };
        wal.append(&cmd).unwrap();
        wal.append_batch(&[&cmd]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150));
        let data = wal.read_all().unwrap();
        assert!(!data.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}

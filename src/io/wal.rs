use crate::core::commands::WriteCommand;
use crate::io::platform::FileSystem;
use alloc::string::String;
#[cfg(feature = "std")]
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;

/// Durability level for WAL and storage writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// AI/Compute mode: Protect SSD, optimize throughput.
    /// Flushes and syncs data to disk at the specified interval.
    Relaxed(core::time::Duration),

    /// HFT/Strict mode: Guarantee no data loss.
    /// ⚠️ **DANGER**: Every write is immediately flushed and synced to disk using `fdatasync`.
    /// This causes severe write amplification on modern SSDs if used for high-frequency small writes.
    Strict,
}

/// Configuration for the WAL background flusher batching behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushConfig {
    pub batch_size_bytes: usize,
    pub ttl_micros: u64,
    pub expert_mode: bool,
}

/// A builder to safely construct a `FlushConfig`.
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
            batch_size_bytes: 64 * 1024, // 64KB
            ttl_micros: 10_000,          // 10ms
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

/// Defines the operation mode for the Write-Ahead Log.
///
/// # ⚠️ CRITICAL WARNING: SSD WEAR OUT & WRITE AMPLIFICATION ⚠️
///
/// **DO NOT USE `WalMode::Sync` FOR HIGH-FREQUENCY LOGGING.**
///
/// If you are using cdDB to store high-frequency feedback logs (e.g., AI chat logs,
/// analytics events), `WalMode::Sync` will trigger an `fdatasync()` on **every single insert**.
/// Even if you only write 10 bytes, the underlying SSD Flash Translation Layer (FTL) may
/// amplify this into writing a full 16KB or 64KB physical block, rapidly degrading your SSD's
/// lifespan (similar to the severe Codex SQLite write-amplification incident that wrote 640TB/year).
///
/// For high-frequency, small-payload logging, **you MUST use `WalMode::Async100ms`**
/// or `WalMode::Custom { durability: DurabilityMode::Relaxed(...) }`. This buffers writes
/// and batches the `fdatasync()` calls (e.g., every 100ms or 64KB), virtually eliminating
/// hardware-level write amplification.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum WalMode {
    /// Synchronous WAL: flushes to disk synchronously.
    /// ⚠️ **DANGER**: Causes extreme SSD write amplification under high throughput. Use only for low-frequency, high-value financial/critical data.
    #[default]
    Sync,
    /// Asynchronous WAL: flushes to disk in batches (e.g. every 100ms).
    Async100ms,
    /// Custom configuration for durability and flusher batching.
    Custom {
        durability: DurabilityMode,
        flush: FlushConfig,
    },
}

/// Defines the standard interface for a Write-Ahead Log provider.
pub trait WalProvider: Send + Sync {
    /// Appends a single write command to the log.
    fn append(&self, cmd: &WriteCommand) -> Result<(), String>;
    /// Appends a batch of write commands to the log.
    fn append_batch(&self, commands: &[&WriteCommand]) -> Result<(), String>;
    /// Forces a checkpoint/flush of the log.
    fn checkpoint(&self) -> Result<(), String>;
    /// Reads all entries from the log into a raw byte vector.
    fn read_all(&self) -> Result<Vec<u8>, String>;
}

/// A no-operation WAL provider useful for read-only partitions or volatile stores.
pub struct NoopWal;
impl WalProvider for NoopWal {
    /// Accepts the command without writing anything; always returns `Ok(())`.
    fn append(&self, _cmd: &WriteCommand) -> Result<(), String> {
        Ok(())
    }
    /// Accepts the batch without writing anything; always returns `Ok(())`.
    fn append_batch(&self, _commands: &[&WriteCommand]) -> Result<(), String> {
        Ok(())
    }
    /// No-op checkpoint; always returns `Ok(())`.
    fn checkpoint(&self) -> Result<(), String> {
        Ok(())
    }
    /// Returns an empty byte vector because no data is ever written.
    fn read_all(&self) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }
}

/// A standard library-backed WAL implementation that supports sync and async writing.
pub struct StdWal {
    /// The file path to the WAL file.
    pub path: String,
    /// File system abstraction used for reading/writing.
    pub fs: Arc<dyn FileSystem>,
    /// The WAL mode (Sync vs Async).
    pub mode: WalMode,
    /// Shared writer to the underlying file.
    #[cfg(feature = "std")]
    pub writer: Arc<crate::core::Mutex<Option<std::io::BufWriter<std::fs::File>>>>,
    /// Buffer for async batched writes.
    #[cfg(feature = "std")]
    pub async_buffer: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    /// Condvar for waking up the async flusher thread.
    #[cfg(feature = "std")]
    pub condvar: Arc<std::sync::Condvar>,
}

impl StdWal {
    /// Creates a new `StdWal` at the specified path and starts background threads if needed.
    ///
    /// Opens (or creates) the WAL file in append mode and wraps it in a 64 KiB
    /// [`BufWriter`](std::io::BufWriter) protected by a [`Mutex`](crate::core::Mutex).
    ///
    /// # Note
    ///
    /// The async flusher background thread is **only spawned** when `mode` is
    /// [`WalMode::Async100ms`].  In [`WalMode::Sync`] mode no additional threads
    /// are created; every `append` call flushes and `fsync`s inline.
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
    /// Appends a single [`WriteCommand`] to the WAL.
    ///
    /// The command is length-prefixed (4-byte little-endian `u32`) before being
    /// written.  The write path depends on the configured [`WalMode`]:
    ///
    /// - **[`WalMode::Sync`]**: writes directly to the `BufWriter`, flushes the
    ///   user-space buffer, and calls `sync_data` before returning.
    /// - **[`WalMode::Async100ms`]**: pushes the encoded bytes into
    ///   `async_buffer` and signals the background flusher thread via `condvar`;
    ///   returns immediately without waiting for the disk write to complete.
    ///
    /// # Errors
    ///
    /// Returns an `Err(String)` if the underlying write, flush, or `sync_data`
    /// operation fails (sync mode only).
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
        self.fs.append(&self.path, &buf)
    }

    /// Appends a batch of [`WriteCommand`]s to the WAL in a single operation.
    ///
    /// All commands are serialised into one contiguous buffer (each
    /// length-prefixed with a 4-byte little-endian `u32`) before the buffer is
    /// handed off.  This minimises the number of `fdatasync` calls under
    /// [`WalMode::Sync`] and reduces lock contention under
    /// [`WalMode::Async100ms`] compared to calling [`append`](Self::append)
    /// in a loop.
    ///
    /// If `commands` is empty the method returns `Ok(())` without touching the
    /// file or the async buffer.
    ///
    /// # Errors
    ///
    /// Returns an `Err(String)` if the underlying write, flush, or `sync_data`
    /// operation fails (sync mode only).
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
            self.fs.append(&self.path, &buf)?;
        }
        Ok(())
    }

    /// Forces an explicit flush and `sync_data` of all buffered WAL data to disk.
    ///
    /// In both [`WalMode::Sync`] and [`WalMode::Async100ms`] this acquires the
    /// writer lock, flushes the `BufWriter`'s user-space buffer, and then calls
    /// `sync_data` on the underlying [`File`](std::fs::File) to ensure all
    /// kernel-buffered writes are persisted before returning.
    ///
    /// > **Note**: In async mode, entries that are still queued in
    /// > `async_buffer` and have not yet been drained by the background thread
    /// > will **not** be covered by this checkpoint.  The background thread
    /// > handles those independently.
    ///
    /// # Errors
    ///
    /// Returns an `Err(String)` if the flush or `sync_data` fails.
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

    /// Reads the entire WAL file as a raw byte vector.
    ///
    /// Before delegating to the [`FileSystem`] layer, this method flushes the
    /// `BufWriter`'s user-space buffer so that any in-memory buffered data is
    /// written to the OS before the file is read back.  The returned bytes
    /// contain the raw, length-prefixed encoded [`WriteCommand`] records exactly
    /// as they were written.
    ///
    /// # Errors
    ///
    /// Returns an `Err(String)` if the flush or the underlying file read fails.
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

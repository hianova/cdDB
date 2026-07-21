use alloc::string::String;
use alloc::string::ToString;
use alloc::vec::Vec;
pub use no_std_tool::sync::Backoff;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoError {
    NotFound,
    PermissionDenied,
    AlreadyExists,
    Other(&'static str),
}
#[cfg(feature = "std")]
impl From<std::io::Error> for IoError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => IoError::NotFound,
            std::io::ErrorKind::PermissionDenied => IoError::PermissionDenied,
            std::io::ErrorKind::AlreadyExists => IoError::AlreadyExists,
            _ => IoError::Other("IO Error"),
        }
    }
}
impl From<IoError> for alloc::string::String {
    fn from(err: IoError) -> Self {
        match err {
            IoError::NotFound => alloc::string::String::from("NotFound"),
            IoError::PermissionDenied => alloc::string::String::from("PermissionDenied"),
            IoError::AlreadyExists => alloc::string::String::from("AlreadyExists"),
            IoError::Other(msg) => alloc::string::String::from(msg),
        }
    }
}
#[repr(C, align(64))]
pub struct NullFileSystem;
impl FileSystem for NullFileSystem {
    fn write(&self, _path: &str, _data: &[u8]) -> Result<(), IoError> {
        Ok(())
    }
    fn read(&self, _path: &str) -> Result<Vec<u8>, IoError> {
        Ok(Vec::new())
    }
    fn append(&self, _path: &str, _data: &[u8]) -> Result<(), IoError> {
        Ok(())
    }
    fn read_range(&self, _path: &str, _offset: u64, _len: usize) -> Result<Vec<u8>, IoError> {
        Ok(Vec::new())
    }
    fn file_size(&self, _path: &str) -> Result<u64, IoError> {
        Ok(0)
    }
    fn exists(&self, _path: &str) -> bool {
        true
    }
    fn create_dir_all(&self, _path: &str) -> Result<(), IoError> {
        Ok(())
    }
    fn read_dir(&self, _path: &str) -> Result<Vec<String>, IoError> {
        Ok(Vec::new())
    }
}
#[doc = " Platform Abstraction Layer (PAL) for all file I/O operations."]
#[doc = ""]
#[doc = " `FileSystem` is the central trait that decouples the database engine from any"]
#[doc = " concrete storage backend. By programming against this trait instead of"]
#[doc = " `std::fs` directly, the crate remains portable to `no_std` environments"]
#[doc = " (embedded targets, WASM, custom runtimes, etc.) where a different"]
#[doc = " implementation can be injected at compile time or at runtime."]
#[doc = ""]
#[doc = " Implementations must be both `Send` and `Sync` so that a single shared"]
#[doc = " reference can be used safely across threads."]
#[doc = ""]
#[doc = " # Examples"]
#[doc = ""]
#[doc = " ```rust,ignore"]
#[doc = " use cddb::platform::{FileSystem, StdFileSystem};"]
#[doc = ""]
#[doc = " let fs = StdFileSystem;"]
#[doc = " fs.write(\"data.bin\", b\"hello\").unwrap();"]
#[doc = " let bytes = fs.read(\"data.bin\").unwrap();"]
#[doc = " assert_eq!(bytes, b\"hello\");"]
#[doc = " ```"]
pub trait FileSystem: Send + Sync {
    #[doc = " Atomically overwrites (or creates) the file at `path` with `data`."]
    #[doc = ""]
    #[doc = " If the file already exists its previous contents are discarded."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the file cannot be created or written to"]
    #[doc = " (e.g. permission denied, path is a directory)."]
    fn write(&self, path: &str, data: &[u8]) -> Result<(), IoError>;
    #[doc = " Reads the entire contents of the file at `path` into a `Vec<u8>`."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the file does not exist or cannot be read."]
    fn read(&self, path: &str) -> Result<Vec<u8>, IoError>;
    #[doc = " Appends `data` to the end of the file at `path`, creating it if necessary."]
    #[doc = ""]
    #[doc = " Existing file contents are preserved; the new bytes are written after them."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the file cannot be opened or written to."]
    fn append(&self, path: &str, data: &[u8]) -> Result<(), IoError>;
    #[doc = " Returns `true` if a file or directory exists at `path`."]
    fn exists(&self, path: &str) -> bool;
    #[doc = " Recursively creates `path` and all of its missing parent directories."]
    #[doc = ""]
    #[doc = " This is a no-op if the directory already exists."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if any component of the path cannot be created"]
    #[doc = " (e.g. permission denied)."]
    fn create_dir_all(&self, path: &str) -> Result<(), IoError>;
    #[doc = " Returns the names (not full paths) of all entries inside the directory"]
    #[doc = " at `path`."]
    #[doc = ""]
    #[doc = " The order of the returned names is implementation-defined."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if `path` does not exist, is not a directory, or"]
    #[doc = " cannot be read."]
    fn read_dir(&self, path: &str) -> Result<Vec<String>, IoError>;
    #[doc = " Reads exactly `len` bytes from the file at `path`, starting at `offset`."]
    #[doc = ""]
    #[doc = " The default implementation reads the whole file into memory and slices"]
    #[doc = " it. Platform implementations should override this with a more efficient"]
    #[doc = " `seek`-based approach where possible (as [`StdFileSystem`] does)."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the file cannot be read or if"]
    #[doc = " `offset + len` exceeds the file length."]
    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, IoError> {
        let all = self.read(path)?;
        let start = offset as usize;
        if start + len <= all.len() {
            Ok(all[start..start + len].to_vec())
        } else {
            Err(IoError::Other("Read out of bounds"))
        }
    }
    #[doc = " Returns the size of the file at `path` in bytes."]
    #[doc = ""]
    #[doc = " The default implementation reads the whole file and returns its length."]
    #[doc = " Platform implementations should override this with a cheaper metadata"]
    #[doc = " query where possible (as [`StdFileSystem`] does)."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the file does not exist or cannot be"]
    #[doc = " accessed."]
    fn file_size(&self, path: &str) -> Result<u64, IoError> {
        let bytes = self.read(path)?;
        Ok(bytes.len() as u64)
    }
}
#[doc = " Abstraction over thread or task spawning."]
#[doc = ""]
#[doc = " `Executor` decouples the engine from any particular concurrency runtime."]
#[doc = " An implementation backed by `std::thread` is provided ([`StdExecutor`]),"]
#[doc = " but alternative implementations could target async runtimes, thread pools,"]
#[doc = " or `no_std` cooperative schedulers."]
#[doc = ""]
#[doc = " Implementations must be both `Send` and `Sync` so that a single shared"]
#[doc = " reference can be used across threads."]
#[doc = " A handle to a spawned background task/thread."]
#[cfg(feature = "std")]
#[repr(C, align(64))]
pub struct TaskHandle(pub std::thread::JoinHandle<()>);
#[cfg(not(feature = "std"))]
#[repr(C, align(64))]
pub struct TaskHandle;
impl TaskHandle {
    #[cfg(feature = "std")]
    pub fn join(self) -> Result<(), String> {
        self.0.join().map_err(|_| "Thread panicked".to_string())
    }
    #[cfg(not(feature = "std"))]
    pub fn join(self) -> Result<(), String> {
        Ok(())
    }
}
pub trait Executor: Send + Sync {
    #[doc = " Spawns `f` as an independent, fire-and-forget task."]
    #[doc = ""]
    #[doc = " The closure takes ownership of its captured environment and is executed"]
    #[doc = " concurrently with the caller. The caller does not wait for the task to"]
    #[doc = " complete."]
    #[doc = ""]
    #[doc = " # Panics"]
    #[doc = ""]
    #[doc = " May panic (or silently fail, depending on the implementation) if the"]
    #[doc = " underlying runtime has been shut down or has reached its task limit."]
    fn spawn_task(&self, f: alloc::boxed::Box<dyn FnOnce() + Send + 'static>) -> TaskHandle;
}
#[doc = " Standard-library backed [`FileSystem`] implementation."]
#[doc = ""]
#[doc = " Delegates every operation directly to the corresponding `std::fs` function."]
#[doc = " Available only when the `std` feature is enabled."]
#[cfg(feature = "std")]
#[repr(C, align(64))]
pub struct StdFileSystem;
#[cfg(feature = "std")]
impl FileSystem for StdFileSystem {
    fn write(&self, path: &str, data: &[u8]) -> Result<(), IoError> {
        use std::io::Write;
        let mut file = std::fs::File::create(path).map_err(Into::<IoError>::into)?;
        file.write_all(data).map_err(Into::<IoError>::into)
    }
    fn read(&self, path: &str) -> Result<Vec<u8>, IoError> {
        std::fs::read(path).map_err(Into::<IoError>::into)
    }
    fn append(&self, path: &str, data: &[u8]) -> Result<(), IoError> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(Into::<IoError>::into)?;
        file.write_all(data).map_err(Into::<IoError>::into)
    }
    fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, IoError> {
        use std::io::{Read, Seek};
        let mut file = std::fs::File::open(path).map_err(Into::<IoError>::into)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .map_err(Into::<IoError>::into)?;
        let mut buf = alloc :: vec ! [0 ; len];
        file.read_exact(&mut buf).map_err(Into::<IoError>::into)?;
        Ok(buf)
    }
    fn file_size(&self, path: &str) -> Result<u64, IoError> {
        std::fs::metadata(path)
            .map(|m| m.len())
            .map_err(Into::<IoError>::into)
    }
    fn exists(&self, path: &str) -> bool {
        std::path::Path::new(path).exists()
    }
    fn create_dir_all(&self, path: &str) -> Result<(), IoError> {
        std::fs::create_dir_all(path).map_err(Into::<IoError>::into)
    }
    fn read_dir(&self, path: &str) -> Result<Vec<String>, IoError> {
        let entries = std::fs::read_dir(path).map_err(Into::<IoError>::into)?;
        let mut names = Vec::new();
        for entry in entries {
            if let Ok(entry) = entry
                && let Ok(name) = entry.file_name().into_string()
            {
                names.push(name);
            }
        }
        Ok(names)
    }
}
#[doc = " Standard-library backed [`Executor`] that spawns each task as an OS thread."]
#[doc = ""]
#[doc = " Each call to [`spawn_task`][Executor::spawn_task] creates a new"]
#[doc = " `std::thread`, which is suitable for coarse-grained background work."]
#[doc = " Available only when the `std` feature is enabled."]
#[cfg(feature = "std")]
#[repr(C, align(64))]
pub struct StdExecutor;
#[doc = " Abstracts blocking and non-blocking receive from a command queue."]
#[doc = ""]
#[doc = " `MessageQueue` provides a uniform interface for consuming"]
#[doc = " [`PartitionCommand`][crate::core::commands::PartitionCommand] values regardless of"]
#[doc = " the underlying queue implementation. Callers can choose between a blocking"]
#[doc = " receive ([`recv`][MessageQueue::recv]) that parks the thread until a command"]
#[doc = " is available, or a non-blocking attempt ([`try_recv`][MessageQueue::try_recv])"]
#[doc = " that returns immediately."]
#[doc = ""]
#[doc = " Implementations must be both `Send` and `Sync`."]
pub trait MessageQueue: Send + Sync {
    #[doc = " Blocks the current thread until a [`PartitionCommand`][crate::core::commands::PartitionCommand]"]
    #[doc = " is available, then returns it."]
    #[doc = ""]
    #[doc = " Internally uses a spin-then-yield backoff (see [`Backoff`]) to avoid"]
    #[doc = " busy-waiting indefinitely while remaining responsive."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the queue is in an unrecoverable error state."]
    fn recv(&self) -> Result<crate::core::commands::PartitionCommand, String>;
    #[doc = " Attempts to receive a [`PartitionCommand`][crate::core::commands::PartitionCommand]"]
    #[doc = " without blocking."]
    #[doc = ""]
    #[doc = " Returns immediately: `Ok(cmd)` if a command was queued, or an error if"]
    #[doc = " the queue was empty."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns `Err(\"Empty\")` when no command is currently available."]
    fn try_recv(&self) -> Result<crate::core::commands::PartitionCommand, String>;
}
#[doc = " Standard-library backed [`MessageQueue`] that wraps a [`BoundedQueue`][crate::core::queue::BoundedQueue]."]
#[doc = ""]
#[doc = " Uses a shared [`Arc`][alloc::sync::Arc] so the same queue can be owned by"]
#[doc = " both a [`StdMessageSender`] (producer side) and a `StdMessageQueue`"]
#[doc = " (consumer side) concurrently. Available only when the `std` feature is"]
#[doc = " enabled."]
#[cfg(feature = "std")]
#[repr(C, align(64))]
pub struct StdMessageQueue {
    #[doc = " The shared bounded queue from which commands are consumed."]
    pub rx: alloc::sync::Arc<
        no_std_tool::collections::BoundedQueue<crate::core::commands::PartitionCommand, 262144>,
    >,
}
#[cfg(feature = "std")]
impl MessageQueue for StdMessageQueue {
    fn recv(&self) -> Result<crate::core::commands::PartitionCommand, String> {
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
    fn try_recv(&self) -> Result<crate::core::commands::PartitionCommand, String> {
        self.rx.pop().ok_or_else(|| "Empty".to_string())
    }
}
#[cfg(feature = "std")]
impl Executor for StdExecutor {
    fn spawn_task(&self, f: alloc::boxed::Box<dyn FnOnce() + Send + 'static>) -> TaskHandle {
        let handle = std::thread::Builder::new()
            .name("cddb_partition_executor".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(f)
            .expect("Failed to spawn partition thread");
        TaskHandle(handle)
    }
}
#[doc = " Pluggable Thread-Local Storage (TLS) abstraction for `no_std` environments."]
#[doc = ""]
#[doc = " Standard Rust TLS (`thread_local!`) is unavailable in `no_std` contexts."]
#[doc = " This trait lets callers inject an alternative TLS implementation —"]
#[doc = " for example, one backed by a global array indexed by a task ID — while"]
#[doc = " keeping the rest of the codebase independent of the concrete mechanism."]
#[doc = ""]
#[doc = " The type parameter `T` is the value stored per-thread (or per-task)."]
#[doc = ""]
#[doc = " Implementations must be both `Send` and `Sync` so that the provider itself"]
#[doc = " can be shared across threads even though the values it manages are"]
#[doc = " per-thread."]
pub trait ThreadLocalProvider<T>: Send + Sync {
    #[doc = " Runs `f` with a shared reference to the current thread's value, if any."]
    #[doc = ""]
    #[doc = " If no value has been set for the current thread, `f` receives `None`."]
    #[doc = " The return value of `f` is forwarded to the caller."]
    fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&T>) -> R;
    #[doc = " Stores `val` as the current thread's value, replacing any previous one."]
    fn set(&self, val: T);
}
#[doc = " Abstracts sending [`PartitionCommand`][crate::core::commands::PartitionCommand]"]
#[doc = " values into a command queue."]
#[doc = ""]
#[doc = " `MessageSender` is the producer-side counterpart to [`MessageQueue`]. It"]
#[doc = " decouples callers from the concrete queue implementation and allows"]
#[doc = " alternative transports (e.g. channels, shared-memory rings, or network"]
#[doc = " sockets) to be substituted without changing engine code."]
#[doc = ""]
#[doc = " Implementations must be both `Send` and `Sync`."]
#[allow(dead_code)]
pub trait MessageSender: Send + Sync {
    #[doc = " Enqueues `cmd` for delivery to the corresponding [`MessageQueue`]."]
    #[doc = ""]
    #[doc = " Blocks (using a spin-then-yield backoff) until space is available in the"]
    #[doc = " queue if it is full. Implementations that cannot block should document"]
    #[doc = " their own overflow behaviour."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns an error string if the command could not be sent due to an"]
    #[doc = " unrecoverable queue error."]
    fn send(&self, cmd: crate::core::commands::PartitionCommand) -> Result<(), String>;
}
#[doc = " Standard-library backed [`MessageSender`] that writes into a shared"]
#[doc = " [`BoundedQueue`][crate::core::queue::BoundedQueue]."]
#[doc = ""]
#[doc = " Pairs with [`StdMessageQueue`]: both hold an [`Arc`][alloc::sync::Arc] to"]
#[doc = " the same underlying queue, forming a single-producer / single-consumer"]
#[doc = " (or multi-producer) channel. Available only when the `std` feature is"]
#[doc = " enabled."]
#[cfg(feature = "std")]
#[allow(dead_code)]
#[repr(C, align(64))]
pub struct StdMessageSender {
    #[doc = " The shared bounded queue into which commands are pushed."]
    pub tx: alloc::sync::Arc<
        no_std_tool::collections::BoundedQueue<crate::core::commands::PartitionCommand, 262144>,
    >,
}
#[cfg(feature = "std")]
impl MessageSender for StdMessageSender {
    fn send(&self, mut cmd: crate::core::commands::PartitionCommand) -> Result<(), String> {
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
#[repr(C, align(64))]
pub struct SpinMessageQueue {
    pub rx: alloc::sync::Arc<
        no_std_tool::sync::SpinMutex<
            alloc::collections::VecDeque<crate::core::commands::PartitionCommand>,
        >,
    >,
}
impl MessageQueue for SpinMessageQueue {
    fn recv(&self) -> Result<crate::core::commands::PartitionCommand, String> {
        let mut backoff = Backoff::new();
        loop {
            if let Some(cmd) = self.rx.lock().unwrap().pop_front() {
                return Ok(cmd);
            }
            if backoff.is_completed() {
                #[cfg(feature = "std")]
                std::thread::yield_now();
                #[cfg(not(feature = "std"))]
                core::hint::spin_loop();
            } else {
                backoff.snooze();
            }
        }
    }
    fn try_recv(&self) -> Result<crate::core::commands::PartitionCommand, String> {
        self.rx
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| "Empty".to_string())
    }
}
#[repr(C, align(64))]
pub struct SpinMessageSender {
    pub tx: alloc::sync::Arc<
        no_std_tool::sync::SpinMutex<
            alloc::collections::VecDeque<crate::core::commands::PartitionCommand>,
        >,
    >,
}
impl MessageSender for SpinMessageSender {
    fn send(&self, cmd: crate::core::commands::PartitionCommand) -> Result<(), String> {
        self.tx.lock().unwrap().push_back(cmd);
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_std_file_system() {
        let fs = StdFileSystem;
        let path = "test_fs.txt";
        let _ = std::fs::remove_file(path);
        fs.write(path, b"hello").unwrap();
        assert!(fs.exists(path));
        assert_eq!(fs.file_size(path).unwrap(), 5);
        let content = fs.read(path).unwrap();
        assert_eq!(content, b"hello");
        fs.append(path, b" world").unwrap();
        let content2 = fs.read(path).unwrap();
        assert_eq!(content2, b"hello world");
        let range = fs.read_range(path, 0, 5).unwrap();
        assert_eq!(range, b"hello");
        assert!(fs.read_range(path, 100, 5).is_err());
        fs.create_dir_all("test_dir").unwrap();
        let dir_content = fs.read_dir("test_dir").unwrap();
        assert_eq!(dir_content.len(), 0);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir("test_dir");
    }
    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_std_executor_and_queue() {
        let exec = StdExecutor;
        let q = alloc::sync::Arc::new(no_std_tool::collections::BoundedQueue::new());
        let mq = StdMessageQueue { rx: q.clone() };
        let ms = StdMessageSender { tx: q };
        let cmd = crate::core::commands::PartitionCommand::Shutdown;
        ms.send(cmd).unwrap();
        let recv_cmd = mq.recv().unwrap();
        use std::assert_matches;
        assert_matches!(recv_cmd, crate::core::commands::PartitionCommand::Shutdown);
        exec.spawn_task(alloc::boxed::Box::new(|| {
            let _ = 1 + 1;
        }));
    }
    #[test]
    fn test_filesystem_default_impls() {
        use alloc::vec;
        struct DummyFS;
        impl FileSystem for DummyFS {
            fn write(&self, _path: &str, _data: &[u8]) -> Result<(), IoError> {
                Ok(())
            }
            fn read(&self, path: &str) -> Result<Vec<u8>, IoError> {
                if path == "ok" {
                    Ok(vec![1, 2, 3])
                } else {
                    Err(IoError::Other("err"))
                }
            }
            fn append(&self, _path: &str, _data: &[u8]) -> Result<(), IoError> {
                Ok(())
            }
            fn exists(&self, _path: &str) -> bool {
                false
            }
            fn create_dir_all(&self, _path: &str) -> Result<(), IoError> {
                Ok(())
            }
            fn read_dir(&self, _path: &str) -> Result<Vec<String>, IoError> {
                Ok(vec![])
            }
        }
        let fs = DummyFS;
        assert_eq!(fs.read_range("ok", 0, 2).unwrap(), vec![1, 2]);
        assert!(fs.read_range("ok", 0, 5).is_err());
        assert_eq!(fs.file_size("ok").unwrap(), 3);
        assert!(fs.file_size("err").is_err());
        assert!(fs.write("test", &[]).is_ok());
        assert!(fs.append("test", &[]).is_ok());
        assert!(!fs.exists("test"));
        assert!(fs.create_dir_all("test").is_ok());
        assert!(fs.read_dir("test").unwrap().is_empty());
    }
    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_std_message_sender_backoff() {
        let q = alloc::sync::Arc::new(no_std_tool::collections::BoundedQueue::new());
        let ms = StdMessageSender { tx: q.clone() };
        ms.tx
            .push(crate::core::commands::PartitionCommand::Shutdown)
            .unwrap();
        let q_clone = q.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let _ = q_clone.pop();
        });
        ms.send(crate::core::commands::PartitionCommand::Shutdown)
            .unwrap();
    }
    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_std_filesystem_errors() {
        let fs = StdFileSystem;
        assert!(fs.read_dir("/nonexistent_dir_12345").is_err());
        let _ = std::fs::remove_file("test_file_dir");
        std::fs::write("test_file_dir", "data").unwrap();
        assert!(fs.create_dir_all("test_file_dir/sub").is_err());
        let _ = std::fs::remove_file("test_file_dir");
    }
}

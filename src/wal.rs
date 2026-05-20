use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use crate::commands::WriteCommand;
use crate::platform::FileSystem;

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
}

impl StdWal {
    pub fn new(path: String, fs: Arc<dyn FileSystem>) -> Self {
        Self { path, fs }
    }
}

impl WalProvider for StdWal {
    fn append(&self, cmd: &WriteCommand) -> Result<(), String> {
        let bytes = cmd.encode();
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
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
            self.fs.append(&self.path, &buf)?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), String> {
        // In a more complex implementation, this would truncate the WAL
        // after a full state snapshot. For now, it's a no-op or sync.
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<u8>, String> {
        self.fs.read(&self.path)
    }
}

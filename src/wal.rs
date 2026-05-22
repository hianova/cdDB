use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::{String, ToString};
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
    #[cfg(feature = "std")]
    pub writer: crate::platform::Mutex<Option<std::io::BufWriter<std::fs::File>>>,
}

impl StdWal {
    pub fn new(path: String, fs: Arc<dyn FileSystem>) -> Self {
        #[cfg(feature = "std")]
        {
            use std::fs::OpenOptions;
            let file_opt = OpenOptions::new()
                .create(true)
                .append(true)
                .write(true)
                .open(&path)
                .ok()
                .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f));
            Self {
                path,
                fs,
                writer: crate::platform::Mutex::new(file_opt),
            }
        }
        #[cfg(not(feature = "std"))]
        Self { path, fs }
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

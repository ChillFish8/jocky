use std::fmt::{Debug, Formatter};
use std::io;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken,
    FileHandle,
    OwnedBytes,
    TerminatingWrite,
    WatchCallback,
    WatchCallbackList,
    WatchHandle,
    WritePtr,
};
use tantivy::Directory;

use crate::directories::IGNORE_FILES;
use crate::metadata::SegmentMetadata;

/// An immutable segment reader which act as a tantivy directory.
pub struct DirectoryReader {
    file_path: PathBuf,
    metadata: Arc<SegmentMetadata>,
    bytes: OwnedBytes,
    watcher: Arc<WatchCallbackList>,
}

impl DirectoryReader {
    /// Create a new directory writer.
    pub(crate) fn new(
        fp: impl AsRef<Path>,
        bytes: OwnedBytes,
        metadata: SegmentMetadata,
    ) -> Self {
        Self {
            file_path: fp.as_ref().to_path_buf(),
            metadata: Arc::new(metadata),
            watcher: Default::default(),
            bytes,
        }
    }
}

impl Debug for DirectoryReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectoryReader({:?})", self.file_path)
    }
}

impl Clone for DirectoryReader {
    fn clone(&self) -> Self {
        Self {
            file_path: self.file_path.clone(),
            metadata: self.metadata.clone(),
            bytes: self.bytes.clone(),
            watcher: self.watcher.clone(),
        }
    }
}

impl Directory for DirectoryReader {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let path_str = path.to_string_lossy();
        let pos = self
            .metadata
            .get_location(&path_str)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        Ok(Arc::new(
            self.bytes.slice(pos.start as usize..pos.end as usize),
        ))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let fp = path.to_string_lossy();
        if IGNORE_FILES.contains(&fp.as_ref()) {
            Ok(())
        } else {
            Err(DeleteError::IoError {
                io_error: Arc::new(io::Error::new(
                    ErrorKind::Other,
                    "Cannot perform mutable operations on a immutable segment",
                )),
                filepath: path.to_path_buf(),
            })
        }
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let fp = path.to_string_lossy();
        if !IGNORE_FILES.contains(&fp.as_ref()) {
            Ok(false)
        } else {
            Ok(self.metadata.get_location(&fp).is_some())
        }
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        Ok(WritePtr::new(Box::new(NoOpWriter)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let path_str = path.to_string_lossy();
        let pos = self
            .metadata
            .get_location(&path_str)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        Ok(self
            .bytes
            .slice(pos.start as usize..pos.end as usize)
            .to_vec())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watcher.subscribe(watch_callback))
    }
}

/// A writer which only performs no ops while returning ok.
pub struct NoOpWriter;

impl Write for NoOpWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for NoOpWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        Ok(())
    }
}

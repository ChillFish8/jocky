use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use puppet::{DeferredResponse, Message};
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
use tantivy::{Directory, HasLen};
use tracing::error;

use crate::actors::messages::WriteBuffer;
use crate::actors::writers::AutoWriterSelector;

#[derive(Clone)]
pub struct LinearSegmentWriter {
    pub writer: AutoWriterSelector,
    pub watches: Arc<WatchCallbackList>,
    pub atomic_files: Arc<RwLock<BTreeMap<PathBuf, Vec<u8>>>>,
}

impl Debug for LinearSegmentWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectoryWriter")
    }
}

impl Directory for LinearSegmentWriter {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.writer
            .get_file_handle(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.writer.delete(path).map_err(|e| {
            error!(error = ?e, "Failed to delete file.");
            e
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.writer.exists(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let writer = MessageWriter {
            writer: self.writer.clone(),
            path: path.to_path_buf(),
            deferred: None,
        };

        Ok(WritePtr::new(Box::new(writer)))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.atomic_files
            .read()
            .get(path)
            .cloned()
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        {
            self.atomic_files
                .write()
                .insert(path.to_path_buf(), data.to_vec());
        }

        self.writer.atomic_write(path, data).map_err(|e| {
            error!(error = ?e, "Failed to atomic-write file.");
            e
        })
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.watches.subscribe(watch_callback))
    }
}

pub struct MessageWriter {
    path: PathBuf,
    writer: AutoWriterSelector,
    deferred: Option<DeferredResponse<<WriteBuffer as Message>::Output>>,
}

impl Write for MessageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(deferred) = self.deferred.as_mut().and_then(|d| d.try_recv()) {
            if let Err(e) = deferred {
                error!(error = ?e, file_path = %self.path.display(), "Deferred response failed to write data for file");
                return Err(e);
            }
        }

        let n = buf.len();

        let deferred = self.writer.write(&self.path, buf);
        self.deferred = Some(deferred);

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for MessageWriter {
    fn terminate_ref(&mut self, _token: AntiCallToken) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct FileReader {
    pub path: PathBuf,
    pub file_size: usize,
    pub writer: AutoWriterSelector,
}

impl Debug for FileReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileReader(path={:?})", self.path)
    }
}

impl HasLen for FileReader {
    fn len(&self) -> usize {
        self.file_size
    }
}

impl FileHandle for FileReader {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let buf = self.writer.read(&self.path, range).map_err(|e| {
            error!(error = ?e, "Failed to atomic-write file.");
            e
        })?;
        Ok(buf)
    }
}

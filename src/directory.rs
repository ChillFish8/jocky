use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use puppet::{ActorMailbox, DeferredResponse, Message};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{AntiCallToken, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchCallbackList, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};

use crate::actors::AioDirectoryStreamWriter;
use crate::actors::messages::{FileExists, FileLen, ReadRange, RemoveFile, WriteBuffer, WriteStaticBuffer};


#[derive(Clone)]
pub struct LinearSegmentWriter {
    pub writer: ActorMailbox<AioDirectoryStreamWriter>,
    pub watches: Arc<WatchCallbackList>,
    pub atomic_files: Arc<RwLock<BTreeMap<PathBuf, Vec<u8>>>>,
}

impl Debug for LinearSegmentWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectoryWriter")
    }
}

impl Directory for LinearSegmentWriter {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let msg = FileLen {
            file_path: path.to_path_buf(),
        };
        self.writer
            .send_sync(msg)
            .map(|file_size| {
                Arc::new(FileReader { path: path.to_path_buf(), writer: self.writer.clone(), file_size })
                    as Arc<dyn FileHandle>
            })
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let msg = RemoveFile {
            file_path: path.to_path_buf(),
        };

        self.writer
            .send_sync(msg)
            .map_err(|e| DeleteError::IoError { io_error: e.into(), filepath: path.to_path_buf() })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let msg = FileExists {
            file_path: path.to_path_buf(),
        };

        Ok(self.writer.send_sync(msg))
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

        // SAFETY:
        // This is safe because we ensure that the buffer lives
        // at least as long as the actor requires it for.
        let fake_lifetime_buffer = unsafe {
            std::mem::transmute::<_, &'static [u8]>(data)
        };

        let msg = WriteStaticBuffer {
            file_path: path.to_path_buf(),
            buffer: fake_lifetime_buffer,
            overwrite: true,
        };

        self.writer.send_sync(msg)
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
    writer: ActorMailbox<AioDirectoryStreamWriter>,
    deferred: Option<DeferredResponse<<WriteBuffer as Message>::Output>>
}

impl Write for MessageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(deferred) = self.deferred.as_mut().and_then(|d| d.try_recv()) {
            if let Err(e) = deferred {
                return Err(e);
            }
        }
        
        let n = buf.len();

        let msg = WriteBuffer {
            file_path: self.path.to_path_buf(),
            buffer: buf.to_vec(),
            overwrite: false,
        };

        let deferred = futures_lite::future::block_on(self.writer.deferred_send(msg));
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
    path: PathBuf,
    file_size: usize,
    writer: ActorMailbox<AioDirectoryStreamWriter>,
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
        let msg = ReadRange {
            range,
            file_path: self.path.clone(),
        };

        let buf = self.writer.send_sync(msg)?;
        Ok(OwnedBytes::new(buf))
    }
}

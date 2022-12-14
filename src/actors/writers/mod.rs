use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use glommio::Placement;
use puppet::{ActorMailbox, DeferredResponse, Message};
use tantivy::directory::error::DeleteError;
use tantivy::directory::{FileHandle, OwnedBytes};
use tracing::warn;

use crate::actors::messages::{ExportSegment, FileExists, FileLen, ReadRange, RemoveFile, WriteBuffer, WriteStaticBuffer};
use crate::directory::FileReader;

#[cfg(target_os = "linux")]
pub mod aio;
pub mod blocking;

#[derive(Clone)]
pub enum AutoWriterSelector {
    #[cfg(target_os = "linux")]
    /// An asynchronous writer.
    ///
    /// This makes use of io_uring and direct IO, typically resulting in more
    /// consistent performance and lower memory usage when indexing.
    Aio(ActorMailbox<aio::AioDirectoryStreamWriter>),
    /// The default std File.
    ///
    /// This is backed by a BufWriter which aids in the writing performance.
    Blocking(ActorMailbox<blocking::DirectoryStreamWriter>),
}

impl AutoWriterSelector {
    pub async fn create(
        file_path: impl AsRef<Path> + Send + 'static,
        size_hint: u64,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        if check_uring_ok() {
            warn!("Using AIO API");
            let aio_actor = aio::AioDirectoryStreamWriter::create(file_path, size_hint);
            return Ok(Self::Aio(aio_actor));
        }

        warn!("Using blocking API");
        let blocking_actor =
            blocking::DirectoryStreamWriter::create(file_path, size_hint).await?;
        Ok(Self::Blocking(blocking_actor))
    }

    pub fn exists(&self, path: &Path) -> bool {
        let msg = FileExists {
            file_path: path.to_path_buf(),
        };

        match self {
            AutoWriterSelector::Aio(writer) => writer.send_sync(msg),
            AutoWriterSelector::Blocking(writer) => writer.send_sync(msg),
        }
    }

    pub fn get_file_handle(&self, path: &Path) -> Option<Arc<dyn FileHandle>> {
        let msg = FileLen {
            file_path: path.to_path_buf(),
        };

        let res = match self {
            AutoWriterSelector::Aio(writer) => writer.send_sync(msg),
            AutoWriterSelector::Blocking(writer) => writer.send_sync(msg),
        };

        res.map(|file_size| {
            Arc::new(FileReader {
                path: path.to_path_buf(),
                writer: self.clone(),
                file_size,
            }) as Arc<dyn FileHandle>
        })
    }

    pub fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let msg = RemoveFile {
            file_path: path.to_path_buf(),
        };

        let res = match self {
            AutoWriterSelector::Aio(writer) => writer.send_sync(msg),
            AutoWriterSelector::Blocking(writer) => writer.send_sync(msg),
        };

        res.map_err(|e| DeleteError::IoError {
            io_error: e.into(),
            filepath: path.to_path_buf(),
        })
    }

    pub fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        // SAFETY:
        // This is safe because we ensure that the buffer lives
        // at least as long as the actor requires it for.
        let fake_lifetime_buffer =
            unsafe { std::mem::transmute::<_, &'static [u8]>(data) };

        let msg = WriteStaticBuffer {
            file_path: path.to_path_buf(),
            buffer: fake_lifetime_buffer,
            overwrite: true,
        };

        match self {
            AutoWriterSelector::Aio(writer) => writer.send_sync(msg),
            AutoWriterSelector::Blocking(writer) => writer.send_sync(msg),
        }
    }

    pub fn write(
        &mut self,
        path: &Path,
        buf: &[u8],
    ) -> DeferredResponse<<WriteBuffer as Message>::Output> {
        let msg = WriteBuffer {
            file_path: path.to_path_buf(),
            buffer: buf.to_vec(),
            overwrite: false,
        };

        match self {
            AutoWriterSelector::Aio(writer) => {
                futures_lite::future::block_on(writer.deferred_send(msg))
            },
            AutoWriterSelector::Blocking(writer) => {
                futures_lite::future::block_on(writer.deferred_send(msg))
            },
        }
    }

    pub fn read(&self, path: &Path, range: Range<usize>) -> io::Result<OwnedBytes> {
        let msg = ReadRange {
            range,
            file_path: path.to_path_buf(),
        };

        match self {
            AutoWriterSelector::Aio(writer) => writer.send_sync(msg),
            AutoWriterSelector::Blocking(writer) => writer.send_sync(msg),
        }
    }

    pub async fn export_to(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let msg = ExportSegment {
            file_path: path.as_ref().to_path_buf()
        };

        match self {
            AutoWriterSelector::Aio(writer) => writer.send(msg).await,
            AutoWriterSelector::Blocking(writer) => writer.send(msg).await,
        }
    }
}

#[cfg(any(not(target_os = "linux"), feature = "disable-aio"))]
fn check_uring_ok() -> bool {
    false
}

#[cfg(all(target_os = "linux", not(feature = "disable-aio")))]
fn check_uring_ok() -> bool {
    glommio::LocalExecutorBuilder::new(Placement::Unbound)
        .spawn(|| async move {})
        .expect("Spawn worker thread.")
        .join()
        .map_err(
            |e| warn!(error = ?e, "System was unable to use AIO writer due to error."),
        )
        .is_ok()
}

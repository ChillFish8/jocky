use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use glommio::Placement;
use puppet::{ActorMailbox, DeferredResponse, Message};
use tantivy::directory::error::DeleteError;
use tantivy::directory::{FileHandle, OwnedBytes};
use tracing::{debug, warn};

use crate::actors::messages::{
    ExportSegment,
    FileExists,
    FileLen,
    ReadRange,
    RemoveFile,
    WriteBuffer,
    WriteStaticBuffer,
};
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
    /// Creates a new directory writer.
    ///
    /// A size hint can be provided in order to pre-allocate a file of that size, this can be useful
    /// in places where you know the rough or minimum size of the file.
    pub async fn create(
        file_path: impl AsRef<Path> + Send + 'static,
        size_hint: u64,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        if check_uring_ok() {
            debug!("IO uring is available, creating backing writer.");
            let aio_actor = aio::AioDirectoryStreamWriter::create(file_path, size_hint);
            return Ok(Self::Aio(aio_actor));
        }

        debug!("IO uring is not available on this machine of does not have the necessary environment to use it. Defaulting to blocking IO.");
        let blocking_actor =
            blocking::DirectoryStreamWriter::create(file_path, size_hint).await?;
        Ok(Self::Blocking(blocking_actor))
    }

    /// Checks if a file exists in the writer.
    pub fn exists(&self, path: &Path) -> bool {
        let msg = FileExists {
            file_path: path.to_path_buf(),
        };

        match self {
            AutoWriterSelector::Aio(writer) => writer.send_sync(msg),
            AutoWriterSelector::Blocking(writer) => writer.send_sync(msg),
        }
    }

    /// Creates a new file handle for a given path.
    ///
    /// This is just a handle to the writer itself rather than creating a new file.
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

    /// Deletes a file from the writer.
    ///
    /// Due to the nature of this file being append-only, deletes simply
    /// mark file data as not used rather than free the disk space.
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

    /// Writes some data to the writer.
    ///
    /// This will overwrite any existing data associated with the file
    /// if it already exists.
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

    /// Appends some data to the writer for a given file.
    ///
    /// Writes occur asynchronously to prevent a stall when several writers
    /// try contact and wait for the actor at once. Instead, a deferred response is
    /// returned.
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

    /// Reads a range of data from the given path.
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

    /// Exports all data written to this writer to a de-fragmented file with a given set of metadata
    /// and hot cache (buffer accessed as part of the metadata).
    ///
    /// Unlike the file being actively written to, the data exported is contiguous on a per-file basis
    /// rather than being heavily fragmented.
    ///
    /// NOTE:
    /// *Once exported, the internal writer will reset it's position and being overwriting old data.*
    pub async fn export_to(
        &self,
        path: impl AsRef<Path>,
        hot_cache: Vec<u8>,
    ) -> io::Result<()> {
        let msg = ExportSegment {
            file_path: path.as_ref().to_path_buf(),
            hot_cache,
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

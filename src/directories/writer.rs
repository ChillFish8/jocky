use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
use tantivy::Directory;

use crate::directories::IGNORE_FILES;

/// A writer which wraps an inner directory.
///
/// This just tracks the currently live files which need to be
/// exported into a given segment.
pub struct DirectoryWriter<D: Directory> {
    inner: D,
    file_path: PathBuf,
    files_to_read: Arc<Mutex<BTreeSet<PathBuf>>>,
}

impl<D: Directory> DirectoryWriter<D> {
    /// Create a new directory writer.
    pub(crate) fn new(inner: D, fp: impl AsRef<Path>) -> Self {
        Self {
            inner,
            files_to_read: Default::default(),
            file_path: fp.as_ref().to_path_buf(),
        }
    }

    /// The current set of files to export.
    pub fn files(&self) -> BTreeSet<PathBuf> {
        self.files_to_read.lock().clone()
    }
}

impl<D: Directory> Debug for DirectoryWriter<D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectoryWriter({:?})", self.inner)
    }
}

impl<D: Directory + Clone> Clone for DirectoryWriter<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            file_path: self.file_path.clone(),
            files_to_read: self.files_to_read.clone(),
        }
    }
}

impl<D: Directory + Clone> Directory for DirectoryWriter<D> {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.files_to_read.lock().remove(path);
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let fp = path.to_string_lossy();
        if !IGNORE_FILES.contains(&fp.as_ref()) {
            self.files_to_read.lock().insert(path.to_path_buf());
        }
        self.inner.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        let fp = path.to_string_lossy();
        if !IGNORE_FILES.contains(&fp.as_ref()) {
            self.files_to_read.lock().insert(path.to_path_buf());
        }
        self.inner.atomic_write(path, data)
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        self.inner.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

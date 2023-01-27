use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
use tantivy::Directory;

use crate::directories::{DirectoryReader, DirectoryWriter};

pub struct DirectoryMerger<D: Directory> {
    readers: Vec<DirectoryReader>,
    file_mapping: Arc<BTreeMap<PathBuf, usize>>,
    live_atomic_files: Arc<RwLock<BTreeMap<PathBuf, Vec<u8>>>>,
    writer: DirectoryWriter<D>,
}

impl<D: Directory> DirectoryMerger<D> {
    /// Create a new directory writer.
    pub(crate) fn new(
        writer: DirectoryWriter<D>,
        readers: Vec<DirectoryReader>,
    ) -> Self {
        Self {
            writer,
            readers,
            file_mapping: Arc::new(BTreeMap::new()),
            live_atomic_files: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Consumes the merger and returns the inner writer.
    pub fn into_writer(self) -> DirectoryWriter<D> {
        self.writer
    }
}

impl<D: Directory> Debug for DirectoryMerger<D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectoryMerger")
    }
}

impl<D: Directory + Clone> Clone for DirectoryMerger<D> {
    fn clone(&self) -> Self {
        Self {
            writer: self.writer.clone(),
            readers: self.readers.clone(),
            live_atomic_files: self.live_atomic_files.clone(),
            file_mapping: self.file_mapping.clone(),
        }
    }
}

impl<D: Directory + Clone> Directory for DirectoryMerger<D> {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if let Some(index) = self.file_mapping.get(path) {
            return self.readers[*index].get_file_handle(path);
        }

        self.writer.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        if self.file_mapping.contains_key(path) {
            return Ok(());
        }

        self.writer.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.file_mapping.contains_key(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.writer.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(data) = self.live_atomic_files.read().get(path) {
            return Ok(data.clone());
        }

        if let Some(index) = self.file_mapping.get(path) {
            return self.readers[*index].atomic_read(path);
        }

        self.writer.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.live_atomic_files
            .write()
            .insert(path.to_path_buf(), data.to_vec());

        self.writer.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.writer.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.writer.watch(watch_callback)
    }
}

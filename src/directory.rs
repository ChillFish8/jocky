use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use bytes::Bytes;

use parking_lot::RwLock;
use puppet::ActorMailbox;
use tantivy::directory::error::{
    DeleteError,
    OpenDirectoryError,
    OpenReadError,
    OpenWriteError,
};
use tantivy::directory::{AntiCallToken, FileHandle, FileSlice, MmapDirectory, OwnedBytes, TerminatingWrite, WatchCallback, WatchCallbackList, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};
use tracing::info;
use crate::actors::DirectoryStreamWriter;
use crate::actors::messages::{FileExists, FileLen, ReadRange, RemoveFile, WriteBuffer, WriteStaticBuffer};


#[derive(Clone)]
pub struct LinearSegmentWriter {
    pub writer: ActorMailbox<DirectoryStreamWriter>,
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
                info!(file_size = file_size, path = ?path, "Got file with size.");
                Arc::new(FileReader { path: path.to_path_buf(), writer: self.writer.clone(), file_size })
                    as Arc<dyn FileHandle>
            })
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        info!(path = %path.display(), "Delete");

        let msg = RemoveFile {
            file_path: path.to_path_buf(),
        };

        self.writer
            .send_sync(msg)
            .map_err(|e| DeleteError::IoError { io_error: e.into(), filepath: path.to_path_buf() })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        info!(path = %path.display(), "Exists");

        let msg = FileExists {
            file_path: path.to_path_buf(),
        };

        Ok(self.writer.send_sync(msg))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        info!(path = %path.display(), "Open write");

        let writer = MessageWriter {
            writer: self.writer.clone(),
            path: path.to_path_buf(),
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
    writer: ActorMailbox<DirectoryStreamWriter>,
}

impl Write for MessageWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = buf.len();

        // SAFETY:
        // This is safe because we ensure that the buffer lives
        // at least as long as the actor requires it for.
        let fake_lifetime_buffer = unsafe {
            std::mem::transmute::<_, &'static [u8]>(buf)
        };

        let msg = WriteStaticBuffer {
            file_path: self.path.to_path_buf(),
            buffer: fake_lifetime_buffer,
            overwrite: false,
        };

        self.writer.send_sync(msg)?;

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
    writer: ActorMailbox<DirectoryStreamWriter>,
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

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use tantivy::schema::{Schema, STORED, TEXT};
    use tantivy::{doc, Index, IndexSettings};
    use crate::actors::ThreadedExecutor;

    use super::*;

    #[tokio::test]
    async fn test_writer_directory() {
        let _ = tracing_subscriber::fmt::try_init();

        tokio::fs::create_dir_all("./test-data").await.unwrap();
        let actor = DirectoryStreamWriter::create("./test-data/data.index").await.unwrap();
        let mailbox = actor.spawn_actor_with("directory-writer", 100, ThreadedExecutor).await;
        let directory = LinearSegmentWriter {
            writer: mailbox,
            watches: Arc::new(Default::default()),
            atomic_files: Arc::new(RwLock::default()),
        };

        let mut schema_builder = Schema::builder();

        let title = schema_builder.add_text_field("title", TEXT | STORED);
        let body = schema_builder.add_text_field("body", TEXT);

        let schema = schema_builder.build();
        let index = Index::create(
            directory,
            schema,
            IndexSettings::default()
        ).expect("Create index.");

        let mut index_writer = index.writer(50_000_000).expect("Create index writer.");

        index_writer.add_document(doc!(
            title => "Of Mice and Men",
            body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
                    bank and runs deep and green. The water is warm too, for it has slipped twinkling \
                    over the yellow sands in the sunlight before reaching the narrow pool. On one \
                    side of the river the golden foothill slopes curve up to the strong and rocky \
                    Gabilan Mountains, but on the valley side the water is lined with trees—willows \
                    fresh and green with every spring, carrying in their lower leaf junctures the \
                    debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
                    limbs and branches that arch over the pool"
        )).expect("Add document.");

        index_writer.add_document(doc!(
            title => "Frankenstein",
            title => "The Modern Prometheus",
            body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
                     enterprise which you have regarded with such evil forebodings.  I arrived here \
                     yesterday, and my first task is to assure my dear sister of my welfare and \
                     increasing confidence in the success of my undertaking."
        )).expect("Add document.");

        index_writer.commit().expect("Commit documents.");
    }
}
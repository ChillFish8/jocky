use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, WatchCallback, WatchHandle, WritePtr};
use tantivy::Directory;

use crate::directories::IGNORE_FILES;
use crate::metadata::SegmentMetadata;

/// A writer which wraps an inner directory.
///
/// This just tracks the currently live files which need to be
/// exported into a given segment.
pub struct DirectoryWriter<D: Directory> {
    inner: D,
    files_to_read: Arc<Mutex<BTreeSet<PathBuf>>>,
}

impl<D: Directory + Clone> DirectoryWriter<D> {
    /// Create a new directory writer.
    pub(crate) fn new(inner: D) -> Self {
        Self {
            inner,
            files_to_read: Default::default(),
        }
    }

    /// The current set of files to export.
    pub fn files(&self) -> BTreeSet<PathBuf> {
        self.files_to_read.lock().clone()
    }

    /// Writes the contents of the directory to a given writer.
    pub fn write_segment<W: Write>(&self, mut writer: W) -> io::Result<()> {
        let mut cursor = 0;
        let mut metadata = SegmentMetadata::default();

        for file in self.files() {
            let handle = match self.get_file_handle(&file) {
                Ok(handle) => handle,
                Err(OpenReadError::IoError { io_error, .. }) => {
                    return Err(io::Error::new(io_error.kind(), io_error.to_string()))
                },
                _ => unimplemented!()
            };

            let file_start = cursor;
            let bytes = handle.read_bytes(0..handle.len())?;
            writer.write_all(&bytes)?;
            cursor += bytes.len() as u64;

            let fp = file.to_string_lossy().to_string();
            metadata.add_file(fp, file_start..cursor);
        }

        let metadata_start = cursor;
        let bytes = metadata.to_bytes()?;
        writer.write_all(&bytes)?;
        cursor += bytes.len() as u64;

        crate::metadata::write_metadata_offsets(&mut writer, metadata_start, cursor)?;

        writer.flush()?;

        Ok(())
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

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let fp = path.to_string_lossy();
        if !IGNORE_FILES.contains(&fp.as_ref()) {
            self.files_to_read.lock().insert(path.to_path_buf());
        }
        self.inner.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}


#[cfg(test)]
mod tests {
    use tantivy::collector::TopDocs;
    use tantivy::query::QueryParser;
    use tantivy::schema::*;
    use tantivy::{doc, Index, IndexSettings, ReloadPolicy};
    use tantivy::directory::MmapDirectory;

    use super::*;

    #[test]
    fn test_create_segment() {
        let dir = MmapDirectory::create_from_tempdir().unwrap();
        let write = DirectoryWriter::new(dir);

        create_segment(write.clone()).unwrap();

        let mut segment = Vec::new();
        write.write_segment(&mut segment).unwrap();
        assert_eq!(segment.len(), 32)
    }

    fn create_segment(directory: impl Directory) -> tantivy::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT | STORED);
        schema_builder.add_text_field("body", TEXT);

        let schema = schema_builder.build();
        let index = Index::create(directory, schema.clone(), IndexSettings::default())?;
        let mut index_writer = index.writer(50_000_000)?;

        let title = schema.get_field("title").unwrap();
        let body = schema.get_field("body").unwrap();

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
        ))?;

        index_writer.add_document(doc!(
        title => "Frankenstein",
        title => "The Modern Prometheus",
        body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
                 enterprise which you have regarded with such evil forebodings.  I arrived here \
                 yesterday, and my first task is to assure my dear sister of my welfare and \
                 increasing confidence in the success of my undertaking."
        ))?;

        index_writer.commit()?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![title, body]);
        let query = query_parser.parse_query("sea whale")?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            println!("{}", schema.to_json(&retrieved_doc));
        }

        Ok(())
    }
}
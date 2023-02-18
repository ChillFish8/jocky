use std::io;
use std::io::{ErrorKind, Write};

use crate::document::ReferencingDoc;
use crate::schema::BasicSchema;

/// The compressed block size of a set of documents.
///
/// This was determined by run some example datasets and
/// doing blocks of 512 KB, 1 MB, 1.5 MB, out of this the
/// 512 KB blocks were weirdly about the same performance as
/// the 1.5MB blocks with a slightly lower compression ratio.
///
/// The trade off here is that the smaller the block size
/// the quicker it is for us to decompress the data for
/// fetching.
pub const BLOCK_SIZE: usize = 512 << 10;

/// The zstandard compression level.
///
/// This was selected by comparing the various levels for
/// performance vs compression ratio on sample data, and this
/// proved to be the most effective with the given block size.
pub const COMPRESSION_LEVEL: i32 = 1;

#[derive(Default, Debug, Copy, Clone)]
pub struct Stats {
    /// The number of documents processed during the processor's lifetime.
    pub num_docs_processed: usize,
    /// The number of bytes that have been written after compression.
    pub num_compressed_bytes: usize,
    /// The number of bytes have have be produced before compression.
    pub num_uncompressed_bytes: usize,
}

pub struct BlockProcessor<W: Write> {
    writer: W,
    schema: BasicSchema,
    temp_buffer: Vec<u8>,
    stats: Stats,
}

impl<W: Write> BlockProcessor<W> {
    /// Creates a new processor at the given path.
    pub fn new(writer: W, schema: BasicSchema) -> Self {
        Self {
            writer,
            schema,
            temp_buffer: Vec::new(),
            stats: Stats::default(),
        }
    }

    #[inline]
    /// Gets a copy of the live processor statistics.
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Consumes the processor and returns the underlying writer.
    ///
    /// This will drop any temporary buffers and will not be flushed.
    pub fn into_writer(self) -> W {
        self.writer
    }

    /// Writes a document block to the processor.
    pub fn write_docs(&mut self, msg: Vec<ReferencingDoc>) -> io::Result<()> {
        let initial_len = self.temp_buffer.len();
        for doc in msg {
            let values = doc.as_values();
            super::encoding::encode_document_to(
                &mut self.temp_buffer,
                doc.timestamp(),
                self.schema.fields(),
                values.len(),
                values,
                self.schema.hash_key(),
            );

            if let Err(e) = self.check_and_process() {
                self.temp_buffer.truncate(initial_len);
                return Err(e);
            }

            self.stats.num_docs_processed += 1;
        }

        if let Err(e) = self.check_and_process() {
            self.temp_buffer.truncate(initial_len);
            return Err(e);
        }

        Ok(())
    }

    /// Flushes the in-memory buffer to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.drain_and_compress()?;

        let schema = rkyv::to_bytes::<_, 4096>(&self.schema)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        let schema_len = schema.len() as u32;

        self.writer.write_all(&schema)?;
        self.writer.write_all(&schema_len.to_le_bytes())?;

        self.writer.flush()
    }

    /// Checks to see if the temporary buffer has been filled and needs to be
    /// processed.
    fn check_and_process(&mut self) -> io::Result<()> {
        if self.temp_buffer.len() < BLOCK_SIZE {
            return Ok(());
        }

        self.drain_and_compress()
    }

    /// Drains the temporary buffer and compresses the data.
    fn drain_and_compress(&mut self) -> io::Result<()> {
        self.stats.num_uncompressed_bytes += self.temp_buffer.len();

        let compressed = zstd::bulk::compress(&self.temp_buffer, COMPRESSION_LEVEL)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        self.writer.write_all(&compressed)?;
        self.stats.num_compressed_bytes += compressed.len();

        self.temp_buffer.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::File;

    use datacake_crdt::HLCTimestamp;
    use serde_json::json;

    use super::*;
    use crate::schema::FieldInfo;
    use crate::ValueType;

    fn create_processor() -> BlockProcessor<File> {
        let tmp_file = tempfile::tempfile().unwrap();

        let mut fields = BTreeMap::new();
        fields.insert("name".to_string(), 0);
        fields.insert("age".to_string(), 1);

        let field_info = vec![
            FieldInfo::new(ValueType::String, false),
            FieldInfo::new(ValueType::U64, false),
        ];

        let schema = BasicSchema::new(fields, field_info, None);

        BlockProcessor::new(tmp_file, schema)
    }

    #[test]
    fn test_processor_basic() {
        let mut processor = create_processor();

        let doc = json!({
            "name": "bobby",
            "age": 12,
        })
        .to_string();

        let doc = ReferencingDoc::new(doc, HLCTimestamp::now(0, 0)).unwrap();
        processor.write_docs(vec![doc]).expect("Ingest document.");
        processor.flush().expect("Flush document.");

        assert!(processor.temp_buffer.is_empty(), "Buffer should be empty.");
        assert_eq!(
            processor.stats.num_docs_processed, 1,
            "Number of docs processed should match."
        );
    }

    #[test]
    fn test_processor_automatic_flush() {
        let mut processor = create_processor();

        let doc = json!({
            "name": "bobby",
            "age": 12,
        })
        .to_string();

        let safe_approx_num_docs = (BLOCK_SIZE / doc.len()) + 10;

        for _ in 0..safe_approx_num_docs {
            let doc = ReferencingDoc::new(doc.clone(), HLCTimestamp::now(0, 0)).unwrap();
            processor.write_docs(vec![doc]).expect("Ingest document.");
        }

        assert!(
            processor.temp_buffer.len() < BLOCK_SIZE,
            "Temp buffer should have been flushed."
        );
        processor.flush().expect("Flush document.");

        assert!(processor.temp_buffer.is_empty(), "Buffer should be empty.");
        assert_eq!(
            processor.stats.num_docs_processed, safe_approx_num_docs,
            "Number of docs processed should match."
        );
    }
}

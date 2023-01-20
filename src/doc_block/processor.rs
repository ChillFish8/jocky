use std::io;
use std::io::ErrorKind;

use crate::doc_block::writer::FileWriter;
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

pub struct BlockProcessor {
    writer: FileWriter,
    schema: BasicSchema,
    temp_buffer: Vec<u8>,
}

impl BlockProcessor {
    /// Creates a new processor at the given path.
    pub fn new(writer: FileWriter, schema: BasicSchema) -> Self {
        Self { writer, schema, temp_buffer: Vec::new() }
    }

    /// Consumes the processor and returns the underlying writer.
    ///
    /// This will drop any temporary buffers and will not be flushed.
    pub fn into_writer(self) -> FileWriter {
        self.writer
    }

    /// Writes a document block to the processor.
    pub fn write_docs(&mut self, msg: Vec<ReferencingDoc>) -> io::Result<()> {
        for doc in msg.0 {
            let values = doc.as_values();
            super::encoding::encode_document_to(
                &mut self.temp_buffer,
                doc.timestamp(),
                self.schema.fields(),
                values.len(),
                values,
            );

            self.check_and_process()?
        }

        self.check_and_process()?;

        Ok(())
    }

    /// Flushes the in-memory buffer to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.drain_and_compress()?;

        let schema = rkyv::to_bytes::<_, 4096>(&self.schema)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        self.writer.write_raw(&schema)?;
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
        let compressed = zstd::bulk::compress(&self.temp_buffer, COMPRESSION_LEVEL)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        self.writer.write_raw(&compressed)?;

        self.temp_buffer.clear();

        Ok(())
    }
}


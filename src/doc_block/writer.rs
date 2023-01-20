use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct FileWriter {
    writer: BufWriter<File>,
}

impl FileWriter {
    /// Creates a new writer at the given path.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(524 << 10, file),
        })
    }

    /// Consumes the writer and returns the underlying BufWriter.
    pub fn into_buf_writer(self) -> BufWriter<File> {
        self.writer
    }

    /// Writes some raw bytes to the writer.
    pub fn write_raw(&mut self, msg: &[u8]) -> io::Result<()> {
        self.writer.write_all(msg)
    }

    /// Flushes the in-memory buffer to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

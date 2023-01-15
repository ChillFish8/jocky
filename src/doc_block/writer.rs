use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::path::Path;
use puppet::{derive_message, puppet_actor};

use crate::doc_block::Flush;

pub struct BlockWriter {
    writer: BufWriter<File>,
}

#[puppet_actor]
impl BlockWriter {
    /// Creates a new writer at the given path.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(524 << 10, file),
        })
    }

    #[puppet]
    /// Writes a document block to the writer.
    async fn write_block(&mut self, msg: WriteBlock) -> io::Result<()> {
        self.writer.write_all(&msg.0)
    }

    #[puppet]
    /// Writes some raw bytes to the writer.
    async fn write_raw(&mut self, msg: WriteRaw) -> io::Result<()> {
        self.writer.write_all(&msg.0)
    }

    #[puppet]
    /// Flushes the in-memory buffer to disk.
    async fn flush(&mut self, _msg: Flush) -> io::Result<()> {
        self.writer.flush()
    }
}

pub struct WriteBlock(pub Vec<u8>);
derive_message!(WriteBlock, io::Result<()>);

pub struct WriteRaw(pub Vec<u8>);
derive_message!(WriteRaw, io::Result<()>);
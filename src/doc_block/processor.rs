use std::{io, mem};
use std::io::ErrorKind;
use puppet::{ActorMailbox, derive_message, puppet_actor};
use crate::doc_block::Flush;
use crate::doc_block::writer::{BlockWriter, WriteBlock};
use crate::document::RawDocument;

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
pub const COMPRESSION_LEVEL: usize = 1;


pub struct BlockProcessor {
    temp_buffer: Vec<u8>,
    writer: ActorMailbox<BlockWriter>,
}

#[puppet_actor]
impl BlockProcessor {
    pub fn new(writer: ActorMailbox<BlockWriter>) -> Self {
        Self {
            temp_buffer: vec![0; BLOCK_SIZE],
            writer,
        }
    }

    #[puppet]
    async fn compress_entry(&mut self, msg: CompressDocs) -> io::Result<()> {
        let start = self.temp_buffer.len();
        for doc in msg.0 {
            match rkyv::to_bytes::<_, 4096>(&doc) {
                Ok(bytes) => {
                    self.temp_buffer.extend_from_slice(&bytes);
                },
                Err(e) => {
                    self.temp_buffer.truncate(start);
                    return Err(io::Error::new(ErrorKind::Other, e.to_string()));
                }
            };
        }

        if self.temp_buffer.len() < BLOCK_SIZE {
            return Ok(())
        }

        self.writer
            .send(WriteBlock(mem::take(&mut self.temp_buffer)))
            .await?;

        Ok(())
    }

    #[puppet]
    async fn flush(&mut self, msg: Flush) -> io::Result<()> {
        self.writer
            .send(WriteBlock(mem::take(&mut self.temp_buffer)))
            .await?;
        self.writer.send(msg).await
    }
}

pub struct CompressDocs(pub Vec<RawDocument>);
derive_message!(CompressDocs, io::Result<()>);
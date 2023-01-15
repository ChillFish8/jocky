use std::io;
use puppet::derive_message;

mod writer;
mod processor;

pub use writer::{BlockWriter, WriteBlock};
pub use processor::{BlockProcessor, BLOCK_SIZE, COMPRESSION_LEVEL};

pub struct Flush;
derive_message!(Flush, io::Result<()>);
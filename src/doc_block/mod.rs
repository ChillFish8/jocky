use std::io;
use puppet::derive_message;

mod writer;
mod processor;
mod encoding;

pub use writer::{BlockWriter, WriteBlock};
pub use processor::{BLOCK_SIZE, COMPRESSION_LEVEL};
pub use encoding::{ValueType, DocHeader, Field, encode_document_to};

pub struct Flush;
derive_message!(Flush, io::Result<()>);
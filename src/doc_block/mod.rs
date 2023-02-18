mod encoding;
mod processor;

pub use encoding::{
    encode_document_to,
    field_to_value,
    Corrupted,
    DocHeader,
    Field,
    ValueType,
};
pub use processor::{BLOCK_SIZE, COMPRESSION_LEVEL, BlockProcessor};

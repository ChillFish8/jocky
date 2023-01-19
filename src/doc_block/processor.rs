use std::{io, mem};
use std::io::ErrorKind;

use puppet::{ActorMailbox, derive_message, puppet_actor};

use crate::doc_block::Flush;
use crate::doc_block::writer::{BlockWriter, WriteBlock};

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

use std::io;
use std::ops::Range;
use std::path::PathBuf;

use puppet::derive_message;
use tantivy::{DateTime, Document};
use bytecheck::CheckBytes;
use rkyv::{Archive, Serialize, Deserialize};
use tantivy::schema::Facet;

/// Copy a file's content into the segment writer.
pub struct WriteFile {
    pub file_path: PathBuf,
}
derive_message!(WriteFile, io::Result<()>);

/// Writes a buffer into the segment writer.
pub struct WriteBuffer {
    pub file_path: PathBuf,
    pub buffer: Vec<u8>,
    pub overwrite: bool,
}
derive_message!(WriteBuffer, io::Result<()>);

/// Writes a buffer into the segment writer.
pub struct WriteStaticBuffer {
    pub file_path: PathBuf,
    pub buffer: &'static [u8],
    pub overwrite: bool,
}
derive_message!(WriteStaticBuffer, io::Result<()>);

/// Removes a file.
pub struct RemoveFile {
    pub file_path: PathBuf,
}
derive_message!(RemoveFile, io::Result<()>);

/// Checks if a file exists.
pub struct FileExists {
    pub file_path: PathBuf,
}
derive_message!(FileExists, bool);

#[derive(Debug)]
/// Reads a range of data from the file.
pub struct ReadRange {
    pub file_path: PathBuf,
    pub range: Range<usize>,
}
derive_message!(ReadRange, Option<io::Result<Vec<u8>>>);

/// Reads a range of data from the file.
pub struct FileLen {
    pub file_path: PathBuf,
}
derive_message!(FileLen, Option<usize>);

/// Ensures all data is flushed out to disk and writes the metadata file.
pub struct Finalise {
    pub indexes: Vec<String>,
}
derive_message!(Finalise, io::Result<()>);

/// Indexes a set of documents.
pub struct IndexDocuments {
    pub docs: Vec<Document>,
}
derive_message!(IndexDocuments, tantivy::Result<()>);

/// Remove a set of documents from the index.
///
/// This applies to the current index **and** get stored as a set of terms
/// in the segment.
pub struct RemoveDocuments {
    pub terms: Vec<(String, Term)>,
}
derive_message!(RemoveDocuments, tantivy::Result<()>);

pub struct Commit {
    // pub writer: ActorMailbox<SegmentWriter>
}
derive_message!(Commit, tantivy::Result<()>);

pub struct Rollback;
derive_message!(Rollback, tantivy::Result<()>);

pub struct FileSize;
derive_message!(FileSize, u64);

#[repr(C)]
#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub enum Term {
    /// The str type is used for any text information.
    Str(String),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Date/time with second precision
    Date(i64),
    /// Facet
    Facet(String),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
}

impl From<Term> for tantivy::schema::Value {
    fn from(v: Term) -> Self {
        match v {
            Term::Str(v) => Self::Str(v),
            Term::U64(v) => Self::U64(v),
            Term::I64(v) => Self::I64(v),
            Term::F64(v) => Self::F64(v),
            Term::Date(ts) => Self::Date(DateTime::from_timestamp_micros(ts)),
            Term::Facet(v) => Self::Facet(Facet::from_text(&v).unwrap()),
            Term::Bytes(v) => Self::Bytes(v),
        }
    }
}
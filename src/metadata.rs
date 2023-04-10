use std::array::TryFromSliceError;
use std::collections::BTreeMap;
use std::io::{ErrorKind, Write};
use std::ops::Range;
use std::{io, mem};

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

pub const METADATA_HEADER_SIZE: usize = mem::size_of::<u64>() * 2;

#[repr(C)]
#[derive(Debug, Default, Serialize, Deserialize, Archive)]
#[archive_attr(repr(C), derive(CheckBytes, Debug))]
pub struct SegmentMetadata {
    files: BTreeMap<String, Range<u64>>,
    hot_cache: Vec<u8>,
}

impl SegmentMetadata {
    pub fn with_hot_cache(&mut self, buf: Vec<u8>) {
        self.hot_cache = buf;
    }

    pub fn add_file(&mut self, file: String, location: Range<u64>) {
        self.files.insert(file, location);
    }

    pub fn get_location(&self, file: &str) -> Option<Range<u64>> {
        self.files.get(file).cloned()
    }

    pub fn files(&self) -> &BTreeMap<String, Range<u64>> {
        &self.files
    }

    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        rkyv::to_bytes::<_, 4096>(self)
            .map(|buf| buf.into_vec())
            .map_err(|e| {
                io::Error::new(
                    ErrorKind::Other,
                    format!("Could not serialize metadata: {e:?}"),
                )
            })
    }

    pub fn from_buffer(buf: &[u8]) -> io::Result<Self> {
        rkyv::from_bytes(buf).map_err(|e| {
            io::Error::new(
                ErrorKind::Other,
                format!("Could not deserialize metadata: {e:?}"),
            )
        })
    }
}

pub fn get_metadata_offsets(
    mut offset_slice: &[u8],
) -> Result<(u64, u64), TryFromSliceError> {
    let start = read_be_u64(&mut offset_slice)?;
    let len = read_be_u64(&mut offset_slice)?;
    Ok((start, len))
}

pub fn write_metadata_offsets<W: Write>(
    file: &mut W,
    start: u64,
    len: u64,
) -> io::Result<()> {
    file.write_all(&start.to_be_bytes())?;
    file.write_all(&len.to_be_bytes())?;

    Ok(())
}

fn read_be_u64(input: &mut &[u8]) -> Result<u64, TryFromSliceError> {
    let (int_bytes, rest) = input.split_at(mem::size_of::<u64>());
    *input = rest;

    let converted = int_bytes.try_into()?;

    Ok(u64::from_be_bytes(converted))
}

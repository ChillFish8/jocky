use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use itertools::Itertools;
use puppet::puppet_actor;
use tracing::{info, warn};
use crate::actors::messages::{FileExists, FileLen, FileSize, ReadRange, WriteStaticBuffer};

use super::messages::{RemoveFile, WriteBuffer};
use super::exporter::BLOCK_SIZE;

pub const BUFFER_CAPACITY: usize = BLOCK_SIZE * 10;

pub struct DirectoryStreamWriter {
    current_pos: usize,
    file: File,
    writer: Option<BufWriter<File>>,

    /// A mapping of files to their applicable locations.
    ///
    /// A fragment is essentially part of a virtual file located within
    /// the segment itself, virtual files may not be contiguous
    fragments: BTreeMap<PathBuf, Vec<Range<u64>>>,
}

#[puppet_actor]
impl DirectoryStreamWriter {
    pub async fn create(file_path: impl AsRef<Path>) -> io::Result<Self> {
        let path = file_path.as_ref().to_path_buf();
        let (writer, file) = tokio::task::spawn_blocking(move || {
            let writer = File::create(&path)?;
            let reader =  File::open(&path)?;

            Ok::<_, io::Error>((writer, reader))
        }).await.expect("Spawn blocking")?;

        Ok(Self {
            current_pos: 0,
            file,
            writer: Some(BufWriter::with_capacity(BUFFER_CAPACITY, writer)),
            fragments: BTreeMap::new(),
        })
    }

    #[puppet]
    async fn get_file_size(&mut self, _msg: FileSize) -> u64 {
        self.current_pos as u64
    }

    #[puppet]
    async fn file_exists(&mut self, msg: FileExists) -> bool {
        self.fragments.contains_key(&msg.file_path)
    }

    #[puppet]
    async fn file_len(&mut self, msg: FileLen) -> Option<usize> {
        self.fragments
            .get(&msg.file_path)
            .map(|fragments: &Vec<Range<u64>>| {
                fragments
                    .iter()
                    .map(|range| range.end - range.start)
                    .sum::<u64>() as usize
            })
    }

    #[puppet]
    async fn write_fragment(&mut self, msg: WriteStaticBuffer) -> io::Result<()> {
        if msg.overwrite {
            self.fragments.remove(&msg.file_path);
        }

        let start = self.current_pos as u64;
        self.writer_mut()?
            .write_all(msg.buffer)?;
        self.current_pos += msg.buffer.len();
        let end = self.current_pos as u64;

        self.mark_fragment_location(msg.file_path, start..end);

        Ok(())
    }

    #[puppet]
    async fn delete_file(&mut self, msg: RemoveFile) -> io::Result<()> {
        self.fragments.remove(&msg.file_path);
        Ok(())
    }

    #[puppet]
    async fn read_range(&mut self, msg: ReadRange) -> io::Result<Vec<u8>> {
        // Ensure the writer buffer is actually flushed.
        self.writer_mut()?
            .flush()?;

        let fragments = match self.fragments.get(&msg.file_path) {
            None => return Err(io::Error::new(ErrorKind::NotFound, "File not found")),
            Some(fragments) => fragments,
        };

        let fragments_iter = fragments
            .iter()
            .sorted_by_key(|range| range.start);

        let mut num_bytes_to_skip = msg.range.start;
        let mut buffer = Vec::with_capacity(msg.range.len());
        for range in fragments_iter {
            let fragment_len = range.end - range.start;

            if fragment_len < num_bytes_to_skip as u64 {
                num_bytes_to_skip -= fragment_len as usize;
                continue
            }

            // We don't want to read the bytes we dont care about.
            let seek_to = range.start + num_bytes_to_skip as u64;

            // We've skipped all the bytes we need to.
            num_bytes_to_skip = 0;

            self.file.seek(SeekFrom::Start(seek_to))?;

            let len = range.end - seek_to;
            let mut intermediate = vec![0u8; len as usize].into_boxed_slice();
            self.file.read_exact(&mut intermediate[..])?;
            buffer.extend_from_slice(&intermediate);
        }

        buffer.truncate(msg.range.len());
        Ok(buffer)
    }

    pub fn writer_mut(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.writer
            .as_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Writer has already been finalised."))
    }

    pub fn mark_fragment_location(&mut self, path: PathBuf, location: Range<u64>) {
        self.fragments
            .entry(path)
            .or_default()
            .push(location);
    }
}


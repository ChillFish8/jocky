use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::ops::Range;
use std::path::Path;
use datacake_crdt::HLCTimestamp;
use puppet::puppet_actor;

use super::messages::{Finalise, WriteBuffer, WriteFile};
use crate::metadata::SegmentMetadata;

pub const BLOCK_SIZE: usize = 512 << 10;

pub fn make_buffer() -> Box<[u8]> {
    vec![0u8; BLOCK_SIZE].into_boxed_slice()
}

pub struct SegmentWriter {
    current_pos: usize,
    writer: Option<BufWriter<File>>,
    metadata: SegmentMetadata,
}

#[puppet_actor]
impl SegmentWriter {
    pub async fn create(
        file_path: impl AsRef<Path>,
        segment_id: HLCTimestamp,
    ) -> io::Result<Self> {
        let path = file_path.as_ref().to_path_buf();
        let file = tokio::task::spawn_blocking(move || {
            let file = File::create(&path)?;

            // Ensure parent is synced.
            #[cfg(unix)]
            if let Some(parent) = path.parent() {
                File::open(parent)?
                    .sync_data()?;
            }

            Ok::<_, io::Error>(file)
        }).await.expect("Spawn blocking")?;

        Ok(Self {
            current_pos: 0,
            writer: Some(BufWriter::with_capacity(BLOCK_SIZE, file)),
            metadata: SegmentMetadata::new(segment_id),
        })
    }

    #[puppet]
    async fn write_file(&mut self, msg: WriteFile) -> io::Result<()> {
        let start = self.current_pos as u64;
        self.current_pos += copy_file_contents(msg.file_path.as_path(), self.writer_mut()?)?;
        let end = self.current_pos as u64;

        let path = msg.file_path.to_string_lossy().to_string();
        self.mark_file_location(path, start..end);

        Ok(())
    }

    #[puppet]
    async fn write_buffer(&mut self, msg: WriteBuffer) -> io::Result<()> {
        let start = self.current_pos as u64;
        self.writer_mut()?
            .write_all(&msg.buffer)?;
        self.current_pos += msg.buffer.len();
        let end = self.current_pos as u64;

        let path = msg.file_path.to_string_lossy().to_string();
        self.mark_file_location(path, start..end);

        Ok(())
    }

    #[puppet]
    async fn finalise(&mut self, msg: Finalise) -> io::Result<()> {
        self.metadata.indexes.extend(msg.indexes);
        let buffer = self.metadata.to_bytes()?;
        let start = self.current_pos as u64;
        let len = buffer.len() as u64;

        let mut writer = self.writer
            .take()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Writer has already been finalised."))?;

        writer.write_all(&buffer)?;
        crate::metadata::write_metadata_offsets(&mut writer, start, len)?;

        let file = writer
            .into_inner()
            .map_err(|e| e.into_error())?;
        file.sync_data()?;

        Ok(())
    }

    pub fn writer_mut(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.writer
            .as_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Writer has already been finalised."))
    }

    pub fn mark_file_location(&mut self, path: String, location: Range<u64>) {
        self.metadata.files.insert(path, location);
    }
}


fn copy_file_contents(file_path: &Path, writer: &mut BufWriter<File>) -> io::Result<usize> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::with_capacity(BLOCK_SIZE * 2, file);

    let mut total_written = 0;
    let mut buffer = make_buffer();

    loop {
        let n = reader.read(&mut buffer[..])?;

        if n == 0 {
            break
        }
        total_written += n;

        writer.write_all(&buffer[..n])?;
    }

    Ok(total_written)
}
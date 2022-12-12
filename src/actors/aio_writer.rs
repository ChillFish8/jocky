use std::collections::BTreeMap;
use std::{cmp, io};
use std::io::ErrorKind;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, MergedBufferLimit, ReadAmplificationLimit};
use futures_lite::{AsyncWriteExt, StreamExt};
use glommio::Placement;
use itertools::Itertools;
use puppet::{Actor, ActorMailbox, puppet_actor};
use tracing::info;

use crate::actors::messages::{FileExists, FileLen, FileSize, ReadRange, WriteStaticBuffer};
use super::messages::{RemoveFile, WriteBuffer};

pub struct AioDirectoryStreamWriter {
    path: PathBuf,
    file: Option<Rc<DmaFile>>,
    writer: Option<DmaStreamWriter>,

    /// A mapping of files to their applicable locations.
    ///
    /// A fragment is essentially part of a virtual file located within
    /// the segment itself, virtual files may not be contiguous
    fragments: BTreeMap<PathBuf, Vec<Range<u64>>>,
}

#[puppet_actor]
impl AioDirectoryStreamWriter {
    pub fn create(file_path: impl AsRef<Path> + Send + 'static) -> ActorMailbox<Self> {
        let (tx, rx) = flume::bounded::<<Self as Actor>::Messages>(100);

        glommio::LocalExecutorBuilder::new(Placement::Unbound)
            .spin_before_park(Duration::from_millis(10))
            .spawn(|| async move {
                let actor = Self {
                    path: file_path.as_ref().to_path_buf(),
                    file: None,
                    writer: None,
                    fragments: BTreeMap::new(),
                };

                actor.run_actor(rx).await;
            }).unwrap();

        let name = std::borrow::Cow::Owned("AioDirectoryWriter".to_string());
        ActorMailbox::new(tx, name)
    }

    async fn lazy_init(&mut self) -> io::Result<()> {
        let file = DmaFile::create(self.path.as_path()).await?;
        file.pre_allocate(2 << 30).await?;
        let writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(512 << 10)
            .with_write_behind(10)
            .build();
        let file = DmaFile::open(self.path.as_path()).await?;

        self.file = Some(Rc::new(file));
        self.writer = Some(writer);

        Ok(())
    }

    #[puppet]
    async fn get_file_size(&mut self, _msg: FileSize) -> u64 {
        self.writer
            .as_ref()
            .map(|w| w.current_pos())
            .unwrap_or_default()
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

        let writer = self.writer_mut().await?;

        let start = writer.current_pos();
        writer.write_all(msg.buffer).await?;
        let end = writer.current_pos();

        self.mark_fragment_location(msg.file_path, start..end);

        Ok(())
    }

    #[puppet]
    async fn write_fragment_owned(&mut self, msg: WriteBuffer) -> io::Result<()> {
        if msg.overwrite {
            self.fragments.remove(&msg.file_path);
        }

        let writer = self.writer_mut().await?;

        let start = writer.current_pos();
        writer.write_all(&msg.buffer).await?;
        let end = writer.current_pos();

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
        let fragments = match self.fragments.get(&msg.file_path) {
            None => return Err(io::Error::new(ErrorKind::NotFound, "File not found")),
            Some(fragments) => fragments,
        };

        let mut max_selection_area = 0;
        let fragments_iter = fragments
            .iter()
            .map(|range| {
                max_selection_area = cmp::max(max_selection_area, range.end);
                range
            })
            .cloned()
            .sorted_by_key(|range| range.start);

        // Ensure our inflight buffers are not needed.
        let writer = self.writer_mut().await?;
        let flushed_pos = writer.current_flushed_pos();
        if max_selection_area > flushed_pos {
            writer.flush().await?;
        }

        let file = self.file().await?;
        let mut num_bytes_to_skip = msg.range.start;
        let mut buffer = Vec::with_capacity(msg.range.len());

        let mut total_bytes_planned_read = 0;
        let mut selected_fragments = Vec::new();
        for range in fragments_iter {
            if total_bytes_planned_read >= msg.range.len() {
                break;
            }

            let fragment_len = range.end - range.start;

            if fragment_len < num_bytes_to_skip as u64 {
                num_bytes_to_skip -= fragment_len as usize;
                continue
            }

            // We don't want to read the bytes we dont care about.
            let seek_to = range.start + num_bytes_to_skip as u64;

            // We've skipped all the bytes we need to.
            num_bytes_to_skip = 0;

            let len = range.end - seek_to;
            selected_fragments.push((seek_to, len as usize));

            total_bytes_planned_read += len as usize;
        }

        let read_requests = futures_lite::stream::iter(selected_fragments);
        let mut stream = file.read_many(
            read_requests,
            MergedBufferLimit::Custom(512 << 10),
            ReadAmplificationLimit::Custom(4 << 10)
        );

        // let start = Instant::now();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            let ((start, _), res) = result.map_err(|e| {
                io::Error::new(ErrorKind::WouldBlock, "Yeet it was the read many.")
            })?;
            results.push((start, res));
        }
        results.sort_by_key(|v| v.0);
        // info!(range = ?msg.range, "Many read completed! {:?}", start.elapsed());

        for (_, data) in results {
            buffer.extend_from_slice(&data);
        }

        buffer.truncate(msg.range.len());
        Ok(buffer)
    }

    pub async fn writer_mut(&mut self) -> io::Result<&mut DmaStreamWriter> {
        if self.file.is_none() {
            self.lazy_init().await?;
        }

        self.writer
            .as_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Writer has already been finalised."))
    }

    pub async fn file(&mut self) -> io::Result<&Rc<DmaFile>> {
        if self.file.is_none() {
            self.lazy_init().await?;
        }

        self.file
            .as_ref()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Writer has already been finalised."))
    }

    pub fn mark_fragment_location(&mut self, path: PathBuf, location: Range<u64>) {
        self.fragments
            .entry(path)
            .or_default()
            .push(location);
    }
}

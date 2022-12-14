use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::ErrorKind;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;

use futures_lite::{AsyncWriteExt, StreamExt};
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, MergedBufferLimit, ReadAmplificationLimit, ReadResult};
use glommio::Placement;
use humansize::DECIMAL;
use memmap2::Mmap;
use moka::unsync::Cache;
use puppet::{puppet_actor, Actor, ActorMailbox};
use tantivy::directory::OwnedBytes;
use tracing::warn;

use crate::actors::messages::{ExportSegment, FileExists, FileLen, ReadRange, RemoveFile, SegmentSize, WriteBuffer, WriteStaticBuffer};
use crate::fragments::DiskFragments;

#[derive(Default, Debug)]
struct Counters {
    paths: BTreeMap<PathBuf, usize>,
}

impl Counters {
    fn register(&mut self, path: &Path) {
        let val = self.paths
            .entry(path.to_path_buf())
            .or_default();
        (*val) += 1;
    }
}

#[derive(Hash, Eq, PartialEq)]
pub struct CacheKey {
    file_path: PathBuf,
    start: u64,
    len: usize,
}

pub struct AioDirectoryStreamWriter {
    size_hint: u64,
    path: PathBuf,
    file: Option<Rc<DmaFile>>,
    writer: Option<DmaStreamWriter>,
    fragments: DiskFragments,
    counters: Counters,
}

#[puppet_actor]
impl AioDirectoryStreamWriter {
    pub fn create(
        file_path: impl AsRef<Path> + Send + 'static,
        size_hint: u64,
    ) -> ActorMailbox<Self> {
        let (tx, rx) = flume::bounded::<<Self as Actor>::Messages>(100);

        glommio::LocalExecutorBuilder::new(Placement::Unbound)
            .spin_before_park(Duration::from_millis(10))
            .spawn(move || async move {
                let actor = Self {
                    size_hint,
                    path: file_path.as_ref().to_path_buf(),
                    file: None,
                    writer: None,
                    fragments: Default::default(),
                    counters: Counters::default(),
                };

                actor.run_actor(rx).await;
            })
            .unwrap();

        let name = std::borrow::Cow::Owned("AioDirectoryWriter".to_string());
        ActorMailbox::new(tx, name)
    }

    async fn lazy_init(&mut self) -> io::Result<()> {
        let file = DmaFile::create(self.path.as_path()).await?;
        file.hint_extent_size(64 << 20).await?;
        file.pre_allocate(self.size_hint).await?;
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
    async fn get_segment_size(&mut self, _msg: SegmentSize) -> u64 {
        self.writer
            .as_ref()
            .map(|w| w.current_pos())
            .unwrap_or_default()
    }

    #[puppet]
    async fn file_exists(&mut self, msg: FileExists) -> bool {
        self.fragments.exists(&msg.file_path)
    }

    #[puppet]
    async fn file_len(&mut self, msg: FileLen) -> Option<usize> {
        self.fragments.file_size(&msg.file_path)
    }

    #[puppet]
    async fn write_fragment(&mut self, msg: WriteStaticBuffer) -> io::Result<()> {
        let writer = self.writer_mut().await?;

        let start = writer.current_pos();
        writer.write_all(msg.buffer).await?;
        let end = writer.current_pos();

        self.fragments
            .mark_fragment_location(msg.file_path, start..end, msg.overwrite);

        Ok(())
    }

    #[puppet]
    async fn write_fragment_owned(&mut self, msg: WriteBuffer) -> io::Result<()> {
        let writer = self.writer_mut().await?;

        let start = writer.current_pos();
        writer.write_all(&msg.buffer).await?;
        let end = writer.current_pos();

        self.fragments
            .mark_fragment_location(msg.file_path, start..end, msg.overwrite);

        Ok(())
    }

    #[puppet]
    async fn delete_file(&mut self, msg: RemoveFile) -> io::Result<()> {
        self.fragments.clear_fragments(&msg.file_path);
        Ok(())
    }

    #[puppet]
    /// Reads a given range as if was a separate file.
    ///
    /// In the very nature of the writer, reads can be heavily fragmented so naturally this can
    /// lead to a reasonable high amount of random reads, although the reader API will try optimise
    /// it as best as it can.
    async fn read_range(&mut self, msg: ReadRange) -> io::Result<OwnedBytes> {
        self.counters.register(&msg.file_path);

        let mut buffer = Vec::with_capacity(msg.range.len());

        let selected_info = self
            .fragments
            .get_selected_fragments(&msg.file_path, msg.range.clone())?;

        self.ensure_flushed_to(selected_info.minimum_flushed_pos)
            .await?;

        let file = self.file.as_ref().ok_or_else(|| {
            io::Error::new(
                ErrorKind::Other,
                "File has not be initialised, this is a bug.",
            )
        })?;

        let read_requests = futures_lite::stream::iter(selected_info.fragments);
        let mut stream = file.read_many(
            read_requests,
            MergedBufferLimit::Custom(512 << 10),
            ReadAmplificationLimit::Custom(64 << 10),
        );

        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            let ((start, _), res) = result?;
            results.push((start, res));
        }
        results.sort_by_key(|v| v.0);

        for (_, data) in results {
            buffer.extend_from_slice(&data);
        }

        buffer.truncate(msg.range.len());
        Ok(OwnedBytes::new(buffer))
    }

    #[puppet]
    async fn export_segment(&mut self, msg: ExportSegment) -> io::Result<()> {
        // Ensure all data is safely on disk.
        self.writer_mut().await?.sync().await?;

        let total_size = self.fragments.total_size();

        let writer = DmaFile::create(msg.file_path).await?;
        writer.pre_allocate(total_size as u64).await?;
        let mut writer = DmaStreamWriterBuilder::new(writer)
            .with_buffer_size(512 << 10)
            .with_write_behind(10)
            .build();

        let file = self.file.as_ref().ok_or_else(|| {
            io::Error::new(
                ErrorKind::Other,
                "File has not be initialised, this is a bug.",
            )
        })?;

        let locations = self.fragments
            .inner()
            .values()
            .flatten()
            .map(|range| {
                (range.start, (range.end - range.start) as usize)
            })
            .collect::<Vec<_>>();

        let chunk_size = locations.len() / self.fragments.inner().len();
        for block in locations.chunks(chunk_size) {
            let read_requests = futures_lite::stream::iter(block.to_vec());
            let mut stream = file.read_many(
                    read_requests,
                    MergedBufferLimit::Custom(512 << 10),
                    ReadAmplificationLimit::Custom(64 << 10),
                );

            while let Some(res) = stream.next().await {
                let (_, data) = res?;
                writer.write_all(&data).await?;
            }
        }

        writer.sync().await?;

        Ok(())
    }

    async fn writer_mut(&mut self) -> io::Result<&mut DmaStreamWriter> {
        if self.file.is_none() {
            self.lazy_init().await?;
        }

        self.writer.as_mut().ok_or_else(|| {
            io::Error::new(ErrorKind::Other, "Writer has already been finalised.")
        })
    }

    async fn ensure_flushed_to(&mut self, max_selection_area: u64) -> io::Result<()> {
        // Ensure our inflight buffers are not needed.
        let writer = self.writer_mut().await?;
        let flushed_pos = writer.current_flushed_pos();
        if max_selection_area > flushed_pos {
            writer.flush().await?;
        }
        Ok(())
    }
}

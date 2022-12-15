use std::fs::File;
use std::io::{self, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use puppet::{puppet_actor, Actor, ActorMailbox};
use tantivy::directory::OwnedBytes;

use crate::actors::messages::{
    ExportSegment,
    FileExists,
    FileLen,
    ReadRange,
    RemoveFile,
    SegmentSize,
    WriteBuffer,
    WriteStaticBuffer,
};
use crate::fragments::DiskFragments;
use crate::metadata::SegmentMetadata;

const BUFFER_CAPACITY: usize = 512 << 10;

pub struct DirectoryStreamWriter {
    file: File,
    current_pos: usize,
    writer: Option<BufWriter<File>>,
    fragments: DiskFragments,
}

#[puppet_actor]
impl DirectoryStreamWriter {
    pub async fn create(
        file_path: impl AsRef<Path>,
        size_hint: u64,
    ) -> io::Result<ActorMailbox<Self>> {
        let path = file_path.as_ref().to_path_buf();
        let (writer, file) = tokio::task::spawn_blocking(move || {
            let writer = File::create(&path)?;
            writer.set_len(size_hint)?;
            let reader = File::open(&path)?;

            Ok::<_, io::Error>((writer, reader))
        })
        .await
        .expect("Spawn blocking")?;

        let actor = Self {
            file,
            current_pos: 0,
            writer: Some(BufWriter::with_capacity(BUFFER_CAPACITY, writer)),
            fragments: DiskFragments::default(),
        };

        let (tx, rx) = flume::bounded::<<Self as Actor>::Messages>(100);
        std::thread::spawn(move || {
            futures_lite::future::block_on(actor.run_actor(rx));
        });

        let name = std::borrow::Cow::Owned("BlockingDirectoryWriter".to_string());
        Ok(ActorMailbox::new(tx, name))
    }

    #[puppet]
    async fn get_segment_size(&mut self, _msg: SegmentSize) -> u64 {
        self.current_pos as u64
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
        let start = self.current_pos as u64;
        self.writer_mut()?.write_all(msg.buffer)?;
        self.current_pos += msg.buffer.len();
        let end = self.current_pos as u64;

        self.fragments
            .mark_fragment_location(msg.file_path, start..end, msg.overwrite);

        Ok(())
    }

    #[puppet]
    async fn write_fragment_owned(&mut self, msg: WriteBuffer) -> io::Result<()> {
        let start = self.current_pos as u64;
        self.writer_mut()?.write_all(&msg.buffer)?;
        self.current_pos += msg.buffer.len();
        let end = self.current_pos as u64;

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
        // Ensure the writer buffer is actually flushed.
        self.writer_mut()?.flush()?;

        let mut buffer = Vec::with_capacity(msg.range.len());

        let selected_info = self
            .fragments
            .get_selected_fragments(&msg.file_path, msg.range.clone())?;

        for (seek_to, len) in selected_info.fragments {
            self.file.seek(SeekFrom::Start(seek_to))?;

            let mut intermediate = vec![0u8; len as usize].into_boxed_slice();
            self.file.read_exact(&mut intermediate[..])?;
            buffer.extend_from_slice(&intermediate);
        }

        buffer.truncate(msg.range.len());
        Ok(OwnedBytes::new(buffer))
    }

    #[puppet]
    async fn export_segment(&mut self, msg: ExportSegment) -> io::Result<()> {
        // Ensure all data is safely on disk.
        self.writer_mut()?.flush()?;

        let total_size = self.fragments.total_size();

        let file = File::create(msg.file_path)?;
        file.set_len(total_size as u64)?;

        let mut current_pos: u64 = 0;
        let mut writer = BufWriter::with_capacity(BUFFER_CAPACITY, file);

        let mut metadata = SegmentMetadata::default();
        metadata.with_hot_cache(msg.hot_cache);
        for (path, locations) in self.fragments.inner() {
            let start = current_pos;

            let mut locations = locations.clone();
            locations.sort_by_key(|range| range.start);
            for range in locations {
                self.file.seek(SeekFrom::Start(range.start))?;

                let mut temp_buffer =
                    vec![0u8; (range.end - range.start) as usize].into_boxed_slice();
                self.file.read_exact(&mut temp_buffer[..])?;
                writer.write_all(&temp_buffer)?;

                current_pos += temp_buffer.len() as u64;
            }

            let path = path.to_string_lossy().to_string();
            metadata.add_file(path, start..current_pos);
        }

        // Serialize and write metadata.
        let metadata = metadata.to_bytes()?;
        let start = current_pos;
        writer.write_all(&metadata)?;

        // Write metadata footer.
        let mut buf = Vec::new();
        crate::metadata::write_metadata_offsets(
            &mut buf,
            start,
            start + metadata.len() as u64,
        )?;
        writer.write_all(&metadata)?;

        writer
            .into_inner()
            .map_err(|e| e.into_error())?
            .sync_all()?;

        Ok(())
    }

    pub fn writer_mut(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.writer.as_mut().ok_or_else(|| {
            io::Error::new(ErrorKind::Other, "Writer has already been finalised.")
        })
    }
}

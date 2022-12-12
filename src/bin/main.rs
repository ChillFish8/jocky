use std::sync::Arc;
use parking_lot::RwLock;
use tantivy::{doc, Index, IndexSettings};
use tantivy::directory::MmapDirectory;
use tantivy::schema::{FAST, Field, Schema, STORED, TEXT};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::Instant;
use jocky::actors::{AioDirectoryStreamWriter, DirectoryStreamWriter, ThreadedExecutor};
use jocky::directory::LinearSegmentWriter;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Document {
    pub user: String,
    pub item: String,
    pub rating: f64,
    pub timestamp: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    run_stream().await?;

    run_basic().await?;

    Ok(())
}

async fn run_basic() -> anyhow::Result<()> {
    let file = File::open("../../datasets/amazon-reviews/data.json").await?;
    let reader = BufReader::with_capacity(512 << 20, file);

    let mut schema_builder = Schema::builder();

    let user = schema_builder.add_text_field("user", TEXT | STORED);
    let item = schema_builder.add_text_field("item", TEXT | STORED);
    let rating = schema_builder.add_f64_field("rating", FAST | STORED);
    let timestamp = schema_builder.add_i64_field("timestamp", FAST | STORED);

    let schema = schema_builder.build();
    let dir = MmapDirectory::create_from_tempdir()?;
    let index = Index::create(
        dir,
        schema,
        IndexSettings {
            docstore_blocksize: 1 << 20,
            ..Default::default()
        }
    ).expect("Create index.");

    index_data(
        &index,
        reader,
        user,
        item,
        rating,
        timestamp,
    ).await?;
    println!("Basic ^^^");

    Ok(())
}

async fn run_stream() -> anyhow::Result<()> {
    let file = File::open("../../datasets/amazon-reviews/data.json").await?;
    let reader = BufReader::with_capacity(512 << 20, file);

    tokio::fs::create_dir_all("./test-data").await.unwrap();
    let mailbox = AioDirectoryStreamWriter::create("./test-data/data.index");
    // let mailbox = actor.spawn_actor_with("directory-writer", 10, ThreadedExecutor).await;
    let directory = LinearSegmentWriter {
        writer: mailbox,
        watches: Arc::new(Default::default()),
        atomic_files: Arc::new(RwLock::default()),
    };

    let mut schema_builder = Schema::builder();

    let user = schema_builder.add_text_field("user", TEXT | STORED);
    let item = schema_builder.add_text_field("item", TEXT | STORED);
    let rating = schema_builder.add_f64_field("rating", FAST | STORED);
    let timestamp = schema_builder.add_i64_field("timestamp", FAST | STORED);

    let schema = schema_builder.build();
    let index = Index::create(
        directory,
        schema,
        IndexSettings::default()
    ).expect("Create index.");

    index_data(
        &index,
        reader,
        user,
        item,
        rating,
        timestamp,
    ).await?;
    println!("Stream ^^^");

    Ok(())
}

async fn index_data(
    index: &Index,
    reader: BufReader<File>,
    user: Field,
    item: Field,
    rating: Field,
    timestamp: Field,
) -> anyhow::Result<()> {
    let mut index_writer = index.writer(800_000_000).expect("Create index writer.");

    let mut lines = reader.lines();
    let mut parsed_lines = Vec::new();
    while let Some(line) = lines.next_line().await? {
        let doc: Document = serde_json::from_str(&line)?;
        parsed_lines.push(doc);
    }

    let start = Instant::now();
    let total = parsed_lines.len();
    let mut counter = 0;
    for doc in parsed_lines {
        counter += 1;
        if (counter % 1_000_000) == 0 {
            println!("Completed: {}/{}", counter, total);
        }

        index_writer.add_document(doc!(
            user => doc.user,
            item => doc.item,
            rating => doc.rating,
            timestamp => doc.timestamp,
        ))?;
    }

    index_writer.commit().expect("Commit documents.");
    println!("writer took: {:?}", start.elapsed());


    Ok(())
}
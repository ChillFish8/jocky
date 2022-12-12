use std::sync::Arc;
use parking_lot::RwLock;
use tantivy::{Directory, doc, Index, IndexSettings};
use tantivy::directory::MmapDirectory;
use tantivy::schema::{Schema, STORED, TEXT};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::Instant;
use tracing::info;
use jocky::actors::{AioDirectoryStreamWriter, DirectoryStreamWriter, ThreadedExecutor};
use jocky::directory::LinearSegmentWriter;

use std::alloc;
use cap::Cap;
use humansize::DECIMAL;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);


#[derive(serde::Serialize, serde::Deserialize)]
pub struct Document {
    pub id: u64,
    pub event_type: String,
    pub actor_login: String,
    pub repo_name: String,
    pub created_at: i64,
    pub action: String,
    pub number: u64,
    pub title: String,
    pub labels: Vec<String>,
    pub commit_id: String,
    pub body: String,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    info!("Starting stream");
    run_stream().await?;

    info!("Starting basic");
    run_basic().await?;

    Ok(())
}

async fn run_basic() -> anyhow::Result<()> {
    let dir = MmapDirectory::open("./test-data/haha")?;

    index_data(
        dir,
        200_000_000,
    ).await?;
    println!("Basic ^^^");

    Ok(())
}

async fn run_stream() -> anyhow::Result<()> {
    tokio::fs::create_dir_all("./test-data").await.unwrap();
    let mailbox = AioDirectoryStreamWriter::create("./test-data/data.index");
    // let mailbox = actor.spawn_actor_with("directory-writer", 10, ThreadedExecutor).await;
    let directory = LinearSegmentWriter {
        writer: mailbox,
        watches: Arc::new(Default::default()),
        atomic_files: Arc::new(RwLock::default()),
    };

    index_data(
        directory,
        80_000_000,
    ).await?;
    println!("Stream ^^^");

    Ok(())
}

async fn index_data(
    directory: impl Directory,
    buffer: usize,
) -> anyhow::Result<()> {
    let mut schema_builder = Schema::builder();

    let data = schema_builder.add_json_field("data", TEXT | STORED);

    let schema = schema_builder.build();
    let index = Index::create(
        directory,
        schema,
        IndexSettings {
            docstore_blocksize: 1 << 20,
            ..Default::default()
        }
    ).expect("Create index.");

    let mut index_writer = index.writer(buffer).expect("Create index writer.");

    let start = Instant::now();
    let mut usage_averages = vec![];
    let mut counter = 0;
    for path_name in ["hdfs-logs.json", "gh-archive.json"] {
        let file = File::open(format!("/var/lib/datasets/{}", path_name)).await?;
        let reader = BufReader::with_capacity(512 << 20, file);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            let doc: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&line)?;

            counter += 1;
            if (counter % 1_000_000) == 0 {
                usage_averages.push(ALLOCATOR.allocated());
                println!("Completed: {}/unknown", counter);
            }

            index_writer.add_document(doc!(
                data => doc,
            ))?;
        }
    }

    index_writer.commit().expect("Commit documents.");

    let total = usage_averages.iter().sum::<usize>();
    let average = total / usage_averages.len();

    println!(
        "writer took: {:?}, {}, {} docs.",
        start.elapsed(),
        humansize::format_size(average, DECIMAL),
        counter,
    );


    Ok(())
}
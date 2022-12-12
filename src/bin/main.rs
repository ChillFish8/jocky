use std::sync::Arc;
use parking_lot::RwLock;
use tantivy::{doc, Index, IndexSettings};
use tantivy::directory::MmapDirectory;
use tantivy::schema::{FAST, Field, Schema, STORED, TEXT};
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
    #[serde(default = "default_level")]
    pub severity_text: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub resource: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub tenant_id: i64,
    pub timestamp: i64,
}

fn default_level() -> String {
    "INFO".to_string()
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
    let file = File::open("../../datasets/hdfs-logs.json").await?;
    let reader = BufReader::with_capacity(512 << 20, file);

    let mut schema_builder = Schema::builder();

    let severity_text = schema_builder.add_text_field("severity_text", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT | STORED);
    let resource = schema_builder.add_json_field("resource", TEXT | STORED);
    let attributes = schema_builder.add_json_field("attributes", TEXT | STORED);
    let tenant_id = schema_builder.add_i64_field("tenant_id", FAST | STORED);
    let timestamp = schema_builder.add_i64_field("timestamp", FAST | STORED);

    let schema = schema_builder.build();
    let dir = MmapDirectory::open("./test-data/haha")?;
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
        severity_text,
        body,
        resource,
        attributes,
        tenant_id,
        timestamp,
        200_000_000,
    ).await?;
    println!("Basic ^^^");

    Ok(())
}

async fn run_stream() -> anyhow::Result<()> {
    let file = File::open("../../datasets/hdfs-logs.json").await?;
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

    let severity_text = schema_builder.add_text_field("severity_text", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT | STORED);
    let resource = schema_builder.add_json_field("resource", TEXT | STORED);
    let attributes = schema_builder.add_json_field("attributes", TEXT | STORED);
    let tenant_id = schema_builder.add_i64_field("tenant_id", FAST | STORED);
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
        severity_text,
        body,
        resource,
        attributes,
        tenant_id,
        timestamp,
        80_000_000,
    ).await?;
    println!("Stream ^^^");

    Ok(())
}

async fn index_data(
    index: &Index,
    reader: BufReader<File>,
    severity_text: Field,
    body: Field,
    resource: Field,
    attributes: Field,
    tenant_id: Field,
    timestamp: Field,
    buffer: usize,
) -> anyhow::Result<()> {
    let mut index_writer = index.writer(buffer).expect("Create index writer.");

    let mut lines = reader.lines();
    let mut usage_averages = vec![];
    let start = Instant::now();
    let mut counter = 0;
    while let Some(line) = lines.next_line().await? {
        let doc: Document = serde_json::from_str(&line)?;

        counter += 1;
        if (counter % 1_000_000) == 0 {
            usage_averages.push(ALLOCATOR.allocated());
            println!("Completed: {}/unknown", counter);
        }

        index_writer.add_document(doc!(
            severity_text => doc.severity_text.clone(),
            body => doc.body.clone(),
            resource => doc.resource,
            attributes => doc.attributes,
            tenant_id => doc.tenant_id,
            timestamp => doc.timestamp,
        ))?;
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
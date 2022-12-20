use std::alloc;
use std::future::Future;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cap::Cap;
use humantime::format_duration;
use jocky::actors::writers::AutoWriterSelector;
use jocky::directory::LinearSegmentWriter;
use parking_lot::RwLock;
use tantivy::directory::MmapDirectory;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{doc, Directory, Index, IndexSettings};
use tracing::{info, warn};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const NUM_PARTITIONS: usize = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err(){
        std::env::set_var("RUST_LOG", "warn");
    }

    let _ = tracing_subscriber::fmt::try_init();

    for _ in 0..3 {
        std::fs::remove_dir_all("./test-data/")?;
        std::fs::create_dir_all("./test-data/chunky")?;
        std::fs::create_dir_all("./test-data/singles")?;

        //tokio::time::sleep(Duration::from_secs(60)).await;
        //info!("Starting stream");
        //run_stream().await?;

        tokio::time::sleep(Duration::from_secs(60)).await;
        info!("Starting basic");
        run_basic().await?;
    }

    Ok(())
}

async fn run_basic() -> anyhow::Result<()> {
    let dir = move |id| async move {
        let path = format!("./test-data/chunky/{}-partition", id);
        std::fs::create_dir_all(&path).expect("create dir.");
        MmapDirectory::open(path).expect("create mmap dir")
        //RamDirectory::create()
    };

    index_data(dir, 80_000_000).await?;
    println!("Basic ^^^");

    Ok(())
}

async fn run_stream() -> anyhow::Result<()> {
    let dir = move |id: usize| {
        async move {
            let path = format!("./test-data/singles/{}-data.index", id);
            let mailbox = AutoWriterSelector::create(path, 100 << 20)
                .await
                .expect("Create selector");

            LinearSegmentWriter {
                prefix: Path::new("partition").join(id.to_string()),
                writer: mailbox,
                watches: Arc::new(Default::default()),
                atomic_files: Arc::new(RwLock::default()),
            }
        }
    };

    index_data(dir, 10_000_000).await?;
    println!("Stream ^^^");

    Ok(())
}

async fn index_data<D, F>(
    directory: impl Fn(usize) -> F,
    buffer: usize,
) -> anyhow::Result<()>
where
    D: Directory,
    F: Future<Output = D>,
{
    let mut dataset = Vec::new();
    let file = std::fs::File::open("../../datasets/amazon-reviews/data.json")
        .expect("read file");
    let reader = std::io::BufReader::with_capacity(512 << 10, file);
    let lines = reader.lines();
    for (i, line) in lines.enumerate() {
        if i >= 25_000_000 {
            break;
        }

        let doc: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&line.expect("get line")).expect("Parse");
        dataset.push(doc);
    }
    let dataset = Arc::new(dataset);

    let mut schema_builder = Schema::builder();

    let data = schema_builder.add_json_field("data", TEXT | STORED);

    let schema = schema_builder.build();

    let start = Instant::now();
    let mut tasks = vec![];
    for id in 0..NUM_PARTITIONS {
        let dir = directory(id).await;
        let index = Index::create(
            dir,
            schema.clone(),
            IndexSettings {
                docstore_blocksize: 1 << 20,
                ..Default::default()
            },
        )
        .expect("Create index.");

        let mut index_writer = index
            .writer_with_num_threads(1, buffer)
            .expect("Create index writer.");

        let ds = dataset.clone();
        let task = tokio::task::spawn_blocking(move || {
            let mut start = Instant::now();
            for doc in ds.iter().cloned() {
                if start.elapsed() >= Duration::from_secs(30) {
                    index_writer.commit().expect("Commit docs");
                    std::thread::sleep(Duration::from_secs(2));
                    start = Instant::now();
                }

                index_writer
                    .add_document(doc!(
                        data => doc,
                    ))
                    .expect("Add doc.");
            }

            index_writer.commit().expect("Commit documents.");
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.expect("run.");
    }
//
    println!(
        "writer took: {} for {} partitions.",
        format_duration(start.elapsed()),
        NUM_PARTITIONS,
    );

    Ok(())
}

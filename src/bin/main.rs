use std::sync::Arc;
use parking_lot::RwLock;
use tantivy::{doc, Index, IndexSettings};
use tantivy::schema::{Schema, STORED, TEXT};
use jocky::actors::{DirectoryStreamWriter, ThreadedExecutor};
use jocky::directory::LinearSegmentWriter;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    tokio::fs::create_dir_all("./test-data").await.unwrap();
    let actor = DirectoryStreamWriter::create("./test-data/data.index").await.unwrap();
    let mailbox = actor.spawn_actor_with("directory-writer", 100, ThreadedExecutor).await;
    let directory = LinearSegmentWriter {
        writer: mailbox,
        watches: Arc::new(Default::default()),
        atomic_files: Arc::new(RwLock::default()),
    };

    let mut schema_builder = Schema::builder();

    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT);

    let schema = schema_builder.build();
    let index = Index::create(
        directory,
        schema,
        IndexSettings::default()
    ).expect("Create index.");

    let mut index_writer = index.writer(50_000_000).expect("Create index writer.");

    index_writer.add_document(doc!(
        title => "Of Mice and Men",
        body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
                bank and runs deep and green. The water is warm too, for it has slipped twinkling \
                over the yellow sands in the sunlight before reaching the narrow pool. On one \
                side of the river the golden foothill slopes curve up to the strong and rocky \
                Gabilan Mountains, but on the valley side the water is lined with trees—willows \
                fresh and green with every spring, carrying in their lower leaf junctures the \
                debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
                limbs and branches that arch over the pool"
    )).expect("Add document.");

    index_writer.add_document(doc!(
        title => "Frankenstein",
        title => "The Modern Prometheus",
        body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
                 enterprise which you have regarded with such evil forebodings.  I arrived here \
                 yesterday, and my first task is to assure my dear sister of my welfare and \
                 increasing confidence in the success of my undertaking."
    )).expect("Add document.");

    index_writer.commit().expect("Commit documents.");

}
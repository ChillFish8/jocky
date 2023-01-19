use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::mem;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use datacake_crdt::HLCTimestamp;
use tracing::info;
use jocky::{DocValue, encode_document_to};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut writer = BufWriter::with_capacity(512 << 10, File::create("./datasets/data.store")?);
    let reader = BufReader::with_capacity(512 << 10, File::open("./datasets/data.json")?).lines();

    let mut clock = HLCTimestamp::now(0, 0);
    let mut schema = BTreeMap::new();
    schema.insert("user".to_string(), 0);
    schema.insert("item".to_string(), 1);
    schema.insert("rating".to_string(), 2);
    schema.insert("timestamp".to_string(), 3);

    let (tx, rx) = mpsc::sync_channel(4096);
    std::thread::spawn(move || {
        let mut serde_time = Duration::default();
        for line in reader {
            let line = line?;
            let str = unsafe { mem::transmute::<_, &'static str>(line.as_str()) };

            let start = Instant::now();
            let document: BTreeMap<&str, DocValue> = serde_json::from_str(str)?;
            serde_time += start.elapsed();

            let _ = tx.send(StaticDoc {
                _line: line,
                doc: document,
            });
        }

        info!("Serde Took: {:?}", serde_time);
        Ok::<_, anyhow::Error>(())
    });

    let mut buffer = Vec::with_capacity(4096);
    let mut encode_time = Duration::default();
    let mut count = 0;
    while let Ok(document) = rx.recv() {
        let start = Instant::now();
        let ts = clock.send()?;
        encode_document_to(&mut buffer, ts, &schema, document.doc.len(), &document.doc);
        encode_time += start.elapsed();

        writer.write_all(&buffer)?;
        buffer.clear();

        count += 1;

        if (count % 1_000_000) == 0 {
            info!("Total: {}, Avg encode time: {:?}", count, encode_time / count)
        }
    }
    writer.flush()?;

    info!("Encoding Took: {:?}", encode_time);

    Ok(())
}


pub struct StaticDoc {
    _line: String,
    doc: BTreeMap<&'static str, DocValue<'static>>,
}

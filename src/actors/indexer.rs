use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::Path;
use datacake_crdt::HLCTimestamp;
use puppet::puppet_actor;
use tantivy::{DateTime, IndexWriter};
use tantivy::schema::Facet;
use tokio::io;
use crate::actors::messages::Term;

use super::exporter::SegmentWriter;
use super::messages::{IndexDocuments, RemoveDocuments, Commit, Rollback};



pub struct Indexer {
    index_writer: IndexWriter,
    deletes: BTreeMap<String, Vec<Term>>
}

#[puppet_actor]
impl Indexer {
    pub async fn create(index_writer: IndexWriter) -> io::Result<Self> {
        Ok(Self {
            index_writer,
            deletes: BTreeMap::new(),
        })
    }

    #[puppet]
    async fn index_documents(&mut self, msg: IndexDocuments) -> tantivy::Result<()> {
        for doc in msg.docs {
            self.index_writer.add_document(doc)?;
        }
        Ok(())
    }

    #[puppet]
    async fn remove_documents(&mut self, msg: RemoveDocuments) -> tantivy::Result<()> {
        let schema = self.index_writer.index().schema();

        for (field_name, term) in msg.terms {
            let field_id = match schema.get_field(&field_name) {
                None => continue,
                Some(field) => field,
            };

            let tantivy_term = match &term {
                Term::Str(v) => tantivy::Term::from_field_text(field_id, v),
                Term::U64(v) => tantivy::Term::from_field_u64(field_id, *v),
                Term::I64(v) => tantivy::Term::from_field_i64(field_id, *v),
                Term::F64(v) => tantivy::Term::from_field_f64(field_id, *v),
                Term::Date(v) => tantivy::Term::from_field_date(field_id, DateTime::from_timestamp_micros(*v)),
                Term::Facet(v) => {
                    let term = Facet::from_text(v)
                        .map_err(|_| tantivy::error::TantivyError::InvalidArgument(
                            format!("Invalid facet provided {:?}", v)
                        ))?;
                    tantivy::Term::from_facet(
                        field_id,
                        &term,
                    )
                },
                Term::Bytes(v) => tantivy::Term::from_field_bytes(field_id, v),
            };

            self.index_writer.delete_term(tantivy_term);
            self.deletes
                .entry(field_name)
                .or_default()
                .push(term);
        }

        Ok(())
    }

    #[puppet]
    async fn commit(&mut self, msg: Commit) -> tantivy::Result<()> {
        let deletes = rkyv::to_bytes::<_, 4096>(&self.deletes)
            .map_err(|_| io::Error::new(ErrorKind::Other, "Failed to serialize deletes payload."))?;


        Ok(())
    }

    #[puppet]
    async fn rollback(&mut self, _msg: Rollback) -> tantivy::Result<()> {
        self.deletes.clear();
        self.index_writer.rollback()?;
        Ok(())
    }
}
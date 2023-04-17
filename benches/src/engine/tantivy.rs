use crate::utils::io;

use std::time::Instant;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::ReloadPolicy;

pub struct Tantivy {
  schema: Schema,
  index: Index,
}

impl Tantivy {
  pub fn new(index_dir_path: &str, is_stored: bool) -> Tantivy {
    let mut schema_builder = Schema::builder();
    if is_stored {
      schema_builder.add_text_field("message", TEXT | STORED);
    } else {
      schema_builder.add_text_field("message", TEXT);
    }
    let schema = schema_builder.build();
    let index = Index::create_in_dir(&index_dir_path, schema.clone()).unwrap();

    Tantivy { schema, index }
  }

  pub async fn index_lines(&mut self, input_data_path: &str, max_docs: i32) {
    let mut num_docs = 0;
    let mut index_writer = self.index.writer(50_000_000).unwrap();
    let now = Instant::now();
    if let Ok(lines) = io::read_lines(input_data_path) {
      for line in lines {
        num_docs += 1;

        // If max_docs is less than 0, we index all the documents.
        // Otherwise, do not indexing more than max_docs documents.
        if max_docs > 0 && num_docs > max_docs {
          println!(
            "Already indexed {} documents. Not indexing anymore.",
            max_docs
          );
          break;
        }
        if let Ok(message) = line {
          let message_field = self.schema.get_field("message").unwrap();

          let mut doc = Document::default();
          doc.add_text(message_field, message);

          index_writer.add_document(doc);
        }
      }
      index_writer.commit();
    }
    let elapsed = now.elapsed();
    println!("Tantivy time required for insertion: {:.2?}", elapsed);
  }

  // Searches the document against the query and returns the count of of matching document
  pub fn search(self, query: &str) -> usize {
    let reader = self
      .index
      .reader_builder()
      .reload_policy(ReloadPolicy::OnCommit)
      .try_into()
      .unwrap();

    let searcher = reader.searcher();
    let query_parser =
      QueryParser::for_index(&self.index, vec![self.schema.get_field("message").unwrap()]);
    let query = query_parser.parse_query(query).unwrap();
    let now = Instant::now();
    let top_docs = searcher
      .search(&query, &TopDocs::with_limit(100_000))
      .unwrap();
    let elapsed = now.elapsed();
    println!("Tantivy time required for search: {:.2?}", elapsed);
    return top_docs.len();
  }
}

use crate::engine::elasticsearch::ElasticsearchEngine;
use crate::engine::infino::InfinoEngine;
use crate::engine::tantivy::Tantivy;
use crate::utils::io::get_directory_size;

use std::fs::create_dir;
use uuid::Uuid;

mod engine;
mod utils;

const DEFAULT_CONFIG_FILE_NAME: &str = "default.toml";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Path to the input data to index from. Points to a log file - where each line is indexed
  // as a separate document in the elasticsearch index and the infino index.
  let input_data_path = "data/Apache.log";

  // Maximum number of documents to index. Set this to -1 to index all the documents.
  let max_docs = -1;

  // INFINO START
  // Index the data using infino and find the output size.
  let curr_dir = std::env::current_dir().unwrap();

  let config_path = format!("{}/{}", &curr_dir.to_str().unwrap(), "config");

  let mut infino = InfinoEngine::new(&config_path);
  infino.index_lines(input_data_path, max_docs).await;
  let infino_index_size = get_directory_size(infino.get_index_dir_path());
  println!("Infino index size = {} bytes", infino_index_size);

  // Perform search on infino index
  let term_to_search = String::from("Directory");
  let num_docs = infino.search(&term_to_search, 0, u64::MAX);
  println!(
    "Number of documents with term {} are {}",
    term_to_search, num_docs
  );

  // INFINO END

  // TANTIVY START
  // Index the data using tantivy with STORED and find the output size
  let suffix = Uuid::new_v4();
  let tantivy_index_stored_path = format!("/tmp/tantivy-index-stored-{suffix}");
  create_dir(&tantivy_index_stored_path).unwrap();

  let mut tantivy_with_stored = Tantivy::new(&tantivy_index_stored_path, true);
  tantivy_with_stored
    .index_lines(input_data_path, max_docs)
    .await;
  let tantivy_index_stored_size = get_directory_size(&tantivy_index_stored_path);
  println!(
    "Tantivy index size with STORED flag = {} bytes",
    tantivy_index_stored_size
  );

  // Perform search on Tantivy index
  let term_to_search = String::from(r#"message:"Directory""#);
  let num_docs = tantivy_with_stored.search(&term_to_search);
  println!(
    "Number of documents with term {} are {}",
    term_to_search, num_docs
  );

  // TANTIVY END

  // ELASTICSEARCH START
  // Index the data using elasticsearch and find the output size.
  let es = ElasticsearchEngine::new().await;
  es.index_lines(input_data_path, max_docs).await;

  // Force merge the index so that the index size is optimized.
  es.forcemerge().await;

  let output = es.get_index_size().await;
  println!(
    "Elasticsearch index size in the following statement: {}",
    output
  );

  // Perform search on elasticsearch index
  let term_to_search = String::from("Directory");
  let num_docs = es.search(&term_to_search).await;
  println!(
    "Number of documents with term {} are {}",
    term_to_search, num_docs
  );

  // ELASTICSEARCH END

  Ok(())
}

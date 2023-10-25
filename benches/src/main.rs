use crate::engine::clickhouse::ClickhouseEngine;
use crate::engine::es::ElasticsearchEngine;
use crate::engine::infino::InfinoEngine;
use crate::engine::infino_rest::InfinoApiClient;
use crate::engine::tantivy::Tantivy;
use crate::utils::io::get_directory_size;

use std::{
  fs::{self, create_dir},
  thread, time,
};
use timeseries::{infino::InfinoTsClient, prometheus::PrometheusClient};
use uuid::Uuid;

mod engine;
mod timeseries;
mod utils;

static INFINO_SEARCH_QUERIES: &'static [&'static str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];
static CLICKHOUSE_SEARCH_QUERIES: &'static [&'static str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];
static TANTIVY_SEARCH_QUERIES: &'static [&'static str] = &[
  r#"message:"Directory""#,
  r#"message:"Digest: done""#,
  r#"message:"2006] [notice] mod_jk2 Shutting down""#,
  r#"message:"mod_jk child workerEnv in error state 5""#,
  r#"message:"Directory index forbidden""#,
  r#"message:"Jun 09 06:07:05 2005] [notice] LDAP:""#,
  r#"message:"unable to stat""#,
];
static ELASTICSEARCH_SEARCH_QUERIES: &'static [&'static str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Path to the input data to index from. Points to a log file - where each line is indexed
  // as a separate document in the elasticsearch index and the infino index.
  let input_data_path = "data/Apache.log";
  let cell_input_data_size = std::fs::metadata(input_data_path)
    .map(|metadata| metadata.len())
    .expect("Could not get the input data size");

  // Maximum number of documents to index. Set this to -1 to index all the documents.
  let max_docs = -1;

  // INFINO START
  println!("\n\n***Now running Infino via tsldb library***");

  // Index the data using infino and find the output size.
  let curr_dir = std::env::current_dir().unwrap();

  let config_path = format!("{}/{}", &curr_dir.to_str().unwrap(), "config");

  let mut infino = InfinoEngine::new(&config_path);
  let cell_infino_index_time = infino.index_lines(input_data_path, max_docs).await;
  let cell_infino_index_size = get_directory_size(infino.get_index_dir_path());
  println!("Infino index size = {} bytes", cell_infino_index_size);

  // Perform search on infino index
  let cell_infino_search_time = infino.search_multiple_queries(INFINO_SEARCH_QUERIES);

  let _ = fs::remove_dir_all(format! {"{}/index", &curr_dir.to_str().unwrap()});

  // INFINO END

  // INFINO REST START
  println!("\n\n***Now running Infino via REST API client***");

  // Index the data using infino and find the output size.
  let infino_rest = InfinoApiClient::new();
  let cell_infino_rest_index_time = infino_rest.index_lines(input_data_path, max_docs).await;

  // TODO: The flush does not flush to disk reliably - anf it make take more time before the index is updated on disk.
  // Figure out how to flush reliably and the code below can be uncommented.
  // ---
  // Flush the index to disk - sleep for a second to let the OS complete flushing.
  //let _ = infino_rest.flush();
  //thread::sleep(std::time::Duration::from_millis(1000));

  //let cell_infino_rest_index_size = get_directory_size(infino_rest.get_index_dir_path());
  //println!(
  //  "Infino via API index size = {} bytes",
  //  cell_infino_rest_index_size
  //);

  // Perform search on infino index
  let cell_infino_rest_search_time = infino_rest
    .search_multiple_queries(INFINO_SEARCH_QUERIES)
    .await;

  // INFINO REST END

  // CLICKHOUSE START
  println!("\n\n***Now running Clickhouse***");

  let mut clickhouse = ClickhouseEngine::new().await;
  let cell_clickhouse_index_time = clickhouse.index_lines(input_data_path, max_docs).await;

  // The index is not necessarily immediately written by clickhouse, so sleep for some time
  thread::sleep(std::time::Duration::from_millis(5000));
  let cell_clickhouse_index_size = get_directory_size(clickhouse.get_index_dir_path());
  println!(
    "Clickhouse index size = {} bytes",
    cell_clickhouse_index_size
  );

  // Perform search on clickhouse index
  let cell_clickhouse_search_time = clickhouse
    .search_multiple_queries(CLICKHOUSE_SEARCH_QUERIES)
    .await;

  // CLICKHOUSE END

  // TANTIVY START
  println!("\n\n***Now running Tantivy***");

  // Index the data using tantivy with STORED and find the output size
  let suffix = Uuid::new_v4();
  let tantivy_index_stored_path = format!("/tmp/tantivy-index-stored-{suffix}");
  create_dir(&tantivy_index_stored_path).unwrap();

  let mut tantivy_with_stored = Tantivy::new(&tantivy_index_stored_path, true);
  let _cell_tantivy_index_time = tantivy_with_stored
    .index_lines(input_data_path, max_docs)
    .await;
  let cell_tantivy_index_size = get_directory_size(&tantivy_index_stored_path);
  println!(
    "Tantivy index size with STORED flag = {} bytes",
    cell_tantivy_index_size
  );

  // Perform search on Tantivy index
  let _cell_tantivy_search_time =
    tantivy_with_stored.search_multiple_queries(TANTIVY_SEARCH_QUERIES);

  // TANTIVY END

  // ELASTICSEARCH START
  println!("\n\n***Now running Elasticsearch***");

  // Index the data using elasticsearch and find the output size.
  let es = ElasticsearchEngine::new().await;
  let cell_es_index_time = es.index_lines(input_data_path, max_docs).await;

  // Force merge the index so that the index size is optimized.
  es.forcemerge().await;

  let cell_es_index_size = es.get_index_size().await;

  // Perform search on elasticsearch index
  let cell_es_search_time = es
    .search_multiple_queries(ELASTICSEARCH_SEARCH_QUERIES)
    .await;

  // ELASTICSEARCH END

  // Time series related stats
  // INFINO API FOR TIME SERIES START
  println!("\n\n***Now running Infino API client for time series***");

  let infino_ts_client = InfinoTsClient::new();
  // Sleep for 5 seconds to let it collect some data
  thread::sleep(time::Duration::from_millis(10000));
  let mut cell_infino_ts_search_time = 0;
  for _ in 1..10 {
    cell_infino_ts_search_time += infino_ts_client.search().await;
  }
  cell_infino_ts_search_time = cell_infino_ts_search_time / 10;
  println!(
    "Infino timeseries search avg {} microseconds",
    cell_infino_ts_search_time
  );
  // INFINO API FOR TIME SERIES END

  // PROMETHEUS START
  println!("\n\n***Now running Prometheus for time series***");

  let prometheus_client = PrometheusClient::new();

  let append_task = tokio::spawn(async move {
    prometheus_client.append_ts().await;
  });

  // Sleep for 5 seconds to let it collect some data
  thread::sleep(time::Duration::from_millis(10000));
  let mut cell_prometheus_search_time = 0;
  for _ in 1..10 {
    cell_prometheus_search_time += prometheus_client.search().await;
  }
  cell_prometheus_search_time = cell_prometheus_search_time / 10;
  println!(
    "Prometheus timeseries search avg {} microseconds",
    cell_prometheus_search_time
  );
  prometheus_client.stop();

  append_task.abort();

  // PROMETHEUS END

  // Print the output in markdown
  println!("\n\n## Results: ");
  println!(
    "\nRun date: {}",
    chrono::Local::now().format("%Y-%m-%d").to_string()
  );
  println!("\nOperating System: {}", std::env::consts::OS);
  println!("\nMachine description: <Please fill in>");
  println!("\nDataset: {}", input_data_path);
  println!("\nDataset size: {}bytes", cell_input_data_size);
  println!();

  let elasticsearch_index_size;
  if cell_es_index_size == 0 {
    elasticsearch_index_size = "<figure out from cat response>".to_owned();
  } else {
    elasticsearch_index_size = cell_es_index_size.to_string();
  }

  println!("\n\n### Index size\n");
  println!("| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |");
  println!("| ----- | ----- | ----- | ----- | ---- |");
  println!(
    "| {} | {} bytes | {} bytes | {} bytes | Same as infino |",
    input_data_path, elasticsearch_index_size, cell_clickhouse_index_size, cell_infino_index_size,
  );

  println!("\n\n### Indexing latency\n");
  println!("| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |");
  println!("| ----- | ----- | ----- | ----- | ---- |");
  println!(
    "| {} | {} microseconds  | {} microseconds  | {} microseconds  | {} microseconds  |",
    input_data_path,
    cell_es_index_time,
    cell_clickhouse_index_time,
    cell_infino_index_time,
    cell_infino_rest_index_time
  );

  println!("\n\n### Search latency\n");
  println!("Average across different query types. See the detailed output for granular info.\n");
  println!("| dataset | Elasticsearch | Clickhouse | Infino | Infino-Rest |");
  println!("| ----- | ----- | ----- | ---- | ---- |");
  println!(
    "| {} | {} microseconds  | {} microseconds  | {} microseconds  | {} microseconds  |",
    input_data_path,
    cell_es_search_time / ELASTICSEARCH_SEARCH_QUERIES.len() as u128,
    cell_clickhouse_search_time / CLICKHOUSE_SEARCH_QUERIES.len() as u128,
    cell_infino_search_time / INFINO_SEARCH_QUERIES.len() as u128,
    cell_infino_rest_search_time / INFINO_SEARCH_QUERIES.len() as u128
  );

  println!("\n\n### Timeseries search latency");
  println!("\nAverage over 10 queries on time series.\n");
  println!("| Data points | Prometheus | Infino |");
  println!("| ----------- | ---------- | ---------- |");
  println!(
    "| Search Latency | {} microseconds | {} microseconds |\n",
    cell_prometheus_search_time, cell_infino_ts_search_time
  );

  Ok(())
}

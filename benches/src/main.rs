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
  "not exist: /var/www/html/file",
  "[notice] workerEnv.init() ok /etc/httpd/conf/workers2.properties",
  "[client 222.166.160.244] Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "script not found or unable to stat",
];
static CLICKHOUSE_SEARCH_QUERIES: &'static [&'static str] = &[
  "Directory",
  "Digest: done",
  "not exist: /var/www/html/file",
  "[notice] workerEnv.init() ok /etc/httpd/conf/workers2.properties",
  "[client 222.166.160.244] Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "script not found or unable to stat",
];
static TANTIVY_SEARCH_QUERIES: &'static [&'static str] = &[
  r#"message:"Directory""#,
  r#"message:"Digest: done""#,
  r#"message:"not exist: /var/www/html/file""#,
  r#"message:"[notice] workerEnv.init() ok /etc/httpd/conf/workers2.properties""#,
  r#"message:"[client 222.166.160.244] Directory index forbidden""#,
  r#"message:"Jun 09 06:07:05 2005] [notice] LDAP:""#,
  r#"message:"script not found or unable to stat""#,
];
static ELASTICSEARCH_SEARCH_QUERIES: &'static [&'static str] = &[
  "Directory",
  "Digest: done",
  "not exist: /var/www/html/file",
  "[notice] workerEnv.init() ok /etc/httpd/conf/workers2.properties",
  "[client 222.166.160.244] Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "script not found or unable to stat",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Path to the input data to index from. Points to a log file - where each line is indexed
  // as a separate document in the elasticsearch index and the infino index.
  let input_data_path = "data/Apache.log";

  // Maximum number of documents to index. Set this to -1 to index all the documents.
  let max_docs = -1;

  // INFINO START
  println!("\n\n***Now running Infino via tsldb library***");

  // Index the data using infino and find the output size.
  let curr_dir = std::env::current_dir().unwrap();

  let config_path = format!("{}/{}", &curr_dir.to_str().unwrap(), "config");

  let mut infino = InfinoEngine::new(&config_path);
  infino.index_lines(input_data_path, max_docs).await;
  let infino_index_size = get_directory_size(infino.get_index_dir_path());
  println!("Infino index size = {} bytes", infino_index_size);

  // Perform search on infino index
  infino.search_multiple_queries(INFINO_SEARCH_QUERIES);

  let _ = fs::remove_dir_all(format! {"{}/index", &curr_dir.to_str().unwrap()});

  // INFINO END

  // INFINO REST START
  println!("\n\n***Now running Infino via REST API client***");

  // Index the data using infino and find the output size.
  let infino_api = InfinoApiClient::new();
  infino_api.index_lines(input_data_path, max_docs).await;
  let infino_api_index_size = get_directory_size(infino_api.get_index_dir_path());
  println!(
    "Infino via API index size = {} bytes",
    infino_api_index_size
  );

  // Perform search on infino index
  infino_api
    .search_multiple_queries(INFINO_SEARCH_QUERIES)
    .await;

  // INFINO REST END

  // CLICKHOUSE START
  println!("\n\n***Now running Clickhouse***");

  let mut clickhouse = ClickhouseEngine::new().await;
  clickhouse.index_lines(input_data_path, max_docs).await;
  let clickhouse_index_size = get_directory_size(clickhouse.get_index_dir_path());
  println!("Clickhouse index size = {} bytes", clickhouse_index_size);

  // Perform search on clickhouse index
  clickhouse
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
  tantivy_with_stored
    .index_lines(input_data_path, max_docs)
    .await;
  let tantivy_index_stored_size = get_directory_size(&tantivy_index_stored_path);
  println!(
    "Tantivy index size with STORED flag = {} bytes",
    tantivy_index_stored_size
  );

  // Perform search on Tantivy index
  tantivy_with_stored.search_multiple_queries(TANTIVY_SEARCH_QUERIES);

  // TANTIVY END

  // ELASTICSEARCH START
  println!("\n\n***Now running Elasticsearch***");

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
  es.search_multiple_queries(ELASTICSEARCH_SEARCH_QUERIES)
    .await;

  // ELASTICSEARCH END

  // Time series related stats
  // INFINO API FOR TIME SERIES START
  println!("\n\n***Now running Infino API client for time series***");

  let infino_ts_client = InfinoTsClient::new();
  // Sleep for 5 seconds to let it collect some data
  thread::sleep(time::Duration::from_millis(10000));
  let mut sum_nanos = 0;
  for _ in 1..10 {
    sum_nanos += infino_ts_client.search().await;
  }
  println!("Infino timeseries search avg {} nanos", sum_nanos / 10);
  // INFINO API FOR TIME SERIES END

  // PROMETHEUS START
  println!("\n\n***Now running Prometheus for time series***");

  let prometheus_client = PrometheusClient::new();

  let append_task = tokio::spawn(async move {
    prometheus_client.append_ts().await;
  });

  // Sleep for 5 seconds to let it collect some data
  thread::sleep(time::Duration::from_millis(10000));
  let mut sum_nanos = 0;
  for _ in 1..10 {
    sum_nanos += prometheus_client.search().await;
  }
  println!("Prometheus timeseries search avg {} nanos", sum_nanos / 10);
  prometheus_client.stop();

  append_task.abort();

  // PROMETHEUS END

  Ok(())
}

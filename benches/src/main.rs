// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0

use crate::logs::clickhouse::ClickhouseEngine;
use crate::logs::es::ElasticsearchEngine;
use crate::logs::infino::InfinoEngine;
use crate::logs::infino_rest::InfinoApiClient;
use crate::utils::io::get_directory_size;

use metrics::{infino::InfinoMetricsClient, prometheus::PrometheusClient};
use std::{fs, thread, time};
use structopt::StructOpt;

mod logs;
mod metrics;
mod utils;

static INFINO_SEARCH_QUERIES: &[&str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];
static CLICKHOUSE_SEARCH_QUERIES: &[&str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];

static ELASTICSEARCH_SEARCH_QUERIES: &[&str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
  #[structopt(short, long)]
  stop_after_infino: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let opt = Opt::from_args();
  println!("{:#?}", opt.stop_after_infino);

  // Path to the input data to index from. Points to a log file - where each line is indexed
  // as a separate document in the elasticsearch index and the infino index.
  let input_data_path = "data/Apache.log";
  let cell_input_data_size = std::fs::metadata(input_data_path)
    .map(|metadata| metadata.len())
    .expect("Could not get the input data size");

  // Maximum number of documents to index. Set this to -1 to index all the documents.
  let max_docs = -1;

  // INFINO START
  println!("\n\n***Now running Infino via CoreDB library***");

  // Index the data using infino and find the output size.
  let curr_dir = std::env::current_dir().unwrap();

  let config_path = format!("{}/{}", &curr_dir.to_str().unwrap(), "config");

  let mut infino = InfinoEngine::new(&config_path).await;
  let cell_infino_index_time = infino.index_lines(input_data_path, max_docs).await;
  let cell_infino_index_size = get_directory_size(infino.get_index_dir_path());
  println!("Infino index size = {} bytes", cell_infino_index_size);

  // Perform search on infino index
  let cell_infino_search_time = infino.search_multiple_queries(INFINO_SEARCH_QUERIES).await;

  let _ = fs::remove_dir_all(format! {"{}/index", &curr_dir.to_str().unwrap()});

  // INFINO END

  if opt.stop_after_infino {
    println!("stop_after_infino is set. Stopping now...");
    return Ok(());
  }

  // INFINO REST START
  println!("\n\n***Now running Infino via the REST API client***");

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

  // Metrics related stats
  // INFINO API FOR METRICS START
  println!("\n\n***Now running Infino API client for time series***");

  let infino_metrics_client = InfinoMetricsClient::new();
  // Sleep for 5 seconds to let it collect some data
  thread::sleep(time::Duration::from_millis(10000));
  let mut cell_infino_metrics_search_time = 0;
  for _ in 1..10 {
    cell_infino_metrics_search_time += infino_metrics_client.search_metrics().await;
  }
  cell_infino_metrics_search_time /= 10;
  println!(
    "Infino metrics search avg {} microseconds",
    cell_infino_metrics_search_time
  );
  // INFINO API FOR METRICS END

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
    cell_prometheus_search_time += prometheus_client.search_logs().await;
  }
  cell_prometheus_search_time /= 10;
  println!(
    "Prometheus timeseries search avg {} microseconds",
    cell_prometheus_search_time
  );
  prometheus_client.stop();

  append_task.abort();

  // PROMETHEUS END

  // Print the output in markdown
  println!("\n\n## Results: ");
  println!("\nRun date: {}", chrono::Local::now().format("%Y-%m-%d"));
  println!("\nOperating System: {}", std::env::consts::OS);
  println!("\nMachine description: <Please fill in>");
  println!("\nDataset: {}", input_data_path);
  println!("\nDataset size: {}bytes", cell_input_data_size);
  println!();

  let elasticsearch_index_size = if cell_es_index_size == 0 {
    "<figure out from cat response>".to_owned()
  } else {
    cell_es_index_size.to_string()
  };

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

  println!("\n\n### Log Search Latency\n");
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

  println!("\n\n### Metrics Search Latency");
  println!("\nAverage over 10 queries on metrics.\n");
  println!("| Metric points | Prometheus | Infino |");
  println!("| ----------- | ---------- | ---------- |");
  println!(
    "| Search Latency | {} microseconds | {} microseconds |\n",
    cell_prometheus_search_time, cell_infino_metrics_search_time
  );

  Ok(())
}

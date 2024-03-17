// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0
use crate::logs::clickhouse::ClickhouseEngine;
use crate::logs::es::ElasticsearchEngine;
use crate::logs::infino::InfinoEngine;
use crate::logs::infino_rest::InfinoApiClient;
use crate::logs::infino_os_rest::InfinoOSApiClient;
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
  infino: bool,

  #[structopt(short, long)]
  infino_rest: bool,
  
  #[structopt(short, long)]
  infino_bulk: bool,
  
  #[structopt(short, long)]
  infino_os: bool,

  #[structopt(short, long)]
  elastic: bool,

  #[structopt(short, long)]
  clickhouse: bool,

  #[structopt(short, long)]
  infino_metrics: bool,

  #[structopt(short, long)]
  prometheus: bool,

  #[structopt(short, long, default_value = "data/Apache.log")]
  input_file: String,

  #[structopt(short, long)]
  print_markdown: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let opt = Opt::from_args();
  let run_all = !opt.infino &&
                !opt.infino_rest &&
                !opt.infino_bulk &&
                !opt.infino_os &&
                !opt.elastic &&
                !opt.clickhouse &&
                !opt.infino_metrics &&
                !opt.prometheus;

  // Some string variables to progressively collect the test results and print it at the end
  let mut idx_size_title_str = String::from("| dataset |");
  let mut idx_size_dashes_str = String::from("| ----- |");
  let mut idx_size_values_str = String::from(format!("| {} |", &opt.input_file));
  let mut idx_lat_title_str = String::from("| dataset |");
  let mut idx_lat_dashes_str = String::from("| ----- |");
  let mut idx_lat_values_str = String::from(format!("| {} |", &opt.input_file));
  let mut log_search_lat_title_str = String::from("| dataset |");
  let mut log_search_lat_dashes_str = String::from("| ----- |");
  let mut log_search_lat_values_str = String::from(format!("| {} |", &opt.input_file));
  let mut metrics_search_lat_title_str = String::from("| Metric Points |");
  let mut metrics_search_lat_dashes_str = String::from("| ----- |");
  let mut metrics_search_lat_values_str = String::from("| Search Latency |");

  // Path to the input data to index from. Points to a log file - where each line is indexed
  // as a separate document in the elasticsearch index and the infino index.
  let cell_input_data_size = std::fs::metadata(&opt.input_file)
    .map(|metadata| metadata.len())
    .expect("Could not get the input data size");

  // Maximum number of documents to index. Set this to -1 to index all the documents.
  let max_docs = -1;

  if run_all || opt.infino {
    // INFINO START
    println!("\n\n***Now running Infino via CoreDB library***");

    // Index the data using infino and find the output size.
    let curr_dir = std::env::current_dir().unwrap();
    let config_path = format!("{}/{}", &curr_dir.to_str().unwrap(), "config");
    let mut infino = InfinoEngine::new(&config_path).await;
    let cell_infino_index_time = infino.index_lines(&opt.input_file, max_docs).await;
    let cell_infino_index_size = get_directory_size(infino.get_index_dir_path());
    println!("Infino index size = {} bytes", cell_infino_index_size);

    // Perform search on infino index
    let cell_infino_search_time = infino.search_multiple_queries(INFINO_SEARCH_QUERIES).await;
    let _ = fs::remove_dir_all(format! {"{}/index", &curr_dir.to_str().unwrap()});

    idx_size_title_str += " Infino |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += &format!(" {} |", cell_infino_index_size);

    idx_lat_title_str += " Infino |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_infino_index_time);

    log_search_lat_title_str += " Infino |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(" {} |",
                  cell_infino_search_time / INFINO_SEARCH_QUERIES.len() as u128);
    // INFINO END
  }

  if run_all || opt.infino_rest {
    // INFINO REST START
    println!("\n\n***Now running Infino via the REST API client***");

    // Index the data using infino and find the output size.
    let infino_rest = InfinoApiClient::new();
    let cell_infino_rest_index_time = infino_rest.index_lines(&opt.input_file, max_docs, false).await;

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

    idx_size_title_str += " Infino-REST |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += " Not available via API |";

    idx_lat_title_str += " Infino-REST |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_infino_rest_index_time);

    log_search_lat_title_str += " Infino-REST |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(" {} |",
                  cell_infino_rest_search_time / INFINO_SEARCH_QUERIES.len() as u128);
    // INFINO REST END
  }

  if run_all || opt.infino_bulk{
    // INFINO REST BULK START
    println!("\n\n***Now running Infino Bulk via the REST API client***");

    // Index the data using infino and find the output size.
    let infino_bulk = InfinoApiClient::new();
    let cell_infino_bulk_index_time = infino_bulk.index_lines(&opt.input_file, max_docs, true).await;

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
    let cell_infino_bulk_search_time = infino_bulk
      .search_multiple_queries(INFINO_SEARCH_QUERIES)
      .await;

    idx_size_title_str += " Infino-Bulk-REST |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += " Not available via API |";

    idx_lat_title_str += " Infino-Bulk-REST |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_infino_bulk_index_time);

    log_search_lat_title_str += " Infino-REST |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(" {} |",
                  cell_infino_bulk_search_time / INFINO_SEARCH_QUERIES.len() as u128);
    // INFINO REST BULK END
  }

  if run_all || opt.infino_os {
    // INFINO OS REST START
    println!("\n\n***Now running Infino OpenSearch via the REST API client***");

    // Index the data using infino and find the output size.
    let infino_os_rest = InfinoOSApiClient::new();
    let cell_infino_os_rest_index_time = infino_os_rest.index_lines(&opt.input_file, max_docs).await;

    // Perform search on infino index
    let cell_infino_os_rest_search_time = infino_os_rest
      .search_multiple_queries(INFINO_SEARCH_QUERIES)
      .await;

    idx_size_title_str += " Infino-OS-REST |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += " Not available via API |";

    idx_lat_title_str += " Infino-OS-REST |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_infino_os_rest_index_time);

    log_search_lat_title_str += " Infino-OS-REST |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(" {} |",
                  cell_infino_os_rest_search_time / INFINO_SEARCH_QUERIES.len() as u128);
    // INFINO OS REST END
  }

  if run_all || opt.elastic {
    // ELASTICSEARCH START
    println!("\n\n***Now running Elasticsearch***");

    // Index the data using elasticsearch and find the output size.
    let es = ElasticsearchEngine::new().await;
    let cell_es_index_time = es.index_lines(&opt.input_file, max_docs).await;

    // Force merge the index so that the index size is optimized.
    es.forcemerge().await;
    let cell_es_index_size = es.get_index_size().await;

    // Perform search on elasticsearch index
    let cell_es_search_time = es
      .search_multiple_queries(ELASTICSEARCH_SEARCH_QUERIES)
      .await;

    let elasticsearch_index_size = if cell_es_index_size == 0 {
      "<figure out from cat response>".to_owned()
    } else {
      cell_es_index_size.to_string()
    };

    idx_size_title_str += " Elasticsearch |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += &format!(" {} |", elasticsearch_index_size);

    idx_lat_title_str += " Elasticsearch |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_es_index_time);

    log_search_lat_title_str += " Elasticsearch |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(" {} |",
                  cell_es_search_time / ELASTICSEARCH_SEARCH_QUERIES.len() as u128);
    // ELASTICSEARCH END
  }

  if run_all || opt.clickhouse {
    // CLICKHOUSE START
    println!("\n\n***Now running Clickhouse***");

    let mut clickhouse = ClickhouseEngine::new().await;
    let cell_clickhouse_index_time = clickhouse.index_lines(&opt.input_file, max_docs).await;

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

    idx_size_title_str += " Clickhouse |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += &format!(" {} |", cell_clickhouse_index_size);

    idx_lat_title_str += " Clickhouse |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_clickhouse_index_time);

    log_search_lat_title_str += " Clickhouse |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(" {} |",
                  cell_clickhouse_search_time / CLICKHOUSE_SEARCH_QUERIES.len() as u128);
    // CLICKHOUSE END
  }

  // Metrics related stats
  if run_all || opt.infino_metrics {
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

    metrics_search_lat_title_str += " Infino |";
    metrics_search_lat_dashes_str += " ----- |";
    metrics_search_lat_values_str += &format!(" {} |", cell_infino_metrics_search_time);
    // INFINO API FOR METRICS END
  }

  if run_all || opt.prometheus {
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

    metrics_search_lat_title_str += " Prometheus |";
    metrics_search_lat_dashes_str += " ----- |";
    metrics_search_lat_values_str += &format!(" {} |", cell_prometheus_search_time);
    // PROMETHEUS END
  }

  if opt.print_markdown {
    // Print the output in markdown
    println!("\n\n## Results: ");
    println!("\nRun date: {}", chrono::Local::now().format("%Y-%m-%d"));
    println!("\nOperating System: {}", std::env::consts::OS);
    println!("\nMachine description: <Please fill in>");
    println!("\nDataset: {}", &opt.input_file);
    println!("\nDataset size: {}bytes", cell_input_data_size);
    println!();

    if run_all ||
       opt.infino ||
       opt.infino_rest ||
       opt.infino_bulk ||
       opt.infino_os ||
       opt.elastic ||
       opt.clickhouse {
      println!("\n\n### Index size\n");
      println!("{}", idx_size_title_str);
      println!("{}", idx_size_dashes_str);
      println!("{}", idx_size_values_str);

      println!("\n\n### Indexing latency\n");
      println!("{}", idx_lat_title_str);
      println!("{}", idx_lat_dashes_str);
      println!("{}", idx_lat_values_str);

      println!("\n\n### Log Search Latency\n");
      println!("Average across different query types. See the detailed output for granular info.\n");
      println!("{}", log_search_lat_title_str);
      println!("{}", log_search_lat_dashes_str);
      println!("{}", log_search_lat_values_str);
    }

    if run_all || opt.infino_metrics || opt.prometheus {
      println!("\n\n### Metrics Search Latency");
      println!("\nAverage over 10 queries on metrics.\n");
      println!("{}", metrics_search_lat_title_str);
      println!("{}", metrics_search_lat_dashes_str);
      println!("{}", metrics_search_lat_values_str);
    }
  }
  Ok(())
}
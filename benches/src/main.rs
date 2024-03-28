// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0
use crate::logs::clickhouse::ClickhouseEngine;
use crate::logs::es::ElasticsearchEngine;
use crate::logs::infino_os_rest::InfinoOpenSearchEngine;
use crate::logs::infino_rest::InfinoApiClient;
use crate::logs::os::OpenSearchEngine;
use crate::utils::io::get_directory_size;
use metrics::{infino::InfinoMetricsClient, prometheus::PrometheusClient};
use std::collections::HashMap;
use std::process::Command;
use std::{thread, time};
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

static OPENSEARCH_SEARCH_QUERIES: &[&str] = &[
  "Directory",
  "Digest: done",
  "2006] [notice] mod_jk2 Shutting down",
  "mod_jk child workerEnv in error state 5",
  "Directory index forbidden",
  "Jun 09 06:07:05 2005] [notice] LDAP:",
  "unable to stat",
];

static INFINO_OPENSEARCH_SEARCH_QUERIES: &[&str] = &[
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
  infino_os: bool,

  #[structopt(short, long)]
  elastic: bool,

  #[structopt(short, long)]
  opensearch: bool,

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

struct SoftwareComponent {
  name: &'static str,
  docker_image: &'static str,
  container_name: &'static str,
  ports: &'static str,
  environment_variables: Option<Vec<&'static str>>,
  bind_mounts: Option<Vec<&'static str>>,
}

fn setup_docker_container(component: &SoftwareComponent) -> Result<(), Box<dyn std::error::Error>> {
  println!("Removing existing {} docker container....", component.name);

  // Remove the existing container if it exists
  Command::new("docker")
    .arg("rm")
    .arg("-f")
    .arg(&component.container_name)
    .output()
    .ok(); // Ignore any errors if the container doesn't exist

  println!("Pulling {} docker image....", component.name);

  Command::new("docker")
    .arg("pull")
    .arg(&component.docker_image)
    .output()
    .expect("Failed to pull Docker image");

  println!("Running {} docker image....", component.name);

  let mut cmd = Command::new("docker");
  cmd
    .arg("run")
    .arg("-d")
    .arg("--name")
    .arg(&component.container_name)
    .arg("-p")
    .arg(&component.ports);

  if let Some(env_vars) = &component.environment_variables {
    for var in env_vars {
      cmd.arg(var);
    }
  }

  if let Some(bind_vars) = &component.bind_mounts {
    for var in bind_vars {
      cmd.arg(var);
    }
  }

  cmd
    .arg(&component.docker_image)
    .output()
    .expect("Failed to run Docker container");

  println!("{} setup complete.", component.name);

  Ok(())
}

fn teardown_docker_container(
  component: &SoftwareComponent,
) -> Result<(), Box<dyn std::error::Error>> {
  println!("Starting {} teardown....", component.name);

  Command::new("docker")
    .arg("rm")
    .arg("-f")
    .arg(&component.container_name)
    .output()
    .expect("Failed to remove Docker container");

  println!("{} teardown complete.", component.name);

  Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let opt = Opt::from_args();
  let run_all = !opt.infino_rest
    && !opt.infino_os
    && !opt.opensearch
    && !opt.elastic
    && !opt.clickhouse
    && !opt.infino_metrics
    && !opt.prometheus;

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

  // Note OS needs a strong password to be set for admin so we use a dummy password
  let mut software_components: HashMap<&str, SoftwareComponent> = HashMap::new();
  software_components.insert(
    "infino",
    SoftwareComponent {
      name: "Infino",
      docker_image: "infinohq/infino:latest",
      container_name: "infino_test",
      ports: "3000:3000",
      environment_variables: None,
      bind_mounts: Some(vec![
        "-v",
        "./build/infino_test/logs:/usr/share/infino/logs",
        "-v",
        "./build/infino_test/data:/opt/infino/data",
      ]),
    },
  );
  software_components.insert(
    "infino_os",
    SoftwareComponent {
      name: "Infino OpenSearch",
      docker_image: "infinohq/infino-opensearch:latest",
      container_name: "infino_opensearch_test",
      ports: "9200:9200,9300:9300,9600:9600,9650:9650",
      environment_variables: Some(vec![
        "-e",
        "discovery.type=single-node",
        "-e",
        "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Tr0ub4dor&3$",
      ]),
      bind_mounts: Some(vec![
        "-v",
        "./build/infino_opensearch_test/logs:/usr/share/opensearch/logs",
      ]),
    },
  );
  software_components.insert(
    "opensearch",
    SoftwareComponent {
      name: "OpenSearch",
      docker_image: "opensearchproject/opensearch:2.11.0",
      container_name: "opensearch_test",
      ports: "9200:9200,9300:9300,9600:9600,9650:9650",
      environment_variables: Some(vec![
        "-e",
        "discovery.type=single-node",
        "-e",
        "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Tr0ub4dor&3$",
      ]),
      bind_mounts: Some(vec![
        "-v",
        "./build/opensearch_test/logs:/usr/share/opensearch/logs",
      ]),
    },
  );
  software_components.insert(
    "elasticsearch",
    SoftwareComponent {
      name: "Elasticsearch",
      docker_image: "elasticsearch:8.13.0",
      container_name: "elasticsearch_test",
      ports: "9200:9200,9300:9300,9600:9600,9650:9650",
      environment_variables: Some(vec!["-e", "discovery.type=single-node"]),
      bind_mounts: Some(vec![
        "-v",
        "./build/elasticsearch_test/logs:/usr/share/elasticsearch/logs",
      ]),
    },
  );

  software_components.insert(
    "prometheus",
    SoftwareComponent {
      name: "Prometheus",
      docker_image: "prom/prometheus:latest",
      container_name: "prometheus_test",
      ports: "9090:9090",
      environment_variables: None,
      bind_mounts: None,
    },
  );
  software_components.insert(
    "clickhouse",
    SoftwareComponent {
      name: "Clickhouse",
      docker_image: "clickhouse/clickhouse-server:latest",
      container_name: "clickhouse_test",
      ports: "8123:8123",
      environment_variables: None,
      bind_mounts: None,
    },
  );

  if run_all || opt.infino_rest {
    // INFINO REST START
    println!("\n\n***Now running Infino via the REST API client***");

    setup_docker_container(software_components.get("infino").unwrap())?;

    // Index the data and find the output size.
    let infino_rest = InfinoApiClient::new();
    let cell_infino_rest_index_time = infino_rest
      .index_lines(&opt.input_file, max_docs, false)
      .await;

    // TODO: The flush does not flush to disk reliably - anf it make take more time before the index is updated on disk.
    // Figure out how to flush reliably and the code below can be uncommented.
    // ---
    // Flush the index to disk - sleep for a second to let the OS complete flushing.
    let _ = infino_rest.flush();
    thread::sleep(std::time::Duration::from_millis(1000));
    let cell_infino_rest_index_size = get_directory_size(infino_rest.get_index_dir_path());
    println!("Infino index size = {} bytes", cell_infino_rest_index_size);

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
    log_search_lat_values_str += &format!(
      " {} |",
      cell_infino_rest_search_time / INFINO_SEARCH_QUERIES.len() as u128
    );

    teardown_docker_container(software_components.get("infino").unwrap())?;

    // INFINO REST END
  }

  if run_all || opt.infino_os {
    // INFINP OPENSEARCH START
    println!("\n\n***Now running Infino OpenSearch via the OpenSearch REST API client***");

    setup_docker_container(software_components.get("infino_os").unwrap())?;

    // Index the data and find the output size.
    let infino_os = InfinoOpenSearchEngine::new().await;
    let cell_infino_os_index_time = infino_os.index_lines(&opt.input_file, max_docs).await;

    // Force merge the index so that the index size is optimized.
    infino_os.forcemerge().await;
    let cell_infino_os_index_size = infino_os.get_index_size().await;

    // Perform search on opensearch index
    let cell_infino_os_search_time = infino_os
      .search_multiple_queries(OPENSEARCH_SEARCH_QUERIES)
      .await;

    let infino_os_index_size = if cell_infino_os_index_size == 0 {
      "<figure out from cat response>".to_owned()
    } else {
      cell_infino_os_index_size.to_string()
    };

    idx_size_title_str += " Infino OpenSearch |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += &format!(" {} |", infino_os_index_size);

    idx_lat_title_str += " Infino OpenSearch |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_infino_os_index_time);

    log_search_lat_title_str += " Infino OpenSearch |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(
      " {} |",
      cell_infino_os_search_time / INFINO_OPENSEARCH_SEARCH_QUERIES.len() as u128
    );

    teardown_docker_container(software_components.get("infino_os").unwrap())?;

    // OPENSEARCH END
  }

  if run_all || opt.elastic {
    // ELASTICSEARCH START
    println!("\n\n***Now running Elasticsearch via the Elasticsearch REST API client***");

    setup_docker_container(software_components.get("elastic").unwrap())?;

    // Index the data and find the output size.
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
    log_search_lat_values_str += &format!(
      " {} |",
      cell_es_search_time / ELASTICSEARCH_SEARCH_QUERIES.len() as u128
    );

    teardown_docker_container(software_components.get("elastic").unwrap())?;

    // ELASTICSEARCH END
  }

  if run_all || opt.opensearch {
    // OPENSEARCH START
    println!("\n\n***Now running OpenSearch via the OpenSearch REST API client***");

    // setup_docker_container(software_components.get("opensearch").unwrap())?;

    // Index the data using opensearch and find the output size.
    let os = OpenSearchEngine::new().await;
    let cell_os_index_time = os.index_lines(&opt.input_file, max_docs).await;

    // Force merge the index so that the index size is optimized.
    os.forcemerge().await;
    let cell_os_index_size = os.get_index_size().await;

    // Perform search on opensearch index
    let cell_os_search_time = os.search_multiple_queries(OPENSEARCH_SEARCH_QUERIES).await;

    let opensearch_index_size = if cell_os_index_size == 0 {
      "<figure out from cat response>".to_owned()
    } else {
      cell_os_index_size.to_string()
    };

    idx_size_title_str += " OpenSearch |";
    idx_size_dashes_str += " ----- |";
    idx_size_values_str += &format!(" {} |", opensearch_index_size);

    idx_lat_title_str += " OpenSearch |";
    idx_lat_dashes_str += " ----- |";
    idx_lat_values_str += &format!(" {} |", cell_os_index_time);

    log_search_lat_title_str += " OpenSearch |";
    log_search_lat_dashes_str += " ----- |";
    log_search_lat_values_str += &format!(
      " {} |",
      cell_os_search_time / OPENSEARCH_SEARCH_QUERIES.len() as u128
    );

    // teardown_docker_container(software_components.get("opensearch").unwrap())?;

    // OPENSEARCH END
  }

  if run_all || opt.clickhouse {
    // CLICKHOUSE START
    println!("\n\n***Now running Clickhouse***");

    setup_docker_container(software_components.get("clickhouse").unwrap())?;

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
    log_search_lat_values_str += &format!(
      " {} |",
      cell_clickhouse_search_time / CLICKHOUSE_SEARCH_QUERIES.len() as u128
    );

    teardown_docker_container(software_components.get("clickhouse").unwrap())?;

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
    cell_infino_metrics_search_time /= 10000;
    println!(
      "Infino metrics search avg {} ms",
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

    setup_docker_container(software_components.get("prometheus").unwrap())?;

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
    cell_prometheus_search_time /= 10000;
    println!(
      "Prometheus timeseries search avg {} ms",
      cell_prometheus_search_time
    );

    prometheus_client.stop();
    append_task.abort();

    metrics_search_lat_title_str += " Prometheus |";
    metrics_search_lat_dashes_str += " ----- |";
    metrics_search_lat_values_str += &format!(" {} |", cell_prometheus_search_time);

    teardown_docker_container(software_components.get("prometheus").unwrap())?;

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

    if run_all
      || opt.infino
      || opt.infino_rest
      || opt.opensearch
      || opt.infino_os
      || opt.elastic
      || opt.clickhouse
      || opt.prometheus
      || opt.infino_metrics
    {
      println!("\n\n### Index size\n");
      println!("{}", idx_size_title_str);
      println!("{}", idx_size_dashes_str);
      println!("{}", idx_size_values_str);

      println!("\n\n### Indexing latency\n");
      println!("{}", idx_lat_title_str);
      println!("{}", idx_lat_dashes_str);
      println!("{}", idx_lat_values_str);

      println!("\n\n### Log Search Latency\n");
      println!(
        "Average across different query types. See the detailed output for granular info.\n"
      );
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

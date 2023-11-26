//! The Infino server application and interface.
//!
//! The Infino server is an [Axum](https://docs.rs/axum/latest/axum/) web application that handles all API
//! requests to Infino.
//!
//! See an Infino architecture overview [here](https://github.com/infinohq/infino).
//! Infino has data ingestion APIs for storing data in Infino and query APIs
//! for retrieving data form Infino. Ingested data is persisted in a queue and forwarded to the 
//! CoreDB database that stores and retrieves telemetry data in Infino.
//!
//! We also summarize logs using Generative AI models; we are currently using [OpenAI](https://platform.openai.com/)
//! but we are evaulating alternatives like [Llama2](https://github.com/facebookresearch/llama) and our own homegrown
//! models. More to come.

mod queue_manager;
mod utils;

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Query};
use axum::routing::{delete, put};
use axum::{extract::State, routing::get, routing::post, Json, Router};
use chrono::Utc;
use hyper::StatusCode;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use coredb::log::log_message::LogMessage;
use coredb::CoreDB;
use utils::error::InfinoError;

use crate::queue_manager::queue::RabbitMQ;
use crate::utils::openai_helper::OpenAIHelper;
use crate::utils::settings::Settings;
use crate::utils::shutdown::shutdown_signal;

/// Represents application state.
struct AppState {
  // The queue will be created only if use_rabbitmq = yes is specified in server config.
  queue: Option<RabbitMQ>,
  coredb: CoreDB,
  settings: Settings,
  openai_helper: OpenAIHelper,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents a logs query.
struct LogsQuery {
  text: String,
  start_time: Option<u64>,
  end_time: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents a metrics query.
struct MetricsQuery {
  label_name: String,
  label_value: String,
  start_time: Option<u64>,
  end_time: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents summarization query. 'k' is the number of results to summarize.
struct SummarizeQuery {
  text: String,
  k: Option<u32>,
  start_time: Option<u64>,
  end_time: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents summarization query. 'k' is the number of results to summarize.
struct SummarizeQueryResponse {
  summary: String,
  results: Vec<LogMessage>,
}

/// Periodically commits coredb (typically called in a thread, so that coredb can be asyncronously committed).
async fn commit_in_loop(
  state: Arc<AppState>,
  commit_interval_in_seconds: u32,
  shutdown_flag: Arc<Mutex<bool>>,
) {
  loop {
    state.coredb.commit(true);

    if *shutdown_flag.lock().await {
      info!("Received shutdown in commit thread. Exiting...");
      break;
    }

    sleep(Duration::from_secs(commit_interval_in_seconds as u64)).await;
  }
}

/// Axum application for Infino server.
async fn app(
  config_dir_path: &str,
  image_name: &str,
  image_tag: &str,
) -> (Router, JoinHandle<()>, Arc<Mutex<bool>>, Arc<AppState>) {
  // Read the settings from the config directory.
  let settings = Settings::new(config_dir_path).unwrap();

  // Create a new coredb.
  let coredb = match CoreDB::new(config_dir_path) {
    Ok(coredb) => coredb,
    Err(err) => panic!("Unable to initialize coredb with err {}", err),
  };

  // Create RabbitMQ to store incoming requests.
  let rabbitmq_settings = settings.get_rabbitmq_settings();
  let container_name = rabbitmq_settings.get_container_name();
  let listen_port = rabbitmq_settings.get_listen_port();
  let stream_port = rabbitmq_settings.get_stream_port();

  let use_rabbitmq = settings.get_server_settings().get_use_rabbitmq();
  let mut queue = None;
  if use_rabbitmq {
    queue = Some(
      RabbitMQ::new(
        container_name,
        image_name,
        image_tag,
        listen_port,
        stream_port,
      )
      .await,
    );
  }

  let openai_helper = OpenAIHelper::new();

  let shared_state = Arc::new(AppState {
    queue,
    coredb,
    settings,
    openai_helper,
  });

  let server_settings = shared_state.settings.get_server_settings();
  let commit_interval_in_seconds = server_settings.get_commit_interval_in_seconds();

  // Start a thread to periodically commit coredb.
  info!("Spawning new thread to periodically commit");
  let commit_thread_shutdown_flag = Arc::new(Mutex::new(false));
  let commit_thread_handle = tokio::spawn(commit_in_loop(
    shared_state.clone(),
    commit_interval_in_seconds,
    commit_thread_shutdown_flag.clone(),
  ));

  // Build our application with a route
  let router: Router = Router::new()
    // GET methods
    .route("/get_index_dir", get(get_index_dir))
    .route("/ping", get(ping))
    .route("/search_logs", get(search_logs))
    .route("/search_metrics", get(search_metrics))
    .route("/summarize", get(summarize))
    //---
    // POST methods
    // TODO: all the APIs should have index name
    .route("/append_log", post(append_log))
    .route("/append_metric", post(append_metric))
    .route("/flush", post(flush))
    // POST methods to create and delete index
    .route("/:index_name", put(create_index))
    .route("/:index_name", delete(delete_index))
    .with_state(shared_state.clone())
    .layer(TraceLayer::new_for_http());

  (
    router,
    commit_thread_handle,
    commit_thread_shutdown_flag,
    shared_state,
  )
}

#[tokio::main]
/// Program entry point.
async fn main() {
  // If log level isn't set, set it to info.
  if env::var("RUST_LOG").is_err() {
    env::set_var("RUST_LOG", "info")
  }

  // Set up logging.
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();

  let config_dir_path = "config";
  let image_name = "rabbitmq";
  let image_tag = "3";

  // Create app.
  let (app, commit_thread_handle, commit_thread_shutdown_flag, shared_state) =
    app(config_dir_path, image_name, image_tag).await;

  // Start server.
  let port = shared_state.settings.get_server_settings().get_port();
  let addr = SocketAddr::from(([0, 0, 0, 0], port));

  info!(
    "Infino server listening on {}. Use Ctrl-C or SIGTERM to gracefully exit...",
    addr
  );
  axum::Server::bind(&addr)
    .serve(app.into_make_service())
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();

  if shared_state.queue.is_some() {
    info!("Closing RabbitMQ connection...");
    let queue = shared_state.queue.as_ref().unwrap();
    queue.close_connection().await;

    info!("Stopping RabbitMQ container...");
    let rabbitmq_container_name = queue.get_container_name();
    RabbitMQ::stop_queue_container(rabbitmq_container_name)
      .expect("Could not stop rabbitmq container");
  }

  info!("Shutting down commit thread and waiting for it to finish...");
  *commit_thread_shutdown_flag.lock().await = true;
  commit_thread_handle
    .await
    .expect("Error while completing the commit thread");

  info!("Completed Infino server shuwdown");
}

/// Helper function to parse json input.
fn parse_json(value: &serde_json::Value) -> Result<Vec<Map<String, Value>>, InfinoError> {
  let mut json_objects: Vec<Map<String, Value>> = Vec::new();
  if value.is_object() {
    json_objects.push(value.as_object().unwrap().clone());
  } else if value.is_array() {
    let value_array = value.as_array().unwrap();
    for v in value_array {
      json_objects.push(v.as_object().unwrap().clone());
    }
  } else {
    let msg = format!("Invalid entry {}", value);
    error!("{}", msg);
    return Err(InfinoError::InvalidInput(msg));
  }

  Ok(json_objects)
}

/// Helper function to get timestamp value from given json object.
fn get_timestamp(value: &Map<String, Value>, timestamp_key: &str) -> Result<u64, InfinoError> {
  let result = value.get(timestamp_key);
  let timestamp: u64;
  match result {
    Some(v) => {
      if v.is_f64() {
        timestamp = v.as_f64().unwrap() as u64;
      } else if v.is_u64() {
        timestamp = v.as_u64().unwrap();
      } else {
        let msg = format!("Invalid timestamp {} in json {:?}", v, value);
        return Err(InfinoError::InvalidInput(msg));
      }
    }
    None => {
      let msg = format!("Could not find timestamp in json {:?}", value);
      return Err(InfinoError::InvalidInput(msg));
    }
  }

  Ok(timestamp)
}

/// Append log data to CoreDB.
async fn append_log(
  State(state): State<Arc<AppState>>,
  Json(log_json): Json<serde_json::Value>,
) -> Result<(), (StatusCode, String)> {
  debug!("Appending log entry {}", log_json);

  let is_queue = state.queue.is_some();

  let result = parse_json(&log_json);
  if result.is_err() {
    let msg = format!("Invalid log entry {}", log_json);
    error!("{}", msg);
    return Err((StatusCode::BAD_REQUEST, msg));
  }
  let log_json_objects = result.unwrap();

  let server_settings = state.settings.get_server_settings();
  let timestamp_key = server_settings.get_timestamp_key();

  for obj in log_json_objects {
    let obj_string = serde_json::to_string(&obj).unwrap();
    if is_queue {
      state
        .queue
        .as_ref()
        .unwrap()
        .publish(&obj_string)
        .await
        .unwrap();
    }

    let result = get_timestamp(&obj, timestamp_key);
    if result.is_err() {
      error!("Timestamp error, ignoring entry {:?}", obj);
      continue;
    }
    let timestamp = result.unwrap();

    let mut fields: HashMap<String, String> = HashMap::new();
    let mut text = String::new();
    let count = obj.len();
    for (i, (key, value)) in obj.iter().enumerate() {
      if key != timestamp_key && value.is_string() {
        let value_str = value.as_str().unwrap();
        fields.insert(key.to_owned(), value_str.to_owned());
        text.push_str(value_str);

        if i != count - 1 {
          // Seperate different field entries in text by space, so that they can be tokenized.
          text.push(' ');
        }
      }
    }

    state.coredb.append_log_message(timestamp, &fields, &text);
  }

  Ok(())
}

/// Deprecated function for backwards-compatibility. Wraps append_metric().
// TODO: Remove this function by Jan 2024.
#[allow(dead_code)]
#[deprecated(note = "Use append_metric instead")]
async fn append_ts(
  State(state): State<Arc<AppState>>,
  Json(ts_json): Json<serde_json::Value>,
) -> Result<(), (StatusCode, String)> {
  append_metric(axum::extract::State(state), axum::Json(ts_json)).await
}

/// Append metric data to CoreDB.
async fn append_metric(
  State(state): State<Arc<AppState>>,
  Json(ts_json): Json<serde_json::Value>,
) -> Result<(), (StatusCode, String)> {
  debug!("Appending metric entry: {:?}", ts_json);

  let is_queue = state.queue.is_some();

  let result = parse_json(&ts_json);
  if result.is_err() {
    let msg = format!("Invalid time series entry {}", ts_json);
    error!("{}", msg);
    return Err((StatusCode::BAD_REQUEST, msg));
  }
  let ts_objects = result.unwrap();

  let server_settings = state.settings.get_server_settings();
  let timestamp_key: &str = server_settings.get_timestamp_key();
  let labels_key: &str = server_settings.get_labels_key();

  for obj in ts_objects {
    let obj_string = serde_json::to_string(&obj).unwrap();

    if is_queue {
      state
        .queue
        .as_ref()
        .unwrap()
        .publish(&obj_string)
        .await
        .unwrap();
    }

    // Retrieve the timestamp for this time series entry.
    let result = get_timestamp(&obj, timestamp_key);
    if result.is_err() {
      error!("Timestamp error, ignoring entry {:?}", obj);
      continue;
    }
    let timestamp = result.unwrap();

    // Find the labels for this time series entry.
    let mut labels: HashMap<String, String> = HashMap::new();
    for (key, value) in obj.iter() {
      if key == labels_key && value.is_object() {
        let value_object = value.as_object().unwrap();

        for (key, value) in value_object.iter() {
          if value.is_string() {
            let value_str = value.as_str().unwrap();
            labels.insert(key.to_owned(), value_str.to_owned());
          }
        }
      }
    }

    // Find individual metric points in this time series entry and insert in coredb.
    for (key, value) in obj.iter() {
      if key != timestamp_key && key != labels_key {
        let value_f64: f64;
        if value.is_f64() {
          value_f64 = value.as_f64().expect("Unexpected value type");
        } else if value.is_i64() {
          value_f64 = value.as_i64().expect("Unexpected value type") as f64;
        } else if value.is_u64() {
          value_f64 = value.as_u64().expect("Unexpected value type") as f64;
        } else {
          error!(
            "Ignoring value {} for key {} as it is not a number",
            value, key
          );
          continue;
        }

        state
          .coredb
          .append_metric_point(key, &labels, timestamp, value_f64);
      }
    }
  }

  Ok(())
}

/// Deprecated function for backwards-compatibility. Wraps search_logs().
// TODO: Remove this function by Jan 2024.
#[allow(dead_code)]
#[deprecated(note = "Use search_logs instead")]
async fn search_log(
  State(state): State<Arc<AppState>>,
  Query(logs_query): Query<LogsQuery>,
) -> String {
  search_logs(
    axum::extract::State(state),
    axum::extract::Query(logs_query),
  )
  .await
}

/// Search logs in CoreDB.
async fn search_logs(
  State(state): State<Arc<AppState>>,
  Query(logs_query): Query<LogsQuery>,
) -> String {
  debug!("Searching logs: {:?}", logs_query);

  let results = state.coredb.search_logs(
    &logs_query.text,
    // The default for range start time is 0.
    logs_query.start_time.unwrap_or(0),
    // The default for range end time is the current time.
    logs_query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64),
  );

  serde_json::to_string(&results).expect("Could not convert search results to json")
}

/// Search and summarize logs in CoreDB.
async fn summarize(
  State(state): State<Arc<AppState>>,
  Query(summarize_query): Query<SummarizeQuery>,
) -> Result<String, (StatusCode, String)> {
  debug!("Summarizing logs: {:?}", summarize_query);

  // Number of log message to summarize.
  let k = summarize_query.k.unwrap_or(100);

  let results = state.coredb.search_logs(
    &summarize_query.text,
    // The default for range start time is 0.
    summarize_query.start_time.unwrap_or(0),
    // The default for range end time is the current time.
    summarize_query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64),
  );

  let summary = state.openai_helper.summarize(&results, k);
  match summary {
    Some(summary) => {
      let response = SummarizeQueryResponse { summary, results };

      let retval =
        serde_json::to_string(&response).expect("Could not convert search results to json");
      Ok(retval)
    }
    None => {
      let mut msg: String = "Could not summarize logs.".to_owned();
      let is_var_set = std::env::var_os("OPENAI_API_KEY").is_some();
      if !is_var_set {
        msg = format!("{} Pl check if OPENAI_API_KEY is set.", msg);
      }

      Err((StatusCode::FAILED_DEPENDENCY, msg))
    }
  }
}

/// Deprecated function for backwards-compatibility. Wraps search_metrics().
// TODO: Remove this function by Jan 2024.
#[allow(dead_code)]
#[deprecated(note = "Use search_metrics instead")]
async fn search_ts(
  State(state): State<Arc<AppState>>,
  Query(metrics_query): Query<MetricsQuery>,
) -> String {
  search_metrics(
    axum::extract::State(state),
    axum::extract::Query(metrics_query),
  )
  .await
}

/// Search metrics in CoreDB.
async fn search_metrics(
  State(state): State<Arc<AppState>>,
  Query(metrics_query): Query<MetricsQuery>,
) -> String {
  debug!("Searching metrics: {:?}", metrics_query);

  let results = state.coredb.get_metrics(
    &metrics_query.label_name,
    &metrics_query.label_value,
    // The default for range start time is 0.
    metrics_query.start_time.unwrap_or(0_u64),
    // The default for range end time is the current time.
    metrics_query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64),
  );

  serde_json::to_string(&results).expect("Could not convert search results to json")
}

/// Flush the index to disk.
async fn flush(State(state): State<Arc<AppState>>) -> Result<(), (StatusCode, String)> {
  // sync_after_commit flag is set to true to focibly flush the index to disk. This is used usually during tests and should be avoided in production.
  state.coredb.commit(true);

  Ok(())
}

/// Get index directory used by CoreDB.
async fn get_index_dir(State(state): State<Arc<AppState>>) -> String {
  state.coredb.get_index_dir()
}

/// Ping to check if the server is up.
async fn ping(State(_state): State<Arc<AppState>>) -> String {
  "OK".to_owned()
}

/// Create a new index in CoreDB with the given name.
async fn create_index(
  state: State<Arc<AppState>>,
  Path(index_name): Path<String>,
) -> Result<(), (StatusCode, String)> {
  debug!("Creating index {}", index_name);

  let result = state.coredb.create_index(&index_name);

  if result.is_err() {
    let msg = format!("Could not create index {}", index_name);
    error!("{}", msg);
    return Err((StatusCode::BAD_REQUEST, msg));
  }

  Ok(())
}

/// Create a function to delete an index with given name.
async fn delete_index(
  State(state): State<Arc<AppState>>,
  Path(index_name): Path<String>,
) -> Result<(), (StatusCode, String)> {
  debug!("Deleting index {}", index_name);

  let result = state.coredb.delete_index(&index_name);
  if result.is_err() {
    let msg = format!("Could not delete index {}", index_name);
    error!("{} with error: {}", msg, result.err().unwrap());
    return Err((StatusCode::BAD_REQUEST, msg));
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use std::fs::File;
  use std::io::Write;

  use axum::{
    body::Body,
    http::{self, Request, StatusCode},
  };
  use chrono::Utc;
  use serde_json::json;
  use tempdir::TempDir;
  use test_case::test_case;
  use tower::Service;
  use urlencoding::encode;

  use coredb::log::log_message::LogMessage;
  use coredb::metric::metric_point::MetricPoint;
  use coredb::utils::io::get_joined_path;
  use coredb::utils::tokenize::FIELD_DELIMITER;

  use super::*;

  #[derive(Debug, Deserialize, Serialize)]
  /// Represents an entry in the metric append request.
  struct Metric {
    time: u64,
    metric_name_value: HashMap<String, f64>,
    labels: HashMap<String, String>,
  }

  /// Helper function to initialize a logger for tests.
  fn init() {
    let _ = env_logger::builder()
      .is_test(true)
      .filter_level(log::LevelFilter::Info)
      .try_init();
  }

  /// Helper function to create a test configuration.
  fn create_test_config(
    config_dir_path: &str,
    index_dir_path: &str,
    container_name: &str,
    use_rabbitmq: bool,
  ) {
    // Create a test config in the directory config_dir_path.
    let config_file_path =
      get_joined_path(config_dir_path, Settings::get_default_config_file_name());
    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let default_index_name = format!("default_index_name = \"{}\"\n", "default");
      let container_name_line = format!("container_name = \"{}\"\n", container_name);
      let use_rabbitmq_str = use_rabbitmq
        .then(|| "yes".to_string())
        .unwrap_or_else(|| "no".to_string());
      let use_rabbitmq_line = format!("use_rabbitmq = \"{}\"\n", use_rabbitmq_str);

      // Note that we use different rabbitmq ports from the Infino server as well as other tests, so that there is no port conflict.
      let rabbitmq_listen_port = 2224;
      let rabbitmq_stream_port = 2225;
      let rabbimq_listen_port_line = format!("listen_port = \"{}\"\n", rabbitmq_listen_port);
      let rabbimq_stream_port_line = format!("stream_port = \"{}\"\n", rabbitmq_stream_port);

      let mut file = File::create(config_file_path).unwrap();

      // Write coredb section.
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file.write_all(default_index_name.as_bytes()).unwrap();
      file
        .write_all(b"num_log_messages_threshold = 1000\n")
        .unwrap();
      file
        .write_all(b"num_metric_points_threshold = 10000\n")
        .unwrap();

      // Write server section.
      file.write_all(b"[server]\n").unwrap();
      file.write_all(b"port = 3000\n").unwrap();
      file.write_all(b"commit_interval_in_seconds = 1\n").unwrap();
      file.write_all(b"timestamp_key = \"date\"\n").unwrap();
      file.write_all(b"labels_key = \"labels\"\n").unwrap();
      file.write_all(use_rabbitmq_line.as_bytes()).unwrap();

      // Write rabbitmq section.
      file.write_all(b"[rabbitmq]\n").unwrap();
      file.write_all(rabbimq_listen_port_line.as_bytes()).unwrap();
      file.write_all(rabbimq_stream_port_line.as_bytes()).unwrap();
      file.write_all(container_name_line.as_bytes()).unwrap();
    }
  }

  async fn check_search_logs(
    app: &mut Router,
    config_dir_path: &str,
    search_text: &str,
    query: LogsQuery,
    log_messages_expected: Vec<LogMessage>,
  ) {
    let query_start_time = query
      .start_time
      .map_or_else(|| "".to_owned(), |value| format!("&start_time={}", value));
    let query_end_time = query
      .end_time
      .map_or_else(|| "".to_owned(), |value| format!("&end_time={}", value))
      .to_owned();
    let query_string = format!(
      "text={}{}{}",
      encode(&query.text),
      query_start_time,
      query_end_time
    );

    let uri = format!("/search_logs?{}", query_string);
    info!("Checking for uri: {}", uri);

    // Now call search to get the documents.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri(uri)
          .header(http::header::CONTENT_TYPE, mime::TEXT_PLAIN.as_ref())
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let mut log_messages_received: Vec<LogMessage> = serde_json::from_slice(&body).unwrap();

    assert_eq!(log_messages_expected.len(), log_messages_received.len());
    assert_eq!(log_messages_expected, log_messages_received);

    // Sleep for 2 seconds and refresh from the index directory.
    sleep(Duration::from_millis(2000)).await;

    let refreshed_coredb = CoreDB::refresh(config_dir_path);
    let start_time = query.start_time.unwrap_or(0);
    let end_time = query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64);
    log_messages_received = refreshed_coredb.search_logs(search_text, start_time, end_time);

    assert_eq!(log_messages_expected.len(), log_messages_received.len());
    assert_eq!(log_messages_expected, log_messages_received);
  }

  fn check_metric_point_vectors(expected: &Vec<MetricPoint>, received: &Vec<MetricPoint>) {
    assert_eq!(expected.len(), received.len());

    // The time series is sorted by time - and in tests we may insert multiple values at the same time instant.
    // To avoid test failure in such scenarios, we compare the times and values separately. We also need to sort
    // received values as they may not be in sorted order (only time is the sort key in time series).
    let expected_times: Vec<u64> = expected.iter().map(|item| item.get_time()).collect();
    let expected_values: Vec<f64> = expected.iter().map(|item| item.get_value()).collect();
    let received_times: Vec<u64> = received.iter().map(|item| item.get_time()).collect();
    let mut received_values: Vec<f64> = received.iter().map(|item| item.get_value()).collect();
    received_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

    assert_eq!(expected_times, received_times);
    assert_eq!(expected_values, received_values);
  }

  async fn check_time_series(
    app: &mut Router,
    config_dir_path: &str,
    query: MetricsQuery,
    metric_points_expected: Vec<MetricPoint>,
  ) {
    let query_start_time = query
      .start_time
      .map_or_else(|| "".to_owned(), |value| format!("&start_time={}", value));
    let query_end_time = query
      .end_time
      .map_or_else(|| "".to_owned(), |value| format!("&end_time={}", value));
    let query_string = format!(
      "label_name={}&label_value={}{}{}",
      encode(&query.label_name),
      encode(&query.label_value),
      query_start_time,
      query_end_time
    );

    let uri = format!("/search_metrics?{}", query_string);
    info!("Checking for uri: {}", uri);

    // Now call search to get the documents.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri(uri)
          .header(http::header::CONTENT_TYPE, mime::TEXT_PLAIN.as_ref())
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();

    println!("Response is {:?}", response);
    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let mut metric_points_received: Vec<MetricPoint> = serde_json::from_slice(&body).unwrap();

    check_metric_point_vectors(&metric_points_expected, &metric_points_received);

    // Sleep for 2 seconds and refresh from the index directory.
    sleep(Duration::from_millis(2000)).await;

    let refreshed_coredb = CoreDB::refresh(config_dir_path);
    let start_time = query.start_time.unwrap_or(0);
    let end_time = query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64);
    metric_points_received =
      refreshed_coredb.get_metrics(&query.label_name, &query.label_value, start_time, end_time);

    check_metric_point_vectors(&metric_points_expected, &metric_points_received);
  }

  // Only run the tests withour rabbitmq, as that is the use-case we are targeting.
  //  #[test_case(true ; "use rabbitmq")]
  #[test_case(false ; "do not use rabbitmq")]
  #[tokio::test]
  async fn test_basic(use_rabbitmq: bool) {
    init();

    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let container_name = "infino-test-main-rs";

    create_test_config(
      config_dir_path,
      index_dir_path,
      container_name,
      use_rabbitmq,
    );
    println!("Config dir path {}", config_dir_path);

    // Create the app.
    let (mut app, _, _, _) = app(config_dir_path, "rabbitmq", "3").await;

    // Check whether the /ping works.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri("/ping")
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // **Part 1**: Test insertion and search of log messages
    let num_log_messages = 100;
    let mut log_messages_expected = Vec::new();
    for _ in 0..num_log_messages {
      let time = Utc::now().timestamp_millis() as u64;

      let mut log = HashMap::new();
      log.insert("date", json!(time));
      log.insert("field12", json!("value1 value2"));
      log.insert("field34", json!("value3 value4"));

      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri("/append_log")
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_string(&log).unwrap()))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);

      // Create the expected LogMessage.
      let mut fields = HashMap::new();
      fields.insert("field12".to_owned(), "value1 value2".to_owned());
      fields.insert("field34".to_owned(), "value3 value4".to_owned());
      let text = "value1 value2 value3 value4";
      let log_message_expected = LogMessage::new_with_fields_and_text(time, &fields, text);
      log_messages_expected.push(log_message_expected);
    } // end for

    // Sort the expected log messages in reverse chronological order.
    log_messages_expected.sort();

    let search_query = &format!("value1 field34{}value4", FIELD_DELIMITER);

    let query = LogsQuery {
      start_time: None,
      end_time: None,
      text: search_query.to_owned(),
    };
    check_search_logs(
      &mut app,
      config_dir_path,
      search_query,
      query,
      log_messages_expected,
    )
    .await;

    // End time in this query is too old - this should yield 0 results.
    let query_too_old = LogsQuery {
      start_time: Some(1),
      end_time: Some(10000),
      text: search_query.to_owned(),
    };
    check_search_logs(
      &mut app,
      config_dir_path,
      search_query,
      query_too_old,
      Vec::new(),
    )
    .await;

    // **Part 2**: Test insertion and search of time series metric points.
    let num_metric_points = 100;
    let mut metric_points_expected = Vec::new();
    let name_for_metric_name_label = "__name__";
    let metric_name = "some_metric_name";

    for i in 0..num_metric_points {
      let time = Utc::now().timestamp_millis() as u64;
      let value = i as f64;
      let metric_point = MetricPoint::new(time, i as f64);

      let json_str = format!("{{\"date\": {}, \"{}\":{}}}", time, metric_name, value);
      metric_points_expected.push(metric_point);

      // Insert the metric.
      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri("/append_metric")
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(json_str))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);
    }

    // Check whether we get all the metric points back when the start and end_times are not specified
    // (i.e., they will default to 0 and to current time respectively).
    let query = MetricsQuery {
      label_name: name_for_metric_name_label.to_owned(),
      label_value: metric_name.to_owned(),
      start_time: None,
      end_time: None,
    };
    check_time_series(&mut app, config_dir_path, query, metric_points_expected).await;

    // End time in this query is too old - this should yield 0 results.
    let query_too_old = MetricsQuery {
      label_name: name_for_metric_name_label.to_owned(),
      label_value: metric_name.to_owned(),
      start_time: Some(1),
      end_time: Some(10000),
    };
    check_time_series(&mut app, config_dir_path, query_too_old, Vec::new()).await;

    // Check whether the /flush works.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::POST)
          .uri("/flush")
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Stop the RabbbitMQ container.
    let _ = RabbitMQ::stop_queue_container(container_name);
  }

  /// Write test to test Create and Delete index APIs.
  #[test_case(false ; "do not use rabbitmq")]
  #[tokio::test]
  async fn test_create_delete_index(use_rabbitmq: bool) {
    init();

    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let container_name = "infino-test-main-rs";

    create_test_config(
      config_dir_path,
      index_dir_path,
      container_name,
      use_rabbitmq,
    );
    println!("Config dir path {}", config_dir_path);

    // Create the app.
    let (mut app, _, _, _) = app(config_dir_path, "rabbitmq", "3").await;

    // Create an index.
    let index_name = "test_index";
    let response = app
      .call(
        Request::builder()
          .method(http::Method::PUT)
          .uri(&format!("/{}", index_name))
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Check whether the index directory exists.
    let index_dir_path = get_joined_path(index_dir_path, index_name);
    assert!(std::path::Path::new(&index_dir_path).exists());

    // Delete the index.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::DELETE)
          .uri(&format!("/{}", index_name))
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Check whether the index directory exists.
    assert!(!std::path::Path::new(&index_dir_path).exists());

    // Stop the RabbbitMQ container.
    let _ = RabbitMQ::stop_queue_container(container_name);
  }
}

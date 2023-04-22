mod queue_manager;
mod utils;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::Query;
use axum::{extract::State, routing::get, routing::post, Json, Router};
use chrono::Utc;
use hyper::StatusCode;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use tsldb::Tsldb;
use utils::error::InfinoError;

use crate::queue_manager::queue::RabbitMQ;
use crate::utils::settings::Settings;
use crate::utils::shutdown::shutdown_signal;

/// Represents application state.
struct AppState {
  queue: RabbitMQ,
  tsldb: Tsldb,
  settings: Settings,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents search query.
struct SearchQuery {
  text: String,
  start_time: u64,
  end_time: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents a query to time series db.
struct TimeSeriesQuery {
  label_name: String,
  label_value: String,
  start_time: u64,
  end_time: Option<u64>,
}

/// Periodically commits tsldb (typically called in a thread, so that tsldb can be asyncronously committed).
async fn commit_in_loop(
  state: Arc<AppState>,
  commit_interval_in_seconds: u32,
  shutdown_flag: Arc<Mutex<bool>>,
) {
  loop {
    state.tsldb.commit(true);

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

  // Create a new tsldb.
  let tsldb = match Tsldb::new(config_dir_path) {
    Ok(tsldb) => tsldb,
    Err(err) => panic!("Unable to initialize tsldb with err {}", err),
  };

  // Create RabbitMQ to store incoming requests.
  let rabbitmq_settings = settings.get_rabbitmq_settings();
  let container_name = rabbitmq_settings.get_container_name();
  let listen_port = rabbitmq_settings.get_listen_port();
  let stream_port = rabbitmq_settings.get_stream_port();
  let queue = RabbitMQ::new(
    container_name,
    image_name,
    image_tag,
    listen_port,
    stream_port,
  )
  .await;

  let shared_state = Arc::new(AppState {
    queue,
    tsldb,
    settings,
  });

  let server_settings = shared_state.settings.get_server_settings();
  let commit_interval_in_seconds = server_settings.get_commit_interval_in_seconds();

  // Start a thread to periodically commit tsldb.
  info!("Spawning new thread to periodically commit");
  let commit_thread_shutdown_flag = Arc::new(Mutex::new(false));
  let commit_thread_handle = tokio::spawn(commit_in_loop(
    shared_state.clone(),
    commit_interval_in_seconds,
    commit_thread_shutdown_flag.clone(),
  ));

  // Build our application with a route
  let router: Router = Router::new()
    .route("/append_log", post(append_log))
    .route("/append_ts", post(append_ts))
    .route("/search_log", get(search_log))
    .route("/search_ts", get(search_ts))
    .route("/get_index_dir", get(get_index_dir))
    .with_state(shared_state.clone());

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
  // Initialize logger from env. If no log level specified, default to info.
  env_logger::init_from_env(
    env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
  );

  let config_dir_path = "config";
  let image_name = "rabbitmq";
  let image_tag = "3";

  // Create app.
  let (app, commit_thread_handle, commit_thread_shutdown_flag, shared_state) =
    app(config_dir_path, image_name, image_tag).await;

  // Start server.
  let port = shared_state.settings.get_server_settings().get_port();
  let addr = SocketAddr::from(([127, 0, 0, 1], port));

  info!(
    "Infino server listening on {}. Use Ctrl-C or SIGTERM to gracefully exit...",
    addr
  );
  axum::Server::bind(&addr)
    .serve(app.into_make_service())
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();

  info!("Closing RabbitMQ connection...");
  shared_state.queue.close_connection().await;

  info!("Stopping RabbitMQ container...");
  let rabbitmq_container_name = shared_state.queue.get_container_name();
  RabbitMQ::stop_queue_container(rabbitmq_container_name)
    .expect("Could not stop rabbitmq container");

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

/// Append log data to tsldb.
async fn append_log(
  State(state): State<Arc<AppState>>,
  Json(log_json): Json<serde_json::Value>,
) -> Result<(), (StatusCode, String)> {
  debug!("Appending log entry {}", log_json);

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
    state.queue.publish(&obj_string).await.unwrap();

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

    state.tsldb.append_log_message(timestamp, &fields, &text);
  }

  Ok(())
}

/// Append time series data to tsldb.
async fn append_ts(
  State(state): State<Arc<AppState>>,
  Json(ts_json): Json<serde_json::Value>,
) -> Result<(), (StatusCode, String)> {
  debug!("Appending time series entry: {:?}", ts_json);

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
    state.queue.publish(&obj_string).await.unwrap();

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

    // Find individual data points in this time series entry and insert in tsldb.
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
          .tsldb
          .append_data_point(key, &labels, timestamp, value_f64);
      }
    }
  }

  Ok(())
}

/// Search logs in tsldb.
async fn search_log(
  State(state): State<Arc<AppState>>,
  Query(search_query): Query<SearchQuery>,
) -> String {
  debug!("Searching log: {:?}", search_query);

  let results = state.tsldb.search(
    &search_query.text,
    search_query.start_time,
    search_query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64),
  );

  serde_json::to_string(&results).expect("Could not convert search results to json")
}

/// Search time series.
async fn search_ts(
  State(state): State<Arc<AppState>>,
  Query(time_series_query): Query<TimeSeriesQuery>,
) -> String {
  debug!("Searching time series: {:?}", time_series_query);

  let results = state.tsldb.get_time_series(
    &time_series_query.label_name,
    &time_series_query.label_value,
    time_series_query.start_time,
    // The default for range end is the current time.
    time_series_query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64),
  );

  serde_json::to_string(&results).expect("Could not convert search results to json")
}

/// Get index directory used by tsldb.
async fn get_index_dir(State(state): State<Arc<AppState>>) -> String {
  state.tsldb.get_index_dir()
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
  use tower::Service;
  use urlencoding::encode;

  use tsldb::log::log_message::LogMessage;
  use tsldb::ts::data_point::DataPoint;
  use tsldb::utils::io::get_joined_path;

  use super::*;

  #[derive(Debug, Deserialize, Serialize)]
  /// Represents an entry in the time series append request.
  struct TimeSeriesEntry {
    time: u64,
    metric_name_value: HashMap<String, f64>,
    labels: HashMap<String, String>,
  }

  /// Helper function initialize logger for tests.
  fn init() {
    let _ = env_logger::builder()
      .is_test(true)
      .filter_level(log::LevelFilter::Info)
      .try_init();
  }

  /// Helper function to create a test configuration.
  fn create_test_config(config_dir_path: &str, index_dir_path: &str, container_name: &str) {
    // Create a test config in the directory config_dir_path.
    let config_file_path =
      get_joined_path(config_dir_path, Settings::get_default_config_file_name());
    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let container_name_line = format!("container_name = \"{}\"\n", container_name);

      // Note that we use different rabbitmq ports from the Infino server as well as other tests, so that there is no port conflict.
      let rabbitmq_listen_port = 2224;
      let rabbitmq_stream_port = 2225;
      let rabbimq_listen_port_line = format!("listen_port = \"{}\"\n", rabbitmq_listen_port);
      let rabbimq_stream_port_line = format!("stream_port = \"{}\"\n", rabbitmq_stream_port);

      let mut file = File::create(&config_file_path).unwrap();
      file.write_all(b"[tsldb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file
        .write_all(b"num_log_messages_threshold = 1000\n")
        .unwrap();
      file
        .write_all(b"num_data_points_threshold = 10000\n")
        .unwrap();
      file.write_all(b"[server]\n").unwrap();
      file.write_all(b"port = 3000\n").unwrap();
      file.write_all(b"commit_interval_in_seconds = 1\n").unwrap();
      file.write_all(b"timestamp_key = \"date\"\n").unwrap();
      file.write_all(b"labels_key = \"labels\"\n").unwrap();
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
    query: SearchQuery,
    log_messages_expected: Vec<LogMessage>,
  ) {
    let query_string;
    match query.end_time {
      Some(end_time) => {
        query_string = format!(
          "start_time={}&end_time={}&text={}",
          query.start_time,
          end_time,
          encode(&query.text)
        )
      }
      None => {
        query_string = format!(
          "start_time={}&text={}",
          query.start_time,
          encode(&query.text)
        )
      }
    }

    let uri = format!("/search_log?{}", query_string);
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

    let refreshed_tsldb = Tsldb::refresh(config_dir_path);
    let end_time = query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64);
    log_messages_received = refreshed_tsldb.search(search_text, query.start_time, end_time);

    assert_eq!(log_messages_expected.len(), log_messages_received.len());
    assert_eq!(log_messages_expected, log_messages_received);
  }

  fn check_data_point_vectors(expected: &Vec<DataPoint>, received: &Vec<DataPoint>) {
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
    query: TimeSeriesQuery,
    data_points_expected: Vec<DataPoint>,
  ) {
    let query_string;
    match query.end_time {
      Some(end_time) => {
        query_string = format!(
          "label_name={}&label_value={}&start_time={}&end_time={}",
          encode(&query.label_name),
          encode(&query.label_value),
          query.start_time,
          end_time,
        )
      }
      None => {
        query_string = format!(
          "label_name={}&label_value={}&start_time={}",
          encode(&query.label_name),
          encode(&query.label_value),
          query.start_time,
        )
      }
    }

    let uri = format!("/search_ts?{}", query_string);
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
    let mut data_points_received: Vec<DataPoint> = serde_json::from_slice(&body).unwrap();

    check_data_point_vectors(&data_points_expected, &data_points_received);

    // Sleep for 2 seconds and refresh from the index directory.
    sleep(Duration::from_millis(2000)).await;

    let refreshed_tsldb = Tsldb::refresh(config_dir_path);
    let end_time = query
      .end_time
      .unwrap_or(Utc::now().timestamp_millis() as u64);
    data_points_received = refreshed_tsldb.get_time_series(
      &query.label_name,
      &query.label_value,
      query.start_time,
      end_time,
    );

    check_data_point_vectors(&data_points_expected, &data_points_received);
  }

  #[tokio::test]
  async fn test_basic() {
    init();

    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let container_name = "infino-test-main-rs";
    create_test_config(config_dir_path, index_dir_path, container_name);
    println!("Config dir path {}", config_dir_path);

    // Create the app.
    let (mut app, _, _, _) = app(config_dir_path, "rabbitmq", "3").await;

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

    let search_query = "value1 field34:value4";

    let query = SearchQuery {
      start_time: 0,
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
    let query_too_old = SearchQuery {
      start_time: 0,
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

    // **Part 2**: Test insertion and search of time series data points.
    let num_data_points = 100;
    let mut data_points_expected = Vec::new();
    let name_for_metric_name_label = "__name__";
    let metric_name = "some_metric_name";

    for i in 0..num_data_points {
      let time = Utc::now().timestamp_millis() as u64;
      let value = i as f64;
      let data_point = DataPoint::new(time, i as f64);

      let json_str = format!("{{\"date\": {}, \"{}\":{}}}", time, metric_name, value);
      data_points_expected.push(data_point);

      // Insert the time series.
      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri("/append_ts")
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(json_str))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);
    }

    // Check whether we get all the data points back when end_time is not specified (i.e., it will defauly to current time).
    let query = TimeSeriesQuery {
      label_name: name_for_metric_name_label.to_owned(),
      label_value: metric_name.to_owned(),
      start_time: 0,
      end_time: None,
    };
    check_time_series(&mut app, config_dir_path, query, data_points_expected).await;

    // End time in this query is too old - this should yield 0 results.
    let query_too_old = TimeSeriesQuery {
      label_name: name_for_metric_name_label.to_owned(),
      label_value: metric_name.to_owned(),
      start_time: 0,
      end_time: Some(10000),
    };
    check_time_series(&mut app, config_dir_path, query_too_old, Vec::new()).await;

    // Stop the RabbbitMQ container.
    let _ = RabbitMQ::stop_queue_container(container_name);
  }
}

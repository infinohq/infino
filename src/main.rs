mod queue_manager;
mod utils;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{extract::State, routing::get, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

// TODO: figure out a way to not have LogMessage and DataPoint way deep in tsldb / or change the API to time/value.
use tsldb::log::log_message::LogMessage;
use tsldb::ts::data_point::DataPoint;
use tsldb::Tsldb;

use crate::queue_manager::queue::RabbitMQ;
use crate::utils::settings::Settings;

async fn create_queue(container_name: &str, image_name: &str, image_tag: &str) -> RabbitMQ {
  // TODO: make usage of existing queue possible rather than starting every time.
  let _ = RabbitMQ::stop_queue_container(container_name);

  let start_result = RabbitMQ::start_queue_container(container_name, image_name, image_tag).await;
  assert!(start_result.is_ok());
  // The container is not immediately ready to accept connections - hence sleep for some time.
  sleep(Duration::from_millis(5000)).await;

  RabbitMQ::new(container_name, image_name, image_tag).await
}
struct AppState {
  queue: RabbitMQ,
  tsldb: Tsldb,
}

#[derive(Deserialize, Serialize)]
struct SearchQuery {
  text: String,
  start_time: u64,
  end_time: u64,
}

#[derive(Deserialize, Serialize)]
struct TimeSeriesEntry {
  metric_name: String,
  labels: HashMap<String, String>,
  data_point: DataPoint,
}

#[derive(Deserialize, Serialize)]
struct TimeSeriesQuery {
  label_name: String,
  label_value: String,
  start_time: u64,
  end_time: u64,
}

async fn commit_in_loop(state: Arc<AppState>, commit_interval_in_seconds: u32) {
  loop {
    let now = chrono::Utc::now().to_rfc2822();
    println!("Committing at {}", now);
    state.tsldb.commit(true);
    sleep(Duration::from_secs(commit_interval_in_seconds as u64)).await;
  }
}

async fn app(config_dir_path: &str, image_name: &str, image_tag: &str) -> (Router, Settings) {
  // Read the settings from the config directory.
  let settings = Settings::new(config_dir_path).unwrap();

  // Create a new tsldb.
  let tsldb = match Tsldb::new(config_dir_path) {
    Ok(tsldb) => tsldb,
    Err(err) => panic!("Unable to initialize tsldb with err {}", err),
  };

  // Create RabbitMQ to store incoming requests.
  let container_name = settings.get_rabbitmq_settings().get_container_name();
  let queue = create_queue(container_name, image_name, image_tag).await;

  let shared_state = Arc::new(AppState { queue, tsldb });

  // Start a thread to periodically commit tsldb.
  println!("Spawning new thread to periodically commit");
  let commit_interval_in_seconds = settings
    .get_server_settings()
    .get_commit_interval_in_seconds();
  tokio::spawn(commit_in_loop(
    shared_state.clone(),
    commit_interval_in_seconds,
  ));

  // Build our application with a route
  let router = Router::new()
    .route("/append_log", post(append_log))
    .route("/append_ts", post(append_ts))
    .route("/search_log", get(search_log))
    .route("/search_ts", get(search_ts))
    .route("/get_index_dir", get(get_index_dir))
    .with_state(shared_state);

  (router, settings)
}

#[tokio::main]
async fn main() {
  let config_dir_path = "config";
  let image_name = "rabbitmq";
  let image_tag = "3";

  // Create app.
  let (app, settings) = app(config_dir_path, image_name, image_tag).await;

  // Start server.
  let port = settings.get_server_settings().get_port();
  let addr = SocketAddr::from(([127, 0, 0, 1], port));
  println!("listening on {}", addr);
  axum::Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}

async fn append_log(State(state): State<Arc<AppState>>, Json(log_message): Json<LogMessage>) {
  let log_message_string = serde_json::to_string(&log_message).unwrap();
  state.queue.publish(&log_message_string).await.unwrap();
  state
    .tsldb
    .append_log_message(log_message.get_time(), log_message.get_message());
}

async fn append_ts(
  State(state): State<Arc<AppState>>,
  Json(time_series_entry): Json<TimeSeriesEntry>,
) {
  let json_string = serde_json::to_string(&time_series_entry).unwrap();
  state.queue.publish(&json_string).await.unwrap();
  let data_point = time_series_entry.data_point;
  state.tsldb.append_data_point(
    &time_series_entry.metric_name,
    &time_series_entry.labels,
    data_point.get_time(),
    data_point.get_value(),
  );
}

async fn search_log(
  State(state): State<Arc<AppState>>,
  Json(search_query): Json<SearchQuery>,
) -> Json<Vec<LogMessage>> {
  let results = state.tsldb.search(
    &search_query.text,
    search_query.start_time,
    search_query.end_time,
  );
  Json(results)
}

/// Search time series.
async fn search_ts(
  State(state): State<Arc<AppState>>,
  Json(time_series_query): Json<TimeSeriesQuery>,
) -> Json<Vec<DataPoint>> {
  let results = state.tsldb.get_time_series(
    &time_series_query.label_name,
    &time_series_query.label_value,
    time_series_query.start_time,
    time_series_query.end_time,
  );
  Json(results)
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
  use serial_test::serial;
  use tempdir::TempDir;
  use tower::Service;
  use tsldb::utils::io::get_joined_path;

  use super::*;

  fn create_test_config(config_dir_path: &str, index_dir_path: &str, container_name: &str) {
    // Create a test config in the directory config_dir_path.
    let config_file_path =
      get_joined_path(config_dir_path, Settings::get_default_config_file_name());
    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let container_name_line = format!("container_name = \"{}\"\n", container_name);
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
      file.write_all(b"commit_interval_in_seconds = 1\n").unwrap();
      file.write_all(b"[rabbitmq]\n").unwrap();
      file.write_all(container_name_line.as_bytes()).unwrap();
    }
  }

  #[ignore = "avoid port conflict in case the machine running tests also has infino running in production"]
  #[serial]
  #[tokio::test]
  async fn test_basic() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let container_name = "infino-test";
    create_test_config(config_dir_path, index_dir_path, container_name);

    println!("Config dir path {}", config_dir_path);
    let (mut app, _) = app(config_dir_path, "rabbitmq", "3").await;

    // **Part 1**: Test insertion and search of log messages
    let num_log_messages = 100;
    let mut log_messages_expected = Vec::new();
    let search_query = "message";
    for i in 0..num_log_messages {
      let log_message = LogMessage::new(
        Utc::now().timestamp_millis() as u64,
        &format!("this is my log message #{}", i),
      );

      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri("/append_log")
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_string(&log_message).unwrap()))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);
      log_messages_expected.push(log_message);
    } // end for

    let query = SearchQuery {
      start_time: 0,
      end_time: u64::MAX,
      text: search_query.to_owned(),
    };
    // Now call search to get the documents.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri("/search_log")
          .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
          .body(Body::from(serde_json::to_string(&query).unwrap()))
          .unwrap(),
      )
      .await
      .unwrap();
    println!("Response is {:?}", response);
    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let mut log_messages_received: Vec<LogMessage> = serde_json::from_slice(&body).unwrap();

    assert_eq!(log_messages_received.len(), num_log_messages);
    assert_eq!(log_messages_expected, log_messages_received);

    // Sleep for 2 seconds and refresh from the index directory.
    sleep(Duration::from_millis(2000)).await;

    let refreshed_tsldb = Tsldb::refresh(config_dir_path);
    log_messages_received = refreshed_tsldb.search(search_query, 0, u64::MAX);

    assert_eq!(log_messages_received.len(), num_log_messages);
    assert_eq!(log_messages_expected, log_messages_received);

    // **Part 2**: Test insertion and search of time series data points.
    let num_data_points = 100;
    let mut data_points_expected = Vec::new();
    let label_name = "__name__";
    let label_value = "some_name";

    for i in 0..num_data_points {
      let data_point = DataPoint::new(i as u64, i as f64);
      let time_series_entry = TimeSeriesEntry {
        metric_name: label_value.to_owned(),
        labels: HashMap::new(),
        data_point: data_point.clone(),
      };

      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri("/append_ts")
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(
              serde_json::to_string(&time_series_entry).unwrap(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);

      data_points_expected.push(data_point);
    } // end for

    let query = TimeSeriesQuery {
      label_name: label_name.to_owned(),
      label_value: label_value.to_owned(),
      start_time: 0,
      end_time: u64::MAX,
    };
    // Now call search to get the data points.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri("/search_ts")
          .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
          .body(Body::from(serde_json::to_string(&query).unwrap()))
          .unwrap(),
      )
      .await
      .unwrap();
    println!("Response is {:?}", response);
    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let mut data_points_received: Vec<DataPoint> = serde_json::from_slice(&body).unwrap();

    assert_eq!(data_points_expected.len(), data_points_received.len());
    assert_eq!(data_points_expected, data_points_received);

    // Sleep for 2 seconds and refresh from the index directory.
    sleep(Duration::from_millis(2000)).await;

    let refreshed_tsldb = Tsldb::refresh(config_dir_path);
    data_points_received = refreshed_tsldb.get_time_series(label_name, label_value, 0, u64::MAX);

    assert_eq!(data_points_received.len(), num_data_points);
    assert_eq!(data_points_expected, data_points_received);

    let _ = RabbitMQ::stop_queue_container(container_name);
  }
}

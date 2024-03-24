// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! The Infino server application and interface.
//!
//! The Infino server is an [Axum](https://docs.rs/axum/latest/axum/) web application that handles all API
//! requests to Infino.
//!
//! See an Infino architecture overview [here](https://github.com/infinohq/infino).
//! Infino has data ingestion APIs for storing data in Infino and query APIs
//! for retrieving data form Infino. Ingested data is forwarded to the CoreDB database that stores and retrieves
//!  telemetry data in Infino. The data is written to a write ahead log for disaster recovery before being
//! written to the index.
//!
//! We also summarize logs using Generative AI models; we are currently using [OpenAI](https://platform.openai.com/)
//! but we are evaulating alternatives like [Llama2](https://github.com/facebookresearch/llama) and our own homegrown
//! models. More to come.

mod background_threads;
mod utils;

// If the `dhat-heap` feature is enabled, we use dhat to track heap usage.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Write;
use std::result::Result;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::response::IntoResponse;
use axum::routing::{delete, put};
use axum::{extract::State, routing::get, routing::post, Json, Router};
use chrono::{DateTime, Utc};
use coredb::request_manager::query_dsl_object::QueryDSLObject;
use crossbeam::atomic::AtomicCell;
use hyper::StatusCode;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use coredb::utils::environment::load_env;
use coredb::utils::error::{CoreDBError, QueryError};
use coredb::utils::request::{check_query_time, parse_time_range};
use coredb::CoreDB;

use crate::background_threads::check_and_start_background_threads;
use crate::utils::error::InfinoError;
use crate::utils::openai_helper::OpenAIHelper;
use crate::utils::settings::Settings;
use crate::utils::shutdown::shutdown_signal;

lazy_static! {
  static ref IS_SHUTDOWN: AtomicCell<bool> = AtomicCell::new(false);
}

/// Represents application state.
struct AppState {
  coredb: CoreDB,
  settings: Settings,
  wal_file: Arc<File>,
  openai_helper: OpenAIHelper,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents a logs query.
struct LogsQuery {
  q: Option<String>,
  start_time: Option<String>,
  end_time: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents a metrics query.
struct MetricsQuery {
  query: Option<String>,
  label_name: Option<String>,
  label_value: Option<String>,
  timeout: Option<String>,
  start: Option<u64>,
  end: Option<u64>,
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
struct SummarizeQueryResponse {
  summary: String,
  results: QueryDSLObject,
}

/// Axum application for Infino server.
async fn app(config_dir_path: &str) -> (Router, JoinHandle<()>, Arc<AppState>) {
  // Read the settings from the config directory.
  let settings = Settings::new(config_dir_path).unwrap();

  // Create a new coredb.
  let coredb = match CoreDB::new(config_dir_path).await {
    Ok(coredb) => coredb,
    Err(err) => panic!("Unable to initialize coredb with err {}", err),
  };

  let openai_helper = OpenAIHelper::new();
  let wal_file = Arc::new(
    std::fs::OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open("/tmp/wal.log")
      .unwrap(),
  );

  let shared_state = Arc::new(AppState {
    coredb,
    settings,
    openai_helper,
    wal_file,
  });

  // Start a thread to periodically commit coredb.
  info!("Spawning background threads for commit, and other tasks...");
  let background_threads_handle =
    tokio::spawn(check_and_start_background_threads(shared_state.clone()));

  // Build our application with a route
  // TODO: Support use case when index is not specified in the bulk call
  let router: Router = Router::new()
    // GET methods
    .route("/:index_name/get_index_dir", get(get_index_dir))
    .route("/ping", get(ping))
    .route("/", get(ping))
    .route("/:index_name/search_logs", get(search_logs))
    .route("/:index_name/search_metrics", get(search_metrics))
    .route("/:index_name/summarize", get(summarize))
    //---
    // POST methods
    .route("/:index_name/append_log", post(append_log))
    .route("/:index_name/append_metric", post(append_metric))
    .route("/:index_name/bulk", post(bulk))
    .route("/flush", post(flush))
    // PUT and DELETE methods
    .route("/:index_name", put(create_index))
    .route("/:index_name", delete(delete_index))
    .route(
      "/:index_name/_delete_by_query",
      delete(delete_logs_by_query),
    )
    // ---
    // State that is passed to each request.
    .with_state(shared_state.clone())
    // ---
    // Layer for tracing in debug mode.
    .layer(TraceLayer::new_for_http())
    // Make the default for body to be 5MB (instead of 2MB http default.)
    .layer(DefaultBodyLimit::max(5 * 1024 * 1024));

  (router, background_threads_handle, shared_state)
}

async fn run_server() {
  // Config directory path is relative to the current directory, and set in environment variable "INFINO_CONFIG_DIR_PATH".
  // Defaults to "config" if not set.
  let config_dir_path = &env::var("INFINO_CONFIG_DIR_PATH").unwrap_or_else(|_| "config".to_owned());

  // Create app.
  let (app, background_threads_handle, shared_state) = app(config_dir_path).await;

  // Start server.
  let port = shared_state.settings.get_server_settings().get_port();
  let host: &str = shared_state.settings.get_server_settings().get_host();
  let connection_string = &format!("{}:{}", host, port);
  let listener = TcpListener::bind(connection_string)
    .await
    .unwrap_or_else(|_| panic!("Could not listen using {}", connection_string));

  info!(
    "Starting Infino server on {}. Use Ctrl-C or SIGTERM to gracefully exit...",
    connection_string
  );

  axum::serve(listener, app)
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();

  // Set the flag to indicate the background threads to shutdown, and wait for them to finish.
  IS_SHUTDOWN.store(true);
  info!("Shutting down background threads and waiting for it to finish...");
  background_threads_handle
    .await
    .expect("Error while shutting down the background threads");

  info!("Completed Infino server shutdown");
}

/// Program entry point.
fn main() {
  // If the `dhat-heap` feature is enabled, we use dhat to track heap usage.
  #[cfg(feature = "dhat-heap")]
  let _profiler = dhat::Profiler::new_heap();

  // Load environment variables from ".env" and ".env-creds" file.
  load_env();

  // If log level isn't set, set it to info.
  if env::var("RUST_LOG").is_err() {
    env::set_var("RUST_LOG", "info")
  }

  // Set up logging.
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();

  // TODO: this value could be read from config file, with the default calculated as below if not set.
  // Set the number of threads to be 1 less than the number of CPUs (or 1 if there are fewer than 2 CPUs).
  let num_threads = std::cmp::max(1, num_cpus::get() - 1);

  let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_threads) // Limit the number of worker threads
    .enable_all() // Enables both I/O and time drivers
    .build()
    .unwrap();

  runtime.block_on(async {
    run_server().await;
  });
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

// Helper function to append log messages
async fn append_single_log_message(
  obj: Map<String, Value>,
  index_name: &str,
  state: Arc<AppState>,
  timestamp_key: &str,
) -> Result<u32, CoreDBError> {
  let timestamp = get_timestamp(&obj, timestamp_key).unwrap_or_else(|err| {
    warn!(
      "Timestamp error, adding current time stamp. Entry {:?}: {}",
      obj, err
    );
    chrono::Utc::now().timestamp_millis() as u64
  });

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

  state
    .coredb
    .append_log_message(index_name, timestamp, &fields, &text)
    .await
}

/// Append log data to CoreDB.
#[allow(unused_assignments)]
async fn append_log(
  State(state): State<Arc<AppState>>,
  Path(index_name): Path<String>,
  Json(log_json): Json<serde_json::Value>,
) -> Result<String, (StatusCode, String)> {
  debug!("Appending log entry {}", log_json);

  let append_start_time = Utc::now().timestamp_millis() as u64;

  let mut response = String::new();

  state
    .wal_file
    .clone()
    .write_all(&log_json.to_string().into_bytes()[..])
    .unwrap();

  let result = parse_json(&log_json);
  if result.is_err() {
    let msg = format!("Invalid log entry {}", log_json);
    error!("{}", msg);
    return Err((StatusCode::BAD_REQUEST, msg));
  }
  let log_json_objects = result.unwrap();

  let server_settings = state.settings.get_server_settings();
  let timestamp_key = server_settings.get_timestamp_key();

  let mut items = Vec::new();

  // Append each entry
  // TODO: Skip logs with errors instead of throwing
  for obj in log_json_objects {
    let result = append_single_log_message(obj, &index_name, state.clone(), timestamp_key).await;

    match result {
      Ok(doc_id) => {
        let index_item = json!({
            "index": {
                "_index": "my_index",
                "_type": "_doc",
                "_id": doc_id,
                "_version": 1,
                "result": "created",
                "_shards": {
                    "total": 1,
                    "successful": 1,
                    "failed": 0
                },
                "_seq_no": 0,
                "_primary_term": 1
            }
        });
        items.push(index_item);
      }
      Err(error) => {
        match error {
          CoreDBError::TooManyAppendsError() => {
            return Err((StatusCode::TOO_MANY_REQUESTS, error.to_string()))
          }
          CoreDBError::IndexNotFound(_) => {
            return Err((StatusCode::BAD_REQUEST, "Could not find index".to_string()))
          }
          _ => {
            return Err((
              StatusCode::INTERNAL_SERVER_ERROR,
              "An unexpected error occurred".to_string(),
            ));
          }
        };
      }
    }
  }

  // Constructing the bulk response JSON
  let response_json = json!({
      // Check_query_time will not throw an error when timeout is 0. Fine to unwrap().
      "took": check_query_time(0, append_start_time).unwrap(),
      "errors": false,
      "items": items
  });

  // Serialize the JSON response
  response =
    serde_json::to_string(&response_json).expect("Could not convert append response to JSON");

  Ok(response)
}

/// Append metric data to CoreDB.
async fn append_metric(
  State(state): State<Arc<AppState>>,
  Path(index_name): Path<String>,
  Json(ts_json): Json<serde_json::Value>,
) -> Result<(), (StatusCode, String)> {
  debug!("Appending metric entry: {:?}", ts_json);

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

        let result = state
          .coredb
          .append_metric_point(&index_name, key, &labels, timestamp, value_f64)
          .await;

        if let Err(error) = result {
          match error {
            CoreDBError::TooManyAppendsError() => {
              return Err((StatusCode::TOO_MANY_REQUESTS, error.to_string()));
            }
            _ => {
              error!("An unexpected error occurred.");
            }
          }
        }
      }
    }
  }

  Ok(())
}

#[allow(clippy::single_char_pattern)]
fn process_bulk_json_body(json_body: &str) -> Result<Vec<Value>, (StatusCode, String)> {
  // Preprocess the bulk request input to ensure it's in a JSON array format
  let json_array_string = format!("[{}]", json_body.replace("\r\n", ",").replace('\n', ","));

  // Parse the preprocessed bulk request as a JSON array
  match serde_json::from_str::<Vec<Value>>(&json_array_string) {
    Ok(json_array) => {
      debug!(
        "Successfully parsed JSON array with {} elements",
        json_array.len()
      );
      Ok(json_array)
    }
    Err(e) => {
      error!("Failed to parse JSON array: {}", e);
      Err((
        StatusCode::BAD_REQUEST,
        "Failed to parse JSON array from input.".to_string(),
      ))
    }
  }
}

/// Bulk append data to CoreDB.
#[allow(unused_assignments)]
#[allow(dead_code)]
async fn bulk(
  State(state): State<Arc<AppState>>,
  Path(index_name): Path<String>,
  json_body: String,
) -> Result<String, (StatusCode, String)> {
  debug!("Executing bulk append request {}", json_body);

  let append_start_time = Utc::now().timestamp_millis() as u64;

  let mut response = String::new();

  state
    .wal_file
    .clone()
    .write_all(&json_body.to_string().into_bytes()[..])
    .unwrap();

  let actions = process_bulk_json_body(&json_body)?;

  let server_settings = state.settings.get_server_settings();
  let timestamp_key = server_settings.get_timestamp_key();

  let mut items = Vec::new();
  let mut i = 0;

  while i < actions.len() {
    let action_obj = &actions[i];
    i += 1; // Move to the next item, potentially the document.

    let operation = action_obj.as_object().and_then(|obj| {
      obj.keys().next().map(String::from) // Gets the first key as the operation type.
    });

    match operation.as_deref() {
      Some("index") | Some("create") => {
        if i >= actions.len() {
          let msg = "Missing document for index/create operation.".to_string();
          error!("{}", msg);
          return Err((StatusCode::BAD_REQUEST, msg));
        }
        let document = &actions[i];
        i += 1; // Move past the document for the next iteration.

        // Ensure document is an object and convert to Map<String, Value>
        if let Some(doc_map) = document.as_object() {
          let result =
            append_single_log_message(doc_map.clone(), &index_name, state.clone(), timestamp_key)
              .await;

          match result {
            Ok(doc_id) => {
              let index_item = json!({
                  "index": {
                      "_index": index_name,
                      "_type": "_doc",
                      "_id": doc_id,
                      "status": 200,
                      "_version": 1,
                      "result": "created",
                      "_shards": {
                          "total": 1,
                          "successful": 1,
                          "failed": 0
                      },
                      "_seq_no": 0,
                      "_primary_term": 1
                  }
              });
              items.push(index_item);
            }
            Err(error) => {
              let mut status_code = 200;
              match error {
                CoreDBError::TooManyAppendsError() => {
                  status_code = 429;
                }
                CoreDBError::IndexNotFound(_) => {
                  status_code = 400;
                }
                _ => {
                  status_code = 500;
                }
              };

              let index_item = json!({
                "index": {
                  "_index": index_name,
                  "_type": "_doc",
                  "_id": "1",
                  "status": status_code,
                  "error": {
                    "type": "Invalid request",
                    "reason": error.to_string(),
                    "index_uuid": "aAsFqTI0Tc2W0LCWgPNrOA",
                    "shard": "1",
                    "index": index_name
                  }
                }
              });

              items.push(index_item);
            }
          }
        } else {
          let msg = "Document for index/create operation is not a valid JSON object.".to_string();
          error!("{}", msg);
          return Err((StatusCode::BAD_REQUEST, msg));
        }
      }
      Some("delete") => {
        let msg = "Missing document for update operation.".to_string();
        warn!("{}", msg);
        continue;
      }
      Some("update") => {
        if i >= actions.len() {
          let msg = "Missing document for update operation.".to_string();
          error!("{}", msg);
          return Err((StatusCode::BAD_REQUEST, msg));
        }
        let update_doc = &actions[i];
        i += 1; // Move past the document for the next iteration.

        if let Some(doc) = update_doc.as_object() {
          if doc.contains_key("doc") || doc.contains_key("doc_as_upsert") {
            // Current unsupported
          } else {
            let msg = "Invalid update document format.".to_string();
            error!("{}", msg);
            return Err((StatusCode::BAD_REQUEST, msg));
          }
        } else {
          let msg = "Update operation requires a valid JSON object.".to_string();
          error!("{}", msg);
          return Err((StatusCode::BAD_REQUEST, msg));
        }
      }
      _ => {
        let msg = "Unknown or unsupported action in bulk append request.".to_string();
        error!("{}", msg);
        return Err((StatusCode::BAD_REQUEST, msg));
      }
    }
  }

  // Constructing the bulk response JSON
  let response_json = json!({
      // Check_query_time will not throw an error when timeout is 0. Fine to unwrap().
      "took": check_query_time(0, append_start_time).unwrap(),
      "errors": false,
      "items": items
  });

  // Serialize the JSON response
  response =
    serde_json::to_string(&response_json).expect("Could not convert append response to JSON");

  Ok(response)
}

/// Search logs in CoreDB.
async fn search_logs(
  State(state): State<Arc<AppState>>,
  Query(logs_query): Query<LogsQuery>,
  Path(index_name): Path<String>,
  json_body: String,
) -> Result<String, (StatusCode, String)> {
  debug!(
    "Searching logs with URL query: {:?}, JSON body: {:?}",
    logs_query, json_body
  );

  let start_time = logs_query
    .start_time
    .as_deref()
    .map(|s| s.replace(' ', "+")) // Correct the format from Axum
    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
    .map(|dt| dt.timestamp_millis() as u64)
    .unwrap_or(0); // Default to 0 if None

  let end_time = logs_query
    .end_time
    .as_deref()
    .map(|s| s.replace(' ', "+")) // Correct the format from Axum
    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
    .map(|dt| dt.timestamp_millis() as u64)
    .unwrap_or_else(|| Utc::now().timestamp_millis() as u64); // Default to current time if None

  let results = state
    .coredb
    .search_logs(
      &index_name,
      &logs_query.q.unwrap_or_default(),
      &json_body,
      start_time,
      end_time,
    )
    .await;

  match results {
    Ok(log_messages) => {
      let result_json =
        serde_json::to_string(&log_messages).expect("Could not convert search results to JSON");
      Ok(result_json)
    }
    Err(coredb_error) => {
      match coredb_error {
        CoreDBError::QueryError(ref search_logs_error) => {
          // Handle the error and return an appropriate status code and error message.
          match search_logs_error {
            QueryError::JsonParseError(_) => {
              Err((StatusCode::BAD_REQUEST, coredb_error.to_string()))
            }
            QueryError::IndexNotFoundError(_) => {
              Err((StatusCode::BAD_REQUEST, coredb_error.to_string()))
            }
            QueryError::TimeOutError(_) => {
              Err((StatusCode::INTERNAL_SERVER_ERROR, coredb_error.to_string()))
            }
            QueryError::NoQueryProvided => Err((StatusCode::BAD_REQUEST, coredb_error.to_string())),
            _ => Err((
              StatusCode::INTERNAL_SERVER_ERROR,
              "Internal server error".to_string(),
            )),
          }
        }
        _ => Err((
          StatusCode::INTERNAL_SERVER_ERROR,
          "Internal server error".to_string(),
        )),
      }
    }
  }
}

/// Delete logs by query in CoreDB.
async fn delete_logs_by_query(
  State(state): State<Arc<AppState>>,
  Query(logs_query): Query<LogsQuery>,
  Path(index_name): Path<String>,
  json_body: String,
) -> Result<String, (StatusCode, String)> {
  debug!(
    "Deleting logs with URL query: {:?}, JSON body: {:?}",
    logs_query, json_body
  );

  let start_time = logs_query
    .start_time
    .as_deref()
    .map(|s| s.replace(' ', "+")) // Correct the format from Axum
    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
    .map(|dt| dt.timestamp_millis() as u64)
    .unwrap_or(0); // Default to 0 if None

  let end_time = logs_query
    .end_time
    .as_deref()
    .map(|s| s.replace(' ', "+")) // Correct the format from Axum
    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
    .map(|dt| dt.timestamp_millis() as u64)
    .unwrap_or_else(|| Utc::now().timestamp_millis() as u64); // Default to current time if None

  let results = state
    .coredb
    .delete_logs_by_query(
      &index_name,
      &logs_query.q.unwrap_or_default(),
      &json_body,
      start_time,
      end_time,
    )
    .await;

  match results {
    Ok(log_messages) => {
      // Return the number of deleted messages.
      Ok(log_messages.to_string())
    }
    Err(coredb_error) => {
      match coredb_error {
        CoreDBError::QueryError(ref search_logs_error) => {
          // Handle the error and return an appropriate status code and error message.
          match search_logs_error {
            QueryError::JsonParseError(_) => {
              Err((StatusCode::BAD_REQUEST, coredb_error.to_string()))
            }
            QueryError::IndexNotFoundError(_) => {
              Err((StatusCode::BAD_REQUEST, coredb_error.to_string()))
            }
            QueryError::TimeOutError(_) => {
              Err((StatusCode::INTERNAL_SERVER_ERROR, coredb_error.to_string()))
            }
            QueryError::NoQueryProvided => Err((StatusCode::BAD_REQUEST, coredb_error.to_string())),
            _ => Err((
              StatusCode::INTERNAL_SERVER_ERROR,
              "Internal server error".to_string(),
            )),
          }
        }
        _ => Err((
          StatusCode::INTERNAL_SERVER_ERROR,
          "Internal server error".to_string(),
        )),
      }
    }
  }
}

async fn summarize(
  State(state): State<Arc<AppState>>,
  Query(summarize_query): Query<SummarizeQuery>,
  Path(index_name): Path<String>,
  json_body: String,
) -> Result<String, (StatusCode, String)> {
  debug!(
    "Summarizing logs with URL query: {:?}, JSON body: {:?}",
    summarize_query, json_body
  );

  // Number of log messages to summarize.
  let k = summarize_query.k.unwrap_or(100);

  // Call search_logs and handle errors
  let results = state
    .coredb
    .search_logs(
      &index_name,
      &summarize_query.text,
      &json_body,
      summarize_query.start_time.unwrap_or(0),
      summarize_query
        .end_time
        .unwrap_or(Utc::now().timestamp_millis() as u64),
    )
    .await;

  match results {
    Ok(results) => {
      // Call openai_helper.summarize and handle errors
      match state
        .openai_helper
        .summarize(results.get_messages().as_slice(), k)
      {
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
    Err(coredb_error) => {
      match coredb_error {
        CoreDBError::QueryError(search_logs_error) => {
          // Handle the error and return an appropriate status code and error message.
          match search_logs_error {
            QueryError::JsonParseError(_) => {
              Err((StatusCode::BAD_REQUEST, "Invalid JSON input".to_string()))
            }
            QueryError::TimeOutError(_) => Err((
              StatusCode::INTERNAL_SERVER_ERROR,
              "Internal server error".to_string(),
            )),
            QueryError::NoQueryProvided => {
              Err((StatusCode::BAD_REQUEST, "No query provided".to_string()))
            }
            _ => Err((
              StatusCode::INTERNAL_SERVER_ERROR,
              "Internal server error".to_string(),
            )),
          }
        }
        _ => Err((
          StatusCode::INTERNAL_SERVER_ERROR,
          "Internal server error".to_string(),
        )),
      }
    }
  }
}

/// Search metrics in CoreDB.
async fn search_metrics(
  State(state): State<Arc<AppState>>,
  Query(metrics_query): Query<MetricsQuery>,
  Path(index_name): Path<String>,
  json_body: String,
) -> impl IntoResponse {
  debug!("MAIN: Search metrics for HTTP query: {:?}", metrics_query);

  let query_text: String = if let (Some(label_name), Some(label_value)) = (
    metrics_query.label_name.as_ref(),
    metrics_query.label_value.as_ref(),
  ) {
    format!("{{ {}=\"{}\" }}", label_name, label_value)
  } else {
    "".to_owned()
  };

  let default_text = "".to_string();
  let text_ref = if !query_text.is_empty() {
    &query_text
  } else {
    metrics_query.query.as_ref().unwrap_or(&default_text)
  };

  let timeout = parse_time_range(&metrics_query.timeout.unwrap_or(String::new()))
    .expect("Could not parse timeout parameter");

  let start_time = metrics_query.start.unwrap_or(0_u64);
  let end_time = metrics_query
    .end
    .unwrap_or_else(|| Utc::now().timestamp_millis() as u64);

  let results = state
    .coredb
    .search_metrics(
      &index_name,
      text_ref,
      &json_body,
      timeout.num_seconds() as u64,
      start_time,
      end_time,
    )
    .await;

  match results {
    Ok(mut metrics) => {
      let response = json!({
          "status": "success",
          "data": metrics,
      });
      debug!(
        "Query {:?} completed in {:?} seconds.\n",
        text_ref,
        metrics.get_execution_time()
      );
      (StatusCode::OK, Json(response))
    }
    Err(coredb_error) => {
      let (status_code, error_type, error_message) = match coredb_error {
        CoreDBError::QueryError(search_metrics_error) => match search_metrics_error {
          QueryError::JsonParseError(_) => {
            (StatusCode::BAD_REQUEST, "bad_data", "Invalid JSON input")
          }
          QueryError::SearchMetricsError(_) | QueryError::CoreDBError(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Internal server error",
          ),
          QueryError::NoQueryProvided => (StatusCode::BAD_REQUEST, "bad_data", "No query provided"),
          QueryError::TimeOutError(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "timeout",
            "Query timed out at {}",
          ),
          QueryError::UnsupportedQuery(_) => {
            (StatusCode::BAD_REQUEST, "bad_data", "Unsupported Query")
          }
          _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Internal server error",
          ),
        },
        _ => (
          StatusCode::INTERNAL_SERVER_ERROR,
          "internal_error",
          "Internal server error",
        ),
      };

      let response = json!({
          "status": "error",
          "errorType": error_type,
          "error": error_message,
      });
      (status_code, Json(response))
    }
  }
}

/// Flush the index to disk.
async fn flush(State(state): State<Arc<AppState>>) -> Result<(), (StatusCode, String)> {
  let _ = state.coredb.flush_wal().await;

  // Flush entire index, including the current segment.
  let result = state.coredb.commit(true).await;

  match result {
    Ok(result) => Ok(result),

    // TODO: separate between user triggered errors and internal errors.
    Err(_) => Err((
      StatusCode::INTERNAL_SERVER_ERROR,
      "Internal server error".to_string(),
    )),
  }
}

/// Get index directory used by CoreDB.
async fn get_index_dir(
  State(state): State<Arc<AppState>>,
  Path(index_name): Path<String>,
) -> String {
  state.coredb.get_index_dir(&index_name)
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
  info!("Creating index {}", index_name);

  let result = state.coredb.create_index(&index_name).await;

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
  info!("Deleting index {}", index_name);

  let result = state.coredb.delete_index(&index_name).await;
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
    body::{to_bytes, Body},
    http::{self, Request, StatusCode},
  };
  use chrono::{TimeZone, Utc};
  use serde_json::json;
  use tempdir::TempDir;
  use tokio::time::{sleep, Duration};
  use tower::Service;
  use urlencoding::encode;

  use coredb::metric::metric_point::MetricPoint;
  use coredb::storage_manager::storage::Storage;
  use coredb::storage_manager::storage::StorageType;
  use coredb::utils::config::config_test_logger;
  use coredb::utils::error::QueryError;
  use coredb::utils::io::get_joined_path;
  use coredb::utils::tokenize::FIELD_DELIMITER;
  use coredb::{index_manager::index::Index, request_manager::query_dsl_object::QueryDSLObject};
  use coredb::{log::log_message::LogMessage, segment_manager::search_logs::QueryLogMessage};

  use super::*;

  #[derive(Debug, Deserialize, Serialize)]
  /// Represents an entry in the metric append request.
  struct Metric {
    time: u64,
    metric_name_value: HashMap<String, u64>,
    labels: HashMap<String, String>,
  }

  /// Helper function to create a test configuration.
  fn create_test_config(config_dir_path: &str, index_dir_path: &str, wal_dir_path: &str) {
    config_test_logger();

    // Create a test config in the directory config_dir_path.
    let config_file_path =
      get_joined_path(config_dir_path, Settings::get_default_config_file_name());
    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let default_index_name = format!("default_index_name = \"{}\"\n", "default");
      let wal_dir_path_line = format!("wal_dir_path = \"{}\"\n", wal_dir_path);

      let mut file = File::create(config_file_path).unwrap();

      // Write coredb section.
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file.write_all(wal_dir_path_line.as_bytes()).unwrap();
      file.write_all(default_index_name.as_bytes()).unwrap();
      file.write_all(b"log_messages_threshold = 1000\n").unwrap();
      file.write_all(b"metric_points_threshold = 1000\n").unwrap();
      file
        .write_all(b"uncommitted_segments_threshold = 10\n")
        .unwrap();
      file
        .write_all(b"search_memory_budget_megabytes = 0.4\n")
        .unwrap();
      file.write_all(b"retention_days = 30\n").unwrap();
      file.write_all(b"storage_type = \"local\"\n").unwrap();
      file.write_all(b"target_segment_size = 1024\n").unwrap();
      file
        .write_all(b"target_segment_size_megabytes = 1024\n")
        .unwrap();

      // Write server section.
      file.write_all(b"[server]\n").unwrap();
      file.write_all(b"port = 3000\n").unwrap();
      file.write_all(b"host = \"0.0.0.0\"\n").unwrap();
      file.write_all(b"timestamp_key = \"date\"\n").unwrap();
      file.write_all(b"labels_key = \"labels\"\n").unwrap();
    }
  }

  async fn check_search_logs(
    app: &mut Router,
    index_name: &str,
    config_dir_path: &str,
    search_text: &str,
    query: LogsQuery,
    log_messages_expected: QueryDSLObject,
  ) -> Result<(), CoreDBError> {
    debug!(
      "Calling check_search_logs with index_name {}, config_dir_path {}, search_text {}, query: {:?}, expected log messages length {}",
      index_name, config_dir_path, search_text, query,
      log_messages_expected.get_messages().len(),
    );

    let query_string = format!(
      "q={}",
      encode(&query.q.unwrap_or_else(|| "default_query".to_string())),
    );

    let start_time_query = query
      .start_time
      .as_ref()
      .map(|start_time| format!("&start_time={}", start_time))
      .unwrap_or_default();

    let end_time_query = query
      .end_time
      .as_ref()
      .map(|end_time| format!("&end_time={}", end_time))
      .unwrap_or_default();

    let query_string = format!("{}{}{}", query_string, start_time_query, end_time_query);

    let path = format!("/{}/search_logs?{}", index_name, query_string);

    let request = Request::builder()
      .method(http::Method::GET)
      .uri(path)
      .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
      .body(Body::from(""))
      .unwrap();

    let result = app.call(request).await;

    if let Ok(response) = result {
      assert_eq!(response.status(), StatusCode::OK);
      let body = response.into_body();
      let max_body_size = 10 * 1024 * 1024; // Example: 10MB limit
      let bytes = to_bytes(body, max_body_size)
        .await
        .expect("Failed to read body");
      let body_str = std::str::from_utf8(&bytes).expect("Body was not valid UTF-8");
      debug!("Response content: {}", body_str);

      let log_messages_received: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

      let hits = log_messages_received["hits"]["total"]["value"]
        .as_u64()
        .unwrap();

      assert_eq!(log_messages_expected.get_messages().len() as u64, hits);
    } else if let Err(e) = result {
      error!("Failed to make a call: {:?}", e);
      return Err(CoreDBError::IOError(e.to_string()));
    }

    // Flush the index.
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

    // The i/o operations on Github actions may take long time to get reflected on disk. Hence, we don't run
    // the refresh part os this test while running as part of Github actions.
    if env::var("GITHUB_ACTIONS").is_err() {
      // Sleep for 10 seconds and refresh from the index directory.
      sleep(Duration::from_millis(10000)).await;

      let refreshed_coredb = CoreDB::refresh(index_name, config_dir_path).await?;

      let start_time = query
        .start_time
        .as_deref()
        .map(|s| s.replace(' ', "+")) // Correct the format by using a char for the space
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.timestamp_millis() as u64)
        .unwrap_or(0); // Default to 0 if None

      let end_time = query
        .end_time
        .as_deref()
        .map(|s| s.replace(' ', "+")) // Correct the format by using a char for the space
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.timestamp_millis() as u64)
        .unwrap_or_else(|| Utc::now().timestamp_millis() as u64); // Default to current time if None
                                                                  // Handle errors from search_logs
      let log_messages_result = refreshed_coredb
        .search_logs(index_name, search_text, "", start_time, end_time)
        .await;

      match log_messages_result {
        Ok(log_messages_received) => {
          assert_eq!(
            log_messages_expected.get_messages().len(),
            log_messages_received.get_messages().len()
          );
          assert_eq!(
            log_messages_expected.get_messages(),
            log_messages_received.get_messages()
          );
        }
        Err(search_logs_error) => {
          error!("Error in search_logs: {:?}", search_logs_error);
        }
      }
    }

    Ok(())
  }

  fn check_metric_point_vectors(expected: &[MetricPoint], results: &[MetricPoint]) {
    debug!(
      "Check metric point vectors: Expected {:?}, Results: {:?}",
      expected, results
    );

    // Extract expected and results values into Vec<f64>
    let mut expected_values: Vec<f64> = expected.iter().map(|mp| mp.get_value()).collect();
    let mut results_values: Vec<f64> = results.iter().map(|mp| mp.get_value()).collect();

    // Sort both vectors to ensure they can be compared correctly
    expected_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    results_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // Compare the sorted vectors
    assert_eq!(
      expected_values, results_values,
      "Expected and result values do not match"
    );
  }

  // Check that metrics queries adhere to Prometheus input and output format
  async fn check_time_series(
    app: &mut Router,
    index_name: &str,
    config_dir_path: &str,
    query: MetricsQuery,
    metric_points_expected: Vec<MetricPoint>,
  ) -> Result<(), CoreDBError> {
    debug!("Checking time series with app: {:?}, config_dir_path: {:?}, query: {:?}, and expected metrics points: {:?}", app, config_dir_path, query, metric_points_expected);

    let query_start_time = query
      .start
      .map_or_else(|| "".to_owned(), |value| format!("&start={}", value));
    let query_end_time = query
      .end
      .map_or_else(|| "".to_owned(), |value| format!("&end={}", value));
    let query_text = query.query.as_deref().unwrap_or("");
    let query_encode = format!("query={}", query_text);

    let query_string = format!(
      "query={}{}{}",
      encode(&query_encode),
      query_start_time,
      query_end_time
    );

    let uri = format!("/{}/search_metrics?{}", index_name, query_string);

    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri(uri)
          .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
      .await
      .unwrap();

    let json: Value = serde_json::from_slice(&body).unwrap();

    let status = json["status"]
      .as_str()
      .expect("Expected status field in response");

    match status {
      "success" => {
        let data = json.get("data").expect("Expected data field in response");
        let results_vec = data["result"]
          .as_array()
          .expect("Expected result field to be an array");

        if results_vec.is_empty() {
          // Handle empty result case
        } else {
          for result_item in results_vec {
            let metric_points_received: Vec<MetricPoint> = result_item["values"]
              .as_array()
              .expect("Expected 'values' to be an array in result item")
              .iter()
              .map(|value_pair| {
                let time = value_pair[0].as_u64().expect(
                  "First element of the value pair should be an u64 representing the timestamp",
                );
                let value = value_pair[1].as_f64().expect(
                  "Second element of the value pair should be a f64 representing the value",
                );

                MetricPoint::new(time, value)
              })
              .collect();

            check_metric_point_vectors(
              &metric_points_expected.clone(),
              &metric_points_received.clone(),
            );
          }
        }
      }
      "error" => {
        let error_type = json["errorType"]
          .as_str()
          .expect("Expected errorType field in response");
        let error = json["error"]
          .as_str()
          .expect("Expected error field in response");
        error!("Error response from Prometheus: {} - {}", error_type, error);
        return Err(CoreDBError::QueryError(QueryError::CoreDBError(format!(
          "Error from Prometheus: {} - {}",
          error_type, error
        ))));
      }
      _ => panic!("Unexpected status value in response"),
    }

    // Flush the index.
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

    // The i/o operations on Github actions may take long time to get reflected on disk. Hence, we don't run
    // the refresh part os this test while running as part of Github actions.
    if env::var("GITHUB_ACTIONS").is_err() {
      // Sleep for 10 seconds to simulate delay or wait for a condition.
      sleep(Duration::from_secs(10)).await;

      // Refresh CoreDB instance with the given configuration directory path.
      let refreshed_coredb = CoreDB::refresh(index_name, config_dir_path).await?;

      // Calculate the start and end time for querying metrics post-refresh.
      let start_time = query.start.unwrap_or(0);
      let end_time = query
        .end
        .unwrap_or_else(|| Utc::now().timestamp_millis() as u64);

      // Use the refreshed CoreDB instance to search metrics with updated parameters.
      let mut results = refreshed_coredb
        .search_metrics(
          index_name,
          &query.query.unwrap_or_default(),
          "",
          0,
          start_time,
          end_time,
        )
        .await?;

      // If there are expected metric points and actual results, proceed to validate them.
      if !metric_points_expected.is_empty() && !results.get_vector().is_empty() {
        let mut tmpvec = results.take_vector();
        let metric_points_received = tmpvec[0].get_metric_points();
        check_metric_point_vectors(&metric_points_expected.clone(), metric_points_received);
      } else if metric_points_expected.is_empty() && results.get_vector().is_empty() {
        // If both expected and received results are empty, consider it as a pass.
        info!("Both expected and received metric points are empty, test passes.");
      } else {
        panic!("Mismatch in expected and received results: one is empty and the other is not.");
      }
    }

    Ok(())
  }

  #[tokio::test]
  async fn test_basic_main() -> Result<(), CoreDBError> {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_name = "test_basic_main_test";
    let index_dir = TempDir::new(index_name).unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_dir_path = wal_dir.path().to_str().unwrap();

    create_test_config(config_dir_path, index_dir_path, wal_dir_path);

    // Create the app.
    let (mut app, _, _) = app(config_dir_path).await;

    // Check whether the / works.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::GET)
          .uri("/")
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

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

    debug!("----------Basic main starting Part 1--------");

    // **Part 1**: Test insertion and search of log messages across many indexes
    let num_log_messages = 10;
    for i in 0..num_log_messages {
      let time = Utc::now().timestamp_millis() as u64;

      let mut log = HashMap::new();
      log.insert("date", json!(time));
      log.insert("field12", json!("value1 value2"));
      log.insert("field34", json!("value3 value4"));

      let index_str = format!("{}+{}", index_name, i);

      // Create a single index.
      let response = app
        .call(
          Request::builder()
            .method(http::Method::PUT)
            .uri(&format!("/{}", index_str))
            .body(Body::from(""))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);

      let path = format!("/{}/append_log", index_str);

      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri(path)
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(Body::from(serde_json::to_string(&log).unwrap()))
            .unwrap(),
        )
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::OK);
    }

    for i in 0..num_log_messages {
      let mut log_messages_expected: Vec<QueryLogMessage> = Vec::new();

      let time = Utc::now().timestamp_millis() as u64;

      // Create the expected LogMessage.
      let mut fields = HashMap::new();
      fields.insert("field12".to_owned(), "value1 value2".to_owned());
      fields.insert("field34".to_owned(), "value3 value4".to_owned());
      let text = "value1 value2 value3 value4";
      let message = LogMessage::new_with_fields_and_text(time, &fields, text);

      log_messages_expected.push(QueryLogMessage::new_with_params(0, message));

      // Sort the expected log messages in reverse chronological order.
      log_messages_expected.sort();

      let search_query = &format!("value1 field34{}value4", FIELD_DELIMITER);

      let mut query_object = QueryDSLObject::new();
      query_object.set_messages(log_messages_expected);

      let query = LogsQuery {
        start_time: None,
        end_time: None,
        q: Some(search_query.to_owned()),
      };

      let index_str = format!("{}+{}", index_name, i);

      check_search_logs(
        &mut app,
        &index_str,
        config_dir_path,
        search_query,
        query,
        query_object,
      )
      .await?;
    }

    debug!("----------Basic main starting Part 2--------");

    // **Part 2**: Test insertion and search of log messages in single index

    // Create a single index.
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

    let num_log_messages = 100;
    let mut log_messages_expected: Vec<QueryLogMessage> = Vec::new();
    for i in 0..num_log_messages {
      let time = Utc::now().timestamp_millis() as u64;

      let mut log = HashMap::new();
      log.insert("date", json!(time));
      log.insert("field12", json!("value1 value2"));
      log.insert("field34", json!("value3 value4"));

      let path = format!("/{}/append_log", index_name);

      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri(path)
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
      let message = LogMessage::new_with_fields_and_text(time, &fields, text);

      log_messages_expected.push(QueryLogMessage::new_with_params(i, message));
    }

    // Sort the expected log messages in reverse chronological order.
    log_messages_expected.sort();

    let search_query = &format!("value1 field34{}value4", FIELD_DELIMITER);

    let mut query_object = QueryDSLObject::new();
    query_object.set_messages(log_messages_expected);

    let query = LogsQuery {
      start_time: None,
      end_time: None,
      q: Some(search_query.to_owned()),
    };
    check_search_logs(
      &mut app,
      index_name,
      config_dir_path,
      search_query,
      query,
      query_object,
    )
    .await?;

    debug!("----------Basic main starting Part 3--------");

    // **Part 3**: End time in this query is too old - this should yield 0 results.
    let start_time = Utc
      .timestamp_opt(1, 0) // Creates a DateTime<Utc> at 1 second past the UNIX epoch
      .single()
      .expect("Invalid start timestamp")
      .to_rfc3339(); // Converts the DateTime<Utc> to an RFC 3339 formatted string

    let end_time = Utc
      .timestamp_opt(10, 0) // Creates a DateTime<Utc> at 10 seconds past the UNIX epoch
      .single()
      .expect("Invalid end timestamp")
      .to_rfc3339(); // Converts the DateTime<Utc> to an RFC 3339 formatted string

    let query_too_old = LogsQuery {
      start_time: Some(start_time),
      end_time: Some(end_time),
      q: Some(search_query.to_owned()),
    };

    check_search_logs(
      &mut app,
      index_name,
      config_dir_path,
      search_query,
      query_too_old,
      QueryDSLObject::new(),
    )
    .await?;

    debug!("----------Basic main starting Part 4--------");

    // **Part 4**: Test insertion and search of time series metric points.
    let num_metric_points = 100;
    let mut metric_points_expected = Vec::new();
    let name_for_metric_name_label = "__name__";
    let metric_name = "some_metric_name";

    for i in 0..num_metric_points {
      let time = Utc::now().timestamp_millis() as u64;
      let value = i as f64;
      let metric_point = MetricPoint::new(time, value);

      let json_str = format!("{{\"date\": {}, \"{}\":{}}}", time, metric_name, value);
      metric_points_expected.push(metric_point);

      let path = format!("/{}/append_metric", index_name);

      // Insert the metric.
      let response = app
        .call(
          Request::builder()
            .method(http::Method::POST)
            .uri(path)
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
    let query_text = format!("{{{}=\"{}\"}}", name_for_metric_name_label, metric_name);
    let query = MetricsQuery {
      query: Some(query_text),
      timeout: None,
      label_name: None,
      label_value: None,
      start: None,
      end: None,
    };
    check_time_series(
      &mut app,
      index_name,
      config_dir_path,
      query,
      metric_points_expected,
    )
    .await?;

    // End time in this query is too old - this should yield 0 results.
    // Test legacy Infino syntax here
    let query = MetricsQuery {
      query: None,
      timeout: None,
      label_name: Some(name_for_metric_name_label.to_string()),
      label_value: Some(metric_name.to_string()),
      start: Some(1),
      end: Some(10000),
    };
    check_time_series(&mut app, index_name, config_dir_path, query, Vec::new()).await?;

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

    Ok(())
  }

  #[tokio::test]
  async fn test_body_limit() -> Result<(), CoreDBError> {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_name = "index_test";
    let index_dir = TempDir::new(index_name).unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_dir_path = wal_dir.path().to_str().unwrap();

    create_test_config(config_dir_path, index_dir_path, wal_dir_path);

    // Create the app.
    let (mut app, _, _) = app(config_dir_path).await;

    // We need a http body of >2MB and <5MB for this test.
    let num_log_messages = 15 * 1024;
    let mut logs = Vec::new();
    let value = json!("value1 value2 value3 value4 value5 value6 value7 value8 value9");
    for i in 0..num_log_messages {
      let mut log = HashMap::new();
      log.insert("date", json!(i));
      log.insert("field1", value.clone());
      log.insert("field2", value.clone());
      log.insert("field3", value.clone());
      logs.push(log);
    }

    let body = serde_json::to_string(logs.as_slice()).unwrap();
    // Make sure that the body is >2MB, but <5MB
    let body_len = body.len();
    assert!(body_len > 2 * 1024 * 1024 && body_len < 5 * 1024 * 1024);

    // Create an index.
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

    let path = format!("/{}/append_log", index_name);

    // Send a large request.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::POST)
          .uri(path)
          .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
          .body(Body::from(body))
          .unwrap(),
      )
      .await
      .unwrap();
    let status = response.status();

    // We may return OK or TOO_MANY_REQUESTS as the number of uncommitted segments are too high.
    // However, we should not return other error codes such as CONTENT_TOO_LARGE.
    assert!(status == StatusCode::OK || status == StatusCode::TOO_MANY_REQUESTS);

    Ok(())
  }

  /// Write test to test Create and Delete index APIs.
  #[tokio::test]
  async fn test_create_delete_index() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_name = "index_test";
    let index_dir = TempDir::new(index_name).unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_dir_path = wal_dir.path().to_str().unwrap();
    let storage = Storage::new(&StorageType::Local)
      .await
      .expect("Could not create storage");

    create_test_config(config_dir_path, index_dir_path, wal_dir_path);

    // Create the app.
    let (mut app, _, _) = app(config_dir_path).await;

    // Create an index.
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

    // Check whether the metadata file in the index directory exists.
    let index_dir_path = get_joined_path(index_dir_path, index_name);
    let metadata_file_path = &format!("{}/{}", index_dir_path, Index::get_metadata_file_name());
    assert!(storage.check_path_exists(metadata_file_path).await);

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
    assert!(!storage.check_path_exists(metadata_file_path).await);
  }

  /// Write test to test Create and Delete index APIs.
  #[tokio::test]
  async fn test_create_delete_multiple_indexes() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_name = "index_test";
    let index_dir = TempDir::new(index_name).unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_dir_path = wal_dir.path().to_str().unwrap();
    let storage = Storage::new(&StorageType::Local)
      .await
      .expect("Could not create storage");

    create_test_config(config_dir_path, index_dir_path, wal_dir_path);

    let mut index_dirs = Vec::<String>::new();

    // Create the app.
    let (mut app, _, _) = app(config_dir_path).await;

    for i in 0..9 {
      let index_name = format!("index_test+{}", i);
      index_dirs.push(index_name.to_string());

      // Create an index.
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

      // Check whether the metadata file in the index directory exists.
      let joined_index_dir_path = get_joined_path(index_dir_path, &index_name);
      let metadata_file_path = &format!(
        "{}/{}",
        joined_index_dir_path,
        Index::get_metadata_file_name()
      );
      assert!(storage.check_path_exists(metadata_file_path).await);
    }

    // Delete the index.
    let response = app
      .call(
        Request::builder()
          .method(http::Method::DELETE)
          .uri("/*")
          .body(Body::from(""))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Check whether the metadata file in the index directories exists.
    // None of them should.
    for index_dir in index_dirs.iter() {
      let metadata_file_path = &format!("{}/{}", index_dir, Index::get_metadata_file_name());

      // Check whether the index directory exists.
      assert!(!storage.check_path_exists(metadata_file_path).await);
    }
  }

  #[tokio::test]
  async fn test_bulk_operation() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_name = "bulk_test";
    let index_dir = TempDir::new(index_name).unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_dir_path = wal_dir.path().to_str().unwrap();

    create_test_config(config_dir_path, index_dir_path, wal_dir_path);

    let (mut app, _, _) = app(config_dir_path).await;

    // Create a single index.
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

    // *** Test newline delimited inserts
    let bulk_request = format!(
      "{{\"index\": {{\"_index\": \"{}\", \"_id\": \"1\"}}}}\r\n{{\"title\": \"Document 1\", \"content\": \"Example content 1\"}}\r\n{{\"delete\": {{\"_index\": \"{}\", \"_id\": \"2\"}}}}\r\n{{\"create\": {{\"_index\": \"{}\", \"_id\": \"3\"}}}}\r\n{{\"title\": \"Document 3\", \"content\": \"Example content 3\"}}\r\n{{\"update\": {{\"_index\": \"{}\", \"_id\": \"1\"}}}}\r\n{{\"doc\": {{\"content\": \"Updated content 1\"}}}}",
      index_name, index_name, index_name, index_name
    );

    let body = bulk_request;
    let path = format!("/{}/bulk", index_name);

    let response = app
      .call(
        Request::builder()
          .method(http::Method::POST)
          .uri(&path)
          .header("Content-Type", "application/x-ndjson")
          .body(Body::from(body))
          .unwrap(),
      )
      .await
      .unwrap();

    // Verify the response status code
    assert_eq!(response.status(), StatusCode::OK);

    // Read the response body and verify the expected outcome
    let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
      .await
      .unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&response_body).unwrap();

    // Make sure documents are inserted correctly.
    assert_eq!(response_json["errors"], false);

    // *** Test comma delimited inserts
    let bulk_request = format!(
      "{{\"index\": {{\"_index\": \"{}\", \"_id\": \"5\"}}}},{{\"title\": \"Document 5\", \"content\": \"Example content 5\"}},{{\"delete\": {{\"_index\": \"{}\", \"_id\": \"6\"}}}},{{\"create\": {{\"_index\": \"{}\", \"_id\": \"7\"}}}},{{\"title\": \"Document 7\", \"content\": \"Example content 7\"}}\r\n{{\"update\": {{\"_index\": \"{}\", \"_id\": \"8\"}}}}\r\n{{\"doc\": {{\"content\": \"Updated content 8\"}}}}",
      index_name, index_name, index_name, index_name
    );

    let body = bulk_request;
    let path = format!("/{}/bulk", index_name);

    let response = app
      .call(
        Request::builder()
          .method(http::Method::POST)
          .uri(&path)
          .header("Content-Type", "application/x-ndjson")
          .body(Body::from(body))
          .unwrap(),
      )
      .await
      .unwrap();

    // Verify the response status code
    assert_eq!(response.status(), StatusCode::OK);

    // Read the response body and verify the expected outcome
    let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
      .await
      .unwrap();
    let response_json: serde_json::Value = serde_json::from_slice(&response_body).unwrap();

    // Make sure documents are inserted correctly.
    assert_eq!(response_json["errors"], false);

    // *** Test searches against the inserts
    let query_dsl_request = "{\"query\":{\"match\":{\"test_field\":{\"query\":\"test_value\",\"operator\":\"OR\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}}";
    let path = format!("/{}/search_logs", index_name);

    let request = Request::builder()
      .method(http::Method::GET)
      .uri(path)
      .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
      .body(Body::from(query_dsl_request))
      .unwrap();

    let result = app.call(request).await;

    let response = result.expect("Failed to make a call");
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body();
    let max_body_size = 10 * 1024 * 1024;
    let bytes = to_bytes(body, max_body_size)
      .await
      .expect("Failed to read body");

    let body_str = std::str::from_utf8(&bytes).expect("Body was not valid UTF-8");
    debug!("Response content: {}", body_str);
  }
}

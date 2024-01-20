//! CoreDB is a telemetry database.
//!
//! It uses time-sharded segments and compressed blocks for data storage
//! and implements indexing for fast data retrieval.

pub(crate) mod index_manager;
pub mod log;
pub mod metric;
pub(crate) mod request_manager;
pub(crate) mod segment_manager;
pub(crate) mod storage_manager;
pub mod utils;

use std::collections::HashMap;

use ::log::{debug, info};
use dashmap::DashMap;
use utils::error::SearchLogsError;

use crate::index_manager::index::Index;
use crate::log::log_message::LogMessage;
use crate::metric::metric_point::MetricPoint;
use crate::utils::config::Settings;
use crate::utils::error::CoreDBError;

/// Database for storing telemetry data, mapping string keys to index objects.
pub struct CoreDB {
  index_map: DashMap<String, Index>,
  settings: Settings,
}

impl CoreDB {
  /// Create a new CoreDB at the directory path specified in the config,
  /// creating a default index if none already exists.
  pub async fn new(config_dir_path: &str) -> Result<Self, CoreDBError> {
    let result = Settings::new(config_dir_path);

    match result {
      Ok(settings) => {
        let coredb_settings = settings.get_coredb_settings();
        let index_dir_path = &coredb_settings.get_index_dir_path();
        let default_index_name = coredb_settings.get_default_index_name();
        let segment_size_threshold_bytes = coredb_settings.get_segment_size_threshold_bytes();
        let search_memory_budget_bytes = coredb_settings.get_search_memory_budget_bytes();
        let storage_type = coredb_settings.get_storage_type();

        // Check if index_dir_path exist and has some directories in it
        let index_map = DashMap::new();
        let index_dir = std::fs::read_dir(index_dir_path);
        match index_dir {
          Ok(_) => {
            info!(
              "Index directory {} already exists. Loading existing index",
              index_dir_path
            );

            let directories = std::fs::read_dir(index_dir_path).unwrap();

            if directories.count() == 0 {
              info!(
                "Index directory {} does not exist. Creating it.",
                index_dir_path
              );
              let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
              let index = Index::new_with_threshold_params(
                storage_type,
                &default_index_dir_path,
                segment_size_threshold_bytes,
                search_memory_budget_bytes,
              )
              .await?;
              index_map.insert(default_index_name.to_string(), index);
            } else {
              for entry in std::fs::read_dir(index_dir_path).unwrap() {
                let entry = entry.unwrap();
                let index_name = entry.file_name().into_string().unwrap();
                let full_index_path_name = format!("{}/{}", index_dir_path, index_name);
                let index = Index::refresh(
                  storage_type,
                  &full_index_path_name,
                  search_memory_budget_bytes,
                )
                .await?;
                index_map.insert(index_name, index);
              }
            }
          }
          Err(_) => {
            info!(
              "Index directory {} does not exist. Creating it.",
              index_dir_path
            );
            let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
            let index = Index::new_with_threshold_params(
              storage_type,
              &default_index_dir_path,
              segment_size_threshold_bytes,
              search_memory_budget_bytes,
            )
            .await?;
            index_map.insert(default_index_name.to_string(), index);
          }
        }
        let coredb = CoreDB {
          index_map,
          settings,
        };

        Ok(coredb)
      }
      Err(e) => {
        let error = CoreDBError::InvalidConfiguration(e.to_string());
        Err(error)
      }
    }
  }

  /// Append a log message.
  pub fn append_log_message(&self, time: u64, fields: &HashMap<String, String>, text: &str) {
    debug!(
      "Appending log message in CoreDB: time {}, fields {:?}, text {}",
      time, fields, text
    );
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .append_log_message(time, fields, text);
  }

  /// Append a metric point.
  pub fn append_metric_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) {
    debug!(
      "Appending metric point in CoreDB: time {}, value {}, labels {:?}, name {}",
      time, value, labels, metric_name
    );
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .append_metric_point(metric_name, labels, time, value);
  }

  /// Search the log messages for given query and range.
  pub async fn search_logs(
    &self,
    query: &str,
    json_body: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<LogMessage>, CoreDBError> {
    let index = self
      .index_map
      .get(self.get_default_index_name())
      .ok_or(SearchLogsError::NoQueryProvided)?;

    index
      .value()
      .search_logs(query, json_body, range_start_time, range_end_time)
      .await
  }

  /// Get the metric points for given label and range.
  pub async fn get_metrics(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<MetricPoint>, CoreDBError> {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .get_metrics(label_name, label_value, range_start_time, range_end_time)
      .await
  }

  /// Commit the index to disk. If the flag sync_after_commit is set to true,
  /// the directory is sync'd immediately instead of relying on the OS to do so,
  /// hence this flag is usually set to true only in tests.
  pub async fn commit(&self, sync_after_commit: bool) -> Result<(), CoreDBError> {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .commit(sync_after_commit)
      .await
  }

  /// Refresh the default index from the given directory path.
  // TODO: what to do if there are multiple indices?
  pub async fn refresh(config_dir_path: &str) -> Self {
    // Read the settings and the index directory path.
    let settings = Settings::new(config_dir_path).unwrap();
    let index_dir_path = settings.get_coredb_settings().get_index_dir_path();
    let default_index_name = settings.get_coredb_settings().get_default_index_name();
    let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
    let search_memory_budget_bytes = settings
      .get_coredb_settings()
      .get_search_memory_budget_bytes();
    let storage_type = settings.get_coredb_settings().get_storage_type();

    // Refresh the index.
    let index = Index::refresh(
      storage_type,
      &default_index_dir_path,
      search_memory_budget_bytes,
    )
    .await
    .unwrap();

    let index_map = DashMap::new();
    index_map.insert(default_index_name.to_string(), index);

    CoreDB {
      index_map,
      settings,
    }
  }

  /// Get the directory where the index is stored.
  pub fn get_index_dir(&self) -> String {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .get_index_dir()
  }

  /// Get the settings for this CoreDB.
  pub fn get_settings(&self) -> &Settings {
    &self.settings
  }

  /// Create a new index.
  pub async fn create_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    let index_dir_path = self.settings.get_coredb_settings().get_index_dir_path();
    let segment_size_threshold_bytes = self
      .settings
      .get_coredb_settings()
      .get_segment_size_threshold_bytes();
    let search_memory_budget_bytes = self
      .settings
      .get_coredb_settings()
      .get_search_memory_budget_bytes();
    let storage_type = self.settings.get_coredb_settings().get_storage_type();

    let index_dir_path = format!("{}/{}", index_dir_path, index_name);
    let index = Index::new_with_threshold_params(
      storage_type,
      &index_dir_path,
      segment_size_threshold_bytes,
      search_memory_budget_bytes,
    )
    .await?;

    self.index_map.insert(index_name.to_string(), index);
    Ok(())
  }

  /// Delete an index.
  pub fn delete_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    let index = self.index_map.remove(index_name);
    match index {
      Some(index) => {
        index.1.delete();
        Ok(())
      }
      None => {
        let error = CoreDBError::IndexNotFound(index_name.to_string());
        Err(error)
      }
    }
  }

  pub fn get_default_index_name(&self) -> &str {
    self.settings.get_coredb_settings().get_default_index_name()
  }
}

#[cfg(test)]
mod tests {
  use std::fs::File;
  use std::io::Write;

  use chrono::Utc;
  use tempdir::TempDir;

  use crate::utils::config::CoreDBSettings;
  use crate::utils::io::get_joined_path;

  use super::*;

  /// Helper function to create a test configuration.
  fn create_test_config(config_dir_path: &str, index_dir_path: &str) {
    // Create a test config in the directory config_dir_path.
    let config_file_path = get_joined_path(
      config_dir_path,
      CoreDBSettings::get_default_config_file_name(),
    );

    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let default_index_name_line = format!("default_index_name = \"{}\"\n", ".default");

      let mut file = File::create(config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file.write_all(default_index_name_line.as_bytes()).unwrap();
      file
        .write_all(b"segment_size_threshold_megabytes = 0.1\n")
        .unwrap();
      file.write_all(b"memory_budget_megabytes = 0.4\n").unwrap();
    }
  }

  #[tokio::test]
  async fn test_basic() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    create_test_config(config_dir_path, index_dir_path);
    println!("Config dir path {}", config_dir_path);

    // Create a new coredb instance.
    let coredb = CoreDB::new(config_dir_path)
      .await
      .expect("Could not create coredb");

    let start = Utc::now().timestamp_millis() as u64;

    // Add a few log messages.
    coredb.append_log_message(
      Utc::now().timestamp_millis() as u64,
      &HashMap::new(),
      "log message 1",
    );
    coredb.append_log_message(
      Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
      &HashMap::new(),
      "log message 2",
    );

    // Add a few metric points.
    coredb.append_metric_point(
      "some_metric",
      &HashMap::new(),
      Utc::now().timestamp_millis() as u64,
      1.0,
    );
    coredb.append_metric_point(
      "some_metric",
      &HashMap::new(),
      Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
      2.0,
    );

    coredb.commit(true).await.expect("Could not commit");
    let coredb = CoreDB::refresh(config_dir_path).await;

    let end = Utc::now().timestamp_millis() as u64;

    // Search for log messages. The order of results should be reverse chronological order.
    if let Err(err) = coredb.search_logs("message", "", start, end).await {
      eprintln!("Error in search_logs: {:?}", err);
    } else {
      let results = coredb
        .search_logs("message", "", start, end)
        .await
        .expect("Error in search_logs");
      assert_eq!(results.first().unwrap().get_text(), "log message 2");
      assert_eq!(results.get(1).unwrap().get_text(), "log message 1");
    }

    // Search for metric points.
    let results = coredb
      .get_metrics("__name__", "some_metric", start, end)
      .await
      .expect("Error in get_metrics");
    assert_eq!(results.first().unwrap().get_value(), 1.0);
    assert_eq!(results.get(1).unwrap().get_value(), 2.0);
  }
}

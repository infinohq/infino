pub(crate) mod index_manager;
pub mod log;
pub(crate) mod segment_manager;
pub mod ts;
pub mod utils;

use std::collections::HashMap;

use ::log::{debug, info};
use dashmap::DashMap;

use crate::index_manager::index::Index;
use crate::log::log_message::LogMessage;
use crate::ts::metric_point::MetricPoint;
use crate::utils::config::Settings;
use crate::utils::error::CoreDBError;

/// Database for storing time series (ts) and logs (l).
pub struct CoreDB {
  index_map: DashMap<String, Index>,
  settings: Settings,
}

impl CoreDB {
  /// Create a new CoreDB at the directory path specified in the config.
  /// Default index is always created with coredb
  pub fn new(config_dir_path: &str) -> Result<Self, CoreDBError> {
    let result = Settings::new(config_dir_path);

    match result {
      Ok(settings) => {
        let coredb_settings = settings.get_coredb_settings();
        let index_dir_path = coredb_settings.get_index_dir_path();
        let default_index_name = coredb_settings.get_default_index_name();
        let num_log_messages_threshold = coredb_settings.get_num_log_messages_threshold();
        let num_metric_points_threshold = coredb_settings.get_num_metric_points_threshold();

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
                &default_index_dir_path,
                num_log_messages_threshold,
                num_metric_points_threshold,
              )?;
              index_map.insert(default_index_name.to_string(), index);
            } else {
              for entry in std::fs::read_dir(index_dir_path).unwrap() {
                let entry = entry.unwrap();
                let index_name = entry.file_name().into_string().unwrap();
                let full_index_path_name = format!("{}/{}", index_dir_path, index_name);
                let index = Index::refresh(&full_index_path_name)?;
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
              &default_index_dir_path,
              num_log_messages_threshold,
              num_metric_points_threshold,
            )?;
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

  /// Append a data point.
  pub fn append_metric_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .append_metric_point(metric_name, labels, time, value);
  }

  /// Search log messages for given query and range.
  pub fn search(&self, query: &str, range_start_time: u64, range_end_time: u64) -> Vec<LogMessage> {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .search(query, range_start_time, range_end_time)
  }

  /// Get the time series for given label and range.
  pub fn get_time_series(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<MetricPoint> {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .get_time_series(label_name, label_value, range_start_time, range_end_time)
  }

  /// Commit the index.
  /// If the flag sync_after_commit is set to true, the directory is sync-ed immediately instead of relying on the OS to do so,
  /// hence this flag is usually set to true only in tests.
  pub fn commit(&self, sync_after_commit: bool) {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .commit(sync_after_commit);
  }

  /// Refresh the default index from the given directory path.
  /// TODO: what to do if there are multiple indices?
  pub fn refresh(config_dir_path: &str) -> Self {
    // Read the settings and the index directory path.
    let settings = Settings::new(config_dir_path).unwrap();
    let index_dir_path = settings.get_coredb_settings().get_index_dir_path();
    let default_index_name = settings.get_coredb_settings().get_default_index_name();
    let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);

    // Refresh the index.
    let index = Index::refresh(&default_index_dir_path).unwrap();

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

  /// Get the settings for this coredb.
  pub fn get_settings(&self) -> &Settings {
    &self.settings
  }

  /// Function to create new index with given name
  pub fn create_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    let index_dir_path = self.settings.get_coredb_settings().get_index_dir_path();
    let num_log_messages_threshold = self
      .settings
      .get_coredb_settings()
      .get_num_log_messages_threshold();
    let num_metric_points_threshold = self
      .settings
      .get_coredb_settings()
      .get_num_metric_points_threshold();

    let index_dir_path = format!("{}/{}", index_dir_path, index_name);
    let index = Index::new_with_threshold_params(
      &index_dir_path,
      num_log_messages_threshold,
      num_metric_points_threshold,
    )?;

    self.index_map.insert(index_name.to_string(), index);
    Ok(())
  }

  /// Function to delete index with given name
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

  use crate::utils::config::TsldbSettings;
  use crate::utils::io::get_joined_path;

  use super::*;

  /// Helper function to create a test configuration.
  fn create_test_config(config_dir_path: &str, index_dir_path: &str) {
    // Create a test config in the directory config_dir_path.
    let config_file_path = get_joined_path(
      config_dir_path,
      TsldbSettings::get_default_config_file_name(),
    );

    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let default_index_name_line = format!("default_index_name = \"{}\"\n", ".default");

      let mut file = File::create(&config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file.write_all(default_index_name_line.as_bytes()).unwrap();
      file
        .write_all(b"num_log_messages_threshold = 1000\n")
        .unwrap();
      file
        .write_all(b"num_metric_points_threshold = 10000\n")
        .unwrap();
    }
  }

  #[test]
  fn test_basic() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    create_test_config(config_dir_path, index_dir_path);
    println!("Config dir path {}", config_dir_path);

    // Create a new coredb instance.
    let coredb = Tsldb::new(config_dir_path).expect("Could not create coredb");

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

    // Add a few data points.
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

    coredb.commit(true);
    let coredb = Tsldb::refresh(config_dir_path);

    let end = Utc::now().timestamp_millis() as u64;

    // Search for log messages. The order of results should be reverse chronological order.
    let results = coredb.search("message", start, end);
    assert_eq!(results.get(0).unwrap().get_text(), "log message 2");
    assert_eq!(results.get(1).unwrap().get_text(), "log message 1");

    // Search for data points.
    let results = coredb.get_time_series(&"__name__", &"some_metric", start, end);
    assert_eq!(results.get(0).unwrap().get_value(), 1.0);
    assert_eq!(results.get(1).unwrap().get_value(), 2.0);
  }
}

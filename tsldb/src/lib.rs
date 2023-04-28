pub(crate) mod index_manager;
pub mod log;
pub(crate) mod segment_manager;
pub mod ts;
pub mod utils;

use ::log::debug;
use std::collections::HashMap;

use crate::index_manager::index::Index;
use crate::log::log_message::LogMessage;
use crate::ts::data_point::DataPoint;
use crate::utils::config::Settings;
use crate::utils::error::TsldbError;

/// Database for storing time series (ts) and logs (l).
pub struct Tsldb {
  /// Index storing the time series and log data.
  index: Index,
  settings: Settings,
}

impl Tsldb {
  /// Create a new tsldb at the directory path specified in the config.
  pub fn new(config_dir_path: &str) -> Result<Self, TsldbError> {
    let result = Settings::new(config_dir_path);

    match result {
      Ok(settings) => {
        let tsldb_settings = settings.get_tsldb_settings();
        let index_dir_path = tsldb_settings.get_index_dir_path();
        let num_log_messages_threshold = tsldb_settings.get_num_log_messages_threshold();
        let num_data_points_threshold = tsldb_settings.get_num_data_points_threshold();

        let index = Index::new_with_threshold_params(
          index_dir_path,
          num_log_messages_threshold,
          num_data_points_threshold,
        )?;

        let tsldb = Tsldb { index, settings };
        Ok(tsldb)
      }
      Err(e) => {
        let error = TsldbError::InvalidConfiguration(e.to_string());
        Err(error)
      }
    }
  }

  /// Append a log message.
  pub fn append_log_message(&self, time: u64, fields: &HashMap<String, String>, text: &str) {
    debug!(
      "Appending log message in tsldb: time {}, fields {:?}, text {}",
      time, fields, text
    );
    self.index.append_log_message(time, fields, text);
  }

  /// Append a data point.
  pub fn append_data_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) {
    self
      .index
      .append_data_point(metric_name, labels, time, value);
  }

  /// Search log messages for given query and range.
  pub fn search(&self, query: &str, range_start_time: u64, range_end_time: u64) -> Vec<LogMessage> {
    self.index.search(query, range_start_time, range_end_time)
  }

  /// Get the time series for given label and range.
  pub fn get_time_series(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<DataPoint> {
    self
      .index
      .get_time_series(label_name, label_value, range_start_time, range_end_time)
  }

  /// Commit the index.
  pub fn commit(&self, sync_after_commit: bool) {
    self.index.commit(sync_after_commit);
  }

  /// Refresh the index from the given directory path.
  pub fn refresh(config_dir_path: &str) -> Self {
    // Read the settings and the index directory path.
    let settings = Settings::new(config_dir_path).unwrap();
    let index_dir_path = settings.get_tsldb_settings().get_index_dir_path();

    // Refresh the index.
    let index = Index::refresh(index_dir_path).unwrap();

    Tsldb { index, settings }
  }

  /// Get the directory where the index is stored.
  pub fn get_index_dir(&self) -> String {
    self.index.get_index_dir()
  }

  /// Get the settings for this tsldb.
  pub fn get_settings(&self) -> &Settings {
    &self.settings
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

      let mut file = File::create(&config_file_path).unwrap();
      file.write_all(b"[tsldb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file
        .write_all(b"num_log_messages_threshold = 1000\n")
        .unwrap();
      file
        .write_all(b"num_data_points_threshold = 10000\n")
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

    // Create a new tsldb instance.
    let tsldb = Tsldb::new(config_dir_path).expect("Could not create tsldb");

    let start = Utc::now().timestamp_millis() as u64;

    // Add a few log messages.
    tsldb.append_log_message(
      Utc::now().timestamp_millis() as u64,
      &HashMap::new(),
      "log message 1",
    );
    tsldb.append_log_message(
      Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
      &HashMap::new(),
      "log message 2",
    );

    // Add a few data points.
    tsldb.append_data_point(
      "some_metric",
      &HashMap::new(),
      Utc::now().timestamp_millis() as u64,
      1.0,
    );
    tsldb.append_data_point(
      "some_metric",
      &HashMap::new(),
      Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
      2.0,
    );

    tsldb.commit(true);
    let tsldb = Tsldb::refresh(config_dir_path);

    let end = Utc::now().timestamp_millis() as u64;

    // Search for log messages.
    let results = tsldb.search("message", start, end);
    assert_eq!(results.get(0).unwrap().get_text(), "log message 1");
    assert_eq!(results.get(1).unwrap().get_text(), "log message 2");

    // Search for data points.
    let results = tsldb.get_time_series(&"__name__", &"some_metric", start, end);
    assert_eq!(results.get(0).unwrap().get_value(), 1.0);
    assert_eq!(results.get(1).unwrap().get_value(), 2.0);
  }
}

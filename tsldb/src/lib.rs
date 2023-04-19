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

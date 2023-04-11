pub(crate) mod index_manager;
pub mod log;
pub(crate) mod segment_manager;
pub mod ts;
pub(crate) mod utils;

use std::collections::HashMap;

use utils::error::TsldbError;

use crate::index_manager::index::Index;
use crate::log::log_message::LogMessage;
use crate::ts::data_point::DataPoint;

/// Database for storing time series (ts) and logs (l).
pub struct Tsldb {
  /// Index storing the time series and log data.
  index: Index,
}

impl Tsldb {
  /// Create a new tsldb at the given directory path.
  pub fn new(index_dir_path: &str) -> Result<Self, TsldbError> {
    match Index::new(index_dir_path) {
      Ok(index) => {
         Ok(Tsldb { index })
      }
      Err(err) => {
        Err(err)
      }
    }
  }

  /// Create a new tsldb at the given directory path, with specified parameters for
  /// the number of log messages and data points after which a new segment should be created.
  pub fn new_with_max_params(
    index_dir_path: &str,
    max_log_messages: u32,
    max_data_points: u32,
  ) -> Result<Self, TsldbError> {
    match Index::new_with_max_params(index_dir_path, max_log_messages, max_data_points) {
      Ok(index) => { 
        Ok(Tsldb {index})
      }
      Err(err) => {
        Err(err)
      }
    }
  }

  /// Append a log message.
  pub fn append_log_message(&self, time: u64, message: &str) {
    self.index.append_log_message(time, message);
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
  pub fn refresh(index_dir_path: &str) -> Self {
    let index = Index::refresh(index_dir_path).unwrap();
    Tsldb { index }
  }

  /// Get the directory where the index is stored.
  pub fn get_index_dir(&self) -> String {
    self.index.get_index_dir()
  }
}

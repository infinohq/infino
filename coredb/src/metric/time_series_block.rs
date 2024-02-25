// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use log::debug;
use serde::{Deserialize, Serialize};

use crate::metric::constants::BLOCK_SIZE_FOR_TIME_SERIES;
use crate::metric::metric_point::MetricPoint;
use crate::metric::metricutils::decompress_numeric_vector;
use crate::metric::time_series_block_compressed::TimeSeriesBlockCompressed;
use crate::utils::error::CoreDBError;

/// Represents a time series block.
#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSeriesBlock {
  /// Vector of metric points, wrapped in a RwLock.
  metric_points: Vec<MetricPoint>,
}

impl TimeSeriesBlock {
  /// Create a new time series block.
  pub fn new() -> Self {
    // We allocate a fixed capacity at the beginning, so that the vector doesn't get dynamically reallocated during appends.
    let metric_points_vec: Vec<MetricPoint> = Vec::with_capacity(BLOCK_SIZE_FOR_TIME_SERIES);
    let metric_points_lock = metric_points_vec;

    Self {
      metric_points: metric_points_lock,
    }
  }

  /// Create a time series block from the given vector of metric points.
  pub fn new_with_metric_points(metric_points_vec: Vec<MetricPoint>) -> Self {
    Self {
      metric_points: metric_points_vec,
    }
  }

  /// Check whether this time series block is empty.
  pub fn is_empty(&self) -> bool {
    self.metric_points.is_empty()
  }

  /// Get the vector of metric points, wrapped in RwLock.
  pub fn get_metric_points(&self) -> &Vec<MetricPoint> {
    &self.metric_points
  }

  /// Append a new metric point with given time and value.
  pub fn append(&mut self, time: u64, value: f64) -> Result<(), CoreDBError> {
    if self.metric_points.len() >= BLOCK_SIZE_FOR_TIME_SERIES {
      debug!("Capacity full error while inserting time/value {}/{}. Typically a new block will now be created.",
             time, value);
      return Err(CoreDBError::CapacityFull(BLOCK_SIZE_FOR_TIME_SERIES));
    }

    let dp = MetricPoint::new(time, value);

    // Always keep metric_points vector sorted (by time), as the compression needs it to be sorted.
    if self.metric_points.is_empty() || self.metric_points.last().unwrap() < &dp {
      self.metric_points.push(dp);
    } else {
      let pos = self.metric_points.binary_search(&dp).unwrap_or_else(|e| e);
      self.metric_points.insert(pos, dp);
    }

    Ok(())
  }

  /// Get the metric points in the specified range (both range_start_time and range_end_time inclusive).
  pub fn get_metric_points_in_range(
    &self,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<MetricPoint> {
    let mut retval = Vec::new();

    for dp in self.metric_points.as_slice() {
      let time = dp.get_time();

      if time >= range_start_time && time <= range_end_time {
        retval.push((*dp).clone());
      }
    }

    retval
  }

  /// Get the number of metric points in this time series block.
  #[cfg(test)]
  pub fn len(&self) -> usize {
    self.metric_points.len()
  }
}

impl PartialEq for TimeSeriesBlock {
  fn eq(&self, other: &Self) -> bool {
    self.metric_points == other.metric_points
  }
}

impl Eq for TimeSeriesBlock {}

impl TryFrom<&TimeSeriesBlockCompressed> for TimeSeriesBlock {
  type Error = CoreDBError;

  // Decompress a compressed time series block.
  fn try_from(
    time_series_block_compressed: &TimeSeriesBlockCompressed,
  ) -> Result<Self, Self::Error> {
    let metric_points_compressed = time_series_block_compressed.get_metric_points_compressed();
    let metric_points_decompressed = decompress_numeric_vector(metric_points_compressed).unwrap();
    let time_series_block = TimeSeriesBlock::new_with_metric_points(metric_points_decompressed);

    Ok(time_series_block)
  }
}

impl Default for TimeSeriesBlock {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use rand::Rng;

  use super::*;
  use crate::utils::sync::{is_sync_send, thread, Arc, RwLock};

  #[test]
  fn test_new_time_series_block() {
    // Check that time series block implements sync.
    is_sync_send::<TimeSeriesBlock>();

    // Check that a new time series block is empty.
    let tsb = TimeSeriesBlock::new();
    assert_eq!(tsb.metric_points.len(), 0);
  }

  #[test]
  fn test_default_time_series_block() {
    // Check that a default time series block is empty.
    let tsb = TimeSeriesBlock::default();
    assert_eq!(tsb.metric_points.len(), 0);
  }

  #[test]
  fn test_single_append() {
    // After appending a single value, check that the time series block has that value.
    let mut tsb = TimeSeriesBlock::new();
    tsb.append(1000, 1.0).unwrap();
    assert_eq!(tsb.len(), 1);
    assert_eq!(tsb.get_metric_points().first().unwrap().get_time(), 1000);
    assert_eq!(tsb.get_metric_points().first().unwrap().get_value(), 1.0);
  }

  #[test]
  fn test_block_size_appends() {
    let mut tsb = TimeSeriesBlock::new();
    let mut expected: Vec<MetricPoint> = Vec::new();

    // Append BLOCK_SIZE_FOR_TIME_SERIES values, and check that the time series block has those values.
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      tsb.append(i as u64, i as f64).unwrap();
      expected.push(MetricPoint::new(i as u64, i as f64));
    }
    assert_eq!(*tsb.get_metric_points(), expected);
  }

  #[test]
  fn test_metric_points_in_range() {
    let mut tsb = TimeSeriesBlock::new();
    tsb.append(100, 1.0).unwrap();
    tsb.append(200, 1.0).unwrap();
    tsb.append(300, 1.0).unwrap();

    assert_eq!(tsb.get_metric_points_in_range(50, 70).len(), 0);
    assert_eq!(tsb.get_metric_points_in_range(50, 150).len(), 1);
    assert_eq!(tsb.get_metric_points_in_range(50, 350).len(), 3);
    assert_eq!(tsb.get_metric_points_in_range(350, 1350).len(), 0);
  }

  #[test]
  fn test_concurrent_appends() {
    // Append BLOCK_SIZE_FOR_TIME_SERIES metric points in multiple threads.
    // Check that all the metric points are appended in sorted order.

    let num_threads = 16;
    let num_metric_points_per_thread = BLOCK_SIZE_FOR_TIME_SERIES / 16;
    let tsb = Arc::new(RwLock::new(TimeSeriesBlock::new()));

    let mut handles = Vec::new();
    let expected = Arc::new(RwLock::new(Vec::new()));
    for _ in 0..num_threads {
      let tsb_arc = tsb.clone();
      let expected_arc = expected.clone();
      let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        for _ in 0..num_metric_points_per_thread {
          let time = rng.gen_range(0..10000);
          let dp = MetricPoint::new(time, 1.0);
          tsb_arc.write().append(time, 1.0).unwrap();
          (*(expected_arc.write())).push(dp);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    // Sort the expected values, as the metric points should be appended in sorted order.
    (*expected.write()).sort();

    assert_eq!(*expected.read(), *tsb.read().metric_points);

    // If we append more than BLOCK_SIZE, it should result in an error.
    let retval = tsb.write().append(1000, 1000.0);
    assert!(retval.is_err());
  }
}

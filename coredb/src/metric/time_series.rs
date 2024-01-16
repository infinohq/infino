// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::collections::BinaryHeap;

use serde::{Deserialize, Serialize};

use crate::metric::metric_point::MetricPoint;
use crate::utils::custom_serde::rwlock_serde;
use crate::utils::error::CoreDBError;
use crate::utils::range::is_overlap;
use crate::utils::sync::RwLock;

use super::constants::BLOCK_SIZE_FOR_TIME_SERIES;
use super::time_series_block::TimeSeriesBlock;
use super::time_series_block_compressed::TimeSeriesBlockCompressed;

/// Separator between the label name and label value to create a label term. For example,
/// if the label name is 'method' and the value is 'GET',and the LABEL_SEPARATOR is '~',
/// in the labels map this will be stored as 'method~GET'.
const LABEL_SEPARATOR: &str = "~";

/// The label for the metric name when stored in the time series. For exmaple, if the METRIC_NAME_PREFIX
/// is '__name__', the LABEL_SEPARATOR is '~', and the matric name is 'request_count', in the labels map,
/// this will be stored as '__name__~request_count'.
const METRIC_NAME_PREFIX: &str = "__name__";

/// Represents a time series. The time series consists of time series blocks, each containing BLOCK_SIZE_FOR_TIME_SERIES
/// metric points. All but the last block are compressed. In order to quickly get to the right block, a vector of
/// initial values in each block is also stored (also called 'skip pointer' in literature).
#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSeries {
  /// A list of compressed time series blocks.
  #[serde(with = "rwlock_serde")]
  compressed_blocks: RwLock<Vec<TimeSeriesBlockCompressed>>,

  /// We only compress blocks that have 128 integers. The last block which
  /// may have <=127 integers is stored in uncompressed form.
  #[serde(with = "rwlock_serde")]
  last_block: RwLock<TimeSeriesBlock>,

  /// The initial timestamps in the time series blocks. The length of the initial
  /// values will 1 plus length of the 'compressed_blocks'.
  /// (The additional 1 is to account for the uncompressed 'last' block)
  #[serde(with = "rwlock_serde")]
  initial_times: RwLock<Vec<u64>>,
}

impl TimeSeries {
  /// Create a new empty time series.
  pub fn new() -> Self {
    TimeSeries {
      compressed_blocks: RwLock::new(Vec::new()),
      last_block: RwLock::new(TimeSeriesBlock::new()),
      initial_times: RwLock::new(Vec::new()),
    }
  }

  /// Append the given time and value to the time series.
  pub fn append(&self, time: u64, value: f64) {
    // Grab all the locks - as we may need to update any of these. In any scenario where all the locks need to captured,
    // the consistent sequence of capturing is important as otherwise it can lead to deadlock.
    let mut compressed_blocks_lock = self.compressed_blocks.write().unwrap();
    let mut last_block_lock = self.last_block.write().unwrap();
    let mut initial_times_lock = self.initial_times.write().unwrap();

    let mut is_initial = false;
    if last_block_lock.is_empty() {
      // First insertion in this time series.
      is_initial = true;
    }

    // Try to append the time+value to the last block.
    let retval = last_block_lock.append(time, value);

    if retval.is_err()
      && retval.err().unwrap() == CoreDBError::CapacityFull(BLOCK_SIZE_FOR_TIME_SERIES)
    {
      // The last block is full. So, compress it and append it time_series_block_compressed.
      let tsbc: TimeSeriesBlockCompressed =
        TimeSeriesBlockCompressed::try_from(&*last_block_lock).unwrap();
      compressed_blocks_lock.push(tsbc);

      // Create a new last block and append the time+value to it.
      *last_block_lock = TimeSeriesBlock::new();
      last_block_lock.append(time, value).unwrap();

      // We created a new block and pushed initial value - so set is_initial to true.
      is_initial = true;
    }

    // If the is_initial flag is set, append the time to initial times vector.
    if is_initial {
      initial_times_lock.push(time);
    }
  }

  /// Get the time series between give start and end time (both inclusive).
  pub fn get_metrics(&self, range_start_time: u64, range_end_time: u64) -> Vec<MetricPoint> {
    // While each TimeSeriesBlock as well as TimeSeriesBlockCompressed has metric points sorted by time, in a
    // multithreaded environment, they might not be sorted across blocks. Hence, we collect all the datapoints in a heap,
    // and return a vector created from the heap, so that the return value is a sorted vector of metric points.

    let mut retval: BinaryHeap<MetricPoint> = BinaryHeap::new();

    let initial_times = self.initial_times.read().unwrap();
    let compressed_blocks = self.compressed_blocks.read().unwrap();
    let last_block = self.last_block.read().unwrap();

    // Get overlapping metric points from the compressed blocks.
    if initial_times.len() > 1 {
      for i in 0..initial_times.len() - 1 {
        let block_start = *initial_times.get(i).unwrap();

        // The maximum block end time would be one less than the start time of the next block.
        let block_end = initial_times.get(i + 1).unwrap() - 1;

        if is_overlap(block_start, block_end, range_start_time, range_end_time) {
          let compressed_block = compressed_blocks.get(i).unwrap();
          let block = TimeSeriesBlock::try_from(compressed_block).unwrap();
          let metric_points_in_range =
            block.get_metric_points_in_range(range_start_time, range_end_time);
          for dp in metric_points_in_range {
            retval.push(dp);
          }
        }
      }
    }

    // Get overlapping metric points from the last block.
    if initial_times.last().is_some() {
      let metric_points_in_range =
        last_block.get_metric_points_in_range(range_start_time, range_end_time);
      for dp in metric_points_in_range {
        retval.push(dp);
      }
    }

    retval.into_sorted_vec()
  }

  /// Get the label term that is used for given metric name.
  pub fn get_label_for_metric_name(metric_name: &str) -> String {
    format!("{METRIC_NAME_PREFIX}{LABEL_SEPARATOR}{metric_name}")
  }

  /// Get the label term used for given label name and label value.
  pub fn get_label<'a>(label_name: &'a str, label_value: &'a str) -> String {
    format!("{label_name}{LABEL_SEPARATOR}{label_value}")
  }

  #[cfg(test)]
  /// Get the last block, wrapped in RwLock.
  pub fn get_last_block(&self) -> &RwLock<TimeSeriesBlock> {
    &self.last_block
  }

  #[cfg(test)]
  /// Get the vector of compressed blocks, wrapped in RwLock.
  pub fn get_compressed_blocks(&self) -> &RwLock<Vec<TimeSeriesBlockCompressed>> {
    &self.compressed_blocks
  }

  #[cfg(test)]
  /// Get the initial times, wrapped in RwLock.
  pub fn get_initial_times(&self) -> &RwLock<Vec<u64>> {
    &self.initial_times
  }
}

impl Default for TimeSeries {
  fn default() -> Self {
    Self::new()
  }
}

impl PartialEq for TimeSeries {
  fn eq(&self, other: &Self) -> bool {
    let compressed_blocks_lock = self.compressed_blocks.read().unwrap();
    let other_compressed_blocks_lock = other.compressed_blocks.read().unwrap();
    let last_block_lock = self.last_block.read().unwrap();
    let other_last_block_lock = other.last_block.read().unwrap();
    let initial_times_lock = self.initial_times.read().unwrap();
    let other_initial_times_lock = other.initial_times.read().unwrap();

    *compressed_blocks_lock == *other_compressed_blocks_lock
      && *initial_times_lock == *other_initial_times_lock
      && *last_block_lock == *other_last_block_lock
  }
}

impl Eq for TimeSeries {}

#[cfg(test)]
mod tests {
  use std::sync::Arc;
  use std::thread;

  use rand::Rng;

  use super::*;
  use crate::utils::sync::is_sync_send;

  #[test]
  fn test_new() {
    // Check that the time series implements sync.
    is_sync_send::<TimeSeries>();

    // Check that a new time series is empty.
    let ts = TimeSeries::new();
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
    assert_eq!(ts.last_block.read().unwrap().len(), 0);
    assert_eq!(ts.initial_times.read().unwrap().len(), 0);
  }

  #[test]
  fn test_default() {
    let ts = TimeSeries::default();

    // Check that a default time series is empty.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
    assert_eq!(ts.last_block.read().unwrap().len(), 0);
    assert_eq!(ts.initial_times.read().unwrap().len(), 0);
  }

  #[test]
  fn test_one_entry() {
    let ts = TimeSeries::new();
    ts.append(100, 200.0);

    // The entry should get apppended only to 'last' block.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);

    assert_eq!(ts.last_block.read().unwrap().len(), 1);
    let last_block_lock = ts.last_block.read().unwrap();
    let time_series_metric_points = &*last_block_lock.get_metrics_metric_points().read().unwrap();
    let metric_point = time_series_metric_points.first().unwrap();
    assert_eq!(metric_point.get_time(), 100);
    assert_eq!(metric_point.get_value(), 200.0);

    assert_eq!(ts.initial_times.read().unwrap().len(), 1);
    assert_eq!(ts.initial_times.read().unwrap().first().unwrap(), &100);
  }

  #[test]
  fn test_block_size_entries() {
    let ts = TimeSeries::new();
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      ts.append(i as u64, i as f64);
    }

    // All the entries will go to 'last', as we have pushed exactly BLOCK_SIZE_FOR_TIME_SERIES entries.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 0);
    assert_eq!(
      ts.last_block.read().unwrap().len(),
      BLOCK_SIZE_FOR_TIME_SERIES
    );
    assert_eq!(ts.initial_times.read().unwrap().len(), 1);
    assert_eq!(ts.initial_times.read().unwrap().first().unwrap(), &0);

    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      let last_block_lock = ts.last_block.read().unwrap();
      let time_series_metric_points = &*last_block_lock.get_metrics_metric_points().read().unwrap();
      let metric_point = time_series_metric_points.get(i).unwrap();
      assert_eq!(metric_point.get_time(), i as u64);
      assert_eq!(metric_point.get_value(), i as f64);
    }
  }

  #[test]
  fn test_block_size_plus_one_entries() {
    let ts = TimeSeries::new();

    // Append block_size+1 entries, so that two blocks are created.
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES + 1 {
      ts.append(i as u64, i as f64);
    }

    // We should have 1 compressed block with 128 entries, and a last block with 1 entry.
    assert_eq!(ts.compressed_blocks.read().unwrap().len(), 1);
    assert_eq!(ts.last_block.read().unwrap().len(), 1);

    // There should be 2 initial_times per the two blocks, with start times 0 and
    // BLOCK_SIZE_FOR_TIME_SERIES
    assert_eq!(ts.initial_times.read().unwrap().len(), 2);
    assert_eq!(ts.initial_times.read().unwrap().first().unwrap(), &0);
    assert_eq!(
      ts.initial_times.read().unwrap().get(1).unwrap(),
      &(BLOCK_SIZE_FOR_TIME_SERIES as u64)
    );

    let uncompressed =
      TimeSeriesBlock::try_from(ts.compressed_blocks.read().unwrap().first().unwrap()).unwrap();
    assert_eq!(uncompressed.len(), BLOCK_SIZE_FOR_TIME_SERIES);
    let metric_points_lock = uncompressed.get_metrics_metric_points().read().unwrap();
    for i in 0..BLOCK_SIZE_FOR_TIME_SERIES {
      let metric_point = metric_points_lock.get(i).unwrap();
      assert_eq!(metric_point.get_time(), i as u64);
      assert_eq!(metric_point.get_value(), i as f64);
    }
  }

  #[test]
  fn test_metric_points_in_range() {
    let num_blocks = 4;
    let ts = TimeSeries::new();
    let num_metric_points = num_blocks * BLOCK_SIZE_FOR_TIME_SERIES as u64;
    for i in 0..num_metric_points {
      ts.append(i, i as f64);
    }

    assert_eq!(
      ts.get_metrics(0, num_metric_points - 1).len() as u64,
      num_metric_points
    );
    assert_eq!(
      ts.get_metrics(0, num_metric_points + 1000).len() as u64,
      num_metric_points
    );

    assert_eq!(
      ts.get_metrics(0, BLOCK_SIZE_FOR_TIME_SERIES as u64).len(),
      BLOCK_SIZE_FOR_TIME_SERIES + 1
    );

    assert_eq!(
      ts.get_metrics(
        BLOCK_SIZE_FOR_TIME_SERIES as u64,
        BLOCK_SIZE_FOR_TIME_SERIES as u64 + 10
      )
      .len(),
      11
    );
  }

  #[test]
  fn test_concurrent_append() {
    let num_blocks: usize = 10;
    let num_threads = 16;
    let num_metric_points_per_thread = num_blocks * BLOCK_SIZE_FOR_TIME_SERIES / num_threads;
    let ts = Arc::new(TimeSeries::new());

    let mut handles = Vec::new();
    let expected = Arc::new(RwLock::new(Vec::new()));
    for _ in 0..num_threads {
      let ts_arc = ts.clone();
      let expected_arc = expected.clone();
      let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        for _ in 0..num_metric_points_per_thread {
          let time = rng.gen_range(0..10000);
          let dp = MetricPoint::new(time, 1.0);
          ts_arc.append(time, 1.0);
          expected_arc.write().unwrap().push(dp);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    let compressed_blocks = ts.compressed_blocks.read().unwrap();
    let last_block = ts.last_block.read().unwrap();
    let initial_times = ts.initial_times.read().unwrap();

    assert_eq!(compressed_blocks.len(), num_blocks - 1);
    assert_eq!(last_block.len(), BLOCK_SIZE_FOR_TIME_SERIES);
    assert_eq!(initial_times.len(), num_blocks);

    let received = ts.get_metrics(0, u64::MAX);
    (*expected.write().unwrap()).sort();
    assert_eq!(*expected.read().unwrap(), received);
  }
}

use crossbeam::atomic::AtomicCell;
use serde::{Deserialize, Serialize};

use crate::utils::custom_serde::atomic_cell_serde;

#[derive(Debug, Deserialize, Serialize)]
/// Metadata for tsldb's index.
pub struct Metadata {
  /// Number of segments.
  /// Note that this may not be same as the number of segments in the index, esp when
  /// merging of older segments is implemented. The primary use of this field is to
  /// provide a unique numeric key for each segment in the index.
  #[serde(with = "atomic_cell_serde")]
  segment_count: AtomicCell<u32>,

  /// Number of the current segment.
  #[serde(with = "atomic_cell_serde")]
  current_segment_number: AtomicCell<u32>,

  /// Maximum number of log messages per segment. This is only approximate and a segment can
  /// contain more messages than this number depending on the frequecy at which commit() is called.
  #[serde(with = "atomic_cell_serde")]
  approx_max_log_message_count_per_segment:  AtomicCell<u32>,

  /// Maximum number of data points per segment. This is only approximate and a segment can
  /// contain more data points than this number depending on the frequecy at which commit() is called.
  #[serde(with = "atomic_cell_serde")]
  approx_max_data_point_count_per_segment:  AtomicCell<u32>,
}

impl Metadata {
  /// Create new Metadata with given values.
  pub fn new(
    segment_count: u32,
    current_segment_number: u32,
    max_log_messges: u32,
    max_data_points: u32,
  ) -> Metadata {
    Metadata {
      segment_count: AtomicCell::new(segment_count),
      current_segment_number: AtomicCell::new(current_segment_number),
      approx_max_log_message_count_per_segment: AtomicCell::new(max_log_messges),
      approx_max_data_point_count_per_segment: AtomicCell::new(max_data_points),
    }
  }

  #[cfg(test)]
  /// Get segment count.
  pub fn get_segment_count(&self) -> u32 {
    self.segment_count.load()
  }

  /// Get the current segment number.
  pub fn get_current_segment_number(&self) -> u32 {
    self.current_segment_number.load()
  }

  /// Fetch the segment count and increment it by 1.
  pub fn fetch_increment_segment_count(&self) -> u32 {
    self.segment_count.fetch_add(1)
  }

  /// Update the current segment number to the given value.
  pub fn update_current_segment_number(&self, value: u32) {
    self.current_segment_number.store(value);
  }

  /// Get the (approx) max log message count per segment.
  pub fn get_approx_max_log_message_count_per_segment(&self) -> u32 {
    self.approx_max_log_message_count_per_segment.load()
  }

  /// Get the (approx) max data point count per segment.
  pub fn get_approx_max_data_point_count_per_segment(&self) -> u32 {
    self.approx_max_data_point_count_per_segment.load()
  }

  /// Update the current segment number to the given value.
  pub fn update_max_log_message_count_per_segment(&self, value: u32) {
    self.approx_max_log_message_count_per_segment.store(value);
  }

  /// Update the current segment number to the given value.
  pub fn update_max_data_point_count_per_segment(&self, value: u32) {
    self.approx_max_data_point_count_per_segment.store(value);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  pub fn test_new_metadata() {
    // Check if the metadata implements Sync + Send.
    is_sync::<Metadata>();

    // Check a newly created Metadata.
    let m: Metadata = Metadata::new(10, 5, 1000, 2000);

    assert_eq!(m.get_segment_count(), 10);
    assert_eq!(m.get_current_segment_number(), 5);
    assert_eq!(m.get_approx_max_log_message_count_per_segment(), 1000);
    assert_eq!(m.get_approx_max_data_point_count_per_segment(), 2000);
  }

  #[test]
  pub fn test_increment_and_update() {
    // Check the increment and update operations on Metadata.
    let m: Metadata = Metadata::new(10, 5, 1000, 2000);
    assert_eq!(m.fetch_increment_segment_count(), 10);
    m.update_current_segment_number(7);
    assert_eq!(m.get_segment_count(), 11);
    assert_eq!(m.get_current_segment_number(), 7);
  }
}

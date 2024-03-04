use crossbeam::atomic::AtomicCell;
use serde::{Deserialize, Serialize};

use crate::utils::custom_serde::atomic_cell_serde;

#[derive(Debug, Deserialize, Serialize)]
/// Metadata for CoreDB's index.
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

  /// Log messages threshold for creating a new segment during appends.
  #[serde(with = "atomic_cell_serde")]
  append_log_messages_threshold: AtomicCell<u32>,

  /// Metric points threshold for creating a new segment during appends.
  #[serde(with = "atomic_cell_serde")]
  append_metric_points_threshold: AtomicCell<u32>,
}

impl Metadata {
  /// Create new Metadata with given values.
  pub fn new(
    segment_count: u32,
    current_segment_number: u32,
    append_log_messages_threshold: u32,
    append_metric_points_threshold: u32,
  ) -> Metadata {
    Metadata {
      segment_count: AtomicCell::new(segment_count),
      current_segment_number: AtomicCell::new(current_segment_number),
      append_log_messages_threshold: AtomicCell::new(append_log_messages_threshold),
      append_metric_points_threshold: AtomicCell::new(append_metric_points_threshold),
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

  /// Get append log messages threshold (new segment is created during appends after reaching this threshold)
  pub fn get_append_log_messages_threshold(&self) -> u32 {
    self.append_log_messages_threshold.load()
  }

  /// Get append metric points threshold (new segment is created during appends after reaching this threshold)
  pub fn get_append_metric_points_threshold(&self) -> u32 {
    self.append_metric_points_threshold.load()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::is_sync_send;

  #[test]
  pub fn test_new_metadata() {
    // Check if the metadata implements Sync + Send.
    is_sync_send::<Metadata>();

    // Check a newly created Metadata.
    let m: Metadata = Metadata::new(10, 5, 100, 1000);

    assert_eq!(m.get_segment_count(), 10);
    assert_eq!(m.get_current_segment_number(), 5);
    assert_eq!(m.get_append_log_messages_threshold(), 100);
    assert_eq!(m.get_append_metric_points_threshold(), 1000);
  }

  #[test]
  pub fn test_increment_and_update() {
    // Check the increment and update operations on Metadata.
    let m: Metadata = Metadata::new(10, 5, 100, 1000);
    assert_eq!(m.fetch_increment_segment_count(), 10);
    m.update_current_segment_number(7);
    assert_eq!(m.get_segment_count(), 11);
    assert_eq!(m.get_current_segment_number(), 7);
  }
}

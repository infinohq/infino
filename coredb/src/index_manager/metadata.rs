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

  /// Threshold size of a segment in megabytes. If the size of a segment exceeds this, a new segment
  /// will be created in the next commit operation.
  #[serde(with = "atomic_cell_serde")]
  segment_size_threshold_megabytes: AtomicCell<f32>,
}

impl Metadata {
  /// Create new Metadata with given values.
  pub fn new(
    segment_count: u32,
    current_segment_number: u32,
    segment_size_threshold_megabytes: f32,
  ) -> Metadata {
    Metadata {
      segment_count: AtomicCell::new(segment_count),
      current_segment_number: AtomicCell::new(current_segment_number),
      segment_size_threshold_megabytes: AtomicCell::new(segment_size_threshold_megabytes),
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

  /// Get the segment size threshold in megabytes.
  pub fn get_segment_size_threshold_megabytes(&self) -> f32 {
    self.segment_size_threshold_megabytes.load()
  }

  /// Update the current segment number to the given value.
  pub fn update_segment_size_threshold_megabytes(&self, value: f32) {
    self.segment_size_threshold_megabytes.store(value);
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
    let m: Metadata = Metadata::new(10, 5, 1000 as f32);

    assert_eq!(m.get_segment_count(), 10);
    assert_eq!(m.get_current_segment_number(), 5);
    assert_eq!(m.get_segment_size_threshold_megabytes(), 1000 as f32);
  }

  #[test]
  pub fn test_increment_and_update() {
    // Check the increment and update operations on Metadata.
    let m: Metadata = Metadata::new(10, 5, 1000 as f32);
    assert_eq!(m.fetch_increment_segment_count(), 10);
    m.update_current_segment_number(7);
    assert_eq!(m.get_segment_count(), 11);
    assert_eq!(m.get_current_segment_number(), 7);
  }
}

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

  /// Threshold size of a segment in bytes. If the size of a segment exceeds this, a new segment
  /// will be created in the next commit operation.
  #[serde(with = "atomic_cell_serde")]
  segment_size_threshold_bytes: AtomicCell<u64>,
}

impl Metadata {
  /// Create new Metadata with given values.
  pub fn new(
    segment_count: u32,
    current_segment_number: u32,
    segment_size_threshold_bytes: u64,
  ) -> Metadata {
    Metadata {
      segment_count: AtomicCell::new(segment_count),
      current_segment_number: AtomicCell::new(current_segment_number),
      segment_size_threshold_bytes: AtomicCell::new(segment_size_threshold_bytes),
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

  /// Get the segment size threshold in bytes.
  pub fn get_segment_size_threshold_bytes(&self) -> u64 {
    self.segment_size_threshold_bytes.load()
  }

  /// Update the current segment number to the given value.
  pub fn update_segment_size_threshold_bytes(&self, value: u64) {
    self.segment_size_threshold_bytes.store(value);
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
    let m: Metadata = Metadata::new(10, 5, 1000);

    assert_eq!(m.get_segment_count(), 10);
    assert_eq!(m.get_current_segment_number(), 5);
    assert_eq!(m.get_segment_size_threshold_bytes(), 1000);
  }

  #[test]
  pub fn test_increment_and_update() {
    // Check the increment and update operations on Metadata.
    let m: Metadata = Metadata::new(10, 5, 1000);
    assert_eq!(m.fetch_increment_segment_count(), 10);
    m.update_current_segment_number(7);
    assert_eq!(m.get_segment_count(), 11);
    assert_eq!(m.get_current_segment_number(), 7);
  }
}

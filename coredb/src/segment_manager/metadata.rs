// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crossbeam::atomic::AtomicCell;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::storage_manager::storage::COMPRESSION_LEVEL;
use crate::utils::custom_serde::atomic_cell_serde;

#[derive(Debug, Deserialize, Serialize)]
/// Metadata for a segment.
pub struct Metadata {
  /// Unique id.
  id: String,

  /// Number of log messages.
  #[serde(with = "atomic_cell_serde")]
  log_message_count: AtomicCell<u32>,

  /// Number of terms.
  #[serde(with = "atomic_cell_serde")]
  term_count: AtomicCell<u32>,

  /// Number of labels.
  #[serde(with = "atomic_cell_serde")]
  label_count: AtomicCell<u32>,

  /// Number of metric points.
  #[serde(with = "atomic_cell_serde")]
  metric_point_count: AtomicCell<u32>,

  /// Size of the segment in bytes - uncompressed.
  /// This typically represents how much memory is needed to load the segment.
  #[serde(with = "atomic_cell_serde")]
  uncompressed_size: AtomicCell<u64>,

  /// Size of the segment in bytes - compressed.
  /// This typically represents how much disk is needed to store the segment.
  #[serde(with = "atomic_cell_serde")]
  compressed_size: AtomicCell<u64>,

  /// Least timestamp.
  #[serde(with = "atomic_cell_serde")]
  start_time: AtomicCell<u64>,

  /// End timestamp.
  #[serde(with = "atomic_cell_serde")]
  end_time: AtomicCell<u64>,
}

impl Metadata {
  /// Create new Metadata.
  pub fn new() -> Metadata {
    Metadata {
      id: Uuid::new_v4().to_string(),
      log_message_count: AtomicCell::new(0),
      term_count: AtomicCell::new(0),
      label_count: AtomicCell::new(0),
      metric_point_count: AtomicCell::new(0),
      uncompressed_size: AtomicCell::new(0),
      compressed_size: AtomicCell::new(0),
      start_time: AtomicCell::new(u64::MAX),
      end_time: AtomicCell::new(0),
    }
  }

  #[allow(dead_code)]
  /// Get segment id.
  pub fn get_id(&self) -> &str {
    &self.id
  }

  #[allow(dead_code)]
  /// Get number of log message in this segment.
  pub fn get_log_message_count(&self) -> u32 {
    self.log_message_count.load()
  }

  #[allow(dead_code)]
  /// Get number of terms in this segment.
  pub fn get_term_count(&self) -> u32 {
    self.term_count.load()
  }

  #[allow(dead_code)]
  /// Get number of labels in this segement.
  pub fn get_label_count(&self) -> u32 {
    self.label_count.load()
  }

  #[allow(dead_code)]
  /// Get number of metric points in this segment.
  pub fn get_metric_point_count(&self) -> u32 {
    self.metric_point_count.load()
  }

  #[allow(dead_code)]
  /// Get uncompressed segment size.
  pub fn get_uncompressed_size(&self) -> u64 {
    self.uncompressed_size.load()
  }

  #[allow(dead_code)]
  /// Get compressed segment size.
  pub fn get_compressed_size(&self) -> u64 {
    self.compressed_size.load()
  }

  /// Get the earliest timestamp in this segment.
  pub fn get_start_time(&self) -> u64 {
    self.start_time.load()
  }

  /// Get the latest timestamp in this segment.
  pub fn get_end_time(&self) -> u64 {
    self.end_time.load()
  }

  /// Get the current log message count in this segment and increment it by 1.
  pub fn fetch_increment_log_message_count(&self) -> u32 {
    self.log_message_count.fetch_add(1)
  }

  /// Get the current term count in this segment and increment it by 1.
  pub fn fetch_increment_term_count(&self) -> u32 {
    self.term_count.fetch_add(1)
  }

  /// Get the current label count in this segment and increment it by 1.
  pub fn fetch_increment_label_count(&self) -> u32 {
    self.label_count.fetch_add(1)
  }

  /// Get the current count of metric points in this segement and increment it by 1.
  pub fn fetch_increment_metric_point_count(&self) -> u32 {
    self.metric_point_count.fetch_add(1)
  }

  /// Update the size of this segment to the given uncompressed and compressed values.
  pub fn update_segment_size(&self, uncompressed_size: u64, compressed_size: u64) {
    self.uncompressed_size.store(uncompressed_size);
    self.compressed_size.store(compressed_size);
  }

  /// Update the start time of this segment to the given value.
  pub fn update_start_time(&self, time: u64) {
    self.start_time.store(time);
  }

  /// Update the end time of this segment to the given value.
  pub fn update_end_time(&self, time: u64) {
    self.end_time.store(time);
  }

  /// Get the size of this segment in bytes - (uncompressed, compressed)
  pub fn get_metadata_size(&self) -> (u64, u64) {
    let input = serde_json::to_string(self).unwrap();
    let uncomepressed_size = input.as_bytes().len() as u64;

    let mut output: Vec<u8> = Vec::new();
    zstd::stream::copy_encode(input.as_bytes(), &mut output, COMPRESSION_LEVEL).unwrap();
    let compressed_size = output.len() as u64;

    (uncomepressed_size, compressed_size)
  }
}

#[cfg(test)]
mod tests {
  use test_case::test_case;

  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  pub fn test_new_metadata() {
    // Check that the metadata implements Sync and Send.
    is_sync::<Metadata>();

    // Check that empty/new metyadata is as expected.
    let m: Metadata = Metadata::new();
    assert_eq!(m.get_log_message_count(), 0);
    assert_eq!(m.get_term_count(), 0);
    assert_eq!(m.get_label_count(), 0);
    assert_eq!(m.get_metric_point_count(), 0);
    assert_eq!(m.get_start_time(), u64::MAX);
    assert_eq!(m.get_end_time(), 0);
  }

  #[test_case(1,2,3,4; "different increments - 1,2,3,4")]
  #[test_case(14,13,12,11; "different increments - 14,13,12,11")]
  pub fn test_increment_specific_values(
    log_message_increment: u32,
    term_increment: u32,
    metric_point_increment: u32,
    label_increment: u32,
  ) {
    let m: Metadata = Metadata::new();
    for _ in 0..log_message_increment {
      m.fetch_increment_log_message_count();
    }
    for _ in 0..term_increment {
      m.fetch_increment_term_count();
    }
    for _ in 0..metric_point_increment {
      m.fetch_increment_metric_point_count();
    }
    for _ in 0..label_increment {
      m.fetch_increment_label_count();
    }

    assert_eq!(m.get_log_message_count(), log_message_increment);
    assert_eq!(m.get_term_count(), term_increment);
    assert_eq!(m.get_metric_point_count(), metric_point_increment);
    assert_eq!(m.get_label_count(), label_increment);
  }

  #[test]
  pub fn test_metadata_size() {
    let m: Metadata = Metadata::new();
    let (uncompressed, compressed) = m.get_metadata_size();

    assert!(uncompressed > 0);
    assert!(compressed > 0);
  }

  #[test]
  pub fn test_segment_size() {
    let m: Metadata = Metadata::new();
    m.update_segment_size(2000, 1000);

    assert_eq!(m.get_uncompressed_size(), 2000);
    assert_eq!(m.get_compressed_size(), 1000);
  }
}

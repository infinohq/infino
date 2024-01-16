use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::segment_manager::segment::Segment;
use crate::utils::range::is_overlap;

#[derive(Debug, Deserialize, Serialize)]
pub struct SegmentSummary {
  /// Unique segment id.
  segment_id: String,

  /// Segment number in this index.
  segment_number: u32,

  /// Start time.
  start_time: u64,

  /// Last modified time.
  end_time: u64,

  /// Uncompressed size (i.e., size when the segment is loaded in memory)
  uncompressed_size: u64,
}

impl SegmentSummary {
  pub fn new(segment_number: u32, segment: &Segment) -> Self {
    SegmentSummary {
      segment_id: segment.get_id().to_owned(),
      segment_number,
      start_time: segment.get_start_time(),
      end_time: segment.get_end_time(),
      uncompressed_size: segment.get_uncompressed_size(),
    }
  }

  pub fn get_segment_id(&self) -> &str {
    &self.segment_id
  }

  pub fn get_segment_number(&self) -> u32 {
    self.segment_number
  }

  pub fn get_start_time(&self) -> u64 {
    self.start_time
  }

  pub fn get_end_time(&self) -> u64 {
    self.end_time
  }

  pub fn get_uncompressed_size(&self) -> u64 {
    self.uncompressed_size
  }

  pub fn update_start_end_time(&mut self, start_time: u64, end_time: u64) {
    self.start_time = start_time;
    self.end_time = end_time;
  }

  /// Returns true if this segment summary overlaps with the given range.
  pub fn is_overlap(&self, range_start_time: u64, range_end_time: u64) -> bool {
    is_overlap(
      self.get_start_time(),
      self.get_end_time(),
      range_start_time,
      range_end_time,
    )
  }
}

impl PartialEq for SegmentSummary {
  fn eq(&self, other: &Self) -> bool {
    self.segment_id == other.segment_id
  }
}

impl Eq for SegmentSummary {}

impl PartialOrd for SegmentSummary {
  // Suppress custom ordering warning - which happens in some Clippy versions.
  // However, the specific warning can't be suppressed as older Clippy versions
  // do not know about it.
  #[allow(clippy::all)]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    other.end_time.partial_cmp(&self.end_time)
  }
}

impl Ord for SegmentSummary {
  fn cmp(&self, other: &Self) -> Ordering {
    self.partial_cmp(other).unwrap()
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use crate::utils::sync::is_sync_send;

  use super::*;

  #[test]
  pub fn test_new_segment_summary() {
    // Check if the SegmentSummary implements Sync + Send.
    is_sync_send::<SegmentSummary>();

    let segment = Segment::new();
    let segment_summary = SegmentSummary::new(1, &segment);

    assert_eq!(segment_summary.get_segment_id(), segment.get_id());
    assert_eq!(segment_summary.get_segment_number(), 1);
    assert_eq!(segment_summary.get_start_time(), segment.get_start_time());
    assert_eq!(segment_summary.get_end_time(), segment.get_end_time());
    assert_eq!(
      segment_summary.get_uncompressed_size(),
      segment.get_uncompressed_size()
    );
  }

  #[test]
  pub fn test_sort_segment_summary() {
    let num_segments = 3;
    let mut segment_summaries = Vec::new();
    let mut expected_segment_ids = Vec::new();

    // Create a few segments along with their summaries.
    for i in 1..=num_segments {
      let segment = Segment::new();
      segment
        .append_log_message(i, &HashMap::new(), "some log message")
        .expect("Could not append to segment");
      let segment_summary = SegmentSummary::new(i as u32, &segment);
      segment_summaries.push(segment_summary);

      // The latest created segment is expected to the first one in sorted segement_summaries - since the
      // summaries are sorted in reverse chronological order.
      expected_segment_ids.insert(0, segment.get_id().to_owned());
    }

    // Sort the segment summaries and retrieve their ids.
    segment_summaries.sort();
    let retrieved_segment_ids: Vec<String> = segment_summaries
      .iter()
      .map(|summary| summary.segment_id.clone())
      .collect();

    // Make sure that the retrieved ids are in reverse cronological order - i.e., they are same as the expected ids.
    assert_eq!(retrieved_segment_ids, expected_segment_ids);
  }
}

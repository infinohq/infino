use crate::index_manager::segment_summary::SegmentSummary;
use dashmap::DashMap;

pub trait MergePolicy: Send + Sync {
  fn apply(
    &self,
    segment_summaries: &DashMap<u32, SegmentSummary>,
    segments_in_memory: Vec<u32>,
  ) -> Vec<u32>;
}

pub struct SizeBasedMerge {
  target_segment_size_megabytes: u64,
}

impl SizeBasedMerge {
  pub fn new(target_segment_size_megabytes: u64) -> Self {
    SizeBasedMerge {
      target_segment_size_megabytes,
    }
  }
}

impl MergePolicy for SizeBasedMerge {
  fn apply(
    &self,
    segment_summaries: &DashMap<u32, SegmentSummary>,
    segments_in_memory: Vec<u32>,
  ) -> Vec<u32> {
    // Create a writable empty Vec segment ids
    let mut segment_ids: Vec<u32> = Vec::new();

    // Iterate over segment_summaries
    for entry in segment_summaries.iter() {
      let segment_id = *entry.key();
      if !segments_in_memory.contains(&segment_id) {
        segment_ids.push(segment_id);
      }
    }

    // Sort the list of ids
    segment_ids.sort_unstable_by_key(|&id| segment_summaries.get(&id).unwrap().get_end_time());

    // create the segments_to_merge
    let mut segments_to_merge: Vec<u32> = Vec::new();
    let mut total_size: u64 = 0;

    for id in segment_ids {
      let segment_summary = segment_summaries.get(&id).unwrap();
      // Sum of uncompressed size is smaller than target_segment_size_megabytes
      if total_size + segment_summary.get_uncompressed_size()
        <= self.target_segment_size_megabytes * 1024 * 1024
      {
        // Add segments to the segments_to_merge
        segments_to_merge.push(id);
        // Keep track of total size
        total_size += segment_summary.get_uncompressed_size();
      } else {
        break; // Early exit once we've reached size limit
      }
    }

    segments_to_merge
  }
}

// write test case for above function
#[cfg(test)]
mod tests {
  use std::time::SystemTime;

  use super::*;
  use crate::{
    index_manager::segment_summary::SegmentSummary, segment_manager::segment::Segment,
    utils::time::get_current_time_in_seconds,
  };

  #[test]
  fn test_apply() {
    let segment_summaries: DashMap<u32, SegmentSummary> = DashMap::new();
    let current_secs = get_current_time_in_seconds(SystemTime::now());

    // Create three segments: one from 9 days ago, one from 8 days ago and one from 7 day ago
    let days_to_secs = |days: u64| days * 24 * 60 * 60_u64;
    let segment = Segment::new();
    let mut segment1 = SegmentSummary::new(0, &segment);
    segment1.update_start_end_time_test(
      current_secs - days_to_secs(9),
      current_secs - days_to_secs(8),
    );
    segment1.update_uncompressed_size(512 * 1024 * 1024);
    segment_summaries.insert(0, segment1);

    let mut segment2 = SegmentSummary::new(1, &segment);
    segment2.update_start_end_time_test(
      current_secs - days_to_secs(8),
      current_secs - days_to_secs(7),
    );
    segment2.update_uncompressed_size(512 * 1024 * 1024);
    segment_summaries.insert(1, segment2);

    let mut segment3 = SegmentSummary::new(2, &segment);
    segment3.update_start_end_time_test(
      current_secs - days_to_secs(7),
      current_secs - days_to_secs(6),
    );
    segment3.update_uncompressed_size(512 * 1024 * 1024);
    segment_summaries.insert(2, segment3);

    let merge_policy = SizeBasedMerge::new(1024);
    let segments_to_merge = merge_policy.apply(&segment_summaries, vec![]);
    assert_eq!(segments_to_merge, vec![0, 1]);
  }

  #[test]
  fn test_apply_with_memory_segments() {
    let segment_summaries: DashMap<u32, SegmentSummary> = DashMap::new();
    let current_secs = get_current_time_in_seconds(SystemTime::now());

    // Create three segments: one from 9 days ago, one from 8 days ago and one from 7 day ago
    let days_to_secs = |days: u64| days * 24 * 60 * 60_u64;
    let segment = Segment::new();
    let mut segment1 = SegmentSummary::new(0, &segment);
    segment1.update_start_end_time_test(
      current_secs - days_to_secs(9),
      current_secs - days_to_secs(8),
    );
    segment1.update_uncompressed_size(512 * 1024 * 1024);
    segment_summaries.insert(0, segment1);

    let mut segment2 = SegmentSummary::new(1, &segment);
    segment2.update_start_end_time_test(
      current_secs - days_to_secs(8),
      current_secs - days_to_secs(7),
    );
    segment2.update_uncompressed_size(512 * 1024 * 1024);
    segment_summaries.insert(1, segment2);

    let mut segment3 = SegmentSummary::new(2, &segment);
    segment3.update_start_end_time_test(
      current_secs - days_to_secs(7),
      current_secs - days_to_secs(6),
    );
    segment3.update_uncompressed_size(512 * 1024 * 1024);
    segment_summaries.insert(2, segment3);

    let merge_policy = SizeBasedMerge::new(1024);
    let segments_to_merge = merge_policy.apply(&segment_summaries, vec![1, 2]);
    assert_eq!(segments_to_merge, vec![0]);
  }
}

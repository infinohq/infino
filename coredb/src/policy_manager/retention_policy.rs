use std::time::SystemTime;

use crate::{
  index_manager::segment_summary::SegmentSummary, utils::time::get_current_time_in_seconds,
};

pub trait RetentionPolicy: Send + Sync {
  fn apply(&self, segment_summary: Vec<SegmentSummary>) -> Vec<u32>;
}

pub struct TimeBasedRetention {
  retention_days: u32,
}

impl TimeBasedRetention {
  pub fn new(retention_days: u32) -> Self {
    TimeBasedRetention { retention_days }
  }
}

impl RetentionPolicy for TimeBasedRetention {
  fn apply(&self, segment_summary: Vec<SegmentSummary>) -> Vec<u32> {
    let current_time = get_current_time_in_seconds(SystemTime::now());
    let mut ids_to_delete: Vec<u32> = Vec::new();
    for segment_summary in segment_summary {
      if segment_summary.get_end_time() < current_time - (self.retention_days as u64 * 24 * 60 * 60)
      {
        ids_to_delete.push(segment_summary.get_segment_number().to_owned());
      }
    }
    ids_to_delete
  }
}

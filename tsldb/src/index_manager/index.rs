use std::collections::HashMap;
use std::path::Path;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use log::{debug, info};

use crate::index_manager::metadata::Metadata;
use crate::log::log_message::LogMessage;
use crate::segment_manager::segment::Segment;
use crate::ts::data_point::DataPoint;
use crate::utils::error::TsldbError;
use crate::utils::io;
use crate::utils::serialize;
use crate::utils::sync::thread;
use crate::utils::sync::{Arc, Mutex};

/// File name where the list (ids) of all segements is stored.
const ALL_SEGMENTS_LIST_FILE_NAME: &str = "all_segments_list.bin";

/// File name to store index metadata.
const METADATA_FILE_NAME: &str = "metadata.bin";

/// Default threshold for the number of max log messages per segment. Note that since we create new segments only on commit,
/// this number will be approximate.
const DEFAULT_NUM_LOG_MESSAGES_THRESHOLD: u32 = 100_000;

/// Default threshold for the number of max data points per segment. Note that since we create new segments only on commit,
/// this number will be approximate.
const DEFAULT_NUM_DATA_POINTS_THRESHOLD: u32 = 100_000;

#[derive(Debug)]
/// Index for storing log messages and time series data points.
pub struct Index {
  /// Metadata for this index.
  metadata: Metadata,

  /// DashMap of segment number to segment.
  all_segments_map: DashMap<u32, Segment>,

  /// Directory where the index is serialized.
  index_dir_path: String,

  /// Mutex for locking the directory where the index is committed / read from, so that two threads
  /// don't write the directory at the same time.
  index_dir_lock: Arc<Mutex<thread::ThreadId>>,
}

impl Index {
  /// Create a new index with default threshold log messages / threshold data points parameters.
  /// However, if a directory with the same path already exists and has a metadata file in it,
  /// the function will refresh the existing index instead of creating a new one.
  /// If the refresh process fails, an error will be thrown to indicate the issue.
  pub fn new(index_dir_path: &str) -> Result<Self, TsldbError> {
    Index::new_with_threshold_params(
      index_dir_path,
      DEFAULT_NUM_LOG_MESSAGES_THRESHOLD,
      DEFAULT_NUM_DATA_POINTS_THRESHOLD,
    )
  }

  /// Creates a new index at the specified directory(index_dir_path) path with customizable parameters for
  /// threshold log messages and data points.
  /// However, if a directory with the same path already exists and has a metadata file in it,
  /// the function will refresh the existing index instead of creating a new one.
  /// If the refresh process fails, an error will be thrown to indicate the issue.
  pub fn new_with_threshold_params(
    index_dir_path: &str,
    num_log_messages_threshold: u32,
    num_data_points_threshold: u32,
  ) -> Result<Self, TsldbError> {
    info!("Creating index - dir {}, max log messages per segment (approx): {}, max data points per segment {}",
          index_dir_path, num_log_messages_threshold, num_data_points_threshold);

    if !Path::new(index_dir_path).is_dir() {
      // Directory does not exist. Create it.
      std::fs::create_dir_all(index_dir_path).unwrap();
    } else if Path::new(&io::get_joined_path(index_dir_path, METADATA_FILE_NAME)).is_file() {
      // index_dir_path has metadata file, refresh the index instead of creating new one
      match Self::refresh(index_dir_path) {
        Ok(index) => {
          // Update metadata with max log message and data points
          index
            .metadata
            .update_max_log_message_count_per_segment(num_log_messages_threshold);
          index
            .metadata
            .update_max_data_point_count_per_segment(num_data_points_threshold);
          return Ok(index);
        }
        Err(err) => {
          // Received a error while refreshing index
          return Err(err);
        }
      }
    } else {
      return Err(TsldbError::CannotFindIndexMetadataInDirectory(
        String::from(index_dir_path),
      ));
    }

    // Create an initial segment.
    let segment = Segment::new();
    let metadata = Metadata::new(0, 0, num_log_messages_threshold, num_data_points_threshold);

    // Update the initial segment as the current segment.
    let current_segment_number = metadata.fetch_increment_segment_count();
    metadata.update_current_segment_number(current_segment_number);

    let all_segments_map = DashMap::new();
    all_segments_map.insert(current_segment_number, segment);

    let index_dir_lock = Arc::new(Mutex::new(thread::current().id()));

    let index = Index {
      metadata,
      all_segments_map,
      index_dir_path: index_dir_path.to_owned(),
      index_dir_lock,
    };

    // Commit the empty index so that the index directory will be created.
    index.commit(false);

    Ok(index)
  }

  /// Get the reference for the current segment.
  fn get_current_segment_ref(&self) -> Ref<u32, Segment> {
    self
      .all_segments_map
      .get(&self.metadata.get_current_segment_number())
      .unwrap()
  }

  /// Append a log message to this index.
  pub fn append_log_message(&self, time: u64, message: &str) {
    debug!(
      "Appending log message, time: {}, message: {}",
      time, message
    );

    // Get the current segment.
    let current_segment_ref = self.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // Append the log message to the current segment.
    current_segment.append_log_message(time, message).unwrap();
  }

  /// Append a data point to this index.
  pub fn append_data_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) {
    debug!(
      "Appending data time, metric name: {}, labels: {:?}, time: {}, value: {}",
      metric_name, labels, time, value
    );

    // Get the current segment.
    let current_segment_ref = self.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // Append the data point to the current segment.
    current_segment
      .append_data_point(metric_name, labels, time, value)
      .unwrap();
  }

  /// Search for given query in the given time range.
  pub fn search(&self, query: &str, range_start_time: u64, range_end_time: u64) -> Vec<LogMessage> {
    debug!(
      "Search logs for query: {}, range_start_time: {}, range_end_time: {}",
      query, range_start_time, range_end_time
    );

    let mut retval = Vec::new();

    // Get the segments overlapping with the given time range.
    let segment_numbers = self.get_overlapping_segments(range_start_time, range_end_time);

    // Search in each of the segments.
    for segment_number in segment_numbers {
      let segment = self.all_segments_map.get(&segment_number).unwrap();

      let mut results = segment.search(query, range_start_time, range_end_time);
      retval.append(&mut results);
    }
    retval
  }

  /// Helper function to commit a segment with given segment_number to disk.
  fn commit_segment(&self, segment_number: u32, sync_after_write: bool) {
    debug!("Commiting segment with segment_number: {}", segment_number);

    // Get the segment corresponding to the segment_number.
    let segment_ref = self.all_segments_map.get(&segment_number).unwrap();
    let segment = segment_ref.value();

    // Commit this segment.
    let segment_dir_path =
      io::get_joined_path(&self.index_dir_path, segment_number.to_string().as_str());
    segment.commit(segment_dir_path.as_str(), sync_after_write);
  }

  /// Helper function to check if the given segment is 'too big'. In other words, it returns 'true' if the
  /// number of log messages or the number of data points in this segment exceeded the corresponding max values.
  fn is_too_big(&self, segment_number: u32) -> bool {
    // Get the segment corresponding to the segment_number.
    let segment_ref = self.all_segments_map.get(&segment_number).unwrap();
    let segment = segment_ref.value();

    // Check if this segment is 'too big', and return a boolean indicating the same.
    segment.get_log_message_count() > self.metadata.get_approx_max_log_message_count_per_segment()
      || segment.get_data_point_count()
        > self.metadata.get_approx_max_data_point_count_per_segment()
  }

  /// Commit the segment to disk.
  ///
  /// If sync_after_write is set to true, make sure that the OS buffers are flushed to
  /// disk before returning (typically sync_after_write should be set to true in tests that refresh the index
  /// immediately after committing).
  pub fn commit(&self, sync_after_write: bool) {
    info!("Commiting index at {}", chrono::Utc::now());

    // Lock to make sure only one thread calls commit at a time.
    let mut lock = self.index_dir_lock.lock().unwrap();
    *lock = thread::current().id();

    let mut all_segments_map_changed = false;
    let all_segments_list_path =
      io::get_joined_path(&self.index_dir_path, ALL_SEGMENTS_LIST_FILE_NAME);

    if !Path::new(&all_segments_list_path).is_file() {
      // Initial commit - set all_segments_map_changed to true so that all_segments_map is written to disk.
      all_segments_map_changed = true;
    }

    let original_current_segment_number = self.metadata.get_current_segment_number();
    let too_big = self.is_too_big(original_current_segment_number);

    // Create a new segment if the current one has become too big.
    if too_big {
      let new_segment = Segment::new();
      let new_segment_number = self.metadata.fetch_increment_segment_count();
      let new_segment_dir_path = io::get_joined_path(
        &self.index_dir_path,
        new_segment_number.to_string().as_str(),
      );

      // Write the new segment.
      new_segment.commit(new_segment_dir_path.as_str(), sync_after_write);

      // Note that DashMap::insert *may* cause a single-thread deadlock if the thread has a read
      // reference to an item in the map. Make sure that no read reference for all_segments_map
      // is present before the insert and visible in this block.
      self
        .all_segments_map
        .insert(new_segment_number, new_segment);

      self
        .metadata
        .update_current_segment_number(new_segment_number);

      // Commit the new_segment again as there might be more documents added after making it the
      // current segment.
      self.commit_segment(new_segment_number, sync_after_write);

      all_segments_map_changed = true;
    }

    if all_segments_map_changed {
      // Write ids of all segments to disk. Note that these may not be in a sorted order.
      let all_segment_ids: Vec<u32> = self
        .all_segments_map
        .iter()
        .map(|entry| *entry.key())
        .collect();

      serialize::write(
        &all_segment_ids,
        all_segments_list_path.as_str(),
        sync_after_write,
      );
    }

    let metadata_path = io::get_joined_path(&self.index_dir_path, METADATA_FILE_NAME);
    serialize::write(&self.metadata, metadata_path.as_str(), sync_after_write);

    self.commit_segment(original_current_segment_number, sync_after_write);
  }

  /// Read the index from the given index_dir_path.
  pub fn refresh(index_dir_path: &str) -> Result<Self, TsldbError> {
    info!("Refreshing index from index_dir_path: {}", index_dir_path);

    // Check if the directory exists.
    if !Path::new(&index_dir_path).is_dir() {
      return Err(TsldbError::CannotReadDirectory(String::from(
        index_dir_path,
      )));
    }

    // Read all segments map and metadata from disk.
    let all_segments_list_path = io::get_joined_path(index_dir_path, ALL_SEGMENTS_LIST_FILE_NAME);

    if !Path::new(&all_segments_list_path).is_file() {
      return Err(TsldbError::CannotFindIndexMetadataInDirectory(
        String::from(index_dir_path),
      ));
    }

    let all_segment_ids: Vec<u32> = serialize::read(all_segments_list_path.as_str());
    if all_segment_ids.is_empty() {
      // No all_segments_map present - so this may not be an index directory. Return an empty index.
      return Ok(Index::new(index_dir_path).unwrap());
    }

    let metadata_path = io::get_joined_path(index_dir_path, METADATA_FILE_NAME);
    let metadata: Metadata = serialize::read(metadata_path.as_str());

    let all_segments_map: DashMap<u32, Segment> = DashMap::new();
    for id in all_segment_ids {
      let segment_dir_path = io::get_joined_path(index_dir_path, id.to_string().as_str());
      let segment = Segment::refresh(&segment_dir_path);
      all_segments_map.insert(id, segment);
    }

    let index_dir_lock = Arc::new(Mutex::new(thread::current().id()));
    Ok(Index {
      metadata,
      all_segments_map,
      index_dir_path: index_dir_path.to_owned(),
      index_dir_lock,
    })
  }

  /// Returns segment numbers of segments that overlap with the given time range.
  pub fn get_overlapping_segments(&self, range_start_time: u64, range_end_time: u64) -> Vec<u32> {
    let mut segment_numbers = Vec::new();
    for item in &self.all_segments_map {
      if item.value().is_overlap(range_start_time, range_end_time) {
        segment_numbers.push(*item.key());
      }
    }
    segment_numbers
  }

  /// Get time series corresponding to given label name and value, within the given range (inclusive of
  /// both start and end time).
  pub fn get_time_series(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<DataPoint> {
    let mut retval = Vec::new();

    let segment_numbers = self.get_overlapping_segments(range_start_time, range_end_time);
    for segment_number in segment_numbers {
      let segment = self.all_segments_map.get(&segment_number).unwrap();
      let mut data_points =
        segment.get_time_series(label_name, label_value, range_start_time, range_end_time);
      retval.append(&mut data_points);
    }
    retval
  }

  pub fn get_index_dir(&self) -> String {
    self.index_dir_path.to_owned()
  }
}

#[cfg(test)]
mod tests {
  use std::path::Path;
  use std::time::Duration;

  use chrono::Utc;
  use tempdir::TempDir;
  use test_case::test_case;

  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn test_empty_index() {
    is_sync::<Index>();

    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_empty_index"
    );

    let index = Index::new(&index_dir_path).unwrap();
    let segment_ref = index.get_current_segment_ref();
    let segment = segment_ref.value();
    assert_eq!(segment.get_log_message_count(), 0);
    assert_eq!(segment.get_term_count(), 0);
    assert_eq!(index.index_dir_path, index_dir_path);

    // Check that the index directory exists, and has expected structure.
    let base = Path::new(&index_dir_path);
    assert!(base.is_dir());
    assert!(base.join(ALL_SEGMENTS_LIST_FILE_NAME).is_file());
    assert!(base
      .join(
        index
          .metadata
          .get_current_segment_number()
          .to_string()
          .as_str()
      )
      .is_dir());
  }

  #[test]
  fn test_commit_refresh() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_commit_refresh"
    );

    let expected = Index::new(&index_dir_path).unwrap();
    let num_log_messages = 5;
    let message_prefix = "content#";
    let num_data_points = 5;

    for i in 1..=num_log_messages {
      let message = format!("{}{}", message_prefix, i);
      expected.append_log_message(Utc::now().timestamp_millis() as u64, &message);
    }

    let metric_name = "request_count";
    let other_label_name = "method";
    let other_label_value = "GET";
    let mut label_map = HashMap::new();
    label_map.insert(other_label_name.to_owned(), other_label_value.to_owned());
    for i in 1..=num_data_points {
      expected.append_data_point(
        metric_name,
        &label_map,
        Utc::now().timestamp_millis() as u64,
        i as f64,
      );
    }

    expected.commit(false);
    let received = Index::refresh(&index_dir_path).unwrap();

    assert_eq!(&expected.index_dir_path, &received.index_dir_path);
    assert_eq!(
      &expected.all_segments_map.len(),
      &received.all_segments_map.len()
    );

    let expected_segment_ref = expected.get_current_segment_ref();
    let expected_segment = expected_segment_ref.value();
    let received_segment_ref = received.get_current_segment_ref();
    let received_segment = received_segment_ref.value();
    assert_eq!(
      &expected_segment.get_log_message_count(),
      &received_segment.get_log_message_count()
    );
    assert_eq!(
      &expected_segment.get_data_point_count(),
      &received_segment.get_data_point_count()
    );
  }

  #[test]
  fn test_basic_search() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_basic_search"
    );

    let index = Index::new(&index_dir_path).unwrap();
    let num_log_messages = 1000;
    let message_prefix = "this is my log message";
    let mut expected_log_messages: Vec<String> = Vec::new();

    for i in 1..num_log_messages {
      let message = format!("{} {}", message_prefix, i);
      index.append_log_message(Utc::now().timestamp_millis() as u64, &message);
      expected_log_messages.push(message);
    }
    // Now add a unique log message.
    index.append_log_message(Utc::now().timestamp_millis() as u64, "thisisunique");

    // For the query "message", we should expect num_log_messages-1 results.
    // We collect each message in received_log_messages and then compare it with expected_log_messages.
    let mut results = index.search("message", 0, u64::MAX);
    assert_eq!(results.len(), num_log_messages - 1);
    let mut received_log_messages: Vec<String> = Vec::new();
    for i in 1..num_log_messages {
      received_log_messages.push(results.get(i - 1).unwrap().get_message().to_owned());
    }
    assert_eq!(expected_log_messages.sort(), received_log_messages.sort());

    // For the query "thisisunique", we should expect only 1 result.
    results = index.search("thisisunique", 0, u64::MAX);
    assert_eq!(results.len(), 1);
    assert_eq!(results.get(0).unwrap().get_message(), "thisisunique");
  }

  #[test]
  fn test_basic_time_series() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_basic_time_series"
    );

    let index = Index::new(&index_dir_path).unwrap();
    let num_data_points = 1000;
    let mut expected_data_points: Vec<DataPoint> = Vec::new();

    for i in 1..num_data_points {
      index.append_data_point("some_name", &HashMap::new(), i, i as f64);
      let dp = DataPoint::new(i, i as f64);
      expected_data_points.push(dp);
    }

    let metric_name_label = "__name__";
    println!("{}", metric_name_label);
    let received_data_points = index.get_time_series(metric_name_label, "some_name", 0, u64::MAX);

    assert_eq!(expected_data_points, received_data_points);
  }

  #[test_case(true, false; "when only logs are appended")]
  #[test_case(false, true; "when only data points are appended")]
  #[test_case(true, true; "when both logs and data points are appended")]
  fn test_two_segments(append_log: bool, append_data_point: bool) {
    // We run this test multiple times, as it works well to find deadlocks (and doesn't take as much as time as a full test using loom).
    for _ in 0..100 {
      let index_dir = TempDir::new("index_test").unwrap();
      let index_dir_path = format!(
        "{}/{}",
        index_dir.path().to_str().unwrap(),
        "test_two_segments"
      );

      let index = Index::new_with_threshold_params(&index_dir_path, 1000, 2000).unwrap();
      let original_segment_number = index.metadata.get_current_segment_number();
      let original_segment_path =
        Path::new(&index_dir_path).join(original_segment_number.to_string().as_str());

      let message_prefix = "message";
      let mut expected_log_messages: Vec<String> = Vec::new();
      let mut expected_data_points: Vec<DataPoint> = Vec::new();

      let mut original_segment_num_log_messages = if append_log {
        index
          .metadata
          .get_approx_max_log_message_count_per_segment()
      } else {
        0
      };
      let mut original_segment_num_data_points = if append_data_point {
        index.metadata.get_approx_max_data_point_count_per_segment()
      } else {
        0
      };

      for i in 0..original_segment_num_log_messages {
        let message = format!("{} {}", message_prefix, i);
        index.append_log_message(Utc::now().timestamp_millis() as u64, &message);
        expected_log_messages.push(message);
      }

      for _ in 0..original_segment_num_data_points {
        let dp = DataPoint::new(Utc::now().timestamp_millis() as u64, 1.0);
        index.append_data_point("some_name", &HashMap::new(), dp.get_time(), dp.get_value());
        expected_data_points.push(dp);
      }

      // Force commit and then refresh the index.
      // No new segment creation should happen right now as we are at exactly
      // APPROX_MAX_LOG_MESSAGE_COUNT_PER_SEGMENT log messages.
      index.commit(false);
      let index = Index::refresh(&index_dir_path).unwrap();
      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();

        assert_eq!(index.all_segments_map.len(), 1);
        assert_eq!(
          current_segment.get_log_message_count(),
          original_segment_num_log_messages
        );
        assert_eq!(
          current_segment.get_data_point_count(),
          original_segment_num_data_points
        );
      }

      // Now add a log message and/or a data point. This will still land in the one and only segment in the index.
      if append_log {
        index.append_log_message(Utc::now().timestamp_millis() as u64, "some_message_1");
        original_segment_num_log_messages += 1;
      }
      if append_data_point {
        index.append_data_point(
          "some_name",
          &HashMap::new(),
          Utc::now().timestamp_millis() as u64,
          1.0,
        );
        original_segment_num_data_points += 1;
      }

      // Force a commit and refresh. This will now create a second empty segment, which would now be
      // the current segment.
      index.commit(false);
      let index = Index::refresh(&index_dir_path).unwrap();
      let mut original_segment = Segment::refresh(&original_segment_path.to_str().unwrap());

      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();
        assert_eq!(current_segment.get_log_message_count(), 0);
        assert_eq!(current_segment.get_data_point_count(), 0);
      }
      assert_eq!(index.all_segments_map.len(), 2);

      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_data_point_count(),
        original_segment_num_data_points
      );

      let mut new_segment_num_log_messages = 0;
      let mut new_segment_num_data_points = 0;

      // Add one more log message and/or a data point. This will land in the empty current_segment.
      if append_log {
        index.append_log_message(Utc::now().timestamp_millis() as u64, "some_message_2");
        new_segment_num_log_messages += 1;
      }
      if append_data_point {
        index.append_data_point(
          "some_name",
          &HashMap::new(),
          Utc::now().timestamp_millis() as u64,
          1.0,
        );
        new_segment_num_data_points += 1;
      }

      // Force a commit and refresh.
      index.commit(false);
      let index = Index::refresh(&index_dir_path).unwrap();
      original_segment = Segment::refresh(&original_segment_path.to_str().unwrap());

      let current_segment_log_message_count;
      let current_segment_data_point_count;
      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();

        assert_eq!(
          current_segment.get_log_message_count(),
          new_segment_num_log_messages
        );
        assert_eq!(
          current_segment.get_data_point_count(),
          new_segment_num_data_points
        );

        current_segment_log_message_count = current_segment.get_log_message_count();
        current_segment_data_point_count = current_segment.get_data_point_count();
      }

      assert_eq!(index.all_segments_map.len(), 2);
      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_data_point_count(),
        original_segment_num_data_points
      );

      // Commit and refresh a few times. The index should not change.
      index.commit(false);
      let index = Index::refresh(&index_dir_path).unwrap();
      index.commit(false);
      index.commit(false);
      Index::refresh(&index_dir_path).unwrap();
      let index_final = Index::refresh(&index_dir_path).unwrap();
      let index_final_current_segment_ref = index_final.get_current_segment_ref();
      let index_final_current_segment = index_final_current_segment_ref.value();

      assert_eq!(
        index.all_segments_map.len(),
        index_final.all_segments_map.len()
      );
      assert_eq!(index.index_dir_path, index_final.index_dir_path);
      assert_eq!(
        current_segment_log_message_count,
        index_final_current_segment.get_log_message_count()
      );
      assert_eq!(
        current_segment_data_point_count,
        index_final_current_segment.get_data_point_count()
      );
    }
  }

  #[test]
  fn test_multiple_segments_logs() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_multiple_segments_logs"
    );

    let mut index = Index::new_with_threshold_params(&index_dir_path, 1000, 2000).unwrap();
    let message_prefix = "message";
    let num_segments = 3;
    let num_log_messages = num_segments
      * (index
        .metadata
        .get_approx_max_log_message_count_per_segment()
        + 1);

    let mut num_log_messages_from_last_commit = 0;
    for i in 1..=num_log_messages {
      let message = format!("{} {}", message_prefix, i);
      index.append_log_message(Utc::now().timestamp_millis() as u64, &message);

      // Commit immediately after we have indexed more than APPROX_MAX_LOG_MESSAGE_COUNT_PER_SEGMENT messages.
      num_log_messages_from_last_commit += 1;
      if num_log_messages_from_last_commit
        > index
          .metadata
          .get_approx_max_log_message_count_per_segment()
      {
        index.commit(false);
        num_log_messages_from_last_commit = 0;
      }
    }

    index = Index::refresh(&index_dir_path).unwrap();
    let current_segment_ref = index.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // The last commit will create an empty segment, so we will have number of segments to be 1 plus num_segments.
    assert_eq!(index.all_segments_map.len() as u32, num_segments + 1);

    // The current segment in the index will be empty (i.e. will have 0 documents.)
    // Rest of the segments should have APPROX_MAX_LOG_MESSAGE_COUNT_PER_SEGMENT+1 documents.
    assert_eq!(current_segment.get_log_message_count(), 0);

    for item in &index.all_segments_map {
      let segment_number = item.key();
      let segment = item.value();
      if *segment_number == index.metadata.get_current_segment_number() {
        assert_eq!(segment.get_log_message_count(), 0);
      } else {
        assert_eq!(
          segment.get_log_message_count(),
          index
            .metadata
            .get_approx_max_log_message_count_per_segment()
            + 1
        );
      }
    }
  }

  #[test]
  fn test_multiple_segments_data_points() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_multiple_segments_data_points"
    );

    let mut index = Index::new_with_threshold_params(&index_dir_path, 1000, 2000).unwrap();
    let num_segments = 100;
    let num_data_points =
      num_segments * (index.metadata.get_approx_max_data_point_count_per_segment() + 1);

    let mut num_data_points_from_last_commit = 0;
    let start_time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    for _ in 1..=num_data_points {
      index.append_data_point(
        "some_name",
        &label_map,
        Utc::now().timestamp_millis() as u64,
        100.0,
      );

      // Commit immediately after we have indexed more than APPROX_MAX_DATA_POINT_COUNT_PER_SEGMENT messages.
      num_data_points_from_last_commit += 1;
      if num_data_points_from_last_commit
        > index.metadata.get_approx_max_data_point_count_per_segment()
      {
        index.commit(false);
        num_data_points_from_last_commit = 0;
      }
    }

    let end_time = Utc::now().timestamp_millis() as u64;

    index = Index::refresh(&index_dir_path).unwrap();
    let current_segment_ref = index.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // The last commit will create an empty segment, so we will have number of segments to be 1 plus num_segments.
    assert_eq!(index.all_segments_map.len() as u32, num_segments + 1);

    // The current segment in the index will be empty (i.e. will have 0 data points.)
    // Rest of the segments should have APPROX_MAX_DATA_POINT_COUNT_PER_SEGMENT+1 data points.
    assert_eq!(current_segment.get_data_point_count(), 0);

    for item in &index.all_segments_map {
      let segment_id = item.key();
      let segment = item.value();
      if *segment_id == index.metadata.get_current_segment_number() {
        assert_eq!(segment.get_data_point_count(), 0);
      } else {
        assert_eq!(
          segment.get_data_point_count(),
          index.metadata.get_approx_max_data_point_count_per_segment() + 1
        );
      }
    }

    let ts = index.get_time_series(
      "label_name_1",
      "label_value_1",
      start_time - 100,
      end_time + 100,
    );
    assert_eq!(num_data_points, ts.len() as u32)
  }

  #[test]
  fn test_index_dir_does_not_exist() {
    let index_dir = TempDir::new("index_test").unwrap();

    // Create a path within index_dir that does not exist.
    let temp_path_buf = index_dir.path().join("-doesnotexist");
    let index = Index::new(&temp_path_buf.to_str().unwrap()).unwrap();

    // If we don't get any panic/error during commit, that means the commit is successful.
    index.commit(false);
  }

  #[test]
  fn test_refresh_does_not_exist() {
    let index_dir = TempDir::new("index_test").unwrap();
    let temp_path_buf = index_dir.path().join("-doesnotexist");

    // Expect an error when directory isn't present.
    let mut result = Index::refresh(temp_path_buf.to_str().unwrap());
    assert!(result.is_err());

    // Expect an error when metadata file is not present in the directory.
    std::fs::create_dir(temp_path_buf.to_str().unwrap()).unwrap();
    result = Index::refresh(temp_path_buf.to_str().unwrap());
    assert!(result.is_err());
  }

  #[test]
  fn test_overlap_one_segment() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_overlap_one_segment"
    );
    let index = Index::new(&index_dir_path).unwrap();
    index.append_log_message(1000, "message_1");
    index.append_log_message(2000, "message_2");

    assert_eq!(index.get_overlapping_segments(500, 1500).len(), 1);
    assert_eq!(index.get_overlapping_segments(1500, 2500).len(), 1);
    assert_eq!(index.get_overlapping_segments(1500, 1600).len(), 1);
    assert!(index.get_overlapping_segments(500, 600).is_empty());
    assert!(index.get_overlapping_segments(2500, 2600).is_empty());
  }

  #[test]
  fn test_overlap_multiple_segments() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_overlap_multiple_segments"
    );
    let index = Index::new_with_threshold_params(&index_dir_path, 1, 1).unwrap();

    // Setting it high to test out that there is no single-threaded deadlock while commiting.
    // Note that if you change this value, some of the assertions towards the end of this test
    // may need to be changed.
    let num_segments = 100;

    for i in 0..num_segments {
      let start = i * 2 * 1000;
      index.append_log_message(start, "message_1");
      index.append_log_message(start + 1000, "message_2");
      index.commit(false);
    }

    // We'll have num_segments segments, plus one empty segment at the end.
    assert_eq!(index.all_segments_map.len() as u64, num_segments + 1);

    // The first segment will start at time 0 and end at time 1000.
    // The second segment will start at time 2000 and end at time 3000.
    // The third segment will start at time 4000 and end at time 5000.
    // ... and so on.
    assert_eq!(index.get_overlapping_segments(500, 1800).len(), 1);
    assert_eq!(index.get_overlapping_segments(500, 2800).len(), 2);
    assert_eq!(index.get_overlapping_segments(500, 3800).len(), 2);
    assert_eq!(index.get_overlapping_segments(500, 4800).len(), 3);
    assert_eq!(index.get_overlapping_segments(500, 5800).len(), 3);
    assert_eq!(index.get_overlapping_segments(500, 6800).len(), 4);
    assert_eq!(index.get_overlapping_segments(500, 10000).len(), 6);

    assert!(index.get_overlapping_segments(1500, 1800).is_empty());
    assert!(index.get_overlapping_segments(3500, 3800).is_empty());
    assert!(index
      .get_overlapping_segments(num_segments * 1000 * 10, num_segments * 1000 * 20)
      .is_empty());
  }

  #[test]
  fn test_concurrent_append() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_concurrent_append"
    );
    let index = Index::new_with_threshold_params(&index_dir_path, 1000, 2000).unwrap();

    let arc_index = Arc::new(index);
    let num_threads = 20;
    let num_appends_per_thread = 5000;

    let mut handles = Vec::new();

    // Start a thread to commit the index periodically.
    let arc_index_clone = arc_index.clone();
    let ten_millis = Duration::from_millis(10);
    let handle = thread::spawn(move || {
      for _ in 0..100 {
        // We are committing aggressively every few milliseconds - make sure that the contents are flushed to disk.
        arc_index_clone.commit(true);
        thread::sleep(ten_millis);
      }
    });
    handles.push(handle);

    // Start threads to append to the index.
    for i in 0..num_threads {
      let arc_index_clone = arc_index.clone();
      let start = i * num_appends_per_thread;
      let mut label_map = HashMap::new();
      label_map.insert("label1".to_owned(), "value1".to_owned());

      let handle = thread::spawn(move || {
        for j in 0..num_appends_per_thread {
          let time = start + j;
          arc_index_clone.append_log_message(time as u64, "message");
          arc_index_clone.append_data_point("some_name", &label_map, time as u64, 1.0);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    // Commit again to cover the scenario that append threads run for more time than the commit thread,
    arc_index.commit(true);

    let index = Index::refresh(&index_dir_path).unwrap();
    let expected_len = num_threads * num_appends_per_thread;

    let results = index.search("message", 0, expected_len as u64);
    let received_logs_len = results.len();

    let results = index.get_time_series("label1", "value1", 0, expected_len as u64);
    let received_data_points_len = results.len();

    assert_eq!(expected_len, received_logs_len);
    assert_eq!(expected_len, received_data_points_len);
  }

  #[test]
  fn test_reusing_index_when_available() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_reusing_index_when_available"
    );

    let start_time = Utc::now().timestamp_millis();
    // Create a new index
    let index = Index::new_with_threshold_params(&index_dir_path, 1, 1).unwrap();
    index.append_log_message(start_time as u64, "some_message_1");
    index.commit(true);

    // Create one more new index using same dir location
    let index = Index::new_with_threshold_params(&index_dir_path, 1, 1).unwrap();
    let search_result = index.search(
      "some_message_1",
      start_time as u64,
      Utc::now().timestamp_millis() as u64,
    );

    assert_eq!(search_result.len(), 1);
  }

  #[test]
  fn test_directory_without_metadata() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();

    // Create a new index where directory already exist but metadata is not available
    let index = Index::new_with_threshold_params(&index_dir_path, 1, 1);
    assert!(index.is_err());
  }
}

use std::collections::HashMap;
use std::path::Path;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use log::{debug, error, info};

use crate::index_manager::metadata::Metadata;
use crate::index_manager::segment_summary::SegmentSummary;
use crate::log::log_message::LogMessage;
use crate::metric::metric_point::MetricPoint;
use crate::segment_manager::segment::AstNode;
use crate::segment_manager::segment::BoolQuery;
use crate::segment_manager::segment::Segment;

use crate::utils::error::AstError;
use crate::utils::error::CoreDBError;
use crate::utils::error::SearchLogsError;
use crate::utils::error::SegmentError;
use crate::utils::io;
use crate::utils::serialize;
use crate::utils::sync::thread;
use crate::utils::sync::RwLock;
use crate::utils::sync::{Arc, Mutex};
use crate::utils::tokenize::tokenize;

/// File name where the information about all segements is stored.
const ALL_SEGMENTS_FILE_NAME: &str = "all_segments.bin";

/// File name to store index metadata.
const METADATA_FILE_NAME: &str = "metadata.bin";

/// Default threshold for size of segment. A new segment will be created in the next commit when a segment exceeds this size.
const DEFAULT_SEGMENT_SIZE_THRESHOLD_MEGABYTES: f32 = 256.0;

#[derive(Debug)]
/// Index for storing log messages and metric points.
pub struct Index {
  /// Metadata for this index.
  metadata: Metadata,

  /// A reverse-chronological sorted vector of segment summaries.
  all_segments_summaries: Arc<RwLock<Vec<SegmentSummary>>>,

  /// DashMap of segment number to segment - only for the segments that are in memory.
  memory_segments_map: DashMap<u32, Segment>,

  /// Directory where the index is serialized.
  index_dir_path: String,

  /// Mutex for locking the directory where the index is committed / read from, so that two threads
  /// don't write the directory at the same time.
  index_dir_lock: Arc<Mutex<thread::ThreadId>>,
}

impl Index {
  /// Create a new index with default threshold segment size.
  /// However, if a directory with the same path already exists and has a metadata file in it,
  /// the function will refresh the existing index instead of creating a new one.
  /// If the refresh process fails, an error will be thrown to indicate the issue.
  pub fn new(index_dir_path: &str) -> Result<Self, CoreDBError> {
    Index::new_with_threshold_params(index_dir_path, DEFAULT_SEGMENT_SIZE_THRESHOLD_MEGABYTES)
  }

  /// Creates a new index at a specified directory path with customizable parameter for the segment size threshold.
  /// If a directory with the same path already exists and has a metadata
  /// file in it, the existing index will be refreshed instead of creating a new one. If the refresh
  /// process fails, an error will be thrown to indicate the issue.
  pub fn new_with_threshold_params(
    index_dir: &str,
    segment_size_threshold_megabytes: f32,
  ) -> Result<Self, CoreDBError> {
    info!(
      "Creating index - dir {}, segment size threshold in megabytes: {}",
      index_dir, segment_size_threshold_megabytes
    );

    let index_dir_path = Path::new(index_dir);
    if !index_dir_path.is_dir() {
      // Directory does not exist. Create it.
      std::fs::create_dir_all(index_dir_path).unwrap();
    } else if Path::new(&io::get_joined_path(index_dir, METADATA_FILE_NAME)).is_file() {
      // index_dir_path has metadata file, refresh the index instead of creating new one
      match Self::refresh(index_dir) {
        Ok(index) => {
          index
            .metadata
            .update_segment_size_threshold_megabytes(segment_size_threshold_megabytes);
          return Ok(index);
        }
        Err(err) => {
          // Received a error while refreshing index
          return Err(err);
        }
      }
    } else {
      // Check if a directory is empty. We need to skip "." and "..".
      // https://stackoverflow.com/questions/56744383/how-would-i-check-if-a-directory-is-empty-in-rust
      let is_empty = index_dir_path.read_dir().unwrap().next().is_none();

      if !is_empty {
        error!(
          "The directory {} is not empty. Cannot create index in this directory.",
          index_dir
        );
        return Err(CoreDBError::CannotFindIndexMetadataInDirectory(
          String::from(index_dir),
        ));
      }
    }

    // Create an initial segment.
    let segment = Segment::new();
    let metadata = Metadata::new(0, 0, segment_size_threshold_megabytes);

    // Update the initial segment as the current segment.
    let current_segment_number = metadata.fetch_increment_segment_count();
    metadata.update_current_segment_number(current_segment_number);

    // Create the summary for the initial segment.
    let mut all_segments_summaries_vec = Vec::new();
    let current_segment_summary = SegmentSummary::new(current_segment_number, &segment);
    all_segments_summaries_vec.push(current_segment_summary);
    let all_segments_summaries = Arc::new(RwLock::new(all_segments_summaries_vec));

    let memory_segments_map = DashMap::new();
    memory_segments_map.insert(current_segment_number, segment);

    let index_dir_lock = Arc::new(Mutex::new(thread::current().id()));

    let index = Index {
      metadata,
      all_segments_summaries,
      memory_segments_map,
      index_dir_path: index_dir.to_owned(),
      index_dir_lock,
    };

    // Commit the empty index so that the index directory will be created.
    index.commit(false);

    Ok(index)
  }

  /// Get the reference for the current segment.
  fn get_current_segment_ref(&self) -> Ref<u32, Segment> {
    self
      .memory_segments_map
      .get(&self.metadata.get_current_segment_number())
      .unwrap()
  }

  /// Append a log message to the current segment of the index.
  pub fn append_log_message(&self, time: u64, fields: &HashMap<String, String>, message: &str) {
    debug!(
      "Appending log message, time: {}, fields: {:?}, message: {}",
      time, fields, message
    );

    // Get the current segment.
    let current_segment_ref = self.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    current_segment
      .append_log_message(time, fields, message)
      .unwrap();
  }

  /// Append a metric point to the current segment of the index.
  pub fn append_metric_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) {
    debug!(
      "Appending metric point: metric name: {}, labels: {:?}, time: {}, value: {}",
      metric_name, labels, time, value
    );

    // Get the current segment.
    let current_segment_ref = self.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // Append the metric point to the current segment.
    current_segment
      .append_metric_point(metric_name, labels, time, value)
      .unwrap();
  }

  /// Search for given query in the given time range.
  ///
  /// Infino log searches support simple AND queries with URL parameters:
  /// http://infino-endpoint?terms="term1 term2"&start_time=blah&end_time=blah"
  ///
  /// but these can be overridden by a far more complex query dsl in the
  /// json body sent with the query: https://opensearch.org/docs/latest/query-dsl/.
  ///
  /// Note that while the query terms are not required in the URL, the query parameters
  /// "start_time" and "end_time" are required in the URL. They are always added by the
  /// OpenSearch plugin that calls Infino.
  pub fn search_logs(
    &self,
    url_query: &str,
    json_body: serde_json::Value,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<LogMessage>, SearchLogsError> {
    debug!(
      "Search logs for URL query: {:?}, JSON query: {:?}, range_start_time: {}, range_end_time: {}",
      url_query, json_body, range_start_time, range_end_time
    );

    // Check if JSON body is null or an empty object
    let is_json_empty = match json_body {
      serde_json::Value::Null => true,
      serde_json::Value::Object(ref obj) => obj.is_empty(),
      _ => false,
    };

    // Check if URL query is empty
    let is_url_empty = url_query.trim().is_empty();

    // Return an error if no query is provided
    if is_json_empty && is_url_empty {
      return Err(SearchLogsError::NoQueryProvided);
    }

    let ast_result: Result<AstNode, AstError> = if !is_json_empty {
      let bool_query: BoolQuery =
        serde_json::from_value(json_body).map_err(SearchLogsError::JsonParseError)?;

      Segment::query_to_ast(&bool_query)
    } else if !is_url_empty {
      let url_terms = tokenize(url_query);
      Ok(
        url_terms
          .into_iter()
          .map(AstNode::Match)
          .reduce(|a, b| AstNode::Must(Box::new(a), Box::new(b)))
          .unwrap_or(AstNode::None),
      )
    } else {
      Ok(AstNode::None)
    };

    let ast = ast_result.unwrap_or_else(|e| {
      error!("Error building AST: {:?}", e);
      AstNode::None
    });

    let mut retval = Vec::new();
    let segment_numbers = self.get_overlapping_segments(range_start_time, range_end_time);

    for segment_number in segment_numbers {
      let segment =
        self
          .memory_segments_map
          .get(&segment_number)
          .ok_or(SearchLogsError::SegmentError(
            SegmentError::SegmentNotFoundError(segment_number),
          ))?;

      let mut results = segment
        .search_logs(&ast, range_start_time, range_end_time)
        .map_err(SearchLogsError::SegmentSearchError)?;
      retval.append(&mut results);
    }

    retval.sort();
    Ok(retval)
  }

  /// Helper function to commit a segment with given segment_number to disk.
  /// Returns the (uncompressed, compressed) size of the segment.
  fn commit_segment(&self, segment_number: u32, sync_after_write: bool) -> (u64, u64) {
    debug!("Committing segment with segment_number: {}", segment_number);

    // Get the segment corresponding to the segment_number.
    let segment_ref = self.memory_segments_map.get(&segment_number).unwrap();
    let segment = segment_ref.value();

    // Commit this segment.
    let segment_dir_path =
      io::get_joined_path(&self.index_dir_path, segment_number.to_string().as_str());

    segment.commit(segment_dir_path.as_str(), sync_after_write)
  }

  /// Commit a segment to disk.
  ///
  /// If sync_after_write is set to true, make sure that the OS buffers are flushed to
  /// disk before returning (typically sync_after_write should be set to true in tests that refresh the index
  /// immediately after committing).
  pub fn commit(&self, sync_after_write: bool) {
    info!("Committing index at {}", chrono::Utc::now());

    // Lock to make sure only one thread calls commit at a time.
    let mut lock = self.index_dir_lock.lock().unwrap();
    *lock = thread::current().id();

    let mut write_all_segments_file = false;
    let all_segments_file = io::get_joined_path(&self.index_dir_path, ALL_SEGMENTS_FILE_NAME);

    if !Path::new(&all_segments_file).is_file() {
      // Initial commit - set write_all_segments_file to true so that all_segments_file is created.
      write_all_segments_file = true;
    }

    let original_current_segment_number = self.metadata.get_current_segment_number();
    let (uncompressed_segment_size, _compressed_segment_size) =
      self.commit_segment(original_current_segment_number, sync_after_write);
    let segment_size_in_megabytes = (uncompressed_segment_size as f64 / 1024.0 / 1024.0) as f32;

    if segment_size_in_megabytes > self.metadata.get_segment_size_threshold_megabytes() {
      // Create a new segment since the current one has become too big.
      let new_segment = Segment::new();
      let new_segment_number = self.metadata.fetch_increment_segment_count();
      let new_segment_dir_path = io::get_joined_path(
        &self.index_dir_path,
        new_segment_number.to_string().as_str(),
      );

      // Write the new (empty) segment to disk.
      new_segment.commit(new_segment_dir_path.as_str(), sync_after_write);

      // Add the segment to summaries. Insert at the beginning - as this is the most recent segment.
      let summary = SegmentSummary::new(new_segment_number, &new_segment);
      let mut write_lock_summaries = self
        .all_segments_summaries
        .write()
        .expect("Could not get write lock");
      write_lock_summaries.insert(0, summary);

      // Note that DashMap::insert *may* cause a single-thread deadlock if the thread has a read
      // reference to an item in the map. Make sure that no read reference for all_segments_map
      // is present before the insert and visible in this block.
      self
        .memory_segments_map
        .insert(new_segment_number, new_segment);

      // Appends will start going to the new segment after this point.
      self
        .metadata
        .update_current_segment_number(new_segment_number);

      // Commit the new_segment again as there might be more documents added after making it the
      // current segment.
      self.commit_segment(new_segment_number, sync_after_write);

      // Commit the original segment again to commit any updates from the previous commit till the
      // time of changing the current_sgement_number above.
      self.commit_segment(original_current_segment_number, sync_after_write);

      write_all_segments_file = true;
    }

    if write_all_segments_file {
      // Sort the summaries in reverse chronological order.
      let mut write_lock_summaries = self
        .all_segments_summaries
        .write()
        .expect("Could not get write lock for segment summaries");
      write_lock_summaries.sort();
      let summaries: &Vec<SegmentSummary> = write_lock_summaries.as_ref();

      // Write the summaries to disk.
      serialize::write(summaries, all_segments_file.as_str(), sync_after_write);
    }

    let metadata_path = io::get_joined_path(&self.index_dir_path, METADATA_FILE_NAME);
    serialize::write(&self.metadata, metadata_path.as_str(), sync_after_write);
  }

  /// Read the index from the given index_dir_path.
  pub fn refresh(index_dir_path: &str) -> Result<Self, CoreDBError> {
    info!("Refreshing index from index_dir_path: {}", index_dir_path);

    // Check if the directory exists.
    if !Path::new(&index_dir_path).is_dir() {
      return Err(CoreDBError::CannotReadDirectory(String::from(
        index_dir_path,
      )));
    }

    // Read all segments summaries from disk.
    let all_segments_file = io::get_joined_path(index_dir_path, ALL_SEGMENTS_FILE_NAME);

    if !Path::new(&all_segments_file).is_file() {
      return Err(CoreDBError::CannotFindIndexMetadataInDirectory(
        String::from(index_dir_path),
      ));
    }

    let (all_segments_summaries_vec, _): (Vec<SegmentSummary>, _) =
      serialize::read(&all_segments_file);
    if all_segments_summaries_vec.is_empty() {
      // No segment summary present - so this may not be an index directory. Return an empty index.
      return Ok(Index::new(index_dir_path).unwrap());
    }

    let metadata_path = io::get_joined_path(index_dir_path, METADATA_FILE_NAME);
    let (metadata, _): (Metadata, _) = serialize::read(metadata_path.as_str());

    let memory_segments_map: DashMap<u32, Segment> = DashMap::new();
    for segment_summary in &all_segments_summaries_vec {
      let segment_number = segment_summary.get_segment_number();
      let segment_dir_path = io::get_joined_path(index_dir_path, &segment_number.to_string());
      let (segment, _) = Segment::refresh(&segment_dir_path);
      memory_segments_map.insert(segment_number, segment);
    }

    let all_segments_summaries = Arc::new(RwLock::new(all_segments_summaries_vec));

    info!("Read index with metadata {:?}", metadata);

    let index_dir_lock = Arc::new(Mutex::new(thread::current().id()));
    Ok(Index {
      metadata,
      all_segments_summaries,
      memory_segments_map,
      index_dir_path: index_dir_path.to_owned(),
      index_dir_lock,
    })
  }

  /// Returns segment numbers of segments that overlap with the given time range.
  pub fn get_overlapping_segments(&self, range_start_time: u64, range_end_time: u64) -> Vec<u32> {
    let mut segment_numbers = Vec::new();
    for item in &self.memory_segments_map {
      if item.value().is_overlap(range_start_time, range_end_time) {
        segment_numbers.push(*item.key());
      }
    }
    segment_numbers
  }

  /// Get metric points corresponding to given label name and value, within the
  /// given range (inclusive of both start and end time).
  pub fn get_metrics(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<MetricPoint> {
    let mut retval = Vec::new();

    let segment_numbers = self.get_overlapping_segments(range_start_time, range_end_time);
    for segment_number in segment_numbers {
      let segment = self.memory_segments_map.get(&segment_number).unwrap();
      let mut metric_points =
        segment.search_metrics(label_name, label_value, range_start_time, range_end_time);
      retval.append(&mut metric_points);
    }
    retval
  }

  pub fn get_index_dir(&self) -> String {
    self.index_dir_path.to_owned()
  }

  /// Function to delete the index directory.
  pub fn delete(&self) {
    std::fs::remove_dir_all(&self.index_dir_path).unwrap();
  }
}

#[cfg(test)]
mod tests {
  use std::fs::File;
  use std::path::Path;
  use std::thread::sleep;
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
    assert!(base.join(ALL_SEGMENTS_FILE_NAME).is_file());
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
    let num_metric_points = 5;

    for i in 1..=num_log_messages {
      let message = format!("{}{}", message_prefix, i);
      expected.append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        &message,
      );
    }

    let metric_name = "request_count";
    let other_label_name = "method";
    let other_label_value = "GET";
    let mut label_map = HashMap::new();
    label_map.insert(other_label_name.to_owned(), other_label_value.to_owned());
    for i in 1..=num_metric_points {
      expected.append_metric_point(
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
      &expected.memory_segments_map.len(),
      &received.memory_segments_map.len()
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
      &expected_segment.get_metric_point_count(),
      &received_segment.get_metric_point_count()
    );
  }

  #[test]
  fn test_basic_search_logs() {
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
      index.append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        &message,
      );
      expected_log_messages.push(message);
    }
    // Now add a unique log message.
    index.append_log_message(
      Utc::now().timestamp_millis() as u64,
      &HashMap::new(),
      "thisisunique",
    );

    // Prepare an empty JSON body for the query
    let json_body = serde_json::Value::Null;

    // For the query "message", handle errors from search_logs
    let results = match index.search_logs("message", json_body.clone(), 0, u64::MAX) {
      Ok(results) => results,
      Err(err) => {
        eprintln!("Error in search_logs: {:?}", err);
        Vec::new()
      }
    };

    // Continue with assertions
    assert_eq!(results.len(), num_log_messages - 1);
    let mut received_log_messages: Vec<String> = Vec::new();
    for i in 1..num_log_messages {
      received_log_messages.push(results.get(i - 1).unwrap().get_text().to_owned());
    }
    expected_log_messages.sort();
    received_log_messages.sort();
    assert_eq!(expected_log_messages, received_log_messages);

    // For the query "thisisunique", we should expect only 1 result.
    let results = match index.search_logs("thisisunique", json_body, 0, u64::MAX) {
      Ok(results) => results,
      Err(err) => {
        eprintln!("Error in search_logs: {:?}", err);
        Vec::new()
      }
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results.get(0).unwrap().get_text(), "thisisunique");
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
    let num_metric_points = 1000;
    let mut expected_metric_points: Vec<MetricPoint> = Vec::new();

    for i in 1..num_metric_points {
      index.append_metric_point("some_name", &HashMap::new(), i, i as f64);
      let dp = MetricPoint::new(i, i as f64);
      expected_metric_points.push(dp);
    }

    let metric_name_label = "__name__";
    let received_metric_points = index.get_metrics(metric_name_label, "some_name", 0, u64::MAX);

    assert_eq!(expected_metric_points, received_metric_points);
  }

  #[test_case(true, false; "when only logs are appended")]
  #[test_case(false, true; "when only metric points are appended")]
  #[test_case(true, true; "when both logs and metric points are appended")]
  fn test_two_segments(append_log: bool, append_metric_point: bool) {
    // We run this test multiple times, as it works well to find deadlocks (and doesn't take as much as time as a full test using loom).
    for _ in 0..10 {
      let index_dir = TempDir::new("index_test").unwrap();
      let index_dir_path = format!(
        "{}/{}",
        index_dir.path().to_str().unwrap(),
        "test_two_segments"
      );

      // Create an index with a small segment size threshold.
      let index = Index::new_with_threshold_params(&index_dir_path, 0.001).unwrap();

      let original_segment_number = index.metadata.get_current_segment_number();
      let original_segment_path =
        Path::new(&index_dir_path).join(original_segment_number.to_string().as_str());

      let message_prefix = "message";
      let mut expected_log_messages: Vec<String> = Vec::new();
      let mut expected_metric_points: Vec<MetricPoint> = Vec::new();

      let original_segment_num_log_messages = if append_log { 1000 } else { 0 };
      let original_segment_num_metric_points = if append_metric_point { 50000 } else { 0 };

      for i in 0..original_segment_num_log_messages {
        let message = format!("{} {}", message_prefix, i);
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          &message,
        );
        expected_log_messages.push(message);
      }

      for _ in 0..original_segment_num_metric_points {
        let dp = MetricPoint::new(Utc::now().timestamp_millis() as u64, 1.0);
        index.append_metric_point("some_name", &HashMap::new(), dp.get_time(), dp.get_value());
        expected_metric_points.push(dp);
      }

      // Force commit and then refresh the index.
      // This will write one segment to disk and create a new empty segment.
      index.commit(true);

      // Read the index from disk and see that it has expected number of log messages and metric points.
      let index = Index::refresh(&index_dir_path).unwrap();
      let (original_segment, original_segment_size) =
        Segment::refresh(&original_segment_path.to_str().unwrap());
      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_metric_point_count(),
        original_segment_num_metric_points
      );
      assert!(original_segment_size > 0);

      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();

        assert_eq!(index.memory_segments_map.len(), 2);
        assert_eq!(current_segment.get_log_message_count(), 0);
        assert_eq!(current_segment.get_metric_point_count(), 0);
      }

      // Now add a log message and/or a metric point. This will still land in the current (empty) segment in the index.
      let mut new_segment_num_log_messages = 0;
      let mut new_segment_num_metric_points = 0;
      if append_log {
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          "some_message_1",
        );
        new_segment_num_log_messages += 1;
      }
      if append_metric_point {
        index.append_metric_point(
          "some_name",
          &HashMap::new(),
          Utc::now().timestamp_millis() as u64,
          1.0,
        );
        new_segment_num_metric_points += 1;
      }

      // Force a commit and refresh. The index should still have only 2 segments.
      index.commit(true);
      let index = Index::refresh(&index_dir_path).unwrap();
      let (mut original_segment, original_segment_size) =
        Segment::refresh(&original_segment_path.to_str().unwrap());
      assert_eq!(index.memory_segments_map.len(), 2);

      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_metric_point_count(),
        original_segment_num_metric_points
      );
      assert!(original_segment_size > 0);

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
          current_segment.get_metric_point_count(),
          new_segment_num_metric_points
        );
      }

      // Add one more log message and/or a metric point. This should land in the current_segment that has
      // only 1 log message and/or metric point.
      if append_log {
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          "some_message_2",
        );
        new_segment_num_log_messages += 1;
      }
      if append_metric_point {
        index.append_metric_point(
          "some_name",
          &HashMap::new(),
          Utc::now().timestamp_millis() as u64,
          1.0,
        );
        new_segment_num_metric_points += 1;
      }

      // Force a commit and refresh.
      index.commit(false);
      let index = Index::refresh(&index_dir_path).unwrap();
      (original_segment, _) = Segment::refresh(&original_segment_path.to_str().unwrap());

      let current_segment_log_message_count;
      let current_segment_metric_point_count;
      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();
        current_segment_log_message_count = current_segment.get_log_message_count();
        current_segment_metric_point_count = current_segment.get_metric_point_count();

        assert_eq!(
          current_segment_log_message_count,
          new_segment_num_log_messages
        );
        assert_eq!(
          current_segment_metric_point_count,
          new_segment_num_metric_points
        );
      }

      assert_eq!(index.memory_segments_map.len(), 2);
      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_metric_point_count(),
        original_segment_num_metric_points
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
        index.memory_segments_map.len(),
        index_final.memory_segments_map.len()
      );
      assert_eq!(index.index_dir_path, index_final.index_dir_path);
      assert_eq!(
        current_segment_log_message_count,
        index_final_current_segment.get_log_message_count()
      );
      assert_eq!(
        current_segment_metric_point_count,
        index_final_current_segment.get_metric_point_count()
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
    let start_time = Utc::now().timestamp_millis() as u64;

    // Create a new index with a low threshold for the segment size.
    let mut index = Index::new_with_threshold_params(&index_dir_path, 0.001).unwrap();

    let message_prefix = "message";
    let num_log_messages = 10000;
    let commit_after = 1000;

    // Append log messages.
    let mut num_log_messages_from_last_commit = 0;
    for i in 1..=num_log_messages {
      let message = format!("{} {}", message_prefix, i);
      index.append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        &message,
      );

      // Commit after indexing more than commit_after messages.
      num_log_messages_from_last_commit += 1;
      if num_log_messages_from_last_commit > commit_after {
        index.commit(false);
        num_log_messages_from_last_commit = 0;
        sleep(Duration::from_millis(1000));
      }
    }

    // Commit and sleep to ensure the index is written to disk.
    index.commit(true);
    sleep(Duration::from_millis(1000));

    let end_time = Utc::now().timestamp_millis() as u64;

    // Read the index from disk.
    index = match Index::refresh(&index_dir_path) {
      Ok(index) => index,
      Err(err) => {
        eprintln!("Error refreshing index: {:?}", err);
        return;
      }
    };

    // Ensure that more than 1 segment was created.
    assert!(index.memory_segments_map.len() > 1);

    // The current segment should be empty (i.e., have 0 documents).
    let current_segment_ref = index.get_current_segment_ref();
    let current_segment = current_segment_ref.value();
    assert_eq!(current_segment.get_log_message_count(), 0);

    for item in &index.memory_segments_map {
      let segment_number = item.key();
      let segment = item.value();
      if *segment_number == index.metadata.get_current_segment_number() {
        assert_eq!(segment.get_log_message_count(), 0);
      }
    }

    // Prepare an empty JSON body for the queries.
    let json_body = serde_json::Value::Null;

    // Ensure the prefix is in every log message.
    let results = match index.search_logs(message_prefix, json_body.clone(), start_time, end_time) {
      Ok(results) => results,
      Err(err) => {
        eprintln!("Error searching logs: {:?}", err);
        return;
      }
    };
    assert_eq!(results.len(), num_log_messages);

    // Ensure the suffix is in exactly one log message.
    for i in 1..=num_log_messages {
      let suffix = &format!("{}", i);
      let results = match index.search_logs(suffix, json_body.clone(), start_time, end_time) {
        Ok(results) => results,
        Err(err) => {
          eprintln!("Error searching logs: {:?}", err);
          return;
        }
      };
      assert_eq!(results.len(), 1);
    }

    // Ensure the prefix+suffix is in exactly one log message.
    for i in 1..=num_log_messages {
      let message = &format!("{} {}", message_prefix, i);
      let results = match index.search_logs(message, json_body.clone(), start_time, end_time) {
        Ok(results) => results,
        Err(err) => {
          eprintln!("Error searching logs: {:?}", err);
          return;
        }
      };
      assert_eq!(results.len(), 1);
    }
  }

  #[test]
  fn test_search_logs_count() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_search_logs_count"
    );

    let index = Index::new_with_threshold_params(&index_dir_path, 1.0).unwrap();
    let message_prefix = "message";
    let num_message_suffixes = 20;

    // Create tokens with different numeric message suffixes
    for i in 1..num_message_suffixes {
      let message = &format!("{}{}", message_prefix, i);
      let count = 2u32.pow(i);
      for _ in 0..count {
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          &message,
        );
      }
      index.commit(false);
    }

    // Prepare an empty JSON body for the queries
    let json_body = serde_json::Value::Null;

    for i in 1..num_message_suffixes {
      let message = &format!("{}{}", message_prefix, i);
      let expected_count = 2u32.pow(i);
      let results = index.search_logs(
        message,
        json_body.clone(),
        0,
        Utc::now().timestamp_millis() as u64,
      );

      match results {
        Ok(logs) => {
          assert_eq!(expected_count, logs.len() as u32);
        }
        Err(err) => {
          eprintln!("Error in search_logs for '{}': {:?}", message, err);
        }
      }
    }
  }

  #[test]
  fn test_multiple_segments_metric_points() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_multiple_segments_metric_points"
    );

    // Create an index with a low threshold for segment size.
    let mut index = Index::new_with_threshold_params(&index_dir_path, 0.0001).unwrap();
    let num_metric_points = 10000;
    let mut num_metric_points_from_last_commit = 0;
    let commit_after = 1000;

    // Append metric points to the index.
    let start_time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    for _ in 1..=num_metric_points {
      index.append_metric_point(
        "some_name",
        &label_map,
        Utc::now().timestamp_millis() as u64,
        100.0,
      );
      num_metric_points_from_last_commit += 1;

      // Commit after we have indexed more than commit_after messages.
      if num_metric_points_from_last_commit >= commit_after {
        index.commit(false);
        num_metric_points_from_last_commit = 0;
      }
    }
    // Commit and sleep to make sure the index is written to disk.
    index.commit(true);
    sleep(Duration::from_millis(1000));

    let end_time = Utc::now().timestamp_millis() as u64;

    // Refresh the segment from disk.
    index = Index::refresh(&index_dir_path).unwrap();
    let current_segment_ref = index.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // Make sure that more than 1 segment got created.
    assert!(index.memory_segments_map.len() > 1);

    // The current segment in the index will be empty (i.e. will have 0 metric points.)
    assert_eq!(current_segment.get_metric_point_count(), 0);
    for item in &index.memory_segments_map {
      let segment_id = item.key();
      let segment = item.value();
      if *segment_id == index.metadata.get_current_segment_number() {
        assert_eq!(segment.get_metric_point_count(), 0);
      }
    }

    // The number of metric points in the index should be equal to the number of metric points we indexed.
    let ts = index.get_metrics(
      "label_name_1",
      "label_value_1",
      start_time - 100,
      end_time + 100,
    );
    assert_eq!(num_metric_points, ts.len() as u32)
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
    index.append_log_message(1000, &HashMap::new(), "message_1");
    index.append_log_message(2000, &HashMap::new(), "message_2");

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
    let index = Index::new_with_threshold_params(&index_dir_path, 0.0003).unwrap();

    // Setting it high to test out that there is no single-threaded deadlock while commiting.
    // Note that if you change this value, some of the assertions towards the end of this test
    // may need to be changed.
    let num_segments = 20;

    for i in 0..num_segments {
      let start = i * 2 * 1000;
      index.append_log_message(start, &HashMap::new(), "message_1");
      index.append_log_message(start + 500, &HashMap::new(), "message_2");
      index.commit(false);
    }

    // We'll have num_segments segments, plus one empty segment at the end.
    assert_eq!(index.memory_segments_map.len() as u64, num_segments + 1);

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
    let index = Index::new_with_threshold_params(&index_dir_path, 1.0).unwrap();

    let arc_index = Arc::new(index);
    let num_threads = 20;
    let num_appends_per_thread = 5000;

    let mut handles = Vec::new();

    // Start a thread to commit the index periodically.
    let arc_index_clone = arc_index.clone();
    let ten_millis = Duration::from_millis(10);
    let handle = thread::spawn(move || {
      for _ in 0..100 {
        arc_index_clone.commit(true);
        sleep(ten_millis);
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
          arc_index_clone.append_log_message(time as u64, &HashMap::new(), "message");
          arc_index_clone.append_metric_point("some_name", &label_map, time as u64, 1.0);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    // Commit again to cover the scenario that append threads run for more time than the commit thread
    arc_index.commit(true);

    let index = Index::refresh(&index_dir_path).unwrap();
    let expected_len = num_threads * num_appends_per_thread;

    // Prepare an empty JSON body for the query
    let json_body = serde_json::Value::Null;

    let results = index.search_logs("message", json_body.clone(), 0, expected_len as u64);
    match results {
      Ok(logs) => {
        let received_logs_len = logs.len();
        assert_eq!(expected_len, received_logs_len);
      }
      Err(err) => {
        eprintln!("Error in search_logs: {:?}", err);
      }
    }

    let results = index.get_metrics("label1", "value1", 0, expected_len as u64);
    let received_metric_points_len = results.len();

    assert_eq!(expected_len, results.len());
    assert_eq!(expected_len, received_metric_points_len);
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
    let index = Index::new_with_threshold_params(&index_dir_path, 1.0).unwrap();
    index.append_log_message(start_time as u64, &HashMap::new(), "some_message_1");
    index.commit(true);

    // Create one more new index using the same dir location
    let index = Index::new_with_threshold_params(&index_dir_path, 1.0).unwrap();

    // Prepare an empty JSON body for the query
    let json_body = serde_json::Value::Null;

    // Call search_logs and handle errors
    let search_result = index.search_logs(
      "some_message_1",
      json_body,
      start_time as u64,
      Utc::now().timestamp_millis() as u64,
    );

    // Check if there was an error calling search_logs.
    if let Err(err) = search_result {
      eprintln!("Error in search_logs: {:?}", err);
    } else {
      // Assert the results when there's no error.
      assert_eq!(search_result.unwrap().len(), 1);
    }
  }

  #[test]
  fn test_directory_without_metadata() {
    // Create a new index in an empty directory - this should work.
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let index = Index::new_with_threshold_params(&index_dir_path, 1.0);
    assert!(index.is_ok());

    // Create a new index in an non-empty directory that does not have metadata - this should give an error.
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let file_path = index_dir.path().join("my_file.txt");
    let _ = File::create(&file_path).unwrap();
    let index = Index::new_with_threshold_params(&index_dir_path, 1.0);
    assert!(index.is_err());
  }
}

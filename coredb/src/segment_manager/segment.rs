// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::collections::HashMap;
use std::vec::Vec;

use dashmap::DashMap;
use log::debug;

use super::metadata::Metadata;
use super::search_logs::QueryLogMessage;
use crate::log::inverted_map::InvertedMap;
use crate::log::log_message::LogMessage;
use crate::log::postings_list::PostingsList;
use crate::metric::time_series::TimeSeries;
use crate::metric::time_series_map::TimeSeriesMap;
use crate::storage_manager::storage::Storage;
use crate::utils::error::CoreDBError;
use crate::utils::error::QueryError;
use crate::utils::io::get_joined_path;
use crate::utils::range::is_overlap;
use crate::utils::sync::thread;
use crate::utils::sync::{Arc, RwLock, TokioMutex};

const METADATA_FILE_NAME: &str = "metadata.bin";
const TERMS_FILE_NAME: &str = "terms.bin";
const INVERTED_MAP_FILE_NAME: &str = "inverted_map.bin";
const FORWARD_MAP_FILE_NAME: &str = "forward_map.bin";
const LABELS_FILE_NAME: &str = "labels.bin";
const TIME_SERIES_FILE_NAME: &str = "time_series.bin";

/// A segment with inverted map (term-ids to log-message-ids) as well
/// as forward map (log-message-ids to log messages).
#[derive(Debug)]
pub struct Segment {
  /// Metadata for this segment.
  metadata: Metadata,

  /// Terms present in this segment.
  /// Applicable only for log messages.
  terms: DashMap<String, u32>,

  /// Inverted map - term-id to a postings list.
  /// Applicable only for log messages.
  inverted_map: InvertedMap,

  /// Forward map - log_message-id to the corresponding log message.
  /// Applicable only for log messages.
  forward_map: DashMap<u32, LogMessage>,

  /// Labels present in this segment.
  /// Applicable only for time series.
  labels: DashMap<String, u32>,

  // Time series map - label-id to corresponding time series.
  // Applicable only for time series.
  time_series_map: TimeSeriesMap,

  // Mutex for only one thread to commit this segment at a time.
  commit_lock: TokioMutex<thread::ThreadId>,
}

impl Segment {
  /// Create an empty segment.
  pub fn new() -> Self {
    Segment {
      metadata: Metadata::new(),
      terms: DashMap::new(),
      forward_map: DashMap::new(),
      inverted_map: InvertedMap::new(),
      labels: DashMap::new(),
      time_series_map: TimeSeriesMap::new(),
      commit_lock: TokioMutex::new(thread::current().id()),
    }
  }

  /// Get the terms in this segment.
  pub fn get_terms(&self) -> &DashMap<String, u32> {
    &self.terms
  }

  /// Get the term entry in this segment.
  pub fn get_term(&self, term: &str) -> Option<u32> {
    let result = self.terms.get(term);
    result.map(|result| *result.value())
  }

  /// Get a PostingsList for a given term
  pub fn get_postings_list(&self, term: &str) -> Option<Arc<RwLock<PostingsList>>> {
    // Attempt to find the term in the terms DashMap
    self.terms.get(term).and_then(|term_id| {
      // If found, use the term_id to look up the corresponding PostingsList in the inverted_map
      self.inverted_map.get_postings_list(*term_id)
    })
  }

  /// Get the forward map for this segment.
  pub fn get_forward_map(&self) -> &DashMap<u32, LogMessage> {
    &self.forward_map
  }

  pub fn get_labels(&self) -> &DashMap<String, u32> {
    &self.labels
  }

  pub fn get_time_series_map(&self) -> &TimeSeriesMap {
    &self.time_series_map
  }

  /// Get id of this segment.
  pub fn get_id(&self) -> &str {
    self.metadata.get_id()
  }

  /// Get log message count of this segment.
  pub fn get_log_message_count(&self) -> u32 {
    self.metadata.get_log_message_count()
  }

  /// Get the number of terms in this segment.
  pub fn get_term_count(&self) -> u32 {
    self.metadata.get_term_count()
  }

  /// Get the number of labels in this segment.
  pub fn get_label_count(&self) -> u32 {
    self.metadata.get_label_count()
  }

  /// Get the number of metric points in this segment.
  pub fn get_metric_point_count(&self) -> u32 {
    self.metadata.get_metric_point_count()
  }

  /// Get the earliest time in this segment.
  pub fn get_start_time(&self) -> u64 {
    self.metadata.get_start_time()
  }

  /// Get the latest time in this segment.
  pub fn get_end_time(&self) -> u64 {
    self.metadata.get_end_time()
  }

  /// Get the uncompressed size.
  pub fn get_uncompressed_size(&self) -> u64 {
    self.metadata.get_uncompressed_size()
  }

  /// Check if this segment is empty.
  pub fn is_empty(&self) -> bool {
    self.metadata.get_log_message_count() == 0
      && self.metadata.get_term_count() == 0
      && self.terms.is_empty()
      && self.forward_map.is_empty()
      && self.inverted_map.is_empty()
      && self.labels.is_empty()
      && self.time_series_map.is_empty()
  }

  // This functions with #[cfg(test)] annotation below should only be used in testing -
  // we should never insert directly in inverted map or in terms map.
  // (as otherwise it would compromise integrity of the segment - e.g, we may have an entry in inverted
  // map for a term, but not in terms or corresponding document in the forward map).
  #[cfg(test)]
  pub fn insert_in_inverted_map(&self, term_id: u32, postings_list: PostingsList) {
    self.inverted_map.insert_unchecked(term_id, postings_list);
  }
  #[cfg(test)]
  pub fn insert_in_terms(&self, term: &str, term_id: u32) {
    self.terms.insert(term.to_owned(), term_id);
  }
  #[cfg(test)]
  pub fn clear_inverted_map(&self) {
    self.inverted_map.clear_inverted_map();
  }

  /// Append a log message with timestamp to the segment (inverted as well as forward map).
  pub fn append_log_message(
    &self,
    time: u64,
    fields: &HashMap<String, String>,
    text: &str,
  ) -> Result<(), CoreDBError> {
    let log_message = LogMessage::new_with_fields_and_text(time, fields, text);
    let terms = log_message.get_terms();

    // Increment the number of log messages appended so far, and get the id for this log message.
    let log_message_id = self.metadata.fetch_increment_log_message_count();

    // Update the inverted map.
    terms.into_iter().for_each(|term| {
      let term_id = *self
        .terms
        .entry(term)
        .or_insert_with(|| self.metadata.fetch_increment_term_count());

      self
        .inverted_map
        .append(term_id, log_message_id)
        .expect("Could not append to postings list");
    });

    // Insert in the forward map.
    self.forward_map.insert(log_message_id, log_message);

    // Update the start and end time for this segment.
    self.update_start_end_time(time);

    Ok(())
  }

  /// Append a metric point with specified time and value to the segment.
  pub fn append_metric_point(
    &self,
    metric_name: &str,
    name_value_labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) -> Result<(), CoreDBError> {
    // Increment the number of metric points appended so far.
    self.metadata.fetch_increment_metric_point_count();

    let mut my_labels = Vec::new();

    // Push the metric name label.
    my_labels.push(TimeSeries::get_label_for_metric_name(metric_name));

    // Push the rest of the name-value labels.
    for (name, value) in name_value_labels.iter() {
      my_labels.push(TimeSeries::get_label(name, value));
    }

    // my_labels should no longer be mutable.
    let my_labels = my_labels;

    for label in my_labels {
      // We actually mutate this variable in the match block below, so suppress the warning.
      #[allow(unused_mut)]
      let mut label_id: u32;

      // Need to lock the shard that contains the label, so that some other thread doesn't insert the same label.
      // Use the entry api - https://github.com/xacrimon/dashmap/issues/169#issuecomment-1009920032
      {
        let entry = self
          .labels
          .entry(label.to_owned())
          .or_insert(self.metadata.fetch_increment_label_count());
        label_id = *entry;
      }

      // Need to lock the shard that contains the label_id, so that some other thread doesn't insert the same label_id.
      // Add this in a separate block to minimize the locking time.
      {
        self.time_series_map.append(label_id, time, value)?;
      }
    } // end for label in my_labels

    self.update_start_end_time(time);
    Ok(())
  }

  pub async fn commit(&self, storage: &Storage, dir: &str) -> Result<(u64, u64), CoreDBError> {
    // Acquire a lock - so that only one thread can commit at a time.
    let mut lock = self.commit_lock.lock().await;
    *lock = thread::current().id();

    // Function to serialize a component to a given path.
    async fn serialize_component<T: serde::Serialize>(
      component: &T,
      path: String,
      storage: &Storage,
    ) -> (u64, u64) {
      // TODO: handle the error gracefully.
      storage
        .write(component, &path)
        .await
        .unwrap_or_else(|_| panic!("Could not write to file {}", path))
    }

    // Serialize each of the components of a segment. Run these concurrently in the same task using tokio::join.
    let (terms_result, inverted_map_result, forward_map_result, labels_result, time_series_result) = tokio::join!(
      serialize_component(&self.terms, get_joined_path(dir, TERMS_FILE_NAME), storage),
      serialize_component(
        &self.inverted_map,
        get_joined_path(dir, INVERTED_MAP_FILE_NAME),
        storage
      ),
      serialize_component(
        &self.forward_map,
        get_joined_path(dir, FORWARD_MAP_FILE_NAME),
        storage
      ),
      serialize_component(
        &self.labels,
        get_joined_path(dir, LABELS_FILE_NAME),
        storage
      ),
      serialize_component(
        &self.time_series_map,
        get_joined_path(dir, TIME_SERIES_FILE_NAME),
        storage
      ),
    );

    // Calculate uncompressed and compressed segment size.
    let (uncompressed_segment_size, compressed_segment_size) = (
      terms_result.0
        + inverted_map_result.0
        + forward_map_result.0
        + labels_result.0
        + time_series_result.0,
      terms_result.1
        + inverted_map_result.1
        + forward_map_result.1
        + labels_result.1
        + time_series_result.1,
    );

    // Update the metadata with segment size.
    let (uncompressed_metadata_size, compressed_metadata_size) = self.metadata.get_metadata_size();
    let uncompressed_segment_size = uncompressed_segment_size + uncompressed_metadata_size;
    let compressed_segment_size = compressed_segment_size + compressed_metadata_size;
    self
      .metadata
      .update_segment_size(uncompressed_segment_size, compressed_segment_size);
    storage
      .write(&self.metadata, &get_joined_path(dir, METADATA_FILE_NAME))
      .await?;

    debug!(
      "Serialized segment to {} bytes uncompressed, {} bytes compressed",
      uncompressed_segment_size, compressed_segment_size
    );

    Ok((uncompressed_segment_size, compressed_segment_size))
  }

  /// Read the segment from the specified directory.
  pub async fn refresh(storage: &Storage, dir: &str) -> Result<(Segment, u64), CoreDBError> {
    let metadata_path = get_joined_path(dir, METADATA_FILE_NAME);
    let terms_path = get_joined_path(dir, TERMS_FILE_NAME);
    let inverted_map_path = get_joined_path(dir, INVERTED_MAP_FILE_NAME);
    let forward_map_path = get_joined_path(dir, FORWARD_MAP_FILE_NAME);
    let labels_path = get_joined_path(dir, LABELS_FILE_NAME);
    let time_series_map_path = get_joined_path(dir, TIME_SERIES_FILE_NAME);

    let (metadata, metadata_size): (Metadata, _) = storage.read(&metadata_path).await?;
    let (terms, terms_size): (DashMap<String, u32>, _) = storage.read(&terms_path).await?;
    let (inverted_map, inverted_map_size): (InvertedMap, _) =
      storage.read(&inverted_map_path).await?;
    let (forward_map, forward_map_size): (DashMap<u32, LogMessage>, _) =
      storage.read(&forward_map_path).await?;
    let (labels, labels_size): (DashMap<String, u32>, _) = storage.read(&labels_path).await?;
    let (time_series_map, time_series_map_size): (TimeSeriesMap, _) =
      storage.read(&time_series_map_path).await?;
    let commit_lock = TokioMutex::new(thread::current().id());

    let total_size = metadata_size
      + terms_size
      + inverted_map_size
      + forward_map_size
      + labels_size
      + time_series_map_size;

    let segment = Segment {
      metadata,
      terms,
      inverted_map,
      forward_map,
      labels,
      time_series_map,
      commit_lock,
    };

    Ok((segment, total_size))
  }

  /// Return the log messages within the given time range corresponding to the given log message ids.
  pub fn get_log_messages_from_ids(
    &self,
    log_message_ids: &[u32],
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<QueryLogMessage>, QueryError> {
    let mut log_messages = Vec::<QueryLogMessage>::new();
    for log_message_id in log_message_ids {
      let retval = self
        .forward_map
        .get(log_message_id)
        .ok_or(QueryError::LogMessageNotFound(*log_message_id))?;
      let log_message = retval.value();
      let time = log_message.get_time();
      if time >= range_start_time && time <= range_end_time {
        log_messages.push(QueryLogMessage::new_with_params(
          *log_message_id,
          LogMessage::new_with_fields_and_text(
            time,
            log_message.get_fields(),
            log_message.get_text(),
          ),
        ));
      }
    }

    Ok(log_messages)
  }

  /// Returns true if this segment overlaps with the given range.
  pub fn is_overlap(&self, range_start_time: u64, range_end_time: u64) -> bool {
    is_overlap(
      self.metadata.get_start_time(),
      self.metadata.get_end_time(),
      range_start_time,
      range_end_time,
    )
  }

  /// Update the start and end time of this segment.
  fn update_start_end_time(&self, time: u64) {
    // Update start and end timestamps.
    if time > self.metadata.get_end_time() {
      self.metadata.update_end_time(time);
    }

    if time < self.metadata.get_start_time() {
      self.metadata.update_start_time(time);
    }
  }

  #[cfg(test)]
  pub fn get_metadata_file_name() -> String {
    METADATA_FILE_NAME.to_owned()
  }
}

impl Default for Segment {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use crate::utils::sync::{Arc, RwLock};

  use chrono::Utc;
  use log::error;
  use pest::Parser;
  use tempdir::TempDir;

  use super::*;
  use crate::metric::constants::MetricsQueryCondition;
  use crate::metric::metric_point::MetricPoint;
  use crate::request_manager::query_dsl::{QueryDslParser, Rule};
  use crate::storage_manager::storage::StorageType;
  use crate::utils::config::config_test_logger;
  use crate::utils::sync::{is_sync_send, thread};
  use pest::iterators::Pairs;

  fn create_term_test_node(term: &str) -> Result<Pairs<Rule>, Box<pest::error::Error<Rule>>> {
    config_test_logger();

    let test_string = term;
    let result = QueryDslParser::parse(Rule::start, test_string);

    match result {
      Ok(pairs) => Ok(pairs),

      // The error can be arbitrarily large. To utilize the stack effectively, Box the error and return.
      Err(e) => Err(Box::new(e)),
    }
  }

  #[tokio::test]
  async fn test_new_segment() {
    is_sync_send::<Segment>();

    let segment = Segment::new();
    assert!(segment.is_empty());

    let query_node_result = create_term_test_node("doesnotexist");

    if let Ok(query_node) = query_node_result {
      if let Err(err) = segment.search_logs(&query_node, 0, u64::MAX).await {
        error!("Error in search_logs: {:?}", err);
      } else {
        let results = segment.search_logs(&query_node, 0, u64::MAX).await.unwrap();
        assert!(results.get_messages().is_empty());
      }
    } else {
      error!("Error parsing the query.");
    }
  }

  #[tokio::test]
  async fn test_default_segment() {
    let segment = Segment::default();
    assert!(segment.is_empty());

    let query_node_result = create_term_test_node("doesnotexist");

    if let Ok(query_node) = query_node_result {
      if let Err(err) = segment.search_logs(&query_node, 0, u64::MAX).await {
        error!("Error in search_logs: {:?}", err);
      } else {
        let results = segment.search_logs(&query_node, 0, u64::MAX).await.unwrap();
        assert!(results.get_messages().is_empty());
      }
    } else {
      error!("Error parsing the query.");
    }
  }

  #[tokio::test]
  async fn test_commit_refresh() {
    let original_segment = Segment::new();
    let segment_dir = TempDir::new("segment_test").unwrap();
    let segment_dir_path = segment_dir.path().to_str().unwrap();
    let storage = Storage::new(&StorageType::Local)
      .await
      .expect("Could not create storage");

    original_segment
      .append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        "this is my 1st log message",
      )
      .unwrap();

    let metric_name = "request_count";
    let other_label_name = "method";
    let other_label_value = "GET";
    let mut label_map: HashMap<String, String> = HashMap::new();
    label_map.insert(other_label_name.to_owned(), other_label_value.to_owned());
    original_segment
      .append_metric_point(
        metric_name,
        &label_map,
        Utc::now().timestamp_millis() as u64,
        100.0,
      )
      .unwrap();

    // Commit so that the segment is serialized to disk, and refresh it from disk.
    let (uncompressed_original_segment_size, compressed_original_segment_size) = original_segment
      .commit(&storage, segment_dir_path)
      .await
      .expect("Error while commmiting segment");
    assert!(uncompressed_original_segment_size > 0);
    assert!(compressed_original_segment_size > 0);

    let (from_disk_segment, from_disk_segment_size) = Segment::refresh(&storage, segment_dir_path)
      .await
      .expect("Error while refreshing segment");

    // Verify that both the segments are equal.
    assert_eq!(
      from_disk_segment.get_log_message_count(),
      original_segment.get_log_message_count()
    );
    assert_eq!(
      from_disk_segment.get_metric_point_count(),
      original_segment.get_metric_point_count()
    );

    // Verify that the segment size from disk is almost the same as the original segment size.
    assert!(i64::abs((from_disk_segment_size - uncompressed_original_segment_size) as i64) < 32);

    // Test metadata.
    assert_eq!(from_disk_segment.metadata.get_log_message_count(), 1);
    assert_eq!(from_disk_segment.metadata.get_label_count(), 2);
    assert_eq!(from_disk_segment.metadata.get_metric_point_count(), 1);
    assert_eq!(from_disk_segment.metadata.get_term_count(), 6); // 6 terms in "this is my 1st log message"

    // Test terms map.
    assert!(from_disk_segment.terms.contains_key("1st"));

    // Test labels.
    let metric_name_key = TimeSeries::get_label_for_metric_name(metric_name);
    let other_label_key = TimeSeries::get_label(other_label_name, other_label_value);
    assert!(from_disk_segment.labels.contains_key(&metric_name_key));
    assert!(from_disk_segment.labels.contains_key(&other_label_key));

    // Test time series.
    assert_eq!(from_disk_segment.metadata.get_metric_point_count(), 1);
    let result = from_disk_segment.labels.get(&metric_name_key).unwrap();
    let metric_name_id = *result.value();
    let other_result = from_disk_segment.labels.get(&other_label_key).unwrap();
    let other_label_id = *other_result.value();
    let ts = from_disk_segment
      .time_series_map
      .get_time_series(metric_name_id)
      .unwrap();
    {
      let ts = ts.clone();
      let ts = &*ts.read();
      let other_label_ts = from_disk_segment
        .time_series_map
        .get_time_series(other_label_id)
        .unwrap();
      let other_label_ts = other_label_ts.read();
      assert!(ts.eq(&other_label_ts));
      assert_eq!(ts.get_compressed_blocks().len(), 0);
      assert_eq!(ts.get_initial_times().len(), 1);
      assert_eq!(ts.get_last_block().len(), 1);
      assert_eq!(
        ts.get_last_block()
          .get_metric_points()
          .first()
          .unwrap()
          .get_value(),
        100.0
      );
    }

    let query_node_result_for_this = create_term_test_node("this");

    if let Ok(query_node_for_this) = query_node_result_for_this {
      let results = from_disk_segment
        .search_logs(&query_node_for_this, 0, u64::MAX)
        .await;
      match &results {
        Ok(logs) => {
          assert_eq!(logs.get_messages().len(), 1);
          assert_eq!(
            logs
              .get_messages()
              .first()
              .unwrap()
              .get_message()
              .get_text(),
            "this is my 1st log message"
          );
        }
        Err(err) => {
          error!("Error in search_logs for 'this': {:?}", err);
        }
      }
    } else {
      error!("Error parsing the query for 'this'.");
    }

    // Test search for "blah".
    let query_node_result_for_blah = create_term_test_node("blah");

    if let Ok(query_node_for_blah) = query_node_result_for_blah {
      let results = from_disk_segment
        .search_logs(&query_node_for_blah, 0, u64::MAX)
        .await;
      match results {
        Ok(logs) => {
          assert!(logs.get_messages().is_empty());
        }
        Err(err) => {
          error!("Error in search_logs for 'blah': {:?}", err);
        }
      }
    } else {
      error!("Error parsing the query for 'blah'.");
    }

    // Test metadata for labels.
    assert_eq!(from_disk_segment.metadata.get_label_count(), 2);
  }

  #[test]
  fn test_one_log_message() {
    let segment = Segment::new();
    let time = Utc::now().timestamp_millis() as u64;

    segment
      .append_log_message(time, &HashMap::new(), "some log message")
      .unwrap();

    assert_eq!(segment.metadata.get_start_time(), time);
    assert_eq!(segment.metadata.get_end_time(), time);
  }

  #[tokio::test]
  async fn test_one_metric_point() {
    let segment = Segment::new();
    let time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    segment
      .append_metric_point("metric_name_1", &label_map, time, 100.0)
      .unwrap();

    assert_eq!(segment.metadata.get_start_time(), time);
    assert_eq!(segment.metadata.get_end_time(), time);

    let mut labels = HashMap::new();
    labels.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    let results = segment
      .search_metrics(
        &labels,
        &MetricsQueryCondition::Equals,
        time - 100,
        time + 100,
      )
      .await
      .unwrap();

    assert_eq!(results.len(), 1)
  }

  #[test]
  fn test_multiple_log_messages() {
    let num_messages = 1000;
    let segment = Segment::new();

    let start_time = Utc::now().timestamp_millis() as u64;
    for _ in 0..num_messages {
      segment
        .append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          "some log message",
        )
        .unwrap();
    }
    let end_time = Utc::now().timestamp_millis() as u64;

    assert!(segment.metadata.get_start_time() >= start_time);
    assert!(segment.metadata.get_end_time() <= end_time);
  }

  #[tokio::test]
  async fn test_concurrent_append_metric_points() {
    let num_threads = 20;
    let num_metric_points_per_thread = 5000;
    let segment = Arc::new(Segment::new());
    let start_time = Utc::now().timestamp_millis() as u64;
    let expected = Arc::new(RwLock::new(Vec::new()));

    let mut handles = Vec::new();
    for _ in 0..num_threads {
      let segment_arc = segment.clone();
      let expected_arc = expected.clone();
      let mut label_map = HashMap::new();
      label_map.insert("label1".to_owned(), "value1".to_owned());
      let handle = thread::spawn(move || {
        for _ in 0..num_metric_points_per_thread {
          let dp = MetricPoint::new(Utc::now().timestamp_millis() as u64, 1.0);
          segment_arc
            .append_metric_point("metric_name", &label_map, dp.get_time(), dp.get_value())
            .unwrap();
          expected_arc.write().push(dp);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    let end_time = Utc::now().timestamp_millis() as u64;

    assert!(segment.metadata.get_start_time() >= start_time);
    assert!(segment.metadata.get_end_time() <= end_time);

    let mut expected = (*expected.read()).clone();

    let mut labels = HashMap::new();
    labels.insert("label1".to_owned(), "value1".to_owned());
    let received = segment
      .search_metrics(
        &labels,
        &MetricsQueryCondition::Equals,
        start_time - 100,
        end_time + 100,
      )
      .await
      .expect("Error search metrics");

    expected.sort();
    assert_eq!(expected, received);
  }

  #[test]
  fn test_range_overlap() {
    let (start, end) = (1000, 2000);
    let segment = Segment::new();

    segment
      .append_log_message(start, &HashMap::new(), "message_1")
      .unwrap();
    segment
      .append_log_message(end, &HashMap::new(), "message_2")
      .unwrap();
    assert_eq!(segment.metadata.get_start_time(), start);
    assert_eq!(segment.metadata.get_end_time(), end);

    // The range is inclusive.
    assert!(segment.is_overlap(start, end));
    assert!(segment.is_overlap(start, start));
    assert!(segment.is_overlap(end, end));
    assert!(segment.is_overlap(0, start));
    assert!(segment.is_overlap(end, end + 1000));

    // Overlapping ranges.
    assert!(segment.is_overlap(start, end + 1000));
    assert!(segment.is_overlap((start + end) / 2, end + 1000));
    assert!(segment.is_overlap(start - 100, (start + end) / 2));
    assert!(segment.is_overlap(start - 100, end + 100));
    assert!(segment.is_overlap(start + 100, end - 100));

    // Non-overlapping ranges.
    assert!(!segment.is_overlap(start - 100, start - 1));
    assert!(!segment.is_overlap(end + 1, end + 100));
  }

  #[tokio::test]
  async fn test_duplicates() {
    let segment = Segment::new();

    segment
      .append_log_message(1000, &HashMap::new(), "hello world")
      .unwrap();
    segment
      .append_log_message(1001, &HashMap::new(), "some message")
      .unwrap();
    segment
      .append_log_message(1002, &HashMap::new(), "hello world hello world")
      .unwrap();

    // Test terms map.
    assert_eq!(segment.terms.len(), 4);
    assert!(segment.terms.contains_key("hello"));
    assert!(segment.terms.contains_key("world"));
    assert!(segment.terms.contains_key("some"));
    assert!(segment.terms.contains_key("message"));

    // Test search.
    let query_node_result = create_term_test_node("hello");

    if let Ok(query_node) = query_node_result {
      if let Err(err) = segment.search_logs(&query_node, 0, u64::MAX).await {
        error!("Error in search_logs: {:?}", err);
      } else {
        // Sort the expected results to match the sorted results from the function.
        let mut expected_results = vec!["hello world", "hello world hello world"];
        expected_results.sort();

        // Sort the actual results.
        let results = segment.search_logs(&query_node, 0, u64::MAX).await.unwrap();
        let mut actual_results: Vec<String> = results
          .get_messages()
          .iter()
          .map(|log| log.get_message().get_text().to_owned())
          .collect();
        actual_results.sort();

        // Test the sorted results.
        assert_eq!(actual_results, expected_results);
      }
    } else {
      error!("Error parsing the query for 'hello'.");
    }
  }
}

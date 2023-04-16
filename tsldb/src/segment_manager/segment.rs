use std::collections::HashMap;
use std::fs::create_dir;
use std::path::Path;
use std::vec::Vec;

use dashmap::DashMap;

use crate::log::log_message::LogMessage;
use crate::log::postings_list::PostingsList;
use crate::ts::data_point::DataPoint;
use crate::ts::time_series::TimeSeries;
use crate::utils::error::TsldbError;
use crate::utils::range::is_overlap;
use crate::utils::serialize;
use crate::utils::sync::thread;
use crate::utils::sync::Mutex;

use super::metadata::Metadata;

const METADATA_FILE_NAME: &str = "metadata.bin";
const TERMS_FILE_NAME: &str = "terms.bin";
const INVERTED_MAP_FILE_NAME: &str = "inverted_map.bin";
const FORWARD_MAP_FILE_NAME: &str = "forward_map.bin";
const LABELS_FILE_NAME: &str = "labels.bin";
const TIME_SERIES_FILE_NAME: &str = "time_series.bin";

/// A segment with inverted map (term-ids to log-message-ids) as well as forward map (log-message-ids to log messages).
#[derive(Debug)]
pub struct Segment {
  /// Metadata for this segment.
  metadata: Metadata,

  /// Terms present in this segment.
  /// Applicable only for log messages.
  terms: DashMap<String, u32>,

  /// Inverted map - term-id to a postings list.
  /// Applicable only for log messages.
  inverted_map: DashMap<u32, PostingsList>,

  /// Forward map - log_message-id to the corresponding log message.
  /// Applicable only for log messages.
  forward_map: DashMap<u32, LogMessage>,

  /// Labels present in this segment.
  /// Applicable only for time series.
  labels: DashMap<String, u32>,

  /// Time series map - label-id to corresponding time series.
  /// Applicable only for log messages.
  time_series: DashMap<u32, TimeSeries>,

  // Mutex for only one thread to commit this segment at a time.
  commit_lock: Mutex<thread::ThreadId>,
}

impl Segment {
  /// Create an empty segment.
  pub fn new() -> Self {
    Segment {
      metadata: Metadata::new(),
      terms: DashMap::new(),
      forward_map: DashMap::new(),
      inverted_map: DashMap::new(),
      labels: DashMap::new(),
      time_series: DashMap::new(),
      commit_lock: Mutex::new(thread::current().id()),
    }
  }

  #[allow(dead_code)]
  /// Get id of this segment.
  pub fn get_id(&self) -> &str {
    self.metadata.get_id()
  }

  /// Get log message count of this segment.
  pub fn get_log_message_count(&self) -> u32 {
    self.metadata.get_log_message_count()
  }

  #[allow(dead_code)]
  /// Get the number of terms in this segment.
  pub fn get_term_count(&self) -> u32 {
    self.metadata.get_term_count()
  }

  #[allow(dead_code)]
  /// Get the number of labels in this segment.
  pub fn get_label_count(&self) -> u32 {
    self.metadata.get_label_count()
  }

  /// Get the number of data points in this segment.
  pub fn get_data_point_count(&self) -> u32 {
    self.metadata.get_data_point_count()
  }

  #[allow(dead_code)]
  /// Get the earliest time in this segment.
  pub fn get_start_time(&self) -> u64 {
    self.metadata.get_start_time()
  }

  #[allow(dead_code)]
  /// Get the latest time in this segment.
  pub fn get_end_time(&self) -> u64 {
    self.metadata.get_end_time()
  }

  #[allow(dead_code)]
  /// Check if this segment is empty.
  pub fn is_empty(&self) -> bool {
    self.metadata.get_log_message_count() == 0
      && self.metadata.get_term_count() == 0
      && self.terms.is_empty()
      && self.forward_map.is_empty()
      && self.inverted_map.is_empty()
      && self.labels.is_empty()
      && self.time_series.is_empty()
  }

  /// Append a log message with timestamp to the segment (inverted as well as forward map).
  pub fn append_log_message(
    &self,
    time: u64,
    fields: &HashMap<String, String>,
    text: &str,
  ) -> Result<(), TsldbError> {
    let log_message = LogMessage::new_with_fields_and_text(time, fields, text);
    let terms = log_message.get_terms();

    let log_message_id = self.metadata.fetch_increment_log_message_count();
    // Update the forward map.
    self.forward_map.insert(log_message_id, log_message); // insert in forward map

    // Update the inverted map.
    for term in terms {
      // We actually mutate this variable in the match block below, so suppress the warning.
      #[allow(unused_mut)]
      let mut term_id: u32;

      // Need to lock the shard that contains the term, so that some other thread doesn't insert the same term.
      // Use the entry api - https://github.com/xacrimon/dashmap/issues/169#issuecomment-1009920032
      {
        let entry = self
          .terms
          .entry(term.to_owned())
          .or_insert(self.metadata.fetch_increment_term_count());
        term_id = *entry;
      }

      // Need to lock the shard that contains the term, so that some other thread doesn't insert the same term.
      // Use the entry api - https://github.com/xacrimon/dashmap/issues/169#issuecomment-1009920032
      {
        let entry = self
          .inverted_map
          .entry(term_id)
          .or_insert(PostingsList::new());
        let pl = &*entry;
        pl.append(log_message_id);
      }
    }

    self.update_start_end_time(time);
    Ok(())
  }

  /// Add a time-series data point with specified time and value.
  pub fn append_data_point(
    &self,
    metric_name: &str,
    name_value_labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) -> Result<(), TsldbError> {
    // Increment the number of data points appended so far.
    self.metadata.fetch_increment_data_point_count();

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
      // Use the entry api - https://github.com/xacrimon/dashmap/issues/169#issuecomment-1009920032
      {
        let entry = self
          .time_series
          .entry(label_id)
          .or_insert(TimeSeries::new());
        let ts = &*entry;
        ts.append(time, value);
      }
    } // end for label in my_labels

    self.update_start_end_time(time);
    Ok(())
  }

  /// Serialize the segment to the specified directory.
  pub fn commit(&self, dir: &str, sync_after_write: bool) {
    let mut lock = self.commit_lock.lock().unwrap();
    *lock = thread::current().id();

    let dir_path = Path::new(dir);

    if !dir_path.exists() {
      // Directory does not exist - create it.
      create_dir(dir_path).unwrap();
    }

    let metadata_path = dir_path.join(METADATA_FILE_NAME);
    let terms_path = dir_path.join(TERMS_FILE_NAME);
    let inverted_map_path = dir_path.join(INVERTED_MAP_FILE_NAME);
    let forward_map_path = dir_path.join(FORWARD_MAP_FILE_NAME);
    let labels_path = dir_path.join(LABELS_FILE_NAME);
    let time_series_path = dir_path.join(TIME_SERIES_FILE_NAME);

    serialize::write(
      &self.metadata,
      metadata_path.to_str().unwrap(),
      sync_after_write,
    );
    serialize::write(&self.terms, terms_path.to_str().unwrap(), sync_after_write);
    serialize::write(
      &self.inverted_map,
      inverted_map_path.to_str().unwrap(),
      sync_after_write,
    );
    serialize::write(
      &self.forward_map,
      forward_map_path.to_str().unwrap(),
      sync_after_write,
    );
    serialize::write(
      &self.labels,
      labels_path.to_str().unwrap(),
      sync_after_write,
    );
    serialize::write(
      &self.time_series,
      time_series_path.to_str().unwrap(),
      sync_after_write,
    );
  }

  /// Read the segment from the specified directory.
  pub fn refresh(dir: &str) -> Segment {
    let dir_path = Path::new(dir);
    let metadata_path = dir_path.join(METADATA_FILE_NAME);
    let terms_path = dir_path.join(TERMS_FILE_NAME);
    let inverted_map_path = dir_path.join(INVERTED_MAP_FILE_NAME);
    let forward_map_path = dir_path.join(FORWARD_MAP_FILE_NAME);
    let labels_path = dir_path.join(LABELS_FILE_NAME);
    let time_series_path = dir_path.join(TIME_SERIES_FILE_NAME);

    let metadata: Metadata = serialize::read(metadata_path.to_str().unwrap());
    let terms: DashMap<String, u32> = serialize::read(terms_path.to_str().unwrap());
    let inverted_map: DashMap<u32, PostingsList> =
      serialize::read(inverted_map_path.to_str().unwrap());
    let forward_map: DashMap<u32, LogMessage> = serialize::read(forward_map_path.to_str().unwrap());
    let labels: DashMap<String, u32> = serialize::read(labels_path.to_str().unwrap());
    let time_series: DashMap<u32, TimeSeries> = serialize::read(time_series_path.to_str().unwrap());
    let commit_lock = Mutex::new(thread::current().id());

    Segment {
      metadata,
      terms,
      inverted_map,
      forward_map,
      labels,
      time_series,
      commit_lock,
    }
  }

  /// Search the segment for the given query. If a query has multiple terms, it is by default taken as AND.
  /// Boolean queries are not yet supported.
  pub fn search(&self, query: &str, range_start_time: u64, range_end_time: u64) -> Vec<LogMessage> {
    // TODO: make the implementation below more performant by using the skip pointers (aka initial values)
    // and not decompressing every block in every postings list.
    let query_lowercase = query.to_lowercase();
    let terms = query_lowercase.split_whitespace();
    let mut postings_lists: Vec<Vec<u32>> = Vec::new();

    for term in terms {
      let result = self.terms.get(term);
      let term_id: u32 = match result {
        Some(result) => *result,
        None => {
          // Term not found, so return an empty vector as search result.
          return vec![];
        }
      };
      let postings_list = match self.inverted_map.get(&term_id) {
        Some(result) => result,
        None => {
          // Postings list not found. We are performing AND query, so return empty result.
          return vec![];
        }
      };
      let flattened_postings_list = postings_list.flatten().clone();
      postings_lists.push(flattened_postings_list);
    }

    if postings_lists.is_empty() {
      // No postings list
      return vec![];
    }

    let mut log_messages = Vec::new();

    let mut iter = postings_lists.iter();

    // Start with the first postings list as the initial result
    let mut log_message_ids = iter.next().unwrap().clone();

    // Fold over the remaining posting lists, updating the result
    log_message_ids = iter.fold(log_message_ids, |acc, list| {
      // Keep only the elements that appear in both lists
      acc.into_iter().filter(|&x| list.contains(&x)).collect()
    });

    for log_message_id in log_message_ids {
      let retval = self.forward_map.get(&log_message_id).unwrap();
      let log_message = retval.value();
      let time = log_message.get_time();
      if time >= range_start_time && time <= range_end_time {
        log_messages.push(LogMessage::new_with_fields_and_text(
          time,
          log_message.get_fields(),
          log_message.get_text(),
        ));
      }
    }

    log_messages
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

  // TODO: This api needs to be made richer (filter on multiple tags, metric name, prefix/regex, etc)
  /// Get the time series for the given label name/value, within the given (inclusive) time range.
  pub fn get_time_series(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<DataPoint> {
    let label = TimeSeries::get_label(label_name, label_value);
    let label_id = self.labels.get(&label);
    let retval = match label_id {
      Some(label_id) => {
        let ts = self.time_series.get(&label_id).unwrap();
        ts.get_time_series(range_start_time, range_end_time)
      }
      None => Vec::new(),
    };

    retval
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
}

impl Default for Segment {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, RwLock};

  use chrono::Utc;
  use tempdir::TempDir;

  use super::*;
  use crate::utils::sync::is_sync;
  use crate::utils::sync::thread;

  #[test]
  fn test_new_segment() {
    is_sync::<Segment>();

    let segment = Segment::new();
    assert!(segment.is_empty());
    assert!(segment.search("doesnotexist", 0, u64::MAX).is_empty());
  }

  #[test]
  fn test_default_segment() {
    let segment = Segment::default();
    assert!(segment.is_empty());
    assert!(segment.search("doesnotexist", 0, u64::MAX).is_empty());
  }

  #[test]
  fn test_basic_log_messages() {
    let segment = Segment::new();

    let start = Utc::now().timestamp_millis() as u64;
    let mut fields = HashMap::new();
    fields.insert("key1".to_owned(), "val1".to_owned());
    segment
      .append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        "this is my 1st log message",
      )
      .unwrap();
    segment
      .append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        "this is my 2nd log message",
      )
      .unwrap();
    segment
      .append_log_message(Utc::now().timestamp_millis() as u64, &fields, "blah")
      .unwrap();
    let end = Utc::now().timestamp_millis() as u64;

    // Test terms map.
    assert!(segment.terms.contains_key("blah"));
    assert!(segment.terms.contains_key("log"));
    assert!(segment.terms.contains_key("1st"));
    assert!(segment.terms.contains_key("key1:val1"));

    // Test search.
    let mut results = segment.search("this", 0, u64::MAX);
    assert!(results.len() == 2);

    assert!(
      results.get(0).unwrap().get_text() == "this is my 1st log message"
        || results.get(0).unwrap().get_text() == "this is my 2nd log message"
    );
    assert!(
      results.get(1).unwrap().get_text() == "this is my 1st log message"
        || results.get(1).unwrap().get_text() == "this is my 2nd log message"
    );

    results = segment.search("blah", start, end);
    assert!(results.len() == 1);
    assert_eq!(results.get(0).unwrap().get_text(), "blah");

    results = segment.search("key1:val1", start, end);
    assert!(results.len() == 1);
    assert_eq!(results.get(0).unwrap().get_text(), "blah");

    // Test search for a term that does not exist in the segment.
    results = segment.search("__doesnotexist__", start, end);
    assert!(results.is_empty());

    // Test multi-term queries, which are implicit AND.
    results = segment.search("blah message", start, end);
    assert!(results.is_empty());

    results = segment.search("log message", 0, u64::MAX);
    assert_eq!(results.len(), 2);

    results = segment.search("log message this", 0, u64::MAX);
    assert_eq!(results.len(), 2);

    results = segment.search("1st message", start, end);
    assert_eq!(results.len(), 1);

    results = segment.search("1st log message", start, end);
    assert_eq!(results.len(), 1);

    results = segment.search("blah key1:val1", start, end);
    assert_eq!(results.len(), 1);

    // Test with ranges that do not exist in the index.
    results = segment.search("log message", start - 1000, start - 100);
    assert_eq!(results.len(), 0);

    results = segment.search("log message", end + 100, end + 1000);
    assert_eq!(results.len(), 0);
  }

  #[test]
  fn test_commit_refresh() {
    let original_segment = Segment::new();
    let segment_dir = TempDir::new("segment_test").unwrap();
    let segment_dir_path = segment_dir.path().to_str().unwrap();

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
      .append_data_point(
        metric_name,
        &label_map,
        Utc::now().timestamp_millis() as u64,
        100.0,
      )
      .unwrap();

    // Commit so that the segment is serialized to disk, and refresh it from disk.
    original_segment.commit(segment_dir_path, false);

    let from_disk_segment: Segment = Segment::refresh(segment_dir_path);

    // Verify that both the segments are equal.
    assert_eq!(
      from_disk_segment.get_log_message_count(),
      original_segment.get_log_message_count()
    );
    assert_eq!(
      from_disk_segment.get_data_point_count(),
      original_segment.get_data_point_count()
    );

    // Test metadata.
    assert!(from_disk_segment.metadata.get_log_message_count() == 1);
    assert!(from_disk_segment.metadata.get_label_count() == 2);
    assert!(from_disk_segment.metadata.get_data_point_count() == 1);

    // 6 terms corresponding to each word in the sentence "this is my 1st log message"
    assert!(from_disk_segment.metadata.get_term_count() == 6);

    // Test terms map.
    assert!(from_disk_segment.terms.contains_key("1st"));

    // Test labels.
    let metric_name_key = TimeSeries::get_label_for_metric_name(metric_name);
    let other_label_key = TimeSeries::get_label(other_label_name, other_label_value);
    from_disk_segment.labels.contains_key(&metric_name_key);
    from_disk_segment.labels.contains_key(&other_label_key);

    // Test time series.
    assert_eq!(from_disk_segment.metadata.get_data_point_count(), 1);
    let result = from_disk_segment.labels.get(&metric_name_key).unwrap();
    let metric_name_id = result.value();
    let other_result = from_disk_segment.labels.get(&other_label_key).unwrap();
    let other_label_id = &other_result.value();
    let ts = from_disk_segment.time_series.get(metric_name_id).unwrap();
    let other_label_ts = from_disk_segment.time_series.get(other_label_id).unwrap();
    assert!(ts.eq(&other_label_ts));
    assert_eq!(ts.get_compressed_blocks().read().unwrap().len(), 0);
    assert_eq!(ts.get_initial_times().read().unwrap().len(), 1);
    assert_eq!(ts.get_last_block().read().unwrap().len(), 1);
    assert_eq!(
      ts.get_last_block()
        .read()
        .unwrap()
        .get_time_series_data_points()
        .read()
        .unwrap()
        .get(0)
        .unwrap()
        .get_value(),
      100.0
    );

    // Test search.
    let mut results = from_disk_segment.search("this", 0, u64::MAX);
    assert_eq!(results.len(), 1);
    assert_eq!(
      results.get(0).unwrap().get_text(),
      "this is my 1st log message"
    );

    results = from_disk_segment.search("blah", 0, u64::MAX);
    assert!(results.is_empty());

    // Test metadata for labels.
    assert!(from_disk_segment.metadata.get_label_count() == 2);
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

  #[test]
  fn test_one_data_point() {
    let segment = Segment::new();
    let time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    segment
      .append_data_point("metric_name_1", &label_map, time, 100.0)
      .unwrap();

    assert_eq!(segment.metadata.get_start_time(), time);
    assert_eq!(segment.metadata.get_end_time(), time);

    assert_eq!(
      segment
        .get_time_series("label_name_1", "label_value_1", time - 100, time + 100)
        .len(),
      1
    )
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

  #[test]
  fn test_concurrent_append_data_points() {
    let num_threads = 20;
    let num_data_points_per_thread = 5000;
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
        for _ in 0..num_data_points_per_thread {
          let dp = DataPoint::new(Utc::now().timestamp_millis() as u64, 1.0);
          segment_arc
            .append_data_point("metric_name", &label_map, dp.get_time(), dp.get_value())
            .unwrap();
          expected_arc.write().unwrap().push(dp);
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

    let mut expected = (*expected.read().unwrap()).clone();
    let received = segment.get_time_series("label1", "value1", start_time - 100, end_time + 100);

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
}

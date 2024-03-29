// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::collections::HashMap;
use std::vec::Vec;

use dashmap::DashMap;
use log::{debug, error};

use super::metadata::Metadata;
use super::search_logs::QueryLogMessage;
use super::wal::MetricWalEntry;
use crate::log::inverted_map::InvertedMap;
use crate::log::log_message::LogMessage;
use crate::log::postings_list::PostingsList;
use crate::metric::time_series::TimeSeries;
use crate::metric::time_series_map::TimeSeriesMap;
use crate::segment_manager::wal::LogWalEntry;
use crate::segment_manager::wal::WalEntry;
use crate::segment_manager::wal::WriteAheadLog;
use crate::storage_manager::storage::Storage;
use crate::utils::error::CoreDBError;
use crate::utils::error::QueryError;
use crate::utils::io::get_joined_path;
use crate::utils::range::is_overlap;
use crate::utils::sync::{Arc, Mutex, RwLock, TokioMutex};

const METADATA_FILE_NAME: &str = "metadata.bin";
const SEGMENT_FILE_NAME: &str = "segment.bin";

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
  commit_lock: TokioMutex<()>,

  // Disabled Trie Temporarily
  /// Trie data structure containing terms present in this segment.
  /// Primarily used for efficient prefix searches on log message terms.
  /// The Trie is protected by an Arc (atomic reference counting) and RwLock to ensure concurrent access and modification safety.
  // trie: Arc<RwLock<Trie>>,

  // Write ahead log.
  wal: Arc<Mutex<WriteAheadLog>>,
}

impl Segment {
  /// Create an empty segment.
  pub fn new(wal_file_path: &str) -> Self {
    let wal = WriteAheadLog::new(wal_file_path).unwrap();
    let wal = Arc::new(Mutex::new(wal));
    Segment {
      metadata: Metadata::new(),
      terms: DashMap::new(),
      forward_map: DashMap::new(),
      inverted_map: InvertedMap::new(),
      labels: DashMap::new(),
      time_series_map: TimeSeriesMap::new(),
      commit_lock: TokioMutex::new(()),
      // Disabled Trie Temporarily

      // trie: Arc::new(RwLock::new(Trie::new())),
      wal,
    }
  }

  /// Create a new segment from the given wal file.
  pub fn new_from_wal(wal_file_path: &str) -> Result<Self, CoreDBError> {
    // First create a new segment with /dev/null as the wal file path.
    let segment = Self::new("/dev/null");

    // Now open the wal file and replay all the entries.
    let wal = WriteAheadLog::new(wal_file_path)?;
    let entries = wal.read_all()?;

    // Reaplay each entry on the segment.
    for entry in entries {
      match entry {
        WalEntry::Log(log_entry) => {
          let result = segment.append_log_message_internal(
            log_entry.get_log_message_id(),
            log_entry.get_time(),
            log_entry.get_fields(),
            log_entry.get_text(),
          );

          // If there is an error, log it and continue.
          let _ = result.map_err(|e| debug!("Could not replay log entry: {}", e));
        }
        WalEntry::Metric(metric_wal_entry) => {
          let result = segment.append_metric_point_internal(
            metric_wal_entry.get_metric_name(),
            metric_wal_entry.get_name_value_labels(),
            metric_wal_entry.get_time(),
            metric_wal_entry.get_value(),
          );

          // If there is an error, log it and continue.
          let _ = result.map_err(|e| debug!("Could not replay metric entry: {}", e));
        }
      }
    }

    Ok(segment)
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

  /// Check if this segment is equal to another segment.
  /// This is an approximate check and only considers lengths of different fields.
  pub fn quick_equals(&self, other: &Segment) -> bool {
    self.metadata.get_log_message_count() == other.metadata.get_log_message_count()
      && self.metadata.get_term_count() == other.metadata.get_term_count()
      && self.terms.len() == other.terms.len()
      && self.forward_map.len() == other.forward_map.len()
      && self.inverted_map.len() == other.inverted_map.len()
      && self.labels.len() == other.labels.len()
      && self.time_series_map.len() == other.time_series_map.len()
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

  /// Append a log message to this segment, without writing WAL.
  /// This is typically called by Self::append_log_message() after writing WAL, or
  /// during recovery when we are building a segment and do not want to write WAL.
  fn append_log_message_internal(
    &self,
    log_message_id: u32,
    time: u64,
    fields: &HashMap<String, String>,
    text: &str,
  ) -> Result<u32, CoreDBError> {
    let log_message = LogMessage::new_with_fields_and_text(time, fields, text);
    let terms = log_message.get_terms();

    // Increment the number of log messages appended so far
    let _ = self.metadata.fetch_increment_log_message_count();

    // Disabled Trie Temporarily

    // let trie = self.trie.clone();

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

      // trie.write().insert(&term);
    });

    // Insert in the forward map.
    self.forward_map.insert(log_message_id, log_message);

    // Update the start and end time for this segment.
    self.update_start_end_time(time);

    debug!("Sending back doc Id {}", log_message_id);

    Ok(log_message_id)
  }

  /// Append a log message with timestamp to the segment (inverted as well as forward map).
  // Note that this function isn't async - this helps with testing and esuring correctness.
  pub fn append_log_message(
    &self,
    log_message_id: u32,
    time: u64,
    fields: &HashMap<String, String>,
    text: &str,
  ) -> Result<u32, CoreDBError> {
    debug!(
      "SEGMENT: Appending log message, time: {}, fields: {:?}, message: {}",
      time, fields, text
    );

    // Write to write-ahead-log.
    let wal_entry = WalEntry::Log(LogWalEntry::new(log_message_id, time, fields, text));
    {
      let wal_clone = self.wal.clone();
      let wal = &mut wal_clone.lock();
      wal.append(&wal_entry).unwrap();
    }

    self.append_log_message_internal(log_message_id, time, fields, text)
  }

  /// Append a metric point to this segment, without writing WAL.
  /// This is typically called by Self::append_metric_point() after writing WAL, or
  /// during recovery when we are building a segment and do not want to write WAL.
  fn append_metric_point_internal(
    &self,
    metric_name: &str,
    name_value_labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) -> Result<(), CoreDBError> {
    let mut my_labels = Vec::new();

    // Push the metric name label.
    // check if metric name is empty
    if !metric_name.is_empty() {
      my_labels.push(TimeSeries::get_label_for_metric_name(metric_name));
    }

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
          .entry(label)
          .or_insert_with(|| self.metadata.fetch_increment_label_count());
        label_id = *entry;
      }

      // Need to lock the shard that contains the label_id, so that some other thread doesn't insert the same label_id.
      // Add this in a separate block to minimize the locking time.
      {
        self.time_series_map.append(label_id, time, value)?;
      }

      // Increment the number of metric points appended so far.
      self.metadata.fetch_increment_metric_point_count();
    } // end for label in my_labels

    self.update_start_end_time(time);
    Ok(())
  }

  /// Append a metric point with specified time and value to the segment.
  // Note that this function isn't async - this helps with testing and esuring correctness.
  //
  // As an example, say empty segment, say s, is called as follows:
  // - s.append_metric_point(metric_name="http_get", name_value_labels={"status_code":200, "path":"/user", time="1", value="1")
  // - s.append_metric_point(metric_name="http_get", name_value_labels={"status_code":200, "path":"/user", time="2", value="2")
  // - s.append_metric_point(metric_name="http_get", name_value_labels={"status_code":500, "path":"/user", time="3", value="2")
  //
  // This will results into labels "__metric_name__~http_get", "status_code~200", "status_code~500", "path~/user". At the end of the
  // 3 calls, the label_count would be 4.
  //
  // The metric_point_count computes the number of metric points aggregated across all the labels. At the end of the 3 calls above,
  // the metric_point_count would be 9.
  pub fn append_metric_point(
    &self,
    metric_name: &str,
    name_value_labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) -> Result<(), CoreDBError> {
    // Write to write-ahead-log.
    let wal_entry = WalEntry::Metric(MetricWalEntry::new(
      metric_name,
      name_value_labels,
      time,
      value,
    ));
    {
      let wal_clone = self.wal.clone();
      let wal = &mut wal_clone.lock();
      wal.append(&wal_entry).unwrap();
    }

    self.append_metric_point_internal(metric_name, name_value_labels, time, value)
  }

  /// Get all terms with a certain prefix from the segment.
  pub fn get_terms_with_prefix(&self, prefix: &str, _case_insensitive: bool) -> Vec<String> {
    // Disabled Trie Temporarily

    // let trie = self.trie.read();
    // Use the Trie's method to collect terms with the given prefix
    // trie.get_terms_with_prefix(prefix, case_insensitive)
    vec![prefix.to_string()] // Just returning the prefix itself for illustration
  }

  pub async fn commit(&self, storage: &Storage, dir: &str) -> Result<(u64, u64), CoreDBError> {
    // Acquire a lock - so that only one thread can commit at a time.
    let _lock = self.commit_lock.lock().await;

    {
      // Flush write ahead log for this segment.
      let wal = self.wal.clone();
      let mut wal = wal.lock();
      wal.flush()?;
    }

    // Function to serialize a component to a given path.
    async fn serialize_component<T: serde::Serialize>(
      component: &T,
      dir: &str,
      file_path: &str,
      storage: &Storage,
    ) -> Result<(u64, u64), CoreDBError> {
      let path = get_joined_path(dir, file_path);
      let retval = storage.write(component, &path).await?;
      Ok(retval)
    }

    let combined = (
      &self.terms,
      &self.inverted_map,
      &self.forward_map,
      &self.labels,
      &self.time_series_map,
    );

    let (uncompressed_segment_size, compressed_segment_size) =
      serialize_component(&combined, dir, "segment.bin", storage).await?;

    // Update the metadata with segment size.
    let (uncompressed_metadata_size, compressed_metadata_size) = self.metadata.get_metadata_size();
    let uncompressed_segment_size = uncompressed_segment_size + uncompressed_metadata_size;
    let compressed_segment_size = compressed_segment_size + compressed_metadata_size;
    self
      .metadata
      .update_segment_size(uncompressed_segment_size, compressed_segment_size);

    // Seriazlize the metadata to disk.
    serialize_component(&self.metadata, dir, METADATA_FILE_NAME, storage).await?;

    debug!(
      "Serialized segment to {} bytes uncompressed, {} bytes compressed",
      uncompressed_segment_size, compressed_segment_size
    );

    Ok((uncompressed_segment_size, compressed_segment_size))
  }

  /// Read the segment from the specified directory.
  pub async fn refresh(storage: &Storage, dir: &str) -> Result<Segment, CoreDBError> {
    let metadata_path = get_joined_path(dir, METADATA_FILE_NAME);
    let segment_path = get_joined_path(dir, SEGMENT_FILE_NAME);

    let metadata: Metadata = storage.read(&metadata_path).await?;
    #[allow(clippy::type_complexity)]
    let (terms, inverted_map, forward_map, labels, time_series_map): (
      DashMap<String, u32>,
      InvertedMap,
      DashMap<u32, LogMessage>,
      DashMap<String, u32>,
      TimeSeriesMap,
    ) = storage.read(&segment_path).await?;
    let commit_lock = TokioMutex::new(());
    let wal = WriteAheadLog::new("/tmp/x").unwrap();
    let wal = Arc::new(Mutex::new(wal));

    // Disabled Trie Temporarily

    // Create a new thread-safe trie
    // let mut temp_trie = Trie::new();

    // // Insert terms into the trie
    // for term_entry in terms.iter() {
    //   let term = term_entry.key().clone();
    //   temp_trie.insert(&term);
    // }

    // let trie = Arc::new(RwLock::new(temp_trie));

    let segment = Segment {
      metadata,
      terms,
      inverted_map,
      forward_map,
      labels,
      time_series_map,
      commit_lock,
      // trie,
      wal,
    };

    Ok(segment)
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
      if time >= range_start_time
        && time <= range_end_time
        && !self.metadata.get_deleted_log_ids().contains(log_message_id)
      {
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
  // Note: Changing logic of this function may need corresponding change in
  // SegmentSummary::update_start_end_time.
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

  // Take two segments and merge them into one.
  pub fn merge(segment1: Segment, segment2: Segment) -> Result<Segment, CoreDBError> {
    // Remove this once WAL can be made skippable
    let temp_dir = tempfile::tempdir().map_err(|e| CoreDBError::IOError(e.to_string()))?;
    let path = temp_dir.path().join("merged_segment");

    // TODO: disable WAL while merging segments
    let mut merged_segment = Segment::new(
      path
        .to_str()
        .ok_or(CoreDBError::IOError("Invalid path".to_string()))?,
    );

    let append_log_message =
      |segment: &Segment, merged_segment: &mut Segment, log_id| -> Result<u32, CoreDBError> {
        if let Some(log_message) = segment.forward_map.get(&log_id) {
          let time = log_message.get_time();
          let fields = log_message.get_fields();
          let text = log_message.get_text();

          merged_segment
            .append_log_message(log_id, time, fields, text)
            .map_err(CoreDBError::from)
        } else {
          Err(CoreDBError::InvalidLogId(format!(
            "Log ID {} not found in segment",
            log_id
          )))
        }
      };

    let merge_logs_of_segment =
      |segment: &Segment, merged_segment: &mut Segment| -> Result<(), CoreDBError> {
        let log_ids: Vec<u32> = segment
          .forward_map
          .iter()
          .filter(|entry| !segment.metadata.get_deleted_log_ids().contains(entry.key()))
          .map(|entry| *entry.key())
          .collect();

        for log_id in log_ids {
          append_log_message(segment, merged_segment, log_id)?;
        }

        Ok(())
      };

    merge_logs_of_segment(&segment1, &mut merged_segment)?;
    merge_logs_of_segment(&segment2, &mut merged_segment)?;

    merged_segment
      .copy_time_series_from_segment(&segment1)
      .map_err(|e| {
        error!("Error copying time series from segment1: {:?}", e);
        e
      })?;
    merged_segment
      .copy_time_series_from_segment(&segment2)
      .map_err(|e| {
        error!("Error copying time series from segment2: {:?}", e);
        e
      })?;

    Ok(merged_segment)
  }

  fn copy_time_series_from_segment(&self, source: &Segment) -> Result<(), CoreDBError> {
    // Iterate over labels of the source
    for entry in source.get_labels().iter() {
      let label = entry.key();
      let label_id = entry.value();
      let mut metric_name = String::new();
      let mut name_value_labels = HashMap::new();
      // Check if the label is metric name label
      if TimeSeries::is_metric(label) {
        metric_name = TimeSeries::extract_metric_name(label);
      } else {
        let (name, value) = TimeSeries::extract_label(label);
        name_value_labels.insert(name, value);
      }

      let time_series = source
        .get_time_series_map()
        .get_time_series(*label_id)
        .unwrap();
      let metric_points = time_series.read().flatten();

      for metric_point in metric_points {
        self
          .append_metric_point(
            metric_name.as_str(),
            &name_value_labels,
            metric_point.get_time(),
            metric_point.get_value(),
          )
          .map_err(CoreDBError::from)?;
      }
    }
    Ok(())
  }
  /// Remove the write ahead log - typically called when after a segment is committed and will
  /// no longer be written to.
  pub fn remove_wal(&self) -> Result<(), CoreDBError> {
    let wal_clone = self.wal.clone();
    let wal = &mut wal_clone.lock();
    wal.remove()
  }

  /// Flush write ahead log to disk.
  pub fn flush_wal(&self) -> Result<(), CoreDBError> {
    let wal_clone = self.wal.clone();
    let wal = &mut wal_clone.lock();
    wal.flush()
  }

  #[cfg(test)]
  /// Create a new segment with a temporary WAL file.
  pub fn new_with_temp_wal() -> Self {
    let wal_file_path = format!("/tmp/{}.tmp", uuid::Uuid::new_v4());
    Self::new(&wal_file_path)
  }

  #[cfg(test)]
  pub fn get_wal_file_path(&self) -> String {
    let wal_clone = self.wal.clone();
    let wal_clone_lock = wal_clone.lock();
    wal_clone_lock.get_file_path()
  }

  pub fn mark_log_message_as_deleted(&self, log_message_ids: &[u32]) -> u32 {
    self.metadata.add_deleted_log_ids(log_message_ids.to_vec());
    log_message_ids.len() as u32
  }

  pub fn empty_deleted_log_ids(&self) {
    self.metadata.empty_deleted_log_ids();
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

    let segment = Segment::new_with_temp_wal();
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
  async fn test_new_segment_from_wal() {
    let segment = Segment::new_with_temp_wal();

    // Add a few log messages.
    for i in 0..10 {
      let time = Utc::now().timestamp_millis() as u64;
      let log_message = format!("some log message {}", i);
      segment
        .append_log_message(i, time, &HashMap::new(), &log_message)
        .unwrap();
    }

    // Add a few metric points.
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    for i in 0..100 {
      let time = Utc::now().timestamp_millis() as u64;
      segment
        .append_metric_point("metric_name_1", &label_map, time, i as f64)
        .unwrap();
    }

    // Flush the wal.
    segment.flush_wal().unwrap();

    // Create the segment with contents from the wal file.
    let wal_file_path = segment.get_wal_file_path();
    let segment_from_wal = Segment::new_from_wal(&wal_file_path).unwrap();

    // Check that both the segments are equal.
    assert!(segment.quick_equals(&segment_from_wal));
  }

  #[tokio::test]
  async fn test_default_segment() {
    let segment = Segment::new_with_temp_wal();
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
    let original_segment = Segment::new_with_temp_wal();
    let segment_dir = TempDir::new("segment_test").unwrap();
    let segment_dir_path = segment_dir.path().to_str().unwrap();
    let storage = Storage::new(&StorageType::Local)
      .await
      .expect("Could not create storage");

    original_segment
      .append_log_message(
        0,
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

    let from_disk_segment = Segment::refresh(&storage, segment_dir_path)
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
    let from_disk_segment_size = from_disk_segment.get_uncompressed_size();
    assert!(i64::abs((from_disk_segment_size - uncompressed_original_segment_size) as i64) < 32);

    // Test metadata.
    assert_eq!(from_disk_segment.metadata.get_log_message_count(), 1);
    assert_eq!(from_disk_segment.metadata.get_label_count(), 2);
    assert_eq!(from_disk_segment.metadata.get_metric_point_count(), 2);
    assert_eq!(from_disk_segment.metadata.get_term_count(), 6); // 6 terms in "this is my 1st log message"

    // Test terms map.
    assert!(from_disk_segment.terms.contains_key("1st"));

    // Test labels.
    let metric_name_key = TimeSeries::get_label_for_metric_name(metric_name);
    let other_label_key = TimeSeries::get_label(other_label_name, other_label_value);
    assert!(from_disk_segment.labels.contains_key(&metric_name_key));
    assert!(from_disk_segment.labels.contains_key(&other_label_key));

    // Test time series.
    assert_eq!(from_disk_segment.metadata.get_metric_point_count(), 2);
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

  #[tokio::test]
  async fn test_one_log_message() {
    let segment = Segment::new_with_temp_wal();
    let time = Utc::now().timestamp_millis() as u64;

    segment
      .append_log_message(0, time, &HashMap::new(), "some log message")
      .unwrap();

    assert_eq!(segment.metadata.get_start_time(), time);
    assert_eq!(segment.metadata.get_end_time(), time);
  }

  #[tokio::test]
  async fn test_one_metric_point() {
    let segment = Segment::new_with_temp_wal();
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

  #[tokio::test]
  async fn test_multiple_log_messages() {
    let num_messages = 1000;
    let segment = Segment::new_with_temp_wal();

    let start_time = Utc::now().timestamp_millis() as u64;
    for i in 0..num_messages {
      segment
        .append_log_message(
          i,
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
    let segment = Arc::new(Segment::new_with_temp_wal());
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

  // Disabled Trie test Temporarily

  /// Tests concurrent log message appending and trie verification.
  // #[tokio::test]
  // async fn test_concurrent_append_log_messages() {
  //   let num_threads = 20;
  //   let num_log_messages_per_thread = 500;

  //   // Shared segment and trie for concurrent testing.
  //   let segment = Arc::new(Segment::new_with_temp_wal());
  //   let trie = Arc::clone(&segment.trie);

  //   // Shared vector to collect expected words for trie verification.
  //   let expected_words = Arc::new(Mutex::new(Vec::new()));

  //   let mut handles = Vec::new();
  //   for thread_number in 0..num_threads {
  //     let segment_arc = Arc::clone(&segment);
  //     let expected_words_arc = Arc::clone(&expected_words);

  //     let handle = thread::spawn(move || {
  //       for i in 0..num_log_messages_per_thread {
  //         // Generate a unique identifier for the log message.
  //         let i_thread_number = format!("{}_{}", i, thread_number);

  //         // Create log text and fields with unique values.
  //         let log_text = format!("log message {} {}", i_thread_number, thread_number);
  //         let mut fields = HashMap::new();
  //         fields.insert("field12".to_owned(), format!("value1 {}", i_thread_number));
  //         fields.insert("field34".to_owned(), format!("value3 {}", thread_number));

  //         let time = Utc::now().timestamp_millis() as u64;
  //         let log_message: LogMessage =
  //           LogMessage::new_with_fields_and_text(time, &fields, &log_text);
  //         let terms = log_message.get_terms();
  //
  //          // Append the log message to the segment.
  //         segment_arc
  //           .append_log_message(i, time, &fields, &log_text)
  //           .unwrap();
  //
  //         // Collect terms for trie verification.
  //         let mut expected_words = expected_words_arc.lock().expect("Mutex lock failed");
  //         expected_words.extend(terms.iter().map(|word| word.to_owned()));
  //       }
  //     });

  //     handles.push(handle);
  //   }

  //   // Wait for all threads to complete.
  //   for handle in handles {
  //     handle.join().unwrap();
  //   }

  //   // Verify trie contains all expected words.
  //   let trie_read_lock = trie.read();
  //   let expected_words = expected_words.lock().expect("Mutex lock failed");
  //   for word in &*expected_words {
  //     assert!(
  //       trie_read_lock.contains(word),
  //       "Word not found in trie: {}",
  //       word
  //     );
  //   }
  // }

  #[tokio::test]
  async fn test_range_overlap() {
    let (start, end) = (1000, 2000);
    let segment = Segment::new_with_temp_wal();

    segment
      .append_log_message(0, start, &HashMap::new(), "message_1")
      .unwrap();
    segment
      .append_log_message(1, end, &HashMap::new(), "message_2")
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
    let segment = Segment::new_with_temp_wal();

    segment
      .append_log_message(0, 1000, &HashMap::new(), "hello world")
      .unwrap();
    segment
      .append_log_message(1, 1001, &HashMap::new(), "some message")
      .unwrap();
    segment
      .append_log_message(2, 1002, &HashMap::new(), "hello world hello world")
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

  /*
   s.append_metric_point(metric_name="http_get", name_value_labels={"status_code":200, "path":"/api/v1", time="1", value="1")
   s.append_metric_point(metric_name="http_get", name_value_labels={"status_code":200, "path":"/api/v1", time="2", value="2")
   s.append_metric_point(metric_name="http_get", name_value_labels={"status_code":500, "path":"/api/v1", time="3", value="2")
  */
  #[tokio::test]
  async fn test_label_metric_count_for_timeseries() {
    let segment = Segment::new_with_temp_wal();

    let mut label_map_1 = HashMap::new();
    label_map_1.insert("status_code".to_owned(), "200".to_owned());
    label_map_1.insert("path".to_owned(), "/api/v1".to_owned());

    segment
      .append_metric_point("http_get", &label_map_1, 1, 1.0)
      .unwrap();

    let mut label_map_2 = HashMap::new();
    label_map_2.insert("status_code".to_owned(), "200".to_owned());
    label_map_2.insert("path".to_owned(), "/api/v1".to_owned());
    segment
      .append_metric_point("http_get", &label_map_2, 2, 1.0)
      .unwrap();

    let mut label_map_3 = HashMap::new();
    label_map_3.insert("status_code".to_owned(), "500".to_owned());
    label_map_3.insert("path".to_owned(), "/api/v1".to_owned());
    segment
      .append_metric_point("http_get", &label_map_3, 3, 2.0)
      .unwrap();

    // Assert label count and metric point count.
    assert_eq!(segment.metadata.get_label_count(), 4);
    assert_eq!(segment.metadata.get_metric_point_count(), 9);
  }

  #[tokio::test]
  async fn test_copy_time_series_from_segment() {
    let segment1 = Segment::new_with_temp_wal();
    let segment2 = Segment::new_with_temp_wal();

    let time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label1".to_owned(), "value1".to_owned());
    segment1
      .append_metric_point("metric_name", &label_map, time, 100.0)
      .unwrap();

    // call copy_time_series_from_segment
    segment2.copy_time_series_from_segment(&segment1).unwrap();

    // check if the time series is copied
    let mut labels = HashMap::new();
    labels.insert("label1".to_owned(), "value1".to_owned());
    let results = segment2
      .search_metrics(
        &labels,
        &MetricsQueryCondition::Equals,
        time - 100,
        time + 100,
      )
      .await
      .unwrap();
    assert_eq!(results.len(), 1);

    // Check other metrics of segment2 related to time series
    // The count is 1 for metric points and 1 for labels
    // TODO: check with @vinaykakade if this is fine
    assert_eq!(segment2.metadata.get_metric_point_count(), 2);
    assert_eq!(segment2.metadata.get_label_count(), 2);
  }

  #[tokio::test]
  async fn test_merge_segments_basic() {
    let segment1 = Segment::new_with_temp_wal();
    let segment2 = Segment::new_with_temp_wal();

    // Insert logs and time series in both segment and merge them
    let time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label1".to_owned(), "value1".to_owned());
    segment1
      .append_metric_point("metric_name_1", &label_map, time, 100.0)
      .unwrap();
    segment2
      .append_metric_point("metric_name_2", &label_map, time, 100.0)
      .unwrap();

    // insert logs in both the segments
    segment1
      .append_log_message(0, time, &HashMap::new(), "log message 1")
      .unwrap();
    segment2
      .append_log_message(1, time, &HashMap::new(), "log message 2")
      .unwrap();

    let merged_segment = Segment::merge(segment1, segment2).unwrap();

    // Check if the logs are merged
    assert_eq!(merged_segment.get_log_message_count(), 2);

    // Check if the time series are merged
    let mut labels = HashMap::new();
    labels.insert("label1".to_owned(), "value1".to_owned());
    let results = merged_segment
      .search_metrics(
        &labels,
        &MetricsQueryCondition::Equals,
        time - 100,
        time + 100,
      )
      .await
      .unwrap();
    assert_eq!(results.len(), 2);

    //  Check metrics of merged segment
    // TODO: check with @vinaykakade if this is fine
    assert_eq!(merged_segment.metadata.get_metric_point_count(), 4);
  }

  // Similar to test_commit_refresh write exhaustive test for merge_segments
  #[tokio::test]
  async fn test_merge_segments() {
    let segment_1_dir = TempDir::new("segment_1_test").unwrap();
    let segment_2_dir = TempDir::new("segment_2_test").unwrap();
    let segment_1_dir_path = segment_1_dir.path().to_str().unwrap();
    let segment_2_dir_path = segment_2_dir.path().to_str().unwrap();
    let storage = Storage::new(&StorageType::Local)
      .await
      .expect("Could not create storage");

    let segment1 = Segment::new_with_temp_wal();
    let segment2 = Segment::new_with_temp_wal();
    let time = Utc::now().timestamp_millis() as u64;

    // Insert 10 unique logs in segment 1 and 2
    for i in 0..10 {
      segment1
        .append_log_message(
          i,
          time,
          &HashMap::new(),
          format!("log_message_1_{}", i).as_str(),
        )
        .unwrap();
      segment2
        .append_log_message(
          i + 10,
          time,
          &HashMap::new(),
          format!("log_message_2_{}", i).as_str(),
        )
        .unwrap();
    }

    let mut label_map_1 = HashMap::new();
    for i in 0..10 {
      label_map_1.insert(
        format!("label_1_{}", i).as_str().to_owned(),
        format!("value_1_{}", i).as_str().to_owned(),
      );
    }
    segment1
      .append_metric_point("metric_name_1", &label_map_1, time, 100.0)
      .unwrap();

    let mut label_map_2 = HashMap::new();
    for i in 0..10 {
      label_map_2.insert(
        format!("label_2_{}", i).as_str().to_owned(),
        format!("value_2_{}", i).as_str().to_owned(),
      );
    }
    segment2
      .append_metric_point("metric_name_2", &label_map_2, time, 100.0)
      .unwrap();

    segment1.commit(&storage, segment_1_dir_path).await.unwrap();
    segment2.commit(&storage, segment_2_dir_path).await.unwrap();

    // Check label cound and metric point count from both segments
    assert_eq!(segment1.metadata.get_label_count(), 11);
    assert_eq!(segment1.metadata.get_metric_point_count(), 11);
    assert_eq!(segment2.metadata.get_label_count(), 11);
    assert_eq!(segment2.metadata.get_metric_point_count(), 11);

    // Merge the segments
    let merged_segment = Segment::merge(segment1, segment2).unwrap();
    // Assert if the logs are merged
    assert_eq!(merged_segment.get_log_message_count(), 20);
    // Assert if metadata is merged correctly
    assert_eq!(merged_segment.metadata.get_log_message_count(), 20);
    assert_eq!(merged_segment.metadata.get_label_count(), 22);
    assert_eq!(merged_segment.metadata.get_metric_point_count(), 22);
    assert_eq!(merged_segment.metadata.get_term_count(), 20);

    // Iterate over merged segments forward map and check if log ids 1-20 are present
    let ids: Vec<u32> = (0..20).collect();
    let logs = merged_segment
      .get_log_messages_from_ids(&ids, 0, u64::MAX)
      .unwrap();
    assert_eq!(logs.len(), 20);
  }

  #[tokio::test]
  async fn test_search_metric_points_with_labels() {
    // Segment with 1000 metric points - each with the same label, different time.
    let segment1 = Segment::new_with_temp_wal();
    let mut label_map_1 = HashMap::new();
    label_map_1.insert("label_1".to_owned(), "value_1".to_owned());
    for _ in 0..1000 {
      let time = Utc::now().timestamp_millis() as u64;
      segment1
        .append_metric_point("metric_name_1", &label_map_1, time, 100.0)
        .unwrap();
    }
    let results = segment1
      .search_metrics(&label_map_1, &MetricsQueryCondition::Equals, 0, u64::MAX)
      .await
      .unwrap();
    assert_eq!(results.len(), 1000);

    // Segment with 1000 metric points - each with a different label, different time.
    let segment1 = Segment::new_with_temp_wal();
    for i in 0..1000 {
      let time = Utc::now().timestamp_millis() as u64;
      let mut label_map_1 = HashMap::new();
      label_map_1.insert(format!("label_1_{}", i), format!("value_1_{}", i));
      segment1
        .append_metric_point("metric_name_1", &label_map_1, time, 100.0)
        .unwrap();
    }

    for i in 0..1000 {
      let mut label_map_1 = HashMap::new();
      label_map_1.insert(format!("label_1_{}", i), format!("value_1_{}", i));

      let results = segment1
        .search_metrics(&label_map_1, &MetricsQueryCondition::Equals, 0, u64::MAX)
        .await
        .unwrap();
      assert_eq!(results.len(), 1);
    }

    // Segment with 1000 metric points - each with the same time, and the same label.
    let segment1 = Segment::new_with_temp_wal();
    let mut label_map_1 = HashMap::new();
    label_map_1.insert("label_1".to_owned(), "value_1".to_owned());
    let time = Utc::now().timestamp_millis() as u64;
    for _ in 0..1000 {
      segment1
        .append_metric_point("metric_name_1", &label_map_1, time, 100.0)
        .unwrap();
    }
    let results = segment1
      .search_metrics(&label_map_1, &MetricsQueryCondition::Equals, 0, u64::MAX)
      .await
      .unwrap();
    assert_eq!(results.len(), 1000);

    // Segment with 1000 metric points - each with a same time, but different label.
    let segment1 = Segment::new_with_temp_wal();
    let time = Utc::now().timestamp_millis() as u64;
    for i in 0..1000 {
      let mut label_map_1 = HashMap::new();
      label_map_1.insert(format!("label_1_{}", i), format!("value_1_{}", i));
      segment1
        .append_metric_point("metric_name_1", &label_map_1, time, 100.0)
        .unwrap();
    }

    for i in 0..1000 {
      let mut label_map_1 = HashMap::new();
      label_map_1.insert(format!("label_1_{}", i), format!("value_1_{}", i));

      let results = segment1
        .search_metrics(&label_map_1, &MetricsQueryCondition::Equals, 0, u64::MAX)
        .await
        .unwrap();
      assert_eq!(results.len(), 1);
    }
  }

  #[tokio::test]
  async fn test_duplicate_metric_points() {
    let segment = Segment::new_with_temp_wal();

    // Add 500 duplicate metric points with the same metric name, timestamp and value.
    // The 1st metric point has 1 label, the 2nd has 2 labels, etc.
    let time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    for i in 0..500 {
      label_map.insert(format!("label_{}", i), format!("value_{}", i));
      segment
        .append_metric_point("metric_name_1", &label_map, time, 100.0)
        .unwrap();
    }

    // Query the metric points and see that the results are as expected.
    let mut label_map = HashMap::new();
    for i in 0..500 {
      label_map.insert(format!("label_{}", i), format!("value_{}", i));
      let results = segment
        .search_metrics(&label_map, &MetricsQueryCondition::Equals, 0, u64::MAX)
        .await
        .unwrap();
      assert_eq!(results.len(), 500 - i);
    }
  }

  #[tokio::test]
  async fn test_mark_logs_as_deleted() {
    let segment = Segment::new_with_temp_wal();

    // Add 1000 log messages.
    let time = Utc::now().timestamp_millis() as u64;
    for i in 0..1000 {
      segment
        .append_log_message(
          i,
          time,
          &HashMap::new(),
          format!("log_message_{}", i).as_str(),
        )
        .unwrap();
    }

    // Create ids as [u32] from 1 to 500 to mark as deleted.
    let ids: Vec<u32> = (0..500).collect();
    segment.mark_log_message_as_deleted(&ids);

    let all_ids: Vec<u32> = (0..1000).collect();

    let log = segment
      .get_log_messages_from_ids(&all_ids, 0, u64::MAX)
      .unwrap();
    assert_eq!(log.len(), 500);
  }
}

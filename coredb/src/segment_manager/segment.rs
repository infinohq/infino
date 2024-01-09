use std::collections::HashMap;
use std::fs::create_dir;
use std::path::Path;
use std::vec::Vec;

use dashmap::DashMap;

use crate::log::log_message::LogMessage;
use crate::log::postings_block::PostingsBlock;
use crate::log::postings_block_compressed::PostingsBlockCompressed;
use crate::log::postings_list::PostingsList;
use crate::metric::metric_point::MetricPoint;
use crate::metric::time_series::TimeSeries;
use crate::utils::error::CoreDBError;
use crate::utils::range::is_overlap;
use crate::utils::serialize;
use crate::utils::sync::thread;
use crate::utils::sync::Mutex;
use crate::utils::tokenize::tokenize;

use super::metadata::Metadata;

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
  inverted_map: DashMap<u32, PostingsList>,

  /// Forward map - log_message-id to the corresponding log message.
  /// Applicable only for log messages.
  forward_map: DashMap<u32, LogMessage>,

  /// Labels present in this segment.
  /// Applicable only for time series.
  labels: DashMap<String, u32>,

  // Time series map - label-id to corresponding time series.
  // Applicable only for log messages.
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

  #[allow(dead_code)]
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

  #[allow(dead_code)]
  /// Get the number of metric points in this segment.
  pub fn get_metric_point_count(&self) -> u32 {
    self.metadata.get_metric_point_count()
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
  ) -> Result<(), CoreDBError> {
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
        let entry = self.inverted_map.entry(term_id).or_default();
        let pl = &*entry;
        pl.append(log_message_id);
      }
    }

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
      // Use the entry api - https://github.com/xacrimon/dashmap/issues/169#issuecomment-1009920032
      {
        let entry = self.time_series.entry(label_id).or_default();
        let ts = &*entry;
        ts.append(time, value);
      }
    } // end for label in my_labels

    self.update_start_end_time(time);
    Ok(())
  }

  /// Serialize the segment to the specified directory. Returns the size of the serialized segment.
  pub fn commit(&self, dir: &str, sync_after_write: bool) -> (u64, u64) {
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

    let (uncompressed_terms_size, compressed_terms_size) =
      serialize::write(&self.terms, terms_path.to_str().unwrap(), sync_after_write);
    log::debug!(
      "Serialized terms to {} bytes uncompressed, {} bytes compressed",
      uncompressed_terms_size,
      compressed_terms_size
    );

    let (uncompressed_inverted_map_size, compressed_inverted_map_size) = serialize::write(
      &self.inverted_map,
      inverted_map_path.to_str().unwrap(),
      sync_after_write,
    );
    log::debug!(
      "Serialized inverted map to {} bytes uncompressed, {} bytes compressed",
      uncompressed_inverted_map_size,
      compressed_inverted_map_size
    );

    let (uncompressed_forward_map_size, compressed_forward_map_size) = serialize::write(
      &self.forward_map,
      forward_map_path.to_str().unwrap(),
      sync_after_write,
    );
    log::debug!(
      "Serialized forward map to {} bytes uncompressed, {} bytes compressed",
      uncompressed_forward_map_size,
      compressed_forward_map_size
    );

    let (uncompressed_labels_size, compressed_labels_size) = serialize::write(
      &self.labels,
      labels_path.to_str().unwrap(),
      sync_after_write,
    );
    log::debug!(
      "Serialized labels to {} bytes uncompressed, {} bytes compressed",
      uncompressed_labels_size,
      compressed_labels_size
    );

    let (uncompressed_time_series_size, compressed_time_series_size) = serialize::write(
      &self.time_series,
      time_series_path.to_str().unwrap(),
      sync_after_write,
    );
    log::debug!(
      "Serialized time series to {} bytes uncompressed, {} bytes compressed",
      uncompressed_time_series_size,
      compressed_time_series_size
    );

    let (uncompressed_metadata_size, compressed_metadata_size) = self.metadata.get_metadata_size();

    let uncompressed_segment_size = uncompressed_metadata_size
      + uncompressed_terms_size
      + uncompressed_inverted_map_size
      + uncompressed_forward_map_size
      + uncompressed_labels_size
      + uncompressed_time_series_size;
    let compressed_segment_size = compressed_metadata_size
      + compressed_terms_size
      + compressed_inverted_map_size
      + compressed_forward_map_size
      + compressed_labels_size
      + compressed_time_series_size;

    self
      .metadata
      .update_segment_size(uncompressed_segment_size, compressed_segment_size);

    // Write the metadata at the end - so that its segment size is updated
    serialize::write(
      &self.metadata,
      metadata_path.to_str().unwrap(),
      sync_after_write,
    );

    log::debug!(
      "Serialized segment to {} bytes uncompressed, {} bytes compressed",
      uncompressed_segment_size,
      compressed_segment_size
    );

    (uncompressed_segment_size, compressed_segment_size)
  }

  /// Read the segment from the specified directory.
  pub fn refresh(dir: &str) -> (Segment, u64) {
    let dir_path = Path::new(dir);
    let metadata_path = dir_path.join(METADATA_FILE_NAME);
    let terms_path = dir_path.join(TERMS_FILE_NAME);
    let inverted_map_path = dir_path.join(INVERTED_MAP_FILE_NAME);
    let forward_map_path = dir_path.join(FORWARD_MAP_FILE_NAME);
    let labels_path = dir_path.join(LABELS_FILE_NAME);
    let time_series_path = dir_path.join(TIME_SERIES_FILE_NAME);

    let (metadata, metadata_size): (Metadata, _) = serialize::read(metadata_path.to_str().unwrap());
    let (terms, terms_size): (DashMap<String, u32>, _) =
      serialize::read(terms_path.to_str().unwrap());
    let (inverted_map, inverted_map_size): (DashMap<u32, PostingsList>, _) =
      serialize::read(inverted_map_path.to_str().unwrap());
    let (forward_map, forward_map_size): (DashMap<u32, LogMessage>, _) =
      serialize::read(forward_map_path.to_str().unwrap());
    let (labels, labels_size): (DashMap<String, u32>, _) =
      serialize::read(labels_path.to_str().unwrap());
    let (time_series, time_series_size): (DashMap<u32, TimeSeries>, _) =
      serialize::read(time_series_path.to_str().unwrap());
    let commit_lock = Mutex::new(thread::current().id());

    let total_size = metadata_size
      + terms_size
      + inverted_map_size
      + forward_map_size
      + labels_size
      + time_series_size;

    let segment = Segment {
      metadata,
      terms,
      inverted_map,
      forward_map,
      labels,
      time_series,
      commit_lock,
    };

    (segment, total_size)
  }

  // Get the posting lists belonging to a set of matching terms in the query
  fn get_postings_lists(
    &self,
    terms: &[String],
  ) -> (
    Vec<Vec<PostingsBlockCompressed>>,
    Vec<PostingsBlock>,
    Vec<Vec<u32>>,
    usize,
  ) {
    // initial_values_list will contain list of initial_values corresponding to every posting_list
    let mut initial_values_list: Vec<Vec<u32>> = Vec::new();

    // postings list will contain list of PostingBlocksCompressed
    let mut postings_lists: Vec<Vec<PostingsBlockCompressed>> = Vec::new();
    let mut last_block_list: Vec<PostingsBlock> = Vec::new();
    let mut shortest_list_index = 0;
    let mut shortest_list_len = usize::MAX;

    println!("We are looking for {:?}", terms);

    for (index, term) in terms.into_iter().enumerate() {
      let result = self.terms.get(term);
      let term_id: u32 = match result {
        Some(result) => *result,
        None => {
          // Term not found.
          return (Vec::new(), Vec::new(), Vec::new(), 0);
        }
      };
      let postings_list = match self.inverted_map.get(&term_id) {
        Some(result) => result,
        None => {
          // Postings list not found.
          return (Vec::new(), Vec::new(), Vec::new(), 0);
        }
      };
      let inital_values = postings_list.get_initial_values().read().unwrap().clone();
      initial_values_list.push(inital_values);

      // Extract list of compressed posting blocks from a posting list.
      // posting_block_compressed is Vec<PostingsBlockCompressed> which is extracted by
      // cloning get_postings_list_compressed from postings_lists
      let mut postings_block_compressed_vec: Vec<PostingsBlockCompressed> = Vec::new();
      for posting_block in postings_list
        .get_postings_list_compressed()
        .read()
        .unwrap()
        .iter()
      {
        postings_block_compressed_vec.push(posting_block.clone());
      }

      // Extract last posting block from posting list
      last_block_list.push(
        postings_list
          .get_last_postings_block()
          .read()
          .unwrap()
          .clone(),
      );

      if postings_block_compressed_vec.len() < shortest_list_len {
        shortest_list_len = postings_block_compressed_vec.len();
        shortest_list_index = index;
      }

      postings_lists.push(postings_block_compressed_vec);
    }

    (
      postings_lists,
      last_block_list,
      initial_values_list,
      shortest_list_index,
    )
  }

  // Get the matching doc IDs corresponding to a set of posting lists
  fn get_matching_doc_ids(
    &self,
    postings_lists: &[Vec<PostingsBlockCompressed>],
    last_block_list: &[PostingsBlock],
    initial_values_list: &Vec<Vec<u32>>,
    shortest_list_index: usize,
    accumulator: &mut Vec<u32>,
  ) {
    // Create accumulator from shortest posting list from postings_lists. Which is flatten the shortest
    // Vec<PostingsBlockCompressed> to Vec<u32> postings_lists.first() will give Vec<PostingsBlockCompressed>
    // which needs to be iterated on to get Vec<u32>
    let first_posting_blocks = &postings_lists[shortest_list_index];
    for posting_block in first_posting_blocks {
      let posting_block = PostingsBlock::try_from(posting_block).unwrap();
      accumulator.append(&mut posting_block.get_log_message_ids().read().unwrap().clone());
    }

    accumulator.append(
      &mut last_block_list[shortest_list_index]
        .get_log_message_ids()
        .read()
        .unwrap()
        .clone(),
    );

    // If postings_list is empty, then accumulator should be loaded from last_block_list
    if accumulator.is_empty() {
      // No postings list
      return;
    }

    for i in 0..initial_values_list.len() {
      // Skip shortest posting list as it is already used to create accumulator
      if i == shortest_list_index {
        continue;
      }
      let posting_list = &postings_lists[i];
      let initial_values = &initial_values_list[i];

      let mut temp_result_set = Vec::new();
      let mut acc_index = 0;
      let mut posting_index = 0;
      let mut initial_index = 0;

      while acc_index < accumulator.len() && initial_index < initial_values.len() {
        // If current accumulator element < initial_value element it means that
        // accumulator value is smaller than what current posting_block will have
        // so increment accumulator till this condition fails
        while acc_index < accumulator.len()
          && accumulator[acc_index] < initial_values[initial_index]
        {
          acc_index += 1;
        }

        if acc_index < accumulator.len() && accumulator[acc_index] > initial_values[initial_index] {
          // If current accumulator element is in between current initial_value and next initial_value
          // then check the existing posting block for matches with accumlator
          // OR if it's the last accumulator is greater than last initial value, then check the last posting block
          if (initial_index + 1 < initial_values.len()
            && accumulator[acc_index] < initial_values[initial_index + 1])
            || (initial_index == initial_values.len() - 1)
          {
            let mut _posting_block = Vec::new();
            // posting_index == posting_list.len() means that we are at last_block
            if posting_index < posting_list.len() {
              _posting_block = PostingsBlock::try_from(&posting_list[posting_index])
                .unwrap()
                .get_log_message_ids()
                .read()
                .unwrap()
                .clone();
            } else {
              // posting block is last block
              _posting_block = last_block_list[i]
                .get_log_message_ids()
                .read()
                .unwrap()
                .clone();
            }
            // start from 1st element of posting_block as 0th element of posting_block is already checked as it was part of intial_values
            let mut posting_block_index = 1;
            while acc_index < accumulator.len() && posting_block_index < _posting_block.len() {
              match accumulator[acc_index].cmp(&_posting_block[posting_block_index]) {
                std::cmp::Ordering::Equal => {
                  temp_result_set.push(accumulator[acc_index]);
                  acc_index += 1;
                  posting_block_index += 1;
                }
                std::cmp::Ordering::Greater => {
                  posting_block_index += 1;
                }
                std::cmp::Ordering::Less => {
                  acc_index += 1;
                }
              }

              // Try to see if we can skip remaining elements of the postings block
              if initial_index + 1 < initial_values.len()
                && acc_index < accumulator.len()
                && accumulator[acc_index] >= initial_values[initial_index + 1]
              {
                break;
              }
            }
          } else {
            // go to next posting_block and correspodning initial_value
            // done at end of the outer while loop
          }
        }

        // If current accumulator and initial value are same, then add it to temporary accumulator
        // and check remaining elements of the postings block
        if acc_index < accumulator.len()
          && initial_index < initial_values.len()
          && accumulator[acc_index] == initial_values[initial_index]
        {
          temp_result_set.push(accumulator[acc_index]);
          acc_index += 1;

          let mut _posting_block = Vec::new();
          // posting_index == posting_list.len() means that we are at last_block
          if posting_index < posting_list.len() {
            _posting_block = PostingsBlock::try_from(&posting_list[posting_index])
              .unwrap()
              .get_log_message_ids()
              .read()
              .unwrap()
              .clone();
          } else {
            // posting block is last block
            _posting_block = last_block_list[i]
              .get_log_message_ids()
              .read()
              .unwrap()
              .clone();
          }

          // Check the remaining elements of posting block
          let mut posting_block_index = 1;
          while acc_index < accumulator.len() && posting_block_index < _posting_block.len() {
            match accumulator[acc_index].cmp(&_posting_block[posting_block_index]) {
              std::cmp::Ordering::Equal => {
                temp_result_set.push(accumulator[acc_index]);
                acc_index += 1;
                posting_block_index += 1;
              }
              std::cmp::Ordering::Greater => {
                posting_block_index += 1;
              }
              std::cmp::Ordering::Less => {
                acc_index += 1;
              }
            }

            // Try to see if we can skip remaining elements of posting_block
            if initial_index + 1 < initial_values.len()
              && acc_index < accumulator.len()
              && accumulator[acc_index] >= initial_values[initial_index + 1]
            {
              break;
            }
          }
        }

        initial_index += 1;
        posting_index += 1;
      }

      *accumulator = temp_result_set;
    }
  }

  /// Search the segment for the given query. If a query has multiple terms, it is by
  /// default taken as AND. Boolean queries are not yet supported.
  pub fn search_logs(
    &self,
    query: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<LogMessage> {
    let query_lowercase = query.to_lowercase();
    let terms = tokenize(&query_lowercase);
    let mut results_accumulator = Vec::new();

    // Get postings lists for the query terms
    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
      self.get_postings_lists(&terms);

    // No postings lists were found so let's return empty handed
    if postings_lists.is_empty() {
      return vec![];
    }

    // Now get the matching document IDs from the postings lists
    self.get_matching_doc_ids(
      &postings_lists,
      &last_block_list,
      &initial_values_list,
      shortest_list_index,
      &mut results_accumulator,
    );

    // Main logic end

    // The above intersection may leave around duplicates - see https://github.com/infinohq/infino/issues/58
    // Remove the consecutive duplicates
    results_accumulator.dedup();

    let mut log_messages =
      self.get_log_messages_from_ids(&results_accumulator, range_start_time, range_end_time);
    log_messages.sort();
    log_messages
  }

  /// Return the log messages within the given time range corresponding to the given log message ids.
  fn get_log_messages_from_ids(
    &self,
    log_message_ids: &Vec<u32>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<LogMessage> {
    let mut log_messages = Vec::new();
    for log_message_id in log_message_ids {
      let retval = self.forward_map.get(log_message_id).unwrap();
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
  pub fn search_metrics(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<MetricPoint> {
    let label = TimeSeries::get_label(label_name, label_value);
    let label_id = self.labels.get(&label);
    let retval = match label_id {
      Some(label_id) => {
        let ts = self.time_series.get(&label_id).unwrap();
        ts.get_metrics(range_start_time, range_end_time)
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
  use crate::utils::tokenize::FIELD_DELIMITER;

  #[test]
  fn test_new_segment() {
    is_sync::<Segment>();

    let segment = Segment::new();
    assert!(segment.is_empty());
    assert!(segment.search_logs("doesnotexist", 0, u64::MAX).is_empty());
  }

  #[test]
  fn test_default_segment() {
    let segment = Segment::default();
    assert!(segment.is_empty());
    assert!(segment.search_logs("doesnotexist", 0, u64::MAX).is_empty());
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
    assert!(segment
      .terms
      .contains_key(&format!("key1{}val1", FIELD_DELIMITER)));

    // Test search.
    let mut results = segment.search_logs("this", 0, u64::MAX);
    assert!(results.len() == 2);

    assert!(
      results.get(0).unwrap().get_text() == "this is my 1st log message"
        || results.get(0).unwrap().get_text() == "this is my 2nd log message"
    );
    assert!(
      results.get(1).unwrap().get_text() == "this is my 1st log message"
        || results.get(1).unwrap().get_text() == "this is my 2nd log message"
    );

    results = segment.search_logs("blah", start, end);
    assert!(results.len() == 1);
    assert_eq!(results.get(0).unwrap().get_text(), "blah");

    results = segment.search_logs(&format!("key1{}val1", FIELD_DELIMITER), start, end);
    assert!(results.len() == 1);
    assert_eq!(results.get(0).unwrap().get_text(), "blah");

    // Test search for a term that does not exist in the segment.
    results = segment.search_logs("__doesnotexist__", start, end);
    assert!(results.is_empty());

    // Test multi-term queries, which are implicit AND.
    results = segment.search_logs("blah message", start, end);
    assert!(results.is_empty());

    results = segment.search_logs("log message", 0, u64::MAX);
    assert_eq!(results.len(), 2);

    results = segment.search_logs("log message this", 0, u64::MAX);
    assert_eq!(results.len(), 2);

    results = segment.search_logs("1st message", start, end);
    assert_eq!(results.len(), 1);

    results = segment.search_logs("1st log message", start, end);
    assert_eq!(results.len(), 1);

    results = segment.search_logs(&format!("blah key1{}val1", FIELD_DELIMITER), start, end);
    assert_eq!(results.len(), 1);

    // Test with ranges that do not exist in the index.
    results = segment.search_logs("log message", start - 1000, start - 100);
    assert_eq!(results.len(), 0);

    results = segment.search_logs("log message", end + 100, end + 1000);
    assert_eq!(results.len(), 0);
  }

  #[test]
  fn test_basic_log_messages_with_colon() {
    let segment = Segment::new();

    segment
      .append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        "test: this is my 1st log message",
      )
      .unwrap();

    // Test terms map.
    assert!(segment.terms.contains_key("log"));
    assert!(segment.terms.contains_key("1st"));
    assert!(segment.terms.contains_key("test"));

    // Test search.
    let results = segment.search_logs("test:", 0, u64::MAX);
    assert!(results.len() == 1);
    assert_eq!(
      results.get(0).unwrap().get_text(),
      "test: this is my 1st log message"
    );
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
      .append_metric_point(
        metric_name,
        &label_map,
        Utc::now().timestamp_millis() as u64,
        100.0,
      )
      .unwrap();

    // Commit so that the segment is serialized to disk, and refresh it from disk.
    let (uncompressed_original_segment_size, compressed_original_segment_size) =
      original_segment.commit(segment_dir_path, false);
    assert!(uncompressed_original_segment_size > 0);
    assert!(compressed_original_segment_size > 0);

    let (from_disk_segment, from_disk_segment_size) = Segment::refresh(segment_dir_path);

    // Verify that both the segments are equal.
    assert_eq!(
      from_disk_segment.get_log_message_count(),
      original_segment.get_log_message_count()
    );
    assert_eq!(
      from_disk_segment.get_metric_point_count(),
      original_segment.get_metric_point_count()
    );

    // Verify that the segment size of the index read from disk is almost same as the uncompressed original segment size.
    // Note that they may not be exactly the same, but may have a difference of a few bytes. This is because the
    // size is stored in metadata file, which itself is written to disk. So, the original segment size may not reflect
    // the exact number of bytes required to store the latest size of the segment.
    assert!(i64::abs((from_disk_segment_size - uncompressed_original_segment_size) as i64) < 32);

    // Test metadata.
    assert!(from_disk_segment.metadata.get_log_message_count() == 1);
    assert!(from_disk_segment.metadata.get_label_count() == 2);
    assert!(from_disk_segment.metadata.get_metric_point_count() == 1);

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
    assert_eq!(from_disk_segment.metadata.get_metric_point_count(), 1);
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
        .get_metrics_metric_points()
        .read()
        .unwrap()
        .get(0)
        .unwrap()
        .get_value(),
      100.0
    );

    // Test search.
    let mut results = from_disk_segment.search_logs("this", 0, u64::MAX);
    assert_eq!(results.len(), 1);
    assert_eq!(
      results.get(0).unwrap().get_text(),
      "this is my 1st log message"
    );

    results = from_disk_segment.search_logs("blah", 0, u64::MAX);
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
  fn test_one_metric_point() {
    let segment = Segment::new();
    let time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    segment
      .append_metric_point("metric_name_1", &label_map, time, 100.0)
      .unwrap();

    assert_eq!(segment.metadata.get_start_time(), time);
    assert_eq!(segment.metadata.get_end_time(), time);

    assert_eq!(
      segment
        .search_metrics("label_name_1", "label_value_1", time - 100, time + 100)
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
  fn test_concurrent_append_metric_points() {
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
    let received = segment.search_metrics("label1", "value1", start_time - 100, end_time + 100);

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

  #[test]
  fn test_duplicates() {
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
    let results = segment.search_logs("hello", 0, u64::MAX);
    assert_eq!(results.len(), 2);
    assert_eq!(
      results.get(0).unwrap().get_text(),
      "hello world hello world"
    );
    assert_eq!(results.get(1).unwrap().get_text(), "hello world");
  }
}

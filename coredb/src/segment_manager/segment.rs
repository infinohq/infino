use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::create_dir;
use std::path::Path;
use std::vec::Vec;

use log::debug;

use dashmap::DashMap;

use serde::{Deserialize, Serialize};

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

// Ast used for deconstructing queries for searches
pub enum AstNode {
  Match(String),
  Must(Box<AstNode>, Box<AstNode>),
  Should(Box<AstNode>, Box<AstNode>),
  MustNot(Box<AstNode>),
  None,
}

// Query condition to evaluate during Ast traversal
#[derive(Serialize, Deserialize)]
pub enum Condition {
  Term(TermQuery),
  Bool(BoolQuery),
}

#[derive(Serialize, Deserialize)]
pub struct TermQuery {
  term: Option<String>,
  field: Option<String>,
  boost: Option<f32>,
  fuzziness: Option<String>,
}

// A Boolean Query definition
#[derive(Serialize, Deserialize)]
pub struct BoolQuery {
  must: Option<Vec<Condition>>,
  must_not: Option<Vec<Condition>>,
  should: Option<Vec<Condition>>,
  filter: Option<Vec<Condition>>,
}

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
    debug!(
      "Serialized terms to {} bytes uncompressed, {} bytes compressed",
      uncompressed_terms_size, compressed_terms_size
    );

    let (uncompressed_inverted_map_size, compressed_inverted_map_size) = serialize::write(
      &self.inverted_map,
      inverted_map_path.to_str().unwrap(),
      sync_after_write,
    );
    debug!(
      "Serialized inverted map to {} bytes uncompressed, {} bytes compressed",
      uncompressed_inverted_map_size, compressed_inverted_map_size
    );

    let (uncompressed_forward_map_size, compressed_forward_map_size) = serialize::write(
      &self.forward_map,
      forward_map_path.to_str().unwrap(),
      sync_after_write,
    );
    debug!(
      "Serialized forward map to {} bytes uncompressed, {} bytes compressed",
      uncompressed_forward_map_size, compressed_forward_map_size
    );

    let (uncompressed_labels_size, compressed_labels_size) = serialize::write(
      &self.labels,
      labels_path.to_str().unwrap(),
      sync_after_write,
    );
    debug!(
      "Serialized labels to {} bytes uncompressed, {} bytes compressed",
      uncompressed_labels_size, compressed_labels_size
    );

    let (uncompressed_time_series_size, compressed_time_series_size) = serialize::write(
      &self.time_series,
      time_series_path.to_str().unwrap(),
      sync_after_write,
    );
    debug!(
      "Serialized time series to {} bytes uncompressed, {} bytes compressed",
      uncompressed_time_series_size, compressed_time_series_size
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

    debug!(
      "Serialized segment to {} bytes uncompressed, {} bytes compressed",
      uncompressed_segment_size, compressed_segment_size
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
  #[allow(clippy::type_complexity)]
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

    for (index, term) in terms.iter().enumerate() {
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
      debug!("Posting list is empty. Loading accumulator from last_block_list.");
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

  /// Search the segment for the given query.
  pub fn search_logs(
    &self,
    ast: &AstNode,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<LogMessage> {
    // Get the set of matching doc IDs for the query
    let results_accumulator = self.traverse_ast(ast);

    // Convert the result set into a vector
    let mut matching_document_ids: Vec<u32> = results_accumulator.iter().cloned().collect();

    // Remove any duplicates
    matching_document_ids.dedup();

    // Get the log messages, sort, and return with the query results
    let mut log_messages =
      self.get_log_messages_from_ids(&matching_document_ids, range_start_time, range_end_time);
    log_messages.sort();
    log_messages
  }

  // Helper function to combine two nodes using a specific AST combiner
  fn combine_two_nodes<F>(
    node1: Option<AstNode>,
    node2: Option<AstNode>,
    combiner: F,
  ) -> Option<AstNode>
  where
    F: FnOnce(Box<AstNode>, Box<AstNode>) -> AstNode,
  {
    match (node1, node2) {
      (Some(n1), Some(n2)) => Some(combiner(Box::new(n1), Box::new(n2))),
      (Some(n), None) | (None, Some(n)) => Some(n),
      (None, None) => None,
    }
  }

  // Helper function to combine AST nodes
  fn combine_ast_nodes(
    must_nodes: Option<Vec<AstNode>>,
    must_not_nodes: Option<Vec<AstNode>>,
    should_nodes: Option<Vec<AstNode>>,
  ) -> Option<AstNode> {
    let must_combined = must_nodes
      .unwrap_or_default()
      .into_iter()
      .reduce(|a, b| AstNode::Must(Box::new(a), Box::new(b)));
    let must_not_combined = must_not_nodes
      .unwrap_or_default()
      .into_iter()
      .reduce(|a, b| AstNode::MustNot(Box::new(AstNode::Must(Box::new(a), Box::new(b)))));
    let should_combined = should_nodes
      .unwrap_or_default()
      .into_iter()
      .reduce(|a, b| AstNode::Should(Box::new(a), Box::new(b)));

    let combined_must_must_not =
      Segment::combine_two_nodes(must_combined, must_not_combined, AstNode::Must);
    Segment::combine_two_nodes(combined_must_must_not, should_combined, AstNode::Should)
  }

  // Helper function to evaluate query conditions
  fn process_conditions(conditions: &Option<Vec<Condition>>) -> Vec<AstNode> {
    conditions
      .as_ref()
      .unwrap_or(&Vec::new())
      .iter()
      .flat_map(Segment::traverse_condition)
      .collect()
  }

  // Convert a JSON query to an AST
  pub fn query_to_ast(query: &BoolQuery) -> AstNode {
    let must_nodes = Some(Segment::process_conditions(&query.must));
    let must_not_nodes = Some(Segment::process_conditions(&query.must_not));
    let should_nodes = Some(Segment::process_conditions(&query.should));

    Segment::combine_ast_nodes(must_nodes, must_not_nodes, should_nodes).unwrap()
  }

  // Traverse the boolean conditions in the AST
  pub fn traverse_condition(condition: &Condition) -> Vec<AstNode> {
    match condition {
      Condition::Term(term_query) => term_query
        .term
        .clone()
        .map(AstNode::Match)
        .into_iter()
        .collect(),
      Condition::Bool(bool_query) => vec![Segment::query_to_ast(bool_query)],
    }
  }

  fn traverse_ast(&self, node: &AstNode) -> HashSet<u32> {
    match node {
      AstNode::Match(terms) => {
        let terms_lowercase = terms.to_lowercase();
        let terms = tokenize(&terms_lowercase);
        let mut results = Vec::new();

        // Get postings lists for the query terms
        let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
          self.get_postings_lists(&terms);

        // No postings lists were found so let's return empty handed
        if postings_lists.is_empty() {
          debug!("No posting list found. Returning empty handed.");
          return HashSet::new();
        }

        // Now get the matching document IDs from the postings lists
        self.get_matching_doc_ids(
          &postings_lists,
          &last_block_list,
          &initial_values_list,
          shortest_list_index,
          &mut results,
        );

        // Convert Vec<T> to HashSet<T> without consuming the Vec
        let result_set: HashSet<_> = results.iter().cloned().collect();
        result_set
      }
      AstNode::Must(left, right) => {
        let left_results = self.traverse_ast(left);
        let right_results = self.traverse_ast(right);

        if matches!(**left, AstNode::None) {
          return right_results;
        } else if matches!(**right, AstNode::None) {
          return left_results;
        }

        left_results.intersection(&right_results).cloned().collect()
      }
      AstNode::MustNot(node) => {
        let node_results = self.traverse_ast(node);
        let all_doc_ids: HashSet<u32> = self.forward_map.iter().map(|entry| *entry.key()).collect();
        all_doc_ids.difference(&node_results).cloned().collect()
      }
      AstNode::Should(left, right) => {
        let left_results = self.traverse_ast(left);
        let right_results = self.traverse_ast(right);
        left_results.union(&right_results).cloned().collect()
      }
      AstNode::None => HashSet::new(),
    }
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

impl PartialEq for Segment {
  fn eq(&self, other: &Self) -> bool {
    self.metadata.get_id() == other.metadata.get_id()
  }
}

impl Eq for Segment {}

impl PartialOrd for Segment {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    other
      .metadata
      .get_end_time()
      .partial_cmp(&self.metadata.get_end_time())
  }
}

impl Ord for Segment {
  fn cmp(&self, other: &Self) -> Ordering {
    self.partial_cmp(other).unwrap()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, RwLock};

  use chrono::Utc;
  use tempdir::TempDir;

  use super::*;
  use crate::utils::sync::{is_sync, thread};

  fn create_term_test_node(term: &str) -> AstNode {
    AstNode::Match(term.to_string())
  }

  // Populate segment with log messages for testing
  fn populate_segment(segment: &mut Segment) {
    let log_messages = vec![
      ("log 1", "this is a test log message"),
      ("log 2", "this is another log message"),
      ("log 3", "test log for different term"),
    ];

    for (key, message) in log_messages.iter() {
      let mut fields = HashMap::new();
      fields.insert("key".to_string(), key.to_string());
      segment
        .append_log_message(Utc::now().timestamp_millis() as u64, &fields, message)
        .unwrap();
    }
  }

  #[test]
  fn test_new_segment() {
    is_sync::<Segment>();

    let segment = Segment::new();
    assert!(segment.is_empty());

    // Create an AstNode for the term "doesnotexist"
    let query_node = create_term_test_node("doesnotexist");
    assert!(segment.search_logs(&query_node, 0, u64::MAX).is_empty());
  }

  #[test]
  fn test_default_segment() {
    let segment = Segment::default();
    assert!(segment.is_empty());

    // Create an AstNode for the term "doesnotexist"
    let query_node = create_term_test_node("doesnotexist");
    assert!(segment.search_logs(&query_node, 0, u64::MAX).is_empty());
  }

  // Test searching with a simple term query
  #[test]
  fn test_search_with_term_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let term_node = create_term_test_node("test");
    let results = segment.search_logs(&term_node, 0, u64::MAX);

    assert_eq!(results.len(), 2); // Assuming two logs contain the term "test"
  }

  // Test searching with a Must (AND) query with single term
  #[test]
  fn test_search_with_must_query_single_term() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let ast = AstNode::None;
    let right_node = create_term_test_node("another");
    let must_node = AstNode::Must(Box::new(ast), Box::new(right_node));
    let results = segment.search_logs(&must_node, 0, u64::MAX);

    assert_eq!(results.len(), 1); // Assuming only one log contains "another"
  }

  // Test searching with a Must (AND) query
  #[test]
  fn test_search_with_must_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let left_node = create_term_test_node("this");
    let right_node = create_term_test_node("another");
    let must_node = AstNode::Must(Box::new(left_node), Box::new(right_node));
    let results = segment.search_logs(&must_node, 0, u64::MAX);

    assert_eq!(results.len(), 1); // Assuming only one log contains both "this" and "another"
  }

  // Test searching with a MustNot (NOT) query
  #[test]
  fn test_search_with_must_not_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let node = create_term_test_node("another");
    let must_not_node = AstNode::MustNot(Box::new(node));
    let results = segment.search_logs(&must_not_node, 0, u64::MAX);

    assert_eq!(results.len(), 2);

    assert!(results
      .iter()
      .all(|log| !log.get_text().contains("another")));
  }

  // Test searching with a Should (OR) query
  #[test]
  fn test_search_with_should_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let left_node = create_term_test_node("different");
    let right_node = create_term_test_node("this");
    let should_node = AstNode::Should(Box::new(left_node), Box::new(right_node));
    let results = segment.search_logs(&should_node, 0, u64::MAX);

    println!("Results: {:?}", results);

    assert_eq!(results.len(), 3); // Assuming all logs contain either "this" or "different"
  }

  // Test searching with a Filter query (time range)
  // #[test]
  // fn test_search_with_filter_query() {
  //   let mut segment = Segment::new();
  //   populate_segment(&mut segment);

  //   let start_time = Utc.ymd(2022, 1, 1).and_hms(0, 0, 0).timestamp_millis() as u64;
  //   let end_time = Utc.ymd(2022, 1, 2).and_hms(23, 59, 59).timestamp_millis() as u64;
  //   let filter_node = create_filter_test_node(start_time, end_time);
  //   let results = segment.search_logs(&filter_node, start_time, end_time);

  //   // Assuming logs within the time range are returned
  //   assert!(!results.is_empty());
  //   assert!(results
  //     .iter()
  //     .all(|log| log.get_time() >= start_time && log.get_time() <= end_time));
  // }

  // Test searching with a complex query combining Must and MustNot
  #[test]
  fn test_search_with_complex_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let term_node_test = create_term_test_node("test");
    let term_node_different = create_term_test_node("different");
    let must_not_node = AstNode::MustNot(Box::new(term_node_different));
    let complex_query = AstNode::Must(Box::new(term_node_test), Box::new(must_not_node));
    let results = segment.search_logs(&complex_query, 0, u64::MAX);

    assert_eq!(results.len(), 1); // Assuming only one log contains "test" but not "different"
  }

  #[test]
  fn test_search_with_nested_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    // Construct a nested AST
    let term_node_test = create_term_test_node("test");
    let term_node_this = create_term_test_node("this");
    let term_node_different = create_term_test_node("different");
    let term_node_another = create_term_test_node("another");

    let should_node = AstNode::Should(Box::new(term_node_this), Box::new(term_node_another));
    let must_not_node = AstNode::MustNot(Box::new(term_node_different));
    let complex_query = AstNode::Must(Box::new(term_node_test), Box::new(must_not_node));
    let nested_query = AstNode::Should(Box::new(should_node), Box::new(complex_query));

    let results = segment.search_logs(&nested_query, 0, u64::MAX);

    assert_eq!(results.len(), 2);
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

    // Test search for "this".
    let query_node_for_this = create_term_test_node("this");
    let mut results = from_disk_segment.search_logs(&query_node_for_this, 0, u64::MAX);
    assert_eq!(results.len(), 1);
    assert_eq!(
      results.get(0).unwrap().get_text(),
      "this is my 1st log message"
    );

    // Test search for "blah".
    let query_node_for_blah = create_term_test_node("blah");
    results = from_disk_segment.search_logs(&query_node_for_blah, 0, u64::MAX);
    assert!(results.is_empty());

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
    let query_node = create_term_test_node("hello");
    let results = segment.search_logs(&query_node, 0, u64::MAX);
    assert_eq!(results.len(), 2);
    assert_eq!(
      results.get(0).unwrap().get_text(),
      "hello world hello world"
    );
    assert_eq!(results.get(1).unwrap().get_text(), "hello world");
  }

  #[test]
  pub fn test_sort_segments() {
    let num_segments = 3;
    let mut segments = Vec::new();
    let mut expected_segment_ids = Vec::new();

    // Create a few segments.
    for i in 1..=num_segments {
      let segment = Segment::new();
      segment
        .append_log_message(i, &HashMap::new(), "some log message")
        .expect("Could not append to segment");
      segments.push(segment);

      // The latest created segment is expected to the first one in sorted segements - since the
      // they are sorted in reverse chronological order.
      expected_segment_ids.insert(0, segment.get_id().to_owned());
    }

    // Sort the segment summaries and retrieve their ids.
    segments.sort();
    let retrieved_segment_ids: Vec<String> = segments
      .iter()
      .map(|segment| segment.metadata.get_id().to_owned())
      .collect();

    // Make sure that the retrieved ids are in reverse cronological order - i.e., they are same as the expected ids.
    assert_eq!(retrieved_segment_ids, expected_segment_ids);
  }
}

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

/// Search a segment for matching document IDs
use std::{
  cmp::Ordering,
  collections::{BTreeMap, HashMap, VecDeque},
};

use crate::log::log_message::LogMessage;
use crate::log::postings_block::PostingsBlock;
use crate::log::postings_block_compressed::PostingsBlockCompressed;
use crate::request_manager::query_dsl::Rule;
use crate::segment_manager::segment::Segment;
use crate::utils::error::QueryError;
use crate::{
  log::constants::BLOCK_SIZE_FOR_LOG_MESSAGES, request_manager::query_dsl_object::QueryDSLObject,
};

use chrono::{TimeZone, Utc};

use log::debug;
use pest::iterators::Pairs;
use std::str;

use regex::bytes::Regex;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};

enum RangeOperator {
  GreaterThanOrEqual,
  LessThanOrEqual,
  GreaterThan,
  LessThan,
}

impl RangeOperator {
  fn from_str(op: &str) -> Option<Self> {
    match op {
      "gte" => Some(Self::GreaterThanOrEqual),
      "lte" => Some(Self::LessThanOrEqual),
      "gt" => Some(Self::GreaterThan),
      "lt" => Some(Self::LessThan),
      _ => None,
    }
  }
}

// These come from different index structures or computation,
// but we pull them together in memory for query processing
#[derive(Debug, Clone, Deserialize)]
pub struct QueryLogMessage {
  #[serde(rename = "_id")]
  id: u32,

  #[serde(skip_serializing)]
  message: LogMessage,
}

impl QueryLogMessage {
  /// Creates a new empty `QueryLogMessage`.
  pub fn new() -> Self {
    QueryLogMessage {
      id: 0,
      message: LogMessage::new(0, ""),
    }
  }

  /// Creates a new `QueryDocIds` with the given labels and metric points.
  pub fn new_with_params(id: u32, message: LogMessage) -> Self {
    QueryLogMessage { id, message }
  }

  /// Gets the doc_id
  pub fn get_id(&self) -> u32 {
    self.id
  }

  /// Sets the doc_id.
  pub fn set_id(&mut self, id: u32) {
    self.id = id;
  }

  /// Gets a reference to the doc_ids
  pub fn get_message(&self) -> &LogMessage {
    &self.message
  }

  /// Sets the doc_ids.
  pub fn set_message(&mut self, message: LogMessage) {
    self.message = message;
  }

  /// Take for vector - getter to allow the message to be
  /// transferred out of the object and comply with Rust's ownership rules
  pub fn take_message(&mut self) -> LogMessage {
    std::mem::take(&mut self.message)
  }
}

impl Default for QueryLogMessage {
  fn default() -> Self {
    Self::new()
  }
}

impl PartialEq for QueryLogMessage {
  fn eq(&self, other: &Self) -> bool {
    self.get_id() == other.get_id()
  }
}

impl PartialOrd for QueryLogMessage {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for QueryLogMessage {
  fn cmp(&self, other: &Self) -> Ordering {
    // Sort in reverse chronological order by time.
    other
      .get_message()
      .get_time()
      .cmp(&self.get_message().get_time())
  }
}

impl Eq for QueryLogMessage {}

impl Serialize for QueryLogMessage {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let log_message = &self.message;
    let mut map = serializer.serialize_map(None)?;

    map.serialize_entry("_index", &"your_index_name_here")?;
    map.serialize_entry("_id", &self.id)?;
    map.serialize_entry("_score", &1.0)?;

    let mut source = HashMap::new();
    source.insert("timestamp", log_message.get_time().to_string());
    source.insert("text", log_message.get_text().to_owned());
    let fields = log_message.get_fields().to_owned();
    for (key, value) in &fields {
      source.insert(key, value.clone());
    }

    map.serialize_entry("_source", &source)?;

    map.end()
  }
}

impl Segment {
  /// Search the segment for the given query.
  pub async fn search_logs(
    &self,
    ast: &Pairs<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<QueryDSLObject, QueryError> {
    debug!("Search index logs: Searching log for {:?}", ast);

    let mut results = QueryDSLObject::new();

    let matching_document_ids = self
      .traverse_query_dsl_ast(ast)
      .await
      .map_err(|_| QueryError::SearchLogsError("Could not process the query".to_string()))?;

    // Get the log messages and return with the query results
    let log_messages = self
      .get_log_messages_from_ids(
        matching_document_ids.get_ids(),
        range_start_time,
        range_end_time,
      )
      .map_err(|_| {
        QueryError::SearchLogsError("Could not get log messages from the ids".to_string())
      })?;

    results.set_execution_time(matching_document_ids.get_execution_time());
    results.set_messages(log_messages);

    debug!("Search index logs: Returning results {:?}", results);

    Ok(results)
  }

  // **** Leaf Query Support ****

  /// Search the index for a set of terms
  pub async fn search_inverted_index(
    &self,
    terms: Vec<String>,
    term_operator: &str,
  ) -> Result<Vec<u32>, QueryError> {
    debug!(
      "Search inverted index: Searching inverted index for {:?} using {:?}",
      terms, term_operator
    );

    // Extract the terms and perform the search
    let mut results = Vec::new();

    // Get postings lists for the query terms
    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) = self
      .get_postings_lists(&terms)
      .map_err(|e| QueryError::TraverseError(format!("Error getting postings lists: {:?}", e)))?;

    if postings_lists.is_empty() {
      debug!("No posting list found. Returning empty handed.");
      return Ok(Vec::new());
    }

    // Now get the doc IDs in the posting lists
    if term_operator == "OR" {
      self
        .get_matching_doc_ids_with_logical_or(&postings_lists, &last_block_list, &mut results)
        .map_err(|e| QueryError::TraverseError(format!("Error matching doc IDs: {:?}", e)))?;
    } else {
      self
        .get_matching_doc_ids_with_logical_and(
          &postings_lists,
          &last_block_list,
          &initial_values_list,
          shortest_list_index,
          &mut results,
        )
        .map_err(|e| QueryError::TraverseError(format!("Error matching doc IDs: {:?}", e)))?;
    }

    debug!("Search inverted index: Returning results {:?}", results);

    Ok(results)
  }

  /// Get the posting lists belonging to a set of matching terms in the query
  #[allow(clippy::type_complexity)]
  pub fn get_postings_lists(
    &self,
    terms: &[String],
  ) -> Result<
    (
      Vec<Vec<PostingsBlockCompressed>>,
      Vec<PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>>,
      Vec<Vec<u32>>,
      usize,
    ),
    QueryError,
  > {
    let mut initial_values_list: Vec<Vec<u32>> = Vec::new();
    let mut postings_lists: Vec<Vec<PostingsBlockCompressed>> = Vec::new();
    let mut last_block_list: Vec<PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>> = Vec::new();
    let mut shortest_list_index = 0;
    let mut shortest_list_len = usize::MAX;

    for (index, term) in terms.iter().enumerate() {
      let postings_list = match self.get_postings_list(term) {
        Some(postings_list_ref) => postings_list_ref,
        None => {
          return Err(QueryError::PostingsListError(format!(
            "Postings list not found for term: {}",
            term
          )))
        }
      };

      let postings_list = postings_list.read();

      let initial_values = postings_list.get_initial_values().clone();
      initial_values_list.push(initial_values);

      let postings_block_compressed_vec: Vec<PostingsBlockCompressed> =
        postings_list.get_postings_list_compressed().to_vec();

      let last_block = postings_list.get_last_postings_block().clone();
      last_block_list.push(last_block);

      if postings_block_compressed_vec.len() < shortest_list_len {
        shortest_list_len = postings_block_compressed_vec.len();
        shortest_list_index = index;
      }

      postings_lists.push(postings_block_compressed_vec);
    }

    Ok((
      postings_lists,
      last_block_list,
      initial_values_list,
      shortest_list_index,
    ))
  }

  /// Get the matching doc IDs corresponding to a set of posting lists
  /// that are combined with a logical AND
  pub fn get_matching_doc_ids_with_logical_and(
    &self,
    postings_lists: &[Vec<PostingsBlockCompressed>],
    last_block_list: &[PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>],
    initial_values_list: &[Vec<u32>],
    shortest_list_index: usize,
    results: &mut Vec<u32>,
  ) -> Result<(), QueryError> {
    let accumulator = &mut Vec::new();

    if postings_lists.is_empty() {
      debug!("No postings lists. Returning");
      return Ok(());
    }

    let first_posting_blocks = &postings_lists[shortest_list_index];
    for posting_block in first_posting_blocks {
      let posting_block = PostingsBlock::try_from(posting_block).map_err(|_| {
        QueryError::DocMatchingError("Failed to convert to PostingsBlock".to_string())
      })?;
      let mut log_message_ids = posting_block.get_log_message_ids();
      accumulator.append(&mut log_message_ids);
    }

    let mut last_block_log_message_ids = last_block_list[shortest_list_index].get_log_message_ids();
    accumulator.append(&mut last_block_log_message_ids);

    if accumulator.is_empty() {
      debug!("Posting list is empty. Loading accumulator from last_block_list.");
      return Ok(());
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
                .map_err(|_| {
                  QueryError::DocMatchingError("Failed to convert to PostingsBlock".to_string())
                })?
                .get_log_message_ids();
            } else {
              _posting_block = last_block_list[i].get_log_message_ids();
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
              .get_log_message_ids();
          } else {
            // posting block is last block
            _posting_block = last_block_list[i].get_log_message_ids();
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

    results.append(&mut *accumulator);

    Ok(())
  }

  /// Get the matching doc IDs corresponding to a set of posting lists
  /// that are combined with a logical OR
  pub fn get_matching_doc_ids_with_logical_or(
    &self,
    postings_lists: &[Vec<PostingsBlockCompressed>],
    last_block_list: &[PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>],
    results: &mut Vec<u32>,
  ) -> Result<(), QueryError> {
    if postings_lists.is_empty() {
      debug!("No postings lists. Returning");
      return Ok(());
    }

    for i in 0..postings_lists.len() {
      let postings_list = &postings_lists[i];

      for posting_block in postings_list {
        let posting_block = PostingsBlock::try_from(posting_block).map_err(|_| {
          QueryError::DocMatchingError("Failed to convert to PostingsBlock".to_string())
        })?;
        let log_message_ids = posting_block.get_log_message_ids();
        results.append(&mut log_message_ids.clone());
      }

      let last_block_log_message_ids = last_block_list[i].get_log_message_ids();
      results.append(&mut last_block_log_message_ids.clone());
    }

    Ok(())
  }

  /// Match the exact phrase in log messages from the given log IDs in parallel.
  /// If the field name is provided, match the phrase in that field; otherwise, match in the text.
  pub fn get_exact_phrase_matches(
    &self,
    doc_ids: &[u32],
    field_name: Option<&str>,
    phrase_text: &str,
  ) -> Vec<u32> {
    let matching_document_ids: Vec<_> = doc_ids
      .iter()
      .filter_map(|log_id| {
        if let Some(log_message) = self.get_forward_map().get(log_id) {
          let log_message = log_message.value();
          let text_to_search = match field_name {
            Some(field) => log_message
              .get_fields()
              .get(field)
              .map_or("", String::as_str),
            None => log_message.get_text(),
          };

          if text_to_search.contains(phrase_text) {
            Some(*log_id)
          } else {
            None
          }
        } else {
          None
        }
      })
      .collect();

    matching_document_ids
  }

  /// Retrieve document IDs with prefix phrase match.
  ///
  /// # Arguments
  ///
  /// * `prefix_phrase_terms` - A vector of terms forming the prefix phrase, of which only the last term's prefix need to be considered
  /// * `field` - The field to search within.
  /// * `prefix_text_str` - The exact prefix text string to match.
  /// * `case_insensitive` - A boolean flag indicating whether the search is case-insensitive.
  ///
  pub async fn get_doc_ids_with_prefix_phrase(
    &self,
    prefix_phrase_terms: Vec<String>,
    field: &str,
    prefix_text_str: &str,
    case_insensitive: bool,
  ) -> Result<Vec<u32>, QueryError> {
    let mut matching_document_ids = Vec::new();

    if prefix_phrase_terms.len() == 1 {
      // If there's only a single term, directly get the terms with prefix, and search them in the segment
      let prefix_matches = self.get_terms_with_prefix(&prefix_phrase_terms[0], case_insensitive);
      let or_doc_ids = self.search_inverted_index(prefix_matches, "OR").await?;

      // From the given document IDs, filter document IDs which start with the exact phrase (in the given field)
      // and return the specific document IDs
      matching_document_ids.extend(self.get_bool_prefix_matches(
        &or_doc_ids,
        field,
        prefix_text_str,
        case_insensitive,
      ));
    } else {
      // Get all prefix matches for the last term

      let prefix_matches =
        self.get_terms_with_prefix(prefix_phrase_terms.last().unwrap(), case_insensitive);

      // Prepare a list to store document IDs from OR operations

      let mut or_doc_ids: Vec<u32> = Vec::new();

      // Perform AND operations on n-1 terms from analyzed_query and the last term's prefix matches
      for term in prefix_matches {
        let mut and_query = prefix_phrase_terms.clone();
        and_query.pop(); // Remove the last term from analyzed_query
        and_query.push(term.clone()); // Add the current term from prefix_matches

        let and_search_result = self.search_inverted_index(and_query, "AND").await?;
        or_doc_ids.extend(and_search_result);
      }
      // Remove duplicates from the list of document IDs

      or_doc_ids.dedup();

      // From the given document IDs, filter document IDs which start with the exact phrase (in the given field)
      // and return the specific document IDs

      matching_document_ids.extend(self.get_bool_prefix_matches(
        &or_doc_ids,
        field,
        prefix_text_str,
        case_insensitive,
      ));
    }

    Ok(matching_document_ids)
  }

  /// Match documents with a boolean prefix in a specified field.
  fn get_bool_prefix_matches(
    &self,
    doc_ids: &[u32],
    field_name: &str,
    prefix_text: &str,
    case_insensitive: bool,
  ) -> Vec<u32> {
    doc_ids
      .iter()
      .filter_map(|&log_id| {
        self.get_forward_map().get(&log_id).and_then(|log_message| {
          let log_message = log_message.value();
          log_message
            .get_fields()
            .get(field_name)
            .and_then(|field_value| {
              let value_to_compare = if case_insensitive {
                field_value.to_lowercase()
              } else {
                field_value.to_string()
              };

              if case_insensitive && value_to_compare.starts_with(&prefix_text.to_lowercase())
                || !case_insensitive && value_to_compare.starts_with(prefix_text)
              {
                Some(log_id)
              } else {
                None
              }
            })
        })
      })
      .collect()
  }

  /// Match documents based on multiple conditions across different fields.
  pub fn get_multi_match(
    &self,
    doc_ids: &[u32],
    conditions: &[(String, String)], // Each tuple contains (field_name, text_to_match)
  ) -> Vec<u32> {
    doc_ids
      .iter()
      .filter_map(|&log_id| {
        if let Some(log_message) = self.get_forward_map().get(&log_id) {
          let log_message = log_message.value();
          for (field_name, text_to_match) in conditions {
            if let Some(field_value) = log_message.get_fields().get(field_name) {
              if field_value.contains(text_to_match) {
                return Some(log_id);
              }
            }
          }
          None
        } else {
          None
        }
      })
      .collect()
  }

  pub fn get_range_matches(&self, doc_ids: &[u32], field_name: &str, range: &str) -> Vec<u32> {
    // Parse the range string
    let re = Regex::new(r"(?P<op>[gte|lte|gt|lt]+)\[(?P<value>[^\]]+)\]").unwrap();
    let captures = re.captures(range.as_bytes()).unwrap();
    let op_bytes = captures.name("op").unwrap().as_bytes();
    let op_str = str::from_utf8(op_bytes).unwrap();
    let value = captures
      .name("value")
      .and_then(|m| str::from_utf8(m.as_bytes()).ok())
      .and_then(|s| s.parse::<f64>().ok())
      .unwrap();

    // Convert operator string to enum
    let op = RangeOperator::from_str(op_str).unwrap();

    // Filter document IDs based on range
    doc_ids
      .iter()
      .filter_map(|&log_id| {
        if let Some(log_message) = self.get_forward_map().get(&log_id) {
          let log_message = log_message.value();
          if let Some(field_value) = log_message.get_fields().get(field_name) {
            let field_value = field_value.parse::<f64>().unwrap();
            match op {
              RangeOperator::GreaterThanOrEqual => {
                if field_value >= value {
                  return Some(log_id);
                }
              }
              RangeOperator::LessThanOrEqual => {
                if field_value <= value {
                  return Some(log_id);
                }
              }
              RangeOperator::GreaterThan => {
                if field_value > value {
                  return Some(log_id);
                }
              }
              RangeOperator::LessThan => {
                if field_value < value {
                  return Some(log_id);
                }
              }
            }
          }
        }
        None
      })
      .collect()
  }

  pub fn get_regexp_matches(&self, doc_ids: &[u32], field_name: &str, regexp: &str) -> Vec<u32> {
    // Compile the regular expression pattern
    let re = Regex::new(regexp).unwrap();

    // Filter document IDs based on regex match
    doc_ids
      .iter()
      .filter_map(|&log_id| {
        if let Some(log_message) = self.get_forward_map().get(&log_id) {
          let log_message = log_message.value();
          if let Some(field_value) = log_message.get_fields().get(field_name) {
            if re.is_match(field_value.as_bytes()) {
              // Convert to byte slice here
              return Some(log_id);
            }
          }
        }
        None
      })
      .collect()
  }

  pub fn get_wildcard_matches(
    &self,
    doc_ids: &[u32],
    field_name: &str,
    wildcard: &str,
  ) -> Vec<u32> {
    // Convert the wildcard pattern to a regex pattern
    let wildcard_pattern = wildcard
      .replace('.', "\\.")
      .replace('*', ".*")
      .replace('?', ".");

    // Compile the regex pattern
    let re = Regex::new(&wildcard_pattern).unwrap();

    // Filter document IDs based on wildcard match
    doc_ids
      .iter()
      .filter_map(|&log_id| {
        if let Some(log_message) = self.get_forward_map().get(&log_id) {
          let log_message = log_message.value();
          if let Some(field_value) = log_message.get_fields().get(field_name) {
            if re.is_match(field_value.as_bytes()) {
              // Convert to byte slice here
              return Some(log_id);
            }
          }
        }
        None
      })
      .collect()
  }

  // *** Metrics Aggregation Query Support ***

  /// Compute the average value for a specified field across documents in doc_ids.
  /// Includes an option to specify how missing values should be treated.
  pub fn avg(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
    treat_missing_as_zero: bool,
  ) -> Result<f64, QueryError> {
    let mut sum = 0.0;
    let mut count = 0;

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();

        match log_message.get_fields().get(field_name) {
          Some(field_value_str) => match field_value_str.parse::<f64>() {
            Ok(field_value) => {
              sum += field_value;
              count += 1;
            }
            Err(_) => {
              return Err(QueryError::CoreDBError(format!(
                "Could not parse field value for field '{}'",
                field_name
              )))
            }
          },
          None if treat_missing_as_zero => count += 1, // Treat missing as zero, effectively adding 0 to the sum and incrementing the count
          None => (),                                  // Skip missing values
        }
      }
    }

    if count == 0 {
      Err(QueryError::SearchLogsError(format!(
        "No valid data found for field '{}'",
        field_name
      )))
    } else {
      Ok(sum / count as f64)
    }
  }

  /// Compute the maximum value for a specified field across documents in doc_ids.
  #[allow(dead_code)]
  fn min(&self, doc_ids: &Vec<u32>, field_name: &str) -> Result<f64, QueryError> {
    let mut min_value: Option<f64> = None;

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();

        if let Some(field_value_str) = log_message.get_fields().get(field_name) {
          match field_value_str.parse::<f64>() {
            Ok(field_value) => {
              min_value = Some(match min_value {
                Some(current_max) => f64::min(current_max, field_value),
                None => field_value,
              });
            }
            Err(_) => {
              return Err(QueryError::CoreDBError(format!(
                "Could not parse field value for field '{}'",
                field_name
              )));
            }
          }
        }
      }
    }

    min_value.ok_or_else(|| {
      QueryError::SearchLogsError(format!("No valid data found for field '{}'", field_name))
    })
  }

  /// Compute the maximum value for a specified field across documents in doc_ids.
  #[allow(dead_code)]
  fn max(&self, doc_ids: &Vec<u32>, field_name: &str) -> Result<f64, QueryError> {
    let mut max_value: Option<f64> = None;

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();

        if let Some(field_value_str) = log_message.get_fields().get(field_name) {
          match field_value_str.parse::<f64>() {
            Ok(field_value) => {
              max_value = Some(match max_value {
                Some(current_max) => f64::max(current_max, field_value),
                None => field_value,
              });
            }
            Err(_) => {
              return Err(QueryError::CoreDBError(format!(
                "Could not parse field value for field '{}'",
                field_name
              )));
            }
          }
        }
      }
    }

    max_value.ok_or_else(|| {
      QueryError::SearchLogsError(format!("No valid data found for field '{}'", field_name))
    })
  }

  pub fn sum(&self, doc_ids: &Vec<u32>, field_name: &str) -> Result<f64, QueryError> {
    let mut total_sum = 0.0;

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();
        if let Some(field_value_str) = log_message.get_fields().get(field_name) {
          match field_value_str.parse::<f64>() {
            Ok(field_value) => total_sum += field_value,
            Err(_) => return Err(QueryError::SearchLogsError(field_name.to_string())),
          }
        }
      }
    }

    Ok(total_sum)
  }

  /// Computes statistics (count, min, max, sum, avg) for a specified field across documents.
  #[allow(dead_code)]
  fn stats(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
  ) -> Result<(u64, f64, f64, f64, f64), QueryError> {
    let mut count = 0;
    let mut sum = 0.0;
    let mut min_value: Option<f64> = None;
    let mut max_value: Option<f64> = None;

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        if let Some(field_value_str) = log_message.value().get_fields().get(field_name) {
          if let Ok(field_value) = field_value_str.parse::<f64>() {
            sum += field_value;
            count += 1;
            min_value = Some(min_value.map_or(field_value, |min| f64::min(min, field_value)));
            max_value = Some(max_value.map_or(field_value, |max| f64::max(max, field_value)));
          }
        }
      }
    }

    let avg = if count > 0 {
      Some(sum / count as f64)
    } else {
      None
    };
    Ok((
      count,
      sum,
      min_value.unwrap(),
      max_value.unwrap(),
      avg.unwrap(),
    ))
  }

  #[allow(dead_code)]
  fn cardinality(&self, doc_ids: &Vec<u32>, field_name: &str) -> Result<usize, QueryError> {
    let mut unique_values = Vec::new();
    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        if let Some(field_value) = log_message.value().get_fields().get(field_name) {
          // Clone the value so it's owned by unique_values
          unique_values.push(field_value.clone());
        }
      }
    }

    Ok(unique_values.len())
  }

  /// Counts the number of values for a specified field across documents.
  #[allow(dead_code)]
  fn value_count(&self, doc_ids: &Vec<u32>, field_name: &str) -> Result<usize, QueryError> {
    let mut count = 0;
    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        if log_message.value().get_fields().contains_key(field_name) {
          count += 1;
        }
      }
    }
    Ok(count)
  }

  pub fn percentile(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
    percentiles: Vec<u64>, // Percentiles requested, as values between 0 and 100.
  ) -> Result<BTreeMap<u64, f64>, QueryError> {
    let mut values = Vec::new();

    // Collecting values from documents
    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        if let Some(field_value_str) = log_message.value().get_fields().get(field_name) {
          match field_value_str.parse::<f64>() {
            Ok(field_value) => values.push(field_value),
            Err(_) => {
              return Err(QueryError::CoreDBError(format!(
                "Could not parse field value for field '{}'",
                field_name
              )))
            }
          }
        }
      }
    }

    // Sorting values to calculate percentiles
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut results = BTreeMap::new();
    for percentile in percentiles {
      let pos = (percentile as f64 / 100.0) * (values.len() as f64 - 1.0) + 1.0;
      let lower = values[(pos.floor() as usize) - 1];
      let upper = values[pos.ceil() as usize - 1];
      let value = lower + (upper - lower) * (pos - pos.floor());

      results.insert(percentile, value);
    }

    Ok(results)
  }

  /// Compute extended statistics for a specified field across documents in doc_ids.
  pub fn extended_stats(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
  ) -> Result<HashMap<String, f64>, QueryError> {
    let mut values = Vec::new();

    // Collecting values from documents
    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        if let Some(field_value_str) = log_message.value().get_fields().get(field_name) {
          match field_value_str.parse::<f64>() {
            Ok(field_value) => values.push(field_value),
            Err(_) => {
              return Err(QueryError::CoreDBError(format!(
                "Could not parse field value for field '{}'",
                field_name
              )))
            }
          }
        }
      }
    }

    let count = values.len() as f64;
    let sum: f64 = values.iter().sum();
    let avg = sum / count;
    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let variance = values
      .iter()
      .map(|&value| (value - avg).powi(2))
      .sum::<f64>()
      / count;
    let std_deviation = variance.sqrt();

    let results = HashMap::from([
      ("count".to_string(), count),
      ("sum".to_string(), sum),
      ("avg".to_string(), avg),
      ("min".to_string(), min),
      ("max".to_string(), max),
      ("variance".to_string(), variance),
      ("std_deviation".to_string(), std_deviation),
    ]);

    Ok(results)
  }

  // *** Bucket Aggregation Query Support ***

  /// Compute terms aggregation with additional parameters for inclusion and exclusion of terms.
  #[allow(clippy::too_many_arguments)]
  pub fn terms(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
    size: Option<usize>, // Maximum number of terms to include in the result.
    min_doc_count: Option<usize>, // Minimum count to include a term in the result.
    order: Option<&str>, // "asc" or "desc" for ordering by count.
    include: Option<Vec<String>>, // Terms to include.
    exclude: Option<Vec<String>>, // Terms to exclude.
  ) -> Result<HashMap<String, usize>, QueryError> {
    let mut counts = HashMap::new();

    // Convert include and exclude vectors to hash sets for efficient lookup.
    let include_set = include.map(|v| v.into_iter().collect::<Vec<_>>());
    let exclude_set = exclude.map(|v| v.into_iter().collect::<Vec<_>>());

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();
        if let Some(field_value) = log_message.get_fields().get(field_name) {
          // Check against include and exclude sets.
          let included = include_set
            .as_ref()
            .map_or(true, |set| set.contains(field_value));
          let excluded = exclude_set
            .as_ref()
            .map_or(false, |set| set.contains(field_value));

          if included && !excluded {
            *counts.entry(field_value.clone()).or_insert(0) += 1;
          }
        }
      }
    }

    // Apply min_doc_count filter.
    if let Some(min_count) = min_doc_count {
      counts.retain(|_, &mut count| count >= min_count);
    }

    // Apply sorting based on the order parameter.
    let mut sorted_counts: Vec<_> = counts.into_iter().collect();
    match order {
      Some("asc") => sorted_counts.sort_by(|a, b| a.1.cmp(&b.1)),
      Some("desc") => sorted_counts.sort_by(|a, b| b.1.cmp(&a.1)),
      _ => (), // No sorting or invalid value, leave as is.
    }

    // Apply size limit.
    let limited_counts: HashMap<String, usize> = if let Some(limit) = size {
      sorted_counts.into_iter().take(limit).collect()
    } else {
      sorted_counts.into_iter().collect()
    };

    if limited_counts.is_empty() {
      Err(QueryError::SearchLogsError(
        "No valid terms found based on the provided criteria".to_string(),
      ))
    } else {
      Ok(limited_counts)
    }
  }

  /// Compute a histogram of the specified numeric field across documents in doc_ids.
  /// Buckets are determined by the specified interval.
  pub fn histogram(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
    interval: f64,
  ) -> Result<BTreeMap<String, usize>, QueryError> {
    let mut histogram = BTreeMap::new();

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();

        if let Some(field_value_str) = log_message.get_fields().get(field_name) {
          match field_value_str.parse::<f64>() {
            Ok(field_value) => {
              let bucket = ((field_value / interval).floor() * interval).to_string(); // Convert bucket to string for BTreeMap
              *histogram.entry(bucket).or_insert(0) += 1;
            }
            Err(_) => {
              return Err(QueryError::CoreDBError(format!(
                "Could not parse field value for field '{}'",
                field_name
              )))
            }
          }
        }
      }
    }

    if histogram.is_empty() {
      Err(QueryError::SearchLogsError(format!(
        "No valid data found for field '{}'",
        field_name
      )))
    } else {
      Ok(histogram)
    }
  }

  #[allow(dead_code)]
  pub fn date_histogram(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
    interval: chrono::Duration,
  ) -> Result<BTreeMap<String, usize>, QueryError> {
    let mut histogram = BTreeMap::new();

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();

        if let Some(field_value_str) = log_message.get_fields().get(field_name) {
          if let Ok(timestamp) = field_value_str.parse::<i64>() {
            // Convert timestamp to DateTime<Utc>
            if let Some(datetime_utc) = Utc.timestamp_opt(timestamp, 0).single() {
              // Calculate the lower bound of the interval containing the datetime
              let lower_bound = datetime_utc - interval;
              // Calculate the upper bound of the interval containing the datetime
              let upper_bound = datetime_utc + interval;

              // Determine which interval the datetime falls into
              let rounded_datetime = if datetime_utc < lower_bound {
                lower_bound
              } else if datetime_utc >= upper_bound {
                upper_bound
              } else {
                datetime_utc
              };

              // Convert rounded datetime to string for BTreeMap
              let bucket = rounded_datetime.to_rfc3339();

              *histogram.entry(bucket).or_insert(0) += 1;
            } else {
              return Err(QueryError::CoreDBError(format!(
                "Invalid timestamp: {}",
                timestamp
              )));
            }
          } else {
            return Err(QueryError::CoreDBError(format!(
              "Could not parse date field value for field '{}'",
              field_name
            )));
          }
        }
      }
    }

    if histogram.is_empty() {
      Err(QueryError::SearchLogsError(format!(
        "No valid data found for date field '{}'",
        field_name
      )))
    } else {
      Ok(histogram)
    }
  }

  #[allow(dead_code)]
  fn set_of_terms(
    &self,
    doc_ids: &Vec<u32>,
    field_name: &str,
    size: Option<usize>, // Maximum number of terms to include in the result
    min_doc_count: Option<usize>, // Minimum count to include a term in the result
    order: Option<&str>, // "asc" or "desc" for ordering by count
  ) -> Result<HashMap<String, usize>, QueryError> {
    let mut term_counts = HashMap::new();

    for &doc_id in doc_ids {
      if let Some(log_message) = self.get_forward_map().get(&doc_id) {
        let log_message = log_message.value();
        if let Some(term) = log_message.get_fields().get(field_name) {
          *term_counts.entry(term.clone()).or_insert(0) += 1;
        }
      }
    }

    // Apply min_doc_count filter
    if let Some(min_count) = min_doc_count {
      term_counts.retain(|_, &mut count| count >= min_count);
    }

    // Convert to BTreeMap for optional sorting
    let mut sorted_counts: BTreeMap<_, _> = term_counts.into_iter().collect();

    // Apply ordering
    if let Some(order) = order {
      let mut items: Vec<_> = sorted_counts.into_iter().collect();
      if order == "desc" {
        items.sort_by(|a, b| b.1.cmp(&a.1));
      } else {
        items.sort_by(|a, b| a.1.cmp(&b.1));
      }
      sorted_counts = items.into_iter().collect();
    }

    // Apply size limit
    let limited_counts: HashMap<String, usize> = if let Some(size) = size {
      sorted_counts.into_iter().take(size).collect()
    } else {
      sorted_counts.into_iter().collect()
    };

    if limited_counts.is_empty() {
      Err(QueryError::SearchLogsError(format!(
        "No terms found for field '{}'",
        field_name
      )))
    } else {
      Ok(limited_counts)
    }
  }

  // *** Pipeline Aggregation Query Support ***

  /// Compute the average value for a specified field across buckets produced by a parent aggregation.
  #[allow(dead_code)]
  fn avg_bucket(
    &self,
    buckets: &HashMap<String, Vec<u32>>,
    field_name: &str,
  ) -> Result<HashMap<String, f64>, QueryError> {
    let mut averages = HashMap::new();

    for (bucket_key, doc_ids) in buckets {
      let mut sum = 0.0;
      let mut count = 0;

      for &doc_id in doc_ids {
        if let Some(log_message) = self.get_forward_map().get(&doc_id) {
          let log_message = log_message.value();

          if let Some(field_value_str) = log_message.get_fields().get(field_name) {
            match field_value_str.parse::<f64>() {
              Ok(field_value) => {
                sum += field_value;
                count += 1;
              }
              Err(_) => {
                return Err(QueryError::SearchLogsError(format!(
                  "Could not parse field value for field '{}' in document ID {}",
                  field_name, doc_id
                )));
              }
            }
          }
        }
      }

      if count > 0 {
        averages.insert(bucket_key.clone(), sum / count as f64);
      } else {
        // Optionally handle the case where a bucket has no valid data
        averages.insert(bucket_key.clone(), 0.0);
      }
    }

    Ok(averages)
  }

  /// Compute the sum of values for a specified field across buckets produced by a parent aggregation.
  #[allow(dead_code)]
  fn sum_bucket(
    &self,
    buckets: &HashMap<String, Vec<u32>>,
    field_name: &str,
  ) -> Result<HashMap<String, f64>, QueryError> {
    let mut sums = HashMap::new();

    for (bucket_key, doc_ids) in buckets {
      let mut sum = 0.0;

      for &doc_id in doc_ids {
        if let Some(log_message) = self.get_forward_map().get(&doc_id) {
          let log_message = log_message.value();

          if let Some(field_value_str) = log_message.get_fields().get(field_name) {
            match field_value_str.parse::<f64>() {
              Ok(field_value) => {
                sum += field_value;
              }
              Err(_) => {
                return Err(QueryError::SearchLogsError(format!(
                  "Could not parse field value for field '{}' in document ID {}",
                  field_name, doc_id
                )));
              }
            }
          }
        }
      }

      sums.insert(bucket_key.clone(), sum);
    }

    Ok(sums)
  }

  #[allow(dead_code)]
  fn cumulative_sum(
    &self,
    buckets: &HashMap<String, Vec<u32>>,
    field_name: &str,
  ) -> Result<HashMap<String, f64>, QueryError> {
    let mut cumulative_sum = 0.0;
    let mut cumulative_sums = HashMap::new();

    // Assuming buckets are sorted by key (e.g., timestamp or other numeric key)
    for (bucket_key, doc_ids) in buckets.iter() {
      let sum_for_bucket: f64 = doc_ids
        .iter()
        .filter_map(|&doc_id| {
          self.get_forward_map().get(&doc_id).and_then(|log_message| {
            log_message
              .value()
              .get_fields()
              .get(field_name)
              .and_then(|value_str| value_str.parse::<f64>().ok())
          })
        })
        .sum();

      cumulative_sum += sum_for_bucket;
      cumulative_sums.insert(bucket_key.clone(), cumulative_sum);
    }

    Ok(cumulative_sums)
  }

  #[allow(dead_code)]
  pub fn moving_avg(
    &self,
    buckets: &HashMap<String, Vec<u32>>,
    field_name: &str,
    window_size: usize,
  ) -> Result<HashMap<String, f64>, QueryError> {
    let mut moving_averages = HashMap::new();
    let mut values_queue: VecDeque<f64> = VecDeque::new();
    let mut sum = 0.0;

    for (bucket_key, doc_ids) in buckets.iter() {
      let value = self.avg(doc_ids, field_name, true)?;
      // Adjustments as per the context
      sum += value;
      values_queue.push_back(value);
      if values_queue.len() > window_size {
        sum -= values_queue.pop_front().unwrap();
      }
      let avg = sum / values_queue.len() as f64;
      moving_averages.insert(bucket_key.clone(), avg);
    }

    Ok(moving_averages)
  }

  // TODO: Interval queries need intervals stored in the index. We need to decide if / how to support this.
  // TODO: Span queries need spans stored in the index. We need to decide if / how to support this.
}

// TODO: We should probably test read locks.
#[cfg(test)]
mod tests {
  use crate::utils::config::config_test_logger;
  use std::collections::HashMap;

  use super::*;
  use crate::{log::postings_list::PostingsList, segment_manager::segment::Segment};
  use std::vec::Vec;

  fn create_mock_compressed_block(
    initial: u32,
    num_bits: u8,
    log_message_ids_compressed: &[u8],
  ) -> PostingsBlockCompressed {
    let mut valid_compressed_data = vec![0; 128];
    valid_compressed_data[..log_message_ids_compressed.len()]
      .copy_from_slice(log_message_ids_compressed);

    PostingsBlockCompressed::new_with_params(initial, num_bits, &valid_compressed_data)
  }

  fn create_mock_postings_block(
    log_message_ids: Vec<u32>,
  ) -> PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
    PostingsBlock::new_with_log_message_ids_vec(log_message_ids)
      .expect("Could not create mock postings block")
  }

  fn create_mock_postings_list(
    compressed_blocks: Vec<PostingsBlockCompressed>,
    last_block: PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>,
    initial_values: Vec<u32>,
  ) -> PostingsList {
    PostingsList::new_with_params(compressed_blocks, last_block, initial_values)
  }

  fn create_mock_segment() -> Segment {
    config_test_logger();

    let segment = Segment::new_with_temp_wal();

    segment.insert_in_terms("term1", 1);
    segment.insert_in_terms("term2", 2);

    let mock_compressed_block1 = create_mock_compressed_block(123, 8, &[0x1A, 0x2B, 0x3C, 0x4D]);
    let mock_compressed_block2 = create_mock_compressed_block(124, 8, &[0x5E, 0x6F, 0x7D, 0x8C]);

    let mock_postings_block1 = create_mock_postings_block(vec![100, 200, 300]);
    let mock_postings_block2 = create_mock_postings_block(vec![400, 500, 600]);

    let postings_list1 = create_mock_postings_list(
      vec![mock_compressed_block1],
      mock_postings_block1,
      vec![1, 2, 3],
    );
    let postings_list2 = create_mock_postings_list(
      vec![mock_compressed_block2],
      mock_postings_block2,
      vec![4, 5, 6],
    );

    segment.insert_in_inverted_map(1, postings_list1);
    segment.insert_in_inverted_map(2, postings_list2);

    segment
  }

  #[test]
  fn test_get_postings_lists_success() {
    let segment = create_mock_segment();
    let terms = vec!["term1".to_string(), "term2".to_string()];

    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());

    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
      result.unwrap();
    assert_eq!(postings_lists.len(), 2);
    assert!(!postings_lists[0].is_empty());
    assert!(!postings_lists[1].is_empty());
    assert_eq!(last_block_list.len(), 2);
    assert_eq!(initial_values_list.len(), 2);
    assert!(!initial_values_list[0].is_empty());
    assert!(!initial_values_list[1].is_empty());
    assert_eq!(shortest_list_index, 0);
  }

  #[test]
  fn test_get_postings_lists_term_not_found() {
    let segment = create_mock_segment();
    let terms = vec!["unknown_term".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(matches!(result, Err(QueryError::PostingsListError(_))));
  }

  #[test]
  fn test_get_postings_lists_empty_terms() {
    let segment = create_mock_segment();
    let terms: Vec<String> = Vec::new();
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());
  }

  #[test]
  fn test_get_postings_lists_empty_segment() {
    let segment = Segment::new_with_temp_wal();
    let terms = vec!["term1".to_string(), "term2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(matches!(result, Err(QueryError::PostingsListError(_))));
  }

  #[test]
  fn test_get_postings_lists_single_term() {
    let segment = create_mock_segment();
    let terms = vec!["term1".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());

    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
      result.unwrap();

    assert_eq!(postings_lists.len(), 1);
    assert!(!postings_lists[0].is_empty());
    assert_eq!(last_block_list.len(), 1);
    assert_eq!(initial_values_list.len(), 1);
    assert!(!initial_values_list[0].is_empty());
    assert_eq!(shortest_list_index, 0);
  }

  #[test]
  fn test_get_postings_lists_multiple_terms_no_common_documents() {
    let segment = create_mock_segment();
    let terms = vec!["term1".to_string(), "term2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());
  }

  #[test]
  fn test_get_postings_lists_invalid_term_id_handling() {
    let segment = create_mock_segment();
    segment.insert_in_inverted_map(999, PostingsList::new());
    let terms = vec!["term1".to_string(), "term2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());
  }

  #[test]
  fn test_get_postings_lists_all_terms_not_found() {
    let segment = create_mock_segment();
    let terms = vec!["unknown1".to_string(), "unknown2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(matches!(result, Err(QueryError::PostingsListError(_))));
  }

  #[test]
  fn test_get_postings_lists_partially_found_terms() {
    let segment = create_mock_segment();
    let terms = vec!["term1".to_string(), "unknown".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(matches!(result, Err(QueryError::PostingsListError(_))));
  }

  #[test]
  fn test_get_postings_lists_with_mixed_found_and_not_found_terms() {
    let segment = create_mock_segment();
    let terms = vec![
      "term1".to_string(),
      "unknown_term".to_string(),
      "term2".to_string(),
    ];
    let result = segment.get_postings_lists(&terms);
    assert!(matches!(result, Err(QueryError::PostingsListError(_))));
  }

  #[test]
  fn test_get_postings_lists_with_terms_having_empty_postings_lists() {
    let segment = create_mock_segment();
    segment.insert_in_inverted_map(3, PostingsList::new());
    segment.insert_in_inverted_map(4, PostingsList::new());
    segment.get_terms().insert("term3".to_string(), 3);
    segment.get_terms().insert("term4".to_string(), 4);
    let terms = vec!["term3".to_string(), "term4".to_string()];
    let result = segment.get_postings_lists(&terms);
    let (postings_lists, _, _, _) = result.unwrap();
    assert!(postings_lists.iter().all(|list| list.is_empty()));
  }

  #[test]
  fn test_get_postings_lists_with_non_string_terms() {
    let segment = create_mock_segment();
    let terms = vec!["".to_string(), "123".to_string(), "!@#".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(matches!(result, Err(QueryError::PostingsListError(_))));
  }

  #[test]
  fn test_get_postings_lists_with_incomplete_data_in_segment() {
    let segment = create_mock_segment();
    // Simulate incomplete data by clearing inverted map
    segment.clear_inverted_map();

    let terms = vec!["term1".to_string(), "term2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_err());
  }

  #[test]
  fn test_get_postings_lists_with_empty_postings_lists() {
    let segment = create_mock_segment();
    let postings_lists: Vec<Vec<PostingsBlockCompressed>> = Vec::new();
    let last_block_list: Vec<PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>> = Vec::new();
    let initial_values_list: Vec<Vec<u32>> = Vec::new();
    let mut accumulator: Vec<u32> = Vec::new();

    let result = segment.get_matching_doc_ids_with_logical_and(
      &postings_lists,
      &last_block_list,
      &initial_values_list,
      0,
      &mut accumulator,
    );

    assert!(result.is_ok());
    assert!(
      accumulator.is_empty(),
      "Accumulator should be empty for empty postings lists"
    );
  }

  #[test]
  fn test_get_matching_doc_ids_with_no_matching_documents() {
    let segment = create_mock_segment();
    let terms = vec!["term1".to_string(), "term2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());

    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
      result.unwrap();

    let mut accumulator: Vec<u32> = Vec::new();
    let result = segment.get_matching_doc_ids_with_logical_and(
      &postings_lists,
      &last_block_list,
      &initial_values_list,
      shortest_list_index,
      &mut accumulator,
    );

    assert!(result.is_ok());
    assert!(accumulator.is_empty());
  }

  #[test]
  fn test_get_matching_doc_ids_with_multiple_terms_common_documents() {
    let segment = create_mock_segment();
    let terms = vec!["term1".to_string(), "term2".to_string()];
    let result = segment.get_postings_lists(&terms);
    assert!(result.is_ok());

    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
      result.unwrap();

    assert_eq!(postings_lists.len(), 2);
    let mut accumulator: Vec<u32> = Vec::new();
    let result = segment.get_matching_doc_ids_with_logical_and(
      &postings_lists,
      &last_block_list,
      &initial_values_list,
      shortest_list_index,
      &mut accumulator,
    );

    assert!(result.is_ok());
    assert!(
      accumulator.is_empty(),
      "No common documents should be found"
    );
  }

  #[test]
  fn test_get_matching_doc_ids_empty_postings_lists() {
    let segment = create_mock_segment();
    let postings_lists = vec![];
    let last_block_list = vec![];
    let initial_values_list = vec![];
    let mut accumulator: Vec<u32> = Vec::new();

    let result = segment.get_matching_doc_ids_with_logical_and(
      &postings_lists,
      &last_block_list,
      &initial_values_list,
      0,
      &mut accumulator,
    );

    assert!(result.is_ok());
    assert!(
      accumulator.is_empty(),
      "Accumulator should be empty for empty postings lists"
    );
  }

  #[test]
  fn test_get_matching_doc_ids_with_logical_or_basic() {
    let segment = create_mock_segment();
    let (postings_lists, last_block_list, _, _) = segment
      .get_postings_lists(&["term1".to_string(), "term2".to_string()])
      .unwrap();

    let mut accumulator: Vec<u32> = Vec::new();
    let result = segment.get_matching_doc_ids_with_logical_or(
      &postings_lists,
      &last_block_list,
      &mut accumulator,
    );

    assert!(result.is_ok());
    assert!(
      !accumulator.is_empty(),
      "Accumulator should not be empty for non-empty postings lists"
    );
    // Check for uniqueness of document IDs
    let unique_ids: Vec<_> = accumulator.iter().collect();
    assert_eq!(
      unique_ids.len(),
      accumulator.len(),
      "Document IDs should be unique"
    );
  }

  #[test]
  fn test_get_matching_doc_ids_with_logical_or_empty_lists() {
    let segment = create_mock_segment();

    let mut accumulator: Vec<u32> = Vec::new();
    let result = segment.get_matching_doc_ids_with_logical_or(&[], &[], &mut accumulator);

    assert!(result.is_ok());
    assert!(
      accumulator.is_empty(),
      "Accumulator should be empty when postings lists are empty"
    );
  }

  #[test]
  fn test_get_matching_doc_ids_with_logical_or_single_list() {
    let segment = create_mock_segment();
    let (postings_lists, last_block_list, _, _) =
      segment.get_postings_lists(&["term1".to_string()]).unwrap();

    let mut accumulator: Vec<u32> = Vec::new();
    let result = segment.get_matching_doc_ids_with_logical_or(
      &postings_lists,
      &last_block_list,
      &mut accumulator,
    );

    assert!(result.is_ok());

    assert!(
      !accumulator.is_empty(),
      "Accumulator should not be empty for a single non-empty postings list"
    );
    let unique_ids: Vec<_> = accumulator.iter().collect();
    assert_eq!(
      unique_ids.len(),
      accumulator.len(),
      "Document IDs should be unique"
    );
  }

  #[test]
  fn test_get_matching_doc_ids_with_logical_or_overlapping_ids() {
    let segment = create_mock_segment();
    let (postings_lists, last_block_list, _, _) = segment
      .get_postings_lists(&["term1".to_string(), "term2".to_string()])
      .unwrap();

    let mut accumulator: Vec<u32> = Vec::new();
    let result = segment.get_matching_doc_ids_with_logical_or(
      &postings_lists,
      &last_block_list,
      &mut accumulator,
    );

    assert!(result.is_ok());
    let unique_ids: Vec<_> = accumulator.iter().collect();
    assert_eq!(
      unique_ids.len(),
      accumulator.len(),
      "Document IDs should be unique after deduplication"
    );
  }

  #[tokio::test]
  async fn test_match_exact_phrase() {
    let segment = Segment::new_with_temp_wal();

    segment
      .append_log_message(1001, &HashMap::new(), "log 1")
      .unwrap();
    segment
      .append_log_message(1002, &HashMap::new(), "log 2")
      .unwrap();
    segment
      .append_log_message(1003, &HashMap::new(), "log 3")
      .unwrap();

    // Get all log message IDs from the forward map.
    let all_log_ids: Vec<u32> = segment
      .get_forward_map()
      .iter()
      .map(|entry| *entry.key())
      .collect();

    // Test matching exact phrase in text.
    let log_ids_text = segment.get_exact_phrase_matches(&all_log_ids, None, "log 2");
    assert_eq!(log_ids_text, Vec::from_iter(vec![1]));

    // Test matching exact phrase in a specific field.
    let mut fields = HashMap::new();
    fields.insert("field_name".to_string(), "log 3".to_string());
    segment
      .append_log_message(1004, &fields, "some message")
      .unwrap();

    let all_log_ids_with_fields: Vec<u32> = segment
      .get_forward_map()
      .iter()
      .map(|entry| *entry.key())
      .collect();

    let log_ids_field =
      segment.get_exact_phrase_matches(&all_log_ids_with_fields, Some("field_name"), "log 3");
    assert_eq!(log_ids_field, Vec::from_iter(vec![3]));
  }
}

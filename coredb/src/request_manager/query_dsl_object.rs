// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

// TODO: Add error checking
// TODO: Histograms are not yet supported
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

// Note that each vector is maintained separately, so if you perform
// an operation on one of the vectors you must update the other vectors.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryDSLDocIds {
  #[serde(rename = "took")]
  execution_time: u64,

  #[serde(skip_serializing)]
  ids: Vec<u32>,
}

impl QueryDSLDocIds {
  /// Constructor
  pub fn new() -> Self {
    QueryDSLDocIds {
      execution_time: 0,
      ids: Vec::new(),
    }
  }

  /// Getter for ids vector
  pub fn get_ids(&self) -> &Vec<u32> {
    &self.ids
  }

  /// Setter for ids vector
  pub fn set_ids(&mut self, ids: Vec<u32>) {
    self.ids = ids;
  }

  /// Take for ids vector - getter to allow the vector to be
  /// transferred out of the object and comply with Rust's ownership rules
  pub fn take_ids(&mut self) -> Vec<u32> {
    std::mem::take(&mut self.ids)
  }

  /// Getter for execution time
  pub fn get_execution_time(&self) -> u64 {
    self.execution_time
  }

  /// Setter for execution time
  pub fn set_execution_time(&mut self, execution_time: u64) {
    self.execution_time = execution_time;
  }

  // ******** Boolean Operators *********

  /// Retains docids in the ids vector that have matching terms with the other `QueryDSLDocIds`.
  pub fn and(&mut self, other: &QueryDSLDocIds) {
    self.ids.retain(|&id| other.ids.contains(&id));
  }

  /// Merges elements from the other `QueryDSLDocIds` into the ids vector if they do not already exist.
  pub fn or(&mut self, other: &QueryDSLDocIds) {
    for &id in &other.ids {
      if !self.ids.contains(&id) {
        self.ids.push(id);
      }
    }
  }

  /// Retains elements in the ids vector unless they have matching terms with the other `QueryDSLDocIds`.
  pub fn not(&mut self, other: &QueryDSLDocIds) {
    self.ids.retain(|&id| !other.ids.contains(&id));
  }
}

use std::cmp::Ordering;

impl Default for QueryDSLDocIds {
  fn default() -> Self {
    Self::new()
  }
}

impl PartialEq for QueryDSLDocIds {
  fn eq(&self, other: &Self) -> bool {
    self.ids == other.ids
  }
}

impl Eq for QueryDSLDocIds {}

impl PartialOrd for QueryDSLDocIds {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.ids.len().cmp(&other.ids.len()))
  }
}

impl Ord for QueryDSLDocIds {
  fn cmp(&self, other: &Self) -> Ordering {
    self.ids.len().cmp(&other.ids.len())
  }
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryDSLObject {
  #[serde(rename = "took")]
  execution_time: u64,

  timed_out: bool,

  index: String,

  #[serde(skip_serializing)]
  messages: Vec<QueryLogMessage>,
}

impl QueryDSLObject {
  /// Constructor
  pub fn new() -> Self {
    QueryDSLObject {
      execution_time: 0,
      timed_out: false,
      index: String::new(),
      messages: Vec::new(),
    }
  }

  /// Constructor with parameters
  pub fn new_with_params(
    execution_time: u64,
    timed_out: bool,
    index: String,
    messages: Vec<QueryLogMessage>,
  ) -> Self {
    QueryDSLObject {
      execution_time,
      timed_out,
      index,
      messages,
    }
  }

  /// Getter for messages vector
  pub fn get_messages(&self) -> &Vec<QueryLogMessage> {
    &self.messages
  }

  /// Setter for messages vector
  pub fn set_messages(&mut self, messages: Vec<QueryLogMessage>) {
    self.messages = messages;
  }

  /// Append to the messages vector
  pub fn append_messages(&mut self, mut messages: Vec<QueryLogMessage>) {
    self.messages.append(&mut messages);
  }

  /// Sort the messages vector
  pub fn sort_messages(&mut self) {
    self.messages.sort();
  }

  /// Take for messages vector - getter to allow the vector to be
  /// transferred out of the object and comply with Rust's ownership rules
  pub fn take_messages(&mut self) -> Vec<QueryLogMessage> {
    std::mem::take(&mut self.messages)
  }

  /// Getter for execution time
  pub fn get_execution_time(&self) -> u64 {
    self.execution_time
  }

  /// Setter for execution time
  pub fn set_execution_time(&mut self, execution_time: u64) {
    self.execution_time = execution_time;
  }

  /// Getter for index name
  pub fn get_index(&self) -> &String {
    &self.index
  }

  /// Setter for index name
  pub fn set_index(&mut self, index: String) {
    self.index = index;
  }

  /// Getter for timed out
  pub fn get_timed_out(&self) -> bool {
    self.timed_out
  }

  /// Setter for timed out
  pub fn set_timed_out(&mut self, timed_out: bool) {
    self.timed_out = timed_out;
  }
}

use crate::segment_manager::search_logs::QueryLogMessage;

impl Default for QueryDSLObject {
  fn default() -> Self {
    Self::new()
  }
}

impl PartialEq for QueryDSLObject {
  fn eq(&self, other: &Self) -> bool {
    self.get_messages() == other.get_messages()
      && self.get_execution_time() == other.get_execution_time()
  }
}

impl Eq for QueryDSLObject {}

impl Serialize for QueryDSLObject {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut state = serializer.serialize_struct("QueryDSLObject", 4)?;
    state.serialize_field("took", &self.execution_time)?;
    state.serialize_field("timed_out", &self.timed_out)?;

    // Serialize "_shards" with hardcoded values.
    let shards = Shards {
      total: 0,
      successful: 0,
      skipped: 1,
      failed: 1,
    };
    state.serialize_field("_shards", &shards)?;

    // Prepare "hits" object.
    let hits = Hits {
      total: Total {
        value: self.messages.len() as u64,
        relation: "eq",
      },
      max_score: 1.0,
      hits: &self.messages,
    };
    state.serialize_field("hits", &hits)?;

    state.end()
  }
}

#[derive(Serialize)]
struct Hits<'a> {
  total: Total,
  max_score: f64,
  hits: &'a Vec<QueryLogMessage>,
}

#[derive(Serialize)]
struct Total {
  value: u64,
  relation: &'static str,
}

#[derive(Serialize)]
struct Shards {
  total: u64,
  successful: u64,
  skipped: u64,
  failed: u64,
}

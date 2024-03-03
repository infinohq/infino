// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

// TODO: Add error checking
// TODO: Histograms are not yet supported
use serde::{Deserialize, Serialize};

// Note that each vector is maintained separately, so if you perform
// an operation on one of the vectors you must update the other vectors.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryDSLDocIds {
  #[serde(skip_serializing)]
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

// Note that each vector is maintained separately, so if you perform
// an operation on one of the vectors you must update the other vectors.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryDSLObject {
  #[serde(skip_serializing)]
  execution_time: u64,

  #[serde(skip_serializing)]
  messages: Vec<QueryLogMessage>,
}

impl QueryDSLObject {
  /// Constructor
  pub fn new() -> Self {
    QueryDSLObject {
      execution_time: 0,
      messages: Vec::new(),
    }
  }

  /// Getter for ids vector
  pub fn get_messages(&self) -> &Vec<QueryLogMessage> {
    &self.messages
  }

  /// Setter for ids vector
  pub fn set_messages(&mut self, messages: Vec<QueryLogMessage>) {
    self.messages = messages;
  }

  /// Take for ids vector - getter to allow the vector to be
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

impl PartialOrd for QueryDSLObject {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for QueryDSLObject {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .execution_time
      .cmp(&other.execution_time)
      .then_with(|| self.messages.len().cmp(&other.messages.len()))
  }
}

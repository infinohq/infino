// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crate::log::log_message::LogMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
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

  /// Gets a reference to the doc_ids
  pub fn get_id(&self) -> u32 {
    self.id
  }

  /// Sets the doc_ids.
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

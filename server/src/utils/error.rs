// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use thiserror::Error;

#[derive(Debug, Error)]
/// Collection of error messages in Infino.
pub enum InfinoError {
  #[error("Invalid input {0}.")]
  InvalidInput(String),
}

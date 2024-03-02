// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crate::utils::error::AstError;
use chrono::{Duration, Utc};

/// Parses a time range string into a `Duration` object, handling Prometheus-style units.
pub fn parse_time_range(s: &str) -> Result<Duration, AstError> {
  if s.is_empty() {
    return Ok(Duration::seconds(0));
  }

  let units = s.chars().last().ok_or(AstError::UnsupportedQuery(
    "Invalid duration string".to_string(),
  ))?;
  let value = s[..s.len() - 1]
    .parse::<i64>()
    .map_err(|_| AstError::UnsupportedQuery("Invalid number in duration".to_string()))?;

  match units {
    's' => Ok(Duration::seconds(value)),
    'm' => Ok(Duration::minutes(value)),
    'h' => Ok(Duration::hours(value)),
    'd' => Ok(Duration::days(value)),
    'w' => Ok(Duration::weeks(value)),
    _ => Err(AstError::UnsupportedQuery(format!(
      "Unsupported duration unit: {:?}",
      s
    ))),
  }
}

// Check elapsed time after processing a query
pub fn check_query_time(timeout: u64, query_start_time: u64) -> Result<u64, AstError> {
  let query_end_time = Utc::now().timestamp_millis() as u64;
  let elapsed = query_end_time - query_start_time;
  if timeout != 0 && elapsed > timeout {
    let elapsed_seconds = elapsed as f64 / 1000.0;
    return Err(AstError::TimeOutError(format!(
      "Query took {:?} s",
      elapsed_seconds
    )));
  }

  Ok(elapsed)
}

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crate::utils::{
  error::QueryError,
  tokenize::{tokenize, FIELD_DELIMITER},
};
use chrono::{DateTime, Datelike, Duration, TimeDelta, Utc};
use log::debug;

/// Parses a time range string into a `Duration` object, handling Prometheus-style units.
pub fn parse_time_range(s: &str) -> Result<Duration, QueryError> {
  if s.is_empty() {
    return Ok(TimeDelta::try_seconds(0).unwrap());
  }

  let units = s.chars().last().ok_or(QueryError::UnsupportedQuery(
    "Invalid duration string".to_string(),
  ))?;
  let value = s[..s.len() - 1]
    .parse::<i64>()
    .map_err(|_| QueryError::UnsupportedQuery("Invalid number in duration".to_string()))?;

  match units {
    's' => Ok(TimeDelta::try_seconds(value).unwrap()),
    'm' => Ok(TimeDelta::try_minutes(value).unwrap()),
    'h' => Ok(TimeDelta::try_hours(value).unwrap()),
    'd' => Ok(TimeDelta::try_days(value).unwrap()),
    'w' => Ok(TimeDelta::try_weeks(value).unwrap()),
    _ => Err(QueryError::UnsupportedQuery(format!(
      "Unsupported duration unit: {:?}",
      s
    ))),
  }
}

// Check elapsed time after processing a query
pub fn check_query_time(timeout: u64, query_start_time: u64) -> Result<u64, QueryError> {
  let query_end_time = Utc::now().timestamp_millis() as u64;
  let elapsed = query_end_time - query_start_time;
  if timeout != 0 && elapsed > timeout {
    let elapsed_seconds = elapsed as f64 / 1000.0;
    return Err(QueryError::TimeOutError(format!(
      "Query took {:?} s",
      elapsed_seconds
    )));
  }

  Ok(elapsed)
}

/// Parses a date string to a bucket key based on the specified interval.
// TODO: come back to this. This is not yet used.
#[allow(dead_code)]
fn parse_date_string_to_bucket_key(date_str: &str, interval: &str) -> Result<String, QueryError> {
  let date = DateTime::parse_from_rfc3339(date_str)
    .map_err(|_| QueryError::SearchLogsError("Failed to parse date string".to_string()))?;

  let result = match interval {
    "year" => date.format("%Y").to_string(),
    "week" => {
      let iso_week = date.iso_week();
      format!("{}-W{}", iso_week.year(), iso_week.week())
    }
    "day" => date.format("%Y-%m-%d").to_string(),
    "hour" => date.format("%Y-%m-%d %H").to_string(),
    "minute" => date.format("%Y-%m-%d %H:%M").to_string(),
    _ => return Err(QueryError::SearchLogsError("Invalid interval".to_string())),
  };

  Ok(result)
}

pub async fn analyze_query_text(
  query_text: &str,
  fieldname: Option<&str>,
  case_insensitive: bool,
) -> Vec<String> {
  debug!(
    "Analyze text: Processing query {:?} {:?} {:?}",
    query_text, fieldname, case_insensitive
  );

  // Prepare the query string, applying lowercase if case_insensitive is set
  let query = if case_insensitive {
    query_text.to_lowercase()
  } else {
    query_text.to_owned()
  };

  let mut terms = Vec::new();
  tokenize(&query, &mut terms);

  // If fieldname is provided, concatenate it with each term; otherwise, use the term as is
  let transformed_terms: Vec<String> = if let Some(field) = fieldname {
    let prefix = format!("{}{}", field, FIELD_DELIMITER); // Prepare the prefix once
    terms
      .into_iter()
      .map(|term| format!("{}{}", prefix, term))
      .collect()
  } else {
    terms.into_iter().map(|term| term.to_owned()).collect() // No fieldname, just clone the term
  };

  transformed_terms
}

pub fn analyze_regex_query_text(
  regex_query_text: &str,
  fieldname: &str,
  case_insensitive: bool,
) -> String {
  debug!(
    "Analyze text: Processing regexp query {:?} {:?} {:?}",
    regex_query_text, fieldname, case_insensitive
  );

  let query = if case_insensitive {
    regex_query_text.to_lowercase()
  } else {
    regex_query_text.to_owned()
  };

  let regex_field_term = format!("{}{}{}", fieldname, FIELD_DELIMITER, query);

  regex_field_term
}

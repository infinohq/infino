// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use approx::abs_diff_eq;
use chrono::{DateTime, Datelike, LocalResult, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Represents a metric point in time series.
#[derive(Debug, Deserialize, Serialize)]
pub struct MetricPoint {
  /// Timestamp from epoch.
  time: u64,

  /// Value for this metric point.
  value: f64,
}

impl MetricPoint {
  /// Create a new MetricPoint from given time and value.
  pub fn new(time: u64, value: f64) -> Self {
    MetricPoint { time, value }
  }

  /// Create a new MetricPoint from given tsz::DataPoint.
  pub fn new_from_tsz_metric_point(tsz_metric_point: tsz::DataPoint) -> Self {
    MetricPoint {
      time: tsz_metric_point.get_time(),
      value: tsz_metric_point.get_value(),
    }
  }

  /// Get time.
  pub fn get_time(&self) -> u64 {
    self.time
  }

  /// Get value.
  pub fn get_value(&self) -> f64 {
    self.value
  }

  /// Set time.
  pub fn set_time(&mut self, time: u64) {
    self.time = time;
  }

  /// Set value.
  pub fn set_value(&mut self, value: f64) {
    self.value = value;
  }

  /// Get tsz::DataPoint corresponding to this MetricPoint.
  pub fn get_tsz_metric_point(&self) -> tsz::DataPoint {
    tsz::DataPoint::new(self.get_time(), self.get_value())
  }

  // Converts the epoch timestamp to a DateTime<Utc> object
  pub fn datetime(&self) -> DateTime<Utc> {
    match Utc.timestamp_opt(self.time as i64, 0) {
      LocalResult::Single(datetime) => datetime,
      _ => panic!("Failed to convert timestamp to DateTime<Utc>"),
    }
  }

  // Extracts the year from the timestamp
  pub fn year(&self) -> i32 {
    self.datetime().year()
  }

  // Extracts the month from the timestamp
  pub fn month(&self) -> u32 {
    self.datetime().month()
  }

  // Extracts the day of the month from the timestamp
  pub fn day(&self) -> u32 {
    self.datetime().day()
  }

  // Extracts the day of the week from the timestamp
  pub fn weekday(&self) -> chrono::Weekday {
    self.datetime().weekday()
  }

  // Extracts the hour from the timestamp
  pub fn hour(&self) -> u32 {
    self.datetime().hour()
  }
}

impl Clone for MetricPoint {
  fn clone(&self) -> MetricPoint {
    MetricPoint {
      time: self.get_time(),
      value: self.get_value(),
    }
  }
}

impl PartialEq for MetricPoint {
  #[inline]
  fn eq(&self, other: &MetricPoint) -> bool {
    // Two metric points are equal if their times are equal, and their values are either equal or are NaN.

    if self.time == other.time {
      if self.value.is_nan() {
        return other.value.is_nan();
      } else {
        return abs_diff_eq!(self.value, other.value);
      }
    }
    false
  }
}

impl Eq for MetricPoint {}

impl Ord for MetricPoint {
  fn cmp(&self, other: &Self) -> Ordering {
    self.time.cmp(&other.time)
  }
}

impl PartialOrd for MetricPoint {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Hash for MetricPoint {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.time.hash(state);
    self.value.to_bits().hash(state); // Converts f64 to its IEEE754 bit representation and hashes it
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_metric_point() {
    let time = 1;
    let value = 2.0;

    let dp = MetricPoint::new(time, value);
    assert_eq!(dp.get_time(), time);
    assert_eq!(dp.get_value(), value);

    let tsz_dp = dp.get_tsz_metric_point();
    assert_eq!(tsz_dp.get_time(), time);
    assert_eq!(tsz_dp.get_value(), value);

    let dp_from_tsz = MetricPoint::new_from_tsz_metric_point(tsz_dp);
    assert_eq!(dp, dp_from_tsz);
  }
}

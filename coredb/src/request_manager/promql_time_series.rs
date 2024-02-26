// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crate::metric::metric_point::MetricPoint;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromQLTimeSeries {
  labels: HashMap<String, String>,
  metric_points: Vec<MetricPoint>,
}

// For implementing PromQL functions on time series

impl PromQLTimeSeries {
  /// Creates a new empty `PromQLTimeSeries`.
  pub fn new() -> Self {
    PromQLTimeSeries {
      labels: HashMap::new(),
      metric_points: Vec::new(),
    }
  }

  /// Creates a new `PromQLTimeSeries` with the given labels and metric points.
  pub fn new_with_params(labels: HashMap<String, String>, metric_points: Vec<MetricPoint>) -> Self {
    PromQLTimeSeries {
      labels,
      metric_points,
    }
  }

  /// Gets a reference to the labels associated with the time series.
  pub fn get_labels(&self) -> &HashMap<String, String> {
    &self.labels
  }

  /// Gets a mutable reference to the metric points of the time series.
  pub fn get_metric_points(&mut self) -> &mut Vec<MetricPoint> {
    &mut self.metric_points
  }

  /// Takes ownership of the metric points, returning them and leaving the time series empty.
  /// This complies with Rust's ownership rules.
  pub fn take_metric_points(&mut self) -> Vec<MetricPoint> {
    std::mem::take(&mut self.metric_points)
  }

  /// Sets the labels associated with the time series.
  pub fn set_labels(&mut self, labels: HashMap<String, String>) {
    self.labels = labels;
  }

  /// Sets the metric points of the time series.
  pub fn set_metric_points(&mut self, metric_points: Vec<MetricPoint>) {
    self.metric_points = metric_points;
  }

  /// Checks if the time series is empty.
  pub fn is_empty(&self) -> bool {
    self.labels.is_empty() && self.metric_points.is_empty()
  }

  /// Returns the most recent timestamp among the metric points, if any.
  fn most_recent_timestamp(&self) -> Option<u64> {
    self.metric_points.iter().map(|mp| mp.get_time()).max()
  }

  // **** Functions: https://prometheus.io/docs/prometheus/latest/querying/functions

  /// Applies the absolute value operation to every metric point's value.
  pub fn abs(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().abs());
    }
  }

  /// Applies the ceiling operation to every metric point's value.
  pub fn ceil(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().ceil());
    }
  }

  /// Computes the changes between consecutive metric points.
  pub fn changes(&mut self) {
    let count = self
      .metric_points
      .windows(2)
      .filter(|window| match window {
        [a, b] => a.get_value() != b.get_value(),
        _ => false,
      })
      .count();
    let last_time = self.get_metric_points().last().unwrap().get_time();
    self.set_metric_points(vec![MetricPoint::new(last_time, count as f64)]);
  }

  /// Applies clamping to every metric point's value.
  pub fn clamp(&mut self, min: f64, max: f64) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().min(max).max(min));
    }
  }

  /// Clamps the maximum value of metric points.
  pub fn clamp_max(&mut self, max: f64) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().min(max));
    }
  }

  /// Clamps the minimum value of metric points.
  pub fn clamp_min(&mut self, min: f64) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().max(min));
    }
  }

  /// Extracts the day of the month from each metric point's timestamp.
  pub fn day_of_month(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.day() as f64);
      }
    }
  }

  /// Extracts the day of the week from each metric point's timestamp.
  pub fn day_of_week(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.weekday().num_days_from_sunday() as f64);
      }
    }
  }

  /// Extracts the day of the year from each metric point's timestamp.
  pub fn day_of_year(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.ordinal() as f64);
      }
    }
  }

  /// Extracts the number of days in the month from each metric point's timestamp.
  pub fn days_in_month(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        let year = datetime.year();
        let month = datetime.month();
        let next_month = if month == 12 { 1 } else { month + 1 };
        let year_for_next_month = if month == 12 { year + 1 } else { year };

        // Calculating the start of the next month to get the last day of the current month
        if let Some(next_month_start) = Utc
          .with_ymd_and_hms(year_for_next_month, next_month, 1, 0, 0, 0)
          .map(|dt| dt - chrono::Duration::days(1))
          .latest()
        {
          let days = next_month_start.day();
          mp.set_value(days as f64);
        }
      }
    }
  }

  /// Computes the difference between the first and last value of the metric points.
  pub fn delta(&mut self) {
    if let Some(first_point) = self.metric_points.first() {
      if let Some(last_point) = self.metric_points.last() {
        let delta = last_point.get_value() - first_point.get_value();
        self.set_metric_points(vec![MetricPoint::new(last_point.get_time(), delta)]);
      }
    }
  }

  /// Computes the derivative of metric points using simple linear regression.
  pub fn deriv(&mut self) {
    if let Some(deriv) = self.calculate_derivative(&self.metric_points) {
      // Replace the metric points with a single MetricPoint containing the derivative
      let last_time = self.metric_points.last().unwrap().get_time();
      self.metric_points = vec![MetricPoint::new(last_time, deriv)];
    } else {
      // If derivative cannot be computed, set the last metric point value to NaN
      let last_time = self.metric_points.last().unwrap().get_time();
      self.metric_points = vec![MetricPoint::new(last_time, f64::NAN)];
    }
  }

  /// Calculates the slope of the linear regression line through the metric points.
  fn calculate_derivative(&self, metric_points: &[MetricPoint]) -> Option<f64> {
    if metric_points.len() < 2 {
      return None; // Not enough points
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_x_squared = 0.0;
    let mut sum_xy = 0.0;
    let n = metric_points.len() as f64;
    let first_time = metric_points.first()?.get_time() as f64;

    for point in metric_points {
      let x = (point.get_time() as f64) - first_time; // Time since first sample
      let y = point.get_value();
      sum_x += x;
      sum_y += y;
      sum_x_squared += x * x;
      sum_xy += x * y;
    }

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x);
    Some(slope / (metric_points.last()?.get_time() as f64 - first_time))
  }

  /// Converts the value of each metric point from radians to degrees.
  pub fn deg(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value() * (180.0 / std::f64::consts::PI));
    }
  }

  /// Applies the exponential function to the value of each metric point.
  pub fn exp(&mut self) {
    for mp in &mut self.metric_points {
      let exponential = mp.get_value().exp();
      mp.set_value(exponential);
    }
  }

  /// Applies the floor function to the value of each metric point.
  pub fn floor(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().floor());
    }
  }

  /// Applies the Holt-Winters forecasting method to the metric points.
  pub fn holt_winters(&mut self, alpha: f64, beta: f64) {
    if self.metric_points.is_empty() {
      return;
    }

    let mut level = self.metric_points[0].get_value(); // Initial level
    let mut trend = 0.0; // Initial trend
    if self.metric_points.len() > 1 {
      trend = self.metric_points[1].get_value() - self.metric_points[0].get_value();
    }

    for i in 0..self.metric_points.len() {
      let value = self.metric_points[i].get_value();
      if i > 0 {
        // Update the forecasts
        let forecast = level + trend;
        self.metric_points[i - 1].set_value(forecast);

        // Update level and trend
        let new_level = alpha * value + (1.0 - alpha) * (level + trend);
        let new_trend = beta * (new_level - level) + (1.0 - beta) * trend;
        level = new_level;
        trend = new_trend;
      }
    }

    // Append one more forecast beyond the last point
    let last_value = self.metric_points.last().unwrap().get_value();
    self
      .metric_points
      .last_mut()
      .unwrap()
      .set_value(last_value + trend);
  }

  /// Extracts the hour from each metric point's timestamp and sets it as the value.
  pub fn hour(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = chrono::Utc
        .timestamp_opt(mp.get_time() as i64, 0)
        .earliest()
      {
        mp.set_value(datetime.hour() as f64);
      }
    }
  }

  /// Computes the instantaneous delta between the last two metric points.
  pub fn idelta(&mut self) {
    if self.metric_points.len() < 2 {
      return;
    }

    let last_value = self.metric_points.last().unwrap().get_value();
    let second_last_value = self.metric_points[self.metric_points.len() - 2].get_value();
    let delta = last_value - second_last_value;

    let last_time = self.metric_points.last().unwrap().get_time();
    self.metric_points = vec![MetricPoint::new(last_time, delta)];
  }

  /// Computes the total increase across all metric points, adjusting for counter resets.
  pub fn increase(&mut self) {
    if self.metric_points.len() < 2 {
      return;
    }

    let first_value = self.metric_points.first().unwrap().get_value();
    let last_value = self.metric_points.last().unwrap().get_value();

    let mut increase = last_value - first_value;
    if increase < 0.0 {
      increase += f64::MAX - first_value;
    }

    let last_time = self.metric_points.last().unwrap().get_time();
    self.metric_points = vec![MetricPoint::new(last_time, increase)];
  }

  /// Computes the instantaneous rate of increase per second between the last two metric points.
  pub fn irate(&mut self) {
    let points_len = self.metric_points.len();
    if points_len < 2 {
      return;
    }

    let penultimate_point = &self.metric_points[points_len - 2];
    let last_point = &self.metric_points[points_len - 1];

    let time_diff = (last_point.get_time() as f64) - (penultimate_point.get_time() as f64);
    let value_diff = last_point.get_value() - penultimate_point.get_value();

    if time_diff == 0.0 {
      return;
    }

    let rate = value_diff / time_diff;
    self.metric_points = vec![MetricPoint::new(last_point.get_time(), rate)];
  }

  /// Joins labels for each metric point using a specified separator.
  pub fn label_join(&mut self, dst_label: &str, separator: &str, src_labels: &[String]) {
    for _mp in &mut self.metric_points {
      let joined_value = src_labels
        .iter()
        .map(|label| self.labels.get(label).unwrap_or(&"".to_string()).clone())
        .collect::<Vec<_>>()
        .join(separator);
      self.labels.insert(dst_label.to_string(), joined_value);
    }
  }

  /// Replaces a label's value with a new value if it matches a regex pattern.
  pub fn label_replace(
    &mut self,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
  ) {
    let re = Regex::new(regex).unwrap();
    for _mp in &mut self.metric_points {
      if let Some(src_value) = self.labels.get(src_label) {
        if re.is_match(src_value) {
          let replaced_value = re.replace_all(src_value, replacement);
          self
            .labels
            .insert(dst_label.to_string(), replaced_value.to_string());
        }
      }
    }
  }

  /// Applies the natural logarithm to the value of each metric point, with special handling for non-positive values.
  pub fn ln(&mut self) {
    for mp in &mut self.metric_points {
      let value = mp.get_value();
      if value > 0.0 {
        mp.set_value(value.ln());
      } else if value == 0.0 {
        mp.set_value(f64::NEG_INFINITY);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  /// Applies the base-2 logarithm to the value of each metric point, with special handling for non-positive values.
  pub fn log2(&mut self) {
    for mp in &mut self.metric_points {
      let value = mp.get_value();
      if value > 0.0 {
        mp.set_value(value.log2());
      } else if value == 0.0 {
        mp.set_value(f64::NEG_INFINITY);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  /// Applies the base-10 logarithm to the value of each metric point, with special handling for non-positive values.
  pub fn log10(&mut self) {
    for mp in &mut self.metric_points {
      let value = mp.get_value();
      if value > 0.0 {
        mp.set_value(value.log10());
      } else if value == 0.0 {
        mp.set_value(f64::NEG_INFINITY);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  /// Sets the value of each metric point to the minute of the hour extracted from its timestamp, handling invalid timestamps by setting value to NaN.
  pub fn minute(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = chrono::Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.minute() as f64);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  /// Sets the value of each metric point to the month extracted from its timestamp, handling invalid timestamps by setting value to NaN.
  pub fn month(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = chrono::Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.month() as f64);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  /// Converts the value of each metric point to its negative equivalent.
  pub fn negative(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value() * -1.0);
    }
  }

  /// Forecasts future metric values based on a linear regression of existing points, clearing existing points and storing the forecasted value.
  pub fn predict_linear(&mut self, t: f64) {
    if self.metric_points.len() < 2 {
      return; // Not enough points to perform linear regression
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_x_squared = 0.0;
    let mut sum_xy = 0.0;
    let n = self.metric_points.len() as f64;
    let first_time = self.metric_points.first().unwrap().get_time() as f64;

    for point in &self.metric_points {
      let x = (point.get_time() as f64) - first_time; // Time since first sample
      let y = point.get_value();
      sum_x += x;
      sum_y += y;
      sum_x_squared += x * x;
      sum_xy += x * y;
    }

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x);
    let offset = (sum_y - slope * sum_x) / n;
    let prediction = slope * (t - first_time) + offset;
    self.metric_points.clear(); // Remove existing points
    self
      .metric_points
      .push(MetricPoint::new((t * 1000.0) as u64, prediction)); // Assuming t is in seconds
  }

  /// Calculates and stores the rate of change between the first and last metric points over their duration.
  pub fn rate(&mut self) {
    if self.metric_points.len() < 2 {
      return; // Not enough points to calculate rate
    }

    let first_time = self.metric_points.first().unwrap().get_time() as f64;
    let last_time = self.metric_points.last().unwrap().get_time() as f64;
    let duration_seconds = last_time - first_time;

    let first_value = self.metric_points.first().unwrap().get_value();
    let last_value = self.metric_points.last().unwrap().get_value();

    let rate = (last_value - first_value) / duration_seconds;
    self.metric_points.clear();
    self
      .metric_points
      .push(MetricPoint::new(last_time as u64, rate));
  }

  /// Counts the number of resets (when the current value is less than the previous value) in the time series.
  pub fn resets(&mut self) {
    if self.metric_points.len() < 2 {
      return; // Not enough points to calculate resets
    }

    let mut resets_count = 0;

    // Iterate over metric points pairs to detect resets
    for i in 0..self.metric_points.len() - 1 {
      let current_value = self.metric_points[i].get_value();
      let next_value = self.metric_points[i + 1].get_value();

      // Detect counter reset
      if next_value < current_value {
        resets_count += 1;
      }
    }

    // Update metric points with the resets count
    self.metric_points.clear();
    self
      .metric_points
      .push(MetricPoint::new(0, resets_count as f64));
  }

  /// Rounds each metric point's value to the nearest multiple of a given number.
  pub fn round(&mut self, to_nearest: f64) {
    for mp in &mut self.metric_points {
      let value = mp.get_value();
      let rounded_value = (value / to_nearest).round() * to_nearest;
      mp.set_value(rounded_value);
    }
  }

  /// Sets each metric point's value to its sign: 1.0 for positive, -1.0 for negative, 0.0 for zero.
  pub fn sgn(&mut self) {
    for mp in &mut self.metric_points {
      let value = mp.get_value();
      let sign = if value > 0.0 {
        1.0
      } else if value < 0.0 {
        -1.0
      } else {
        0.0
      };
      mp.set_value(sign);
    }
  }

  /// Sorts the metric points in ascending order based on their values.
  pub fn sort(&mut self) {
    self.metric_points.sort_by(|a, b| {
      let a_value = a.get_value();
      let b_value = b.get_value();
      a_value.partial_cmp(&b_value).unwrap_or(Ordering::Equal)
    });
  }

  /// Sorts the metric points in descending order based on their values.
  pub fn sort_desc(&mut self) {
    self.metric_points.sort_by(|a, b| {
      let a_value = a.get_value();
      let b_value = b.get_value();
      b_value.partial_cmp(&a_value).unwrap_or(Ordering::Equal)
    });
  }

  /// Applies the square root function to the value of each metric point.
  pub fn sqrt(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().sqrt());
    }
  }

  /// Sets each metric point's value to its timestamp.
  pub fn timestamp(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_time() as f64);
    }
  }

  /// Sets each metric point's value to the current year.
  pub fn year(&mut self) {
    let current_year = chrono::Utc::now().year();
    for mp in &mut self.metric_points {
      mp.set_value(current_year as f64);
    }
  }

  // *** Aggregations over time: https://prometheus.io/docs/prometheus/latest/querying/functions ****

  /// Calculates the average value over all metric points in the time series.
  pub fn avg_over_time(&mut self) {
    let sum: f64 = self.metric_points.iter().map(|mp| mp.get_value()).sum();
    let count = self.metric_points.len() as f64;
    let avg = if count > 0.0 { sum / count } else { f64::NAN };
    self.metric_points = vec![MetricPoint::new(chrono::Utc::now().timestamp() as u64, avg)];
  }

  /// Finds the minimum value over all metric points in the time series.
  pub fn min_over_time(&mut self) {
    let min = self
      .metric_points
      .iter()
      .map(|mp| mp.get_value())
      .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
      .unwrap_or(f64::NAN);
    self.metric_points = vec![MetricPoint::new(chrono::Utc::now().timestamp() as u64, min)];
  }

  /// Finds the maximum value over all metric points in the time series.
  pub fn max_over_time(&mut self) {
    let max = self
      .metric_points
      .iter()
      .map(|mp| mp.get_value())
      .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
      .unwrap_or(f64::NAN);
    self.metric_points = vec![MetricPoint::new(chrono::Utc::now().timestamp() as u64, max)];
  }

  /// Calculates the sum of all values over all metric points in the time series.
  pub fn sum_over_time(&mut self) {
    let sum: f64 = self.metric_points.iter().map(|mp| mp.get_value()).sum();
    self.metric_points = vec![MetricPoint::new(chrono::Utc::now().timestamp() as u64, sum)];
  }

  /// Counts the number of metric points in the time series.
  pub fn count_over_time(&mut self) {
    let count = self.metric_points.len() as f64;
    self.metric_points = vec![MetricPoint::new(
      chrono::Utc::now().timestamp() as u64,
      count,
    )];
  }

  /// Calculates the specified quantile value over all metric points in the time series.
  pub fn quantile_over_time(&mut self, quantile: f64) {
    let mut values: Vec<f64> = self.metric_points.iter().map(|mp| mp.get_value()).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let index = ((quantile * values.len() as f64).round() as usize).saturating_sub(1);
    let quantile_value = values.get(index).copied().unwrap_or(f64::NAN);
    self.metric_points = vec![MetricPoint::new(
      chrono::Utc::now().timestamp() as u64,
      quantile_value,
    )];
  }

  /// Calculates the standard deviation of metric point values over time.
  pub fn stddev_over_time(&mut self) {
    let mean: f64 = self
      .metric_points
      .iter()
      .map(|mp| mp.get_value())
      .sum::<f64>()
      / self.metric_points.len() as f64;
    let variance = self
      .metric_points
      .iter()
      .map(|mp| (mp.get_value() - mean).powi(2))
      .sum::<f64>()
      / self.metric_points.len() as f64;
    let stddev = variance.sqrt();
    self.metric_points = vec![MetricPoint::new(
      chrono::Utc::now().timestamp() as u64,
      stddev,
    )];
  }

  /// Calculates the variance of metric point values over time.
  pub fn stdvar_over_time(&mut self) {
    let mean: f64 = self
      .metric_points
      .iter()
      .map(|mp| mp.get_value())
      .sum::<f64>()
      / self.metric_points.len() as f64;
    let variance = self
      .metric_points
      .iter()
      .map(|mp| (mp.get_value() - mean).powi(2))
      .sum::<f64>()
      / self.metric_points.len() as f64;
    self.metric_points = vec![MetricPoint::new(
      chrono::Utc::now().timestamp() as u64,
      variance,
    )];
  }

  /// Calculates the median of a given data set.
  fn median(&self, data: &[f64]) -> f64 {
    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted_data.len();
    if n % 2 == 0 {
      (sorted_data[n / 2 - 1] + sorted_data[n / 2]) / 2.0
    } else {
      sorted_data[n / 2]
    }
  }

  /// Calculates the median absolute deviation (MAD) of metric point values over time.
  pub fn mad_over_time(&mut self) {
    let median = self.median(
      &self
        .metric_points
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
    );
    let mad = self.median(
      &self
        .metric_points
        .iter()
        .map(|mp| (mp.get_value() - median).abs())
        .collect::<Vec<_>>(),
    );
    self.metric_points = vec![MetricPoint::new(chrono::Utc::now().timestamp() as u64, mad)];
  }

  /// Retains only the last metric point in the time series.
  pub fn last_over_time(&mut self) {
    if let Some(last_point) = self.metric_points.last() {
      self.metric_points = vec![last_point.clone()];
    }
  }

  /// Sets a single metric point with the value 1.0 to indicate presence over time.
  pub fn present_over_time(&mut self) {
    self.metric_points = vec![MetricPoint::new(chrono::Utc::now().timestamp() as u64, 1.0)];
  }

  /// Converts each metric point's value from degrees to radians.
  pub fn rad(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().to_radians());
    }
  }

  /// Converts the first metric point's value to a scalar if it's the only point in the series.
  pub fn scalar(&mut self) -> Option<f64> {
    if self.metric_points.len() == 1 {
      Some(self.metric_points[0].get_value())
    } else {
      None
    }
  }

  /// Applies the arccosine function to each metric point's value.
  pub fn acos(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().acos());
    }
  }

  /// Applies the inverse hyperbolic cosine function to each metric point's value.
  pub fn acosh(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().acosh());
    }
  }

  /// Applies the arcsine function to each metric point's value.
  pub fn asin(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().asin());
    }
  }

  /// Applies the inverse hyperbolic sine function to each metric point's value.
  pub fn asinh(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().asinh());
    }
  }

  /// Applies the arctangent function to each metric point's value.
  pub fn atan(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().atan());
    }
  }

  /// Applies the inverse hyperbolic tangent function to each metric point's value.
  pub fn atanh(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().atanh());
    }
  }

  /// Applies the cosine function to each metric point's value.
  pub fn cos(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().cos());
    }
  }

  /// Applies the hyperbolic cosine function to each metric point's value.
  pub fn cosh(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().cosh());
    }
  }

  /// Applies the sine function to each metric point's value.
  pub fn sin(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().sin());
    }
  }

  /// Applies the hyperbolic sine function to each metric point's value.
  pub fn sinh(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().sinh());
    }
  }

  /// Applies the tangent function to each metric point's value.
  pub fn tan(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().tan());
    }
  }

  /// Applies the hyperbolic tangent function to each metric point's value.
  pub fn tanh(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().tanh());
    }
  }
}

impl PartialEq for PromQLTimeSeries {
  fn eq(&self, other: &Self) -> bool {
    self.labels == other.labels && self.metric_points == other.metric_points
  }
}

impl Eq for PromQLTimeSeries {}

impl PartialOrd for PromQLTimeSeries {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for PromQLTimeSeries {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .most_recent_timestamp()
      .cmp(&other.most_recent_timestamp())
  }
}

impl Default for PromQLTimeSeries {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {

  use super::*;

  use std::collections::HashMap;

  fn create_metric_points(times: &[u64], values: &[f64]) -> Vec<MetricPoint> {
    times
      .iter()
      .zip(values.iter())
      .map(|(time, value)| {
        let mut metric_point = MetricPoint::new(0, 0.0);
        metric_point.set_time(*time);
        metric_point.set_value(*value);
        metric_point
      })
      .collect()
  }

  // Helper function to create consistent labels for testing
  fn create_labels() -> HashMap<String, String> {
    HashMap::from([
      ("instance".to_string(), "localhost:9090".to_string()),
      ("job".to_string(), "prometheus".to_string()),
    ])
  }

  // Helper function to round numbers for comparison
  fn round_to_3(before: f64) -> f64 {
    f64::round(before * 1000.0) / 1000.0
  }

  #[test]
  fn test_promql_time_series_basic() {
    let labels = create_labels();
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);

    let mut ts = PromQLTimeSeries::new_with_params(labels.clone(), metric_points.clone());
    assert_eq!(ts.get_labels(), &labels);
    assert_eq!(ts.get_metric_points(), &metric_points);
  }

  #[test]
  fn test_exp() {
    let expected = [10.0, 20.0, 30.0, 40.0, 50.0];
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &expected);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.exp();

    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let value: f64 = expected[i];
      assert!((mp.get_value() - value.exp()).abs() < f64::EPSILON);
    }
  }

  #[test]
  fn test_floor() {
    let metric_points = create_metric_points(&[1, 2, 3], &[9.5, 16.5, 25.5]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.floor();

    let expected = [9.0, 16.0, 25.0];
    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let rounded_value = round_to_3(mp.get_value());
      assert_eq!(rounded_value, expected[i]);
    }
  }

  #[test]
  fn test_sqrt() {
    let metric_points = create_metric_points(&[1, 2, 3], &[9.5, 16.5, 25.5]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sqrt();

    // Since the original values are slightly above perfect squares, check against the sqrt of the floored values
    let expected = [3.082, 4.062, 5.050];
    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let rounded_value = round_to_3(mp.get_value());
      assert_eq!(rounded_value, expected[i]);
    }
  }

  #[test]
  fn test_sqrt_negative() {
    let metric_points = create_metric_points(&[1], &[-1.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sqrt();
    assert!(
      ts.get_metric_points()[0].get_value().is_nan(),
      "sqrt of negative number should be NaN"
    );
  }

  #[test]
  fn test_sqrt_zero() {
    let metric_points = create_metric_points(&[1], &[0.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sqrt();
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      0.0,
      "sqrt of 0 should be 0"
    );
  }

  #[test]
  fn test_sqrt_large_number() {
    let large_number = 1e10;
    let metric_points = create_metric_points(&[1], &[large_number]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sqrt();
    let expected_value = large_number.sqrt();
    assert_eq!(
      round_to_3(ts.get_metric_points()[0].get_value()),
      round_to_3(expected_value),
      "sqrt of a large number should be close to its mathematical sqrt"
    );
  }

  #[test]
  fn test_label_replace() {
    let labels = create_labels();
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels, metric_points);

    ts.label_replace("job", "replaced_job", "job", ".*");
    assert_eq!(ts.labels.get("job").unwrap(), &"replaced_job");
  }

  #[test]
  fn test_label_join() {
    let mut labels = create_labels();
    labels.insert("region".to_string(), "us-west".to_string());
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels, metric_points);

    ts.label_join("new_label", "-", &["job".to_owned(), "region".to_owned()]);
    assert_eq!(ts.labels.get("new_label").unwrap(), &"prometheus-us-west");
  }

  #[test]
  fn test_log2() {
    let metric_points = create_metric_points(&[1, 2], &[1.0, 2.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log2();

    let expected_values = [0.0, 1.0, 2.0];
    for (mp, &expected) in ts.get_metric_points().iter().zip(expected_values.iter()) {
      assert!((mp.get_value() - expected).abs() < f64::EPSILON);
    }
  }

  #[test]
  fn test_max_over_time() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.max_over_time();
    assert_eq!(ts.get_metric_points()[0].get_value(), 50.0);
  }

  #[test]
  fn test_min_over_time() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.min_over_time();
    assert_eq!(ts.get_metric_points()[0].get_value(), 10.0);
  }

  #[test]
  fn test_increase() {
    let metric_points = create_metric_points(&[1, 2, 3], &[100.0, 150.0, 200.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.increase();

    assert_eq!(ts.get_metric_points()[0].get_value(), 100.0); // 200.0 - 100.0 = 100.0
  }

  #[test]
  fn test_irate() {
    let metric_points = create_metric_points(&[1, 2, 4], &[100.0, 110.0, 130.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    let expected_rate = 10.0; // (130.0 - 110.0) / (4 - 2)
    ts.irate();
    assert_eq!(ts.get_metric_points()[0].get_value(), expected_rate);
  }

  #[test]
  fn test_last_over_time() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.last_over_time();

    // The last value should match the value of the last MetricPoint in the series
    let last_value = ts.get_metric_points().last().unwrap().get_value();
    assert_eq!(ts.get_metric_points()[0].get_value(), last_value);
  }

  #[test]
  fn test_ln() {
    let metric_points =
      create_metric_points(&[1, 2], &[std::f64::consts::E, std::f64::consts::E.powi(2)]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.ln();

    // Check if ln was applied correctly
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 2.0);
  }

  #[test]
  fn test_ln_negative() {
    let metric_points = create_metric_points(&[1], &[-1.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.ln();
    assert!(
      ts.get_metric_points()[0].get_value().is_nan(),
      "ln of negative number should be NaN"
    );
  }

  #[test]
  fn test_ln_zero() {
    let metric_points = create_metric_points(&[1], &[0.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.ln();
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      f64::NEG_INFINITY,
      "ln of 0 should be -Infinity"
    );
  }

  #[test]
  fn test_ln_large_number() {
    let large_number = 1e10;
    let metric_points = create_metric_points(&[1], &[large_number]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.ln();
    let expected_value = large_number.ln();
    assert_eq!(
      round_to_3(ts.get_metric_points()[0].get_value()),
      round_to_3(expected_value),
      "ln of a large number should be close to its mathematical ln"
    );
  }

  #[test]
  fn test_log10() {
    let metric_points = create_metric_points(&[1, 2], &[10.0, 100.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log10();

    // Validate that log10 was applied correctly
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 2.0);
  }

  #[test]
  fn test_log10_negative() {
    let metric_points = create_metric_points(&[1], &[-1.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log10();
    assert!(
      ts.get_metric_points()[0].get_value().is_nan(),
      "log10 of negative number should be NaN"
    );
  }

  #[test]
  fn test_log10_zero() {
    let metric_points = create_metric_points(&[1], &[0.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log10();
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      f64::NEG_INFINITY,
      "log10 of 0 should be -Infinity"
    );
  }

  #[test]
  fn test_log10_large_number() {
    let large_number = 1e10;
    let metric_points = create_metric_points(&[1], &[large_number]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log10();
    let expected_value = large_number.log10();
    assert_eq!(
      round_to_3(ts.get_metric_points()[0].get_value()),
      round_to_3(expected_value),
      "log10 of a large number should be close to its mathematical log10"
    );
  }

  #[test]
  fn test_holt_winters() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    let alpha = 0.8;
    let beta = 0.2;
    ts.holt_winters(alpha, beta);

    let expected = [20.0, 30.0, 40.0, 50.0, 60.0];
    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let rounded_value = round_to_3(mp.get_value());
      assert_eq!(rounded_value, expected[i]);
    }
  }

  #[test]
  #[allow(clippy::unnecessary_literal_unwrap)]
  fn test_rate() {
    let metric_points = create_metric_points(&[1, 2, 4], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // Rate should calculate the rate of change between the first and last points
    let expected_rate = Some((30.0 - 10.0) / (4 - 1) as f64);
    ts.rate();
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      expected_rate.unwrap()
    );
  }

  #[test]
  fn test_sum_over_time() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[10.0, 20.0, 30.0, 40.0, 50.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // sum_over_time should return the sum of all values over the entire time range
    let expected_sum = 150.0; // 10.0 + 20.0 + 30.0 + 40.0 + 50.0
    ts.sum_over_time();
    assert_eq!(ts.get_metric_points()[0].get_value(), expected_sum);
  }

  #[test]
  fn test_timestamp() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.timestamp();
    let extracted_timestamps = ts
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_time())
      .collect::<Vec<_>>();
    assert_eq!(extracted_timestamps, vec![1, 2, 3]);
  }

  #[test]
  fn test_tan() {
    let metric_points = create_metric_points(
      &[1, 2, 3],
      &[0.0, std::f64::consts::PI / 4.0, std::f64::consts::PI / 3.0],
    );
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.tan();

    let expected = [0.0, 1.0, round_to_3((3.0_f64).sqrt())];
    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let rounded_value = round_to_3(mp.get_value());
      assert_eq!(rounded_value, expected[i]);
    }
  }

  #[test]
  fn test_sin() {
    let metric_points = create_metric_points(
      &[1, 2, 3, 4, 5],
      &[
        0.0,
        std::f64::consts::FRAC_PI_2,
        std::f64::consts::PI,
        3.0 * std::f64::consts::FRAC_PI_2,
        2.0 * std::f64::consts::PI,
      ],
    );
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sin();

    let expected = [0.0, 1.0, 0.0, -1.0, 0.0];

    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let rounded_value = round_to_3(mp.get_value());
      assert!((rounded_value - expected[i]).abs() < f64::EPSILON);
    }
  }

  #[test]
  fn test_sinh() {
    let metric_points = create_metric_points(&[1, 2, 3], &[0.0, 1.0, 2.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.sinh();

    let expected = [0.0, 1.175, 3.627];

    for (i, mp) in ts.get_metric_points().iter().enumerate() {
      let rounded_value = round_to_3(mp.get_value());
      assert!((rounded_value - expected[i]).abs() < f64::EPSILON,);
    }
  }

  #[test]
  fn test_delta() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.delta();
    let delta_values = ts
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .collect::<Vec<_>>();
    assert_eq!(delta_values, vec![20.0]);
  }

  #[test]
  fn test_deriv() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.deriv();
    let deriv_values = ts
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .collect::<Vec<_>>();
    assert_eq!(deriv_values, vec![5.0]);
  }

  #[test]
  fn test_avg_over_time() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.avg_over_time();
    assert_eq!(ts.get_metric_points()[0].get_value(), 20.0);
  }
  #[test]
  fn test_count() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.count_over_time();
    let count = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(0.0);
    assert_eq!(count, 3.0);
  }

  #[test]
  fn test_day_of_year_leap_year() {
    let metric_points = create_metric_points(&[1582934400], &[10.0]); // 2020-02-29
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.day_of_year();
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      vec![60.0] // Expect day of year for leap year Feb 29
    );
  }

  #[test]
  fn test_increase_no_points() {
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), Vec::new());
    ts.increase();
    assert!(ts.get_metric_points().is_empty()); // Still expect no points after operation
  }

  #[test]
  fn test_max_min_equal_values() {
    let metric_points = create_metric_points(&[1, 2], &[10.0, 10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.max_over_time();
    let max_value = ts.get_metric_points().first().map(|mp| mp.get_value());
    assert_eq!(max_value, Some(10.0));

    ts.min_over_time();
    let min_value = ts.get_metric_points().first().map(|mp| mp.get_value());
    assert_eq!(min_value, Some(10.0));
  }

  #[test]
  fn test_label_replace_non_matching_regex() {
    let mut labels = HashMap::new();
    labels.insert("env".to_string(), "production".to_string());
    let metric_points = create_metric_points(&[1], &[10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels, metric_points.clone());
    ts.label_replace("new_env", "staging", "env", "^test$");
    assert_eq!(ts.labels.get("env").unwrap(), "production");
    assert!(!ts.labels.contains_key("new_env"));
  }

  #[test]
  fn test_day_of_year() {
    let metric_points =
      create_metric_points(&[1613924400, 1621098000, 1626284400], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.day_of_year();
    let day_of_year = ts
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value() as i32)
      .collect::<Vec<_>>();
    assert_eq!(day_of_year, vec![52, 135, 195]);
  }

  #[test]
  fn test_predict_linear() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points.clone());
    ts.predict_linear(10.0);
    let result = ts.get_metric_points().last().unwrap().get_value();
    assert_eq!(result, 100.0);
  }

  #[test]
  fn test_sgn() {
    let metric_points = create_metric_points(&[1, 2, 3], &[-10.0, 0.0, 10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sgn();
    let expected_signs = vec![-1.0, 0.0, 1.0];
    let actual_signs = ts
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .collect::<Vec<_>>();
    assert_eq!(actual_signs, expected_signs);
  }

  #[test]
  fn test_label_replace_matching_regex() {
    let labels = HashMap::from([("role".to_string(), "db-master".to_string())]);
    let metric_points = create_metric_points(&[1], &[10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels, metric_points);
    ts.label_replace("role", "db-slave", "role", "db-.*");
    assert_eq!(ts.labels.get("role").unwrap(), &"db-slave");
  }

  #[test]
  fn test_resets_no_resets() {
    let metric_points = create_metric_points(&[1, 2, 3], &[1.0, 2.0, 3.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.resets();
    let resets = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(0.0);
    assert_eq!(resets, 0.0);
  }

  #[test]
  fn test_resets_multiple_resets() {
    let metric_points = create_metric_points(&[1, 2, 3, 4], &[3.0, 1.0, 4.0, 2.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.resets();
    let resets = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(0.0);
    assert_eq!(resets, 2.0);
  }

  #[test]
  fn test_log2_powers_of_two() {
    let metric_points = create_metric_points(&[1, 2, 3], &[2.0, 4.0, 8.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log2();
    let expected_values = vec![1.0, 2.0, 3.0];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_values
    );
  }

  #[test]
  fn test_predict_linear_downward_trend() {
    let metric_points = create_metric_points(&[1, 2, 3, 4], &[10.0, 8.0, 6.0, 4.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    // Assuming the future time 't' is relative to the first time point and we want to predict for 2 units after the last time point.
    let t = 4.0 + 2.0 - 1.0; // The '-1.0' adjusts for 't' being relative to the first time point in predict_linear's implementation.
    ts.predict_linear(t);
    let predicted_value = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(f64::NAN);
    assert_eq!(predicted_value, 2.0);
  }

  #[test]
  fn test_sum_with_nan_values() {
    let metric_points = create_metric_points(&[1, 2, 3], &[f64::NAN, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.sum_over_time();
    let sum = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap();
    assert!(sum.is_nan());
  }

  #[test]
  fn test_quantile_over_time_uniform_distribution() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[1.0, 2.0, 3.0, 4.0, 5.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.quantile_over_time(0.5);
    let quantile_value = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(f64::NAN);
    assert_eq!(quantile_value, 3.0);
  }

  #[test]
  fn test_rate_irregular_intervals() {
    let metric_points = create_metric_points(&[1, 3, 6], &[10.0, 30.0, 60.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.rate();
    let rate_value = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(0.0);
    assert_eq!(rate_value, 10.0); // Verify the modified value matches expected rate
  }

  #[test]
  fn test_quantile_sorted_series() {
    let metric_points = create_metric_points(&[1, 2, 3, 4, 5], &[1.0, 2.0, 3.0, 4.0, 5.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // Test for 0th quantile (minimum value)
    ts.quantile_over_time(0.0);
    let quantile_value_0 = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(f64::NAN);
    assert_eq!(quantile_value_0, 1.0);

    // Reset ts for next test
    ts.set_metric_points(create_metric_points(
      &[1, 2, 3, 4, 5],
      &[1.0, 2.0, 3.0, 4.0, 5.0],
    ));

    // Test for 50th quantile (median value)
    ts.quantile_over_time(0.5);
    let quantile_value_50 = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(f64::NAN);
    assert_eq!(quantile_value_50, 3.0);

    // Reset ts for next test
    ts.set_metric_points(create_metric_points(
      &[1, 2, 3, 4, 5],
      &[1.0, 2.0, 3.0, 4.0, 5.0],
    ));

    // Test for 100th quantile (maximum value)
    ts.quantile_over_time(1.0);
    let quantile_value_100 = ts
      .get_metric_points()
      .first()
      .map(|mp| mp.get_value())
      .unwrap_or(f64::NAN);
    assert_eq!(quantile_value_100, 5.0);
  }

  #[test]
  fn test_function_with_nan_input() {
    let metric_points = create_metric_points(&[1, 2, 3], &[f64::NAN, 1.0, 2.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.exp(); // Function that behave unpredictably with NaN
    assert!(
      ts.get_metric_points()[0].get_value().is_nan(),
      "Function output should be NaN when input is NaN"
    );
  }

  #[test]
  fn test_aggregate_function_with_nan_values() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, f64::NAN, 20.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.avg_over_time();
    assert!(
      ts.get_metric_points()[0].get_value().is_nan(),
      "Average should return NaN if NaN encountered",
    );
  }

  #[test]
  fn test_handling_nan_in_comparisons() {
    let metric_points = create_metric_points(&[1, 2], &[f64::NAN, 1.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.max_over_time();
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      1.0,
      "Max should ignore NaN and return the max of other values"
    );
  }

  #[test]
  fn test_function_with_infinity() {
    let metric_points = create_metric_points(&[1, 2], &[f64::INFINITY, 1.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.log10(); // Applying a function that might be influenced by Infinity
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      f64::INFINITY,
      "Log10 of Infinity should be Infinity"
    );
  }

  #[test]
  fn test_aggregate_function_with_infinity() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, f64::INFINITY, 20.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.sum_over_time(); // Summing values including Infinity
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      f64::INFINITY,
      "Sum should be Infinity when any component is Infinity"
    );
  }

  #[test]
  fn test_handling_infinity_in_comparisons() {
    let metric_points = create_metric_points(&[1, 2, 3], &[f64::INFINITY, 1.0, 2.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.min_over_time(); // Comparing values including Infinity
    assert_eq!(
      ts.get_metric_points()[0].get_value(),
      1.0,
      "Min should ignore Infinity and return the min of other values"
    );
  }

  #[test]
  fn test_avg_over_time_empty_series() {
    let metric_points = Vec::new(); // Empty series
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.avg_over_time();
    assert!(
      ts.get_metric_points().first().unwrap().get_value().is_nan(),
      "Average of an empty series should be NaN"
    );
  }

  #[test]
  fn test_sum_over_time_empty_series() {
    let metric_points = Vec::new(); // Empty series
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.sum_over_time();
    // Sum of an empty series could be considered 0
    assert_eq!(
      ts.get_metric_points().first().unwrap().get_value(),
      0.0,
      "Sum of an empty series should be 0"
    );
  }

  #[test]
  fn test_min_over_time_empty_series() {
    let metric_points = Vec::new(); // Empty series
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.min_over_time();
    assert!(
      ts.get_metric_points().first().unwrap().get_value().is_nan(),
      "Min of an empty series should be NaN"
    );
  }

  #[test]
  fn test_max_over_time_empty_series() {
    let metric_points = Vec::new(); // Empty series
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.max_over_time();
    assert!(
      ts.get_metric_points().first().unwrap().get_value().is_nan(),
      "Max of an empty series should be NaN"
    );
  }

  #[test]
  fn test_count_over_time_empty_series() {
    let metric_points = Vec::new(); // Empty series
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.count_over_time();
    // Count of an empty series should logically be 0
    assert_eq!(
      ts.get_metric_points().first().unwrap().get_value(),
      0.0,
      "Count of an empty series should be 0"
    );
  }

  #[test]
  fn test_predict_linear_future() {
    // Time series data: linear increase by 10 every step
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // Predict the value at a future time, t=5 (two steps ahead of the last point, t=3)
    ts.predict_linear(5.0);
    // Expect the value at t=5 to continue the trend: 10 units increase per step
    let expected_prediction = 50.0; // 30 + 2*10
    let actual_prediction = ts.get_metric_points().last().unwrap().get_value();
    assert_eq!(
      actual_prediction, expected_prediction,
      "Future prediction should accurately extrapolate the linear trend"
    );
  }

  #[test]
  fn test_predict_linear_past() {
    // Time series data: linear increase by 10 every step
    let metric_points = create_metric_points(&[2, 3, 4], &[20.0, 30.0, 40.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // Predict the value at a past time, t=1 (one step before the first point, t=2)
    ts.predict_linear(1.0);
    // Expect the value at t=1 to follow the trend backwards: 10 units decrease per step
    let expected_prediction = 10.0; // 20 - 1*10
    let actual_prediction = ts.get_metric_points().last().unwrap().get_value();
    assert_eq!(
      actual_prediction, expected_prediction,
      "Past prediction should accurately extrapolate the linear trend backwards"
    );
  }

  #[test]
  fn test_predict_linear_non_linear_data() {
    // Time series data: exponential growth
    let metric_points = create_metric_points(&[1, 2, 3], &[2.0, 4.0, 8.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // Attempt to predict the future value at t=4
    ts.predict_linear(4.0);
    // The expected value if the trend were linear might not match due to the exponential growth
    // This test checks how the function handles non-linear trends
    let actual_prediction = ts.get_metric_points().last().unwrap().get_value();
    // Note: Asserting specific values might not be meaningful without a clear expectation for handling non-linear data
    // This test could instead document or verify the behavior or limitations of predict_linear with non-linear data
    assert!(actual_prediction != 12.0, "Prediction for non-linear data should consider the limitations of linear extrapolation models");
  }

  #[test]
  fn test_quantile_over_time_zero_quantile() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 5.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.quantile_over_time(0.0); // Quantile of 0 should return the minimum value
    let min_value = ts.get_metric_points()[0].get_value();
    assert_eq!(
      min_value, 5.0,
      "Quantile of 0 should return the minimum value in the series"
    );
  }

  #[test]
  fn test_quantile_over_time_one_quantile() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 5.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.quantile_over_time(1.0); // Quantile of 1 should return the maximum value
    let max_value = ts.get_metric_points()[0].get_value();
    assert_eq!(
      max_value, 20.0,
      "Quantile of 1 should return the maximum value in the series"
    );
  }

  #[test]
  fn test_quantile_over_time_negative_quantile() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.quantile_over_time(-0.5); // Testing with a negative quantile
                                 // Default to the minimum value
    let value = ts.get_metric_points()[0].get_value();
    assert_eq!(
      value, 10.0,
      "Negative quantile should result in the minimum"
    );
  }

  #[test]
  fn test_quantile_over_time_greater_than_one_quantile() {
    let metric_points = create_metric_points(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.quantile_over_time(1.5); // Testing with a quantile greater than 1
    let value = ts.get_metric_points()[0].get_value();
    assert!(
      value.is_nan(),
      "Quantile greater than 1 should result in NaN"
    );
  }

  #[test]
  fn test_irate_irregular_intervals() {
    // Creating a time series with irregular intervals: [1 to 3] then [3 to 6]
    let metric_points = create_metric_points(&[1, 3, 6], &[10.0, 30.0, 70.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    ts.irate();
    // Expectation: The rate calculation between 3 and 6 should consider the interval length
    // Rate calculation: (70 - 30) / (6 - 3) = 40 / 3 = 13.33...
    let expected_rate = 13.333333333333334;
    let actual_rate = ts.get_metric_points().last().unwrap().get_value();
    assert!(
      (actual_rate - expected_rate).abs() < 10.0 * f64::EPSILON,
      "irate should accurately calculate the instant rate over irregular intervals"
    );
  }

  #[test]
  fn test_predict_linear_irregular_intervals() {
    // Series with irregular intervals
    let metric_points = create_metric_points(&[1, 3, 8], &[20.0, 40.0, 80.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);

    // Predicting value at t=10, based on irregular historical intervals
    ts.predict_linear(10.0);
    // Expectation: The linear model should consider the actual intervals in its prediction
    let expected_prediction = 97.43589743589745;
    let actual_prediction = ts.get_metric_points().last().unwrap().get_value();
    assert_eq!(actual_prediction, expected_prediction, "predict_linear should accurately forecast future values considering irregular intervals in the data");
  }

  #[test]
  fn test_label_replace_empty_replacement() {
    let labels = HashMap::from([("team".to_string(), "devops".to_string())]);
    let metric_points = create_metric_points(&[1], &[10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels.clone(), metric_points);

    ts.label_replace("team", "", "team", ".*");
    // Expectation: The label value should be replaced with an empty string
    assert_eq!(
      ts.labels.get("team").unwrap(),
      &"",
      "Label value should be replaced with an empty string"
    );
  }

  #[test]
  fn test_label_join_non_existent_labels() {
    let labels = HashMap::from([("service".to_string(), "web".to_string())]);
    let metric_points = create_metric_points(&[1], &[10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels, metric_points);

    ts.label_join(
      "new_label",
      "",
      &["nonexistent1".to_owned(), "nonexistent2".to_owned()],
    );

    // Expectation: The new label should be created but with an empty value
    assert_eq!(
      ts.labels.get("new_label").unwrap(),
      &"",
      "Joined label value should be empty or default when source labels do not exist"
    );
  }

  #[test]
  fn test_label_join_special_characters_separator() {
    let labels = HashMap::from([
      ("region".to_string(), "us-east".to_string()),
      ("env".to_string(), "prod".to_string()),
    ]);
    let metric_points = create_metric_points(&[1], &[10.0]);
    let mut ts = PromQLTimeSeries::new_with_params(labels, metric_points);

    ts.label_join("new_label", "🚀", &["region".to_owned(), "env".to_owned()]);
    // Expectation: The new label should correctly use the special character as a separator
    assert_eq!(
      ts.labels.get("new_label").unwrap(),
      &"us-east🚀prod",
      "Label values should be joined using the special character separator"
    );
  }
}

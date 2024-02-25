// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crate::metric::metric_point::MetricPoint;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::f64::consts::PI;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromQLTimeSeries {
  labels: HashMap<String, String>,
  metric_points: Vec<MetricPoint>,
}

// For implementing PromQL functions on time series

impl PromQLTimeSeries {
  pub fn new() -> Self {
    PromQLTimeSeries {
      labels: HashMap::new(),
      metric_points: Vec::new(),
    }
  }

  pub fn new_with_params(labels: HashMap<String, String>, metric_points: Vec<MetricPoint>) -> Self {
    PromQLTimeSeries {
      labels,
      metric_points,
    }
  }

  pub fn get_labels(&self) -> &HashMap<String, String> {
    &self.labels
  }

  pub fn get_metric_points(&mut self) -> &mut Vec<MetricPoint> {
    &mut self.metric_points
  }

  /// Take for vector - getter to allow the vector to be
  /// transferred out of the object and comply with Rust's ownership rules
  pub fn take_metric_points(&mut self) -> Vec<MetricPoint> {
    std::mem::take(&mut self.metric_points)
  }

  pub fn set_labels(&mut self, labels: HashMap<String, String>) {
    self.labels = labels;
  }

  pub fn set_metric_points(&mut self, metric_points: Vec<MetricPoint>) {
    self.metric_points = metric_points;
  }

  pub fn is_empty(&self) -> bool {
    self.labels.is_empty() && self.metric_points.is_empty()
  }

  fn most_recent_timestamp(&self) -> Option<u64> {
    self.metric_points.iter().map(|mp| mp.get_time()).max()
  }

  // **** Functions: https://prometheus.io/docs/prometheus/latest/querying/functions

  // Applies the absolute value operation to every metric point's value
  pub fn abs(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().abs());
    }
  }

  // Applies the ceiling operation to every metric point's value
  pub fn ceil(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().ceil());
    }
  }

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

  // Applies clamping to every metric point's value
  pub fn clamp(&mut self, min: f64, max: f64) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().min(max).max(min));
    }
  }

  // Clamps the maximum value of metric points
  pub fn clamp_max(&mut self, max: f64) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().min(max));
    }
  }

  // Clamps the minimum value of metric points
  pub fn clamp_min(&mut self, min: f64) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().max(min));
    }
  }

  pub fn day_of_month(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.day() as f64);
      }
    }
  }

  pub fn day_of_week(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.weekday().num_days_from_sunday() as f64);
      }
    }
  }

  pub fn day_of_year(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.ordinal() as f64);
      }
    }
  }

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

  // Computes the difference between the first and last value of the metric points in the time series
  pub fn delta(&mut self) {
    if let Some(first_point) = self.metric_points.first() {
      if let Some(last_point) = self.metric_points.last() {
        let delta = last_point.get_value() - first_point.get_value();
        self.set_metric_points(vec![MetricPoint::new(last_point.get_time(), delta)]);
      }
    }
  }

  // Computes the derivative using simple linear regression
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

  // Converts radians to degrees for each metric point's value
  pub fn deg(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value() * (180.0 / PI));
    }
  }

  // Computes the exponential function for each metric point's value
  pub fn exp(&mut self) {
    for mp in &mut self.metric_points {
      let exponential = mp.get_value().exp();
      mp.set_value(exponential);
    }
  }

  // Applies the floor function to each metric point's value
  pub fn floor(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().floor());
    }
  }

  pub fn holt_winters(&mut self, alpha: f64, beta: f64) {
    if self.metric_points.is_empty() {
      return;
    }

    let mut level: f64 = self.metric_points[0].get_value(); // Initial level
    let mut trend: f64 = 0.0; // Initial trend
    if self.metric_points.len() > 1 {
      trend = self.metric_points[1].get_value() - self.metric_points[0].get_value();
      // Initial trend
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

  // Extracts the hour from each metric point's timestamp
  pub fn hour(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).earliest() {
        mp.set_value(datetime.hour() as f64);
      }
    }
  }

  // Assuming MetricPoint.get_time() is in seconds and MetricPoint.get_value() is the metric value.
  pub fn idelta(&mut self) {
    if self.metric_points.len() < 2 {
      // Not enough points to compute a delta
      return;
    }

    let last_value = self.metric_points.last().unwrap().get_value();
    let second_last_value = self.metric_points[self.metric_points.len() - 2].get_value();
    let delta = last_value - second_last_value;

    // Replace the metric points with a single MetricPoint containing the delta
    let last_time = self.metric_points.last().unwrap().get_time();
    self.metric_points = vec![MetricPoint::new(last_time, delta)];
  }

  pub fn increase(&mut self) {
    if self.metric_points.len() < 2 {
      // Not enough points to compute an increase
      return;
    }

    let first_value = self.metric_points.first().unwrap().get_value();
    let last_value = self.metric_points.last().unwrap().get_value();

    // Adjust for counter resets
    let mut increase = last_value - first_value;
    if increase < 0.0 {
      // Counter reset, adjust for overflow
      increase += f64::MAX - first_value;
    }

    // Replace the metric points with a single MetricPoint containing the increase
    let last_time = self.metric_points.last().unwrap().get_time();
    self.metric_points = vec![MetricPoint::new(last_time, increase)];
  }

  pub fn irate(&mut self) {
    let points_len = self.metric_points.len();
    // Ensure there are at least two points to compute an irate
    if points_len < 2 {
      return;
    }

    // Use the last two points
    let penultimate_point = &self.metric_points[points_len - 2];
    let last_point = &self.metric_points[points_len - 1];

    let time_diff = (last_point.get_time() as f64) - (penultimate_point.get_time() as f64);
    let value_diff = last_point.get_value() - penultimate_point.get_value();

    // Ensure time_diff is not zero to avoid division by zero
    if time_diff == 0.0 {
      return;
    }

    // Compute per-second rate of increase
    let rate = value_diff / time_diff;

    // Replace the metric points with a single MetricPoint containing the rate
    // Use the time of the last point for the new single metric point
    self.metric_points = vec![MetricPoint::new(last_point.get_time(), rate)];
  }

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

  pub fn minute(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.minute() as f64);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  pub fn month(&mut self) {
    for mp in &mut self.metric_points {
      if let Some(datetime) = Utc.timestamp_opt(mp.get_time() as i64, 0).latest() {
        mp.set_value(datetime.month() as f64);
      } else {
        mp.set_value(f64::NAN);
      }
    }
  }

  // Converts metric points in a time series to negative
  pub fn negative(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value() * -1.0);
    }
  }

  // Forecasts future values based on linear regression.
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

    // Replace the metric points with a single MetricPoint containing the rate
    self.metric_points.clear();
    self
      .metric_points
      .push(MetricPoint::new(last_time as u64, rate));
  }

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

  pub fn round(&mut self, to_nearest: f64) {
    for mp in &mut self.metric_points {
      let value = mp.get_value();
      let rounded_value = (value / to_nearest).round() * to_nearest;
      mp.set_value(rounded_value);
    }
  }

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

  pub fn sort(&mut self) {
    self.metric_points.sort_by(|a, b| {
      let a_value = a.get_value();
      let b_value = b.get_value();
      a_value.partial_cmp(&b_value).unwrap_or(Ordering::Equal)
    });
  }

  pub fn sort_desc(&mut self) {
    self.metric_points.sort_by(|a, b| {
      let a_value = a.get_value();
      let b_value = b.get_value();
      b_value.partial_cmp(&a_value).unwrap_or(Ordering::Equal)
    });
  }

  pub fn sqrt(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_value().sqrt());
    }
  }

  pub fn timestamp(&mut self) {
    for mp in &mut self.metric_points {
      mp.set_value(mp.get_time() as f64);
    }
  }

  pub fn year(&mut self) {
    let current_year = Utc::now().year();
    for mp in &mut self.metric_points {
      mp.set_value(current_year as f64);
    }
  }

  // *** Aggregations over time: https://prometheus.io/docs/prometheus/latest/querying/functions ****

  pub fn avg_over_time(&mut self) {
    let sum: f64 = self.metric_points.iter().map(|mp| mp.get_value()).sum();
    let count = self.metric_points.len() as f64;
    let avg = if count > 0.0 { sum / count } else { f64::NAN };
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, avg)];
  }

  pub fn min_over_time(&mut self) {
    let min = self
      .metric_points
      .iter()
      .map(|mp| mp.get_value())
      .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
      .unwrap_or(f64::NAN);
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, min)];
  }

  pub fn max_over_time(&mut self) {
    let max = self
      .metric_points
      .iter()
      .map(|mp| mp.get_value())
      .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
      .unwrap_or(f64::NAN);
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, max)];
  }

  pub fn sum_over_time(&mut self) {
    let sum: f64 = self.metric_points.iter().map(|mp| mp.get_value()).sum();
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, sum)];
  }

  pub fn count_over_time(&mut self) {
    let count = self.metric_points.len() as f64;
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, count)];
  }

  pub fn quantile_over_time(&mut self, quantile: f64) {
    let mut values: Vec<f64> = self.metric_points.iter().map(|mp| mp.get_value()).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let index = ((quantile * values.len() as f64).round() as usize).saturating_sub(1);
    let quantile_value = values.get(index).copied().unwrap_or(f64::NAN);
    self.metric_points = vec![MetricPoint::new(
      Utc::now().timestamp() as u64,
      quantile_value,
    )];
  }

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
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, stddev)];
  }

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
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, variance)];
  }

  // Add this as a helper function
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
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, mad)];
  }

  pub fn last_over_time(&mut self) {
    if let Some(last_point) = self.metric_points.last() {
      self.metric_points = vec![last_point.clone()];
    }
  }

  pub fn present_over_time(&mut self) {
    self.metric_points = vec![MetricPoint::new(Utc::now().timestamp() as u64, 1.0)];
  }

  // Converts each metric point's value from degrees to radians.
  pub fn rad(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().to_radians());
    }
  }

  // Converts the first metric point's value to a scalar, if it's the only point.
  pub fn scalar(&mut self) -> Option<f64> {
    if self.get_metric_points().len() == 1 {
      Some(self.get_metric_points()[0].get_value())
    } else {
      None
    }
  }

  // **** Trigonometric Functions: https://prometheus.io/docs/prometheus/latest/querying/functions/

  // Applies the arccosine function to each metric point's value.
  pub fn acos(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().acos());
    }
  }

  // Applies the inverse hyperbolic cosine function to each metric point's value.
  pub fn acosh(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().acosh());
    }
  }

  // Applies the arcsine function to each metric point's value.
  pub fn asin(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().asin());
    }
  }

  // Applies the inverse hyperbolic sine function to each metric point's value.
  pub fn asinh(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().asinh());
    }
  }

  // Applies the arctangent function to each metric point's value.
  pub fn atan(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().atan());
    }
  }

  // Applies the inverse hyperbolic tangent function to each metric point's value.
  pub fn atanh(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().atanh());
    }
  }

  // Applies the cosine function to each metric point's value.
  pub fn cos(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().cos());
    }
  }

  // Applies the hyperbolic cosine function to each metric point's value.
  pub fn cosh(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().cosh());
    }
  }

  // Applies the sine function to each metric point's value.
  pub fn sin(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().sin());
    }
  }

  // Applies the hyperbolic sine function to each metric point's value.
  pub fn sinh(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().sinh());
    }
  }

  // Applies the tangent function to each metric point's value.
  pub fn tan(&mut self) {
    for mp in self.get_metric_points() {
      mp.set_value(mp.get_value().tan());
    }
  }

  // Applies the hyperbolic tangent function to each metric point's value.
  pub fn tanh(&mut self) {
    for mp in self.get_metric_points() {
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
  fn test_log10() {
    let metric_points = create_metric_points(&[1, 2], &[10.0, 100.0]);
    let mut ts = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points);
    ts.log10();

    // Validate that log10 was applied correctly
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 2.0);
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
}
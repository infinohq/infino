// TODO: Add error checking, particularly when a vector is not the expected instant vector
// or range vector
use super::*;
use crate::index_manager::promql_time_series::PromQLTimeSeries;
use crate::metric::metric_point::MetricPoint;
use chrono::{Datelike, NaiveDateTime, Utc};
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// Represents both an Instant Vector and a Range Vector, with
// and Instant Vector being a single sample (i.e. metric_points.len() == 1)
// and Range Vector having a range of metrics points (i.e. metric_points.len() > 1)
#[derive(Debug, Clone, PartialEq)]
pub struct PromQLVector {
  vector: Vec<PromQLTimeSeries>,
}

impl PromQLVector {
  pub fn new(vector: Vec<PromQLTimeSeries>) -> Self {
    PromQLVector { vector }
  }

  // ******** Logical Operators: https://prometheus.io/docs/prometheus/latest/querying/operators/

  pub fn and(&mut self, other: &PromQLVector) {
    self.vector.retain(|self_ts| {
      other
        .vector
        .iter()
        .any(|other_ts| self_ts.get_labels() == other_ts.get_labels())
    });
  }

  pub fn or(&mut self, other: &PromQLVector) {
    for other_ts in &other.vector {
      if !self
        .vector
        .iter()
        .any(|self_ts| self_ts.get_labels() == other_ts.get_labels())
      {
        self.vector.push(other_ts.clone());
      }
    }
  }

  pub fn unless(&mut self, other: &PromQLVector) {
    self.vector.retain(|self_ts| {
      !other
        .vector
        .iter()
        .any(|other_ts| self_ts.get_labels() == other_ts.get_labels())
    });
  }

  // ******** Binary Operators: https://prometheus.io/docs/prometheus/latest/querying/operators/

  pub fn equal(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if a == b { 1.0 } else { 0.0 });
  }

  pub fn plus(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| a + b);
  }

  pub fn multiply(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| a * b);
  }

  // Logical "not equal" operation
  pub fn not_equal(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if a != b { 1.0 } else { 0.0 });
  }

  pub fn greater_than(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if a > b { 1.0 } else { 0.0 });
  }

  pub fn less_than(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if a < b { 1.0 } else { 0.0 });
  }

  pub fn greater_than_or_equal(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if a >= b { 1.0 } else { 0.0 });
  }

  pub fn less_than_or_equal(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if a <= b { 1.0 } else { 0.0 });
  }

  pub fn minus(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| a - b);
  }

  pub fn divide(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| if b != 0.0 { a / b } else { f64::NAN });
  }

  pub fn percent(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| (a / b) * 100.0);
  }

  // Helper method to apply a binary operation to matching metric points if the labels
  // match: https://prometheus.io/docs/prometheus/latest/querying/operators/
  fn apply_binary_operation<F: Fn(f64, f64) -> f64>(&mut self, other: &PromQLVector, op: F) {
    for self_ts in &mut self.vector {
      if let Some(other_ts) = other
        .vector
        .iter()
        .find(|x| x.get_labels() == self_ts.get_labels())
      {
        // Assume both vectors have the same number of metric points and are aligned
        for (i, self_mp) in self_ts.get_metric_points().iter_mut().enumerate() {
          if let Some(other_mp) = other_ts.get_metric_points().get(i) {
            // Apply the operation and update the metric point in place
            self_mp.get_value() = op(self_mp.get_value(), other_mp.get_value());
          }
        }
      }
    }
  }

  // ******** Aggregations: https://prometheus.io/docs/prometheus/latest/querying/operators/

  pub fn apply_aggregation_operator(&mut self, operator: AggregationOperator) {
    match operator {
      AggregationOperator::Sum => self.sum(),
      AggregationOperator::Min => self.min(),
      AggregationOperator::Max => self.max(),
      AggregationOperator::Avg => self.avg(),
      AggregationOperator::Group => self.group(),
      AggregationOperator::Stddev => self.stddev(),
      AggregationOperator::Stdvar => self.stdvar(),
      AggregationOperator::Count => self.count(),
      AggregationOperator::CountValues => self.count_values(),
      AggregationOperator::Bottomk(k) => self.bottomk(k),
      AggregationOperator::Topk(k) => self.topk(k),
      AggregationOperator::Quantile(phi) => self.quantile(phi),
    }
  }

  fn sum(&mut self) {
    for ts in &mut self.vector {
      let sum = ts
        .get_metric_points()
        .iter()
        .fold(0.0, |acc, mp| acc + mp.get_value());
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), sum)]);
    }
  }

  fn min(&mut self) {
    for ts in &mut self.vector {
      let min = ts
        .get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), min)]);
    }
  }

  fn max(&mut self) {
    for ts in &mut self.vector {
      let max = ts
        .get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), max)]);
    }
  }

  fn avg(&mut self) {
    for ts in &mut self.vector {
      let sum: f64 = ts.get_metric_points().iter().map(|mp| mp.get_value()).sum();
      let avg = sum / ts.get_metric_points().len() as f64;
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), avg)]);
    }
  }

  fn group(&mut self) {
    for ts in &mut self.vector {
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), 1.0)]);
    }
  }

  fn stddev(&mut self) {
    for ts in &mut self.vector {
      let mean: f64 = ts
        .get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .sum::<f64>()
        / ts.get_metric_points().len() as f64;
      let variance = ts
        .get_metric_points()
        .iter()
        .map(|mp| (mp.get_value() - mean).powi(2))
        .sum::<f64>()
        / ts.get_metric_points().len() as f64;
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), variance.sqrt())]);
    }
  }

  fn stdvar(&mut self) {
    for ts in &mut self.vector {
      let mean: f64 = ts
        .get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .sum::<f64>()
        / ts.get_metric_points().len() as f64;
      let variance = ts
        .get_metric_points()
        .iter()
        .map(|mp| (mp.get_value() - mean).powi(2))
        .sum::<f64>()
        / ts.get_metric_points().len() as f64;
      ts.set_metric_points(vec![MetricPoint::new(ts.get_timestamp(), variance)]);
    }
  }

  fn count(&mut self) {
    for ts in &mut self.vector {
      ts.set_metric_points(vec![MetricPoint::new(
        ts.get_timestamp(),
        ts.get_metric_points().len() as f64,
      )]);
    }
  }

  fn count_values(&mut self) {
    let mut value_counts: HashMap<f64, f64> = HashMap::new();
    for ts in &self.vector {
      for mp in ts.get_metric_points() {
        *value_counts.entry(mp.get_value()).or_insert(0.0) += 1.0;
      }
    }

    self.vector.clear();
    for (value, count) in value_counts {
      self.vector.push(PromQLTimeSeries::new(
        vec![MetricPoint::new(Utc::now().naive_utc(), count)],
        value,
      ));
    }
  }

  fn bottomk(&mut self, k: usize) {
    // Utilizes partial_sort for efficiency
    for ts in &mut self.vector {
      let mut points = ts.get_metric_points();
      points.select_nth_unstable_by(k, |a, b| a.get_value().partial_cmp(&b.get_value()).unwrap());
      let bottom_k = points.into_iter().take(k).collect::<Vec<_>>();
      ts.set_metric_points(bottom_k);
    }
  }

  fn topk(&mut self, k: usize) {
    for ts in &mut self.vector {
      let mut points = ts.get_metric_points();
      points.select_nth_unstable_by(k, |a, b| b.get_value().partial_cmp(&a.get_value()).unwrap());
      let top_k = points.into_iter().rev().take(k).collect::<Vec<_>>();
      ts.set_metric_points(top_k);
    }
  }

  fn quantile(&mut self, phi: f64) {
    for ts in &mut self.vector {
      let points = ts.get_metric_points();
      if points.is_empty() {
        continue;
      }
      let idx = ((phi * points.len() as f64).ceil() as usize).min(points.len()) - 1;
      let mut sorted_points = points.clone();
      sorted_points.select_nth_unstable_by(idx, |a, b| {
        a.get_value().partial_cmp(&b.get_value()).unwrap()
      });
      ts.set_metric_points(vec![sorted_points[idx].clone()]);
    }
  }

  // ******** Trigonometric Functions: https://prometheus.io/docs/prometheus/latest/querying/functions/

  // Calculates the arccosine of all elements in v (special cases).
  pub fn acos(&mut self) {
    for ts in &mut self.vector {
      ts.acos();
    }
  }

  // Calculates the inverse hyperbolic cosine of all elements in v (special cases).
  pub fn acosh(&mut self) {
    for ts in &mut self.vector {
      ts.acosh();
    }
  }

  // Calculates the arcsine of all elements in v (special cases).
  pub fn asin(&mut self) {
    for ts in &mut self.vector {
      ts.asin();
    }
  }

  // Calculates the inverse hyperbolic sine of all elements in v (special cases).
  pub fn asinh(&mut self) {
    for ts in &mut self.vector {
      ts.asinh();
    }
  }

  // Calculates the arctangent of all elements in v (special cases).
  pub fn atan(&mut self) {
    for ts in &mut self.vector {
      ts.atan();
    }
  }

  // Calculates the inverse hyperbolic tangent of all elements in v (special cases).
  pub fn atanh(&mut self) {
    for ts in &mut self.vector {
      ts.atanh();
    }
  }

  // Calculates the cosine of all elements in v (special cases).
  pub fn cos(&mut self) {
    for ts in &mut self.vector {
      ts.cos();
    }
  }

  // Calculates the hyperbolic cosine of all elements in v (special cases).
  pub fn cosh(&mut self) {
    for ts in &mut self.vector {
      ts.cosh();
    }
  }

  // Calculates the sine of all elements in v (special cases).
  pub fn sin(&mut self) {
    for ts in &mut self.vector {
      ts.sin();
    }
  }

  // Calculates the hyperbolic sine of all elements in v (special cases).
  pub fn sinh(&mut self) {
    for ts in &mut self.vector {
      ts.sinh();
    }
  }

  // Calculates the tangent of all elements in v (special cases).
  pub fn tan(&mut self) {
    for ts in &mut self.vector {
      ts.tan();
    }
  }

  // Calculates the hyperbolic tangent of all elements in v (special cases).
  pub fn tanh(&mut self) {
    for ts in &mut self.vector {
      ts.tanh();
    }
  }

  // ******** Aggregation Functions Over Time: https://prometheus.io/docs/prometheus/latest/querying/functions/

  // The average value of all points in the specified interval.
  pub fn avg_over_time(&self) -> Vec<f64> {
    self.vector.iter().map(|ts| ts.avg_over_time()).collect()
  }

  // The count of all values in the specified interval.
  pub fn count_over_time(&self) -> Vec<usize> {
    self.vector.iter().map(|ts| ts.count_over_time()).collect()
  }

  // The maximum value of all points in the specified interval.
  pub fn max_over_time(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.max_over_time()).collect()
  }

  // The minimum value of all points in the specified interval.
  pub fn min_over_time(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.min_over_time()).collect()
  }

  // The φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
  pub fn quantile_over_time(&self, q: f64) -> Vec<Option<f64>> {
    self
      .vector
      .iter()
      .map(|ts| ts.quantile_over_time(q))
      .collect()
  }

  // The population standard deviation of the values in the specified interval.
  pub fn stddev_over_time(&self) -> Vec<f64> {
    self.vector.iter().map(|ts| ts.stddev_over_time()).collect()
  }

  // The population standard variance of the values in the specified interval.
  pub fn stdvar_over_time(&self) -> Vec<f64> {
    self.vector.iter().map(|ts| ts.stdvar_over_time()).collect()
  }

  // The sum of all values in the specified interval.
  pub fn sum_over_time(&self) -> Vec<f64> {
    self.vector.iter().map(|ts| ts.sum_over_time()).collect()
  }

  // The median absolute deviation of all points in the specified interval.
  pub fn mad_over_time(&self) -> Vec<f64> {
    // Assuming the implementation exists in PromQLTimeSeries, pseudo-code here
    self.vector.iter().map(|ts| ts.mad_over_time()).collect()
  }

  // The most recent point value in the specified interval.
  pub fn last_over_time(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.last_over_time()).collect()
  }

  // The value 1 for any series in the specified interval.
  pub fn present_over_time(&self) -> Vec<u32> {
    self.vector.iter().map(|_ts| 1).collect()
  }

  // ******** Other Functions: https://prometheus.io/docs/prometheus/latest/querying/functions/

  // Applies the absolute value operation to every metric point in every time series
  pub fn abs(&mut self) {
    for ts in &mut self.vector {
      ts.abs();
    }
  }

  // Checks if any of the time series are absent (i.e., have no metric points)
  pub fn absent(&self) -> Vec<bool> {
    self
      .vector
      .iter()
      .map(|ts| ts.get_metric_points().is_empty())
      .collect()
  }

  // Applies the ceiling operation to every metric point in every time series
  pub fn ceil(&mut self) {
    for ts in &mut self.vector {
      ts.ceil();
    }
  }

  // Applies clamping to every metric point in every time series
  pub fn clamp(&mut self, min: f64, max: f64) {
    for ts in &mut self.vector {
      ts.clamp(min, max);
    }
  }

  // Clamps the maximum value of metric points for each time series
  pub fn clamp_max(&mut self, max: f64) {
    for ts in &mut self.vector {
      ts.clamp_max(max);
    }
  }

  // Clamps the minimum value of metric points for each time series
  pub fn clamp_min(&mut self, min: f64) {
    for ts in &mut self.vector {
      ts.clamp_min(min);
    }
  }

  // Calculates changes in metric points for each time series
  pub fn changes(&self) -> Vec<usize> {
    self.vector.iter().map(|ts| ts.changes()).collect()
  }

  // Computes the difference between consecutive metric points for each time series
  pub fn delta(&self) -> Vec<Vec<f64>> {
    self.vector.iter().map(|ts| ts.delta()).collect()
  }

  // Computes the derivative of each time series
  pub fn deriv(&self) -> Vec<Vec<f64>> {
    self.vector.iter().map(|ts| ts.deriv()).collect()
  }

  // Computes the total increase over time for each time series
  pub fn increase(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.increase()).collect()
  }

  // Computes the instantaneous delta for each time series
  pub fn idelta(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.idelta()).collect()
  }

  // Computes the instantaneous rate of change for each time series
  pub fn irate(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.irate()).collect()
  }

  // Computes the per-second rate of increase for each time series
  pub fn rate(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.rate()).collect()
  }

  // Rounds each metric point's value in every time series to a specified precision
  pub fn round(&mut self, precision: Option<u32>) {
    for ts in &mut self.vector {
      ts.round(precision);
    }
  }

  // Applies the exponential function to each metric point's value in every time series
  pub fn exp(&mut self) {
    for ts in &mut self.vector {
      ts.exp();
    }
  }

  // Applies the floor function to each metric point's value in every time series
  pub fn floor(&mut self) {
    for ts in &mut self.vector {
      ts.floor();
    }
  }

  // Converts the first metric point's value to a scalar if it's the only point in every time series
  pub fn scalar(&self) -> Vec<Option<f64>> {
    self.vector.iter().map(|ts| ts.scalar()).collect()
  }

  // Determines the sign of each metric point's value in every time series
  pub fn sgn(&mut self) {
    for ts in &mut self.vector {
      ts.sgn();
    }
  }

  // Applies the square root operation to each metric point's value in every time series
  pub fn sqrt(&mut self) {
    for ts in &mut self.vector {
      ts.sqrt();
    }
  }

  // Applies the natural logarithm to each metric point's value in every time series
  pub fn ln(&mut self) {
    for ts in &mut self.vector {
      ts.ln();
    }
  }

  // Applies the logarithm base 10 to each metric point's value in every time series
  pub fn log10(&mut self) {
    for ts in &mut self.vector {
      ts.log10();
    }
  }

  // Applies the logarithm base 2 to each metric point's value in every time series
  pub fn log2(&mut self) {
    for ts in &mut self.vector {
      ts.log2();
    }
  }

  // Forecasts future values based on linear regression for each time series
  pub fn predict_linear(&self, seconds_into_future: f64) -> Vec<Option<f64>> {
    self
      .vector
      .iter()
      .map(|ts| ts.predict_linear(seconds_into_future))
      .collect()
  }

  // Converts each metric point's value from degrees to radians in every time series
  pub fn rad(&mut self) {
    for ts in &mut self.vector {
      ts.rad();
    }
  }

  // Applies the Holt-Winters forecasting method to each time series
  pub fn holt_winters(&mut self, alpha: f64, beta: f64) -> Vec<Vec<f64>> {
    self
      .vector
      .iter()
      .map(|ts| ts.holt_winters(alpha, beta))
      .collect()
  }

  // Returns the timestamp of the last metric point in every time series
  pub fn time(&self) -> Vec<Option<u64>> {
    self.vector.iter().map(|ts| ts.time()).collect()
  }

  // Extracts and returns all timestamps from the metric points in every time series
  pub fn timestamp(&self) -> Vec<Vec<u64>> {
    self.vector.iter().map(|ts| ts.timestamp()).collect()
  }

  // Sets all metric points to a scalar value in every time series
  pub fn vector(&mut self, scalar: f64) {
    for ts in &mut self.vector {
      ts.vector(scalar);
    }
  }

  // Extracts the hour from each metric point's timestamp in every time series
  pub fn hour(&self) -> Vec<Vec<u32>> {
    self.vector.iter().map(|ts| ts.hour()).collect()
  }

  // Calculates the day of the month for each metric point's timestamp in every time series
  pub fn day_of_month(&self) -> Vec<Vec<u32>> {
    self.vector.iter().map(|ts| ts.day_of_month()).collect()
  }

  // Calculates the day of the week for each metric point's timestamp in every time series
  pub fn day_of_week(&self) -> Vec<Vec<chrono::Weekday>> {
    self.vector.iter().map(|ts| ts.day_of_week()).collect()
  }

  // Computes the number of days in the month for each metric point's timestamp in every time series
  pub fn days_in_month(&self) -> Vec<Vec<u32>> {
    self.vector.iter().map(|ts| ts.days_in_month()).collect()
  }

  // Computes the day of the year for each metric point's timestamp in every time series
  pub fn day_of_year(&self) -> Vec<Vec<u32>> {
    self.vector.iter().map(|ts| ts.day_of_year()).collect()
  }

  // Computes the histogram quantile across metric points for each time series
  pub fn histogram_quantile(&self, q: f64) -> Vec<Option<f64>> {
    self
      .vector
      .iter()
      .map(|ts| ts.histogram_quantile(q))
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use chrono::NaiveDateTime;

  fn create_time_series(values: Vec<f64>, timestamp: i64) -> PromQLTimeSeries {
    let metric_points = values
      .iter()
      .map(|&value| MetricPoint::new(NaiveDateTime::from_timestamp(timestamp, 0), value))
      .collect();
    PromQLTimeSeries::new(metric_points, 0.0)
  }

  #[test]
  fn test_abs() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-1.0, 2.0, -3.0], 0),
      create_time_series(vec![4.0, -5.0], 0),
    ]);

    vector.abs();

    let expected = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_round() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.234, 2.345], 0),
      create_time_series(vec![3.456, 4.567], 0),
    ]);

    vector.round(Some(2));

    let expected = vec![vec![1.23, 2.35], vec![3.46, 4.57]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  use chrono::NaiveDateTime;

  fn create_time_series(values: Vec<f64>, timestamp: i64) -> PromQLTimeSeries {
    let metric_points = values
      .iter()
      .map(|&value| MetricPoint::new(NaiveDateTime::from_timestamp(timestamp, 0), value))
      .collect();
    PromQLTimeSeries::new(metric_points, 0.0)
  }

  #[test]
  fn test_acos() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![0.0, 0.5, 1.0], 0),
      create_time_series(vec![0.5, 1.0], 0),
    ]);

    vector.acos();

    let expected = vec![
      vec![std::f64::consts::PI / 2.0, std::f64::consts::PI / 3.0, 0.0],
      vec![std::f64::consts::PI / 3.0, 0.0],
    ];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_asin() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![0.0, 0.5, 1.0], 0),
      create_time_series(vec![0.5, 1.0], 0),
    ]);

    vector.asin();

    let expected = vec![
      vec![0.0, std::f64::consts::PI / 6.0, std::f64::consts::PI / 2.0],
      vec![std::f64::consts::PI / 6.0, std::f64::consts::PI / 2.0],
    ];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_min_over_time() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0], 0),
      create_time_series(vec![4.0, 5.0], 0),
    ]);

    let result = vector.min_over_time();

    let expected = vec![Some(1.0), Some(4.0)];

    for (i, min) in result.iter().enumerate() {
      assert_eq!(*min, expected[i]);
    }
  }

  #[test]
  fn test_max_over_time() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0], 0),
      create_time_series(vec![4.0, 5.0], 0),
    ]);

    let result = vector.max_over_time();

    let expected = vec![Some(3.0), Some(5.0)];

    for (i, max) in result.iter().enumerate() {
      assert_eq!(*max, expected[i]);
    }
  }

  #[test]
  fn test_abs() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-1.0, 2.0, -3.0], 0),
      create_time_series(vec![4.0, -5.0], 0),
    ]);

    vector.abs();

    let expected = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_ceil() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.ceil();

    let expected = vec![vec![2.0, 3.0, 4.0], vec![5.0, -5.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.clamp(2.0, 4.0);

    let expected = vec![vec![2.0, 2.7, 3.5], vec![4.0, -5.2]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp_max() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.clamp_max(3.0);

    let expected = vec![vec![1.2, 2.7, 3.0], vec![3.0, -5.2]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp_min() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.clamp_min(2.0);

    let expected = vec![vec![2.0, 2.7, 3.5], vec![4.8, 2.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_changes() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 1.0, 2.0, 3.0, 3.0], 0),
      create_time_series(vec![1.0, 1.0, 1.0, 2.0], 0),
    ]);

    let result = vector.changes();

    let expected = vec![2, 1];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_delta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    let result = vector.delta();

    let expected = vec![
      vec![None, Some(2.0), Some(3.0), Some(4.0)],
      vec![None, Some(3.0), Some(4.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_deriv() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 4.0, 9.0, 16.0], 0),
      create_time_series(vec![5.0, 12.0, 21.0], 0),
    ]);

    let result = vector.deriv();

    let expected = vec![
      vec![Some(3.0), Some(5.0), Some(7.0)],
      vec![Some(7.0), Some(9.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_increase() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    let result = vector.increase();

    let expected = vec![Some(9.0), Some(7.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_idelta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    let result = vector.idelta();

    let expected = vec![Some(2.0), Some(3.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_irate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    let result = vector.irate();

    let expected = vec![Some(2.0), Some(3.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_rate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    let result = vector.rate();

    let expected = vec![Some(2.0), Some(3.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_round() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.234, 3.678, 6.912], 0),
      create_time_series(vec![5.678, 8.012], 0),
    ]);

    vector.round(Some(2));

    let expected = vec![vec![1.23, 3.68, 6.91], vec![5.68, 8.01]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_abs() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-1.0, 3.0, -6.0, 10.0], 0),
      create_time_series(vec![-5.0, -8.0, 12.0], 0),
    ]);

    vector.abs();

    let expected = vec![vec![1.0, 3.0, 6.0, 10.0], vec![5.0, 8.0, 12.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_absent() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    let result = vector.absent();

    let expected = vec![false, true, false];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_ceil() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, 3.6, -6.2, 10.8], 0),
      create_time_series(vec![-5.9, -8.1, 12.5], 0),
    ]);

    vector.ceil();

    let expected = vec![vec![2.0, 4.0, -6.0, 11.0], vec![-5.0, -8.0, 13.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, 3.6, -6.2, 10.8], 0),
      create_time_series(vec![-5.9, -8.1, 12.5], 0),
    ]);

    vector.clamp(-5.0, 10.0);

    let expected = vec![vec![1.1, 3.6, -5.0, 10.0], vec![-5.0, -5.0, 10.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp_max() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, 3.6, -6.2, 10.8], 0),
      create_time_series(vec![-5.9, -8.1, 12.5], 0),
    ]);

    vector.clamp_max(5.0);

    let expected = vec![vec![1.1, 3.6, -6.2, 5.0], vec![-5.9, -8.1, 5.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp_min() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, 3.6, -6.2, 10.8], 0),
      create_time_series(vec![-5.9, -8.1, 12.5], 0),
    ]);

    vector.clamp_min(-5.0);

    let expected = vec![vec![1.1, 3.6, -5.0, 10.8], vec![-5.0, -5.0, 12.5]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_changes() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, 3.6, 3.6, 3.6, 10.8], 0),
      create_time_series(vec![-5.9, -5.9, -5.9, -5.9, -5.9], 0),
    ]);

    let result = vector.changes();

    let expected = vec![2, 0];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_delta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.delta();

    let expected = vec![
      vec![None, Some(-2.0), Some(4.0), Some(3.0), Some(3.0)],
      vec![None, Some(1.0), Some(0.0), Some(-1.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_deriv() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.deriv();

    let expected = vec![
      vec![
        None,
        Some(-1.0),
        Some(0.6666666666666666),
        Some(0.75),
        Some(0.6),
      ],
      vec![None, Some(0.2), Some(0.0), Some(-0.2)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_increase() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.increase();

    let expected = vec![Some(8.0), Some(1.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_idelta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.idelta();

    let expected = vec![None, Some(1.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_irate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.irate();

    let expected = vec![None, Some(0.2)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_rate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.rate();

    let expected = vec![Some(0.8), Some(0.2)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_round() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![10.124, 8.5, 12.78, 15.0, 18.123], 0),
      create_time_series(vec![5.654, 6.0, 6.999, 5.001], 0),
    ]);

    vector.round(Some(1));

    let expected = vec![vec![10.1, 8.5, 12.8, 15.0, 18.1], vec![5.7, 6.0, 7.0, 5.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_abs() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-10.0, 8.0, -12.0, 15.0, -18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 5.0], 0),
    ]);

    vector.abs();

    let expected = vec![vec![10.0, 8.0, 12.0, 15.0, 18.0], vec![5.0, 6.0, 6.0, 5.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_sgn() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-10.0, 8.0, 0.0, 15.0, -18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 5.0], 0),
    ]);

    vector.sgn();

    let expected = vec![vec![-1.0, 1.0, 0.0, 1.0, -1.0], vec![1.0, -1.0, -1.0, 1.0]];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-10.0, 8.0, 20.0, 15.0, -18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 25.0], 0),
    ]);

    vector.clamp(-5.0, 15.0);

    let expected = vec![
      vec![-5.0, 8.0, 15.0, 15.0, -5.0],
      vec![5.0, -5.0, -5.0, 15.0],
    ];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp_max() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-10.0, 8.0, 20.0, 15.0, -18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 25.0], 0),
    ]);

    vector.clamp_max(15.0);

    let expected = vec![
      vec![-10.0, 8.0, 15.0, 15.0, -18.0],
      vec![5.0, -6.0, -6.0, 15.0],
    ];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_clamp_min() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![-10.0, 8.0, 20.0, 15.0, -18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 25.0], 0),
    ]);

    vector.clamp_min(-5.0);

    let expected = vec![
      vec![-5.0, 8.0, 20.0, 15.0, -5.0],
      vec![5.0, -5.0, -5.0, 25.0],
    ];

    for (i, ts) in vector.vector.iter().enumerate() {
      for (j, mp) in ts.get_metric_points().iter().enumerate() {
        assert_eq!(mp.get_value(), expected[i][j]);
      }
    }
  }

  #[test]
  fn test_changes() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 8.0, 15.0, 15.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.changes();

    let expected = vec![2, 1];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_delta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.delta();

    let expected = vec![
      vec![Some(-2.0), Some(4.0), Some(3.0), Some(3.0), Some(3.0)],
      vec![Some(1.0), Some(0.0), Some(-1.0), Some(-1.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_deriv() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.deriv();

    let expected = vec![
      vec![Some(-2.0), Some(4.0), Some(3.0), Some(3.0), Some(3.0)],
      vec![Some(1.0), Some(0.0), Some(-1.0), Some(-1.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_increase() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.increase();

    let expected = vec![Some(8.0), Some(1.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_idelta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.idelta();

    let expected = vec![Some(-2.0), Some(1.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_irate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.irate();

    let expected = vec![Some(-2.0), Some(1.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_present_over_time() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.present_over_time();

    let expected = vec![1, 1];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_abs() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, -8.0, 12.0, -15.0, 18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 5.0], 0),
    ]);

    vector.abs();

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_clamp() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    vector.clamp(8.0, 15.0);

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 15.0], 0),
      create_time_series(vec![8.0, 8.0, 8.0, 8.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_clamp_max() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    vector.clamp_max(10.0);

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![10.0, 8.0, 10.0, 10.0, 10.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_clamp_min() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    vector.clamp_min(8.0);

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![8.0, 8.0, 8.0, 8.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_changes() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 5.0, 6.0, 5.0], 0),
    ]);

    let result = vector.changes();

    let expected = vec![4, 2];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_delta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.delta();

    let expected = vec![
      vec![Some(1.0), Some(1.0), Some(2.0), Some(3.0)],
      vec![Some(2.0), Some(2.0), Some(3.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_deriv() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.deriv();

    let expected = vec![vec![1.0, 1.0, 2.0, 3.0], vec![2.0, 2.0, 3.0]];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_increase() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.increase();

    let expected = vec![Some(7.0), Some(7.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_idelta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.idelta();

    let expected = vec![Some(1.0), Some(2.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_irate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.irate();

    let expected = vec![Some(1.0), Some(2.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_rate() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.rate();

    let expected = vec![Some(1.0), Some(2.0)];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_round() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.123, 2.345, 3.789], 0),
      create_time_series(vec![5.678, 7.123], 0),
    ]);

    vector.round(Some(2));

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![1.12, 2.35, 3.79], 0),
      create_time_series(vec![5.68, 7.12], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_abs() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, -2.0, 3.0, -5.0, 8.0], 0),
      create_time_series(vec![-5.0, 7.0, -9.0, 12.0], 0),
    ]);

    vector.abs();

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_ceil() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, -2.5, 3.8, -5.9, 8.2], 0),
      create_time_series(vec![-5.7, 7.3, -9.1, 12.6], 0),
    ]);

    vector.ceil();

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![2.0, -2.0, 4.0, -5.0, 9.0], 0),
      create_time_series(vec![-5.0, 8.0, -9.0, 13.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_clamp() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, -2.5, 3.8, -5.9, 8.2], 0),
      create_time_series(vec![-5.7, 7.3, -9.1, 12.6], 0),
    ]);

    vector.clamp(-2.0, 6.0);

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![1.1, -2.0, 3.8, -2.0, 6.0], 0),
      create_time_series(vec![-2.0, 6.0, -2.0, 6.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_clamp_max() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, -2.5, 3.8, -5.9, 8.2], 0),
      create_time_series(vec![-5.7, 7.3, -9.1, 12.6], 0),
    ]);

    vector.clamp_max(6.0);

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![1.1, -2.5, 3.8, -5.9, 6.0], 0),
      create_time_series(vec![-5.7, 6.0, -9.1, 6.0], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_clamp_min() {
    let mut vector = PromQLVector::new(vec![
      create_time_series(vec![1.1, -2.5, 3.8, -5.9, 8.2], 0),
      create_time_series(vec![-5.7, 7.3, -9.1, 12.6], 0),
    ]);

    vector.clamp_min(-2.0);

    let result = vector.vector;

    let expected = vec![
      create_time_series(vec![1.1, -2.0, 3.8, -2.0, 8.2], 0),
      create_time_series(vec![-2.0, 7.3, -2.0, 12.6], 0),
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_changes() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 1.0, 2.0, 3.0, 5.0], 0),
      create_time_series(vec![5.0, 5.0, 5.0, 12.0], 0),
    ]);

    let result = vector.changes();

    let expected = vec![
      3, // 1->2, 2->3, 3->5
      2, // 5->12
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_delta() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.delta();

    let expected = vec![
      vec![Some(1.0), Some(1.0), Some(2.0), Some(3.0)],
      vec![Some(2.0), Some(2.0), Some(3.0)],
    ];

    assert_eq!(result, expected);
  }

  #[test]
  fn test_deriv() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![1.0, 2.0, 3.0, 5.0, 8.0], 0),
      create_time_series(vec![5.0, 7.0, 9.0, 12.0], 0),
    ]);

    let result = vector.deriv();

    let expected = vec![vec![1.0, 1.0, 2.0, 3.0], vec![2.0, 2.0, 3.0]];

    assert_eq!(result, expected);
  }
}

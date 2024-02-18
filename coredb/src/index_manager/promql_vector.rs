// TODO: Add error checking, particularly when a vector is not the expected instant vector
// or range vector
//
// By the time the query reaches here, all time filters should have already been applied,
// so range vector and instant vectors are largely treated the same.
//
// TODO: Histograms are not yet supported
use crate::index_manager::promql_time_series::PromQLTimeSeries;
use crate::metric::metric_point::MetricPoint;
use chrono::Utc;
use std::collections::HashMap;

pub enum AggregationOperator {
  Sum,
  Min,
  Max,
  Avg,
  Group,
  Stddev,
  Stdvar,
  Count,
  CountValues(String),
  Bottomk(usize), // TODO: Check k is of type usize
  Topk(usize),    // TODO: Check k is of type usize
  Quantile(f64),  // TODO: Check phi is of type f64
}

// Represents both an Instant Vector and a Range Vector, with
// and Instant Vector being a single sample (i.e. metric_points.len() == 1)
// and Range Vector having a range of metrics points (i.e. metric_points.len() > 1)
#[derive(Debug, Clone)]
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

  pub fn power(&mut self, other: &PromQLVector) {
    self.apply_binary_operation(other, |a, b| (a.powf(b)));
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
            self_mp.set_value(op(self_mp.get_value(), other_mp.get_value()));
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
      AggregationOperator::CountValues(label_name) => self.count_values(label_name),
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
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        sum,
      )]);
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
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        min,
      )]);
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
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        max,
      )]);
    }
  }

  fn avg(&mut self) {
    for ts in &mut self.vector {
      let sum: f64 = ts.get_metric_points().iter().map(|mp| mp.get_value()).sum();
      let avg = sum / ts.get_metric_points().len() as f64;
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        avg,
      )]);
    }
  }

  fn group(&mut self) {
    for ts in &mut self.vector {
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        1.0,
      )]);
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
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        variance.sqrt(),
      )]);
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
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        variance,
      )]);
    }
  }

  fn count(&mut self) {
    for ts in &mut self.vector {
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        ts.get_metric_points().len() as f64,
      )]);
    }
  }

  pub fn count_values(&mut self, label_key: &str) {
    // Initialize a HashMap to store label values and their counts
    let mut value_counts: HashMap<String, usize> = HashMap::new();

    // Iterate over each time series
    for ts in &self.vector {
      // Iterate over each metric point
      for mp in ts.get_metric_points() {
        // Get the label value associated with the specified label key
        if let Some(label_value) = ts.get_labels().get(label_key) {
          // Increment the count for this label value
          *value_counts.entry(label_value.clone()).or_insert(0) += 1;
        }
      }
    }

    // Clear the existing vector to repopulate it with counted values
    self.vector.clear();

    // Create a new time series for each label and its count
    for (label_value, count) in value_counts {
      let mut labels = HashMap::new();
      labels.insert(label_key.to_string(), label_value.clone());

      // Create a single MetricPoint with the count as its value
      let metric_points = vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        count as f64,
      )];

      // Push a new PromQLTimeSeries with the derived label and the count as its only metric point
      self
        .vector
        .push(PromQLTimeSeries::new(labels.clone(), metric_points));
    }
  }

  fn bottomk(&mut self, k: usize) {
    for ts in &mut self.vector {
      let points = ts.get_metric_points();
      let mut points_clone = points.clone(); // Clone the vector to get Vec<MetricPoint>
      points_clone
        .select_nth_unstable_by(k, |a, b| a.get_value().partial_cmp(&b.get_value()).unwrap());

      let bottom_k = points_clone
        .into_iter()
        .take(k)
        .collect::<Vec<MetricPoint>>();
      ts.set_metric_points(bottom_k);
    }
  }

  fn topk(&mut self, k: usize) {
    for ts in &mut self.vector {
      let points = ts.get_metric_points();
      let mut points_clone = points.clone();

      points_clone
        .select_nth_unstable_by(k, |a, b| b.get_value().partial_cmp(&a.get_value()).unwrap());

      // Reverse, take k, and clone each MetricPoint
      let top_k = points_clone
        .into_iter()
        .take(k)
        .collect::<Vec<MetricPoint>>();
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

  // **** Functions: https://prometheus.io/docs/prometheus/latest/querying/functions/

  // Applies the absolute value operation to every metric point in every time series
  pub fn abs(&mut self) {
    for ts in &mut self.vector {
      ts.abs();
    }
  }

  /// Returns an empty vector if the vector has any elements, and 1 with the current time
  /// and a manufactured label if it doesn't.
  pub fn absent(&mut self) {
    if self.vector.is_empty() {
      let mut labels = HashMap::new();
      labels.insert("absent".to_string(), "true".to_string());
      let mut absent_metric_point = MetricPoint::new(chrono::Utc::now().timestamp() as u64, 1.0);
      let mut absent_series = PromQLTimeSeries::new(labels, vec![absent_metric_point]);
      self.vector.push(absent_series);
    } else {
      self.vector.clear();
    }
  }

  /// Returns an empty vector if the vector has any elements, and 1 with the current time
  /// and a manufactured label if it doesn't.
  pub fn absent_over_time(&mut self) {
    if self.vector.is_empty() {
      let mut labels = HashMap::new();
      labels.insert("absent".to_string(), "true".to_string());
      let mut absent_metric_point = MetricPoint::new(chrono::Utc::now().timestamp() as u64, 1.0);
      let mut absent_series = PromQLTimeSeries::new(labels, vec![absent_metric_point]);
      self.vector.push(absent_series);
    } else {
      self.vector.clear();
    }
  }

  // Applies the ceiling operation to every metric point in every time series
  pub fn ceil(&mut self) {
    for ts in &mut self.vector {
      ts.ceil();
    }
  }

  // Clamps the min and max value of metric points for each time series
  pub fn clamp(&mut self, min: f64, max: f64) {
    if min > max || min.is_nan() || max.is_nan() {
      self.vector.clear();
    } else {
      for ts in &mut self.vector {
        ts.clamp(min, max);
      }
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
  pub fn changes(&mut self) {
    for ts in &mut self.vector {
      ts.changes();
    }
  }

  // Calculates the day of the month for the metric points in each time series
  pub fn day_of_month(&mut self) {
    for ts in &mut self.vector {
      ts.day_of_month();
    }
  }

  // Calculates the day of the week for the metric points in each time series
  pub fn day_of_week(&mut self) {
    for ts in &mut self.vector {
      ts.day_of_week();
    }
  }

  // Calculates the day of the year for the metric points in each time series
  pub fn day_of_year(&mut self) {
    for ts in &mut self.vector {
      ts.day_of_year();
    }
  }

  // Calculates the days in the month for the metric points in each time series
  pub fn days_in_month(&mut self) {
    for ts in &mut self.vector {
      ts.days_in_month();
    }
  }

  // Converts degrees to radians for each time series element in the vector
  pub fn deg(&mut self) {
    for ts in &mut self.vector {
      ts.deg();
    }
  }

  // Computes the difference between the first and last value of each time series element in the vector
  pub fn delta(&mut self) {
    for ts in &mut self.vector {
      ts.delta();
    }
  }

  // Computes the derivative of each time series element in the vector
  pub fn deriv(&mut self) {
    for ts in &mut self.vector {
      ts.deriv();
    }
  }

  // Computes the exponential function for each time series element in the vector
  pub fn exp(&mut self) {
    for ts in &mut self.vector {
      ts.exp();
    }
  }

  // Applies the floor function to each time series element in the vector in place
  pub fn floor(&mut self) {
    for ts in &mut self.vector {
      ts.floor();
    }
  }

  // Produces a smoothed value for time series based on the range
  pub fn holt_winters(&mut self, alpha: f64, beta: f64) {
    for ts in &mut self.vector {
      ts.holt_winters(alpha, beta);
    }
  }

  // Returns the hour of the day for each of the given times in the metric points
  pub fn hour(&mut self) {
    for ts in &mut self.vector {
      ts.hour();
    }
  }

  pub fn idelta(&mut self) {
    for ts in &mut self.vector {
      ts.idelta();
    }
  }

  // Computes the total increase over time for each time series
  pub fn increase(&mut self) {
    for ts in &mut self.vector {
      ts.increase();
    }
  }

  // Computes the instantaneous rate of change for each time series
  pub fn irate(&mut self) {
    for ts in &mut self.vector {
      ts.irate();
    }
  }

  pub fn label_join(&mut self, dst_label: &str, separator: &str, src_labels: &[&str]) {
    for ts in &mut self.vector {
      ts.label_join(dst_label, separator, src_labels);
    }
  }

  pub fn label_replace(
    &mut self,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
  ) {
    for ts in &mut self.vector {
      ts.label_replace(dst_label, replacement, src_label, regex);
    }
  }

  pub fn ln(&mut self) {
    for ts in &mut self.vector {
      ts.ln();
    }
  }

  pub fn log2(&mut self) {
    for ts in &mut self.vector {
      ts.log2();
    }
  }

  pub fn minute(&mut self) {
    for ts in &mut self.vector {
      ts.minute();
    }
  }

  pub fn month(&mut self) {
    for ts in &mut self.vector {
      ts.month();
    }
  }

  pub fn predict_linear(&mut self, t: f64) {
    for ts in &mut self.vector {
      ts.predict_linear(t);
    }
  }

  pub fn rad(&mut self) {
    for ts in &mut self.vector {
      ts.rad();
    }
  }

  pub fn rate(&mut self) {
    for ts in &mut self.vector {
      ts.rate();
    }
  }

  pub fn resets(&mut self) {
    for ts in &mut self.vector {
      ts.resets();
    }
  }

  pub fn round(&mut self, to_nearest: f64) {
    for ts in &mut self.vector {
      ts.round(to_nearest);
    }
  }

  pub fn scalar(&self) -> f64 {
    if self.vector.len() == 1 {
      if let Some(metric_point) = self.vector[0].get_metric_points().first() {
        return metric_point.get_value();
      }
    }
    f64::NAN
  }

  // Determines the sign of each metric point's value in every time series
  pub fn sgn(&mut self) {
    for ts in &mut self.vector {
      ts.sgn();
    }
  }

  pub fn sort(&mut self) {
    for ts in &mut self.vector {
      ts.sort();
    }
  }

  pub fn sort_desc(&mut self) {
    for ts in &mut self.vector {
      ts.sort_desc();
    }
  }

  pub fn sqrt(&mut self) {
    for ts in &mut self.vector {
      ts.sqrt();
    }
  }

  pub fn timestamp(&mut self) {
    for ts in &mut self.vector {
      ts.timestamp();
    }
  }

  pub fn vector(&self, s: f64) -> PromQLVector {
    let current_time = Utc::now().timestamp();
    PromQLVector::new(vec![PromQLTimeSeries::new(
      HashMap::new(),
      vec![MetricPoint::new(current_time as u64, s)],
    )])
  }

  pub fn year(&mut self) {
    for ts in &mut self.vector {
      ts.year();
    }
  }

  // **** Aggregations over time ****

  pub fn avg_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.avg_over_time();
    }
  }

  pub fn min_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.min_over_time();
    }
  }

  pub fn max_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.max_over_time();
    }
  }

  pub fn sum_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.sum_over_time();
    }
  }

  pub fn count_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.count_over_time();
    }
  }

  pub fn quantile_over_time(&mut self, quantile: f64) {
    for ts in &mut self.vector {
      ts.quantile_over_time(quantile);
    }
  }

  pub fn stddev_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.stddev_over_time();
    }
  }

  pub fn stdvar_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.stdvar_over_time();
    }
  }

  pub fn mad_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.mad_over_time();
    }
  }

  pub fn last_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.last_over_time();
    }
  }

  pub fn present_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.present_over_time();
    }
  }
}

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

impl Hash for PromQLVector {
  fn hash<H: Hasher>(&self, state: &mut H) {
    for ts in &self.vector {
      ts.hash(state);
    }
  }
}

impl PartialEq for PromQLVector {
  fn eq(&self, other: &Self) -> bool {
    self.vector == other.vector
  }
}

impl Eq for PromQLVector {}

impl PartialOrd for PromQLVector {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.vector.len().cmp(&other.vector.len()))
  }
}

impl Ord for PromQLVector {
  fn cmp(&self, other: &Self) -> Ordering {
    self.vector.len().cmp(&other.vector.len())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn create_time_series(values: Vec<f64>, timestamp: u64) -> PromQLTimeSeries {
    let metric_points = values
      .iter()
      .map(|&value| MetricPoint::new(timestamp, value))
      .collect();
    PromQLTimeSeries::new(HashMap::new(), metric_points) // Assuming labels are not provided here
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

    let expected = PromQLVector::new(vec![
      create_time_series(vec![None, 2.0, 3.0, 4.0], 0),
      create_time_series(vec![None, 3.0, 4.0], 0),
    ]);

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
  fn test_present_over_time() {
    let vector = PromQLVector::new(vec![
      create_time_series(vec![10.0, 8.0, 12.0, 15.0, 18.0], 0),
      create_time_series(vec![5.0, 6.0, 6.0, 5.0], 0),
    ]);

    let result = vector.present_over_time();

    let expected = vec![1, 1];

    assert_eq!(result, expected);
  }
}

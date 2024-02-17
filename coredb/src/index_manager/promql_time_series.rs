use crate::metric::metric_point::MetricPoint;
use chrono::{Datelike, NaiveDateTime, Utc};
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub struct PromQLTimeSeries {
  labels: HashMap<String, String>,
  metric_points: Vec<MetricPoint>,
}

// Define the InstantTimeSeries trait
pub trait InstantTimeSeries {
  fn is_instant(&self) -> bool;
}

// Define the RangeTimeSeries trait
pub trait RangeTimeSeries {
  fn is_range(&self) -> bool;
}

// Implement InstantTimeSeries for PromQLTimeSeries
impl InstantTimeSeries for PromQLTimeSeries {
  fn is_instant(&self) -> bool {
    self.metric_points.len() == 1
  }
}

// Implement RangeTimeSeries for PromQLTimeSeries
impl RangeTimeSeries for PromQLTimeSeries {
  fn is_range(&self) -> bool {
    self.metric_points.len() > 1
  }
}

// For implementing PromQL functions on time series

impl PromQLTimeSeries {
  pub fn new(labels: HashMap<String, String>, metric_points: Vec<MetricPoint>) -> Self {
    PromQLTimeSeries {
      labels,
      metric_points,
    }
  }

  pub fn get_labels(&self) -> &HashMap<String, String> {
    &self.labels
  }

  pub fn get_metric_points(&self) -> &Vec<MetricPoint> {
    &self.get_metric_points()
  }

  // Computes the day of the year for each metric point's timestamp
  pub fn day_of_year(&self) -> Vec<u32> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| Utc.get_time().stamp(mp.get_time() as i64, 0).ordinal())
      .collect()
  }

  // Converts radians to degrees for each metric point's value
  pub fn deg(&self) -> Vec<f64> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value() * (180.0 / PI))
      .collect()
  }

  // Computes the difference between consecutive metric points
  pub fn delta(&self) -> Vec<f64> {
    self
      .get_metric_points()
      .windows(2)
      .map(|w| w[1].get_value() - w[0].get_value())
      .collect()
  }

  // Computes the derivative of the time series
  pub fn deriv(&self) -> Vec<f64> {
    self
      .get_metric_points()
      .windows(2)
      .map(|w| {
        let time_diff = w[1].get_time() - w[0].get_time();
        if time_diff > 0 {
          (w[1].get_value() - w[0].get_value()) / time_diff as f64
        } else {
          0.0
        }
      })
      .collect()
  }

  // Applies the exponential function to each metric point's value
  pub fn exp(&self) -> Vec<f64> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value().exp())
      .collect()
  }

  // Applies the floor function to each metric point's value
  pub fn floor(&self) -> Vec<f64> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value().floor())
      .collect()
  }

  // Computes the histogram quantile across metric points
  pub fn histogram_quantile(&self, q: f64) -> Option<f64> {
    if q < 0.0 || q > 1.0 || self.get_metric_points().is_empty() {
      return None;
    }
    let mut values: Vec<f64> = self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pos = ((values.len() - 1) as f64 * q).round() as usize;
    values.get(pos).copied()
  }

  pub fn holt_winters(&self, alpha: f64, beta: f64) -> Vec<f64> {
    if self.get_metric_points().is_empty() {
      return vec![];
    }

    let mut forecasts: Vec<f64> = Vec::new();
    let mut level: f64 = self.get_metric_points()[0].get_value(); // Initial level
    let mut trend: f64 = 0.0; // Initial trend
    if self.get_metric_points().len() > 1 {
      trend = self.get_metric_points()[1].get_value() - self.get_metric_points()[0].get_value();
      // Initial trend
    }

    for i in 0..self.get_metric_points().len() {
      let value = self.get_metric_points()[i].get_value();
      if i > 0 {
        // Update the forecasts
        let forecast = level + trend;
        forecasts.push(forecast);

        // Update level and trend
        let new_level = alpha * value + (1.0 - alpha) * (level + trend);
        let new_trend = beta * (new_level - level) + (1.0 - beta) * trend;
        level = new_level;
        trend = new_trend;
      }
    }

    // Append one more forecast beyond the last point
    forecasts.push(level + trend);

    forecasts
  }

  // Extracts the hour from each metric point's timestamp
  pub fn hour(&self) -> Vec<u32> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| Utc.get_time().stamp(mp.get_time() as i64, 0).hour())
      .collect()
  }

  // Assuming MetricPoint.get_time() is in seconds and MetricPoint.get_value() is the metric value.
  pub fn idelta(&self) -> Option<f64> {
    self
      .get_metric_points()
      .windows(2)
      .last()
      .map(|window| window[1].get_value() - window[0].get_value())
  }

  pub fn increase(&self) -> Option<f64> {
    match (
      self.get_metric_points().first(),
      self.get_metric_points().last(),
    ) {
      (Some(first), Some(last)) => Some(last.get_value() - first.get_value()),
      _ => None,
    }
  }

  pub fn irate(&self) -> Option<f64> {
    self.get_metric_points().windows(2).last().map(|window| {
      let time_diff = window[1].get_time() - window[0].get_time();
      if time_diff > 0 {
        (window[1].get_value() - window[0].get_value()) / time_diff as f64
      } else {
        0.0
      }
    })
  }

  pub fn last_over_time(&self) -> Option<f64> {
    self.get_metric_points().last().map(|mp| mp.get_value())
  }

  pub fn ln(&self) -> Vec<Option<f64>> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value().ln().into())
      .collect()
  }

  pub fn log10(&self) -> Vec<Option<f64>> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value().log10().into())
      .collect()
  }

  pub fn label_replace(
    &mut self,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
  ) {
    let re = Regex::new(regex).unwrap();
    if let Some(value) = self.labels.get(src_label) {
      let new_value = re.replace(value, replacement).to_string();
      self.labels.insert(dst_label.to_string(), new_value);
    }
  }

  // Joins multiple label values into a new label.
  pub fn label_join(&mut self, dst_label: &str, separator: &str, src_labels: &[&str]) {
    let new_value = src_labels
      .iter()
      .filter_map(|label| self.labels.get(*label))
      .collect::<Vec<&String>>()
      .join(separator);
    self.labels.insert(dst_label.to_string(), new_value);
  }

  // Applies log2 to each metric point's value.
  pub fn log2(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().log2();
    }
  }

  // Calculates the maximum value over time.
  pub fn max_over_time(&self) -> Option<f64> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .max_by(|a, b| a.partial_cmp(b).unwrap())
  }

  // Calculates the minimum value over time.
  pub fn min_over_time(&self) -> Option<f64> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .min_by(|a, b| a.partial_cmp(b).unwrap())
  }

  // Forecasts future values based on linear regression.
  pub fn predict_linear(&self, seconds_into_future: f64) -> Option<f64> {
    let samples: Vec<(f64, f64)> = self
      .get_metric_points()
      .iter()
      .map(|mp| (mp.get_time() as f64, mp.get_value()))
      .collect();

    let n = samples.len() as f64;
    let sum_x: f64 = samples.iter().map(|(x, _)| *x).sum();
    let sum_y: f64 = samples.iter().map(|(_, y)| *y).sum();
    let sum_xx: f64 = samples.iter().map(|(x, _)| x * x).sum();
    let sum_xy: f64 = samples.iter().map(|(x, y)| x * y).sum();

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
    let intercept = (sum_y - slope * sum_x) / n;

    let latest_timestamp = samples.last().map(|(x, _)| *x).unwrap_or(0.0);
    Some(intercept + slope * (latest_timestamp + seconds_into_future))
  }

  // Calculates a quantile over time.
  pub fn quantile_over_time(&self, q: f64) -> Option<f64> {
    if self.get_metric_points().is_empty() || q < 0.0 || q > 1.0 {
      return None;
    }
    let mut values: Vec<f64> = self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pos = (q * (values.len() as f64 - 1.0)).round() as usize;
    values.get(pos).copied()
  }

  // Converts each metric point's value from degrees to radians.
  pub fn rad(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().to_radians();
    }
  }

  // Calculates the per-second rate of increase.
  pub fn rate(&self) -> Option<f64> {
    if let (Some(first), Some(last)) = (
      self.get_metric_points().first(),
      self.get_metric_points().last(),
    ) {
      let duration = (last.get_time() - first.get_time()) as f64;
      if duration > 0.0 {
        Some((last.get_value() - first.get_value()) / duration)
      } else {
        None
      }
    } else {
      None
    }
  }

  // Counts resets in the time series.
  pub fn resets(&self) -> usize {
    self
      .get_metric_points()
      .windows(2)
      .filter(|window| window[1].get_value() < window[0].get_value())
      .count()
  }

  // Rounds each metric point's value.
  pub fn round(&mut self, precision: Option<u32>) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = match precision {
        Some(p) => {
          let multiplier = 10f64.powi(p as i32);
          (mp.get_value() * multiplier).round() / multiplier
        }
        None => mp.get_value().round(),
      };
    }
  }

  // Converts the first metric point's value to a scalar, if it's the only point.
  pub fn scalar(&self) -> Option<f64> {
    if self.get_metric_points().len() == 1 {
      Some(self.get_metric_points()[0].get_value())
    } else {
      None
    }
  }

  // Determines the sign of each metric point's value.
  pub fn sgn(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().signum();
    }
  }

  // Applies the sine function to each metric point's value.
  pub fn sin(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().sin();
    }
  }

  // Applies the hyperbolic sine function to each metric point's value.
  pub fn sinh(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().sinh();
    }

    // TODO: Dummy implementation. Write the real one.
    pub fn sort(metric_points: &mut Vec<MetricPoint>) {
      metric_points.sort_by(|a, b| {
        a.get_value()
          .partial_cmp(&b.get_value())
          .unwrap_or(std::cmp::Ordering::Equal)
      });
    }
  }

  // Sorts metric points in descending order by their values.
  pub fn sort_desc(metric_points: &mut Vec<MetricPoint>) {
    metric_points.sort_by(|a, b| {
      b.get_value()
        .partial_cmp(&a.get_value())
        .unwrap_or(std::cmp::Ordering::Equal)
    });
  }

  // Applies the square root operation to each metric point's value.
  pub fn sqrt(&mut self) {
    for mp in &mut self.get_metric_points() {
      if mp.get_value() >= 0.0 {
        mp.get_value() = mp.get_value().sqrt();
      }
      // TODO: Handle negative values, perhaps setting them to NaN or 0.
    }
  }

  // Calculates the standard deviation over time.
  pub fn stddev_over_time(&self) -> f64 {
    let mean: f64 = self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .sum::<f64>()
      / self.get_metric_points().len() as f64;
    let variance: f64 = self
      .get_metric_points()
      .iter()
      .map(|mp| (mp.get_value() - mean).powi(2))
      .sum::<f64>()
      / self.get_metric_points().len() as f64;
    variance.sqrt()
  }

  // Calculates the variance over time.
  pub fn stdvar_over_time(&self) -> f64 {
    let mean: f64 = self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .sum::<f64>()
      / self.get_metric_points().len() as f64;
    self
      .get_metric_points()
      .iter()
      .map(|mp| (mp.get_value() - mean).powi(2))
      .sum::<f64>()
      / self.get_metric_points().len() as f64
  }

  // Calculates the sum of metric points over time.
  pub fn sum_over_time(&self) -> f64 {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_value())
      .sum()
  }

  // Applies the tangent function to each metric point's value.
  pub fn tan(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().tan();
    }
  }

  // Applies the hyperbolic tangent function to each metric point's value.
  pub fn tanh(&mut self) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = mp.get_value().tanh();
    }
  }

  // Returns the timestamp of the last metric point.
  pub fn time(&self) -> Option<u64> {
    self.get_metric_points().last().map(|mp| mp.get_time())
  }

  // Extracts and returns all timestamps from the metric points.
  pub fn timestamp(&self) -> Vec<u64> {
    self
      .get_metric_points()
      .iter()
      .map(|mp| mp.get_time())
      .collect()
  }

  // Sets all metric points to a scalar value.
  pub fn vector(&mut self, scalar: f64) {
    for mp in &mut self.get_metric_points() {
      mp.get_value() = scalar;
    }
  }
}

impl Hash for PromQLTimeSeries {
  fn hash<H: Hasher>(&self, state: &mut H) {
    for (key, value) in &self.labels {
      key.hash(state);
      value.hash(state);
    }
    for point in &self.get_metric_points() {
      point.get_time().hash(state);
    }
  }
}

impl PartialEq for PromQLTimeSeries {
  fn eq(&self, other: &Self) -> bool {
    self.labels == other.labels && self.get_metric_points() == other.get_metric_points()
  }
}

impl Eq for PromQLTimeSeries {}

impl PartialOrd for PromQLTimeSeries {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(
      self
        .get_metric_points()
        .len()
        .cmp(&other.get_metric_points().len()),
    )
  }
}

impl Ord for PromQLTimeSeries {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .get_metric_points()
      .len()
      .cmp(&other.get_metric_points().len())
  }
}

#[cfg(test)]
mod tests {
  // Mock MetricPoint for test
  #[derive(Debug, Clone, PartialEq, Hash)]
  pub struct MetricPoint {
    pub time: u64,
    pub value: f64,
  }

  use super::*;

  use std::collections::HashMap;

  fn create_test_metric_points() -> Vec<MetricPoint> {
    vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
      MetricPoint {
        time: 4,
        value: 40.0,
      },
      MetricPoint {
        time: 5,
        value: 50.0,
      },
    ]
  }

  #[test]
  fn test_stddev_calculates_correct_value() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    let expected_stddev = 14.142135623730951; // Calculated value
    assert!((ts.stddev() - expected_stddev).abs() < f64::EPSILON);
  }

  #[test]
  fn test_stdvar_calculates_correct_value() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    let expected_variance = 200.0; // Calculated value
    assert!((ts.stdvar() - expected_variance).abs() < f64::EPSILON);
  }

  use std::collections::HashMap;

  // Helper function to create metric points with at least three entries
  fn create_metric_points_basic() -> Vec<MetricPoint> {
    vec![
      MetricPoint {
        time: 1609459200,
        value: 0.5,
      },
      MetricPoint {
        time: 1609459260,
        value: 0.6,
      },
      MetricPoint {
        time: 1609459320,
        value: 0.7,
      },
    ]
  }

  // Helper function to create consistent labels for testing
  fn create_labels() -> HashMap<String, String> {
    HashMap::from([
      ("instance".to_string(), "localhost:9090".to_string()),
      ("job".to_string(), "prometheus".to_string()),
    ])
  }

  #[test]
  fn test_promql_time_series_basic() {
    let labels = create_labels();
    let metric_points = create_metric_points_basic();

    let ts = PromQLTimeSeries::new(labels.clone(), metric_points.clone());
    assert_eq!(ts.get_labels(), &labels);
    assert_eq!(ts.get_metric_points(), &metric_points);
  }

  #[test]
  fn test_logical_and() {
    let labels_ts1 = create_labels();
    let metric_points_ts1 = create_metric_points_basic();
    let mut ts1 = PromQLTimeSeries::new(labels_ts1, metric_points_ts1);

    let mut labels_ts2 = create_labels();
    // Modifying the "job" label value to simulate a difference
    labels_ts2.insert("job".to_string(), "node_exporter".to_string());
    let metric_points_ts2 = vec![MetricPoint {
      time: 1609459300,
      value: 0.4,
    }];

    let ts2 = PromQLTimeSeries::new(labels_ts2, metric_points_ts2);

    ts1.logical_and(&ts2);
    // Expecting ts1 to have no metric points left after the 'and' operation due to label mismatch
    assert!(ts1.get_metric_points().is_empty());
  }

  #[test]
  fn test_quantile_calculates_correct_value() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Assuming quantile calculation is correct and sorted
    assert_eq!(ts.quantile(0.5), Some(30.0)); // Median value for simplicity
  }

  #[test]
  fn test_topk_selects_correct_values() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    assert_eq!(ts.topk(2), vec![50.0, 40.0]); // Top 2 values
  }

  #[test]
  fn test_bottomk_selects_correct_values() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    assert_eq!(ts.bottomk(2), vec![10.0, 20.0]); // Bottom 2 values
  }

  #[test]
  fn test_exp_applies_to_all_values() {
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.exp(); // Apply exponential function
              // Verify each value is exp(value)
    assert!(ts
      .get_metric_points()
      .iter()
      .all(|mp| (mp.get_value() - mp.get_value().exp()).abs() < f64::EPSILON));
  }

  // New helper function for sqrt and floor friendly metric points
  fn create_sqrt_floor_friendly_metric_points() -> Vec<MetricPoint> {
    vec![
      MetricPoint {
        time: 1,
        value: 9.5,
      }, // sqrt -> 3, floor -> 9
      MetricPoint {
        time: 2,
        value: 16.5,
      }, // sqrt -> 4, floor -> 16
      MetricPoint {
        time: 3,
        value: 25.5,
      }, // sqrt -> 5, floor -> 25
    ]
  }

  #[test]
  fn test_floor_applies_to_all_values() {
    let mut ts = PromQLTimeSeries::new(HashMap::new(), create_sqrt_floor_friendly_metric_points());
    ts.floor(); // Apply floor function

    let expected_values = vec![9.0, 16.0, 25.0];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_values
    );
  }

  #[test]
  fn test_sqrt_applies_to_all_values() {
    let mut ts = PromQLTimeSeries::new(HashMap::new(), create_sqrt_floor_friendly_metric_points());
    ts.sqrt(); // Apply square root function

    // Since the original values are slightly above perfect squares, check against the sqrt of the floored values
    let expected_values = vec![3.0, 4.0, 5.0]; // Corresponding to the sqrt of floored values
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_values
    );
  }

  // Test for label_replace
  #[test]
  fn test_label_replace() {
    let labels = create_labels(); // Assuming create_labels() is already defined
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(labels, metric_points);

    ts.label_replace("job", "replaced_job", "job", ".*");
    assert_eq!(ts.labels.get("job").unwrap(), &"replaced_job");
  }

  // Test for label_join
  #[test]
  fn test_label_join() {
    let mut labels = create_labels();
    labels.insert("region".to_string(), "us-west".to_string());
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(labels, metric_points);

    ts.label_join("new_label", "-", &["job", "region"]);
    assert_eq!(ts.labels.get("new_label").unwrap(), &"prometheus-us-west");
  }

  // Test for log2
  #[test]
  fn test_log2() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 1.0,
      }, // log2(1) = 0
      MetricPoint {
        time: 2,
        value: 2.0,
      }, // log2(2) = 1
      MetricPoint {
        time: 3,
        value: 4.0,
      }, // log2(4) = 2
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.log2();

    let expected_values = vec![0.0, 1.0, 2.0];
    for (mp, &expected) in ts.get_metric_points().iter().zip(expected_values.iter()) {
      assert!((mp.get_value() - expected).abs() < f64::EPSILON);
    }
  }

  // Test for max_over_time
  #[test]
  fn test_max_over_time() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Assuming max_over_time is correctly implemented
    assert_eq!(ts.max_over_time(), Some(50.0)); // Based on the create_test_metric_points values
  }

  // Test for min_over_time
  #[test]
  fn test_min_over_time() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Assuming min_over_time is correctly implemented
    assert_eq!(ts.min_over_time(), Some(10.0)); // Based on the create_test_metric_points values
  }

  // Test for increase
  #[test]
  fn test_increase() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 100.0,
      },
      MetricPoint {
        time: 2,
        value: 150.0,
      },
      MetricPoint {
        time: 3,
        value: 200.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // The increase should be the difference between the first and last value
    assert_eq!(ts.increase(), Some(100.0)); // 200.0 - 100.0 = 100.0
  }

  // Test for irate
  #[test]
  fn test_irate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 100.0,
      },
      MetricPoint {
        time: 2,
        value: 110.0,
      },
      MetricPoint {
        time: 4,
        value: 130.0,
      }, // Note the time jump
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // irate should calculate the rate of increase per second between the last two points
    let expected_rate = 10.0 / 2.0; // (130.0 - 110.0) / (4 - 2)
    assert_eq!(ts.irate(), Some(expected_rate));
  }

  // Test for last_over_time
  #[test]
  fn test_last_over_time() {
    let metric_points = create_test_metric_points(); // Assumes this provides a sorted list by time
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // The last value should match the value of the last MetricPoint in the series
    let last_value = ts.get_metric_points().last().unwrap().get_value();
    assert_eq!(ts.last_over_time(), Some(last_value));
  }

  // Test for ln
  #[test]
  fn test_ln() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: std::f64::consts::E,
      },
      MetricPoint {
        time: 2,
        value: std::f64::consts::E.powi(2),
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.ln(); // Apply natural logarithm

    // Check if ln was applied correctly
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 2.0);
  }

  // Test for log10
  #[test]
  fn test_log10() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 100.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.log10(); // Apply base 10 logarithm

    // Validate that log10 was applied correctly
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 2.0);
  }

  // Test for holt_winters
  #[test]
  fn test_holt_winters() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
      MetricPoint {
        time: 4,
        value: 40.0,
      },
      MetricPoint {
        time: 5,
        value: 50.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    let alpha = 0.8;
    let beta = 0.2;

    // Holt-Winters forecasting should produce accurate forecasts
    let expected_forecasts = vec![40.0, 60.0, 80.0, 100.0, 120.0];
    let forecasts = ts.holt_winters(alpha, beta);
    assert_eq!(forecasts, expected_forecasts);
  }

  // Test for rate
  #[test]
  fn test_rate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 4,
        value: 30.0,
      }, // Note the time gap
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Rate should calculate the rate of change between the first and last points
    let expected_rate = Some((30.0 - 10.0) / (4 - 1)); // (last - first) / time_diff
    assert_eq!(ts.rate(), expected_rate);
  }

  // Test for min_over_time
  #[test]
  fn test_min_over_time() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // min_over_time should return the minimum value over the entire time range
    let expected_min = Some(10.0);
    assert_eq!(ts.min_over_time(), expected_min);
  }

  // Test for max_over_time
  #[test]
  fn test_max_over_time() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // max_over_time should return the maximum value over the entire time range
    let expected_max = Some(50.0);
    assert_eq!(ts.max_over_time(), expected_max);
  }

  // Test for sum_over_time
  #[test]
  fn test_sum_over_time() {
    let metric_points = create_test_metric_points();
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // sum_over_time should return the sum of all values over the entire time range
    let expected_sum = 150.0; // 10.0 + 20.0 + 30.0 + 40.0 + 50.0
    assert_eq!(ts.sum_over_time(), expected_sum);
  }

  // Test for log2
  #[test]
  fn test_log2() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 16.0,
      },
      MetricPoint {
        time: 2,
        value: 64.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.log2();

    let expected_values = vec![4.0, 6.0];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_values
    );
  }

  // Test for label_replace
  #[test]
  fn test_label_replace() {
    let mut labels = HashMap::new();
    labels.insert("source".to_string(), "data1".to_string());
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(labels.clone(), metric_points);

    ts.label_replace("destination", "new", "source", r"^(data)\d+$");
    let expected_labels = [
      ("source".to_string(), "data1".to_string()),
      ("destination".to_string(), "new".to_string()),
    ]
    .iter()
    .cloned()
    .collect::<HashMap<String, String>>();

    // Check if the label has been replaced as expected
    assert_eq!(ts.get_labels(), &expected_labels);
  }

  // Test for label_join
  #[test]
  fn test_label_join() {
    let mut labels = HashMap::new();
    labels.insert("label1".to_string(), "value1".to_string());
    labels.insert("label2".to_string(), "value2".to_string());
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(labels.clone(), metric_points);

    ts.label_join("new_label", "_", &["label1", "label2"]);
    let expected_labels = [
      ("label1".to_string(), "value1".to_string()),
      ("label2".to_string(), "value2".to_string()),
      ("new_label".to_string(), "value1_value2".to_string()),
    ]
    .iter()
    .cloned()
    .collect::<HashMap<String, String>>();

    // Check if the labels have been joined as expected
    assert_eq!(ts.get_labels(), &expected_labels);
  }

  // Test for sin function
  #[test]
  fn test_sin() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: std::f64::consts::FRAC_PI_2,
      }, // π/2
      MetricPoint {
        time: 3,
        value: std::f64::consts::PI,
      }, // π
      MetricPoint {
        time: 4,
        value: 3.0 * std::f64::consts::FRAC_PI_2,
      }, // 3π/2
      MetricPoint {
        time: 5,
        value: 2.0 * std::f64::consts::PI,
      }, // 2π
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.sin(); // Apply sine function

    // Check if the sine function has been applied correctly to all values
    assert_eq!(ts.get_metric_points()[0].get_value(), 0.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 1.0); // sin(π/2) = 1
    assert_eq!(ts.get_metric_points()[2].get_value(), 0.0); // sin(π) = 0
    assert_eq!(ts.get_metric_points()[3].get_value(), -1.0); // sin(3π/2) = -1
    assert_eq!(ts.get_metric_points()[4].get_value(), 0.0); // sin(2π) = 0
  }

  // Test for tanh function
  #[test]
  fn test_tanh() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: 1.0,
      },
      MetricPoint {
        time: 3,
        value: -1.0,
      },
      MetricPoint {
        time: 4,
        value: 2.0,
      },
      MetricPoint {
        time: 5,
        value: -2.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.tanh(); // Apply hyperbolic tangent function

    // Check if the hyperbolic tangent function has been applied correctly to all values
    assert_eq!(ts.get_metric_points()[0].get_value(), 0.0);
    assert_eq!(ts.get_metric_points()[1].get_value(), 0.7615941559557649); // tanh(1) ≈ 0.761594...
    assert_eq!(ts.get_metric_points()[2].get_value(), -0.7615941559557649); // tanh(-1) ≈ -0.761594...
    assert_eq!(ts.get_metric_points()[3].get_value(), 0.9640275800758169); // tanh(2) ≈ 0.964027...
    assert_eq!(ts.get_metric_points()[4].get_value(), -0.9640275800758169); // tanh(-2) ≈ -0.964027...
  }

  // Test for log10 function
  #[test]
  fn test_log10() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 1.0,
      },
      MetricPoint {
        time: 2,
        value: 10.0,
      },
      MetricPoint {
        time: 3,
        value: 100.0,
      },
      MetricPoint {
        time: 4,
        value: 1000.0,
      },
      MetricPoint {
        time: 5,
        value: 10000.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.log10(); // Apply base 10 logarithm function

    // Check if the base 10 logarithm function has been applied correctly to all values
    assert_eq!(ts.get_metric_points()[0].get_value(), 0.0); // log10(1) = 0
    assert_eq!(ts.get_metric_points()[1].get_value(), 1.0); // log10(10) = 1
    assert_eq!(ts.get_metric_points()[2].get_value(), 2.0); // log10(100) = 2
    assert_eq!(ts.get_metric_points()[3].get_value(), 3.0); // log10(1000) = 3
    assert_eq!(ts.get_metric_points()[4].get_value(), 4.0); // log10(10000) = 4
  }

  // Test for resets method
  #[test]
  fn test_resets() {
    // Metric points representing a decreasing sequence followed by an increasing one
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 5.0,
      },
      MetricPoint {
        time: 2,
        value: 4.0,
      },
      MetricPoint {
        time: 3,
        value: 3.0,
      },
      MetricPoint {
        time: 4,
        value: 4.0,
      },
      MetricPoint {
        time: 5,
        value: 5.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // There is one reset from 4 to 3
    assert_eq!(ts.resets(), 1);
  }

  // Test for sort_desc method
  #[test]
  fn test_sort_desc() {
    let mut metric_points = vec![
      MetricPoint {
        time: 1,
        value: 3.0,
      },
      MetricPoint {
        time: 2,
        value: 1.0,
      },
      MetricPoint {
        time: 3,
        value: 5.0,
      },
      MetricPoint {
        time: 4,
        value: 2.0,
      },
      MetricPoint {
        time: 5,
        value: 4.0,
      },
    ];
    PromQLTimeSeries::sort_desc(&mut metric_points);

    // Check if the metric points have been sorted in descending order
    assert_eq!(
      metric_points,
      vec![
        MetricPoint {
          time: 3,
          value: 5.0
        },
        MetricPoint {
          time: 5,
          value: 4.0
        },
        MetricPoint {
          time: 1,
          value: 3.0
        },
        MetricPoint {
          time: 4,
          value: 2.0
        },
        MetricPoint {
          time: 2,
          value: 1.0
        },
      ]
    );
  }

  // Test for day_of_year method
  #[test]
  fn test_day_of_year() {
    let metric_points = vec![
      MetricPoint {
        time: 1609459200,
        value: 10.0,
      },
      MetricPoint {
        time: 1609459260,
        value: 20.0,
      },
      MetricPoint {
        time: 1609459320,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Expected day of the year values for each timestamp
    let expected_day_of_year = vec![367, 367, 367]; // Assuming all timestamps correspond to the same day

    // Check if the day of the year has been computed correctly for each timestamp
    assert_eq!(ts.day_of_year(), expected_day_of_year);
  }

  // Test for hour method
  #[test]
  fn test_hour() {
    let metric_points = vec![
      MetricPoint {
        time: 1609459200,
        value: 10.0,
      },
      MetricPoint {
        time: 1609459260,
        value: 20.0,
      },
      MetricPoint {
        time: 1609459320,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Expected hour values for each timestamp
    let expected_hour = vec![0, 0, 0]; // Assuming all timestamps correspond to the same hour (midnight)

    // Check if the hour has been extracted correctly for each timestamp
    assert_eq!(ts.hour(), expected_hour);
  }

  // Test for scalar method when the time series has only one metric point
  #[test]
  fn test_scalar_single_metric_point() {
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 10.0,
    }];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Check if the scalar value is returned correctly when there is only one metric point
    assert_eq!(ts.scalar(), Some(10.0));
  }

  // Test for scalar method when the time series has multiple metric points
  #[test]
  fn test_scalar_multiple_metric_points() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Check if None is returned when the time series has multiple metric points
    assert_eq!(ts.scalar(), None);
  }

  // Test for sorting metric points in descending order by value
  #[test]
  fn test_sort_descending() {
    let mut metric_points = vec![
      MetricPoint {
        time: 1,
        value: 20.0,
      },
      MetricPoint {
        time: 2,
        value: 10.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    sort_desc(&mut metric_points);

    // Expected order after sorting: [30.0, 20.0, 10.0]
    let expected_sorted_values = vec![30.0, 20.0, 10.0];

    // Check if metric points are sorted in descending order by value
    assert_eq!(
      metric_points
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_sorted_values
    );
  }

  // Test for rounding metric points with precision specified
  #[test]
  fn test_round_with_precision() {
    let mut metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.12345,
      },
      MetricPoint {
        time: 2,
        value: 20.6789,
      },
    ];
    let precision = 2;
    round(&mut metric_points, Some(precision));

    // Expected rounded values with precision 2: [10.12, 20.68]
    let expected_rounded_values = vec![10.12, 20.68];

    // Check if metric points are rounded with the specified precision
    assert_eq!(
      metric_points
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_rounded_values
    );
  }

  // Test for rounding metric points without precision specified
  #[test]
  fn test_round_without_precision() {
    let mut metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.12345,
      },
      MetricPoint {
        time: 2,
        value: 20.6789,
      },
    ];
    round(&mut metric_points, None);

    // Expected rounded values without precision specified: [10.0, 21.0]
    let expected_rounded_values = vec![10.0, 21.0];

    // Check if metric points are rounded without precision specified (default behavior)
    assert_eq!(
      metric_points
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_rounded_values
    );
  }

  // Test for applying the sine function to each metric point's value
  #[test]
  fn test_sin() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint { time: 2, value: PI },
      MetricPoint {
        time: 3,
        value: 0.5 * PI,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());
    ts.sin();

    // Expected sine values: [0.0, 0.0, 1.0]
    let expected_sine_values = vec![0.0, 0.0, 1.0];

    // Check if sine function is applied correctly to each metric point's value
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_sine_values
    );
  }

  // Test for calculating the sum of metric points over time
  #[test]
  fn test_sum_over_time() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Expected sum of metric points: 10.0 + 20.0 + 30.0 = 60.0
    let expected_sum = 60.0;

    // Check if sum of metric points over time is calculated correctly
    assert_eq!(ts.sum_over_time(), expected_sum);
  }
  // Test for applying the hyperbolic tangent function to each metric point's value
  #[test]
  fn test_tanh() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: 1.0,
      },
      MetricPoint {
        time: 3,
        value: -1.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());
    ts.tanh();

    // Expected hyperbolic tangent values: [0.0, 0.761594, -0.761594]
    let expected_tanh_values = vec![0.0, 0.761594, -0.761594];

    // Check if hyperbolic tangent function is applied correctly to each metric point's value
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_tanh_values
    );
  }

  // Test for calculating the per-second rate of increase
  #[test]
  fn test_rate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Expected rate of increase: (30.0 - 10.0) / (3 - 1) = 10.0
    let expected_rate = 10.0;

    // Check if the per-second rate of increase is calculated correctly
    assert_eq!(ts.rate(), Some(expected_rate));
  }

  // Test for applying the sine function to each metric point's value
  #[test]
  fn test_sin() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: std::f64::consts::PI / 2.0,
      },
      MetricPoint {
        time: 3,
        value: std::f64::consts::PI,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());
    ts.sin();

    // Expected sine values: [0.0, 1.0, 0.0]
    let expected_sin_values = vec![0.0, 1.0, 0.0];

    // Check if sine function is applied correctly to each metric point's value
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_sin_values
    );
  }

  // Test for extracting and returning all timestamps from the metric points
  #[test]
  fn test_timestamp() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Extract timestamps from metric points
    let extracted_timestamps = ts.timestamp();

    // Check if timestamps are extracted correctly
    assert_eq!(extracted_timestamps, vec![1, 2, 3]);
  }

  // Test for applying the tangent function to each metric point's value
  #[test]
  fn test_tan() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: std::f64::consts::PI / 4.0,
      },
      MetricPoint {
        time: 3,
        value: std::f64::consts::PI / 3.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());
    ts.tan();

    // Expected tangent values: [0.0, 1.0, approximately 1.732]
    let expected_tan_values = vec![0.0, 1.0, (3.0 as f64).sqrt()];

    // Check if tangent function is applied correctly to each metric point's value
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_tan_values
    );
  }

  // Test for calculating the per-second rate of increase
  #[test]
  fn test_rate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
      MetricPoint {
        time: 4,
        value: 40.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the per-second rate of increase
    let rate = ts.rate();

    // Expected rate: (40.0 - 10.0) / (4 - 1) = 10.0
    assert_eq!(rate, Some(10.0));
  }

  // Test for applying the hyperbolic sine function to each metric point's value
  #[test]
  fn test_sinh() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: 1.0,
      },
      MetricPoint {
        time: 3,
        value: 2.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());
    ts.sinh();

    // Expected sinh values: [0.0, approximately 1.175, approximately 3.626]
    let expected_sinh_values = vec![0.0, 1.175, 3.626];

    // Check if sinh function is applied correctly to each metric point's value
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_sinh_values
    );
  }

  // Test for calculating the sum of metric points over time
  #[test]
  fn test_sum_over_time() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the sum of metric points over time
    let sum_over_time = ts.sum_over_time();

    // Expected sum: 10.0 + 20.0 + 30.0 = 60.0
    assert_eq!(sum_over_time, 60.0);
  }

  // Test for calculating the difference between consecutive metric points
  #[test]
  fn test_delta() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the difference between consecutive metric points
    let delta_values = ts.delta();

    // Expected delta values: [10.0, 10.0]
    assert_eq!(delta_values, vec![10.0, 10.0]);
  }

  // Test for calculating the derivative of the time series
  #[test]
  fn test_deriv() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the derivative of the time series
    let deriv_values = ts.deriv();

    // Expected derivative values: [10.0, 10.0]
    assert_eq!(deriv_values, vec![10.0, 10.0]);
  }

  // Test for calculating the sum of metric points over time
  #[test]
  fn test_sum_over_time() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the sum of metric points over time
    let sum = ts.sum_over_time();

    // Expected sum: 60.0
    assert_eq!(sum, 60.0);
  }

  // Test for calculating the minimum value over time
  #[test]
  fn test_min_over_time() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the minimum value over time
    let min = ts.min_over_time();

    // Expected minimum value: 10.0
    assert_eq!(min, Some(10.0));
  }

  // Test for calculating the maximum value over time
  #[test]
  fn test_max_over_time() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the maximum value over time
    let max = ts.max_over_time();

    // Expected maximum value: 30.0
    assert_eq!(max, Some(30.0));
  }

  // Test for calculating the average value over time
  #[test]
  fn test_avg_over_time() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the average value over time
    let avg = ts.avg();

    // Expected average value: 20.0
    assert_eq!(avg, 20.0);
  }

  // Test for calculating the count of metric points
  #[test]
  fn test_count() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Get the count of metric points
    let count = ts.count();

    // Expected count: 3
    assert_eq!(count, 3);
  }

  // Test for calculating the day of the year for each metric point's timestamp
  #[test]
  fn test_day_of_year() {
    let metric_points = vec![
      MetricPoint {
        time: 1613924400,
        value: 10.0,
      }, // 2021-02-21
      MetricPoint {
        time: 1621098000,
        value: 20.0,
      }, // 2021-05-16
      MetricPoint {
        time: 1626284400,
        value: 30.0,
      }, // 2021-07-15
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Calculate the day of the year for each metric point's timestamp
    let day_of_year = ts.day_of_year();

    // Expected day of the year: [52, 136, 196]
    assert_eq!(day_of_year, vec![52, 136, 196]);
  }

  // Test for forecasting future values based on linear regression
  #[test]
  fn test_predict_linear() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points.clone());

    // Forecast future value based on linear regression
    let forecast_value = ts.predict_linear(10.0); // Forecast for 10 seconds into the future

    // Expected forecast value: Some(40.0)
    assert_eq!(forecast_value, Some(40.0));
  }

  // Test for setting all metric points to a scalar value
  #[test]
  fn test_vector() {
    let mut ts = PromQLTimeSeries::new(HashMap::new(), create_test_metric_points());
    let scalar_value = 5.0;
    ts.vector(scalar_value);

    // Check if all metric points have been set to the scalar value
    assert!(ts
      .get_metric_points()
      .iter()
      .all(|mp| (mp.get_value() - scalar_value).abs() < f64::EPSILON));
  }

  // Test for determining the sign of each metric point's value
  #[test]
  fn test_sgn() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: -10.0,
      },
      MetricPoint {
        time: 2,
        value: 0.0,
      },
      MetricPoint {
        time: 3,
        value: 10.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.sgn();

    // Expected sign values: [-1.0, 0.0, 1.0]
    let expected_signs = vec![-1.0, 0.0, 1.0];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_signs
    );
  }

  // Test for computing the difference between consecutive metric points
  #[test]
  fn test_delta_edge_case() {
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 100.0,
    }];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Expecting an empty vector since there's only one metric point
    let delta_values = ts.delta();
    assert!(delta_values.is_empty());
  }

  // Test for applying the exponential function to each metric point's value
  #[test]
  fn test_exp_edge_case() {
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 0.0,
    }];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.exp();

    // The exponential of 0 is 1
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0);
  }

  // Test for applying the floor function to each metric point's value
  #[test]
  fn test_floor_edge_case() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: -1.7,
      },
      MetricPoint {
        time: 2,
        value: 2.3,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.floor();

    // Expected floor values: [-2.0, 2.0]
    let expected_floor_values = vec![-2.0, 2.0];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_floor_values
    );
  }

  // Test for the day_of_year method handling edge cases or leap years
  #[test]
  fn test_day_of_year_leap_year() {
    let metric_points = vec![
      MetricPoint {
        time: 1582934400,
        value: 10.0,
      }, // 2020-02-29
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Assuming the day_of_year method correctly handles leap years
    // Day of year for 2020-02-29 is 60
    let expected_day_of_year = vec![60];
    assert_eq!(ts.day_of_year(), expected_day_of_year);
  }

  // Test for correctly handling division by zero in the `irate` method
  #[test]
  fn test_irate_division_by_zero() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 100.0,
      },
      MetricPoint {
        time: 1,
        value: 110.0,
      }, // Same timestamp as previous
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);

    // Expecting None due to division by zero (timestamps are the same)
    assert_eq!(ts.irate(), None);
  }

  // Test for `quantile` method handling invalid quantiles
  #[test]
  fn test_quantile_invalid() {
    let ts = PromQLTimeSeries::new(HashMap::new(), create_test_metric_points());
    // Test with a quantile less than 0
    assert_eq!(ts.quantile(-0.1), None);
    // Test with a quantile greater than 1
    assert_eq!(ts.quantile(1.1), None);
  }

  // Test for `increase` method with no metric points
  #[test]
  fn test_increase_no_points() {
    let ts = PromQLTimeSeries::new(HashMap::new(), Vec::new());
    // Expecting None since there are no metric points
    assert_eq!(ts.increase(), None);
  }

  // Test for `predict_linear` method with insufficient points
  #[test]
  fn test_predict_linear_insufficient_points() {
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 10.0,
    }];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting None since there's only one metric point, which is insufficient for linear prediction
    assert_eq!(ts.predict_linear(60.0), None);
  }

  // Test for `sum` method with negative values
  #[test]
  fn test_sum_with_negative_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: -10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: -30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting the sum to correctly include negative values
    assert_eq!(ts.sum(), -20.0);
  }

  // Test for `avg` method with empty vector
  #[test]
  fn test_avg_empty_vector() {
    let ts = PromQLTimeSeries::new(HashMap::new(), Vec::new());
    // Expecting 0.0 since there are no metric points to average
    assert_eq!(ts.avg(), 0.0);
  }

  // Test for `stddev` method with constant values
  #[test]
  fn test_stddev_constant_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 5.0,
      },
      MetricPoint {
        time: 2,
        value: 5.0,
      },
      MetricPoint {
        time: 3,
        value: 5.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting 0.0 as standard deviation for a constant series
    assert_eq!(ts.stddev(), 0.0);
  }

  // Test for `max` and `min` methods with equal values
  #[test]
  fn test_max_min_equal_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 10.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting the same value for both max and min when all metric points have equal values
    assert_eq!(ts.max(), Some(10.0));
    assert_eq!(ts.min(), Some(10.0));
  }

  // Test for `label_replace` with non-matching regex
  #[test]
  fn test_label_replace_non_matching_regex() {
    let mut labels = HashMap::from([("env".to_string(), "production".to_string())]);
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 10.0,
    }];
    let mut ts = PromQLTimeSeries::new(labels.clone(), metric_points);
    ts.label_replace("new_env", "staging", "env", "^test$");
    // Expecting the original label to remain unchanged since the regex does not match
    assert_eq!(ts.labels.get("env").unwrap(), "production");
    assert!(!ts.labels.contains_key("new_env")); // "new_env" should not exist since there was no match
  }

  // Test for `vector` method with large scalar value
  #[test]
  fn test_vector_large_scalar() {
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.vector(1e9); // Set all values to a large scalar
                    // Check if all values are correctly set to the large scalar
    assert!(ts
      .get_metric_points()
      .iter()
      .all(|mp| mp.get_value() == 1e9));
  }

  // Test for `deriv` method with constant rate of change
  #[test]
  fn test_deriv_constant_rate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 1.0,
      },
      MetricPoint {
        time: 2,
        value: 2.0,
      },
      MetricPoint {
        time: 3,
        value: 3.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    let deriv_values = ts.deriv();
    // Expecting a constant rate of change of 1.0
    assert!(deriv_values
      .iter()
      .all(|&value| (value - 1.0).abs() < f64::EPSILON));
  }

  // Test for `exp` method with a mix of positive and negative values
  #[test]
  fn test_exp_mixed_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: -1.0,
      },
      MetricPoint {
        time: 3,
        value: 1.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.exp();
    // Check if exponential function is applied correctly, especially for negative values
    assert_eq!(ts.get_metric_points()[0].get_value(), 1.0); // exp(0) = 1
    assert!((ts.get_metric_points()[1].get_value() - f64::exp(-1.0)).abs() < f64::EPSILON); // exp(-1)
    assert!((ts.get_metric_points()[2].get_value() - f64::exp(1.0)).abs() < f64::EPSILON);
    // exp(1)
  }

  // Test for `deriv` method with constant rate of change
  #[test]
  fn test_deriv_constant_rate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // The derivative should be constant since the rate of change is uniform
    let deriv_values = ts.deriv();
    assert!(deriv_values
      .iter()
      .all(|&x| (x - 10.0).abs() < f64::EPSILON));
  }

  // Test for handling NaN values in `sum` method
  #[test]
  fn test_sum_with_nan_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: f64::NAN,
      },
      MetricPoint {
        time: 2,
        value: 20.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Summing with NaN should result in NaN
    assert!(ts.sum().is_nan());
  }

  // Test for `quantile` method with all equal values
  #[test]
  fn test_quantile_all_equal_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 10.0,
      },
      MetricPoint {
        time: 3,
        value: 10.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Quantile of a constant series should be the constant value
    assert_eq!(ts.quantile(0.5), Some(10.0));
  }

  // Test for `label_replace` with matching regex
  #[test]
  fn test_label_replace_matching_regex() {
    let mut labels = HashMap::from([("role".to_string(), "db-master".to_string())]);
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 10.0,
    }];
    let mut ts = PromQLTimeSeries::new(labels.clone(), metric_points);
    ts.label_replace("role", "db-slave", "role", "db-.*");
    // Expecting the label to be replaced since the regex matches
    assert_eq!(ts.labels.get("role").unwrap(), &"db-slave");
  }

  // Test for `exp` method with zero and negative values
  #[test]
  fn test_exp_zero_negative_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: -1.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.exp();
    // The exponential of 0 is 1, and of -1 is approximately 0.367879441
    let expected_values = vec![1.0, f64::consts::E.powf(-1.0)];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_values
    );
  }

  // Test for `vector` method to handle reset to zero
  #[test]
  fn test_vector_reset_to_zero() {
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.vector(0.0); // Reset all values to zero
                    // Check if all values are reset to zero
    assert!(ts
      .get_metric_points()
      .iter()
      .all(|mp| mp.get_value() == 0.0));
  }

  // Test for `deriv` method with constant rate of change
  #[test]
  fn test_deriv_constant_rate() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 0.0,
      },
      MetricPoint {
        time: 2,
        value: 2.0,
      },
      MetricPoint {
        time: 3,
        value: 4.0,
      },
      MetricPoint {
        time: 4,
        value: 6.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting a constant derivative since the rate of change is constant
    let deriv_values = ts.deriv();
    assert!(deriv_values.iter().all(|&v| (v - 2.0).abs() < f64::EPSILON));
  }

  // Test for `quantile_over_time` with a perfectly uniform distribution
  #[test]
  fn test_quantile_over_time_uniform_distribution() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 1.0,
      },
      MetricPoint {
        time: 2,
        value: 2.0,
      },
      MetricPoint {
        time: 3,
        value: 3.0,
      },
      MetricPoint {
        time: 4,
        value: 4.0,
      },
      MetricPoint {
        time: 5,
        value: 5.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Testing with the median quantile (0.5) in a uniform distribution
    assert_eq!(ts.quantile_over_time(0.5), Some(3.0));
  }

  // Test for `resets` method with no resets in the series
  #[test]
  fn test_resets_no_resets() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 1.0,
      },
      MetricPoint {
        time: 2,
        value: 2.0,
      },
      MetricPoint {
        time: 3,
        value: 3.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting 0 resets since the series is monotonically increasing
    assert_eq!(ts.resets(), 0);
  }

  // Test for `resets` method with multiple resets
  #[test]
  fn test_resets_multiple_resets() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 3.0,
      },
      MetricPoint {
        time: 2,
        value: 1.0,
      },
      MetricPoint {
        time: 3,
        value: 4.0,
      },
      MetricPoint {
        time: 4,
        value: 2.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Expecting 2 resets, corresponding to the drops from 3 to 1 and 4 to 2
    assert_eq!(ts.resets(), 2);
  }

  // Test for `vector` method to handle zero scalar value
  #[test]
  fn test_vector_zero_scalar() {
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.vector(0.0); // Set all values to zero
                    // Verify all metric points are set to zero
    assert!(ts
      .get_metric_points()
      .iter()
      .all(|mp| mp.get_value() == 0.0));
  }

  // Test for `log2` method with values that are powers of two
  #[test]
  fn test_log2_powers_of_two() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 2.0,
      },
      MetricPoint {
        time: 2,
        value: 4.0,
      },
      MetricPoint {
        time: 3,
        value: 8.0,
      },
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.log2();
    // Expected results are exact integers 1, 2, and 3 respectively
    let expected_values = vec![1.0, 2.0, 3.0];
    assert_eq!(
      ts.get_metric_points()
        .iter()
        .map(|mp| mp.get_value())
        .collect::<Vec<_>>(),
      expected_values
    );
  }

  // Test for `predict_linear` method with a downward trend
  #[test]
  fn test_predict_linear_downward_trend() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 2,
        value: 8.0,
      },
      MetricPoint {
        time: 3,
        value: 6.0,
      },
      MetricPoint {
        time: 4,
        value: 4.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Predicting a value 2 units of time into the future from the last timestamp
    let predicted_value = ts.predict_linear(2.0);
    // Expected to continue the downward trend, predicting the value to be 2.0 (linear extrapolation)
    assert_eq!(predicted_value, Some(2.0));
  }

  // Test aggregation functions on an empty series
  #[test]
  fn test_aggregations_on_empty_series() {
    let ts = PromQLTimeSeries::new(HashMap::new(), Vec::new());

    assert_eq!(ts.sum(), 0.0, "Sum of an empty series should be 0.");
    assert_eq!(ts.avg(), 0.0, "Average of an empty series should be 0.");
    assert!(ts.max().is_none(), "Max of an empty series should be None.");
    assert!(ts.min().is_none(), "Min of an empty series should be None.");
    assert_eq!(ts.count(), 0, "Count of an empty series should be 0.");
  }

  // Test for `exp` method under extreme values
  #[test]
  fn test_exp_extreme_values() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: -20.0,
      }, // A large negative value
      MetricPoint {
        time: 2,
        value: 0.0,
      }, // Zero
      MetricPoint {
        time: 3,
        value: 20.0,
      }, // A large positive value
    ];
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    ts.exp();
    // Verify the exponential function is applied correctly
    assert!(
      (ts.get_metric_points()[0].get_value() - f64::exp(-20.0)).abs() < 1e-9,
      "Exp of a large negative value should be close to 0."
    );
    assert_eq!(
      ts.get_metric_points()[1].get_value(),
      1.0,
      "Exp of 0 should be 1."
    );
    assert!(
      ts.get_metric_points()[2].get_value() > 1e8,
      "Exp of a large positive value should be a very large number."
    );
  }

  // Test for `rate` method with irregular time intervals
  #[test]
  fn test_rate_irregular_intervals() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 10.0,
      },
      MetricPoint {
        time: 3,
        value: 30.0,
      }, // 20 units increase over 2 seconds
      MetricPoint {
        time: 6,
        value: 60.0,
      }, // 30 units increase over 3 seconds
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // The average rate should be ((60-10) / (6-1)) = 10 units per second
    assert_eq!(
      ts.rate(),
      Some(10.0),
      "Rate calculation should correctly handle irregular time intervals."
    );
  }

  // Test for `quantile` method with a sorted series
  #[test]
  fn test_quantile_sorted_series() {
    let metric_points = vec![
      MetricPoint {
        time: 1,
        value: 1.0,
      },
      MetricPoint {
        time: 2,
        value: 2.0,
      },
      MetricPoint {
        time: 3,
        value: 3.0,
      },
      MetricPoint {
        time: 4,
        value: 4.0,
      },
      MetricPoint {
        time: 5,
        value: 5.0,
      },
    ];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Test various quantiles
    assert_eq!(
      ts.quantile(0.0),
      Some(1.0),
      "0th quantile should be the minimum value."
    );
    assert_eq!(
      ts.quantile(0.5),
      Some(3.0),
      "50th quantile should be the median value."
    );
    assert_eq!(
      ts.quantile(1.0),
      Some(5.0),
      "100th quantile should be the maximum value."
    );
  }

  // Test for `delta` method when the series contains a single point
  #[test]
  fn test_delta_single_point() {
    let metric_points = vec![MetricPoint {
      time: 1,
      value: 100.0,
    }];
    let ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    // Delta of a single-point series should be an empty vector
    assert!(
      ts.delta().is_empty(),
      "Delta of a single-point series should be empty."
    );
  }

  // Test for `vector` method resetting values to a specific constant
  #[test]
  fn test_vector_reset_to_constant() {
    let metric_points = create_test_metric_points();
    let mut ts = PromQLTimeSeries::new(HashMap::new(), metric_points);
    let reset_value = 42.0;
    ts.vector(reset_value);
    // Verify that all metric points are reset to the specified constant
    assert!(
      ts.get_metric_points()
        .iter()
        .all(|mp| mp.get_value() == reset_value),
      "All metric points should be reset to the specified constant."
    );
  }
}

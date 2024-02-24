// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

// TODO: Add error checking
// TODO: Histograms are not yet supported
use crate::index_manager::promql_time_series::PromQLTimeSeries;
use crate::metric::metric_point::MetricPoint;
use chrono::Utc;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum AggregationOperator<'a> {
  Sum,
  Min,
  Max,
  Avg,
  Stddev,
  Stdvar,
  Count,
  CountValues(&'a str),
  Bottomk(usize), // TODO: Check k is of type usize
  Topk(usize),    // TODO: Check k is of type usize
  Quantile(f64),  // TODO: Check phi is of type f64
  Undefined,
}

#[derive(Debug, Clone)]
pub enum FunctionOperator {
  // Trigonometric Functions
  Acos,
  Acosh,
  Asin,
  Asinh,
  Atan,
  Atanh,
  Cos,
  Cosh,
  Sin,
  Sinh,
  Tan,
  Tanh,

  // General Functions
  Abs,
  Absent,
  AbsentOverTime,
  Ceil,
  Clamp(f64, f64),
  ClampMax(f64),
  ClampMin(f64),
  Changes,
  DayOfMonth,
  DayOfWeek,
  DayOfYear,
  DaysInMonth,
  Deg,
  Delta,
  Deriv,
  Exp,
  Floor,
  HoltWinters(f64, f64),
  Hour,
  Idelta,
  Increase,
  Irate,
  LabelJoin(String, String, Vec<String>), // dst_label, separator, src_labels
  LabelReplace(String, String, String, String), // dst_label, replacement, src_label, regex
  Ln,
  Log2,
  Log10,
  Minute,
  Month,
  Negative,
  PredictLinear(f64),
  Rad,
  Rate,
  Resets,
  Round(f64),
  Scalar,
  Sgn,
  Sort,
  SortDesc,
  Sqrt,
  Timestamp,
  ConvertToVector(f64),
  Year,

  // Aggregations Over Time
  AvgOverTime,
  MinOverTime,
  MaxOverTime,
  SumOverTime,
  CountOverTime,
  QuantileOverTime(f64),
  StddevOverTime,
  StdvarOverTime,
  MadOverTime,
  LastOverTime,
  PresentOverTime,
}

#[derive(Debug, Clone)]
pub enum PromQLObjectType {
  Scalar,
  InstantVector,
  RangeVector,
  Undefined,
}

#[derive(Debug, Clone)]
pub struct PromQLObject {
  vector: Vec<PromQLTimeSeries>,
  scalar: f64,
  object_type: PromQLObjectType,
}

impl PromQLObject {
  /// Constructor
  pub fn new() -> Self {
    PromQLObject {
      vector: Vec::new(),
      scalar: 0.0,
      object_type: PromQLObjectType::Undefined,
    }
  }

  /// Constructor for scalar
  #[allow(dead_code)]
  pub fn new_as_scalar(value: f64) -> Self {
    PromQLObject {
      vector: Vec::new(),
      scalar: value,
      object_type: PromQLObjectType::Scalar,
    }
  }

  /// Constructor for instant and range vectors
  pub fn new_as_vector(vector: Vec<PromQLTimeSeries>) -> Self {
    let mut object = PromQLObject {
      vector,
      scalar: 0.0,
      object_type: PromQLObjectType::InstantVector,
    };
    object.update_object_type();
    object
  }

  /// Method to add a PromQLTimeSeries to the vector
  pub fn add_to_vector(&mut self, series: PromQLTimeSeries) {
    self.vector.push(series);
    self.update_object_type();
  }

  /// Adjusted method name to match naming convention
  pub fn update_object_type(&mut self) {
    if !self.vector.is_empty() {
      if self
        .vector
        .iter_mut()
        .all(|ts| ts.get_metric_points().len() == 1)
      {
        self.object_type = PromQLObjectType::InstantVector;
      } else {
        self.object_type = PromQLObjectType::RangeVector;
      }
    }
  }

  /// Getter for scalar type
  #[allow(dead_code)]
  pub fn is_scalar(&self) -> bool {
    matches!(self.object_type, PromQLObjectType::Scalar)
  }

  /// Getter for vector type
  pub fn is_vector(&self) -> bool {
    self.is_instant_vector() || self.is_range_vector()
  }

  /// Getter for instant vector type
  pub fn is_instant_vector(&self) -> bool {
    matches!(self.object_type, PromQLObjectType::InstantVector)
  }

  /// Getter for range vector type
  pub fn is_range_vector(&self) -> bool {
    matches!(self.object_type, PromQLObjectType::RangeVector)
  }

  /// Getter for vector
  pub fn get_vector(&self) -> &Vec<PromQLTimeSeries> {
    &self.vector
  }

  /// Take for vector - getter to allow the vector to be
  /// transferred out of the object and comply with Rust's ownership rules
  pub fn take_vector(&mut self) -> Vec<PromQLTimeSeries> {
    std::mem::take(&mut self.vector)
  }

  /// Setter for vector
  pub fn set_vector(&mut self, vector: Vec<PromQLTimeSeries>) {
    self.vector = vector;
    self.update_object_type();
  }

  /// Getter for scalar
  #[allow(dead_code)]
  pub fn get_scalar(&self) -> f64 {
    self.scalar
  }

  /// Setter for scalar
  pub fn set_scalar(&mut self, scalar: f64) {
    self.scalar = scalar;
    self.object_type = PromQLObjectType::Scalar;
  }

  // ******** Logical Operators: https://prometheus.io/docs/prometheus/latest/querying/operators/

  pub fn and(&mut self, other: &PromQLObject) {
    self.vector.retain(|self_ts| {
      other
        .vector
        .iter()
        .any(|other_ts| self_ts.get_labels() == other_ts.get_labels())
    });
  }

  pub fn or(&mut self, other: &PromQLObject) {
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

  pub fn unless(&mut self, other: &PromQLObject) {
    self.vector.retain(|self_ts| {
      !other
        .vector
        .iter()
        .any(|other_ts| self_ts.get_labels() == other_ts.get_labels())
    });
  }

  // **** Binary Operators: https://prometheus.io/docs/prometheus/latest/querying/operators/
  // apply to both scalars and vectors.

  pub fn apply_binary_operation<F: Fn(f64, f64) -> f64>(
    &mut self,
    other: &mut PromQLObject,
    op: F,
  ) {
    if !self.is_vector() || !other.is_vector() {
      // Scalar operations or scalar-vector operations
      if self.is_vector() {
        for ts in &mut self.vector {
          for mp in ts.get_metric_points() {
            mp.set_value(op(mp.get_value(), other.scalar));
          }
        }
      } else {
        self.scalar = op(self.scalar, other.scalar);
      }
    } else {
      // Vector-vector operations
      for self_ts in &mut self.vector {
        if let Some(other_ts) = other
          .vector
          .iter_mut()
          .find(|x| x.get_labels() == self_ts.get_labels())
        {
          for (i, self_mp) in &mut self_ts.get_metric_points().iter_mut().enumerate() {
            if let Some(other_mp) = other_ts.get_metric_points().get(i) {
              self_mp.set_value(op(self_mp.get_value(), other_mp.get_value()));
            }
          }
        }
      }
    }
  }

  pub fn equal(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if a == b { 1.0 } else { 0.0 });
  }
  pub fn not_equal(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if a != b { 1.0 } else { 0.0 });
  }
  pub fn greater_than(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if a > b { 1.0 } else { 0.0 });
  }
  pub fn less_than(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if a < b { 1.0 } else { 0.0 });
  }
  pub fn greater_than_or_equal(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if a >= b { 1.0 } else { 0.0 });
  }
  pub fn less_than_or_equal(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if a <= b { 1.0 } else { 0.0 });
  }
  pub fn add(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| a + b);
  }
  pub fn subtract(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| a - b);
  }
  pub fn multiply(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| a * b);
  }
  pub fn divide(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| if b != 0.0 { a / b } else { f64::NAN });
  }
  pub fn modulo(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| a % b);
  }
  pub fn power(&mut self, other: &mut PromQLObject) {
    self.apply_binary_operation(other, |a, b| a.powf(b));
  }

  // **** Aggregations: https://prometheus.io/docs/prometheus/latest/querying/operators/
  // apply only to vectors

  /// Helper function for aggregations
  pub fn apply_aggregation_operator(&mut self, operator: AggregationOperator) {
    match operator {
      AggregationOperator::Sum => self.sum(),
      AggregationOperator::Min => self.minimum(),
      AggregationOperator::Max => self.maximum(),
      AggregationOperator::Avg => self.avg(),
      AggregationOperator::Stddev => self.stddev(),
      AggregationOperator::Stdvar => self.stdvar(),
      AggregationOperator::Count => self.count(),
      AggregationOperator::CountValues(label_name) => self.count_values(label_name),
      AggregationOperator::Bottomk(k) => self.bottomk(k),
      AggregationOperator::Topk(k) => self.topk(k),
      AggregationOperator::Quantile(phi) => self.quantile(phi),
      AggregationOperator::Undefined => {} // TODO: throw an error here
    }
  }

  #[allow(dead_code)]
  pub fn sum(&mut self) {
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

  #[allow(dead_code)]
  pub fn minimum(&mut self) {
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

  #[allow(dead_code)]
  pub fn maximum(&mut self) {
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

  #[allow(dead_code)]
  pub fn avg(&mut self) {
    for ts in &mut self.vector {
      let sum: f64 = ts.get_metric_points().iter().map(|mp| mp.get_value()).sum();
      let avg = sum / ts.get_metric_points().len() as f64;
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        avg,
      )]);
    }
  }

  #[allow(dead_code)]
  pub fn group(&mut self) {
    for ts in &mut self.vector {
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        1.0,
      )]);
    }
  }

  #[allow(dead_code)]
  pub fn stddev(&mut self) {
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

  #[allow(dead_code)]
  pub fn stdvar(&mut self) {
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

  pub fn count(&mut self) {
    for ts in &mut self.vector {
      let len = ts.get_metric_points().len();
      ts.set_metric_points(vec![MetricPoint::new(
        Utc::now().timestamp().try_into().unwrap(),
        len as f64,
      )]);
    }
    self.update_object_type(); // Set to instant vector.
  }

  pub fn count_values(&mut self, label_key: &str) {
    // Initialize a HashMap to store label values and their counts
    let mut value_counts: HashMap<String, usize> = HashMap::new();

    // Iterate over each time series immutably
    for ts in &mut self.vector {
      let metric_points_count = ts.get_metric_points().len();
      if let Some(label_value) = ts.get_labels().get(label_key) {
        *value_counts.entry(label_value.clone()).or_insert(0) += metric_points_count;
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
      self.vector.push(PromQLTimeSeries::new_with_params(
        labels.clone(),
        metric_points,
      ));
    }
    self.update_object_type(); // Set to instant vector.
  }

  pub fn bottomk(&mut self, k: usize) {
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

  pub fn topk(&mut self, k: usize) {
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

  pub fn quantile(&mut self, phi: f64) {
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

  // **** Trigonometric Functions: https://prometheus.io/docs/prometheus/latest/querying/functions/
  // apply only to vectors

  pub fn apply_function_operator(&mut self, operator: FunctionOperator) {
    match operator {
      FunctionOperator::Acos => self.acos(),
      FunctionOperator::Acosh => self.acosh(),
      FunctionOperator::Asin => self.asin(),
      FunctionOperator::Asinh => self.asinh(),
      FunctionOperator::Atan => self.atan(),
      FunctionOperator::Atanh => self.atanh(),
      FunctionOperator::Cos => self.cos(),
      FunctionOperator::Cosh => self.cosh(),
      FunctionOperator::Sin => self.sin(),
      FunctionOperator::Sinh => self.sinh(),
      FunctionOperator::Tan => self.tan(),
      FunctionOperator::Tanh => self.tanh(),
      FunctionOperator::Abs => self.abs(),
      FunctionOperator::Absent => self.absent(),
      FunctionOperator::AbsentOverTime => self.absent_over_time(),
      FunctionOperator::Ceil => self.ceil(),
      FunctionOperator::Clamp(min, max) => self.clamp(min, max),
      FunctionOperator::ClampMax(max) => self.clamp_max(max),
      FunctionOperator::ClampMin(min) => self.clamp_min(min),
      FunctionOperator::Changes => self.changes(),
      FunctionOperator::DayOfMonth => self.day_of_month(),
      FunctionOperator::DayOfWeek => self.day_of_week(),
      FunctionOperator::DayOfYear => self.day_of_year(),
      FunctionOperator::DaysInMonth => self.days_in_month(),
      FunctionOperator::Deg => self.deg(),
      FunctionOperator::Delta => self.delta(),
      FunctionOperator::Deriv => self.deriv(),
      FunctionOperator::Exp => self.exp(),
      FunctionOperator::Floor => self.floor(),
      FunctionOperator::HoltWinters(alpha, beta) => self.holt_winters(alpha, beta),
      FunctionOperator::Hour => self.hour(),
      FunctionOperator::Idelta => self.idelta(),
      FunctionOperator::Increase => self.increase(),
      FunctionOperator::Irate => self.irate(),
      FunctionOperator::LabelJoin(dst_label, separator, src_labels) => {
        self.label_join(&dst_label, &separator, src_labels)
      }
      FunctionOperator::LabelReplace(dst_label, replacement, src_label, regex) => {
        self.label_replace(&dst_label, &replacement, &src_label, &regex)
      }
      FunctionOperator::Ln => self.ln(),
      FunctionOperator::Log2 => self.log2(),
      FunctionOperator::Log10 => self.log10(),
      FunctionOperator::Minute => self.minute(),
      FunctionOperator::Month => self.month(),
      FunctionOperator::Negative => self.negative(),
      FunctionOperator::PredictLinear(t) => self.predict_linear(t),
      FunctionOperator::Rad => self.rad(),
      FunctionOperator::Rate => self.rate(),
      FunctionOperator::Resets => self.resets(),
      FunctionOperator::Round(to_nearest) => self.round(to_nearest),
      FunctionOperator::Scalar => {
        let _ = self.scalar();
      } // Assuming scalar() returns a value that might not be used directly
      FunctionOperator::Sgn => self.sgn(),
      FunctionOperator::Sort => self.sort(),
      FunctionOperator::SortDesc => self.sort_desc(),
      FunctionOperator::Sqrt => self.sqrt(),
      FunctionOperator::Timestamp => self.timestamp(),
      FunctionOperator::ConvertToVector(s) => {
        let _ = self.convert_to_vector(s);
      } // Assuming convert_to_vector() returns a PromQLObject
      FunctionOperator::Year => self.year(),
      FunctionOperator::AvgOverTime => self.avg_over_time(),
      FunctionOperator::MinOverTime => self.min_over_time(),
      FunctionOperator::MaxOverTime => self.max_over_time(),
      FunctionOperator::SumOverTime => self.sum_over_time(),
      FunctionOperator::CountOverTime => self.count_over_time(),
      FunctionOperator::QuantileOverTime(quantile) => self.quantile_over_time(quantile),
      FunctionOperator::StddevOverTime => self.stddev_over_time(),
      FunctionOperator::StdvarOverTime => self.stdvar_over_time(),
      FunctionOperator::MadOverTime => self.mad_over_time(),
      FunctionOperator::LastOverTime => self.last_over_time(),
      FunctionOperator::PresentOverTime => self.present_over_time(),
    }
  }

  // Calculates the arccosine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn acos(&mut self) {
    for ts in &mut self.vector {
      ts.acos();
    }
  }

  // Calculates the inverse hyperbolic cosine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn acosh(&mut self) {
    for ts in &mut self.vector {
      ts.acosh();
    }
  }

  // Calculates the arcsine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn asin(&mut self) {
    for ts in &mut self.vector {
      ts.asin();
    }
  }

  // Calculates the inverse hyperbolic sine of all elements in v (special cases).
  #[allow(dead_code)]
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
  #[allow(dead_code)]
  pub fn atanh(&mut self) {
    for ts in &mut self.vector {
      ts.atanh();
    }
  }

  // Calculates the cosine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn cos(&mut self) {
    for ts in &mut self.vector {
      ts.cos();
    }
  }

  // Calculates the hyperbolic cosine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn cosh(&mut self) {
    for ts in &mut self.vector {
      ts.cosh();
    }
  }

  // Calculates the sine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn sin(&mut self) {
    for ts in &mut self.vector {
      ts.sin();
    }
  }

  // Calculates the hyperbolic sine of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn sinh(&mut self) {
    for ts in &mut self.vector {
      ts.sinh();
    }
  }

  // Calculates the tangent of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn tan(&mut self) {
    for ts in &mut self.vector {
      ts.tan();
    }
  }

  // Calculates the hyperbolic tangent of all elements in v (special cases).
  #[allow(dead_code)]
  pub fn tanh(&mut self) {
    for ts in &mut self.vector {
      ts.tanh();
    }
  }

  // **** Functions: https://prometheus.io/docs/prometheus/latest/querying/functions/
  // apply only to vectors

  // Applies the absolute value operation to every metric point in every time series
  #[allow(dead_code)]
  pub fn abs(&mut self) {
    for ts in &mut self.vector {
      ts.abs();
    }
  }

  /// Returns an empty vector if the vector has any elements, and 1 with the current time
  /// and a manufactured label if it doesn't.
  /// #[allow(dead_code)]
  pub fn absent(&mut self) {
    if self.vector.is_empty() {
      let mut labels = HashMap::new();
      labels.insert("absent".to_string(), "true".to_string());
      let absent_metric_point = MetricPoint::new(chrono::Utc::now().timestamp() as u64, 1.0);
      let absent_series = PromQLTimeSeries::new_with_params(labels, vec![absent_metric_point]);
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
      let absent_metric_point = MetricPoint::new(chrono::Utc::now().timestamp() as u64, 1.0);
      let absent_series = PromQLTimeSeries::new_with_params(labels, vec![absent_metric_point]);
      self.vector.push(absent_series);
    } else {
      self.vector.clear();
    }
  }

  // Applies the ceiling operation to every metric point in every time series
  #[allow(dead_code)]
  pub fn ceil(&mut self) {
    for ts in &mut self.vector {
      ts.ceil();
    }
    self.update_object_type();
  }

  // Clamps the min and max value of metric points for each time series
  #[allow(dead_code)]
  pub fn clamp(&mut self, min: f64, max: f64) {
    if min > max || min.is_nan() || max.is_nan() {
      self.vector.clear();
    } else {
      for ts in &mut self.vector {
        ts.clamp(min, max);
      }
    }
    self.update_object_type();
  }

  // Clamps the maximum value of metric points for each time series
  #[allow(dead_code)]
  pub fn clamp_max(&mut self, max: f64) {
    for ts in &mut self.vector {
      ts.clamp_max(max);
    }
    self.update_object_type();
  }

  // Clamps the minimum value of metric points for each time series
  #[allow(dead_code)]
  pub fn clamp_min(&mut self, min: f64) {
    for ts in &mut self.vector {
      ts.clamp_min(min);
    }
    self.update_object_type();
  }

  // Calculates changes in metric points for each time series
  #[allow(dead_code)]
  pub fn changes(&mut self) {
    for ts in &mut self.vector {
      ts.changes();
    }
    self.update_object_type();
  }

  // Calculates the day of the month for the metric points in each time series
  #[allow(dead_code)]
  pub fn day_of_month(&mut self) {
    for ts in &mut self.vector {
      ts.day_of_month();
    }
  }

  // Calculates the day of the week for the metric points in each time series
  #[allow(dead_code)]
  pub fn day_of_week(&mut self) {
    for ts in &mut self.vector {
      ts.day_of_week();
    }
  }

  // Calculates the day of the year for the metric points in each time series
  #[allow(dead_code)]
  pub fn day_of_year(&mut self) {
    for ts in &mut self.vector {
      ts.day_of_year();
    }
  }

  // Calculates the days in the month for the metric points in each time series
  #[allow(dead_code)]
  pub fn days_in_month(&mut self) {
    for ts in &mut self.vector {
      ts.days_in_month();
    }
  }

  // Converts degrees to radians for each time series element in the vector
  #[allow(dead_code)]
  pub fn deg(&mut self) {
    for ts in &mut self.vector {
      ts.deg();
    }
  }

  // Computes the difference between the first and last value of each time series element in the vector
  #[allow(dead_code)]
  pub fn delta(&mut self) {
    for ts in &mut self.vector {
      ts.delta();
    }
    self.update_object_type();
  }

  // Computes the derivative of each time series element in the vector
  #[allow(dead_code)]
  pub fn deriv(&mut self) {
    for ts in &mut self.vector {
      ts.deriv();
    }
    self.update_object_type();
  }

  // Computes the exponential function for each time series element in the vector
  #[allow(dead_code)]
  pub fn exp(&mut self) {
    for ts in &mut self.vector {
      ts.exp();
    }
  }

  // Applies the floor function to each time series element in the vector in place
  #[allow(dead_code)]
  pub fn floor(&mut self) {
    for ts in &mut self.vector {
      ts.floor();
    }
  }

  // Produces a smoothed value for time series based on the range
  #[allow(dead_code)]
  pub fn holt_winters(&mut self, alpha: f64, beta: f64) {
    for ts in &mut self.vector {
      ts.holt_winters(alpha, beta);
    }
  }

  // Returns the hour of the day for each of the given times in the metric points
  #[allow(dead_code)]
  pub fn hour(&mut self) {
    for ts in &mut self.vector {
      ts.hour();
    }
  }

  #[allow(dead_code)]
  pub fn idelta(&mut self) {
    for ts in &mut self.vector {
      ts.idelta();
    }
    self.update_object_type();
  }

  // Computes the total increase over time for each time series
  #[allow(dead_code)]
  pub fn increase(&mut self) {
    for ts in &mut self.vector {
      ts.increase();
    }
    self.update_object_type();
  }

  // Computes the instantaneous rate of change for each time series
  #[allow(dead_code)]
  pub fn irate(&mut self) {
    for ts in &mut self.vector {
      ts.irate();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn label_join(&mut self, dst_label: &str, separator: &str, src_labels: Vec<String>) {
    for ts in &mut self.vector {
      ts.label_join(dst_label, separator, &src_labels);
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
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
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn ln(&mut self) {
    for ts in &mut self.vector {
      ts.ln();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn log2(&mut self) {
    for ts in &mut self.vector {
      ts.log2();
    }
  }

  #[allow(dead_code)]
  pub fn log10(&mut self) {
    for ts in &mut self.vector {
      ts.log10();
    }
  }

  #[allow(dead_code)]
  pub fn minute(&mut self) {
    for ts in &mut self.vector {
      ts.minute();
    }
  }

  #[allow(dead_code)]
  pub fn month(&mut self) {
    for ts in &mut self.vector {
      ts.month();
    }
  }

  #[allow(dead_code)]
  pub fn negative(&mut self) {
    for ts in &mut self.vector {
      ts.negative();
    }
  }

  #[allow(dead_code)]
  pub fn predict_linear(&mut self, t: f64) {
    for ts in &mut self.vector {
      ts.predict_linear(t);
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn rad(&mut self) {
    for ts in &mut self.vector {
      ts.rad();
    }
  }

  #[allow(dead_code)]
  pub fn rate(&mut self) {
    for ts in &mut self.vector {
      ts.rate();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn resets(&mut self) {
    for ts in &mut self.vector {
      ts.resets();
    }
  }

  #[allow(dead_code)]
  pub fn round(&mut self, to_nearest: f64) {
    for ts in &mut self.vector {
      ts.round(to_nearest);
    }
  }

  #[allow(dead_code)]
  pub fn scalar(&mut self) -> f64 {
    if self.vector.len() == 1 {
      if let Some(metric_point) = self.vector[0].get_metric_points().first() {
        return metric_point.get_value();
      }
    }
    f64::NAN
  }

  // Determines the sign of each metric point's value in every time series
  #[allow(dead_code)]
  pub fn sgn(&mut self) {
    for ts in &mut self.vector {
      ts.sgn();
    }
  }

  #[allow(dead_code)]
  pub fn sort(&mut self) {
    for ts in &mut self.vector {
      ts.sort();
    }
  }

  #[allow(dead_code)]
  pub fn sort_desc(&mut self) {
    for ts in &mut self.vector {
      ts.sort_desc();
    }
  }

  #[allow(dead_code)]
  pub fn sqrt(&mut self) {
    for ts in &mut self.vector {
      ts.sqrt();
    }
  }

  #[allow(dead_code)]
  pub fn timestamp(&mut self) {
    for ts in &mut self.vector {
      ts.timestamp();
    }
  }

  #[allow(dead_code)]
  pub fn convert_to_vector(&self, s: f64) -> PromQLObject {
    let current_time = Utc::now().timestamp();
    PromQLObject::new_as_vector(vec![PromQLTimeSeries::new_with_params(
      HashMap::new(),
      vec![MetricPoint::new(current_time as u64, s)],
    )])
  }

  #[allow(dead_code)]
  pub fn year(&mut self) {
    for ts in &mut self.vector {
      ts.year();
    }
  }

  // **** Aggregations over time ****

  #[allow(dead_code)]
  pub fn avg_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.avg_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn min_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.min_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn max_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.max_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn sum_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.sum_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn count_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.count_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn quantile_over_time(&mut self, quantile: f64) {
    for ts in &mut self.vector {
      ts.quantile_over_time(quantile);
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn stddev_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.stddev_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn stdvar_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.stdvar_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn mad_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.mad_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn last_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.last_over_time();
    }
    self.update_object_type();
  }

  #[allow(dead_code)]
  pub fn present_over_time(&mut self) {
    for ts in &mut self.vector {
      ts.present_over_time();
    }
    self.update_object_type();
  }
}

use std::cmp::Ordering;

impl PartialEq for PromQLObject {
  fn eq(&self, other: &Self) -> bool {
    self.vector == other.vector
  }
}

impl Eq for PromQLObject {}

impl PartialOrd for PromQLObject {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.vector.len().cmp(&other.vector.len()))
  }
}

impl Ord for PromQLObject {
  fn cmp(&self, other: &Self) -> Ordering {
    self.vector.len().cmp(&other.vector.len())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn create_metric_points_with_times(times: &[u64], values: &[f64]) -> Vec<MetricPoint> {
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

  fn create_time_series(values: Vec<f64>, timestamp: u64) -> PromQLTimeSeries {
    let metric_points = values
      .iter()
      .map(|&value| MetricPoint::new(timestamp, value))
      .collect();
    PromQLTimeSeries::new_with_params(HashMap::new(), metric_points)
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_abs() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![-1.0, 2.0, -3.0], 0),
      create_time_series(vec![4.0, -5.0], 0),
    ]);

    vector.abs();

    let expected = [1.0, 2.0, 3.0, 4.0, 5.0];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert_eq!(mp.get_value(), expected[j]);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_round() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.234, 2.345], 0),
      create_time_series(vec![3.456, 4.567], 0),
    ]);

    vector.round(0.01);

    let expected = [1.23, 2.35, 3.46, 4.57];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert!((mp.get_value() - expected[j]) < f64::EPSILON);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_acos() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![0.0, 0.5, 1.0], 0),
      create_time_series(vec![0.5, 1.0], 0),
    ]);

    vector.acos();

    let expected = [
      std::f64::consts::PI / 2.0,
      std::f64::consts::PI / 3.0,
      0.0,
      std::f64::consts::PI / 3.0,
      0.0,
    ];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert!((mp.get_value() - expected[j]) < f64::EPSILON);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_asin() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![0.0, 0.5, 1.0], 0),
      create_time_series(vec![0.5, 1.0], 0),
    ]);

    vector.asin();

    let expected = [
      0.0,
      std::f64::consts::PI / 6.0,
      std::f64::consts::PI / 2.0,
      std::f64::consts::PI / 6.0,
      std::f64::consts::PI / 2.0,
    ];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert!((mp.get_value() - expected[j]) < f64::EPSILON);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_min_over_time() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 2.0, 3.0], 0),
      create_time_series(vec![4.0, 5.0], 0),
    ]);

    vector.min_over_time();

    let expected = [1.0, 4.0];

    let mut j = 0;
    for ts in &mut vector.vector {
      assert_eq!(ts.get_metric_points()[0].get_value(), expected[j]);
      j += 1;
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_max_over_time() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 2.0, 3.0], 0),
      create_time_series(vec![4.0, 5.0], 0),
    ]);

    vector.max_over_time();

    let expected = [3.0, 5.0];

    let mut j = 0;
    for ts in &mut vector.vector {
      assert_eq!(ts.get_metric_points()[0].get_value(), expected[j]);
      j += 1;
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_ceil() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.ceil();

    let expected = [2.0, 3.0, 4.0, 5.0, -5.0];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert_eq!(mp.get_value(), expected[j]);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_clamp_max() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.clamp_max(3.0);

    let expected = [1.2, 2.7, 3.0, 3.0, -5.2];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert_eq!(mp.get_value(), expected[j]);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_clamp_min() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.2, 2.7, 3.5], 0),
      create_time_series(vec![4.8, -5.2], 0),
    ]);

    vector.clamp_min(2.0);

    let expected = [2.0, 2.7, 3.5, 4.8, 2.0];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert_eq!(mp.get_value(), expected[j]);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_changes() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 1.0, 2.0, 3.0, 3.0], 0),
      create_time_series(vec![1.0, 1.0, 1.0, 2.0], 0),
    ]);

    vector.changes();

    let expected = [2, 1]; // Number of changes, not value differences

    let mut j = 0;
    for ts in &mut vector.vector {
      assert_eq!(ts.get_metric_points()[0].get_value() as usize, expected[j]);
      j += 1;
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_delta() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    vector.delta();

    let expected = [9.0, 7.0]; // Delta is the difference between the first and last values

    let mut j = 0;
    for ts in &mut vector.vector {
      assert_eq!(ts.get_metric_points()[0].get_value(), expected[j]);
      j += 1;
    }
  }

  #[test]
  // TODO: Not sure what the timestamps are supposed to be after function completes
  fn test_deriv() {
    let metric_points1 = create_metric_points_with_times(&[1, 2, 3], &[10.0, 20.0, 30.0]);
    let metric_points2 = create_metric_points_with_times(&[2, 4, 6], &[20.0, 40.0, 60.0]);
    let ts1 = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points1);
    let ts2 = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points2);

    let mut vector = PromQLObject::new_as_vector(vec![ts1, ts2]);

    vector.deriv();

    let expected = PromQLObject::new_as_vector(vec![
      create_time_series(vec![5.0], 3),
      create_time_series(vec![2.5], 6),
    ]);

    assert_eq!(vector, expected);
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  // TODO pretty sure the return values should not be Some()
  fn test_increase() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    vector.increase();

    let expected = [Some(9.0), Some(7.0)];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert_eq!(Some(mp.get_value()), expected[j]);
        j += 1;
      }
    }
  }

  #[test]
  fn test_idelta() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    vector.idelta();

    let expected = PromQLObject::new_as_vector(vec![
      create_time_series(vec![4.0], 0),
      create_time_series(vec![4.0], 0),
    ]);

    assert_eq!(vector, expected);
  }

  #[test]
  // TODO: Not sure what the timestamps are supposed to be after function completes
  fn test_irate() {
    let metric_points1 = create_metric_points_with_times(&[1, 2, 4], &[100.0, 110.0, 130.0]);
    let metric_points2 = create_metric_points_with_times(&[1, 5, 10], &[20.0, 50.0, 80.0]);
    let ts1 = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points1);
    let ts2 = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points2);

    let mut vector = PromQLObject::new_as_vector(vec![ts1, ts2]);

    vector.irate();

    let expected = PromQLObject::new_as_vector(vec![
      create_time_series(vec![10.0], 4),
      create_time_series(vec![6.0], 10),
    ]);

    assert_eq!(vector, expected);
  }

  #[test]
  // TODO: Not sure what the timestamps are supposed to be after function completes
  fn test_rate() {
    let metric_points1 = create_metric_points_with_times(&[1, 2, 5], &[100.0, 110.0, 130.0]);
    let metric_points2 = create_metric_points_with_times(&[1, 5, 13], &[20.0, 50.0, 80.0]);
    let ts1 = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points1);
    let ts2 = PromQLTimeSeries::new_with_params(HashMap::new(), metric_points2);

    let mut vector = PromQLObject::new_as_vector(vec![ts1, ts2]);

    vector.rate();

    let expected = PromQLObject::new_as_vector(vec![
      create_time_series(vec![7.5], 5),
      create_time_series(vec![5.0], 13),
    ]);

    assert_eq!(vector, expected);
  }

  #[test]
  fn test_absent() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, 3.0, 6.0, 10.0], 0),
      create_time_series(vec![], 0),
      create_time_series(vec![5.0, 8.0, 12.0], 0),
    ]);

    vector.absent();

    let expected = [false, true, false];

    for (i, ts) in vector.vector.iter().enumerate() {
      assert_eq!(ts.is_empty(), expected[i]);
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_sgn() {
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![-10.0, 8.0, 0.0, 15.0, -18.0], 0),
      create_time_series(vec![5.0, -6.0, -6.0, 5.0], 0),
    ]);

    vector.sgn();

    let expected = [-1.0, 1.0, 0.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0];

    let mut j = 0;
    for ts in &mut vector.vector {
      for mp in ts.get_metric_points() {
        assert_eq!(mp.get_value(), expected[j]);
        j += 1;
      }
    }
  }

  #[test]
  #[allow(clippy::explicit_counter_loop)]
  fn test_non_nan_count() {
    // Assume vector and expected setup
    let mut vector = PromQLObject::new_as_vector(vec![
      create_time_series(vec![1.0, f64::NAN, 3.0], 0),
      create_time_series(vec![f64::NAN, 5.0], 0),
    ]);

    // Expected counts of non-NaN values for each time series
    let expected = [2, 1]; // Assuming the first series has 2 non-NaN and the second has 1

    let mut i = 0;
    for ts in &mut vector.vector {
      let non_nan_count = ts
        .get_metric_points()
        .iter()
        .filter(|mp| !mp.get_value().is_nan())
        .count();
      assert_eq!(
        non_nan_count, expected[i],
        "Mismatch in series at index {}: expected {}, got {}",
        i, expected[i], non_nan_count
      );
      i += 1;
    }
  }
}

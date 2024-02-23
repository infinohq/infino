// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Execute an Infino metrics query. PromQL is supported.
//!
//! Uses the Pest parser with Pest-formatted PEG grammars: https://pest.rs/
//! which validates syntax.
//!
//! Walks the AST via an iterator. We maintain our own stack, processing
//! Infino-supported nodes as they are popped off the stack and pushing children of
//! transitory nodes onto the stack for further processing.

use crate::index_manager::promql_object::{AggregationOperator, PromQLObject};
use crate::metric::constants::{MetricsQueryCondition, METRIC_NAME_PREFIX};
use crate::{index_manager::index::Index, metric::metric_point::MetricPoint};

use crate::utils::error::{AstError, SearchMetricsError};
use chrono::{Duration, Utc};
use pest::iterators::{Pair, Pairs};
use std::collections::{HashMap, VecDeque};

use log::debug;

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

use super::promql_object::FunctionOperator;
use super::promql_time_series::PromQLTimeSeries;

#[derive(Parser)]
#[grammar = "src/index_manager/promql_grammar.pest"]

pub struct PromQLParser;

#[derive(Debug)]
pub struct PromQLSelector {
  label_name: String,
  label_value: String,
  condition: MetricsQueryCondition,
}

impl PromQLSelector {
  pub fn new() -> Self {
    PromQLSelector {
      label_name: String::new(),
      label_value: String::new(),
      condition: MetricsQueryCondition::Undefined,
    }
  }

  pub fn new_with_params(
    label_name: String,
    label_value: String,
    condition: MetricsQueryCondition,
  ) -> Self {
    PromQLSelector {
      label_name,
      label_value,
      condition,
    }
  }

  pub fn get_label_name(&self) -> &str {
    &self.label_name
  }

  pub fn set_label_name(&mut self, label_name: String) {
    self.label_name = label_name;
  }

  pub fn get_label_value(&self) -> &str {
    &self.label_value
  }

  pub fn set_label_value(&mut self, label_value: String) {
    self.label_value = label_value;
  }

  pub fn get_condition(&self) -> &MetricsQueryCondition {
    &self.condition
  }

  pub fn set_condition(&mut self, condition: MetricsQueryCondition) {
    self.condition = condition;
  }
}

impl Default for PromQLSelector {
  fn default() -> Self {
    Self::new()
  }
}

#[derive(Debug)]
pub struct PromQLDuration {
  start_time: u64,
  end_time: u64,
  offset: u64,
}

impl PromQLDuration {
  /// Creates a new `PromQLDuration` with `start_time` at UNIX epoch start and `end_time` as now.
  pub fn new() -> Self {
    // Default to an instant vector, which uses now as the timestamp
    PromQLDuration {
      start_time: 0,
      end_time: Utc::now().timestamp() as u64,
      offset: 0,
    }
  }

  pub fn new_with_params(range_start_time: u64, range_end_time: u64, range_offset: u64) -> Self {
    PromQLDuration {
      start_time: range_start_time,
      end_time: range_end_time,
      offset: range_offset,
    }
  }

  pub fn get_start_time(&self) -> u64 {
    self.start_time
  }

  pub fn set_start_time(&mut self, start_time: u64) {
    self.start_time = start_time;
  }

  pub fn get_end_time(&self) -> u64 {
    self.end_time
  }

  pub fn set_end_time(&mut self, end_time: u64) {
    self.end_time = end_time;
  }

  /// Sets the duration based on a Prometheus duration string (e.g., "2h", "15m").
  pub fn set_duration(&mut self, duration_str: &str) -> Result<(), AstError> {
    let duration = Self::parse_duration(duration_str)?;
    self.end_time = Utc::now().timestamp() as u64;
    self.start_time = self.end_time.saturating_sub(duration.num_seconds() as u64);

    Ok(())
  }

  // Assuming AstError has a variant like InvalidInput(String) or similar
  fn parse_duration(s: &str) -> Result<Duration, AstError> {
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

  pub fn set_offset(&mut self, offset_str: &str) -> Result<(), AstError> {
    let duration = Self::parse_duration(offset_str)?;
    self.offset = duration.num_seconds() as u64;
    Ok(())
  }

  /// Offsets the duration based on a Prometheus-style duration string (e.g., "2h", "15m").
  pub fn adjust_by_offset(&mut self) {
    self.start_time = self.start_time.saturating_add(self.offset);
    self.end_time = self.end_time.saturating_add(self.offset);
  }
}

impl Default for PromQLDuration {
  fn default() -> Self {
    Self::new()
  }
}

impl Index {
  pub fn parse_query(
    json_query: &str,
  ) -> Result<pest::iterators::Pairs<'_, Rule>, SearchMetricsError> {
    PromQLParser::parse(Rule::start, json_query)
      .map_err(|e| SearchMetricsError::JsonParseError(e.to_string()))
  }

  /// Walk the AST using an iterator and process each node
  pub async fn traverse_promql_ast(
    &self,
    nodes: &Pairs<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<PromQLTimeSeries>, AstError> {
    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = PromQLObject::new();

    // Pop the nodes off the stack and process
    while let Some(node) = stack.pop_front() {
      let processing_result = self.query_dispatcher(&node, range_start_time, range_end_time);

      match processing_result.await {
        Ok(mut node_results) => {
          results.set_vector(node_results.take_vector());
        }
        Err(AstError::UnsupportedQuery(_)) => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
        Err(e) => {
          return Err(e);
        }
      }
    }

    Ok(results.take_vector())
  }

  /// General dispatcher for query processing
  async fn query_dispatcher(
    &self,
    node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    match node.as_rule() {
      Rule::expression => {
        self
          .process_expression(node, range_start_time, range_end_time)
          .await
      }
      _ => Err(AstError::UnsupportedQuery(format!(
        "Unsupported rule: {:?}",
        node.as_rule()
      ))),
    }
  }

  async fn process_expression(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut and_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::and_expression => {
          and_results = self
            .process_and_expression(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(and_results.take_vector());
          }
        }
        Rule::or => {
          results.or(&and_results);
        }
        Rule::unless => {
          results.unless(&and_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_and_expression(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut equality_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::equality => {
          equality_results = self
            .process_equality(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(equality_results.take_vector());
          }
        }
        Rule::and => {
          results.and(&equality_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_equality(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut comparison_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::comparison => {
          comparison_results = self
            .process_comparison(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(comparison_results.take_vector());
          }
        }
        Rule::equal => {
          results.equal(&mut comparison_results);
        }
        Rule::not_equal => {
          results.not_equal(&mut comparison_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_comparison(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut term_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::term => {
          term_results = self
            .process_term(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.take_vector());
          }
        }
        Rule::greater_than => {
          results.greater_than(&mut term_results);
        }
        Rule::less_than => {
          results.less_than(&mut term_results);
        }
        Rule::greater_than_or_equal => {
          results.greater_than_or_equal(&mut term_results);
        }
        Rule::less_than_or_equal => {
          results.less_than_or_equal(&mut term_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_term(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut factor_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::factor => {
          factor_results = self
            .process_factor(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(factor_results.take_vector());
          }
        }
        Rule::plus => {
          results.add(&mut factor_results);
        }
        Rule::minus => {
          results.subtract(&mut factor_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_factor(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut exponent_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::exponent => {
          exponent_results = self
            .process_exponent(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.take_vector());
          }
        }
        Rule::multiply => {
          results.multiply(&mut exponent_results);
        }
        Rule::divide => {
          results.divide(&mut exponent_results);
        }
        Rule::modulo => {
          results.modulo(&mut exponent_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_exponent(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut unary_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::unary => {
          unary_results = self
            .process_unary(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(unary_results.take_vector());
          }
        }
        Rule::power => {
          results.power(&mut unary_results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_unary(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::negative => {
          results.negative();
        }
        Rule::leaf => {
          let mut leaf_results = self
            .process_leaf(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(leaf_results.take_vector());
          }
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_leaf(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::scalar => {
          if let Some(scalar) = node.into_inner().next() {
            let scalar_text: &str = scalar.as_str();
            let float: Result<f64, _> = scalar_text.parse();
            match float {
              Ok(number) => {
                results.set_scalar(number);
              }
              Err(_) => {
                return Err(AstError::UnsupportedQuery(
                  "Could not convert scalar text to float for processing".to_string(),
                ))
              }
            }
          }
        }
        Rule::vector => {
          let mut promql_object = self
            .process_vector(&node, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        Rule::aggregations => {
          let mut promql_object = self
            .process_aggregations(&node, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        Rule::functions => {
          let mut promql_object = self
            .process_functions(&node, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_aggregations(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut operator = AggregationOperator::Undefined;

    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::vector => {
          let mut promql_object = self
            .process_vector(&node, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        Rule::sum => operator = AggregationOperator::Sum,
        Rule::avg => operator = AggregationOperator::Avg,
        Rule::max => operator = AggregationOperator::Max,
        Rule::min => operator = AggregationOperator::Min,
        Rule::count => operator = AggregationOperator::Count,
        Rule::count_values => {
          if let Some(label) = node.clone().into_inner().next() {
            let label_name: &str = label.as_str();
            operator = AggregationOperator::CountValues(label_name);
          }
        }
        Rule::quantile => {
          if let Some(parameter) = node.clone().into_inner().next() {
            if let Ok(phi_float) = parameter.as_str().parse::<f64>() {
              operator = AggregationOperator::Quantile(phi_float);
            }
          }
        }
        Rule::stddev => operator = AggregationOperator::Stddev,
        Rule::stdvar => operator = AggregationOperator::Stdvar,
        Rule::topk | Rule::bottomk => {
          if let Some(parameter) = node.clone().into_inner().next() {
            if let Ok(k_int) = parameter.as_str().parse::<usize>() {
              operator = if node.as_rule() == Rule::topk {
                AggregationOperator::Topk(k_int)
              } else {
                AggregationOperator::Bottomk(k_int)
              };
            }
          }
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    results.apply_aggregation_operator(operator);

    Ok(results)
  }

  async fn process_functions(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut operator = FunctionOperator::Absent; // Default or placeholder

    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::vector => {
          let mut promql_object = self
            .process_vector(&node, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        Rule::acos => operator = FunctionOperator::Acos,
        Rule::acosh => operator = FunctionOperator::Acosh,
        Rule::asin => operator = FunctionOperator::Asin,
        Rule::asinh => operator = FunctionOperator::Asinh,
        Rule::atan => operator = FunctionOperator::Atan,
        Rule::atanh => operator = FunctionOperator::Atanh,
        Rule::cos => operator = FunctionOperator::Cos,
        Rule::cosh => operator = FunctionOperator::Cosh,
        Rule::sin => operator = FunctionOperator::Sin,
        Rule::sinh => operator = FunctionOperator::Sinh,
        Rule::tan => operator = FunctionOperator::Tan,
        Rule::tanh => operator = FunctionOperator::Tanh,
        Rule::abs => operator = FunctionOperator::Abs,
        Rule::absent => operator = FunctionOperator::Absent,
        Rule::absent_over_time => operator = FunctionOperator::AbsentOverTime,
        Rule::ceil => operator = FunctionOperator::Ceil,
        Rule::clamp => {
          let (min, max) = self.parse_double_params(&node);
          operator = FunctionOperator::Clamp(min, max);
        }
        Rule::clamp_max => {
          let max = self.parse_single_param(&node);
          operator = FunctionOperator::ClampMax(max);
        }
        Rule::clamp_min => {
          let min = self.parse_single_param(&node);
          operator = FunctionOperator::ClampMin(min);
        }
        Rule::changes => operator = FunctionOperator::Changes,
        Rule::day_of_month => operator = FunctionOperator::DayOfMonth,
        Rule::day_of_week => operator = FunctionOperator::DayOfWeek,
        Rule::day_of_year => operator = FunctionOperator::DayOfYear,
        Rule::days_in_month => operator = FunctionOperator::DaysInMonth,
        Rule::deg => operator = FunctionOperator::Deg,
        Rule::delta => operator = FunctionOperator::Delta,
        Rule::deriv => operator = FunctionOperator::Deriv,
        Rule::exp => operator = FunctionOperator::Exp,
        Rule::floor => operator = FunctionOperator::Floor,
        Rule::holt_winters => {
          let (alpha, beta) = self.parse_double_params(&node);
          operator = FunctionOperator::HoltWinters(alpha, beta);
        }
        Rule::hour => operator = FunctionOperator::Hour,
        Rule::idelta => operator = FunctionOperator::Idelta,
        Rule::increase => operator = FunctionOperator::Increase,
        Rule::irate => operator = FunctionOperator::Irate,
        Rule::label_join => {
          let (dst_label, separator, src_labels) = self.parse_label_join_params(&node);
          operator = FunctionOperator::LabelJoin(dst_label, separator, src_labels);
        }
        Rule::label_replace => {
          let (dst_label, replacement, src_label, regex) = self.parse_label_replace_params(&node);
          operator = FunctionOperator::LabelReplace(dst_label, replacement, src_label, regex);
        }
        Rule::ln => operator = FunctionOperator::Ln,
        Rule::log2 => operator = FunctionOperator::Log2,
        Rule::log10 => operator = FunctionOperator::Log10,
        Rule::minute => operator = FunctionOperator::Minute,
        Rule::month => operator = FunctionOperator::Month,
        Rule::negative => operator = FunctionOperator::Negative,
        Rule::predict_linear => {
          let t = self.parse_single_param(&node);
          operator = FunctionOperator::PredictLinear(t);
        }
        Rule::rad => operator = FunctionOperator::Rad,
        Rule::rate => operator = FunctionOperator::Rate,
        Rule::resets => operator = FunctionOperator::Resets,
        Rule::round => {
          let to_nearest = self.parse_single_param(&node);
          operator = FunctionOperator::Round(to_nearest);
        }
        Rule::scalar_convert => operator = FunctionOperator::Scalar,
        Rule::sgn => operator = FunctionOperator::Sgn,
        Rule::sort => operator = FunctionOperator::Sort,
        Rule::sort_desc => operator = FunctionOperator::SortDesc,
        Rule::sqrt => operator = FunctionOperator::Sqrt,
        Rule::timestamp => operator = FunctionOperator::Timestamp,
        Rule::vector_convert => {
          let s = self.parse_single_param(&node);
          operator = FunctionOperator::ConvertToVector(s);
        }
        Rule::year => operator = FunctionOperator::Year,
        Rule::avg_over_time => operator = FunctionOperator::AvgOverTime,
        Rule::min_over_time => operator = FunctionOperator::MinOverTime,
        Rule::max_over_time => operator = FunctionOperator::MaxOverTime,
        Rule::sum_over_time => operator = FunctionOperator::SumOverTime,
        Rule::count_over_time => operator = FunctionOperator::CountOverTime,
        Rule::quantile_over_time => {
          let quantile = self.parse_single_param(&node);
          operator = FunctionOperator::QuantileOverTime(quantile);
        }
        Rule::stddev_over_time => operator = FunctionOperator::StddevOverTime,
        Rule::stdvar_over_time => operator = FunctionOperator::StdvarOverTime,
        Rule::mad_over_time => operator = FunctionOperator::MadOverTime,
        Rule::last_over_time => operator = FunctionOperator::LastOverTime,
        Rule::present_over_time => operator = FunctionOperator::PresentOverTime,
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    results.apply_function_operator(operator);

    Ok(results)
  }

  // Helper functions for PromQL function parameter processing.
  // We clone the nodes but the nodes should be small.

  fn parse_single_param(&self, node: &Pair<'_, Rule>) -> f64 {
    node
      .clone()
      .into_inner()
      .next()
      .unwrap()
      .as_str()
      .parse::<f64>()
      .unwrap()
  }

  fn parse_double_params(&self, node: &Pair<'_, Rule>) -> (f64, f64) {
    let mut params = node.clone().into_inner();
    let param1 = params.next().unwrap().as_str().parse::<f64>().unwrap();
    let param2 = params.next().unwrap().as_str().parse::<f64>().unwrap();
    (param1, param2)
  }

  fn parse_label_join_params(&self, node: &Pair<'_, Rule>) -> (String, String, Vec<String>) {
    let mut params = node.clone().into_inner();
    let dst_label = params.next().unwrap().as_str().to_string();
    let separator = params.next().unwrap().as_str().to_string();
    let src_labels = params.map(|p| p.as_str().to_string()).collect();
    (dst_label, separator, src_labels)
  }

  fn parse_label_replace_params(&self, node: &Pair<'_, Rule>) -> (String, String, String, String) {
    let mut params = node.clone().into_inner();
    let dst_label = params.next().unwrap().as_str().to_string();
    let replacement = params.next().unwrap().as_str().to_string();
    let src_label = params.next().unwrap().as_str().to_string();
    let regex = params.next().unwrap().as_str().to_string();
    (dst_label, replacement, src_label, regex)
  }

  async fn process_vector(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    // Default to URL supplied range. This is overridden by the PromQL if duration is present.
    let mut duration = PromQLDuration::new_with_params(range_start_time, range_end_time, 0);

    let mut selectors: Vec<PromQLSelector> = Vec::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::metric_name => {
          let metric_name = node.as_str();
          selectors.push(PromQLSelector::new_with_params(
            METRIC_NAME_PREFIX.to_owned(),
            metric_name.to_string(),
            MetricsQueryCondition::Equals,
          ));
        }
        Rule::label => match self.process_label(node) {
          Ok(selector) => selectors.push(selector),
          Err(e) => return Err(e),
        },
        Rule::duration => {
          if let Some(duration_text) = node.into_inner().next() {
            duration.set_duration(duration_text.as_str())?;
          }
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    let results = self
      .process_metric_search(&mut selectors, &duration)
      .await?;

    Ok(results)
  }

  fn process_label(&self, node: Pair<Rule>) -> Result<PromQLSelector, AstError> {
    let inner_nodes: Vec<Pair<Rule>> = node.into_inner().collect();
    let mut label_name: Option<&str> = None;
    let mut label_value: Option<&str> = None;
    let mut condition = MetricsQueryCondition::Undefined;

    for inner_node in inner_nodes {
      match inner_node.as_rule() {
        Rule::label_name => label_name = Some(inner_node.as_str()),
        Rule::label_value => label_value = Some(inner_node.as_str()),
        Rule::condition => {
          condition = match inner_node.as_str() {
            "=" => MetricsQueryCondition::Equals,
            "!=" => MetricsQueryCondition::NotEquals,
            "=~" => MetricsQueryCondition::EqualsRegex,
            "!~" => MetricsQueryCondition::NotEqualsRegex,
            _ => {
              return Err(AstError::UnsupportedQuery(
                "Unsupported condition".to_string(),
              ))
            }
          }
        }
        _ => {}
      }
    }

    if let (Some(ln), Some(lv)) = (label_name, label_value) {
      Ok(PromQLSelector::new_with_params(
        ln.to_string(),
        lv.to_string(),
        condition,
      ))
    } else {
      Err(AstError::UnsupportedQuery("Incomplete label".to_string()))
    }
  }

  // Search the Metrics DB.
  //
  // We optimize for the common case where condition is MetricsQueryCondition::Equals:
  // Iterate through the PromQLSelector, and if condition is Equals,
  // insert label_name and label_value into a HashMap which is used search the DB.
  // If condition is not Equals, retain the selector in the vector and process
  // it differently.
  async fn process_metric_search(
    &self,
    selectors: &mut Vec<PromQLSelector>,
    duration: &PromQLDuration,
  ) -> Result<PromQLObject, AstError> {
    debug!(
      "Metric Search Results is going to be {:?} {:?}\n",
      selectors, duration
    );

    let mut results = PromQLObject::new();
    let mut labels: HashMap<String, String> = HashMap::new();

    // Process the "Equals" conditions first, removing the selector elements
    selectors.retain(|selector| {
      if let MetricsQueryCondition::Equals = selector.condition {
        labels.insert(selector.label_name.clone(), selector.label_value.clone());
        false
      } else {
        true
      }
    });

    let metrics_results = self
      .process_segments(
        &labels,
        &MetricsQueryCondition::Equals,
        duration.start_time,
        duration.end_time,
      )
      .await?;

    if !metrics_results.is_empty() {
      results.add_to_vector(PromQLTimeSeries::new_with_params(labels, metrics_results));
    }

    if results.get_vector().is_empty() {
      debug!("No results found. Returning empty handed.");
      return Ok(PromQLObject::new());
    }

    Ok(results)
  }

  async fn process_segments(
    &self,
    labels: &HashMap<String, String>,
    condition: &MetricsQueryCondition,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<MetricPoint>, AstError> {
    debug!(
      "Searching segments with args {:?} {:?} {} {}",
      labels, condition, range_start_time, range_end_time
    );

    let mut results: Vec<MetricPoint> = Vec::new();

    let segment_numbers = self
      .get_overlapping_segments(range_start_time, range_end_time)
      .await;

    for segment_number in segment_numbers {
      let mut segment_results =
        if let Some(segment) = self.get_memory_segments_map().get(&segment_number) {
          segment
            .search_metrics(labels, condition, range_start_time, range_end_time)
            .await
            .unwrap_or_else(|_| Vec::new())
        } else {
          // If the segment isn't present, load it in memory first
          let segment = self.refresh_segment(segment_number).await?;
          segment
            .search_metrics(labels, condition, range_start_time, range_end_time)
            .await
            .unwrap_or_else(|_| Vec::new())
        };

      results.append(&mut segment_results);
    }

    Ok(results)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::index_manager::promql::PromQLParser;
  // use crate::metric::metric_point::MetricPoint;
  use crate::storage_manager::storage::StorageType;
  use chrono::Utc;
  use pest::Parser;
  use tempdir::TempDir;

  // Helper function to create index
  async fn create_index(name: &str, num_metric_points: u32) -> (Index, String) {
    let storage_type = StorageType::Local;
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!("{}/{}", index_dir.path().to_str().unwrap(), name);
    let index = Index::new(&storage_type, &index_dir_path).await.unwrap();

    // Append metric points to the index.
    let timestamp = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    for _ in 1..=num_metric_points {
      index.append_metric_point("metric", &label_map, timestamp, 100.0);
    }

    (index, index_dir_path)
  }

  async fn execute_query(
    index: &Index,
    query: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<PromQLTimeSeries>, AstError> {
    let parsed_query = PromQLParser::parse(Rule::start, query)
      .expect("Failed to parse query")
      .next()
      .unwrap();

    index
      .traverse_promql_ast(
        &Pairs::single(parsed_query),
        range_start_time,
        range_end_time,
      )
      .await
  }

  // Test sum aggregation with label grouping
  #[tokio::test]
  async fn test_sum_aggregation() {
    let query = "sum(metric{label_name_1=\"label_value_1\"})";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_metric_points().len(), 1);
    assert_eq!(results[0].get_metric_points()[0].get_value(), 1000.0);
  }

  #[tokio::test]
  async fn test_basic() {
    let query = "metric{label_name_1=\"label_value_1\"}";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
      results[0].get_metric_points().len(),
      num_metric_points as usize
    );
  }

  #[tokio::test]
  async fn test_basic_label_only() {
    let query = "{label_name_1=\"label_value_1\"}";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
      results[0].get_metric_points().len(),
      num_metric_points as usize
    );
  }

  #[tokio::test]
  async fn test_avg_aggregation() {
    let query = "avg(metric{label_name_1=\"label_value_1\"})";
    let num_metric_points = 10; // Assuming each has a value of 100.0
    let (index, _index_dir_path) = create_index("test_index_avg", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_metric_points().len(), 1); // Only one aggregated result
    assert_eq!(results[0].get_metric_points()[0].get_value(), 100.0); // Average value
  }

  #[tokio::test]
  async fn test_max_aggregation() {
    let query = "max(metric{label_name_1=\"label_value_1\"})";
    let num_metric_points = 10; // Assuming varied values
    let (index, _index_dir_path) = create_index("test_index_max", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_metric_points()[0].get_value(), 100.0);
  }
  // Add more test cases for each function and aggregation mentioned in the code...
}

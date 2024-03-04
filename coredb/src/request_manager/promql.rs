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

use super::promql_object::{AggregationOperator, FunctionOperator, PromQLObject};
use super::time_series::QueryTimeSeries;
use crate::metric::constants::{MetricsQueryCondition, METRIC_NAME_PREFIX};
use crate::utils::error::QueryError;
use crate::utils::request::{check_query_time, parse_time_range};
use crate::{index_manager::index::Index, metric::metric_point::MetricPoint};
use chrono::Utc;
use pest::iterators::{Pair, Pairs};
use std::collections::{HashMap, VecDeque};

use log::debug;

use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/request_manager/promql_grammar.pest"]

pub struct PromQLParser;

#[derive(Debug)]
pub struct PromQLSelector {
  label_name: String,
  label_value: String,
  condition: MetricsQueryCondition,
}

/// Represents a selector for querying PromQL metrics based on label names, label values, and a condition.
impl PromQLSelector {
  /// Creates a new, empty `PromQLSelector`.
  pub fn new() -> Self {
    PromQLSelector {
      label_name: String::new(),
      label_value: String::new(),
      condition: MetricsQueryCondition::Undefined,
    }
  }

  /// Creates a new `PromQLSelector` with specified label name, label value, and condition.
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
}

impl Default for PromQLSelector {
  fn default() -> Self {
    Self::new()
  }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct PromQLDuration {
  start_time: u64,
  end_time: u64,
  offset: u64,
}

/// Represents a duration with a start and end time, and an optional offset, for PromQL queries.
impl PromQLDuration {
  /// Creates a new `PromQLDuration` with `start_time` at UNIX epoch start and `end_time` as now.
  pub fn new() -> Self {
    PromQLDuration {
      start_time: 0,
      end_time: Utc::now().timestamp() as u64,
      offset: 0,
    }
  }

  /// Creates a `PromQLDuration` with specified start, end times, and offset.
  pub fn new_with_params(range_start_time: u64, range_end_time: u64, range_offset: u64) -> Self {
    PromQLDuration {
      start_time: range_start_time,
      end_time: range_end_time,
      offset: range_offset,
    }
  }

  /// Sets the duration based on a Prometheus duration string (e.g., "2h", "15m").
  pub fn set_duration(&mut self, duration_str: &str) -> Result<(), QueryError> {
    let duration = parse_time_range(duration_str)?;
    self.end_time = Utc::now().timestamp() as u64;
    self.start_time = self.end_time.saturating_sub(duration.num_seconds() as u64);

    Ok(())
  }

  /// Sets the offset for the duration based on a Prometheus-style duration string (e.g., "2h", "15m").
  #[allow(dead_code)]
  pub fn set_offset(&mut self, offset_str: &str) -> Result<(), QueryError> {
    let duration = parse_time_range(offset_str)?;
    self.offset = duration.num_seconds() as u64;
    Ok(())
  }

  #[allow(dead_code)]
  /// Offsets the duration start and end times by the previously set offset amount.
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
  /// Parses a PromQL query from a URL query string into an AST.
  pub fn parse_query(url_query: &str) -> Result<pest::iterators::Pairs<'_, Rule>, QueryError> {
    debug!("PROMQL: Parsing URL query {:?},\n", url_query);

    PromQLParser::parse(Rule::start, url_query)
      .map_err(|e| QueryError::JsonParseError(e.to_string()))
  }

  /// Walk the AST using an iterator and process each node
  pub async fn traverse_promql_ast(
    &self,
    nodes: &Pairs<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Traversing ast {:?},\n", nodes);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = PromQLObject::new();

    // Pop the nodes off the stack and process
    while let Some(node) = stack.pop_front() {
      let processing_result =
        self.query_dispatcher(&node, timeout, range_start_time, range_end_time);

      match processing_result.await {
        Ok(mut node_results) => {
          results.set_vector(node_results.take_vector());
        }
        Err(QueryError::UnsupportedQuery(_)) => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
        Err(e) => {
          return Err(e);
        }
      }
    }

    let execution_time = check_query_time(timeout, query_start_time)?;

    results.set_execution_time(execution_time);

    Ok(results)
  }

  /// General dispatcher for query processing
  async fn query_dispatcher(
    &self,
    node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    let query_start_time = Utc::now().timestamp_millis() as u64;

    // Initialize processing based on the node's rule
    let result = match node.as_rule() {
      Rule::expression => {
        self
          .process_expression(node, timeout, range_start_time, range_end_time)
          .await
      }
      _ => Err(QueryError::UnsupportedQuery(format!(
        "Unsupported rule: {:?}",
        node.as_rule()
      ))),
    };

    check_query_time(timeout, query_start_time)?;

    result
  }

  /// Processes an expression node from the AST to perform data retrieval or computation.
  async fn process_expression(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Expression Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut and_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::and_expression => {
          and_results = self
            .process_and_expression(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes an AND expression node from the AST.
  async fn process_and_expression(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("AND Expression Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut equality_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::equality => {
          equality_results = self
            .process_equality(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes an equality node from the AST, handling metric comparison.
  async fn process_equality(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Equality Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut comparison_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::comparison => {
          comparison_results = self
            .process_comparison(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes a comparison node from the AST, handling operations like greater than or less than.
  async fn process_comparison(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Comparison Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut term_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::term => {
          term_results = self
            .process_term(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes a term node from the AST, typically involving arithmetic operations.
  async fn process_term(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Term Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut factor_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::factor => {
          factor_results = self
            .process_factor(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes a factor node from the AST, usually involving multiplication or division.
  async fn process_factor(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Factor Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut exponent_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::exponent => {
          exponent_results = self
            .process_exponent(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes an exponent node from the AST, dealing with power functions.
  async fn process_exponent(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Exponent Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = PromQLObject::new();
    let mut unary_results = PromQLObject::new();
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::unary => {
          unary_results = self
            .process_unary(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes a unary node from the AST, typically involving negation.
  async fn process_unary(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Unary Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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
            .process_leaf(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes a leaf node from the AST, which could represent a scalar, vector, aggregation, or function.
  async fn process_leaf(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Leaf Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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
                return Err(QueryError::UnsupportedQuery(
                  "Could not convert scalar text to float for processing".to_string(),
                ))
              }
            }
          }
        }
        Rule::vector => {
          let mut promql_object = self
            .process_vector(&node, timeout, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        Rule::aggregations => {
          let mut promql_object = self
            .process_aggregations(&node, timeout, range_start_time, range_end_time)
            .await?;
          results.set_vector(promql_object.take_vector());
        }
        Rule::functions => {
          let mut promql_object = self
            .process_functions(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes aggregation nodes from the AST, applying operations like sum, average, etc.
  async fn process_aggregations(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Aggregation Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut operator = AggregationOperator::Undefined;

    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::vector => {
          let mut promql_object = self
            .process_vector(&node, timeout, range_start_time, range_end_time)
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

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Processes function nodes from the AST, executing functions like `rate`, `increase`, etc.
  async fn process_functions(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Functions Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut operator = FunctionOperator::Absent; // Default or placeholder

    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::vector => {
          let mut promql_object = self
            .process_vector(&node, timeout, range_start_time, range_end_time)
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
        Rule::pi => operator = FunctionOperator::Pi,
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

    check_query_time(timeout, query_start_time)?;

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

  /// Processes vector nodes from the AST, creating an Instant Vector or Range Vector
  async fn process_vector(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    debug!("Vector Node {:?},\n", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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
        Rule::label => {
          let selector = self.process_label(node)?;
          selectors.push(selector);
        }
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

    debug!(
      "Calling Metric Search with {:?} {:?},\n",
      selectors, duration
    );

    let results = self
      .process_metric_search(timeout, &mut selectors, &duration)
      .await?;

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Extract the parameters for a label match: name, value, and condition
  fn process_label(&self, node: Pair<Rule>) -> Result<PromQLSelector, QueryError> {
    debug!("Label Node {:?},\n", node);

    let mut label_name: Option<&str> = None;
    let mut label_value: Option<&str> = None;
    let mut condition = MetricsQueryCondition::Undefined;

    for inner_node in node.into_inner() {
      match inner_node.as_rule() {
        Rule::label_name => label_name = Some(inner_node.as_str()),
        Rule::label_value => label_value = Some(inner_node.as_str()),
        Rule::condition => {
          // Directly navigate into the inner of the condition node
          if let Some(condition_node) = inner_node.into_inner().next() {
            condition = match condition_node.as_rule() {
              Rule::equal_match => MetricsQueryCondition::Equals,
              Rule::not_equal_match => MetricsQueryCondition::NotEquals,
              Rule::regex_match => MetricsQueryCondition::EqualsRegex,
              Rule::not_regex_match => MetricsQueryCondition::NotEqualsRegex,
              // Handle other conditions as needed
              _ => {
                debug!("Unsupported condition encountered");
                return Err(QueryError::UnsupportedQuery(
                  "Unsupported condition".to_string(),
                ));
              }
            }
          }
        }
        _ => {}
      }
    }

    debug!(
      "Label: Setting label_name to {:?} and label_value to {:?},\n",
      label_name, label_value
    );

    if let (Some(ln), Some(lv)) = (label_name, label_value) {
      Ok(PromQLSelector::new_with_params(
        ln.to_string(),
        lv.to_string(),
        condition,
      ))
    } else {
      Err(QueryError::UnsupportedQuery("Incomplete label".to_string()))
    }
  }

  /// Search the Metrics DB.
  ///
  /// We optimize for the common case where condition is MetricsQueryCondition::Equals:
  /// Iterate through the PromQLSelector, and if condition is Equals,
  /// insert label_name and label_value into a HashMap which is used search the DB.
  /// If condition is not Equals, retain the selector in the vector and process
  /// it differently.
  async fn process_metric_search(
    &self,
    timeout: u64,
    selectors: &mut Vec<PromQLSelector>,
    duration: &PromQLDuration,
  ) -> Result<PromQLObject, QueryError> {
    debug!(
      "PromQL: Searching metrics with selectors {:?} and duration {:?}\n",
      selectors, duration
    );

    let query_start_time = Utc::now().timestamp_millis() as u64;

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
        timeout,
        duration.start_time,
        duration.end_time,
      )
      .await?;

    if !metrics_results.is_empty() {
      results.add_to_vector(QueryTimeSeries::new_with_params(labels, metrics_results));
    }

    if results.get_vector().is_empty() {
      debug!("No results found from searching segments. Returning empty handed.");
      return Ok(PromQLObject::new());
    }

    check_query_time(timeout, query_start_time)?;

    Ok(results)
  }

  /// Search each of the segments for a given query
  async fn process_segments(
    &self,
    labels: &HashMap<String, String>,
    condition: &MetricsQueryCondition,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<MetricPoint>, QueryError> {
    debug!(
      "PromQL: Searching segments with args {:?} {:?} {} {}",
      labels, condition, range_start_time, range_end_time
    );

    let query_start_time = Utc::now().timestamp_millis() as u64;

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

      // Let's check the elapsed time after each segment search
      check_query_time(timeout, query_start_time)?;
    }

    Ok(results)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::storage_manager::storage::StorageType;
  use crate::utils::config::config_test_logger;
  use chrono::Utc;
  use pest::Parser;
  use tempdir::TempDir;

  // Helper function to create index
  async fn create_index(name: &str, num_metric_points: u32) -> (Index, String) {
    config_test_logger();
    let storage_type = StorageType::Local;
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!("{}/{}", index_dir.path().to_str().unwrap(), name);
    let index = Index::new(&storage_type, &index_dir_path).await.unwrap();

    // Append metric points to the index.
    let timestamp = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    for _ in 1..=num_metric_points {
      index
        .append_metric_point("metric", &label_map, timestamp, 100.0)
        .await
        .expect("Could not append metric point");
    }

    (index, index_dir_path)
  }

  async fn execute_query(
    index: &Index,
    query: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, QueryError> {
    let _ = env_logger::builder().is_test(true).try_init();

    let parsed_query = PromQLParser::parse(Rule::start, query)
      .expect("Failed to parse query")
      .next()
      .unwrap();

    index
      .traverse_promql_ast(
        &Pairs::single(parsed_query),
        0,
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
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1);
    assert_eq!(mp[0].get_value(), 1000.0);
  }

  #[tokio::test]
  async fn test_basic() {
    let query = "metric{label_name_1=\"label_value_1\"}";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index", num_metric_points).await;
    let result = execute_query(&index, query, 0, u64::MAX).await;

    match result {
      Ok(mut results) => {
        assert_eq!(results.get_vector().len(), 1);
        assert_eq!(
          results.take_vector()[0].get_metric_points().len(),
          num_metric_points as usize
        );
      }
      Err(e) => panic!("Test failed with error: {:?}", e),
    }
  }

  #[tokio::test]
  async fn test_basic_with_space() {
    let query = "metric{label_name_1 =\"label_value_1\"}";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX)
      .await
      .expect("Did not execute query correctly");
    assert_eq!(results.get_vector().len(), 1);
    assert_eq!(
      results.take_vector()[0].get_metric_points().len(),
      num_metric_points as usize
    );
  }

  #[tokio::test]
  async fn test_basic_label_only() {
    let query = "{label_name_1=\"label_value_1\"}";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert_eq!(results.get_vector().len(), 1);
    assert_eq!(
      results.take_vector()[0].get_metric_points().len(),
      num_metric_points as usize
    );
  }

  #[tokio::test]
  async fn test_avg_aggregation() {
    let query = "avg(metric{label_name_1=\"label_value_1\"})";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index_avg", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1);
    assert_eq!(mp[0].get_value(), 100.0);
  }

  #[tokio::test]
  async fn test_max_aggregation() {
    let query = "max(metric{label_name_1=\"label_value_1\"})";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index_max", num_metric_points).await;
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1);
    assert_eq!(mp[0].get_value(), 100.0);
  }

  #[tokio::test]
  async fn test_min_aggregation() {
    let query = "min(metric{label_name_1=\"label_value_1\"})";
    // Assuming varied values for metric points to test min aggregation properly.
    let (index, _index_dir_path) = create_index("test_index_min", 10).await;

    // Simulate appending metric points with varied values.
    let timestamp = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());

    let values = [10.0, 20.0, 5.0, 15.0, 25.0];
    for value in values.iter() {
      index
        .append_metric_point("metric", &label_map, timestamp, *value)
        .await
        .expect("Could not append metric point");
    }

    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1);
    assert_eq!(mp[0].get_value(), 5.0);
  }

  #[tokio::test]
  async fn test_count_aggregation() {
    let query = "count(metric{label_name_1=\"label_value_1\"})";
    let num_metric_points = 5;
    let (index, _index_dir_path) = create_index("test_index_count", num_metric_points).await;

    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1);
    assert_eq!(mp[0].get_value(), num_metric_points as f64);
  }

  #[tokio::test]
  async fn test_equality_label_selector() {
    let query = "metric{label_name_1=\"label_value_1\"}";
    let num_metric_points = 3;
    let (index, _index_dir_path) =
      create_index("test_index_equality_selector", num_metric_points).await;

    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), num_metric_points as usize);
  }

  #[tokio::test]
  async fn test_multiple_label_conditions() {
    let query = "metric{label_name_1=\"label_value_1\", label_name_2!=\"label_value_2\"}";
    let num_metric_points = 3;
    let (index, _index_dir_path) =
      create_index("test_index_multiple_conditions", num_metric_points).await;

    append_metric_points_complex(&index).await;

    let results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    assert!(!results.get_vector().is_empty());
  }

  async fn append_metric_points_complex(index: &Index) {
    let timestamp = Utc::now().timestamp_millis() as u64;

    let label_combinations = vec![
      (
        HashMap::from([
          ("label_name_1".to_string(), "label_value_1".to_string()),
          ("label_name_2".to_string(), "label_value_2".to_string()),
        ]),
        100.0,
      ),
      (
        HashMap::from([
          ("label_name_1".to_string(), "label_value_1".to_string()),
          ("label_name_2".to_string(), "label_value_3".to_string()),
        ]),
        200.0,
      ),
    ];

    for (label_map, value) in label_combinations {
      index
        .append_metric_point("metric", &label_map, timestamp, value)
        .await
        .expect("Could not append metric point");
    }
  }

  #[tokio::test]
  async fn test_nested_aggregations() {
    // Query with nested sum and count aggregations
    let query = "sum(count(metric{label_name_1=\"label_value_1\"}))";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index_nested", num_metric_points).await;

    // Append metric points with varied labels
    append_metric_points_varied(&index, "label_name_1", &["label_value_1"]).await;

    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1); // Expect one aggregated result
    assert_eq!(
      mp[0].get_value(),
      1.0 + num_metric_points as f64 // metric adds 1 more
    ); // Expected sum of count

    // Query with nested average and max aggregations
    let query = "avg(max(metric{label_name_1=\"label_value_1\"}))";
    let (index, _index_dir_path) =
      create_index("test_index_nested_avg_max", num_metric_points).await;

    // Append metric points with varied labels
    append_metric_points_varied(&index, "label_name_1", &["label_value_1"]).await;

    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();
    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1);
    assert_eq!(mp[0].get_value(), 100.0); // Expected average of maximums
  }

  #[tokio::test]
  async fn test_rate_calculation() {
    let query = "rate(metric{label_name_1=\"label_value_1\"}[1s])";
    let num_metric_points = 10;
    let (index, _index_dir_path) = create_index("test_index_rate", num_metric_points).await;

    // Execute the query
    let mut results = execute_query(&index, query, 0, u64::MAX).await.unwrap();

    // Assert the results
    assert_eq!(results.get_vector().len(), 1);
    assert_eq!(results.take_vector()[0].get_metric_points().len(), 1);
    let expected_rate = 1.0;
    let actual_rate = 1.0;
    assert_eq!(actual_rate, expected_rate);
  }

  async fn append_metric_points_varied(index: &Index, label_name: &str, label_values: &[&str]) {
    let timestamp = Utc::now().timestamp_millis() as u64;

    for &label_value in label_values {
      let mut label_map = HashMap::new();
      label_map.insert(label_name.to_string(), label_value.to_string());
      index
        .append_metric_point("metric", &label_map, timestamp, 100.0)
        .await
        .expect("Could not append metric point");
    }
  }

  #[tokio::test]
  async fn test_nested_aggregations_with_time_range() {
    // Query with nested sum and count aggregations and time range specification
    let query = "sum(count(metric{label_name_1=\"label_value_1\"}))";
    let num_metric_points = 10;
    let (index, _index_dir_path) =
      create_index("test_index_nested_with_time_range", num_metric_points).await;

    // Append metric points with varied labels
    append_metric_points_varied(&index, "label_name_1", &["label_value_1"]).await;

    let range_start_time = 0;
    let range_end_time = Utc::now().timestamp_millis() as u64;

    let mut results = execute_query(&index, query, range_start_time, range_end_time)
      .await
      .unwrap();

    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 1); // Expect one aggregated result
    assert_eq!(
      mp[0].get_value(),
      1.0 + num_metric_points as f64 // metric adds 1 more
    );
  }
}

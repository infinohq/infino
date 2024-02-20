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

use crate::index_manager::promql_object::PromQLObject;
use crate::metric::constants::MetricsQueryCondition;
use crate::{index_manager::index::Index, metric::metric_point::MetricPoint};

use crate::utils::error::{AstError, SearchMetricsError};
use chrono::{Duration, Utc};
use pest::iterators::{Pair, Pairs};
use std::collections::{HashMap, VecDeque};

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

use super::promql_time_series::PromQLTimeSeries;

#[derive(Parser)]
#[grammar = "src/index_manager/promql_grammar.pest"]

pub struct PromQLParser;

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
      start_time: Utc::now().timestamp() as u64,
      end_time: Utc::now().timestamp() as u64,
      offset: 0,
    }
  }

  pub fn new_with_params(start_time: u64, end_time: u64, offset: u64) -> Self {
    PromQLDuration {
      start_time: start_time,
      end_time: end_time,
      offset: offset,
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
          .process_expression(&node, range_start_time, range_end_time)
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
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::or => {
          let mut and_results = self
            .process_and(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(and_results.take_vector());
          } else {
            results.or(&mut and_results);
          }
        }
        Rule::unless => {
          let mut and_results = self
            .process_and(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results = and_results;
          } else {
            results.unless(&mut and_results);
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

  async fn process_and(
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
        Rule::and => {
          let mut equality_results = self
            .process_equality(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(equality_results.take_vector());
          } else {
            results.and(&mut equality_results);
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

  async fn process_equality(
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
        Rule::equal => {
          let mut comparison_results = self
            .process_comparison(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(comparison_results.take_vector());
          } else {
            results.equal(&mut comparison_results);
          }
        }
        Rule::not_equal => {
          let mut comparison_results = self
            .process_comparison(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results = comparison_results;
          } else {
            results.not_equal(&mut comparison_results);
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

  async fn process_comparison(
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
        Rule::greater_than => {
          let mut term_results = self
            .process_term(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.take_vector());
          } else {
            results.greater_than(&mut term_results);
          }
        }
        Rule::less_than => {
          let mut term_results = self
            .process_term(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.take_vector());
          } else {
            results.less_than(&mut term_results);
          }
        }
        Rule::greater_than_or_equal => {
          let mut term_results = self
            .process_term(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.take_vector());
          } else {
            results.greater_than_or_equal(&mut term_results);
          }
        }
        Rule::less_than_or_equal => {
          let mut term_results = self
            .process_term(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.take_vector());
          } else {
            results.less_than_or_equal(&mut term_results);
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

  async fn process_term(
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
        Rule::plus => {
          let mut factor_results = self
            .process_factor(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(factor_results.take_vector());
          } else {
            results.add(&mut factor_results);
          }
        }
        Rule::minus => {
          let mut term_results = self
            .process_factor(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results = term_results;
          } else {
            results.subtract(&mut term_results);
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

  async fn process_factor(
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
        Rule::multiply => {
          let mut exponent_results = self
            .process_exponent(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.take_vector());
          } else {
            results.add(&mut exponent_results);
          }
        }
        Rule::divide => {
          let mut exponent_results = self
            .process_exponent(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.take_vector());
          } else {
            results.subtract(&mut exponent_results);
          }
        }
        Rule::modulo => {
          let mut exponent_results = self
            .process_exponent(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.take_vector());
          } else {
            results.subtract(&mut exponent_results);
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

  async fn process_exponent(
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
        Rule::power => {
          let mut unary_results = self
            .process_unary(&node, range_start_time, range_end_time)
            .await?;
          if results.get_vector().is_empty() {
            results.set_vector(unary_results.take_vector());
          } else {
            results.power(&mut unary_results);
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

  async fn process_unary(
    &self,
    root_node: &Pair<'_, Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();
    let mut duration = PromQLDuration::new_with_params(range_start_time, range_end_time, 0);

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::offset => {
          if let Some(offset_text) = node.into_inner().next() {
            duration.set_offset(offset_text.as_str())?;
          }
        }
        Rule::negative => {
          let mut leaf_results = self.process_leaf(&node, &mut duration).await?;
          if results.get_vector().is_empty() {
            results.set_vector(leaf_results.take_vector());
          } else {
            results.negative();
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
    duration: &mut PromQLDuration,
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
          let mut promql_object = self.process_vector(&node, duration).await?;
          results.set_vector(promql_object.take_vector());
        }
        // TODO
        // Rule::aggregations => {
        //   results.set_vector(self.process_aggregations(&node, offset).await?);
        // }
        // Rule::functions => {
        //   results.set_vector(self.process_functions(&node, offset).await?);
        // }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    Ok(results)
  }

  async fn process_vector(
    &self,
    root_node: &Pair<'_, Rule>,
    duration: &mut PromQLDuration,
  ) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut selectors: Vec<PromQLSelector> = Vec::new();
    let mut condition_text: Option<&str> = None;
    let mut label_value: Option<&str> = None;
    let mut label_name: Option<&str> = None;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::metric_name => {
          if let Some(metric) = node.into_inner().next() {
            let metric_name = Some(metric.as_str());
            label_name = Some("__name__");
            label_value = metric_name;
            condition_text = Some("=");
          }
        }
        Rule::label_name => {
          if let Some(label) = node.into_inner().next() {
            label_name = Some(label.as_str());
          }
        }
        Rule::condition => {
          if let Some(operator) = node.into_inner().next() {
            condition_text = Some(operator.as_str());
          }
        }
        Rule::label_value => {
          if let Some(value) = node.into_inner().next() {
            label_value = Some(value.as_str());
          }
        }
        Rule::duration => {
          if let Some(duration_text) = node.into_inner().next() {
            duration.set_duration(Some(duration_text.as_str()).unwrap_or_default())?;
          }
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }

      if let Some(operator) = condition_text {
        let condition = match operator {
          "==" => MetricsQueryCondition::Equals,
          "!=" => MetricsQueryCondition::NotEquals,
          "=~" => MetricsQueryCondition::EqualsRegex,
          "!~" => MetricsQueryCondition::NotEqualsRegex,
          _ => {
            return Err(AstError::UnsupportedQuery(format!(
              "Unsupported condition: {}",
              operator
            )))
          }
        };
        selectors.push(PromQLSelector::new_with_params(
          label_name.unwrap_or_default().to_owned(),
          label_value.unwrap_or_default().to_owned(),
          condition,
        ));
        condition_text = None;
        label_name = None;
        label_value = None;
      }
    }

    let results = self.process_metric_search(&mut selectors, duration).await?;
    Ok(results)
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
      log::debug!("No results found. Returning empty handed.");
      return Ok(PromQLObject::new());
    }

    // TODO process the other conditions in the remaining elements

    Ok(results)
  }

  async fn process_segments(
    &self,
    labels: &HashMap<String, String>,
    condition: &MetricsQueryCondition,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<MetricPoint>, AstError> {
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

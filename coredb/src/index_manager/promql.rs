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

use crate::index_manager::index::Index;
use crate::index_manager::promql_object::PromQLObject;

use crate::utils::error::AstError;
use chrono::{Duration, Utc};
use log::debug;
use pest::iterators::{Pair, Pairs};
use std::collections::HashSet;
use std::collections::VecDeque;

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/index_manager/promql_grammar.pest"]

pub struct PromQLParser;

pub struct PromQLSelectors {
  label_name: String,
  operator: String,
  label_value: String,
}

pub struct PromQLDuration {
  start_time: u64,
  end_time: u64,
  offset: u64,
}

impl PromQLDuration {
  /// Creates a new `PromQLDuration` with `start_time` at UNIX epoch start and `end_time` as now.
  pub fn new() -> Self {
    PromQLDuration {
      start_time: 0,
      end_time: Utc::now().timestamp() as u64,
      offset: 0,
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
  pub fn set_duration(&mut self, duration_str: &str) -> Result<(), &'static str> {
    let duration = Self::parse_duration(duration_str)?;
    self.end_time = Utc::now().timestamp() as u64;
    self.start_time = self.end_time.saturating_sub(duration.num_seconds() as u64);

    Ok(())
  }

  /// Parses a Prometheus duration string into a `chrono::Duration`.
  fn parse_duration(s: &str) -> Result<Duration, &'static str> {
    let units = s.chars().last().ok_or("Invalid duration string")?;
    let value = s[..s.len() - 1]
      .parse::<i64>()
      .map_err(|_| "Invalid number in duration")?;

    match units {
      's' => Ok(Duration::seconds(value)),
      'm' => Ok(Duration::minutes(value)),
      'h' => Ok(Duration::hours(value)),
      'd' => Ok(Duration::days(value)),
      'w' => Ok(Duration::weeks(value)),
      _ => Err("Unsupported duration unit"),
    }
  }

  pub fn set_offset(&mut self, offset_str: &str) -> Result<(), &'static str> {
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
  /// Walk the AST using an iterator and process each node
  pub async fn traverse_promql_ast(
    &self,
    nodes: &Pairs<'_, Rule>,
  ) -> Result<PromQLObject, AstError> {
    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = PromQLObject::new();

    // Pop the nodes off the stack and process
    while let Some(node) = stack.pop_front() {
      let processing_result = self.query_dispatcher(&node);

      match processing_result.await {
        Ok(node_results) => {
          results.set_vector(node_results.get_vector());
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

    Ok(results)
  }

  /// General dispatcher for query processing
  async fn query_dispatcher(&self, node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    match node.as_rule() {
      Rule::expression => self.process_expression(node).await,
      _ => Err(AstError::UnsupportedQuery(format!(
        "Unsupported rule: {:?}",
        node.as_rule()
      ))),
    }
  }

  async fn process_expression(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::or => {
          let mut and_results = self.process_and(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(and_results.get_vector());
          } else {
            results.or(&mut and_results);
          }
        }
        Rule::unless => {
          let mut and_results = self.process_and(&node).await?;
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

  async fn process_and(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::and => {
          let mut equality_results = self.process_equality(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(equality_results.get_vector());
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

  async fn process_equality(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::equal => {
          let mut comparison_results = self.process_comparison(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(comparison_results.get_vector());
          } else {
            results.equal(&mut comparison_results);
          }
        }
        Rule::not_equal => {
          let mut comparison_results = self.process_comparison(&node).await?;
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

  async fn process_comparison(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::greater_than => {
          let mut term_results = self.process_term(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.get_vector());
          } else {
            results.greater_than(&mut term_results);
          }
        }
        Rule::less_than => {
          let mut term_results = self.process_term(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.get_vector());
          } else {
            results.less_than(&mut term_results);
          }
        }
        Rule::greater_than_or_equal => {
          let mut term_results = self.process_term(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.get_vector());
          } else {
            results.greater_than_or_equal(&mut term_results);
          }
        }
        Rule::less_than_or_equal => {
          let mut term_results = self.process_term(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(term_results.get_vector());
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

  async fn process_term(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::plus => {
          let mut factor_results = self.process_factor(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(factor_results.get_vector());
          } else {
            results.add(&mut factor_results);
          }
        }
        Rule::minus => {
          let mut term_results = self.process_factor(&node).await?;
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

  async fn process_factor(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::multiply => {
          let mut exponent_results = self.process_exponent(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.get_vector());
          } else {
            results.add(&mut exponent_results);
          }
        }
        Rule::divide => {
          let mut exponent_results = self.process_exponent(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.get_vector());
          } else {
            results.subtract(&mut exponent_results);
          }
        }
        Rule::modulo => {
          let mut exponent_results = self.process_exponent(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(exponent_results.get_vector());
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

  async fn process_exponent(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::power => {
          let mut unary_results = self.process_unary(&node).await?;
          if results.get_vector().is_empty() {
            results.set_vector(unary_results.get_vector());
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

  async fn process_unary(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLObject, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = PromQLObject::new();
    let mut duration = PromQLDuration::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::offset => {
          if let Some(offset_text) = node.into_inner().next() {
            duration.set_offset(offset_text.as_str());
          }
        }
        Rule::negative => {
          let leaf_results = self.process_leaf(&node, &mut duration).await?;
          if results.get_vector().is_empty() {
            results.set_vector(leaf_results.get_vector());
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
          let promql_object = self.process_vector(&node, duration).await?;
          results.set_vector(promql_object.get_vector());
        }
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

    let mut selectors: Vec<(&str, &str, &str)> = Vec::new();
    let mut match_operator: Option<&str> = None;
    let mut match_value: Option<&str> = None;
    let mut label_name: Option<&str> = None;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::metric_name => {
          if let Some(metric) = node.into_inner().next() {
            let metric_name = Some(metric.as_str());
            selectors.push(("__name__", "==", metric_name.unwrap_or_default()));
          }
        }
        Rule::label_name => {
          if let Some(label) = node.into_inner().next() {
            label_name = Some(label.as_str());
          }
        }
        Rule::match_operator => {
          if let Some(operator) = node.into_inner().next() {
            match_operator = Some(operator.as_str());
          }
        }
        Rule::label_value => {
          if let Some(value) = node.into_inner().next() {
            match_value = Some(value.as_str());
          }
        }
        Rule::duration => {
          if let Some(duration_text) = node.into_inner().next() {
            duration.set_duration(Some(duration_text.as_str()).unwrap_or_default());
          }
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
      selectors.push((
        label_name.unwrap_or_default(),
        match_operator.unwrap_or_default(),
        match_value.unwrap_or_default(),
      ));
    }

    let results = self.process_metric_search(selectors, duration).await?;

    Ok(results)
  }

  /// Search the index
  async fn process_metric_search(
    &self,
    selectors: Vec<(&str, &str, &str)>,
    duration: &PromQLDuration,
  ) -> Result<PromQLObject, AstError> {
    // Extract the terms and perform the search
    let mut results = HashSet::new();

    // Get the segments overlapping with the given time range. This is in the reverse chronological order.
    let segment_numbers = self
      .get_overlapping_segments(duration.start_time, duration.end_time)
      .await;

    // Get the metrics from each of the segments. If a segment isn't present is memory, it is loaded in memory temporarily.
    for segment_number in segment_numbers {
      let segment = self.get_memory_segments_map().get(&segment_number);
      let mut segment_results = match segment {
        Some(segment) => {
          for (label_name, operator, label_value) in selectors {
            segment.search_metrics(
              &label_name,
              label_value,
              duration.start_time,
              duration.end_time,
            )
          }
        }
        None => {
          let segment = self.refresh_segment(segment_number).await;
          for (label_name, operator, label_value) in selectors {
            segment.search_metrics(
              &label_name,
              &label_value,
              duration.start_time,
              duration.end_time,
            )
          }
        }
      };
      results.get_vector().append(&mut segment_results);
    }

    if results.is_empty() {
      debug!("No results found. Returning empty handed.");
      return Ok(Vec::new());
    }

    Ok(results)
  }
}

#[cfg(test)]
mod tests {
  use crate::storage_manager::storage::StorageType;
  use std::collections::HashMap;

  use super::*;
  use tempdir::TempDir;

  async fn create_mock_index() -> Index {
    let storage_type = StorageType::Local;
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!("{}/{}", index_dir.path().to_str().unwrap(), "name");
    let index = Index::new(&storage_type, &index_dir_path).await.unwrap();

    // Create a couple of metric points.
    let mut label_set_1 = HashMap::new();
    label_set_1.insert("label_name_1".to_string(), "label_value_1".to_string());
    label_set_1.insert("label_name_2".to_string(), "label_value_1".to_string());
    label_set_1.insert("label_name_3".to_string(), "label_value_3".to_string());
    index
      .append_metric_point("metric_name_1", &label_set_1, 1, 1.0)
      .expect("Could not append metric point");
    index
      .append_metric_point("metric_name_2", &label_set_1, 2, 2.0)
      .expect("Could not append metric point");

    let mut label_set_2 = HashMap::new();
    label_set_2.insert("label_name_1".to_string(), "label_value_2".to_string());
    label_set_2.insert("label_name_3".to_string(), "label_value_3".to_string());
    label_set_2.insert("label_name_4".to_string(), "label_value_4".to_string());
    index
      .append_metric_point("metric_name_1", &label_set_2, 3, 3.0)
      .expect("Could not append metric point");

    index
  }

  #[tokio::test]
  async fn test_basic_query() {
    let index = create_mock_index();

    let promql_query = r#"metric_name_1{label_name_1=label_value_1"#;

    let results = match index.search_metrics("label_name_1", 0, u64::MAX).await {
      Ok(results) => {
        assert_eq!(
          results.len(),
          1,
          "There should be exactly 1 log matching the query."
        );

        assert!(results
          .iter()
          .all(|metric| metric.get_text().contains(3) && metric.get_text().contains("log")));
      }
      Err(err) => {
        panic!("Error in search_logs: {:?}", err);
      }
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    };
  }
}

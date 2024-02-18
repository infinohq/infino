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
use crate::index_manager::promql_vector::PromQLVector;

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

impl Index {
  /// Walk the AST using an iterator and process each node
  pub async fn traverse_promql_ast(
    &self,
    nodes: &Pairs<'_, Rule>,
  ) -> Result<PromQLVector, AstError> {
    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = PromQLVector::new();

    // Pop the nodes off the stack and process
    while let Some(node) = stack.pop_front() {
      let processing_result = self.query_dispatcher(&node);

      match processing_result.await {
        Ok(node_results) => {
          results.append(node_results);
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
  async fn query_dispatcher(&self, node: &Pair<'_, Rule>) -> Result<PromQLVector, AstError> {
    match node.as_rule() {
      Rule::leaf => self.process_leaf(node).await,
      Rule::unary_expression => self.process_unary_expression(node).await,
      Rule::compound_expression => self.process_compound_expression(node).await,
      _ => Err(AstError::UnsupportedQuery(format!(
        "Unsupported rule: {:?}",
        node.as_rule()
      ))),
    }
  }

  async fn process_expression(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLVector, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = HashSet::new();

    // Process each subtree separately, then logically OR the results
    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::logical_and => {
          let logical_and_results = self.process_logical_and(&node).await?;
          if results.is_empty() {
            results = logical_and_results;
          } else {
            results.extend(logical_and_results);
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

  async fn process_logical_and(
    &self,
    root_node: &Pair<'_, Rule>,
  ) -> Result<PromQLVector, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = Vec::new();

    // Process each subtree separately, then logically AND the results
    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::equality => {
          let equality_results = self.equality(&node).await?;
          if results.is_empty() {
            results = equality_results;
          } else {
            for (a, b) in results.iter_mut().zip(equality_results.iter()) {
              // Assuming non-zero as true and zero as false
              if *a.get_value() == 0 || *b.get_value() == 0 {
                *a.set_value() = 0;
              } else {
                *a.set_value() = 1;
              }
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

    Ok(results)
  }

  async fn process_equality(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLVector, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = Vec::new();

    // Process each subtree separately, then logically compare the results
    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::comparison => {
          let comparison_results = self.process_comparison(&node).await?;
          if results.is_empty() {
            results = comparison_results;
          } else {
            for (a, b) in results.iter_mut().zip(comparison_results.iter()) {
              // Assuming non-zero as true and zero as false
              if *a.get_value() == *b.get_value() {
                *a.set_value() = 0;
              } else {
                *a.set_value() = 1;
              }
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

    Ok(results)
  }

  async fn process_comparison(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLVector, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut results = HashSet::new();

    // Process each subtree separately, then logically compare the results
    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::term => {
          let term_results = self.process_term(&node).await?;
          if results.is_empty() {
            results = term_results;
          } else {
            for (a, b) in results.iter_mut().zip(term_results.iter()) {
              // Assuming non-zero as true and zero as false
              if *a.get_value() == *b.get_value() {
                *a.set_value() = 0;
              } else {
                *a.set_value() = 1;
              }
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

    Ok(results)
  }

  async fn process_leaf(&self, root_node: &Pair<'_, Rule>) -> Result<PromQLVector, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut metric_name: Option<&str> = None;
    let mut duration_text: Option<&str> = None;
    let mut matchers: Vec<(String, String, String)> = Vec::new();
    let mut label_name: Option<&str> = None;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::metric_name => {
          if let Some(metric) = node.into_inner().next() {
            metric = Some(metric.as_str());
          }
        }
        Rule::duration => {
          duration_text = node.into_inner().next().map(|v| v.as_str());
        }
        Rule::matcher => {
          while let Some(matcher_node) = stack.pop_front() {
            let mut match_operator: Option<&str> = None;
            let mut match_value: Option<&str> = None;
            match matcher_node.as_rule() {
              // Assumes these are always in sequence in the expression
              Rule::label_name => {
                if let Some(label) = matcher_node.into_inner().next() {
                  label_name = Some(label.as_str());
                }
              }
              Rule::match_operator => {
                if let Some(operator) = matcher_node.into_inner().next() {
                  match_operator = Some(operator.as_str());
                }
              }
              Rule::match_value => {
                if let Some(value) = matcher_node.into_inner().next() {
                  match_value = Some(value.as_str());
                }
              }
              _ => {
                for matcher_inner_node in matcher_node.into_inner() {
                  stack.push_back(matcher_inner_node);
                }
              }
            }
            matchers.push((Some(label_name), Some(match_operator), Some(match_value)));
          }
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    let (start_time, end_time) = parse_promql_duration(duration_text);

    if let Some(match_operator) = label_name {
      self
        .process_metric_search(matchers, start_time, end_time)
        .await;
    } else {
      Err(AstError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ))
    }
  }

  /// Search the index for the terms extracted from the AST
  async fn process_metric_search(
    &self,
    matchers: Vec<(String, String, String)>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLVector, AstError> {
    // Extract the terms and perform the search
    let mut results = HashSet::new();

    // Get the segments overlapping with the given time range. This is in the reverse chronological order.
    let segment_numbers = self
      .get_overlapping_segments(range_start_time, range_end_time)
      .await;

    // Get the metrics from each of the segments. If a segment isn't present is memory, it is loaded in memory temporarily.
    for segment_number in segment_numbers {
      let segment = self.get_memory_segments_map().get(&segment_number);
      let mut segment_results = match segment {
        Some(segment) => {
          for (label_name, operator, label_value) in matchers {
            segment.search_metrics(&label_name, label_value, range_start_time, range_end_time)
          }
        }
        None => {
          let segment = self.refresh_segment(segment_number).await;
          for (label_name, operator, label_value) in matchers {
            segment.search_metrics(&label_name, &label_value, range_start_time, range_end_time)
          }
        }
      };
      results.get_metric_points().append(&mut segment_results);
    }

    if results.is_empty() {
      debug!("No results found. Returning empty handed.");
      return Ok(Vec::new());
    }

    Ok(results)
  }

  fn parse_promql_duration(duration_str: &str) -> Result<(u64, u64), &'static str> {
    let duration_in_seconds = match duration_str.chars().last().unwrap() {
      's' => {
        duration_str
          .trim_end_matches('s')
          .parse::<i64>()
          .map_err(|_| "Invalid duration")?
          * 1
      }
      'm' => {
        duration_str
          .trim_end_matches('m')
          .parse::<i64>()
          .map_err(|_| "Invalid duration")?
          * 60
      }
      'h' => {
        duration_str
          .trim_end_matches('h')
          .parse::<i64>()
          .map_err(|_| "Invalid duration")?
          * 3600
      }
      'd' => {
        duration_str
          .trim_end_matches('d')
          .parse::<i64>()
          .map_err(|_| "Invalid duration")?
          * 86400
      }
      'w' => {
        duration_str
          .trim_end_matches('w')
          .parse::<i64>()
          .map_err(|_| "Invalid duration")?
          * 604800
      }
      'y' => {
        duration_str
          .trim_end_matches('y')
          .parse::<i64>()
          .map_err(|_| "Invalid duration")?
          * 31536000
      }
      _ => return Err("Unsupported duration unit"),
    };

    let now = Utc::now();
    let start_time = now - Duration::seconds(duration_in_seconds);
    Ok((start_time.timestamp() as u64, now.timestamp() as u64))
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

    let results = match index.get_metrics("label_name_1", 0, u64::MAX).await {
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

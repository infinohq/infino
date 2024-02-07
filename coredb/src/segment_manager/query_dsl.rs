// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Execute an Infino query. Both Query DSL and Lucene Query Syntax are supported.
//!
//! Uses the Pest parser with Pest-formatted PEG grammars: https://pest.rs/
//! which validates syntax.
//!
//! This code walks the AST via stack recursion (i.e. we maintain our own stack, processing
//! Infino-supported nodes on the stack and pushing any children of transitory nodes onto
//! the stack).

use crate::segment_manager::segment::Segment;
use crate::utils::error::AstError;
use crate::utils::tokenize::tokenize;

use log::debug;
use pest::iterators::{Pair, Pairs};
use std::collections::HashSet;
use std::collections::VecDeque;

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/segment_manager/query_dsl_grammar.pest"]

pub struct QueryDslParser;

impl Segment {
  pub fn traverse_ast(&self, nodes: &Pairs<Rule>) -> Result<HashSet<u32>, AstError> {
    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = HashSet::new();

    while let Some(node) = stack.pop_front() {
      let processing_result = self.process_query(&node);

      match processing_result {
        Ok(node_results) => {
          results.extend(node_results);
        }
        Err(AstError::UnsupportedQuery(_)) => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
        Err(e) => {
          // For other errors, propagate the error upwards
          return Err(e);
        }
      }
    }

    Ok(results)
  }

  fn process_query(&self, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
    match node.as_rule() {
      Rule::term_query => self.process_term_query(node),
      Rule::match_query => self.process_match_query(node),
      Rule::bool_query => self.process_bool_query(node),
      _ => Err(AstError::UnsupportedQuery(format!(
        "Unsupported rule: {:?}",
        node.as_rule()
      ))),
    }
  }

  fn process_bool_query(&self, bool_query_node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(bool_query_node.clone());

    let mut must_results = HashSet::new();
    let mut should_results = HashSet::new();
    let mut must_not_results = HashSet::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::must_clauses => {
          let results = self.process_must_clause(&node)?;
          must_results.extend(results);
        }
        Rule::should_clauses => {
          let results = self.process_should_clause(&node)?;
          should_results.extend(results);
        }
        Rule::must_not_clauses => {
          let results = self.process_must_not_clause(&node, &must_results)?;
          must_not_results.extend(results);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    let mut combined_results = if !should_results.is_empty() {
      must_results.union(&should_results).cloned().collect()
    } else {
      must_results
    };

    combined_results = combined_results
      .difference(&must_not_results)
      .cloned()
      .collect();

    Ok(combined_results)
  }

  fn process_term_query(&self, term_query_node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(term_query_node.clone());

    let mut fieldname: Option<&str> = None;
    let mut query_string: Option<&str> = None;
    let mut case_insensitive = true;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::fieldname => {
          if let Some(field) = node.into_inner().next() {
            fieldname = Some(field.as_str());
          }
        }
        Rule::value => {
          query_string = node.into_inner().next().map(|v| v.as_str());
        }
        Rule::case_insensitive => {
          case_insensitive = node
            .into_inner()
            .next()
            .map(|v| v.as_str().parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        }
        _ => {
          // For nodes that can contain other nodes, push their children onto the stack
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    // Perform an exact match search on the fieldname
    if let Some(query_str) = query_string {
      self.process_search(self.process_query_text(query_str, fieldname, case_insensitive))
    } else {
      Err(AstError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ))
    }
  }

  fn process_match_query(&self, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(node.clone());

    let mut fieldname: Option<&str> = None;
    let mut query_string: Option<&str> = None;
    let mut case_insensitive = true;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::fieldname => {
          if let Some(field) = node.into_inner().next() {
            fieldname = Some(field.as_str());
          }
        }
        Rule::match_string => {
          query_string = node.into_inner().next().map(|v| v.as_str());
        }
        Rule::match_array => {
          if let Some(query_node) = node.into_inner().find(|n| n.as_rule() == Rule::query) {
            query_string = query_node.into_inner().next().map(|v| v.as_str());
          }
        }
        Rule::case_insensitive => {
          case_insensitive = node
            .into_inner()
            .next()
            .map(|v| v.as_str().parse::<bool>().unwrap_or(true))
            .unwrap_or(true);
        }
        _ => {
          // For nodes that can contain other nodes, push their children onto the stack
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    // Perform a full text search with tokenization of the query
    if let Some(query_str) = query_string {
      self.process_search(self.process_query_text(query_str, fieldname, case_insensitive))
    } else {
      Err(AstError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ))
    }
  }

  fn process_search(&self, terms: Vec<String>) -> Result<HashSet<u32>, AstError> {
    // Extract the terms and perform the search
    let mut results = Vec::new();

    // Get postings lists for the query terms
    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) = self
      .get_postings_lists(&terms)
      .map_err(|e| AstError::TraverseError(format!("Error getting postings lists: {:?}", e)))?;

    if postings_lists.is_empty() {
      debug!("No posting list found. Returning empty handed.");
      return Ok(HashSet::new());
    }

    self
      .get_matching_doc_ids(
        &postings_lists,
        &last_block_list,
        &initial_values_list,
        shortest_list_index,
        &mut results,
      )
      .map_err(|e| AstError::TraverseError(format!("Error matching doc IDs: {:?}", e)))?;

    // Convert the vector results to a Hashset
    Ok(results.into_iter().collect())
  }

  fn process_must_clause(&self, root_node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
    let mut queue = VecDeque::new();
    queue.push_back(root_node.clone());

    let mut results = HashSet::new();
    let mut first = true;

    while let Some(node) = queue.pop_front() {
      match self.process_query(&node) {
        Ok(node_results) => {
          if first {
            results = node_results;
            first = false;
          } else {
            results = results.intersection(&node_results).cloned().collect();
          }
        }
        Err(AstError::UnsupportedQuery(_)) => {
          for child_node in node.into_inner() {
            queue.push_back(child_node);
          }
        }
        Err(e) => return Err(e),
      }
    }

    Ok(results)
  }

  fn process_should_clause(&self, root_node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
    let mut queue = VecDeque::new();
    queue.push_back(root_node.clone());

    let mut results = HashSet::new();

    while let Some(node) = queue.pop_front() {
      match self.process_query(&node) {
        Ok(node_results) => {
          results.extend(node_results);
        }
        Err(AstError::UnsupportedQuery(_)) => {
          for child_node in node.into_inner() {
            queue.push_back(child_node);
          }
        }
        Err(e) => return Err(e),
      }
    }

    Ok(results)
  }

  fn process_must_not_clause(
    &self,
    root_node: &Pair<Rule>,
    include_results: &HashSet<u32>,
  ) -> Result<HashSet<u32>, AstError> {
    let mut queue = VecDeque::new();
    queue.push_back(root_node.clone());

    let mut exclude_results = HashSet::new();

    while let Some(node) = queue.pop_front() {
      for child_node in node.into_inner() {
        // Use the helper function to process the node.
        match self.process_query(&child_node) {
          Ok(node_results) => {
            exclude_results.extend(node_results);
          }
          Err(AstError::UnsupportedQuery(_)) => {
            queue.push_back(child_node);
          }
          Err(e) => return Err(e),
        }
      }
    }

    Ok(
      include_results
        .difference(&exclude_results)
        .cloned()
        .collect(),
    )
  }

  fn process_query_text(
    &self,
    query_string: &str,
    fieldname: Option<&str>,
    case_insensitive: bool,
  ) -> Vec<String> {
    // Prepare the query string, applying lowercase if case_insensitive is set
    let query = if case_insensitive {
      query_string.to_lowercase()
    } else {
      query_string.to_owned()
    };

    let terms = tokenize(&query);

    // If fieldname is provided, concatenate it with each term; otherwise, use the term as is
    let transformed_terms: Vec<String> = terms
      .into_iter()
      .map(|term| {
        if let Some(field) = fieldname {
          format!("{}~{}", field, term)
        } else {
          term
        }
      })
      .collect();

    transformed_terms
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use chrono::Utc;

  use super::*;
  use pest::Parser;

  fn create_mock_segment() -> Segment {
    let segment = Segment::new();

    let log_messages = [
      ("log 1", "this is a test log message"),
      ("log 2", "this is another log message"),
      ("log 3", "test log for different term"),
    ];

    for (key, message) in log_messages.iter() {
      let mut fields = HashMap::new();
      fields.insert("key".to_string(), key.to_string());
      segment
        .append_log_message(Utc::now().timestamp_millis() as u64, &fields, message)
        .unwrap();
    }

    segment
  }

  #[test]
  fn test_search_with_must_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "bool": {
          "must": [
            { "match": { "_all" : { "query": "test" } } }
          ]
        }
      }
    }
    "#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX) {
        Ok(results) => {
          assert!(results
            .iter()
            .all(|log| log.get_text().contains("test") && log.get_text().contains("log")));
        }
        Err(err) => {
          panic!("Error in search_logs: {:?}", err);
        }
      },
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    }
  }

  #[test]
  fn test_search_with_should_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "bool": {
          "should": [
            { "match": { "_all" : { "query": "test" } } }
          ]
        }
      }
    }
    "#;

    // Parse the query DSL
    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX) {
        Ok(results) => {
          assert!(results
            .iter()
            .any(|log| log.get_text().contains("another") || log.get_text().contains("different")));
        }
        Err(err) => {
          panic!("Error in search_logs: {:?}", err);
        }
      },
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    }
  }

  #[test]
  fn test_search_with_must_not_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "bool": {
          "must_not": [
            { "match": { "_all" : { "query": "test" } } }
          ]
        }
      }
    }
    "#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX) {
        Ok(results) => {
          assert!(!results
            .iter()
            .any(|log| log.get_text().contains("excluded")));
        }
        Err(err) => {
          panic!("Error in search_logs: {:?}", err);
        }
      },
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    }
  }

  #[test]
  fn test_search_with_boolean_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
        "query": {
            "bool": {
                "must": [
                    { "match": { "_all": { "query": "log" } } }
                ],
                "should": [
                    { "match": { "_all": { "query": "test" } } }
                ],
                "must_not": [
                    { "match": { "_all": { "query": "different" } } }
                ]
            }
        }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX) {
        Ok(results) => {
          assert!(results.iter().all(|log| {
            log.get_text().contains("log")
              && (log.get_text().contains("test") || !log.get_text().contains("different"))
          }));
        }
        Err(err) => {
          panic!("Error in search_logs: {:?}", err);
        }
      },
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    }
  }

  #[test]
  fn test_search_with_match_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "match": {
          "log 1": {
            "query": "log"
          }
        }
      }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX) {
        Ok(results) => {
          assert!(results.iter().all(|log| log.get_text().contains("log")));
        }
        Err(err) => {
          panic!("Error in search_logs: {:?}", err);
        }
      },
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    }
  }

  #[test]
  fn test_search_with_match_all_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "match": {
          "key": {
            "query": "log"
          }
        }
      }
    }
    "#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX) {
        Ok(results) => {
          assert!(results.iter().all(|log| log.get_text().contains("log")));
        }
        Err(err) => {
          panic!("Error in search_logs: {:?}", err);
        }
      },
      Err(err) => {
        panic!("Error parsing query DSL: {:?}", err);
      }
    }
  }
}

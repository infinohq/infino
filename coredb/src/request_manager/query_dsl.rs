// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Execute an Infino logs query. Both Query DSL and Lucene Query Syntax are supported.
//!
//! Uses the Pest parser with Pest-formatted PEG grammars: https://pest.rs/
//! which validates syntax.
//!
//! Walks the AST via an iterator. We maintain our own stack, processing
//! Infino-supported nodes as they are popped off the stack and pushing children of
//! transitory nodes onto the stack for further processing.

use crate::segment_manager::segment::Segment;
use crate::utils::error::AstError;

use crate::utils::error::SearchLogsError;
use crate::utils::tokenize::{tokenize, FIELD_DELIMITER};

use futures::StreamExt;
use log::debug;
use pest::iterators::{Pair, Pairs};
use std::collections::HashSet;
use std::collections::VecDeque;

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/request_manager/query_dsl_grammar.pest"]

pub struct QueryDslParser;

impl Segment {
  pub fn parse_query(
    json_query: &str,
  ) -> Result<pest::iterators::Pairs<'_, Rule>, SearchLogsError> {
    QueryDslParser::parse(Rule::start, json_query)
      .map_err(|e| SearchLogsError::JsonParseError(e.to_string()))
  }

  /// Walk the AST using an iterator and process each node
  pub async fn traverse_query_dsl_ast(
    &self,
    nodes: &Pairs<'_, Rule>,
  ) -> Result<HashSet<u32>, AstError> {
    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = HashSet::new();

    // Pop the nodes off the stack and process
    while let Some(node) = stack.pop_front() {
      let processing_result = self.query_dispatcher(&node);

      match processing_result.await {
        Ok(node_results) => {
          results.extend(node_results);
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
  async fn query_dispatcher(&self, node: &Pair<'_, Rule>) -> Result<HashSet<u32>, AstError> {
    match node.as_rule() {
      Rule::term_query => self.process_term_query(node).await,
      Rule::match_query => self.process_match_query(node).await,
      Rule::bool_query => self.process_bool_query(node).await,
      Rule::terms_query => self.process_terms_query(node).await,
      Rule::match_phrase_query => self.process_match_phrase_query(node).await,
      _ => Err(AstError::UnsupportedQuery(format!(
        "Unsupported rule: {:?}",
        node.as_rule()
      ))),
    }
  }

  // Boolean Query Processor: https://opensearch.org/docs/latest/query-dsl/compound/bool/
  async fn process_bool_query(&self, root_node: &Pair<'_, Rule>) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut must_results = HashSet::new();
    let mut should_results = HashSet::new();
    let mut must_not_results = HashSet::new();

    // Process each subtree separately, then combine the logical results afterwards.
    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::must_clauses => {
          must_results = self.process_bool_subtree(&node, true).await?;
        }
        Rule::should_clauses => {
          should_results = self.process_bool_subtree(&node, false).await?;
        }
        Rule::must_not_clauses => {
          must_not_results = self.process_bool_subtree(&node, false).await?;
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    // Start with combining must and should results. If there are must results,
    // should results only add to it, not replace it.
    let combined_results = if !must_results.is_empty() || !should_results.is_empty() {
      let mut combined = must_results;
      if !should_results.is_empty() {
        combined.extend(should_results.iter());
      }
      combined
    } else {
      HashSet::new()
    };

    // Now get final results after excluding must_not results
    let final_results = combined_results
      .difference(&must_not_results)
      .cloned()
      .collect::<HashSet<u32>>();

    Ok(final_results)
  }

  async fn process_bool_subtree(
    &self,
    root_node: &Pair<'_, Rule>,
    must: bool,
  ) -> Result<HashSet<u32>, AstError> {
    let mut queue = VecDeque::new();
    queue.push_back(root_node.clone());

    let mut results = HashSet::new();

    while let Some(node) = queue.pop_front() {
      let processing_result = match node.as_rule() {
        Rule::term_query => self.process_term_query(&node).await,
        Rule::match_query => self.process_match_query(&node).await,
        Rule::bool_query => {
          // For bool_query, instead of processing, we queue its children for processing
          for inner_node in node.into_inner() {
            queue.push_back(inner_node);
          }
          continue; // Skip the rest of the loop since we're not processing a bool_query directly
        }
        _ => Err(AstError::UnsupportedQuery(format!(
          "Unsupported rule: {:?}",
          node.as_rule()
        ))),
      };

      match processing_result {
        Ok(node_results) => {
          if must {
            if results.is_empty() {
              results = node_results;
            } else {
              results = results.intersection(&node_results).cloned().collect();
            }
          } else {
            results.extend(node_results);
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

  /// Term Query Processor: https://opensearch.org/docs/latest/query-dsl/term/term/
  async fn process_term_query(&self, root_node: &Pair<'_, Rule>) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut fieldname: Option<&str> = None;
    let mut query_text: Option<&str> = None;
    let mut case_insensitive = true;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::fieldname => {
          if let Some(field) = node.into_inner().next() {
            fieldname = Some(field.as_str());
          }
        }
        Rule::value => {
          query_text = node.into_inner().next().map(|v| v.as_str());
        }
        Rule::case_insensitive => {
          case_insensitive = node
            .into_inner()
            .next()
            .map(|v| v.as_str().parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    // "AND" is needed even though term queries only have a single term.
    // Our tokenizer breaks up query text on spaces, etc. so we need to match
    // everything in the query text if they end up as different terms.
    //
    // TODO: This is technically incorrect as the term should be an exact string match.
    if let Some(query_str) = query_text {
      self
        .process_search(
          self
            .analyze_query_text(query_str, fieldname, case_insensitive)
            .await,
          "AND",
        )
        .await
    } else {
      Err(AstError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ))
    }
  }

  /// Terms Query Processor: https://opensearch.org/docs/latest/query-dsl/term/terms/
  async fn process_terms_query(
    &self,
    root_node: &Pair<'_, Rule>,
  ) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut fieldname: Option<&str> = None;
    let mut query_values: Vec<&str> = Vec::new();

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::fieldname => {
          if let Some(field) = node.into_inner().next() {
            fieldname = Some(field.as_str());
          }
        }
        Rule::field_element => {
          query_values.push(node.into_inner().next().map(|v| v.as_str()).unwrap_or(""));
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    // Creating the vector of all search strings in the format "field1~field1element", "field1~field2element"
    // etc from the field_element array in the query.
    let query_terms: Vec<String> = futures::stream::iter(query_values)
      .then(|term| async move { self.analyze_query_text(term, fieldname, false).await })
      .collect::<Vec<_>>()
      .await
      .into_iter()
      .flatten()
      .collect();

    // Using the "OR" operator to get all the logs with any of the field elements from the logs.
    if !query_terms.is_empty() {
      self.process_search(query_terms, "OR").await
    } else {
      Err(AstError::UnsupportedQuery(
        "No query terms found".to_string(),
      ))
    }
  }

  /// Match Query Processor: https://opensearch.org/docs/latest/query-dsl/full-text/match/
  async fn process_match_query(
    &self,
    root_node: &Pair<'_, Rule>,
  ) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut fieldname: Option<&str> = None;
    let mut query_text: Option<&str> = None;
    let mut term_operator: &str = "OR"; // Match queries default to OR
    let mut case_insensitive = true;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::fieldname => {
          if let Some(field) = node.into_inner().next() {
            fieldname = Some(field.as_str());
          }
        }
        Rule::operator => {
          term_operator = node.into_inner().next().map_or("OR", |v| v.as_str());
        }
        Rule::match_string | Rule::query => {
          query_text = node.into_inner().next().map(|v| v.as_str());
        }
        Rule::case_insensitive => {
          case_insensitive = node
            .into_inner()
            .next()
            .map(|v| v.as_str().parse::<bool>().unwrap_or(true))
            .unwrap_or(true);
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    if let Some(query_str) = query_text {
      self
        .process_search(
          self
            .analyze_query_text(query_str, fieldname, case_insensitive)
            .await,
          term_operator,
        )
        .await
    } else {
      Err(AstError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ))
    }
  }

  /// Match Phrase Query Processor: https://opensearch.org/docs/latest/query-dsl/full-text/match-phrase/
  async fn process_match_phrase_query(
    &self,
    root_node: &Pair<'_, Rule>,
  ) -> Result<HashSet<u32>, AstError> {
    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut fieldname: Option<&str> = None;
    let mut query_text: Option<&str> = None;

    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::fieldname => {
          if let Some(field) = node.into_inner().next() {
            fieldname = Some(field.as_str());
          }
        }
        Rule::match_phrase_string | Rule::query => {
          query_text = node.into_inner().next().map(|v| v.as_str());
        }
        _ => {
          for inner_node in node.into_inner() {
            stack.push_back(inner_node);
          }
        }
      }
    }

    match (fieldname, query_text) {
      (Some(field), Some(query_str)) => {
        let search_result = self
          .process_search(
            self.analyze_query_text(query_str, Some(field), false).await,
            "AND",
          )
          .await?;

        let search_result_vec: Vec<_> = search_result.into_iter().collect();

        // From the given document IDs, filter document ids which contain the exact phrase (in the given field)
        // and return the specific document ids
        let matching_document_ids = self.find_exact_phrase_matches(
          &search_result_vec,
          Some(field),
          query_str.trim_matches('"'),
        );

        Ok(matching_document_ids)
      }
      (None, _) => Err(AstError::UnsupportedQuery(
        "Field name is missing".to_string(),
      )),
      (_, None) => Err(AstError::UnsupportedQuery(
        "Query string is missing".to_string(),
      )),
    }
  }

  /// Prep the query terms for the search

  async fn analyze_query_text(
    &self,
    query_text: &str,
    fieldname: Option<&str>,
    case_insensitive: bool,
  ) -> Vec<String> {
    // Prepare the query string, applying lowercase if case_insensitive is set
    let query = if case_insensitive {
      query_text.to_lowercase()
    } else {
      query_text.to_owned()
    };

    let mut terms = Vec::new();
    tokenize(&query, &mut terms);

    // If fieldname is provided, concatenate it with each term; otherwise, use the term as is
    let transformed_terms: Vec<String> = if let Some(field) = fieldname {
      let prefix = format!("{}{}", field, FIELD_DELIMITER); // Prepare the prefix once
      terms
        .into_iter()
        .map(|term| format!("{}{}", prefix, term))
        .collect()
    } else {
      terms.into_iter().map(|term| term.to_owned()).collect() // No fieldname, just clone the term
    };

    transformed_terms
  }

  /// Search the index for the terms extracted from the AST
  async fn process_search(
    &self,
    terms: Vec<String>,
    term_operator: &str,
  ) -> Result<HashSet<u32>, AstError> {
    // Extract the terms and perform the search
    let mut results = HashSet::new();

    // Get postings lists for the query terms
    let (postings_lists, last_block_list, initial_values_list, shortest_list_index) = self
      .get_postings_lists(&terms)
      .map_err(|e| AstError::TraverseError(format!("Error getting postings lists: {:?}", e)))?;

    if postings_lists.is_empty() {
      debug!("No posting list found. Returning empty handed.");
      return Ok(HashSet::new());
    }

    // Now get the doc IDs in the posting lists
    if term_operator == "OR" {
      self
        .get_matching_doc_ids_with_logical_or(&postings_lists, &last_block_list, &mut results)
        .map_err(|e| AstError::TraverseError(format!("Error matching doc IDs: {:?}", e)))?;
    } else {
      self
        .get_matching_doc_ids_with_logical_and(
          &postings_lists,
          &last_block_list,
          &initial_values_list,
          shortest_list_index,
          &mut results,
        )
        .map_err(|e| AstError::TraverseError(format!("Error matching doc IDs: {:?}", e)))?;
    }

    Ok(results)
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
      ("log 2", "this is another log message field1value"),
      ("log 3 1", "test log for different term"),
      ("log 4", "field1~field1value testing field name and value"),
      ("log 5", "field1~field2value testing field name two value"),
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

  #[tokio::test]
  async fn test_search_with_must_query() {
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
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

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

  #[tokio::test]
  async fn test_search_with_should_query() {
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
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

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

  #[tokio::test]
  async fn test_search_with_must_not_query() {
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
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
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

  #[tokio::test]
  async fn test_search_with_boolean_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
        "query": {
            "bool": {
                "must": [
                    { "match": { "_all": { "query": "this" } } }
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
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

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

  #[tokio::test]
  async fn test_search_with_match_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "match": {
          "key": {
            "query": "1"
          }
        }
      }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

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

  #[tokio::test]
  async fn test_search_with_match_phrase_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "match_phrase": {
          "key": {
            "query": "log 1"
          }
        }
      }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            1,
            "There should be exactly 1 log matching the query."
          );

          for log in results {
            let key_field_value = log.get_fields().get("key");

            assert!(
              key_field_value.map_or(false, |value| value.contains("log 1")),
              "Each log should have 'key' field containing 'log 1'."
            );
          }
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

  #[tokio::test]
  async fn test_search_with_match_all_query_with_multiple_terms_anded() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
        "query": {
            "match": {
                "_all": {
                    "query": "log test",
                    "operator": "AND"
                }
            }
        }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          for log in results.iter() {
            assert!(
              log.get_text().contains("test") && log.get_text().contains("log"),
              "Each log should contain both 'test' and 'log'."
            );
          }
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

  #[tokio::test]
  async fn test_search_with_match_all_query_with_multiple_terms() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "match": {
          "_all": {
            "query": "another different"
          }
        }
      }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "Default OR on terms should have 2 logs matching the query."
          );

          for log in results.iter() {
            assert!(
              log.get_text().contains("another") || log.get_text().contains("different"),
              "Each log should contain 'another' or 'different'."
            );
          }
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

  #[tokio::test]
  async fn test_search_with_match_all_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "match": {
          "_all": {
            "query": "log"
          }
        }
      }
    }
    "#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
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

  #[tokio::test]
  async fn test_search_with_term_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "term": {
          "field1": {
            "value": "field1value"
          }
        }
      }
    }
    "#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert!(results
            .iter()
            .all(|log| log.get_text().contains("field1~field1value")));
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

  #[tokio::test]
  async fn test_search_with_terms_array_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
      "query": {
        "terms": {
          "field1": [
            "field1value",
            "field2value"
          ]
        }
      }
    }
    "#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert!(results
            .iter()
            .all(|log| log.get_text().contains("field1~field1value")
              || log.get_text().contains("field1~field2value")));
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

  #[tokio::test]
  async fn test_search_with_nested_boolean_query() {
    let segment = create_mock_segment();

    let query_dsl_query = r#"{
        "query": {
            "bool": {
                "must": [
                    { "match": { "_all": { "query": "another" } } }
                ],
                "should": [
                    {
                        "bool": {
                            "must": [
                                { "match": { "_all": { "query": "different" } } }
                            ]
                        }
                    }
                ]
            }
        }
    }"#;

    match QueryDslParser::parse(Rule::start, query_dsl_query) {
      Ok(ast) => match segment.search_logs(&ast, 0, u64::MAX).await {
        Ok(results) => {
          assert_eq!(
            results.len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          for log in results.iter() {
            assert!(
              log.get_text().contains("another") || log.get_text().contains("different"),
              "Each log should contain 'another' or 'different'."
            );
          }
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

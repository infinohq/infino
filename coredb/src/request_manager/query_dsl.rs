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
use crate::utils::error::QueryError;
use crate::utils::request::{analyze_query_text, check_query_time, parse_time_range};

use chrono::Utc;
use futures::StreamExt;
use log::debug;
use pest::iterators::{Pair, Pairs};
use std::collections::VecDeque;

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

use super::query_dsl_object::QueryDSLDocIds;

#[derive(Parser)]
#[grammar = "src/request_manager/query_dsl_grammar.pest"]

pub struct QueryDslParser;

impl Segment {
  pub fn parse_query(json_query: &str) -> Result<pest::iterators::Pairs<'_, Rule>, QueryError> {
    QueryDslParser::parse(Rule::start, json_query)
      .map_err(|e| QueryError::JsonParseError(e.to_string()))
  }

  /// Walk the AST using an iterator and process each node
  pub async fn traverse_query_dsl_ast(
    &self,
    nodes: &Pairs<'_, Rule>,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Traversing AST {:?}", nodes);

    let mut stack = VecDeque::new();

    // Push all nodes to the stack to start processing
    for node in nodes.clone() {
      stack.push_back(node);
    }

    let mut results = QueryDSLDocIds::new();

    // Pop the nodes off the stack and process
    while let Some(node) = stack.pop_front() {
      let processing_result = self.query_dispatcher(&node);

      match processing_result.await {
        Ok(node_results) => {
          results = node_results;
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

    debug!(
      "QueryDSL: Returning results from traverse ast {:?}",
      results
    );

    Ok(results)
  }

  /// General dispatcher for query processing
  async fn query_dispatcher(&self, node: &Pair<'_, Rule>) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Dispatching for AST {:?}", node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut results = QueryDSLDocIds::new();

    let mut timeout: u64 = 0;

    match node.as_rule() {
      Rule::timeout => {
        if let Some(inner_node) = node.clone().into_inner().next() {
          match inner_node.as_rule() {
            Rule::duration => {
              let duration_str = inner_node.as_str();
              // Now use parse_time_range to get Duration
              timeout = parse_time_range(duration_str).unwrap().num_seconds() as u64;
            }
            _ => eprintln!("Unexpected rule under timeout: {:?}", inner_node.as_rule()),
          }
        }
      }
      Rule::term_query => {
        results = self.process_term_query(node, timeout).await?;
      }
      Rule::match_query => {
        results = self.process_match_query(node, timeout).await?;
      }
      Rule::bool_query => {
        results = self.process_bool_query(node, timeout).await?;
      }
      Rule::terms_query => {
        results = self.process_terms_query(node, timeout).await?;
      }
      Rule::match_phrase_query => {
        results = self.process_match_phrase_query(node, timeout).await?;
      }
      _ => {
        return Err(QueryError::UnsupportedQuery(format!(
          "Unsupported rule: {:?}",
          node.as_rule()
        )));
      }
    }

    let execution_time = check_query_time(timeout, query_start_time)?;
    results.set_execution_time(execution_time);

    debug!(
      "QueryDSL: Returning results from query dispatcher {:?}",
      results
    );

    Ok(results)
  }

  // Boolean Query Processor: https://opensearch.org/docs/latest/query-dsl/compound/bool/
  async fn process_bool_query(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Processing boolean query {:?}", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut stack: VecDeque<Pair<Rule>> = VecDeque::new();
    stack.push_back(root_node.clone());

    let mut must_results = QueryDSLDocIds::new();
    let mut should_results = QueryDSLDocIds::new();
    let mut must_not_results = QueryDSLDocIds::new();

    // Process each subtree separately, then combine the logical results afterwards.
    while let Some(node) = stack.pop_front() {
      match node.as_rule() {
        Rule::must_clauses => {
          must_results = self.process_bool_subtree(&node, true, timeout).await?;
        }
        Rule::should_clauses => {
          should_results = self.process_bool_subtree(&node, false, timeout).await?;
        }
        Rule::must_not_clauses => {
          must_not_results = self.process_bool_subtree(&node, false, timeout).await?;
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
    let mut results = if !must_results.get_ids().is_empty() || !should_results.get_ids().is_empty()
    {
      let mut results = must_results;
      if !should_results.get_ids().is_empty() {
        results.or(&should_results);
      }
      results
    } else {
      QueryDSLDocIds::new()
    };

    // Now get final results after excluding must_not results
    results.not(&must_not_results);

    let execution_time = check_query_time(timeout, query_start_time)?;
    results.set_execution_time(execution_time);

    debug!("QueryDSL: Returning results from bool query {:?}", results);

    Ok(results)
  }

  async fn process_bool_subtree(
    &self,
    root_node: &Pair<'_, Rule>,
    must: bool,
    timeout: u64,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Processing boolean subtree {:?}", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

    let mut queue = VecDeque::new();
    queue.push_back(root_node.clone());

    let mut results = QueryDSLDocIds::new();

    while let Some(node) = queue.pop_front() {
      // TODO: Do we need to replicate these calls here or can we just push on the stack?
      let processing_result = match node.as_rule() {
        Rule::term_query => self.process_term_query(&node, timeout).await,
        Rule::match_query => self.process_match_query(&node, timeout).await,
        Rule::bool_query => {
          // For bool_query, instead of processing, we queue its children for processing
          for inner_node in node.into_inner() {
            queue.push_back(inner_node);
          }
          continue; // Skip the rest of the loop since we're not processing a bool_query directly
        }
        _ => Err(QueryError::UnsupportedQuery(format!(
          "Unsupported rule: {:?}",
          node.as_rule()
        ))),
      };

      match processing_result {
        Ok(mut node_results) => {
          if must {
            if results.get_ids().is_empty() {
              results.set_ids(node_results.take_ids());
            } else {
              results.and(&node_results);
            }
          } else {
            results.or(&node_results);
          }
        }
        Err(QueryError::UnsupportedQuery(_)) => {
          for child_node in node.into_inner() {
            queue.push_back(child_node);
          }
        }
        Err(e) => return Err(e),
      }
    }

    let execution_time = check_query_time(timeout, query_start_time)?;
    results.set_execution_time(execution_time);

    debug!(
      "QueryDSL: Returning results from bool subtree {:?}",
      results
    );

    Ok(results)
  }

  /// Term Query Processor: https://opensearch.org/docs/latest/query-dsl/term/term/
  async fn process_term_query(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Processing term query {:?}", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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

    let mut results = QueryDSLDocIds::new();

    // "AND" is needed even though term queries only have a single term.
    // Our tokenizer breaks up query text on spaces, etc. so we need to match
    // everything in the query text if they end up as different terms.
    //
    // TODO: This is technically incorrect as the term should be an exact string match.
    if let Some(query_str) = query_text {
      let analyzed_query = analyze_query_text(query_str, fieldname, case_insensitive).await;
      let search_results = self.search_inverted_index(analyzed_query, "AND").await?;

      let execution_time = check_query_time(timeout, query_start_time)?;
      results.set_execution_time(execution_time);
      results.set_ids(search_results);
    } else {
      return Err(QueryError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ));
    };

    debug!("QueryDSL: Returning results from term query {:?}", results);

    Ok(results)
  }

  /// Terms Query Processor: https://opensearch.org/docs/latest/query-dsl/term/terms/
  async fn process_terms_query(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Processing terms query {:?}", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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
      .then(|term| async move { analyze_query_text(term, fieldname, false).await })
      .collect::<Vec<_>>()
      .await
      .into_iter()
      .flatten()
      .collect();

    let mut results = QueryDSLDocIds::new();

    // Using the "OR" operator to get all the logs with any of the field elements from the logs.
    if !query_terms.is_empty() {
      let search_results = self.search_inverted_index(query_terms, "OR").await?;

      let execution_time = check_query_time(timeout, query_start_time)?;
      results.set_execution_time(execution_time);
      results.set_ids(search_results);
    } else {
      return Err(QueryError::UnsupportedQuery(
        "No query terms found".to_string(),
      ));
    };

    debug!("QueryDSL: Returning results from terms query {:?}", results);

    Ok(results)
  }

  /// Match Query Processor: https://opensearch.org/docs/latest/query-dsl/full-text/match/
  async fn process_match_query(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Processing match query {:?}", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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

    let mut results = QueryDSLDocIds::new();

    if let Some(query_str) = query_text {
      let analyzed_query = analyze_query_text(query_str, fieldname, case_insensitive).await;
      let search_results = self
        .search_inverted_index(analyzed_query, term_operator)
        .await?;

      let execution_time = check_query_time(timeout, query_start_time)?;
      results.set_execution_time(execution_time);
      results.set_ids(search_results);
    } else {
      return Err(QueryError::UnsupportedQuery(
        "Query string is missing".to_string(),
      ));
    };

    debug!("QueryDSL: Returning results from match query {:?}", results);

    Ok(results)
  }

  /// Match Phrase Query Processor: https://opensearch.org/docs/latest/query-dsl/full-text/match-phrase/
  async fn process_match_phrase_query(
    &self,
    root_node: &Pair<'_, Rule>,
    timeout: u64,
  ) -> Result<QueryDSLDocIds, QueryError> {
    debug!("QueryDSL: Processing match phrase query {:?}", root_node);

    let query_start_time = Utc::now().timestamp_millis() as u64;

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
          .search_inverted_index(
            analyze_query_text(query_str, Some(field), false).await,
            "AND",
          )
          .await?;

        let mut results = QueryDSLDocIds::new();

        // From the given document IDs, filter document ids which contain the exact phrase (in the given field)
        // and return the specific document ids
        let matching_document_ids =
          self.get_exact_phrase_matches(&search_result, Some(field), query_str.trim_matches('"'));

        let execution_time = check_query_time(timeout, query_start_time)?;
        results.set_execution_time(execution_time);
        results.set_ids(matching_document_ids);

        debug!(
          "QueryDSL: Returning results from match phrase query {:?}",
          results
        );

        Ok(results)
      }
      (None, _) => Err(QueryError::UnsupportedQuery(
        "Field name is missing".to_string(),
      )),
      (_, None) => Err(QueryError::UnsupportedQuery(
        "Query string is missing".to_string(),
      )),
    }
  }
}

#[cfg(test)]
mod tests {

  use std::collections::HashMap;

  use chrono::Utc;

  use crate::utils::config::config_test_logger;

  use super::*;
  use pest::Parser;

  async fn create_mock_segment() -> Segment {
    config_test_logger();

    let segment = Segment::new_with_temp_wal();

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
        .await
        .unwrap();
    }

    segment
  }

  #[tokio::test]
  async fn test_search_with_must_query() {
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          assert!(results.get_messages().iter().all(|log| log
            .get_message()
            .get_text()
            .contains("test")
            && log.get_message().get_text().contains("log")));
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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          assert!(results.get_messages().iter().any(|log| log
            .get_message()
            .get_text()
            .contains("another")
            || log.get_message().get_text().contains("different")));
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
    let segment = create_mock_segment().await;

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
            .get_messages()
            .iter()
            .any(|log| log.get_message().get_text().contains("excluded")));
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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          assert!(results.get_messages().iter().all(|log| {
            log.get_message().get_text().contains("log")
              && (log.get_message().get_text().contains("test")
                || !log.get_message().get_text().contains("different"))
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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          assert!(results
            .get_messages()
            .iter()
            .all(|log| log.get_message().get_text().contains("log")));
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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            1,
            "There should be exactly 1 log matching the query."
          );

          for log in results.get_messages() {
            let key_field_value = log.get_message().get_fields().get("key");

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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          for log in results.get_messages().iter() {
            assert!(
              log.get_message().get_text().contains("test")
                && log.get_message().get_text().contains("log"),
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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "Default OR on terms should have 2 logs matching the query."
          );

          for log in results.get_messages().iter() {
            assert!(
              log.get_message().get_text().contains("another")
                || log.get_message().get_text().contains("different"),
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
    let segment = create_mock_segment().await;

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
          assert!(results
            .get_messages()
            .iter()
            .all(|log| log.get_message().get_text().contains("log")));
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
    let segment = create_mock_segment().await;

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
            .get_messages()
            .iter()
            .all(|log| log.get_message().get_text().contains("field1~field1value")));
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
    let segment = create_mock_segment().await;

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
          assert!(results.get_messages().iter().all(|log| log
            .get_message()
            .get_text()
            .contains("field1~field1value")
            || log.get_message().get_text().contains("field1~field2value")));
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
    let segment = create_mock_segment().await;

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
            results.get_messages().len(),
            2,
            "There should be exactly 2 logs matching the query."
          );

          for log in results.get_messages().iter() {
            assert!(
              log.get_message().get_text().contains("another")
                || log.get_message().get_text().contains("different"),
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

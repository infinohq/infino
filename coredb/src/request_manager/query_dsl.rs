// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Execute an Infino query. Both Query DSL and Lucene Query Syntax are supported.
//!
//! Uses the Pest parser with Pest-formatted PEG grammars: https://pest.rs/,
//! which is a cleaner and more optimized approach than building our own AST.

use crate::segment_manager::segment::Segment;
use crate::utils::error::AstError;
use crate::utils::tokenize::tokenize;

use log::debug;
use pest::iterators::{Pair, Pairs};
use std::collections::HashSet;

#[allow(unused_imports)]
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/request_manager/query_dsl_grammar.pest"]

pub struct QueryDslParser;

pub fn traverse_ast(segment: &Segment, nodes: &Pairs<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut results = HashSet::new();
  for node in nodes.clone() {
    let accumulated_results = process_ast_node(segment, &node)?;
    results.extend(accumulated_results);
  }

  Ok(results)
}

fn process_ast_node(segment: &Segment, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  match node.as_rule() {
    Rule::term_query => process_term_query(segment, node),
    Rule::match_query => process_match_query(segment, node),
    Rule::bool_query => process_bool_query(segment, node),
    _ => node
      .clone()
      .into_inner()
      .try_fold(HashSet::new(), |acc, p| {
        process_ast_node(segment, &p).map(|set| acc.union(&set).cloned().collect())
      }),
  }
}

fn process_bool_query(
  segment: &Segment,
  bool_query_node: &Pair<Rule>,
) -> Result<HashSet<u32>, AstError> {
  let mut must_results = HashSet::new();
  let mut should_results = HashSet::new();
  let mut must_not_results = HashSet::new();

  for bool_query_child in bool_query_node.clone().into_inner() {
    match bool_query_child.as_rule() {
      Rule::bool_search => {
        for bool_search_child in bool_query_child.clone().into_inner() {
          match bool_search_child.as_rule() {
            Rule::must_clauses => {
              let results = process_must_clause(segment, &bool_search_child)?;
              must_results.extend(results);
            }
            Rule::should_clauses => {
              let results = process_should_clause(segment, &bool_search_child)?;
              should_results.extend(results);
            }
            Rule::must_not_clauses => {
              let results = process_must_not_clause(segment, &bool_search_child, &must_results)?;
              must_not_results.extend(results);
            }
            _ => {
              if is_branch(&bool_search_child.as_rule())? {
                continue;
              }
            }
          }
        }
      }
      _ => {
        if is_branch(&bool_query_child.as_rule())? {
          continue;
        }
      }
    }
  }

  // Assuming you have a function to handle the combination of results:
  let combined_results = if should_results.is_empty() {
    must_results
  } else {
    // Assuming `must_results` and `should_results` are sets or can be treated as such
    must_results.union(&should_results).cloned().collect()
  };

  let final_results = combined_results
    .difference(&must_not_results)
    .cloned()
    .collect();

  Ok(final_results)
}

fn process_term_query(
  segment: &Segment,
  term_query_node: &Pair<Rule>,
) -> Result<HashSet<u32>, AstError> {
  let mut fieldname: Option<&str> = None;
  let mut query_string: Option<&str> = None;
  let mut case_insensitive: Option<bool> = None;

  for term_query_child_node in term_query_node.clone().into_inner() {
    match term_query_child_node.as_rule() {
      Rule::term_search => {
        for term_search_child_node in term_query_child_node.clone().into_inner() {
          match term_search_child_node.as_rule() {
            Rule::fieldname => {
              if let Some(field) = term_search_child_node.into_inner().next() {
                fieldname = Some(field.as_str());
              }
            }
            Rule::term => {
              for term_child_node in term_search_child_node.into_inner() {
                match term_child_node.as_rule() {
                  Rule::value => {
                    if let Some(value) = term_child_node.into_inner().next() {
                      query_string = Some(value.as_str());
                    }
                  }
                  Rule::case_insensitive => {
                    case_insensitive = Some(true);
                  }
                  _ => {
                    let _ = is_branch(&term_child_node.as_rule());
                  }
                }
              }
            }
            _ => {
              let _ = is_branch(&term_search_child_node.as_rule());
            }
          }
        }
      }
      _ => {
        let _ = is_branch(&term_query_child_node.as_rule());
      }
    }
  }

  if let Some(query_str) = query_string {
    process_search(
      segment,
      process_query_text(query_str, fieldname, case_insensitive),
    )
  } else {
    Err(AstError::UnsupportedQuery(
      "Query string is missing".to_string(),
    ))
  }
}

fn process_match_query(
  segment: &Segment,
  match_query_node: &Pair<Rule>,
) -> Result<HashSet<u32>, AstError> {
  let mut fieldname: Option<&str> = None;
  let mut query_string: Option<&str> = None;
  let mut case_insensitive: Option<bool> = None;

  for match_query_child_node in match_query_node.clone().into_inner() {
    match match_query_child_node.as_rule() {
      Rule::match_search => {
        for match_search_child_node in match_query_child_node.clone().into_inner() {
          match match_search_child_node.as_rule() {
            Rule::fieldname => {
              if let Some(word) = match_search_child_node.into_inner().next() {
                fieldname = Some(word.as_str());
              }
            }
            Rule::match_string => {
              if let Some(string) = match_search_child_node.into_inner().next() {
                query_string = Some(string.as_str());
              }
            }
            Rule::match_array => {
              for match_array_child_node in match_search_child_node.into_inner() {
                match match_array_child_node.as_rule() {
                  Rule::query => {
                    if let Some(string) = match_array_child_node.into_inner().next() {
                      query_string = Some(string.as_str());
                    }
                  }
                  Rule::match_terms => {
                    for match_terms_child_node in match_array_child_node.into_inner() {
                      match match_terms_child_node.as_rule() {
                        Rule::case_insensitive => {
                          case_insensitive = Some(true);
                        }
                        _ => {
                          let _ = is_branch(&match_terms_child_node.as_rule());
                        }
                      }
                    }
                  }
                  _ => {
                    let _ = is_branch(&match_array_child_node.as_rule());
                  }
                }
              }
            }
            _ => {
              let _ = is_branch(&match_search_child_node.as_rule());
            }
          }
        }
      }
      _ => {
        let _ = is_branch(&match_query_child_node.as_rule());
      }
    }
  }

  if let Some(query_str) = query_string {
    process_search(
      segment,
      process_query_text(query_str, fieldname, case_insensitive),
    )
  } else {
    Err(AstError::UnsupportedQuery(
      "Query string is missing".to_string(),
    ))
  }
}

fn process_query_text(
  query_string: &str,
  fieldname: Option<&str>,
  case_insensitive: Option<bool>,
) -> Vec<String> {
  // Prepare the query string, applying lowercase if case_insensitive is set
  let query = if case_insensitive.unwrap_or(false) {
    query_string.to_lowercase()
  } else {
    query_string.to_owned()
  };

  // Tokenize the search term
  let terms = tokenize(&query); // Assume tokenize returns Vec<String>

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

fn process_search(segment: &Segment, terms: Vec<String>) -> Result<HashSet<u32>, AstError> {
  // Extract the terms and perform the search
  let mut results = Vec::new();

  // Get postings lists for the query terms
  let (postings_lists, last_block_list, initial_values_list, shortest_list_index) = segment
    .get_postings_lists(&terms)
    .map_err(|e| AstError::TraverseError(format!("Error getting postings lists: {:?}", e)))?;

  if postings_lists.is_empty() {
    debug!("No posting list found. Returning empty handed.");
    return Ok(HashSet::new());
  }

  segment
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

fn process_must_clause(segment: &Segment, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut results = HashSet::new();
  let mut first = true;

  for child_node in node.clone().into_inner() {
    let term_results = process_ast_node(segment, &child_node)?;

    if first {
      results = term_results;
      first = false;
    } else {
      results = results.intersection(&term_results).cloned().collect();
    }
  }

  Ok(results)
}

fn process_should_clause(segment: &Segment, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut results = HashSet::new();
  for child_node in node.clone().into_inner() {
    let term_results = process_ast_node(segment, &child_node)?;
    results = results.union(&term_results).cloned().collect();
  }
  Ok(results)
}

fn process_must_not_clause(
  segment: &Segment,
  node: &Pair<Rule>,
  include_results: &HashSet<u32>,
) -> Result<HashSet<u32>, AstError> {
  let mut exclude_results = HashSet::new();
  for child_node in node.clone().into_inner() {
    let term_results = process_ast_node(segment, &child_node)?;
    exclude_results = exclude_results.union(&term_results).cloned().collect();
  }
  Ok(
    include_results
      .difference(&exclude_results)
      .cloned()
      .collect(),
  )
}

// Skip branches
fn is_branch(rule: &Rule) -> Result<bool, AstError> {
  match rule {
    Rule::start
    | Rule::query_section
    | Rule::query_root
    | Rule::leaf_query
    | Rule::full_text_query
    | Rule::term_level_query
    | Rule::compound_query
    | Rule::bool_search
    | Rule::match_search
    | Rule::query_array => Ok(true),
    _ => Err(AstError::UnsupportedQuery(format!(
      "Unsupported query term: {:?}",
      rule
    ))),
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use chrono::Utc;

  use super::*;
  use crate::request_manager::query_dsl::{QueryDslParser, Rule};
  use pest::Parser;

  fn populate_segment(segment: &mut Segment) {
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
  }

  #[test]
  fn test_search_with_must_query() {
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let query_dsl_query = r#"{
      "query": {
        "bool": {
          "must": [
            { "match": { 
              "_all" : {
                "query": "test" } 
              } 
            }
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
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let query_dsl_query = r#"{
      "query": {
        "bool": {
          "should": [
            { "match": { 
              "_all" : {
                "query": "test" } 
              } 
            }
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
    let mut segment = Segment::new();
    populate_segment(&mut segment);

    let query_dsl_query = r#"{
      "query": {
        "bool": {
          "must_not": [
            { "match": { 
              "_all" : {
                "query": "different" } 
              } 
            }
          ]
        }
      }
    }
    "#;

    // Parse the query DSL
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
}

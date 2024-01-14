// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Execute an Infino query. Both Query DSL and Lucene Query Syntax are supported.
//!
//! Uses the Pest parser with Pest-formatted PEG grammars: https://pest.rs/,
//! which is a cleaner and more optimized approach than building our own AST.

use crate::segment_manager::search::get_matching_doc_ids;
use crate::segment_manager::search::get_postings_lists;
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
  if is_supported_rule(&node.as_rule()) {
    match node.as_rule() {
      Rule::quoted_string => process_terms(segment, node),
      Rule::set_of_terms => process_terms(segment, node),
      Rule::bool_query => process_boolean_query(segment, node),

      // For rules that we support but are not operators, recursively process inner pairs
      _ => node
        .clone()
        .into_inner()
        .try_fold(HashSet::new(), |acc, p| {
          process_ast_node(segment, &p).map(|set| acc.union(&set).cloned().collect())
        }),
    }
  } else {
    Err(AstError::UnsupportedQuery(format!(
      "Unsupported query term: {:?}",
      node.as_rule()
    )))
  }
}

// Process the different parts of the boolean: must (AND), should (OR), and must not (NOT).
// Filters are not yet supported.
fn process_boolean_query(segment: &Segment, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut must_results = HashSet::new();
  let mut should_results = HashSet::new();
  let mut must_not_results = HashSet::new();

  for child_node in node.clone().into_inner() {
    if is_supported_rule(&child_node.as_rule()) {
      match child_node.as_rule() {
        Rule::must_clauses => {
          let results = process_must_clause(segment, &child_node)?;
          must_results.extend(results);
        }
        Rule::should_clauses => {
          let results = process_should_clause(segment, &child_node)?;
          should_results.extend(results);
        }
        Rule::must_not_clauses => {
          let results = process_must_not_clause(segment, &child_node, &must_results)?;
          must_not_results.extend(results);
        }
        // Additional supported rules can be added here if needed
        _ => continue, // Skip other supported but non-relevant rules
      }
    } else {
      return Err(AstError::UnsupportedQuery(format!(
        "Unsupported query term: {:?}",
        child_node.as_rule()
      )));
    }
  }

  // Combine results: Must AND Should, then exclude Must Not
  let combined_results = if should_results.is_empty() {
    must_results
  } else {
    must_results.union(&should_results).cloned().collect()
  };

  let final_results = combined_results
    .difference(&must_not_results)
    .cloned()
    .collect();
  Ok(final_results)
}

fn process_terms(segment: &Segment, node: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  // Extract the terms and perform the search
  let terms_lowercase = node.as_str().to_lowercase();
  let terms = tokenize(&terms_lowercase);
  let mut results = Vec::new();

  // Get postings lists for the query terms
  let (postings_lists, last_block_list, initial_values_list, shortest_list_index) =
    get_postings_lists(segment, &terms)
      .map_err(|e| AstError::TraverseError(format!("Error getting postings lists: {:?}", e)))?;

  if postings_lists.is_empty() {
    debug!("No posting list found. Returning empty handed.");
    return Ok(HashSet::new());
  }

  get_matching_doc_ids(
    &postings_lists,
    &last_block_list,
    &initial_values_list,
    shortest_list_index,
    &mut results,
  )
  .map_err(|e| AstError::TraverseError(format!("Error matching doc IDs: {:?}", e)))?;

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

// Ignore name similarity with parser functions
#[allow(clippy::match_like_matches_macro)]
fn is_supported_rule(rule: &Rule) -> bool {
  match rule {
    Rule::quoted_string
    | Rule::set_of_terms
    | Rule::should_clauses
    | Rule::must_clauses
    | Rule::must_not_clauses
    | Rule::bool_query
    | Rule::bracketed_query
    | Rule::search_on_field
    | Rule::end_brace
    | Rule::end_bracket
    | Rule::word
    | Rule::fieldname
    | Rule::start
    | Rule::query
    | Rule::query_section
    | Rule::match_query
    | Rule::search_on_all
    | Rule::start_brace
    | Rule::colon
    | Rule::start_bracket => true,
    _ => false,
  }
}

//! Execute an Infino query. Both Query DSL and Lucene Query Syntax are supported.
//!
//! Uses the Pest parser with Pest-formatted PEG grammars: https://pest.rs/

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

pub(crate) enum AstNode<'a> {
  Single(&'a Pair<'a, Rule>),
  Multiple(&'a Pairs<'a, Rule>),
}

pub fn traverse_ast(segment: &Segment, ast_node: AstNode) -> Result<HashSet<u32>, AstError> {
  let mut results = HashSet::new();

  match ast_node {
    AstNode::Single(pair) => {
      let partial_results = process_single_pair(segment, pair)?;
      results.extend(partial_results);
    }
    AstNode::Multiple(pairs) => {
      for pair in pairs.clone() {
        let partial_results = process_single_pair(segment, &pair)?;
        results.extend(partial_results);
      }
    }
  }

  Ok(results)
}

fn process_single_pair(segment: &Segment, pair: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  match pair.as_rule() {
    Rule::set_of_terms => process_terms(segment, pair),
    Rule::bool_query => process_boolean_query(segment, pair),
    // Add other rules as needed
    _ => Err(AstError::UnsupportedQuery(format!(
      "Unsupported query: {:?}",
      pair.as_rule()
    ))),
  }
}

fn process_boolean_query(segment: &Segment, pair: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut must_results = HashSet::new();
  let mut should_results = HashSet::new();
  let mut must_not_results = HashSet::new();

  for inner_pair in pair.clone().into_inner() {
    match inner_pair.as_rule() {
      Rule::must_clauses => {
        let results = handle_must_clauses(segment, &inner_pair)?;
        must_results.extend(results);
      }
      Rule::should_clauses => {
        let results = handle_should_clauses(segment, &inner_pair)?;
        should_results.extend(results);
      }
      Rule::must_not_clauses => {
        let results = handle_must_not_clauses(segment, &inner_pair, &must_results)?;
        must_not_results.extend(results);
      }
      _ => {
        return Err(AstError::UnsupportedQuery(format!(
          "Unsupported query: {:?}",
          inner_pair.as_rule()
        )));
      }
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

fn process_terms(segment: &Segment, pair: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  // Extract the terms and perform the search
  let terms_lowercase = pair.as_str().to_lowercase();
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

fn handle_must_clauses(segment: &Segment, pair: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut results = HashSet::new();
  let mut first = true;

  for clause in pair.clone().into_inner() {
    let term_results = traverse_ast(segment, AstNode::Single(&clause))?;

    if first {
      results = term_results;
      first = false;
    } else {
      results = results.intersection(&term_results).cloned().collect();
    }
  }

  Ok(results)
}

fn handle_should_clauses(segment: &Segment, pair: &Pair<Rule>) -> Result<HashSet<u32>, AstError> {
  let mut results = HashSet::new();
  for clause in pair.clone().into_inner() {
    let term_results = traverse_ast(segment, AstNode::Single(&clause))?;
    results = results.union(&term_results).cloned().collect();
  }
  Ok(results)
}

fn handle_must_not_clauses(
  segment: &Segment,
  pair: &Pair<Rule>,
  include_results: &HashSet<u32>,
) -> Result<HashSet<u32>, AstError> {
  let mut exclude_results = HashSet::new();
  for clause in pair.clone().into_inner() {
    let term_results = traverse_ast(segment, AstNode::Single(&clause))?;
    exclude_results = exclude_results.union(&term_results).cloned().collect();
  }
  Ok(
    include_results
      .difference(&exclude_results)
      .cloned()
      .collect(),
  )
}

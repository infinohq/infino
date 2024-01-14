// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

/// Search a segment for matching document IDs
use crate::log::postings_block::PostingsBlock;
use crate::log::postings_block_compressed::PostingsBlockCompressed;
use crate::segment_manager::segment::Segment;
use crate::utils::error::AstError;

use log::debug;

// Get the posting lists belonging to a set of matching terms in the query
#[allow(clippy::type_complexity)]
pub fn get_postings_lists(
  segment: &Segment,
  terms: &[String],
) -> Result<
  (
    Vec<Vec<PostingsBlockCompressed>>,
    Vec<PostingsBlock>,
    Vec<Vec<u32>>,
    usize,
  ),
  AstError,
> {
  let mut initial_values_list: Vec<Vec<u32>> = Vec::new();
  let mut postings_lists: Vec<Vec<PostingsBlockCompressed>> = Vec::new();
  let mut last_block_list: Vec<PostingsBlock> = Vec::new();
  let mut shortest_list_index = 0;
  let mut shortest_list_len = usize::MAX;

  for (index, term) in terms.iter().enumerate() {
    let term_id = match segment.terms.get(term) {
      Some(term_id_ref) => *term_id_ref,
      None => {
        return Err(AstError::PostingsListError(format!(
          "Term not found: {}",
          term
        )))
      }
    };

    let postings_list = match segment.inverted_map.get(&term_id) {
      Some(postings_list_ref) => postings_list_ref,
      None => {
        return Err(AstError::PostingsListError(format!(
          "Postings list not found for term ID: {}",
          term_id
        )))
      }
    };

    let initial_values = postings_list
      .get_initial_values()
      .read()
      .map_err(|_| {
        AstError::PostingsListError("Failed to acquire read lock on initial values".to_string())
      })?
      .clone();
    initial_values_list.push(initial_values);

    let postings_block_compressed_vec: Vec<PostingsBlockCompressed> = postings_list
      .get_postings_list_compressed()
      .read()
      .map_err(|_| {
        AstError::TraverseError(
          "Failed to acquire read lock on postings list compressed".to_string(),
        )
      })?
      .iter()
      .cloned()
      .collect();

    let last_block = postings_list
      .get_last_postings_block()
      .read()
      .map_err(|_| {
        AstError::PostingsListError(
          "Failed to acquire read lock on last postings block".to_string(),
        )
      })?
      .clone();
    last_block_list.push(last_block);

    if postings_block_compressed_vec.len() < shortest_list_len {
      shortest_list_len = postings_block_compressed_vec.len();
      shortest_list_index = index;
    }

    postings_lists.push(postings_block_compressed_vec);
  }

  Ok((
    postings_lists,
    last_block_list,
    initial_values_list,
    shortest_list_index,
  ))
}

// Get the matching doc IDs corresponding to a set of posting lists
pub fn get_matching_doc_ids(
  postings_lists: &[Vec<PostingsBlockCompressed>],
  last_block_list: &[PostingsBlock],
  initial_values_list: &Vec<Vec<u32>>,
  shortest_list_index: usize,
  accumulator: &mut Vec<u32>,
) -> Result<(), AstError> {
  let first_posting_blocks = &postings_lists[shortest_list_index];
  for posting_block in first_posting_blocks {
    let posting_block = PostingsBlock::try_from(posting_block)
      .map_err(|_| AstError::DocMatchingError("Failed to convert to PostingsBlock".to_string()))?;
    let log_message_ids = posting_block.get_log_message_ids().read().map_err(|_| {
      AstError::DocMatchingError("Failed to acquire read lock on log message IDs".to_string())
    })?;
    accumulator.append(&mut log_message_ids.clone());
  }

  let last_block_log_message_ids = last_block_list[shortest_list_index]
    .get_log_message_ids()
    .read()
    .map_err(|_| {
      AstError::DocMatchingError(
        "Failed to acquire read lock on last block log message IDs".to_string(),
      )
    })?;
  accumulator.append(&mut last_block_log_message_ids.clone());

  if accumulator.is_empty() {
    debug!("Posting list is empty. Loading accumulator from last_block_list.");
    return Ok(());
  }

  for i in 0..initial_values_list.len() {
    // Skip shortest posting list as it is already used to create accumulator
    if i == shortest_list_index {
      continue;
    }
    let posting_list = &postings_lists[i];
    let initial_values = &initial_values_list[i];

    let mut temp_result_set = Vec::new();
    let mut acc_index = 0;
    let mut posting_index = 0;
    let mut initial_index = 0;

    while acc_index < accumulator.len() && initial_index < initial_values.len() {
      // If current accumulator element < initial_value element it means that
      // accumulator value is smaller than what current posting_block will have
      // so increment accumulator till this condition fails
      while acc_index < accumulator.len() && accumulator[acc_index] < initial_values[initial_index]
      {
        acc_index += 1;
      }

      if acc_index < accumulator.len() && accumulator[acc_index] > initial_values[initial_index] {
        // If current accumulator element is in between current initial_value and next initial_value
        // then check the existing posting block for matches with accumlator
        // OR if it's the last accumulator is greater than last initial value, then check the last posting block
        if (initial_index + 1 < initial_values.len()
          && accumulator[acc_index] < initial_values[initial_index + 1])
          || (initial_index == initial_values.len() - 1)
        {
          let mut _posting_block = Vec::new();

          // posting_index == posting_list.len() means that we are at last_block
          if posting_index < posting_list.len() {
            _posting_block = PostingsBlock::try_from(&posting_list[posting_index])
              .map_err(|_| {
                AstError::DocMatchingError("Failed to convert to PostingsBlock".to_string())
              })?
              .get_log_message_ids()
              .read()
              .map_err(|_| {
                AstError::DocMatchingError(
                  "Failed to acquire read lock on log message IDs".to_string(),
                )
              })?
              .clone();
          } else {
            _posting_block = last_block_list[i]
              .get_log_message_ids()
              .read()
              .map_err(|_| {
                AstError::DocMatchingError(
                  "Failed to acquire read lock on last block log message IDs".to_string(),
                )
              })?
              .clone();
          }

          // start from 1st element of posting_block as 0th element of posting_block is already checked as it was part of intial_values
          let mut posting_block_index = 1;
          while acc_index < accumulator.len() && posting_block_index < _posting_block.len() {
            match accumulator[acc_index].cmp(&_posting_block[posting_block_index]) {
              std::cmp::Ordering::Equal => {
                temp_result_set.push(accumulator[acc_index]);
                acc_index += 1;
                posting_block_index += 1;
              }
              std::cmp::Ordering::Greater => {
                posting_block_index += 1;
              }
              std::cmp::Ordering::Less => {
                acc_index += 1;
              }
            }

            // Try to see if we can skip remaining elements of the postings block
            if initial_index + 1 < initial_values.len()
              && acc_index < accumulator.len()
              && accumulator[acc_index] >= initial_values[initial_index + 1]
            {
              break;
            }
          }
        } else {
          // go to next posting_block and correspodning initial_value
          // done at end of the outer while loop
        }
      }

      // If current accumulator and initial value are same, then add it to temporary accumulator
      // and check remaining elements of the postings block
      if acc_index < accumulator.len()
        && initial_index < initial_values.len()
        && accumulator[acc_index] == initial_values[initial_index]
      {
        temp_result_set.push(accumulator[acc_index]);
        acc_index += 1;

        let mut _posting_block = Vec::new();
        // posting_index == posting_list.len() means that we are at last_block
        if posting_index < posting_list.len() {
          _posting_block = PostingsBlock::try_from(&posting_list[posting_index])
            .unwrap()
            .get_log_message_ids()
            .read()
            .unwrap()
            .clone();
        } else {
          // posting block is last block
          _posting_block = last_block_list[i]
            .get_log_message_ids()
            .read()
            .unwrap()
            .clone();
        }

        // Check the remaining elements of posting block
        let mut posting_block_index = 1;
        while acc_index < accumulator.len() && posting_block_index < _posting_block.len() {
          match accumulator[acc_index].cmp(&_posting_block[posting_block_index]) {
            std::cmp::Ordering::Equal => {
              temp_result_set.push(accumulator[acc_index]);
              acc_index += 1;
              posting_block_index += 1;
            }
            std::cmp::Ordering::Greater => {
              posting_block_index += 1;
            }
            std::cmp::Ordering::Less => {
              acc_index += 1;
            }
          }

          // Try to see if we can skip remaining elements of posting_block
          if initial_index + 1 < initial_values.len()
            && acc_index < accumulator.len()
            && accumulator[acc_index] >= initial_values[initial_index + 1]
          {
            break;
          }
        }
      }

      initial_index += 1;
      posting_index += 1;
    }

    *accumulator = temp_result_set;
  }

  Ok(())
}

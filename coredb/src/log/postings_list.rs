// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use log::trace;
use serde::{Deserialize, Serialize};

use crate::utils::error::CoreDBError;

use super::constants::BLOCK_SIZE_FOR_LOG_MESSAGES;
use super::postings_block::PostingsBlock;
use super::postings_block_compressed::PostingsBlockCompressed;

/// Represents a postings list. A postings list is made of postings blocks, all but the last one is compressed.
#[derive(Debug, Deserialize, Serialize)]
pub struct PostingsList {
  /// A vector of compressed postings blocks. All but the last one block in a postings list is compressed.
  postings_list_compressed: Vec<PostingsBlockCompressed>,

  // The last block, whish is uncompressed. Note that we only compress blocks that have 128 (i.e., BLOCK_SIZE_FOR_LOG_MESSAGES)
  // integers - so the last block may have upto BLOCK_SIZE_FOR_LOG_MESSAGES integers is stored in uncompressed form.
  last_block: PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>,

  // Store the initial value in each block separately. This is valuable for fast lookups during postings list intersections.
  // Also known as 'skip pointers' in information retrieval literature.
  initial_values: Vec<u32>,
}

impl PostingsList {
  /// Create a new empty postings list.
  pub fn new() -> Self {
    PostingsList {
      postings_list_compressed: Vec::new(),
      last_block: PostingsBlock::new(),
      initial_values: Vec::new(),
    }
  }

  // This constructor is only used in tests.
  #[cfg(test)]
  pub fn new_with_params(
    compressed_blocks: Vec<PostingsBlockCompressed>,
    last_block: PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>,
    initial_values: Vec<u32>,
  ) -> Self {
    PostingsList {
      postings_list_compressed: compressed_blocks,
      last_block,
      initial_values,
    }
  }

  /// Append a log message id to the postings list.
  pub fn append(&mut self, log_message_id: u32) -> Result<(), CoreDBError> {
    trace!("Appending log message id {}", log_message_id);

    // Flag to check if the log message if being appended should be added to initial values.
    let is_initial_value = self.postings_list_compressed.is_empty() && self.last_block.is_empty();

    // Attempt to append the log_message_id to the last block.
    match self.last_block.append(log_message_id) {
      Ok(_) => {
        if is_initial_value {
          self.initial_values.push(log_message_id);
        }
      }
      Err(e) if e == CoreDBError::CapacityFull(BLOCK_SIZE_FOR_LOG_MESSAGES) => {
        // Compress the full last block and push it to postings_list_compressed
        let postings_block_compressed = PostingsBlockCompressed::try_from(&self.last_block)
          .expect("Failed to compress postings block");
        self
          .postings_list_compressed
          .push(postings_block_compressed);

        // Reset the last_block with a new PostingsBlock and retry appending
        self.last_block = PostingsBlock::new();
        self.last_block.append(log_message_id)?;

        // Since we are creating a new last_block, add the log_message_id to initial values
        self.initial_values.push(log_message_id);
      }
      Err(e) => {
        // Return the error if any other error than CapacityFull is encountered.
        return Err(e);
      }
    }

    Ok(())
  }

  /// Get the vector of compressed postings blocks, wrapped in RwLock.
  pub fn get_postings_list_compressed(&self) -> &Vec<PostingsBlockCompressed> {
    &self.postings_list_compressed
  }

  /// Get the vector of initial values, wrapped in RwLock.
  pub fn get_initial_values(&self) -> &Vec<u32> {
    &self.initial_values
  }

  /// Get the last postings block, wrapped in RwLock.
  pub fn get_last_postings_block(&self) -> &PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
    &self.last_block
  }

  #[cfg(test)]
  pub fn flatten(&self) -> Vec<u32> {
    let mut retval: Vec<u32> = Vec::new();

    // Flatten the compressed postings blocks.
    for postings_block_compressed in &self.postings_list_compressed {
      let postings_block = PostingsBlock::try_from(postings_block_compressed).unwrap();
      let log_message_ids = postings_block.get_log_message_ids();
      retval.append(&mut log_message_ids.to_vec());
    }

    // Flatten the last block.
    let log_message_ids = self.last_block.get_log_message_ids();
    retval.append(&mut log_message_ids.to_vec());

    retval
  }
}

impl Default for PostingsList {
  fn default() -> Self {
    Self::new()
  }
}

impl PartialEq for PostingsList {
  fn eq(&self, other: &Self) -> bool {
    let initial = self.get_initial_values();
    let other_initial = other.get_initial_values();

    let compressed = self.get_postings_list_compressed();
    let other_compressed = other.get_postings_list_compressed();

    *initial == *other_initial
      && compressed == other_compressed
      && self.last_block == other.last_block
  }
}

impl Eq for PostingsList {}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::is_sync_send;

  #[test]
  fn test_empty_postings() {
    // Check if the PostingsList implements sync.
    is_sync_send::<PostingsList>();

    // Check if a newly created postings list is empty.
    let pl = PostingsList::new();
    assert_eq!(pl.get_postings_list_compressed().len(), 0);
    assert_eq!(pl.get_last_postings_block().len(), 0);
    assert_eq!(pl.get_initial_values().len(), 0);
  }

  #[test]
  fn test_postings_one_entry() {
    let mut pl = PostingsList::new();
    pl.append(100).expect("Could not append to postings list");

    // After adding only one entry, the last block as well as initial values should have it, while the
    // compressed list of postings blocks should be empty.
    assert_eq!(pl.get_postings_list_compressed().len(), 0);
    assert_eq!(pl.get_last_postings_block().len(), 1);
    assert_eq!(pl.get_initial_values().len(), 1);

    // Check that the first entry in the last block is the same as what we appended.
    let last_block_log_message_ids = pl.get_last_postings_block().get_log_message_ids();
    assert_eq!(last_block_log_message_ids[0], 100_u32);

    // Check that the first entry in the initial values is the same as what we appended.
    assert_eq!(pl.get_initial_values().first().unwrap(), &(100_u32));
  }

  #[test]
  fn test_postings_block_size_entries() {
    let mut pl = PostingsList::new();

    // Append BLOCK_SIZE_FOR_LOG_MESSAGES log message ids.
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      pl.append(i as u32)
        .expect("Could not append to postings list");
    }

    // We should have all the log messages only populated in the last block - so the postings_list_compressed should be empty.
    assert_eq!(pl.get_postings_list_compressed().len(), 0);

    // The last block should have length same as BLOCK_SIZE_FOR_LOG_MESSAGES.
    assert_eq!(
      pl.get_last_postings_block().len(),
      BLOCK_SIZE_FOR_LOG_MESSAGES
    );

    // The initial value length should be 1, with the only initial value being the very first value that was inserted.
    assert_eq!(pl.get_initial_values().len(), 1);
    assert_eq!(pl.get_initial_values().first().unwrap(), &(0_u32));
  }

  #[test]
  fn test_postings_block_size_plus_one_entries() {
    let mut pl = PostingsList::new();

    // Add BLOCK_SIZE_FOR_LOG_MESSAGES entries.
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      pl.append(i as u32)
        .expect("Could not append to postings list");
    }

    // Add an additional entry so that one more block is created.
    let second_block_initial_value = 10_001;
    pl.append(second_block_initial_value)
      .expect("Could not append to postings list");

    // There should be two blocks. The initial value of the first block should be the very first value appended,
    // while the initial value of the second block should be second_block_initial_value
    assert_eq!(pl.get_initial_values().len(), 2);
    assert_eq!(pl.get_initial_values().first().unwrap(), &(0_u32));
    assert_eq!(
      pl.get_initial_values().get(1).unwrap(),
      &second_block_initial_value
    );

    // The first block is compressed, and should have BLOCK_SIZE_FOR_LOG_MESSAGES entries.
    assert_eq!(pl.get_postings_list_compressed().len(), 1);
    let first_compressed_postings_block_lock = pl.get_postings_list_compressed();
    let first_compressed_postings_block = first_compressed_postings_block_lock.first().unwrap();
    let first_postings_block = PostingsBlock::try_from(first_compressed_postings_block).unwrap();
    assert_eq!(first_postings_block.len(), BLOCK_SIZE_FOR_LOG_MESSAGES);

    // The last block should have only 1 entry, which would be second_block_initial_value.
    assert_eq!(pl.get_last_postings_block().len(), 1);
    let last_block_log_message_ids = pl.get_last_postings_block().get_log_message_ids();
    assert_eq!(last_block_log_message_ids[0], second_block_initial_value);
  }

  #[test]
  fn test_postings_create_multiple_blocks() {
    let num_blocks: usize = 4;
    let mut pl = PostingsList::new();
    let mut expected = Vec::new();

    // Insert num_blocks*BLOCK_SIZE_FOR_LOG_MESSAGES entries.
    for i in 0..num_blocks * BLOCK_SIZE_FOR_LOG_MESSAGES {
      pl.append(i as u32)
        .expect("Could not append to postings list");
      expected.push(i as u32);
    }

    // num_blocks blocks should be created. The first num_blocks-1 of these should be compressed.
    assert_eq!(pl.get_postings_list_compressed().len(), num_blocks - 1);
    assert_eq!(pl.get_initial_values().len(), num_blocks);

    let initial_values = &pl.get_initial_values();
    for i in 0..num_blocks - 1 {
      let postings_block_compressed_lock = pl.get_postings_list_compressed();
      let postings_block_compressed = postings_block_compressed_lock.get(i).unwrap();
      let postings_block = PostingsBlock::try_from(postings_block_compressed).unwrap();
      assert_eq!(postings_block.len(), BLOCK_SIZE_FOR_LOG_MESSAGES);
      assert_eq!(initial_values[i], (i * BLOCK_SIZE_FOR_LOG_MESSAGES) as u32);
    }

    assert_eq!(
      pl.get_last_postings_block().len(),
      BLOCK_SIZE_FOR_LOG_MESSAGES
    );
    assert_eq!(
      initial_values[num_blocks - 1],
      ((num_blocks - 1) * BLOCK_SIZE_FOR_LOG_MESSAGES) as u32
    );

    let received = pl.flatten();
    assert_eq!(expected, received);
  }
}

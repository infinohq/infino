use log::trace;
use serde::{Deserialize, Serialize};

use crate::utils::custom_serde::rwlock_serde;
use crate::utils::error::TsldbError;
use crate::utils::sync::RwLock;

use super::constants::BLOCK_SIZE_FOR_LOG_MESSAGES;
use super::postings_block::PostingsBlock;
use super::postings_block_compressed::PostingsBlockCompressed;

/// Represents a postings list. A postings list is made of postings blocks, all but the last one is compressed.
#[derive(Debug, Deserialize, Serialize)]
pub struct PostingsList {
  /// A vector of compressed postings blocks. All but the last one block in a postings list is compressed.
  #[serde(with = "rwlock_serde")]
  postings_list_compressed: RwLock<Vec<PostingsBlockCompressed>>,

  // The last block, whish is uncompressed. Note that we only compress blocks that have 128 (i.e., BLOCK_SIZE_FOR_LOG_MESSAGES)
  // integers - so the last block may have upto BLOCK_SIZE_FOR_LOG_MESSAGES integers is stored in uncompressed form.
  #[serde(with = "rwlock_serde")]
  last_block: RwLock<PostingsBlock>,

  // Store the initial value in each block separately. This is valuable for fast lookups during postings list intersections.
  // Also known as 'skip pointers' in information retrieval literature.
  #[serde(with = "rwlock_serde")]
  initial_values: RwLock<Vec<u32>>,
}

impl PostingsList {
  /// Create a new empty postings list.
  pub fn new() -> Self {
    PostingsList {
      postings_list_compressed: RwLock::new(Vec::new()),
      last_block: RwLock::new(PostingsBlock::new()),
      initial_values: RwLock::new(Vec::new()),
    }
  }

  /// Append a log message id to the postings list.
  pub fn append(&self, log_message_id: u32) {
    trace!("Appending log message id {}", log_message_id);

    let mut postings_list_compressed_lock = self.postings_list_compressed.write().unwrap();
    let mut last_block_lock = self.last_block.write().unwrap();
    let mut initial_values_lock = self.initial_values.write().unwrap();

    // Flag to check if the log message if being appended should be added to initial values.
    let mut is_initial_value = false;

    if postings_list_compressed_lock.is_empty() && last_block_lock.is_empty() {
      // First insertion in this postings list - so this needs to be set as initial value as well.
      is_initial_value = true;
    }

    // Try to append the log_message_id to the last block.
    let retval = last_block_lock.append(log_message_id);

    if retval.is_err()
      && retval.err().unwrap() == TsldbError::CapacityFull(BLOCK_SIZE_FOR_LOG_MESSAGES)
    {
      // The last block is full. So,
      // (1) compress last and append it to postings_list_compressed,
      // (2) set last to an empty postings block, and append log_message_id to it.
      let postings_block_compressed = PostingsBlockCompressed::try_from(&*last_block_lock).unwrap();
      postings_list_compressed_lock.push(postings_block_compressed);
      *last_block_lock = PostingsBlock::new();
      last_block_lock.append(log_message_id).unwrap();

      // We created a new last_block - so we should add the log_message_id to initial values.
      is_initial_value = true;
    }

    if is_initial_value {
      initial_values_lock.push(log_message_id);
    }
  }

  /// Get the vector of compressed postings blocks, wrapped in RwLock.
  pub fn get_postings_list_compressed(&self) -> &RwLock<Vec<PostingsBlockCompressed>> {
    &self.postings_list_compressed
  }

  /// Get the vector of initial values, wrapped in RwLock.
  pub fn get_initial_values(&self) -> &RwLock<Vec<u32>> {
    &self.initial_values
  }

  #[cfg(test)]
  /// Get the last postings block, wrapped in RwLock.
  pub fn get_last_postings_block(&self) -> &RwLock<PostingsBlock> {
    &self.last_block
  }

  /// Flatten the postings list to return vector of log message ids.
  pub fn flatten(&self) -> Vec<u32> {
    let postings_list_compressed = &*self.postings_list_compressed.read().unwrap();
    let last_block = &*self.last_block.read().unwrap();
    let mut retval: Vec<u32> = Vec::new();

    // Flatten the compressed postings blocks.
    for postings_block_compressed in postings_list_compressed {
      let postings_block = PostingsBlock::try_from(postings_block_compressed).unwrap();
      let mut log_message_ids = (*postings_block.get_log_message_ids().read().unwrap()).clone();
      retval.append(&mut log_message_ids);
    }

    // Flatten the last block.
    let mut log_message_ids = (*last_block.get_log_message_ids().read().unwrap()).clone();
    retval.append(&mut log_message_ids);

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
    let initial = self.get_initial_values().read().unwrap();
    let other_initial = other.get_initial_values().read().unwrap();

    let compressed = self.get_postings_list_compressed().read().unwrap();
    let other_compressed = other.get_postings_list_compressed().read().unwrap();

    *initial == *other_initial
      && *compressed == *other_compressed
      && *self.last_block.read().unwrap() == *other.last_block.read().unwrap()
  }
}

impl Eq for PostingsList {}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn test_empty_postings() {
    // Check if the PostingsList implements sync.
    is_sync::<PostingsList>();

    // Check if a newly created postings list is empty.
    let pl = PostingsList::new();
    assert_eq!(pl.get_postings_list_compressed().read().unwrap().len(), 0);
    assert_eq!(pl.get_last_postings_block().read().unwrap().len(), 0);
    assert_eq!(pl.get_initial_values().read().unwrap().len(), 0);
  }

  #[test]
  fn test_postings_one_entry() {
    let pl = PostingsList::new();
    pl.append(100);

    // After adding only one entry, the last block as well as initial values should have it, while the
    // compressed list of postings blocks should be empty.
    assert_eq!(pl.get_postings_list_compressed().read().unwrap().len(), 0);
    assert_eq!(pl.get_last_postings_block().read().unwrap().len(), 1);
    assert_eq!(pl.get_initial_values().read().unwrap().len(), 1);

    // Check that the first entry in the last block is the same as what we appended.
    assert_eq!(
      pl.get_last_postings_block()
        .read()
        .unwrap()
        .get_log_message_ids()
        .read()
        .unwrap()
        .get(0)
        .unwrap(),
      &(100 as u32)
    );

    // Check that the first entry in the initial values is the same as what we appended.
    assert_eq!(
      pl.get_initial_values().read().unwrap().get(0).unwrap(),
      &(100 as u32)
    );
  }

  #[test]
  fn test_postings_block_size_entries() {
    let pl = PostingsList::new();

    // Append BLOCK_SIZE_FOR_LOG_MESSAGES log message ids.
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      pl.append(i as u32);
    }

    // We should have all the log messages only populated in the last block - so the postings_list_compressed should be empty.
    assert_eq!(pl.get_postings_list_compressed().read().unwrap().len(), 0);

    // The last block should have length same as BLOCK_SIZE_FOR_LOG_MESSAGES.
    assert_eq!(
      pl.get_last_postings_block().read().unwrap().len(),
      BLOCK_SIZE_FOR_LOG_MESSAGES
    );

    // The initial value length should be 1, with the only initial value being the very first value that was inserted.
    assert_eq!(pl.get_initial_values().read().unwrap().len(), 1);
    assert_eq!(
      pl.get_initial_values().read().unwrap().get(0).unwrap(),
      &(0 as u32)
    );
  }

  #[test]
  fn test_postings_block_size_plus_one_entries() {
    let pl = PostingsList::new();

    // Add BLOCK_SIZE_FOR_LOG_MESSAGES entries.
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      pl.append(i as u32);
    }

    // Add an additional entry so that one more block is created.
    let second_block_initial_value = 10_001;
    pl.append(second_block_initial_value as u32);

    // There should be two blocks. The initial value of the first block should be the very first value appended,
    // while the initial value of the second block should be second_block_initial_value
    assert_eq!(pl.get_initial_values().read().unwrap().len(), 2);
    assert_eq!(
      pl.get_initial_values().read().unwrap().get(0).unwrap(),
      &(0 as u32)
    );
    assert_eq!(
      pl.get_initial_values().read().unwrap().get(1).unwrap(),
      &second_block_initial_value
    );

    // The first block is compressed, and should have BLOCK_SIZE_FOR_LOG_MESSAGES entries.
    assert_eq!(pl.get_postings_list_compressed().read().unwrap().len(), 1);
    let first_compressed_postings_block_lock = pl.get_postings_list_compressed().read().unwrap();
    let first_compressed_postings_block = first_compressed_postings_block_lock.get(0).unwrap();
    let first_postings_block = PostingsBlock::try_from(first_compressed_postings_block).unwrap();
    assert_eq!(first_postings_block.len(), BLOCK_SIZE_FOR_LOG_MESSAGES);

    // The last block should have only 1 entry, which would be second_block_initial_value.
    assert_eq!(pl.get_last_postings_block().read().unwrap().len(), 1);
    assert_eq!(
      pl.get_last_postings_block()
        .read()
        .unwrap()
        .get_log_message_ids()
        .read()
        .unwrap()
        .get(0)
        .unwrap(),
      &second_block_initial_value
    );
  }

  #[test]
  fn test_postings_create_multiple_blocks() {
    let num_blocks: usize = 4;
    let pl = PostingsList::new();
    let mut expected = Vec::new();

    // Insert num_blocks*BLOCK_SIZE_FOR_LOG_MESSAGES entries.
    for i in 0..num_blocks * BLOCK_SIZE_FOR_LOG_MESSAGES {
      pl.append(i as u32);
      expected.push(i as u32);
    }

    // num_blocks blocks should be created. The first num_blocks-1 of these should be compressed.
    assert_eq!(
      pl.get_postings_list_compressed().read().unwrap().len(),
      num_blocks - 1
    );
    assert_eq!(pl.get_initial_values().read().unwrap().len(), num_blocks);

    let initial_values = &pl.get_initial_values();
    for i in 0..num_blocks - 1 {
      let postings_block_compressed_lock = pl.get_postings_list_compressed().read().unwrap();
      let postings_block_compressed = postings_block_compressed_lock.get(i).unwrap();
      let postings_block = PostingsBlock::try_from(postings_block_compressed).unwrap();
      assert_eq!(postings_block.len(), BLOCK_SIZE_FOR_LOG_MESSAGES);
      assert_eq!(
        initial_values.read().unwrap()[i],
        (i * BLOCK_SIZE_FOR_LOG_MESSAGES) as u32
      );
    }

    assert_eq!(
      pl.get_last_postings_block().read().unwrap().len(),
      BLOCK_SIZE_FOR_LOG_MESSAGES
    );
    assert_eq!(
      initial_values.read().unwrap()[num_blocks - 1],
      ((num_blocks - 1) * BLOCK_SIZE_FOR_LOG_MESSAGES) as u32
    );

    let received = pl.flatten();
    assert_eq!(expected, received);
  }
}

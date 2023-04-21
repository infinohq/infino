use bitpacking::BitPacker;
use log::{debug, trace};
use serde::{Deserialize, Serialize};

use crate::log::constants::{BITPACKER, BLOCK_SIZE_FOR_LOG_MESSAGES};
use crate::log::postings_block_compressed::PostingsBlockCompressed;
use crate::utils::custom_serde::rwlock_serde;
use crate::utils::error::TsldbError;
use crate::utils::sync::RwLock;

/// Represents (an uncompressed) postings block.
#[derive(Debug, Deserialize, Serialize)]
pub struct PostingsBlock {
  #[serde(with = "rwlock_serde")]
  /// Vector of log messages, wrapped in RwLock to ensure concurrent access and appends.
  log_message_ids: RwLock<Vec<u32>>,
}

impl PostingsBlock {
  /// Create a new postings block.
  pub fn new() -> Self {
    // The max capacity of a postings block is BLOCK_SIZE_FOR_LOGMESSAGES. Allocate a vector of
    // that size so that there is no performance penalty of dynamic allocations during appends.
    let log_message_ids_vec: Vec<u32> = Vec::with_capacity(BLOCK_SIZE_FOR_LOG_MESSAGES);

    // Wrap in a RwLock for ensuring correctness in concurrent access.
    let log_message_ids_rwlock = RwLock::new(log_message_ids_vec);

    PostingsBlock {
      log_message_ids: log_message_ids_rwlock,
    }
  }

  /// Create a new postings block with given log message ids.
  pub fn new_with_log_message_ids(log_message_ids: Vec<u32>) -> Self {
    let log_message_ids_rwlock = RwLock::new(log_message_ids);
    PostingsBlock {
      log_message_ids: log_message_ids_rwlock,
    }
  }

  /// Append a log message id to this postings block.
  pub fn append(&self, log_message_id: u32) -> Result<(), TsldbError> {
    trace!("Appending log message id {}", log_message_id);

    let mut log_message_ids_lock = self.log_message_ids.write().unwrap();

    if (*log_message_ids_lock).len() >= BLOCK_SIZE_FOR_LOG_MESSAGES {
      debug!(
        "The postings block capacity is full as it already has {} messages",
        BLOCK_SIZE_FOR_LOG_MESSAGES
      );
      return Err(TsldbError::CapacityFull(BLOCK_SIZE_FOR_LOG_MESSAGES));
    }

    // Note that we don't synchronize across log_message_id generation and its subsequent append to the postings block.
    // So, there is a chance that the call to append arrives out of order. Check for that scenario to make sure that
    // the log_message_ids vector is always sorted.

    if (*log_message_ids_lock).is_empty()
      || (*log_message_ids_lock).last().unwrap() < &log_message_id
    {
      // The log_message_id is already greater than the last one - so only append it at the end.
      (*log_message_ids_lock).push(log_message_id);
    } else {
      // We are inserting in a sorted list - so find the position to insert using binary search.
      let pos = log_message_ids_lock
        .binary_search(&log_message_id)
        .unwrap_or_else(|e| e);
      (*log_message_ids_lock).insert(pos, log_message_id);
    }

    Ok(())
  }

  /// Get the log message ids, wrapped in RwLock.
  pub fn get_log_message_ids(&self) -> &RwLock<Vec<u32>> {
    &self.log_message_ids
  }

  #[cfg(test)]
  /// Get the number of log message ids in this block. Note that this is used only in tests.
  pub fn len(&self) -> usize {
    self.log_message_ids.read().unwrap().len()
  }

  /// Check if this postings block is empty.
  pub fn is_empty(&self) -> bool {
    self.log_message_ids.read().unwrap().is_empty()
  }
}

impl PartialEq for PostingsBlock {
  fn eq(&self, other: &Self) -> bool {
    let v = self.get_log_message_ids().read().unwrap();
    let other_v = other.get_log_message_ids().read().unwrap();
    *v == *other_v
  }
}

impl Eq for PostingsBlock {}

impl TryFrom<&PostingsBlockCompressed> for PostingsBlock {
  type Error = TsldbError;

  /// Create a postings block from compressed postings block. (i.e., decompress a compressed postings block.)
  fn try_from(postings_block_compressed: &PostingsBlockCompressed) -> Result<Self, Self::Error> {
    // Allocate a vector equal to the block length.
    let mut decompressed = vec![0u32; BLOCK_SIZE_FOR_LOG_MESSAGES];

    // The initial value must be the same as the one passed when compressing the block.
    let initial = postings_block_compressed.get_initial();

    // The number of bits per integer must be the same as what was used during compression.
    let num_bits = postings_block_compressed.get_num_bits();

    debug!(
      "Decompressing a postings block with initial value {}, number of bits per integer {}",
      initial, num_bits
    );

    BITPACKER.decompress_sorted(
      initial,
      &postings_block_compressed
        .get_log_message_ids_compressed()
        .read()
        .unwrap(),
      &mut decompressed[..],
      num_bits,
    );

    let postings_block = PostingsBlock::new_with_log_message_ids(decompressed);

    Ok(postings_block)
  }
}

impl Default for PostingsBlock {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use test_case::test_case;

  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn test_empty_postings_block() {
    // Make sure that PostingsBlock implements sync.
    is_sync::<PostingsBlock>();

    // Verify that a new postings block is empty.
    let pb = PostingsBlock::new();
    assert_eq!(pb.log_message_ids.read().unwrap().len(), 0);
  }

  #[test]
  fn test_single_append() {
    let pb = PostingsBlock::new();
    pb.append(1000).unwrap();
    assert_eq!(pb.log_message_ids.read().unwrap()[..], [1000]);
  }

  #[test]
  fn test_block_size_appends() {
    let pb = PostingsBlock::new();
    let mut expected: Vec<u32> = Vec::new();

    // Append BLOCK_SIZE_FOR_LOG_MESSAGES integers,
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      pb.append(i as u32).unwrap();
      expected.push(i as u32);
    }

    assert_eq!(pb.log_message_ids.read().unwrap()[..], expected);
  }

  #[test]
  fn test_too_many_appends() {
    let pb = PostingsBlock::new();

    // Append BLOCK_SIZE_FOR_LOG_MESSAGES integers,
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      pb.append(i as u32).unwrap();
    }

    // If we append more than BLOCK_SIZE_FOR_LOG_MESSAGES, it should result in an error.
    let retval = pb.append(128);
    assert!(retval.is_err());
  }

  #[test_case(1; "when last uncompressed input is 1")]
  #[test_case(2; "when last uncompressed input is 2")]
  #[test_case(4; "when last uncompressed input is 4")]
  fn test_mostly_similar_values(last_uncompressed_input: u32) {
    // Compress a block that is 127 0's followed by last_uncompressed_input.
    let mut uncompressed: Vec<u32> = vec![0; 127];
    uncompressed.push(last_uncompressed_input);
    let expected = PostingsBlock::new_with_log_message_ids(uncompressed);
    let pbc = PostingsBlockCompressed::try_from(&expected).unwrap();

    // Decompress the compressed block, and verify that the result is as expected.
    let received = PostingsBlock::try_from(&pbc).unwrap();
    assert_eq!(&received, &expected);
  }

  #[test]
  fn test_incresing_by_one_values() {
    // When values are monotonically increasing by 1, only 1 bit is required to store each integer.
    let mut increasing_by_one: Vec<u32> = Vec::new();
    for i in 0..128 {
      increasing_by_one.push(i);
    }
    let expected = PostingsBlock::new_with_log_message_ids(increasing_by_one);
    let pbc = PostingsBlockCompressed::try_from(&expected).unwrap();

    // Decompress the compressed block, and verify that the result is as expected.
    let received = PostingsBlock::try_from(&pbc).unwrap();
    assert_eq!(&received, &expected);
  }
}

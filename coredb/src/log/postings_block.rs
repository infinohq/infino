// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use bitpacking::BitPacker;
use crossbeam::atomic::AtomicCell;
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::log::constants::{BITPACKER, BLOCK_SIZE_FOR_LOG_MESSAGES};
use crate::log::postings_block_compressed::PostingsBlockCompressed;
use crate::utils::custom_serde::atomic_cell_serde;
use crate::utils::error::CoreDBError;
use crate::utils::sync::{Arc, RwLock};

/// Represents (an uncompressed) postings block.
#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct PostingsBlock<const N: usize> {
  /// Number of log messages currently in the array.
  #[serde(with = "atomic_cell_serde")]
  num_log_messages: AtomicCell<usize>,

  /// Array of log messages.
  #[serde_as(as = "Arc<RwLock<[_; N]>>")]
  log_message_ids: Arc<RwLock<[u32; N]>>,
}

impl PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
  /// Create a new postings block.
  pub fn new() -> Self {
    let log_message_ids = Arc::new(RwLock::new([0; BLOCK_SIZE_FOR_LOG_MESSAGES]));
    let num_log_messages = AtomicCell::new(0);

    PostingsBlock {
      log_message_ids,
      num_log_messages,
    }
  }

  /// Create a new postings block with given log message ids (array).
  pub fn new_with_log_message_ids(
    num_log_messages: usize,
    log_message_ids: [u32; BLOCK_SIZE_FOR_LOG_MESSAGES],
  ) -> Self {
    let num_log_messages = AtomicCell::new(num_log_messages);
    let log_message_ids = Arc::new(RwLock::new(log_message_ids));

    PostingsBlock {
      num_log_messages,
      log_message_ids,
    }
  }

  /// Create a new postings block with given log message ids (vector).
  pub fn new_with_log_message_ids_vec(log_message_ids: Vec<u32>) -> Result<Self, CoreDBError> {
    let num_log_messages = log_message_ids.len();
    if num_log_messages > BLOCK_SIZE_FOR_LOG_MESSAGES {
      return Err(CoreDBError::InvalicPostingsBlock(format!(
        "Number of log messages ({}) is greater than the maximum allowed ({})",
        num_log_messages, BLOCK_SIZE_FOR_LOG_MESSAGES,
      )));
    }
    let num_log_messages = AtomicCell::new(log_message_ids.len());

    let mut log_message_ids_array = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    for (index, log_message_id) in log_message_ids.iter().enumerate() {
      log_message_ids_array[index] = *log_message_id;
    }
    let log_message_ids_array = Arc::new(RwLock::new(log_message_ids_array));

    let pb = PostingsBlock {
      num_log_messages,
      log_message_ids: log_message_ids_array,
    };

    Ok(pb)
  }

  /// Append a log message id to this postings block.
  pub fn append(&self, log_message_id: u32) -> Result<(), CoreDBError> {
    trace!("Appending log message id {}", log_message_id);

    // First, acquire a write lock so that another thread cannot change the number of log messages in the block.
    let cloned = self.log_message_ids.clone();
    let mut log_message_ids = cloned.write().unwrap();
    let num_log_messages = self.num_log_messages.load();

    if num_log_messages >= BLOCK_SIZE_FOR_LOG_MESSAGES {
      debug!(
        "The postings block capacity is full as it already has {} messages",
        BLOCK_SIZE_FOR_LOG_MESSAGES
      );
      return Err(CoreDBError::CapacityFull(BLOCK_SIZE_FOR_LOG_MESSAGES));
    }

    // Note that we don't synchronize across log_message_id generation and its subsequent append to the postings block.
    // So, there is a chance that the call to append arrives out of order. Check for that scenario to make sure that
    // the log_message_ids vector is always sorted.
    if num_log_messages == 0 || log_message_ids[num_log_messages - 1] < log_message_id {
      // The log_message_id is already greater than the last one - so only append it at the end.
      log_message_ids[num_log_messages] = log_message_id;
    } else {
      // We are inserting in a sorted list - so find the position to insert using binary search.
      let pos = log_message_ids[0..num_log_messages]
        .binary_search(&log_message_id)
        .unwrap_or_else(|e| e);

      // Insert log_message_id at the position 'pos', shifting the elements after 'pos' to the right.
      log_message_ids[pos..].rotate_right(1);
      log_message_ids[pos] = log_message_id;
    }
    // Insertion is complete - so increase the count of number of log messages.
    self.num_log_messages.store(num_log_messages + 1);

    Ok(())
  }

  /// Get the log message ids.
  pub fn get_log_message_ids(&self) -> Vec<u32> {
    let cloned = self.log_message_ids.clone();
    let log_message_ids = *cloned.read().unwrap();
    let num_log_messages = self.num_log_messages.load();
    log_message_ids[0..num_log_messages].to_vec()
  }

  /// Get the number of log message ids in this block. Note that this is used only in tests.
  pub fn len(&self) -> usize {
    self.num_log_messages.load()
  }

  /// Check if this postings block is empty.
  pub fn is_empty(&self) -> bool {
    self.num_log_messages.load() == 0
  }
}

impl PartialEq for PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
  fn eq(&self, other: &Self) -> bool {
    self.len() == other.len() && self.get_log_message_ids() == other.get_log_message_ids()
  }
}

impl Clone for PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
  fn clone(&self) -> Self {
    Self::new_with_log_message_ids_vec(self.get_log_message_ids())
      .expect("Could not clone postings block")
  }
}

impl Eq for PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {}

impl TryFrom<&PostingsBlockCompressed> for PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
  type Error = CoreDBError;

  /// Create a postings block from compressed postings block. (i.e., decompress a compressed postings block.)
  fn try_from(postings_block_compressed: &PostingsBlockCompressed) -> Result<Self, Self::Error> {
    // Allocate an array equal to the block length.
    let mut decompressed = [0u32; BLOCK_SIZE_FOR_LOG_MESSAGES];

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
      postings_block_compressed.get_log_message_ids_compressed(),
      &mut decompressed,
      num_bits,
    );

    let postings_block =
      PostingsBlock::new_with_log_message_ids(BLOCK_SIZE_FOR_LOG_MESSAGES, decompressed);

    Ok(postings_block)
  }
}

impl Default for PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES> {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use test_case::test_case;

  use super::*;
  use crate::utils::sync::is_sync_send;

  #[test]
  fn test_empty_postings_block() {
    // Make sure that PostingsBlock implements sync.
    is_sync_send::<PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>>();

    // Verify that a new postings block is empty.
    let pb = PostingsBlock::new();
    assert_eq!(pb.len(), 0);
  }

  #[test]
  fn test_single_append() {
    let pb = PostingsBlock::new();
    pb.append(1000).unwrap();
    assert_eq!(pb.get_log_message_ids()[..], [1000]);
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

    assert_eq!(pb.get_log_message_ids()[..], expected);
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
    let mut uncompressed = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    uncompressed[uncompressed.len() - 1] = last_uncompressed_input;
    let expected = PostingsBlock::new_with_log_message_ids(128, uncompressed);
    let pbc = PostingsBlockCompressed::try_from(&expected).unwrap();

    // Decompress the compressed block, and verify that the result is as expected.
    let received = PostingsBlock::try_from(&pbc).unwrap();
    assert_eq!(&received, &expected);
  }

  #[test]
  fn test_incresing_by_one_values() {
    // When values are monotonically increasing by 1, only 1 bit is required to store each integer.
    let mut increasing_by_one = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    #[allow(clippy::needless_range_loop)]
    for i in 0..BLOCK_SIZE_FOR_LOG_MESSAGES {
      increasing_by_one[i] = i as u32;
    }
    let expected = PostingsBlock::new_with_log_message_ids(128, increasing_by_one);
    let pbc = PostingsBlockCompressed::try_from(&expected).unwrap();

    // Decompress the compressed block, and verify that the result is as expected.
    let received = PostingsBlock::try_from(&pbc).unwrap();
    assert_eq!(&received, &expected);
  }

  // Write test for Clone of PostingBlock
  #[test]
  fn test_clone() {
    let pb = PostingsBlock::new();
    pb.append(1000).unwrap();
    pb.append(2000).unwrap();
    pb.append(3000).unwrap();
    assert_eq!(pb, pb.clone());

    // Assert log message ids are same but not the same pointer
    #[allow(clippy::redundant_clone)]
    let pbc = pb.clone();
    assert_eq!(pb.get_log_message_ids(), pbc.get_log_message_ids());
    assert_ne!(
      pb.get_log_message_ids().as_ptr(),
      pbc.get_log_message_ids().as_ptr()
    );
  }
}

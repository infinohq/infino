use bitpacking::BitPacker;
use crossbeam::atomic::AtomicCell;
use log::{debug, error};
use serde::{Deserialize, Serialize};

use crate::log::constants::{BITPACKER, BLOCK_SIZE_FOR_LOG_MESSAGES};
use crate::log::postings_block::PostingsBlock;
use crate::utils::custom_serde::{atomic_cell_serde, rwlock_serde};
use crate::utils::error::TsldbError;
use crate::utils::sync::RwLock;

/// Represents a delta-compressed PostingsBlock.
#[derive(Debug, Deserialize, Serialize)]
pub struct PostingsBlockCompressed {
  /// Initial value.
  #[serde(with = "atomic_cell_serde")]
  initial: AtomicCell<u32>,

  /// Number of bits per integer.
  #[serde(with = "atomic_cell_serde")]
  num_bits: AtomicCell<u8>,

  /// Vector of compressed log_message_ids.
  #[serde(with = "rwlock_serde")]
  log_message_ids_compressed: RwLock<Vec<u8>>,
}

impl PostingsBlockCompressed {
  /// Creates an emply compressed postings block.
  pub fn new() -> Self {
    PostingsBlockCompressed {
      initial: AtomicCell::new(0),
      num_bits: AtomicCell::new(0),
      log_message_ids_compressed: RwLock::new(Vec::new()),
    }
  }

  pub fn new_with_params(initial: u32, num_bits: u8, log_message_ids_compressed: &[u8]) -> Self {
    PostingsBlockCompressed {
      initial: AtomicCell::new(initial),
      num_bits: AtomicCell::new(num_bits),
      log_message_ids_compressed: RwLock::new(log_message_ids_compressed.to_owned()),
    }
  }

  /// Get the initial value.
  pub fn get_initial(&self) -> u32 {
    self.initial.load()
  }

  /// Get the number of bits used to represent each integer.
  pub fn get_num_bits(&self) -> u8 {
    self.num_bits.load()
  }

  /// Gets the vector of compressed integers.
  pub fn get_log_message_ids_compressed(&self) -> &RwLock<Vec<u8>> {
    &self.log_message_ids_compressed
  }
}

impl PartialEq for PostingsBlockCompressed {
  /// Two compressed blocks are equal if:
  /// - (a) their initial values are equal, and
  /// - (b) they both use the same number of bits to encode an integer, and
  /// - (c) the list of encoded integers for both is equal.
  fn eq(&self, other: &Self) -> bool {
    self.get_initial() == other.get_initial()
      && self.get_num_bits() == other.get_num_bits()
      && *self.log_message_ids_compressed.read().unwrap()
        == *other.log_message_ids_compressed.read().unwrap()
  }
}

impl Eq for PostingsBlockCompressed {}

impl TryFrom<&PostingsBlock> for PostingsBlockCompressed {
  type Error = TsldbError;

  /// Convert PostingsBlock to PostingsBlockCompressed, i.e. compress a postings block.
  fn try_from(postings_block: &PostingsBlock) -> Result<Self, Self::Error> {
    let log_message_ids_len = postings_block.get_log_message_ids().read().unwrap().len();
    if log_message_ids_len != BLOCK_SIZE_FOR_LOG_MESSAGES {
      // We only encode integers in blocks of BLOCK_SIZE_FOR_LOG_MESSAGES.
      error!(
        "Required length of postings block for compression {}, received length {}",
        BLOCK_SIZE_FOR_LOG_MESSAGES, log_message_ids_len
      );

      return Err(TsldbError::InvalidSize(
        log_message_ids_len,
        BLOCK_SIZE_FOR_LOG_MESSAGES,
      ));
    }

    let entire = postings_block.get_log_message_ids().read().unwrap();
    let initial: u32 = entire[0];
    let num_bits: u8 = BITPACKER.num_bits_sorted(initial, entire.as_slice());

    // A compressed block will take at most 4 bytes per-integer.
    let mut compressed = vec![0u8; 4 * BLOCK_SIZE_FOR_LOG_MESSAGES];

    // The log message ids in the postings block are already sorted, compress them using BitPacker.
    let compressed_len =
      BITPACKER.compress_sorted(initial, entire.as_slice(), &mut compressed[..], num_bits);

    // The compressed vector is only the first compressed_len entries.
    let log_message_ids_compressed_vec = compressed[0..compressed_len].to_vec();

    let log_message_ids_compressed = RwLock::new(log_message_ids_compressed_vec);

    let postings_block_compressed: Self = Self {
      initial: AtomicCell::new(initial),
      num_bits: AtomicCell::new(num_bits),
      log_message_ids_compressed,
    };

    debug!(
      "Uncompressed length (u64): {}, compressed length (u8): {}",
      log_message_ids_len, compressed_len
    );

    Ok(postings_block_compressed)
  }
}

// Write test for this Clone on PostingsBlockCompressed.
impl Clone for PostingsBlockCompressed {
  fn clone(&self) -> PostingsBlockCompressed {
    PostingsBlockCompressed::new_with_params(
      self.get_initial(),
      self.get_num_bits(),
      &self
        .get_log_message_ids_compressed()
        .read()
        .unwrap()
        .clone(),
    )
  }
}

impl Default for PostingsBlockCompressed {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use std::mem::size_of_val;

  use test_case::test_case;

  use super::*;
  use crate::utils::sync::is_sync;

  #[test]
  fn test_empty() {
    is_sync::<PostingsBlockCompressed>();

    let pbc = PostingsBlockCompressed::new();
    assert_eq!(pbc.get_initial(), 0);
    assert_eq!(pbc.get_num_bits(), 0);
    assert_eq!(pbc.log_message_ids_compressed.read().unwrap().len(), 0);
  }

  #[test]
  fn test_read_from_empty() {
    let pb = PostingsBlock::new();
    let retval = PostingsBlockCompressed::try_from(&pb);
    assert!(retval.is_err());
  }

  #[test]
  fn test_too_short_vector() {
    // We only compress integers with length BLOCK_SIZE_FOR_LOG_MESSAGES, so shorter than this
    // length should give an error.
    let short: Vec<u32> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let pb = PostingsBlock::new_with_log_message_ids(short);
    let retval = PostingsBlockCompressed::try_from(&pb);
    assert!(retval.is_err());
  }

  #[test]
  fn test_too_long_vector() {
    // We only compress integers with length BLOCK_SIZE_FOR_LOG_MESSAGES, so longer than this
    // length should give an error.
    let long: Vec<u32> = vec![1000; 1000];
    let pb = PostingsBlock::new_with_log_message_ids(long);
    let retval = PostingsBlockCompressed::try_from(&pb);
    assert!(retval.is_err());
  }

  #[test]
  fn test_all_same_values() {
    // The compression only works when the values are in monotonically increasing order.
    // When passed vector with same elements, the returned vector is empty.
    let same: Vec<u32> = vec![1000; 128];
    let pb = PostingsBlock::new_with_log_message_ids(same);
    let retval = PostingsBlockCompressed::try_from(&pb).unwrap();

    assert_eq!(retval.get_initial(), 1000);
    assert_eq!(retval.get_num_bits(), 0);
    assert!((*retval.log_message_ids_compressed.read().unwrap()).is_empty());
  }

  #[test_case(1, 16, 128; "when last uncompressed input is 1")]
  #[test_case(2, 32, 128; "when last uncompressed input is 2")]
  #[test_case(4, 48, 128; "when last uncompressed input is 4")]
  fn test_mostly_similar_values_1(
    last_uncompressed_input: u32,
    expected_length: u32,
    expected_last_compressed: u8,
  ) {
    // Compress a vector that is 127 0's followed by last_uncompressed_input. This results in a compressed vector
    // of length expected_length. The first expetced_length-1 bytes are integers from 0 to expected_length-1,
    // while the last byte is expected_last_uncompressed.

    let mut uncompressed: Vec<u32> = vec![0; 127];
    uncompressed.push(last_uncompressed_input);

    let pb = PostingsBlock::new_with_log_message_ids(uncompressed);
    let pbc = PostingsBlockCompressed::try_from(&pb).unwrap();
    assert_eq!(
      pbc.log_message_ids_compressed.read().unwrap().len() as u32,
      expected_length
    );

    for i in 0..pbc.log_message_ids_compressed.read().unwrap().len() - 1 {
      assert_eq!(
        pbc
          .log_message_ids_compressed
          .read()
          .unwrap()
          .get(i)
          .unwrap(),
        &0
      )
    }
    assert_eq!(
      pbc
        .log_message_ids_compressed
        .read()
        .unwrap()
        .last()
        .unwrap(),
      &expected_last_compressed
    );
  }

  #[test]
  fn test_incresing_by_one_values() {
    // When values are monotonically increasing by 1, only 1 bit is required to store each integer.
    let mut increasing_by_one: Vec<u32> = Vec::new();
    for i in 0..128 {
      increasing_by_one.push(i);
    }
    let pb = PostingsBlock::new_with_log_message_ids(increasing_by_one);
    let pbc = PostingsBlockCompressed::try_from(&pb).unwrap();

    // Each encoded bit is expected to be 1, except for the initial value (where it would be 0).
    let mut expected: Vec<u8> = vec![255; 16];
    expected[0] = 254;

    assert_eq!(pbc.get_num_bits(), 1);
    assert_eq!(pbc.get_initial(), 0);
    assert_eq!(pbc.log_message_ids_compressed.read().unwrap().len(), 16);
    assert_eq!(*pbc.log_message_ids_compressed.read().unwrap(), expected);

    let mem_pb_log_message_ids = size_of_val(&*pb.get_log_message_ids().read().unwrap().as_slice());
    let mem_pbc_log_message_ids_compressed =
      size_of_val(&*pbc.log_message_ids_compressed.read().unwrap().as_slice());

    // The memory consumed by log_message_ids for uncompressed block should be equal to sizeof(u32)*128,
    // as there are 128 integers, each occupying 4 bytes.
    assert_eq!(mem_pb_log_message_ids, 4 * 128);

    // The memory consumed by log_message_ids for compressed block should be equal to sizeof(u8)*16,
    // as there are 16 integers, each occupying 1 byte.
    assert_eq!(mem_pbc_log_message_ids_compressed, 1 * 16);
  }

  #[test]
  fn test_posting_block_compressed_clone() {
    let mut increasing_by_one: Vec<u32> = Vec::new();
    for i in 0..128 {
      increasing_by_one.push(i);
    }
    let pb = PostingsBlock::new_with_log_message_ids(increasing_by_one);
    let pbc = PostingsBlockCompressed::try_from(&pb).unwrap();

    let pbc_clone = pbc.clone();
    assert_eq!(pbc, pbc_clone);

    assert_eq!(
      *pbc.log_message_ids_compressed.read().unwrap(),
      *pbc_clone.log_message_ids_compressed.read().unwrap()
    );
  }
}

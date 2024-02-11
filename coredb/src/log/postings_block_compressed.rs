// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use bitpacking::BitPacker;
use log::{debug, error};
use serde::{Deserialize, Serialize};

use crate::log::constants::{BITPACKER, BLOCK_SIZE_FOR_LOG_MESSAGES};
use crate::log::postings_block::PostingsBlock;
use crate::utils::error::CoreDBError;

/// Represents a delta-compressed PostingsBlock.
#[derive(Debug, Deserialize, Serialize)]
pub struct PostingsBlockCompressed {
  /// Initial value.
  initial: u32,

  /// Number of bits per integer.
  num_bits: u8,

  /// Vector of compressed log_message_ids.
  log_message_ids_compressed: Vec<u8>,
}

impl PostingsBlockCompressed {
  /// Creates an emply compressed postings block.
  pub fn new() -> Self {
    PostingsBlockCompressed {
      initial: 0,
      num_bits: 0,
      log_message_ids_compressed: Vec::new(),
    }
  }

  pub fn new_with_params(initial: u32, num_bits: u8, log_message_ids_compressed: &[u8]) -> Self {
    PostingsBlockCompressed {
      initial,
      num_bits,
      log_message_ids_compressed: log_message_ids_compressed.to_owned(),
    }
  }

  /// Get the initial value.
  pub fn get_initial(&self) -> u32 {
    self.initial
  }

  /// Get the number of bits used to represent each integer.
  pub fn get_num_bits(&self) -> u8 {
    self.num_bits
  }

  /// Gets the vector of compressed integers.
  pub fn get_log_message_ids_compressed(&self) -> &Vec<u8> {
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
      && *self.log_message_ids_compressed == *other.log_message_ids_compressed
  }
}

impl Eq for PostingsBlockCompressed {}

impl TryFrom<&PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>> for PostingsBlockCompressed {
  type Error = CoreDBError;

  /// Convert PostingsBlock to PostingsBlockCompressed, i.e. compress a postings block.
  fn try_from(
    postings_block: &PostingsBlock<BLOCK_SIZE_FOR_LOG_MESSAGES>,
  ) -> Result<Self, Self::Error> {
    let log_message_ids_len = postings_block.len();
    if log_message_ids_len != BLOCK_SIZE_FOR_LOG_MESSAGES {
      // We only encode integers in blocks of BLOCK_SIZE_FOR_LOG_MESSAGES.
      error!(
        "Required length of postings block for compression {}, received length {}",
        BLOCK_SIZE_FOR_LOG_MESSAGES, log_message_ids_len
      );

      return Err(CoreDBError::InvalidSize(
        log_message_ids_len,
        BLOCK_SIZE_FOR_LOG_MESSAGES,
      ));
    }

    let entire = postings_block.get_log_message_ids();
    let initial: u32 = entire[0];
    let num_bits: u8 = BITPACKER.num_bits_sorted(initial, entire.as_slice());

    // A compressed block will take at most 4 bytes per-integer.
    let mut compressed = vec![0u8; 4 * BLOCK_SIZE_FOR_LOG_MESSAGES];

    // The log message ids in the postings block are already sorted, compress them using BitPacker.
    let compressed_len =
      BITPACKER.compress_sorted(initial, entire.as_slice(), &mut compressed[..], num_bits);

    // Resize the compressed vector in-place to match the actual compressed data length.
    compressed.resize(compressed_len, 0);

    let postings_block_compressed: Self = Self {
      initial,
      num_bits,
      log_message_ids_compressed: compressed,
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
      self.get_log_message_ids_compressed(),
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
  use crate::utils::sync::is_sync_send;

  #[test]
  fn test_empty() {
    is_sync_send::<PostingsBlockCompressed>();

    let pbc = PostingsBlockCompressed::new();
    assert_eq!(pbc.get_initial(), 0);
    assert_eq!(pbc.get_num_bits(), 0);
    assert_eq!(pbc.log_message_ids_compressed.len(), 0);
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
    let short_len = 10;

    // We need to create a vector of length BLOCK_SIZE_FOR_LOG_MESSAGES, but with only values till short_len
    // filled up.
    let mut log_message_ids = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    #[allow(clippy::needless_range_loop)]
    for i in 0..short_len {
      log_message_ids[i] = i as u32;
    }

    let pb = PostingsBlock::new_with_log_message_ids(short_len, log_message_ids);

    // We shouldn't be able to compress a postings block with length short_len.
    let retval = PostingsBlockCompressed::try_from(&pb);
    assert!(retval.is_err());
  }

  #[test]
  fn test_too_long_vector() {
    // We only compress integers with length BLOCK_SIZE_FOR_LOG_MESSAGES, so longer than this
    // length should give an error.

    // Create an array of length 1028, which is longer than BLOCK_SIZE_FOR_LOG_MESSAGES.
    let long = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    let long_len = 1028;
    let pb = PostingsBlock::new_with_log_message_ids(long_len, long);

    // We shouldn't be able to compress a postings block with length long_len.
    let retval = PostingsBlockCompressed::try_from(&pb);
    assert!(retval.is_err());
  }

  #[test]
  fn test_all_same_values() {
    // The compression only works when the values are in monotonically increasing order.
    // When passed vector with same elements, the returned vector is empty.
    let same = [1000; 128];
    let pb = PostingsBlock::new_with_log_message_ids(same.len(), same);
    let retval = PostingsBlockCompressed::try_from(&pb).unwrap();

    assert_eq!(retval.get_initial(), 1000);
    assert_eq!(retval.get_num_bits(), 0);
    assert!((*retval.log_message_ids_compressed).is_empty());
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

    let mut uncompressed = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    uncompressed[uncompressed.len() - 1] = last_uncompressed_input;

    let pb = PostingsBlock::new_with_log_message_ids(BLOCK_SIZE_FOR_LOG_MESSAGES, uncompressed);
    let pbc = PostingsBlockCompressed::try_from(&pb).unwrap();
    assert_eq!(pbc.log_message_ids_compressed.len() as u32, expected_length);

    for i in 0..pbc.log_message_ids_compressed.len() - 1 {
      assert_eq!(pbc.log_message_ids_compressed.get(i).unwrap(), &0)
    }
    assert_eq!(
      pbc.log_message_ids_compressed.last().unwrap(),
      &expected_last_compressed
    );
  }

  #[test]
  fn test_incresing_by_one_values() {
    // When values are monotonically increasing by 1, only 1 bit is required to store each integer.
    let mut increasing_by_one = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    #[allow(clippy::needless_range_loop)]
    for i in 0..128 {
      increasing_by_one[i] = i as u32;
    }
    let pb = PostingsBlock::new_with_log_message_ids(128, increasing_by_one);
    let pbc = PostingsBlockCompressed::try_from(&pb).unwrap();

    // Each encoded bit is expected to be 1, except for the initial value (where it would be 0).
    let mut expected: Vec<u8> = vec![255; 16];
    expected[0] = 254;

    assert_eq!(pbc.get_num_bits(), 1);
    assert_eq!(pbc.get_initial(), 0);
    assert_eq!(pbc.log_message_ids_compressed.len(), 16);
    assert_eq!(*pbc.log_message_ids_compressed, expected);

    let mem_pb_log_message_ids = size_of_val(&pb.get_log_message_ids()[..]);
    let mem_pbc_log_message_ids_compressed = size_of_val(pbc.log_message_ids_compressed.as_slice());

    // The memory consumed by log_message_ids for uncompressed block should be equal to sizeof(u32)*128,
    // as there are 128 integers, each occupying 4 bytes.
    assert_eq!(mem_pb_log_message_ids, 4 * BLOCK_SIZE_FOR_LOG_MESSAGES);

    // The memory consumed by log_message_ids for compressed block should be equal to sizeof(u8)*16,
    // as there are 16 integers, each occupying 1 byte.
    assert_eq!(mem_pbc_log_message_ids_compressed, 16);
  }

  #[test]
  fn test_posting_block_compressed_clone() {
    let mut increasing_by_one = [0; BLOCK_SIZE_FOR_LOG_MESSAGES];
    #[allow(clippy::needless_range_loop)]
    for i in 0..128 {
      increasing_by_one[i] = i as u32;
    }
    let pb = PostingsBlock::new_with_log_message_ids(128, increasing_by_one);
    let pbc = PostingsBlockCompressed::try_from(&pb).unwrap();

    assert_eq!(pbc, pbc.clone());

    assert_eq!(
      *pbc.log_message_ids_compressed,
      *pbc.clone().log_message_ids_compressed
    );
  }
}

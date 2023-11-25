use bitpacking::{BitPacker, BitPacker4x};
use lazy_static::lazy_static;

// Bitpacker used for encoding integers as variable-length bits.
// https://docs.rs/bitpacking/latest/bitpacking/index.html
lazy_static! {
  pub(super) static ref BITPACKER: BitPacker4x = BitPacker4x::new();
}

// We compress blocks of 128 inetgers. Do not modify this if
// BitPacker4x compression is being used. For more details, see
// https://docs.rs/bitpacking/latest/bitpacking/
pub(super) static BLOCK_SIZE_FOR_LOG_MESSAGES: usize = 128;

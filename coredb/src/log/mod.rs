// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Store and retrieve log messages from CoreDB.
//!
//! Log messages are stored in time-sharded segments.
//! Each segment is indexed as a [postings list](https://en.wikipedia.org/wiki/Inverted_index)
//! consisting of compressed postings blocks. Each postings block contains a vector of log message
//! IDs and is wrapped in a read-write lock for thread safety.

pub(super) mod constants;
pub mod log_message;
pub(super) mod postings_block;
pub(super) mod postings_block_compressed;
pub(super) mod postings_list;

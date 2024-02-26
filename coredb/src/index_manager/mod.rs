// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Manages indexes in CoreDB.
//!
//! A CoreDB index is an append-only data structure composed of time-sharded segments that
//! are periodically committed to disk for durability. The index also contains metadata
//! like the directory path for the index, the number of segments, the number of data elements,
//! and the size of the data elements. It is worth noting that logs and metrics are indexed
//! differently, with logs retrieved using an [inverted index](https://en.wikipedia.org/wiki/Inverted_index).

pub mod index;
pub mod metadata;
pub mod segment_summary;

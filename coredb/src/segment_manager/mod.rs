// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Manages segments in CoreDB.
//!
//! Segments are time-sharded sections of telemetry data that are indexed for fast retrieval.

mod metadata;
pub(crate) mod search;
pub(super) mod segment;

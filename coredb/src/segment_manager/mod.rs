//! Manages segments in CoreDB.
//!
//! Segments are time-sharded sections of telemetry data that are indexed for fast retrieval.

mod metadata;
pub(super) mod segment;

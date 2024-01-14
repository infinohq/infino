//! Store and retrieve metric data from CoreDB.
//!
//! Metric points are stored in time-sharded segments.
//! Each segment is indexed as a [time series](https://www.tableau.com/learn/articles/time-series-analysis)
//! consisting of compressed time series blocks. Each time series block contains a vector of metric points
//! and is wrapped in a read-write lock for thread safety.

mod constants;
pub mod metric_point;
mod metricutils;
pub mod time_series;
pub mod time_series_block;
mod time_series_block_compressed;

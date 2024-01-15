// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Utilities for managing CoreDB.

pub(crate) mod custom_serde;
pub mod error;
pub(crate) mod range;

pub mod config;
pub mod io;
pub mod sync;
pub mod tokenize;

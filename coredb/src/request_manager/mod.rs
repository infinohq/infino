// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Executes Infino requests on an index
//!
//! Infino supports a wide range of OpenSearch and Lucene queries.
//! It will attempt to at least parse all such queries
//! and the user will be notified about any query that is not supported.

pub mod query_dsl;

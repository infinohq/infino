// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

//! Manages the ingestion queue for Infino.
//!
//! The ingestion queue manager uses [RabbitMQ](https://github.com/rabbitmq)
//! for persisting data append requests before they are indexed and stored.
//! The queue manager can be run on a separate system to allow Infino to be
//! restarted without losing data.
//!
//! It is worth noting that RabbitMQ is not as robust as we like so we are
//! considering alternatives.

pub(crate) mod queue;

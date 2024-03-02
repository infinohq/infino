//! CoreDB is a telemetry database.
//!
//! It uses time-sharded segments and compressed blocks for data storage
//! and implements indexing for fast data retrieval.

pub mod index_manager;
pub mod log;
pub mod metric;
pub(crate) mod policy_manager;
pub mod request_manager;
pub(crate) mod segment_manager;
pub mod storage_manager;
pub mod utils;

use std::collections::HashMap;

use ::log::{debug, info};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use pest::error::Error as PestError;
use policy_manager::retention_policy::TimeBasedRetention;
use request_manager::promql::Rule;
use request_manager::promql_object::PromQLObject;
use storage_manager::storage::Storage;
use utils::error::SearchLogsError;

use crate::index_manager::index::Index;
use crate::log::log_message::LogMessage;
use crate::policy_manager::retention_policy::RetentionPolicy;
use crate::segment_manager::segment::Segment;
use crate::utils::config::Settings;
use crate::utils::error::{CoreDBError, SearchMetricsError};

impl From<PestError<Rule>> for SearchMetricsError {
  fn from(error: PestError<Rule>) -> Self {
    SearchMetricsError::JsonParseError(error.to_string())
  }
}

/// Database for storing telemetry data, mapping string keys to index objects.
pub struct CoreDB {
  index_map: DashMap<String, Index>,
  settings: Settings,
  policy: Box<dyn RetentionPolicy>,
}

impl CoreDB {
  /// Create a new CoreDB at the directory path specified in the config,
  /// creating a default index if none already exists.
  pub async fn new(config_dir_path: &str) -> Result<Self, CoreDBError> {
    let result = Settings::new(config_dir_path);

    match result {
      Ok(settings) => {
        let coredb_settings = settings.get_coredb_settings();
        let index_dir_path = &coredb_settings.get_index_dir_path();
        let default_index_name = coredb_settings.get_default_index_name();
        let segment_size_threshold_bytes = coredb_settings.get_segment_size_threshold_bytes();
        let search_memory_budget_bytes = coredb_settings.get_search_memory_budget_bytes();
        let storage_type = coredb_settings.get_storage_type()?;
        let storage = Storage::new(&storage_type).await?;

        // Check if index_dir_path exist and has some directories in it
        let index_map = DashMap::new();
        let index_names = storage.read_dir(index_dir_path).await;
        match index_names {
          Ok(index_names) => {
            info!(
              "Index directory {} already exists. Loading existing index",
              index_dir_path
            );

            if index_names.is_empty() {
              info!(
                "Index directory {} does not exist. Creating it.",
                index_dir_path
              );
              let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
              let index = Index::new_with_threshold_params(
                &storage_type,
                &default_index_dir_path,
                segment_size_threshold_bytes,
                search_memory_budget_bytes,
              )
              .await?;
              index_map.insert(default_index_name.to_string(), index);
            } else {
              for index_name in index_names {
                let full_index_path_name = format!("{}/{}", index_dir_path, index_name);
                let index = Index::refresh(
                  &storage_type,
                  &full_index_path_name,
                  search_memory_budget_bytes,
                )
                .await?;
                index_map.insert(index_name, index);
              }
            }
          }
          Err(_) => {
            info!(
              "Index directory {} does not exist. Creating it.",
              index_dir_path
            );
            let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
            let index = Index::new_with_threshold_params(
              &storage_type,
              &default_index_dir_path,
              segment_size_threshold_bytes,
              search_memory_budget_bytes,
            )
            .await?;
            index_map.insert(default_index_name.to_string(), index);
          }
        }

        // Ideally decide which Retention object to create based on the config file
        let retention_period = coredb_settings.get_retention_days();
        let policy = Box::new(TimeBasedRetention::new(retention_period));

        let coredb = CoreDB {
          index_map,
          settings,
          policy,
        };

        Ok(coredb)
      }
      Err(e) => {
        let error = CoreDBError::InvalidConfiguration(e.to_string());
        Err(error)
      }
    }
  }

  /// Append a log message.
  pub async fn append_log_message(
    &self,
    time: u64,
    fields: &HashMap<String, String>,
    text: &str,
  ) -> Result<(), CoreDBError> {
    debug!(
      "Appending log message in CoreDB: time {}, fields {:?}, text {}",
      time, fields, text
    );
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .append_log_message(time, fields, text)
      .await
  }

  /// Append a metric point.
  pub async fn append_metric_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) -> Result<(), CoreDBError> {
    debug!(
      "Appending metric point in CoreDB: time {}, value {}, labels {:?}, name {}",
      time, value, labels, metric_name
    );
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .append_metric_point(metric_name, labels, time, value)
      .await
  }

  /// Search the log messages for given query and range.
  ///
  /// Infino log searches support Lucene Query Syntax: https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
  /// http://infino-endpoint?"my lucene query"&start_time=blah&end_time=blah
  ///
  /// but these can be overridden by a Query DSL in the
  /// json body sent with the query: https://opensearch.org/docs/latest/query-dsl/.
  ///
  /// Note that while the query terms are not required in the URL, the query parameters
  /// "start_time" and "end_time" are indeed required in the URL. They are always added by the
  /// OpenSearch plugin that calls Infino.
  pub async fn search_logs(
    &self,
    url_query: &str,
    json_query: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<LogMessage>, CoreDBError> {
    debug!(
      "COREDB: Search logs for URL query: {:?}, JSON query: {:?}, range_start_time: {}, range_end_time: {}",
      url_query, json_query, range_start_time, range_end_time
    );

    let mut json_query = json_query.to_string();

    // Check if URL or JSON query is empty
    let is_url_empty = url_query.trim().is_empty();
    let is_json_empty = json_query.trim().is_empty();

    // If no JSON query, convert the URL query to Query DSL or return an error if no URL query
    if is_json_empty {
      if is_url_empty {
        return Err(SearchLogsError::NoQueryProvided.into());
      } else {
        // Update json_query with the constructed query from url_query
        json_query = format!(
          r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{}", "operator" : "AND" }} }} }} }}"#,
          url_query
        );
      }
    }

    // Build the query AST
    let ast = Segment::parse_query(&json_query)?;

    let index = self
      .index_map
      .get(self.get_default_index_name())
      .ok_or(SearchLogsError::NoQueryProvided)?;

    index
      .value()
      .search_logs(&ast, range_start_time, range_end_time)
      .await
  }

  /// Get the metric points for given label and range.
  /// Range boundaries can be overridden by PromQL query in the JSON body
  pub async fn search_metrics(
    &self,
    url_query: &str,
    json_query: &str,
    timeout: u64,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, CoreDBError> {
    debug!(
      "COREDB: Search metrics for URL query: {:?}, JSON query: {:?}, range_start_time: {}, range_end_time: {}",
      url_query, json_query, range_start_time, range_end_time
    );

    // Build the query AST
    // TODO: for now we'll ignore the json body but come back to this
    let ast = Index::parse_query(url_query)?;

    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .search_metrics(&ast, timeout, range_start_time, range_end_time)
      .await
  }

  /// Commit the index to disk.
  pub async fn commit(&self, commit_current_segment: bool) -> Result<(), CoreDBError> {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .commit(commit_current_segment)
      .await
  }

  /// Refresh the default index from the given directory path.
  // TODO: what to do if there are multiple indices?
  pub async fn refresh(config_dir_path: &str) -> Result<Self, CoreDBError> {
    // Read the settings and the index directory path.
    let settings = Settings::new(config_dir_path).unwrap();
    let index_dir_path = settings.get_coredb_settings().get_index_dir_path();
    let default_index_name = settings.get_coredb_settings().get_default_index_name();
    let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
    let search_memory_budget_bytes = settings
      .get_coredb_settings()
      .get_search_memory_budget_bytes();
    let storage_type = settings.get_coredb_settings().get_storage_type()?;

    // Refresh the index.
    let index = Index::refresh(
      &storage_type,
      &default_index_dir_path,
      search_memory_budget_bytes,
    )
    .await?;

    let index_map = DashMap::new();
    index_map.insert(default_index_name.to_string(), index);

    let retention_period = settings.get_coredb_settings().get_retention_days();
    let policy = Box::new(TimeBasedRetention::new(retention_period));

    Ok(CoreDB {
      index_map,
      settings,
      policy,
    })
  }

  /// Get the directory where the index is stored.
  pub fn get_index_dir(&self) -> String {
    self
      .index_map
      .get(self.get_default_index_name())
      .unwrap()
      .value()
      .get_index_dir()
  }

  /// Get the settings for this CoreDB.
  pub fn get_settings(&self) -> &Settings {
    &self.settings
  }

  /// Create a new index.
  pub async fn create_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    let index_dir_path = self.settings.get_coredb_settings().get_index_dir_path();
    let segment_size_threshold_bytes = self
      .settings
      .get_coredb_settings()
      .get_segment_size_threshold_bytes();
    let search_memory_budget_bytes = self
      .settings
      .get_coredb_settings()
      .get_search_memory_budget_bytes();
    let storage_type = self.settings.get_coredb_settings().get_storage_type()?;

    let index_dir_path = format!("{}/{}", index_dir_path, index_name);
    let index = Index::new_with_threshold_params(
      &storage_type,
      &index_dir_path,
      segment_size_threshold_bytes,
      search_memory_budget_bytes,
    )
    .await?;

    self.index_map.insert(index_name.to_string(), index);
    Ok(())
  }

  /// Delete an index.
  pub async fn delete_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    let index = self.index_map.remove(index_name);
    match index {
      Some(index) => {
        index.1.delete().await?;
        Ok(())
      }
      None => {
        let error = CoreDBError::IndexNotFound(index_name.to_string());
        Err(error)
      }
    }
  }

  pub fn get_default_index_name(&self) -> &str {
    self.settings.get_coredb_settings().get_default_index_name()
  }

  pub fn get_retention_policy(&self) -> &dyn RetentionPolicy {
    self.policy.as_ref()
  }

  /// Function to help with triggering the retention policy
  pub async fn trigger_retention(&self) -> Result<(), CoreDBError> {
    let default_index_name = self.get_default_index_name();
    let temp_reference = self.index_map.get(default_index_name).unwrap();
    let index = temp_reference.value();

    // TODO: this does not need to read all_segments_summaries from disk - can use the one already
    // in memory in Index::all_segments_summaries()
    let all_segments_summaries = index.get_all_segments_summaries().await?;

    let segment_ids_to_delete = self.get_retention_policy().apply(&all_segments_summaries);

    let mut deletion_futures: FuturesUnordered<_> = segment_ids_to_delete
      .into_iter()
      .map(|segment_id| index.delete_segment(segment_id))
      .collect();

    while let Some(result) = deletion_futures.next().await {
      result?;
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use std::io::Write;

  use chrono::Utc;
  use tempdir::TempDir;

  use crate::utils::config::CoreDBSettings;
  use crate::utils::io::get_joined_path;

  use super::*;

  /// Helper function to create a test configuration.
  fn create_test_config(config_dir_path: &str, index_dir_path: &str) {
    // Create a test config in the directory config_dir_path.
    let config_file_path = get_joined_path(
      config_dir_path,
      CoreDBSettings::get_default_config_file_name(),
    );

    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let default_index_name_line = format!("default_index_name = \"{}\"\n", ".default");

      let mut file = std::fs::File::create(config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file.write_all(default_index_name_line.as_bytes()).unwrap();
      file
        .write_all(b"segment_size_threshold_megabytes = 0.1\n")
        .unwrap();
      file.write_all(b"memory_budget_megabytes = 0.4\n").unwrap();
      file.write_all(b"retention_days = 30\n").unwrap();
      file.write_all(b"storage_type = \"local\"\n").unwrap();
    }
  }

  #[tokio::test]
  async fn test_basic() -> Result<(), CoreDBError> {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    create_test_config(config_dir_path, index_dir_path);
    println!("Config dir path {}", config_dir_path);

    // Create a new coredb instance.
    let coredb = CoreDB::new(config_dir_path)
      .await
      .expect("Could not create coredb");

    let start = Utc::now().timestamp_millis() as u64;

    // Add a few log messages.
    coredb
      .append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        "log message 1",
      )
      .await
      .expect("Could not append log message");
    coredb
      .append_log_message(
        Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
        &HashMap::new(),
        "log message 2",
      )
      .await
      .expect("Could not append log message");

    // Add a few metric points.
    coredb
      .append_metric_point(
        "some_metric",
        &HashMap::new(),
        Utc::now().timestamp_millis() as u64,
        1.0,
      )
      .await
      .expect("Could not append metric point");
    coredb
      .append_metric_point(
        "some_metric",
        &HashMap::new(),
        Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
        2.0,
      )
      .await
      .expect("Could not append metric point");

    coredb.commit(true).await.expect("Could not commit");
    let coredb = CoreDB::refresh(config_dir_path).await?;

    let end = Utc::now().timestamp_millis() as u64;

    // Search for log messages. The order of results should be reverse chronological order.
    if let Err(err) = coredb.search_logs("message", "", start, end).await {
      eprintln!("Error in search_logs: {:?}", err);
    } else {
      let results = coredb
        .search_logs("message", "", start, end)
        .await
        .expect("Error in search_logs");
      assert_eq!(results.first().unwrap().get_text(), "log message 2");
      assert_eq!(results.get(1).unwrap().get_text(), "log message 1");
    }

    // Search for metric points.
    let mut results = coredb
      .search_metrics("some_metric", "", 0, start, end)
      .await
      .expect("Error in get_metrics");

    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 2);
    assert_eq!(mp[0].get_value(), 1.0);
    assert_eq!(mp[1].get_value(), 2.0);

    coredb
      .trigger_retention()
      .await
      .expect("Error in retention policy");

    Ok(())
  }
}

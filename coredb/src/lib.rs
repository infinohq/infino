//! CoreDB is a telemetry database.
//!
//! It uses time-sharded segments and compressed blocks for data storage
//! and implements indexing for fast data retrieval.

pub mod index_manager;
pub mod log;
pub mod metric;
pub(crate) mod policy_manager;
pub mod request_manager;
pub mod segment_manager;
pub mod storage_manager;
pub mod utils;

use ::log::{debug, error, info};
use futures::stream::{FuturesUnordered, StreamExt};
use pest::error::Error as PestError;
use policy_manager::merge_policy::{MergePolicy, SizeBasedMerge};
use policy_manager::retention_policy::TimeBasedRetention;
use request_manager::promql::Rule;
use request_manager::promql_object::PromQLObject;
use request_manager::query_dsl_object::QueryDSLObject;
use std::collections::HashMap;
use std::sync::Arc;
use storage_manager::storage::Storage;
use tokio::sync::RwLock;
use utils::constants;

use crate::index_manager::index::Index;
use crate::policy_manager::retention_policy::RetentionPolicy;
use crate::segment_manager::segment::Segment;
use crate::utils::config::Settings;
use crate::utils::error::{CoreDBError, QueryError};

impl From<PestError<Rule>> for QueryError {
  fn from(error: PestError<Rule>) -> Self {
    QueryError::JsonParseError(error.to_string())
  }
}

enum PolicyType {
  Retention(Box<dyn RetentionPolicy>),
  Merge(Box<dyn MergePolicy>),
}

/// Database for storing telemetry data, mapping string keys to index objects.
pub struct CoreDB {
  index_map: Arc<RwLock<HashMap<String, Index>>>,
  settings: Settings,
  policy: HashMap<String, PolicyType>,
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
        let wal_dir_path = &coredb_settings.get_wal_dir_path();
        let default_index_name = coredb_settings.get_default_index_name();
        let search_memory_budget_bytes = coredb_settings.get_search_memory_budget_bytes();
        let append_log_messages_threshold = coredb_settings.get_log_messages_threshold();
        let append_metric_points_threshold = coredb_settings.get_metric_points_threshold();
        let uncommitted_segments_threshold = coredb_settings.get_uncommitted_segments_threshold();
        let storage_type = coredb_settings.get_storage_type()?;
        let storage = Storage::new(&storage_type).await?;

        // Check if index_dir_path exist and has some directories in it
        let index_map = Arc::new(RwLock::new(HashMap::new()));
        let mut index_map_locked = index_map.write().await;
        let index_names = storage.read_dir(index_dir_path).await;
        match index_names {
          Ok(index_names) => {
            if index_names.is_empty() {
              info!(
                "Index directory {} exists, but has no indices. Creating the default index.",
                index_dir_path
              );
              let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
              let default_wal_dir_path = format!("{}/{}", wal_dir_path, default_index_name);
              let index = Index::new_with_threshold_params(
                &storage_type,
                &default_index_dir_path,
                &default_wal_dir_path,
                search_memory_budget_bytes,
                append_log_messages_threshold,
                append_metric_points_threshold,
                uncommitted_segments_threshold,
              )
              .await?;
              index_map_locked.insert(default_index_name.to_string(), index);
            } else {
              info!(
                "Index directory {} already exists. Loading existing indices.",
                index_dir_path
              );
              for index_name in index_names {
                let full_index_path_name = format!("{}/{}", index_dir_path, index_name);
                let full_wal_path_name = format!("{}/{}", wal_dir_path, index_name);
                let index = Index::refresh(
                  &storage_type,
                  &full_index_path_name,
                  &full_wal_path_name,
                  search_memory_budget_bytes,
                )
                .await?;
                index_map_locked.insert(index_name, index);
              }
            }
          }
          Err(_) => {
            info!(
              "Index directory {} does not exist. Creating it.",
              index_dir_path
            );
            let default_index_dir_path = format!("{}/{}", index_dir_path, default_index_name);
            let default_wal_dir_path = format!("{}/{}", wal_dir_path, default_index_name);
            let index = Index::new_with_threshold_params(
              &storage_type,
              &default_index_dir_path,
              &default_wal_dir_path,
              search_memory_budget_bytes,
              append_log_messages_threshold,
              append_metric_points_threshold,
              uncommitted_segments_threshold,
            )
            .await?;
            index_map_locked.insert(default_index_name.to_string(), index);
          }
        }

        let mut policy = HashMap::new();
        // Ideally decide which Retention object to create based on the config file
        let retention_period = coredb_settings.get_retention_days();
        let merge_limit = coredb_settings.get_target_segment_size_bytes();

        let retention_policy = Box::new(TimeBasedRetention::new(retention_period));
        let merge_policy = Box::new(SizeBasedMerge::new(merge_limit));
        policy.insert(
          constants::RETENTION_POLICY_TRIGGER.to_string(),
          PolicyType::Retention(retention_policy),
        );
        policy.insert(
          constants::MERGE_POLICY_TRIGGER.to_string(),
          PolicyType::Merge(merge_policy),
        );
        let coredb = CoreDB {
          index_map: Arc::clone(&index_map),
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

  pub fn get_index_map(&self) -> Arc<RwLock<HashMap<String, Index>>> {
    self.index_map.clone()
  }

  pub async fn append_log_message(
    &self,
    index_name: &str,
    time: u64,
    fields: &HashMap<String, String>,
    text: &str,
  ) -> Result<u32, CoreDBError> {
    debug!(
      "Appending log message in CoreDB: time {}, fields {:?}, text {}",
      time, fields, text
    );

    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    // Retrieve the index from the map
    let index = match index_map_lock.get(index_name) {
      Some(index) => index,
      None => {
        let error = QueryError::IndexNotFoundError(index_name.to_string());
        error!("Failed to get index '{}': {:?}", index_name, error);
        return Err(utils::error::CoreDBError::IndexNotFound(error.to_string()));
      }
    };

    // Call the async method on the retrieved index
    let results = index.append_log_message(time, fields, text).await;

    // Drop the read lock
    drop(index_map_lock);

    results
  }

  /// Append a metric point.
  pub async fn append_metric_point(
    &self,
    index_name: &str,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) -> Result<(), CoreDBError> {
    debug!(
      "Appending metric point in CoreDB: time {}, value {}, labels {:?}, name {}",
      time, value, labels, metric_name
    );

    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    let index = index_map_lock
      .get(index_name)
      .ok_or(QueryError::IndexNotFoundError(index_name.to_string()))?;

    let results = index
      .append_metric_point(metric_name, labels, time, value)
      .await;

    // Drop the read lock
    drop(index_map_lock);

    results
  }

  /// Search the log messages for given query and range.
  ///
  /// https://opensearch.org/docs/latest/query-dsl/.
  ///
  /// Note that while the query terms are not required in the URL, the query parameters
  /// "start_time" and "end_time" are indeed required in the URL. They are always added by the
  /// OpenSearch plugin that calls Infino.
  pub async fn search_logs(
    &self,
    index_name: &str,
    url_query: &str,
    json_query: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<QueryDSLObject, CoreDBError> {
    debug!(
      "COREDB: Search logs for index name: {}, URL query: {:?}, JSON query: {:?}, range_start_time: {}, range_end_time: {}",
      index_name, url_query, json_query, range_start_time, range_end_time
    );

    let mut json_query = json_query.to_string();

    // Check if URL or JSON query is empty
    let is_url_empty = url_query.trim().is_empty();
    let is_json_empty = json_query.trim().is_empty();

    // If no JSON query, convert the URL query to Query DSL or return an error if no URL query
    if is_json_empty {
      if is_url_empty {
        debug!("No Query was provided. Exiting");
        return Err(QueryError::NoQueryProvided.into());
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

    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    let index = index_map_lock
      .get(index_name)
      .ok_or(QueryError::IndexNotFoundError(format!(
        "Can't find index {}",
        index_name,
      )))?;

    let results = index
      .search_logs(&ast, range_start_time, range_end_time)
      .await;

    // Drop the read lock
    drop(index_map_lock);

    results
  }

  /// Get the metric points for given label and range.
  /// Range boundaries can be overridden by PromQL query in the JSON body
  pub async fn search_metrics(
    &self,
    index_name: &str,
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

    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    let index = index_map_lock
      .get(index_name)
      .ok_or(QueryError::IndexNotFoundError(index_name.to_string()))?;

    let results = index
      .search_metrics(&ast, timeout, range_start_time, range_end_time)
      .await?;

    // Drop the read lock
    drop(index_map_lock);

    Ok(results)
  }

  /// Commit the index to disk.
  pub async fn commit(&self, is_shutdown: bool) -> Result<(), CoreDBError> {
    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    for index_entry in index_map_lock.iter() {
      index_entry.1.commit(is_shutdown).await?
    }

    // Drop the read lock
    drop(index_map_lock);

    Ok(())
  }

  /// Refresh the index from the given directory path.
  pub async fn refresh(index_name: &str, config_dir_path: &str) -> Result<Self, CoreDBError> {
    // Read the settings and the index directory path.
    let settings = Settings::new(config_dir_path).unwrap();
    let coredb_settings = settings.get_coredb_settings();
    let index_dir_path = coredb_settings.get_index_dir_path();
    let wal_dir_path = coredb_settings.get_wal_dir_path();
    let actual_index_dir_path = format!("{}/{}", index_dir_path, index_name);
    let actual_wal_dir_path = format!("{}/{}", wal_dir_path, index_name);
    let search_memory_budget_bytes = coredb_settings.get_search_memory_budget_bytes();
    let storage_type = coredb_settings.get_storage_type()?;

    // Refresh the index.
    let index = Index::refresh(
      &storage_type,
      &actual_index_dir_path,
      &actual_wal_dir_path,
      search_memory_budget_bytes,
    )
    .await?;

    let index_map = Arc::new(RwLock::new(HashMap::new()));
    let mut index_map_locked = index_map.write().await;

    index_map_locked.insert(index_name.to_string(), index);

    let mut policy = HashMap::new();
    // Ideally decide which Retention object to create based on the config file
    let retention_period = coredb_settings.get_retention_days();
    let merge_limit = coredb_settings.get_target_segment_size_bytes();

    let retention_policy = Box::new(TimeBasedRetention::new(retention_period));
    let merge_policy = Box::new(SizeBasedMerge::new(merge_limit));
    policy.insert(
      constants::RETENTION_POLICY_TRIGGER.to_string(),
      PolicyType::Retention(retention_policy),
    );
    policy.insert(
      constants::MERGE_POLICY_TRIGGER.to_string(),
      PolicyType::Merge(merge_policy),
    );

    Ok(CoreDB {
      index_map: Arc::clone(&index_map),
      settings,
      policy,
    })
  }

  /// Get the directory where the index is stored.
  pub async fn get_index_dir(&self, index_name: &str) -> String {
    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;
    index_map_lock.get(index_name).unwrap().get_index_dir()
  }

  /// Get the settings for this CoreDB.
  pub fn get_settings(&self) -> &Settings {
    &self.settings
  }

  /// Create a new index.
  pub async fn create_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    debug!("COREDB: Creating index {}", index_name);

    let coredb_settings = self.settings.get_coredb_settings();
    let index_dir_path = coredb_settings.get_index_dir_path();
    let wal_dir_path = coredb_settings.get_wal_dir_path();
    let search_memory_budget_bytes = coredb_settings.get_search_memory_budget_bytes();
    let append_log_messages_threshold = coredb_settings.get_log_messages_threshold();
    let append_metric_points_threshold = coredb_settings.get_metric_points_threshold();
    let uncommitted_segments_threshold = coredb_settings.get_uncommitted_segments_threshold();
    let storage_type = self.settings.get_coredb_settings().get_storage_type()?;

    let index_dir_path = format!("{}/{}", index_dir_path, index_name);
    let wal_dir_path = format!("{}/{}", wal_dir_path, index_name);

    let index = Index::new_with_threshold_params(
      &storage_type,
      &index_dir_path,
      &wal_dir_path,
      search_memory_budget_bytes,
      append_log_messages_threshold,
      append_metric_points_threshold,
      uncommitted_segments_threshold,
    )
    .await?;

    // Acquire write lock on the index map
    let mut index_map_locked = self.index_map.write().await;

    index_map_locked.insert(index_name.to_string(), index);

    Ok(())
  }

  /// Delete an index.
  pub async fn delete_index(&self, index_name: &str) -> Result<(), CoreDBError> {
    debug!("COREDB: Deleting index {}", index_name);

    let mut indexes_to_delete = Vec::new();

    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    if ["default", ".default", "Default", ".Default"].contains(&index_name) {
      return Err(CoreDBError::CannotDeleteIndex(index_name.to_string()));
    }

    // Create the list of indexes to delete.
    // TODO: Handle regexes.
    if index_name.eq("*") || index_name.eq("_all") {
      // Collect keys to delete
      index_map_lock.iter().for_each(|entry| {
        let name = entry.0.clone();
        if !["default", ".default", "Default", ".Default"].contains(&name.as_str()) {
          indexes_to_delete.push(name);
        }
      });
    } else {
      indexes_to_delete.push(index_name.to_string());
    }

    // Drop the read lock and acquire a write lock
    drop(index_map_lock);
    let mut index_map_locked = self.index_map.write().await;

    // Now delete the indexes without holding references to the index_map
    // which results in deadlock as the 'remove' will wait for the references
    // to be released.
    for name in indexes_to_delete {
      if let Some(index) = index_map_locked.remove(&name) {
        index.delete().await?;
      } else {
        return Err(CoreDBError::IndexNotFound(name));
      }
    }

    // Drop the write lock
    drop(index_map_locked);

    Ok(())
  }

  pub fn get_default_index_name(&self) -> &str {
    self.settings.get_coredb_settings().get_default_index_name()
  }

  fn get_retention_policy(&self) -> Option<&PolicyType> {
    self.policy.get(constants::RETENTION_POLICY_TRIGGER)
  }

  fn get_merge_policy(&self) -> Option<&PolicyType> {
    self.policy.get(constants::MERGE_POLICY_TRIGGER)
  }

  pub async fn trigger_merge(&self) -> Result<Vec<u32>, CoreDBError> {
    let mut merged_segment_ids = Vec::new();

    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    for index_entry in index_map_lock.iter() {
      // Assuming a method to safely retrieve and ensure we're getting a merge policy
      let merge_policy = self
        .get_merge_policy()
        .ok_or(CoreDBError::InvalidPolicy())?;
      let all_segments_summaries = index_entry.1.read_all_segments_summaries().await?;
      // Get all the keys of the segments in memory
      let segments_in_memory = index_entry.1.get_memory_segments_numbers();
      // Apply the merge policy directly here based on the enum variant
      let segment_ids_to_merge = match merge_policy {
        PolicyType::Merge(policy) => policy.apply(&all_segments_summaries, &segments_in_memory),
        _ => return Err(CoreDBError::InvalidPolicy()),
      };
      let merged_result = index_entry.1.merge_segments(&segment_ids_to_merge).await;
      match merged_result {
        Ok(merged_segment_id) => {
          merged_segment_ids.push(merged_segment_id);
        }
        Err(e) => {
          error!("Error merging segments: {:?}", e);
        }
      }
    }

    // Drop the read lock
    drop(index_map_lock);

    Ok(merged_segment_ids)
  }

  /// Function to help with triggering the retention policy
  pub async fn trigger_retention(&self) -> Result<(), CoreDBError> {
    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    for index_entry in index_map_lock.iter() {
      // TODO: this does not need to read all_segments_summaries from disk - can use the one already
      // in memory in Index::all_segments_summaries()
      let all_segments_summaries = index_entry.1.read_all_segments_summaries().await?;

      let retention_policy = self
        .get_retention_policy()
        .ok_or(CoreDBError::InvalidPolicy())?;
      let segment_ids_to_delete = match retention_policy {
        PolicyType::Retention(policy) => {
          policy.apply(&all_segments_summaries) // Directly use the apply method of the retention policy
        }
        _ => return Err(CoreDBError::InvalidPolicy()),
      };

      let mut deletion_futures: FuturesUnordered<_> = segment_ids_to_delete
        .into_iter()
        .map(|segment_id| index_entry.1.delete_segment(segment_id))
        .collect();

      while let Some(result) = deletion_futures.next().await {
        result?;
      }
    }

    // Drop the read lock
    drop(index_map_lock);

    Ok(())
  }

  /// Flush write ahead log for all indices.
  pub async fn flush_wal(&self) {
    // Acquire read lock on the index map
    let index_map_lock = self.index_map.read().await;

    for index_entry in index_map_lock.iter() {
      index_entry.1.flush_wal().await;
    }

    // Drop the read lock
    drop(index_map_lock);
  }

  /// Delete logs matching query, and return the number of logs deleted.
  pub async fn delete_logs_by_query(
    &self,
    index_name: &str,
    url_query: &str,
    json_query: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<u32, CoreDBError> {
    debug!(
      "COREDB: Delete logs by query for index name: {}, URL query: {:?}, JSON query: {:?}, range_start_time: {}, range_end_time: {}",
      index_name, url_query, json_query, range_start_time, range_end_time
    );

    // Call search_logs to get the list of logs to delete
    let logs_to_delete = self
      .search_logs(
        index_name,
        url_query,
        json_query,
        range_start_time,
        range_end_time,
      )
      .await;

    match logs_to_delete {
      Ok(logs) => {
        let mut log_ids = Vec::new();
        for log in logs.get_messages() {
          log_ids.push(log.get_id());
        }

        // Acquire read lock on the index map
        let index_map_lock = self.index_map.read().await;

        let index = index_map_lock
          .get(index_name)
          .ok_or(QueryError::IndexNotFoundError(index_name.to_string()))?;

        let results = index
          .delete_logs_by_query(log_ids, range_start_time, range_end_time)
          .await;

        // Drop the read lock
        drop(index_map_lock);

        results
      }
      Err(_) => Err(CoreDBError::QueryError(QueryError::SearchAndMarkLogsError)),
    }
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
  fn create_test_config(
    index_name: &str,
    config_dir_path: &str,
    index_dir_path: &str,
    wal_dir_path: &str,
  ) {
    // Create a test config in the directory config_dir_path.
    let config_file_path = get_joined_path(
      config_dir_path,
      CoreDBSettings::get_default_config_file_name(),
    );

    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let default_index_name_line = format!("default_index_name = \"{}\"\n", index_name);
      let wal_dir_path_line = format!("wal_dir_path = \"{}\"\n", wal_dir_path);

      let mut file = std::fs::File::create(config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file.write_all(wal_dir_path_line.as_bytes()).unwrap();
      file.write_all(default_index_name_line.as_bytes()).unwrap();
      file.write_all(b"log_messages_threshold = 1000\n").unwrap();
      file
        .write_all(b"metric_points_threshold = 10000\n")
        .unwrap();
      file
        .write_all(b"search_memory_budget_megabytes = 0.4\n")
        .unwrap();
      file
        .write_all(b"uncommitted_segments_threshold = 10\n")
        .unwrap();
      file.write_all(b"retention_days = 30\n").unwrap();
      file.write_all(b"storage_type = \"local\"\n").unwrap();
      file
        .write_all(b"target_segment_size_megabytes = 1024\n")
        .unwrap();
    }
  }

  #[tokio::test]
  async fn test_coredb_basic() -> Result<(), CoreDBError> {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();
    let index_name = "test_coredb_basic";
    let index_dir = TempDir::new(index_name).unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let wal_dir = TempDir::new("wal_test").unwrap();
    let wal_dir_path = wal_dir.path().to_str().unwrap();
    create_test_config(index_name, config_dir_path, index_dir_path, wal_dir_path);
    println!("Config dir path {}", config_dir_path);

    // Create a new coredb instance.
    let coredb = CoreDB::new(config_dir_path)
      .await
      .expect("Could not create coredb");

    let start = Utc::now().timestamp_millis() as u64;

    // Add a few log messages.
    coredb
      .append_log_message(
        index_name,
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        "log message 1",
      )
      .await
      .expect("Could not append log message");
    coredb
      .append_log_message(
        index_name,
        Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
        &HashMap::new(),
        "log message 2",
      )
      .await
      .expect("Could not append log message");

    // Add a few metric points.
    coredb
      .append_metric_point(
        index_name,
        "some_metric",
        &HashMap::new(),
        Utc::now().timestamp_millis() as u64,
        1.0,
      )
      .await
      .expect("Could not append metric point");
    coredb
      .append_metric_point(
        index_name,
        "some_metric",
        &HashMap::new(),
        Utc::now().timestamp_millis() as u64 + 1, // Add a +1 to make the test predictable.
        2.0,
      )
      .await
      .expect("Could not append metric point");

    coredb.commit(true).await.expect("Could not commit");
    let coredb = CoreDB::refresh(index_name, config_dir_path).await?;

    let end = Utc::now().timestamp_millis() as u64;

    // Search for log messages. The order of results should be reverse chronological order.
    if let Err(err) = coredb
      .search_logs(index_name, "message", "", start, end)
      .await
    {
      eprintln!("Error in search_logs: {:?}", err);
    } else {
      let results = coredb
        .search_logs(index_name, "message", "", start, end)
        .await
        .expect("Error in search_logs");
      assert_eq!(
        results
          .get_messages()
          .first()
          .unwrap()
          .get_message()
          .get_text(),
        "log message 2"
      );
      assert_eq!(
        results
          .get_messages()
          .get(1)
          .unwrap()
          .get_message()
          .get_text(),
        "log message 1"
      );
    }

    // Search for metric points.
    let mut results = coredb
      .search_metrics(index_name, "some_metric", "", 0, start, end)
      .await
      .expect("Error in get_metrics");

    let mut v = results.take_vector();
    let mp = v[0].take_metric_points();
    assert_eq!(v.len(), 1);
    assert_eq!(mp.len(), 2);
    assert_eq!(mp[0].get_value(), 1.0);
    assert_eq!(mp[1].get_value(), 2.0);

    // delete by query
    let deleted_count = coredb
      .delete_logs_by_query(index_name, "message", "", start, end)
      .await
      .expect("Error in delete_logs_by_query");

    assert_eq!(deleted_count, 2);

    coredb
      .trigger_retention()
      .await
      .expect("Error in retention policy");

    coredb.trigger_merge().await.expect("Error in merge policy");
    Ok(())
  }
}

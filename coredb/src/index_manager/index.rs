// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::collections::HashMap;

use crossbeam_queue::SegQueue;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use log::error;
use log::{debug, info};
use pest::iterators::Pairs;

use crate::index_manager::metadata::Metadata;
use crate::index_manager::segment_summary::SegmentSummary;
use crate::log::log_message::LogMessage;
use crate::request_manager::promql;
use crate::request_manager::promql_object::PromQLObject;
use crate::request_manager::query_dsl;
use crate::segment_manager::segment::Segment;
use crate::storage_manager::storage::Storage;
use crate::storage_manager::storage::StorageType;
use crate::utils::error::{CoreDBError, SearchMetricsError};
use crate::utils::io;
use crate::utils::sync::thread;
use crate::utils::sync::{Arc, TokioMutex, TokioRwLock};

/// File name where the information about all segements is stored.
const ALL_SEGMENTS_FILE_NAME: &str = "all_segments.bin";

/// File name to store index metadata.
const METADATA_FILE_NAME: &str = "metadata.bin";

/// Default threshold for size of segment used in some tests.
/// A new segment will be created in the next commit when a segment exceeds this size.
#[cfg(test)]
const DEFAULT_SEGMENT_SIZE_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024; // 256MB

/// Default memory budget for search in bytes used in some tests.
#[cfg(test)]
const DEFAULT_SEARCH_MEMORY_BUDGET_BYTES: u64 = 1024 * 1024 * 1024; // 1GB

#[derive(Debug)]
/// Index for storing log messages and metric points.
pub struct Index {
  /// Metadata for this index.
  metadata: Metadata,

  /// A reverse-chronological sorted vector of segment summaries.
  all_segments_summaries: Arc<TokioRwLock<Vec<SegmentSummary>>>,

  /// DashMap of segment number to segment - only for the segments that are in memory.
  memory_segments_map: DashMap<u32, Segment>,

  /// Directory where the index is serialized.
  index_dir_path: String,

  /// Mutex for locking the directory where the index is committed / read from, so that two threads
  /// don't write the directory at the same time.
  /// Essentially, this mutex serializes the commit() and refresh() operations on this index.
  index_dir_lock: Arc<TokioMutex<thread::ThreadId>>,

  /// Memory budget for searching this index.
  search_memory_budget_bytes: u64,

  /// Storage for this index.
  storage: Storage,

  /// Segment numbers that aren't yet committed to storage.
  uncommitted_segment_numbers: SegQueue<u32>,
}

impl Index {
  /// Create a new index with default threshold segment size.
  /// However, if a directory with the same path already exists and has a metadata file in it,
  /// the function will refresh the existing index instead of creating a new one.
  /// If the refresh process fails, an error will be thrown to indicate the issue.
  #[cfg(test)]
  pub async fn new(storage_type: &StorageType, index_dir_path: &str) -> Result<Self, CoreDBError> {
    Index::new_with_threshold_params(
      storage_type,
      index_dir_path,
      DEFAULT_SEGMENT_SIZE_THRESHOLD_BYTES,
      DEFAULT_SEARCH_MEMORY_BUDGET_BYTES,
    )
    .await
  }

  /// Creates a new index at a specified directory path with customizable parameter for the segment size threshold.
  /// If a directory with the same path already exists and has a metadata
  /// file in it, the existing index will be refreshed instead of creating a new one. If the refresh
  /// process fails, an error will be thrown to indicate the issue.
  pub async fn new_with_threshold_params(
    storage_type: &StorageType,
    index_dir: &str,
    segment_size_threshold_bytes: u64,
    search_memory_budget_bytes: u64,
  ) -> Result<Self, CoreDBError> {
    info!(
      "Creating index - storage type {:?}, dir {}, segment size threshold in megabytes: {}",
      storage_type, index_dir, segment_size_threshold_bytes
    );

    let storage = Storage::new(storage_type).await?;

    if !storage.check_path_exists(index_dir).await {
      // Index directory does not exist - create it.
      storage.create_dir(index_dir)?;
    }

    // Check whether index directory already has a metadata file.
    let metadata_path = &format!("{}/{}", index_dir, METADATA_FILE_NAME);
    if storage.check_path_exists(metadata_path).await {
      // index_dir_path has metadata file, refresh the index instead of creating new one
      match Self::refresh(storage_type, index_dir, search_memory_budget_bytes).await {
        Ok(mut index) => {
          index
            .metadata
            .update_segment_size_threshold_bytes(segment_size_threshold_bytes);
          index.search_memory_budget_bytes = search_memory_budget_bytes;
          return Ok(index);
        }
        Err(err) => {
          // Received a error while refreshing index
          return Err(err);
        }
      }
    }

    // The directory did not have a metadata file - so create a new index.

    // Create an initial segment.
    let segment = Segment::new();
    let metadata = Metadata::new(0, 0, segment_size_threshold_bytes);

    // Update the initial segment as the current segment.
    let current_segment_number = metadata.fetch_increment_segment_count();
    metadata.update_current_segment_number(current_segment_number);

    // Create the summary for the initial segment.
    let mut all_segments_summaries_vec = Vec::new();
    let current_segment_summary = SegmentSummary::new(current_segment_number, &segment);
    all_segments_summaries_vec.push(current_segment_summary);
    let all_segments_summaries = Arc::new(TokioRwLock::new(all_segments_summaries_vec));

    let memory_segments_map = DashMap::new();
    memory_segments_map.insert(current_segment_number, segment);

    let index_dir_lock = Arc::new(TokioMutex::new(thread::current().id()));

    let uncommitted_segment_numbers = SegQueue::new();

    let index = Index {
      metadata,
      all_segments_summaries,
      memory_segments_map,
      index_dir_path: index_dir.to_owned(),
      index_dir_lock,
      search_memory_budget_bytes,
      storage,
      uncommitted_segment_numbers,
    };

    // Commit the empty index so that the index directory will be created.
    index.commit().await.expect("Could not commit index");

    Ok(index)
  }

  /// Insert a segment in the memory segments map.
  fn insert_memory_segments_map(&self, segment_number: u32, segment: Segment) {
    self.memory_segments_map.insert(segment_number, segment);
  }

  /// Get the memory segments map.
  pub fn get_memory_segments_map(&self) -> &DashMap<u32, Segment> {
    &self.memory_segments_map
  }

  /// Possibly remove older segments from the memory segments map, so that the memory consumed is
  /// within the search_memory_budget_bytes.
  fn shrink_to_fit(&self) {
    // Create a vector that has each segment's number, uncompressed size and end time.
    let mut segment_data: Vec<(u32, u64, u64)> = Vec::new();
    let mut memory_consumed = 0;
    for entry in self.memory_segments_map.iter() {
      let segment_number = *entry.key();
      let segment = entry.value();
      let uncompressed_size = segment.get_uncompressed_size();
      let end_time = segment.get_end_time();
      segment_data.push((segment_number, uncompressed_size, end_time));
      memory_consumed += uncompressed_size;
    }

    if memory_consumed <= self.search_memory_budget_bytes {
      // We are within the memory budget - no eviction needed.
      return;
    }

    // Find out the memory to evict (from the older segments), so that we'll still be
    // within the memory budget.
    let memory_to_evict = memory_consumed - self.search_memory_budget_bytes;

    info!("Evicting memory {} bytes", memory_to_evict);

    // Sort this vector by end time in ascending order (i.e., oldest segments first).
    segment_data.sort_by_key(|&(_, _, end_time)| end_time);

    // Counter to track memory evicted so far.
    let mut memory_evicted_so_far = 0;

    // Iterate and evict the oldest segments first.
    for segment in segment_data {
      let segment_number = segment.0;
      let uncompressed_size = segment.1;

      // Do not evict the current segment - as it would be needed for inserts.
      if segment_number == self.metadata.get_current_segment_number() {
        debug!(
          "Not evicting the current segment with segment_number {}",
          segment_number
        );
        continue;
      }

      // We already evicted enough memory - stop evicting.
      if memory_evicted_so_far >= memory_to_evict {
        debug!(
          "Already evicted {} bytes of memory, greather than or equal to {}. Not evicting further.",
          memory_evicted_so_far, memory_to_evict
        );
        break;
      }

      // Evict the segment with segment_number from memory_segments_map.
      if let Some((_, segment)) = self.memory_segments_map.remove(&segment_number) {
        info!("Evicting segment with segment_number {}", segment_number);
        // Drop the segment to free the memory.
        drop(segment);
        memory_evicted_so_far += uncompressed_size;
      }
    }
  }

  /// Get the reference for the current segment.
  fn get_current_segment_ref(&self) -> Ref<u32, Segment> {
    let segment_number = self.metadata.get_current_segment_number();

    self
      .memory_segments_map
      .get(&segment_number)
      .unwrap_or_else(|| {
        // Here, we may choose to load the current segment in memory. However,
        // we always keep the segment being inserted into (i.e. current segment) in
        // memory, so this should never happen. Keeping a panic for now to know quickly
        // in case this happens due to an unanticipated scenario.
        panic!(
          "Could not get segment corresponding to segment number {} in memory",
          segment_number
        )
      })
  }

  /// Append a log message to the current segment of the index.
  pub fn append_log_message(&self, time: u64, fields: &HashMap<String, String>, message: &str) {
    debug!(
      "Appending log message, time: {}, fields: {:?}, message: {}",
      time, fields, message
    );

    // Get the current segment.
    let current_segment_ref = self.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    current_segment
      .append_log_message(time, fields, message)
      .unwrap();
  }

  /// Append a metric point to the current segment of the index.
  pub fn append_metric_point(
    &self,
    metric_name: &str,
    labels: &HashMap<String, String>,
    time: u64,
    value: f64,
  ) {
    debug!(
      "Appending metric point: metric name: {}, labels: {:?}, time: {}, value: {}",
      metric_name, labels, time, value
    );

    // Get the current segment.
    let current_segment_ref = self.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // Append the metric point to the current segment.
    current_segment
      .append_metric_point(metric_name, labels, time, value)
      .unwrap();
  }

  /// Search for given query in the given time range.
  pub async fn search_logs(
    &self,
    ast: &Pairs<'_, query_dsl::Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<Vec<LogMessage>, CoreDBError> {
    debug!(
      "INDEX: Ast {:?}, range_start_time {:?}, and range_end_time {:?}\n",
      ast, range_start_time, range_end_time
    );

    let mut retval = Vec::new();

    // First, get the segments overlapping with the given time range. This is in the reverse chronological order.
    let segment_numbers = self
      .get_overlapping_segments(range_start_time, range_end_time)
      .await;

    // Search in each of the segments. Note these these are in reverse chronological order - so when we add a
    // limit to the number of results, one can break out of the loop when desired number of results are retrieved.
    // If a segment isn't in memory, refresh it to memory from storage then execute the search.
    for segment_number in segment_numbers {
      let segment = self.memory_segments_map.get(&segment_number);
      let mut results = match segment {
        Some(segment) => segment
          .search_logs(&ast.clone(), range_start_time, range_end_time)
          .await
          .unwrap_or_else(|_| Vec::new()),
        None => {
          let segment = self.refresh_segment(segment_number).await?;
          segment
            .search_logs(ast, range_start_time, range_end_time)
            .await
            .unwrap_or_else(|_| Vec::new())
        }
      };

      retval.append(&mut results);
    }

    retval.sort();

    Ok(retval)
  }

  /// Get metric points corresponding to a promql query.
  pub async fn search_metrics(
    &self,
    ast: &Pairs<'_, promql::Rule>,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Result<PromQLObject, CoreDBError> {
    debug!(
      "INDEX: Ast {:?}, range_start_time {:?}, and range_end_time {:?}\n",
      ast, range_start_time, range_end_time
    );

    // Now start the search
    let mut results = self
      .traverse_promql_ast(&ast.clone(), range_start_time, range_end_time)
      .await
      .map_err(SearchMetricsError::AstError)?;

    // Note that PromQL results are explicitly unsorted but we sort here
    // to be consistent with search_logs.
    results.sort();

    Ok(results)
  }

  /// Helper function to commit a segment with given segment_number to disk.
  /// Returns the (id, start_time, end_time, uncompressed_size, compressed_size) for the segment.
  async fn commit_segment(
    &self,
    segment_number: u32,
  ) -> Result<(String, u64, u64, u64, u64), CoreDBError> {
    debug!("Committing segment with segment_number: {}", segment_number);

    // Get the segment corresponding to the segment_number.
    let segment_ref = self
      .memory_segments_map
      .get(&segment_number)
      .unwrap_or_else(|| {
        panic!(
          "Could not commit segment {} since it isn't in memory",
          segment_number
        )
      });
    let segment = segment_ref.value();
    let segment_id = segment.get_id();
    let start_time = segment.get_start_time();
    let end_time = segment.get_end_time();

    // Commit this segment.
    let segment_dir_path =
      io::get_joined_path(&self.index_dir_path, segment_number.to_string().as_str());

    let (uncompressed, compressed) = segment
      .commit(&self.storage, segment_dir_path.as_str())
      .await?;

    Ok((
      segment_id.to_owned(),
      start_time,
      end_time,
      uncompressed,
      compressed,
    ))
  }

  /// Get the summaries of the segments in this index.
  pub async fn get_all_segments_summaries(&self) -> Result<Vec<SegmentSummary>, CoreDBError> {
    info!(
      "Getting segment summaries of index from index_dir_path: {}",
      self.index_dir_path
    );

    // Read all segments summaries from disk.
    let all_segments_file = io::get_joined_path(&self.index_dir_path, ALL_SEGMENTS_FILE_NAME);

    if !self.storage.check_path_exists(&all_segments_file).await {
      return Err(CoreDBError::CannotFindIndexMetadataInDirectory(
        String::from(&self.index_dir_path),
      ));
    }

    let (all_segments_summaries_vec, _): (Vec<SegmentSummary>, _) =
      self.storage.read(&all_segments_file).await?;

    info!(
      "Number of segment summaries in index dir path {}: {}",
      self.index_dir_path,
      all_segments_summaries_vec.len()
    );

    Ok(all_segments_summaries_vec)
  }

  /// Commit an index to disk.
  pub async fn commit(&self) -> Result<(), CoreDBError> {
    info!("Committing index at {}", chrono::Utc::now());

    // Lock to make sure only one thread calls commit at a time. If the lock isn't avilable, we simply
    // log a message and return - so that the caller, typically on a schedule, can retry on the next
    // scheduled run.
    let lock = self.index_dir_lock.try_lock();
    let mut lock = match lock {
      Ok(lock) => lock,
      Err(_) => {
        info!(
          "Could not acquire commit lock for index at path {}. Retrying in the next commit run.",
          self.index_dir_path
        );
        return Ok(());
      }
    };
    *lock = thread::current().id();

    // We will be updating the self.all_segment_summaries, so acquire the lock.
    let write_lock_summaries = &mut self.all_segments_summaries.write().await;

    let all_segments_file = io::get_joined_path(&self.index_dir_path, ALL_SEGMENTS_FILE_NAME);

    // Commit the current segment. This also updates the start and end times in the corresponding segment summary.
    let original_current_segment_number = self.metadata.get_current_segment_number();
    let (segment_id, start_time, end_time, uncompressed_segment_size, _compressed_segment_size) =
      self.commit_segment(original_current_segment_number).await?;

    // Update the start and end time in the summary for this segment.
    // We don't update these in append_* methods for performance, and update only in commit.
    if let Some(summary) = write_lock_summaries
      .iter_mut()
      .find(|s| s.get_segment_id() == segment_id)
    {
      summary.update_start_end_time(start_time, end_time);
    }

    if uncompressed_segment_size > self.metadata.get_segment_size_threshold_bytes() {
      // Create a new segment since the current one has become too big.
      let new_segment = Segment::new();
      let new_segment_number = self.metadata.fetch_increment_segment_count();
      let new_segment_dir_path = io::get_joined_path(
        &self.index_dir_path,
        new_segment_number.to_string().as_str(),
      );

      // Write the new (empty) segment to disk.
      new_segment
        .commit(&self.storage, new_segment_dir_path.as_str())
        .await?;

      // Add the segment to summaries. Insert at the beginning - as this is the most recent segment.
      let summary = SegmentSummary::new(new_segment_number, &new_segment);
      write_lock_summaries.insert(0, summary);

      // Note that DashMap::insert *may* cause a single-thread deadlock if the thread has a read
      // reference to an item in the map. Make sure that no read reference for all_segments_map
      // is present before the insert and visible in this block.
      self.insert_memory_segments_map(new_segment_number, new_segment);

      // Appends will start going to the new segment after this point.
      self
        .metadata
        .update_current_segment_number(new_segment_number);

      // Commit the new_segment again as there might be more documents added after making it the
      // current segment.
      self.commit_segment(new_segment_number).await?;

      // Commit the original segment again to commit any updates from the previous commit till the
      // time of changing the current_sgement_number above.
      self.commit_segment(original_current_segment_number).await?;

      // We created a new segment - possibly exceeding the memory budget. So, evict older segments if needed.
      self.shrink_to_fit();
    }

    // Sort the summaries in reverse chronological order.
    write_lock_summaries.sort();
    let summaries: &Vec<SegmentSummary> = write_lock_summaries.as_ref();

    // Write the summaries to disk.
    self
      .storage
      .write(summaries, all_segments_file.as_str())
      .await?;

    let metadata_path = io::get_joined_path(&self.index_dir_path, METADATA_FILE_NAME);
    self
      .storage
      .write(&self.metadata, metadata_path.as_str())
      .await?;

    Ok(())
  }

  /// Reads a segment from memory and insert it in memory_segments_map.
  pub async fn refresh_segment(&self, segment_number: u32) -> Result<Segment, CoreDBError> {
    let segment_dir_path = io::get_joined_path(&self.index_dir_path, &segment_number.to_string());
    debug!(
      "Loading segment with segment number {} and path {}",
      segment_number, segment_dir_path
    );
    let (segment, _) = Segment::refresh(&self.storage, &segment_dir_path).await?;

    Ok(segment)
  }

  /// Read the index from the given index_dir_path.
  pub async fn refresh(
    storage_type: &StorageType,
    index_dir_path: &str,
    search_memory_budget_bytes: u64,
  ) -> Result<Self, CoreDBError> {
    info!("Refreshing index from index_dir_path: {}", index_dir_path);

    let storage = Storage::new(storage_type).await?;

    // Read metadata.
    let metadata_path = io::get_joined_path(index_dir_path, METADATA_FILE_NAME);
    let (metadata, _): (Metadata, _) = storage.read(metadata_path.as_str()).await?;

    let index_dir_lock = Arc::new(TokioMutex::new(thread::current().id()));

    // No segment is uncommitted when the index is refreshed.
    let uncommitted_segment_numbers = SegQueue::new();

    // Create an index with empty segment summaries and empry memory_segments_map.
    let mut index = Index {
      metadata,
      all_segments_summaries: Arc::new(TokioRwLock::new(Vec::new())),
      memory_segments_map: DashMap::new(),
      index_dir_path: index_dir_path.to_owned(),
      index_dir_lock,
      search_memory_budget_bytes,
      storage,
      uncommitted_segment_numbers,
    };

    let all_segments_summaries_vec = index.get_all_segments_summaries().await?;

    if all_segments_summaries_vec.is_empty() {
      // No segment summary present - so this may not be an index directory. Return an error.
      return Err(CoreDBError::NotAnIndexDirectory(index_dir_path.to_string()));
    }

    // Populate the segment summaries and memory_segments_map.
    let memory_segments_map: DashMap<u32, Segment> = DashMap::new();
    let mut search_memory_budget_consumed_bytes = 0;
    for segment_summary in &all_segments_summaries_vec {
      let uncompressed_size = segment_summary.get_uncompressed_size();
      search_memory_budget_consumed_bytes += uncompressed_size;
      if search_memory_budget_consumed_bytes <= search_memory_budget_bytes {
        let segment_number = segment_summary.get_segment_number();
        let segment = index.refresh_segment(segment_number).await?;
        memory_segments_map.insert(segment_number, segment);
      } else {
        // We have reached the memory budget - so do not load any more segments.
        break;
      }
    }

    // Update the index.
    let all_segments_summaries = Arc::new(TokioRwLock::new(all_segments_summaries_vec));
    index.all_segments_summaries = all_segments_summaries;
    index.memory_segments_map = memory_segments_map;

    info!("Read index with metadata {:?}", index.metadata);
    Ok(index)
  }

  /// Returns segment numbers of segments, in reverse chronological order, that overlap with the given time range.
  pub async fn get_overlapping_segments(
    &self,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<u32> {
    let mut segment_numbers = Vec::new();
    let all_segments_summaries = &*self.all_segments_summaries.read().await;

    // The segment start and end times in segment summaries are updated only in commit. So, prefer
    // getting the start and end times of the segment in memory in case it is in memory_segment_map,
    // else get the start and end times from the summary.
    for segment_summary in all_segments_summaries {
      let segment_number = segment_summary.get_segment_number();
      let segment = self.memory_segments_map.get(&segment_number);
      match segment {
        Some(segment) => {
          if segment.is_overlap(range_start_time, range_end_time) {
            segment_numbers.push(segment_number);
          }
        }
        _ => {
          if segment_summary.is_overlap(range_start_time, range_end_time) {
            segment_numbers.push(segment_number);
          }
        }
      }
    }
    segment_numbers
  }

  pub fn get_index_dir(&self) -> String {
    self.index_dir_path.to_owned()
  }

  /// Function to delete the index directory.
  pub async fn delete(&self) -> Result<(), CoreDBError> {
    self.storage.remove_dir(&self.index_dir_path).await
  }

  pub fn get_metadata_file_name() -> String {
    METADATA_FILE_NAME.to_owned()
  }

  pub async fn delete_segment(&self, segment_number: u32) -> Result<(), CoreDBError> {
    // Delete the segment only if it is not in memory
    if !self.memory_segments_map.contains_key(&segment_number) {
      let segment_dir_path = io::get_joined_path(&self.index_dir_path, &segment_number.to_string());
      let delete_result = self.storage.remove_dir(segment_dir_path.as_str()).await;
      match delete_result {
        Ok(_) => {
          debug!("Deleted segment with segment number {}", segment_number);
        }
        Err(e) => {
          error!("Failed to delete file: {:?}", segment_dir_path.as_str());
          return Err(e);
        }
      }
    } else {
      // Return error saying that the segment is in memory
      return Err(CoreDBError::SegmentInMemory(segment_number));
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use std::thread::sleep;
  use std::time::Duration;

  use chrono::Utc;
  use pest::Parser;
  use tempdir::TempDir;
  use test_case::test_case;
  use tests::promql::PromQLParser;

  use super::*;
  use crate::metric::metric_point::MetricPoint;
  use crate::request_manager::query_dsl::QueryDslParser;
  use crate::utils::io::get_joined_path;
  use crate::utils::sync::is_sync_send;

  // Helper function to create index
  async fn create_index<'a>(name: &'a str, storage_type: &'a StorageType) -> (Index, String) {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!("{}/{}", index_dir.path().to_str().unwrap(), name);
    let index = Index::new(storage_type, &index_dir_path).await.unwrap();
    (index, index_dir_path)
  }

  async fn create_index_with_thresholds<'a>(
    name: &'a str,
    storage_type: &'a StorageType,
    segment_size: u64,
    memory_budget: u64,
  ) -> (Index, String) {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!("{}/{}", index_dir.path().to_str().unwrap(), name);
    let index =
      Index::new_with_threshold_params(storage_type, &index_dir_path, segment_size, memory_budget)
        .await
        .unwrap();
    (index, index_dir_path)
  }

  #[tokio::test]
  async fn test_empty_index() {
    is_sync_send::<Index>();

    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_empty_index"
    );

    let index = Index::new(&StorageType::Local, &index_dir_path)
      .await
      .unwrap();
    let segment_ref = index.get_current_segment_ref();
    let segment = segment_ref.value();
    assert_eq!(segment.get_log_message_count(), 0);
    assert_eq!(segment.get_term_count(), 0);
    assert_eq!(index.index_dir_path, index_dir_path);

    // Check that the index directory exists, and has expected structure.
    let all_segments_file_path = get_joined_path(&index_dir_path, ALL_SEGMENTS_FILE_NAME);
    assert!(
      index
        .storage
        .check_path_exists(&all_segments_file_path)
        .await
    );

    let segment_path = get_joined_path(
      &index_dir_path,
      &index.metadata.get_current_segment_number().to_string(),
    );
    let segment_metadata_path = get_joined_path(&segment_path, &Segment::get_metadata_file_name());
    assert!(
      index
        .storage
        .check_path_exists(&segment_metadata_path)
        .await
    );
  }

  #[tokio::test]
  async fn test_commit_refresh() {
    let storage_type = StorageType::Local;
    let (expected, index_dir_path) = create_index("test_commit_refresh", &storage_type).await;
    let num_log_messages = 5;
    let message_prefix = "content#";
    let num_metric_points = 5;

    for i in 1..=num_log_messages {
      let message = format!("{}{}", message_prefix, i);
      expected.append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        &message,
      );
    }

    let metric_name = "request_count";
    let other_label_name = "method";
    let other_label_value = "GET";
    let mut label_map = HashMap::new();
    label_map.insert(other_label_name.to_owned(), other_label_value.to_owned());
    for i in 1..=num_metric_points {
      expected.append_metric_point(
        metric_name,
        &label_map,
        Utc::now().timestamp_millis() as u64,
        i as f64,
      );
    }

    expected.commit().await.expect("Could not commit");
    let received = Index::refresh(&storage_type, &index_dir_path, 1024)
      .await
      .unwrap();

    assert_eq!(&expected.index_dir_path, &received.index_dir_path);
    assert_eq!(
      &expected.memory_segments_map.len(),
      &received.memory_segments_map.len()
    );

    let expected_segment_ref = expected.get_current_segment_ref();
    let expected_segment = expected_segment_ref.value();
    let received_segment_ref = received.get_current_segment_ref();
    let received_segment = received_segment_ref.value();
    assert_eq!(
      &expected_segment.get_log_message_count(),
      &received_segment.get_log_message_count()
    );
    assert_eq!(
      &expected_segment.get_metric_point_count(),
      &received_segment.get_metric_point_count()
    );
  }

  #[tokio::test]
  async fn test_basic_search_logs() {
    let storage_type = StorageType::Local;
    let (index, _index_dir_path) = create_index("test_basic_search", &storage_type).await;
    let num_log_messages = 1000;
    let message_prefix = "this is my log message";
    let mut expected_log_messages: Vec<String> = Vec::new();

    for i in 1..num_log_messages {
      let message = format!("{} {}", message_prefix, i);
      index.append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        &message,
      );
      expected_log_messages.push(message);
    }
    // Now add a unique log message.
    index.append_log_message(
      Utc::now().timestamp_millis() as u64,
      &HashMap::new(),
      "thisisunique",
    );

    let query_message = r#"{
      "query": {
        "bool": {
          "must": [
            { "match": { "_all" : { "query": "message", "operator" : "AND" } } }
          ]
        }
      }
    }
    "#;

    // For the query "message", handle errors from search_logs
    let ast =
      QueryDslParser::parse(query_dsl::Rule::start, query_message).expect("Failed to parse query");
    let results = index
      .search_logs(&ast, 0, u64::MAX)
      .await
      .expect("Error in search_logs");

    // Continue with assertions
    assert_eq!(results.len(), num_log_messages - 1);
    let mut received_log_messages: Vec<String> = Vec::new();
    for i in 1..num_log_messages {
      received_log_messages.push(results.get(i - 1).unwrap().get_text().to_owned());
    }
    expected_log_messages.sort();
    received_log_messages.sort();
    assert_eq!(expected_log_messages, received_log_messages);

    let query_message = r#"{
      "query": {
        "bool": {
          "must": [
            { "match": { "_all" : { "query": "thisisunique", "operator" : "AND" } } }
          ]
        }
      }
    }
    "#;

    // For the query "thisisunique", we should expect only 1 result.
    let ast =
      QueryDslParser::parse(query_dsl::Rule::start, query_message).expect("Failed to parse query");
    let results = index
      .search_logs(&ast, 0, u64::MAX)
      .await
      .expect("Error in search_logs");

    assert_eq!(results.len(), 1);
    assert_eq!(results.first().unwrap().get_text(), "thisisunique");
  }

  #[tokio::test]
  async fn test_basic_time_series() {
    let storage_type = StorageType::Local;
    let (index, _index_dir_path) = create_index("test_basic_time_series", &storage_type).await;
    let num_metric_points = 1000;
    let mut expected_metric_points: Vec<MetricPoint> = Vec::new();

    for i in 1..num_metric_points {
      index.append_metric_point("metric", &HashMap::new(), i, i as f64);
      let dp = MetricPoint::new(i, i as f64);
      expected_metric_points.push(dp);
    }

    // The number of metric points in the index should be equal to the number of metric points we indexed.
    let ast = PromQLParser::parse(promql::Rule::start, "metric{label_name_1=label_value_1}")
      .expect("Failed to parse query");
    let mut results = index
      .search_metrics(&ast, 0, u64::MAX)
      .await
      .expect("Error in get_metrics");

    assert_eq!(
      &mut expected_metric_points,
      results.take_vector()[0].get_metric_points()
    )
  }

  #[test_case(true, false; "when only logs are appended")]
  #[test_case(false, true; "when only metric points are appended")]
  #[test_case(true, true; "when both logs and metric points are appended")]
  #[tokio::test]
  async fn test_two_segments(
    append_log: bool,
    append_metric_point: bool,
  ) -> Result<(), CoreDBError> {
    // We run this test multiple times, as it works well to find deadlocks (and doesn't take as much as time as a full test using loom).
    for _ in 0..10 {
      let storage_type = StorageType::Local;
      let storage = Storage::new(&storage_type).await?;
      let (index, index_dir_path) =
        create_index_with_thresholds("test_two_segments", &storage_type, 1500, 1024 * 1024).await;

      let original_segment_number = index.metadata.get_current_segment_number();
      let original_segment_path =
        get_joined_path(&index_dir_path, &original_segment_number.to_string());

      let message_prefix = "message";
      let mut expected_log_messages: Vec<String> = Vec::new();
      let mut expected_metric_points: Vec<MetricPoint> = Vec::new();

      let original_segment_num_log_messages = if append_log { 1000 } else { 0 };
      let original_segment_num_metric_points = if append_metric_point { 50000 } else { 0 };

      for i in 0..original_segment_num_log_messages {
        let message = format!("{} {}", message_prefix, i);
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          &message,
        );
        expected_log_messages.push(message);
      }

      for _ in 0..original_segment_num_metric_points {
        let dp = MetricPoint::new(Utc::now().timestamp_millis() as u64, 1.0);
        index.append_metric_point("some_name", &HashMap::new(), dp.get_time(), dp.get_value());
        expected_metric_points.push(dp);
      }

      // Force commit and then refresh the index.
      // This will write one segment to disk and create a new empty segment.
      index.commit().await.expect("Could not commit index");

      // Read the index from disk and see that it has expected number of log messages and metric points.
      let index = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
        .await
        .expect("Could not refresh index");
      let (original_segment, original_segment_size) =
        Segment::refresh(&storage, &original_segment_path)
          .await
          .expect("Could not refresh segment");
      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_metric_point_count(),
        original_segment_num_metric_points
      );
      assert!(original_segment_size > 0);

      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();

        assert_eq!(index.memory_segments_map.len(), 2);
        assert_eq!(current_segment.get_log_message_count(), 0);
        assert_eq!(current_segment.get_metric_point_count(), 0);
      }

      // Now add a log message and/or a metric point. This will still land in the current (empty) segment in the index.
      let mut new_segment_num_log_messages = 0;
      let mut new_segment_num_metric_points = 0;
      if append_log {
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          "some_message_1",
        );
        new_segment_num_log_messages += 1;
      }
      if append_metric_point {
        index.append_metric_point(
          "some_name",
          &HashMap::new(),
          Utc::now().timestamp_millis() as u64,
          1.0,
        );
        new_segment_num_metric_points += 1;
      }

      // Force a commit and refresh. The index should still have only 2 segments.
      index.commit().await.expect("Could not commit index");
      let index = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
        .await
        .unwrap();
      let (mut original_segment, original_segment_size) =
        Segment::refresh(&storage, &original_segment_path)
          .await
          .expect("Could not refresh segment");
      assert_eq!(index.memory_segments_map.len(), 2);

      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_metric_point_count(),
        original_segment_num_metric_points
      );
      assert!(original_segment_size > 0);

      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();
        assert_eq!(
          current_segment.get_log_message_count(),
          new_segment_num_log_messages
        );
        assert_eq!(
          current_segment.get_metric_point_count(),
          new_segment_num_metric_points
        );
      }

      // Add one more log message and/or a metric point. This should land in the current_segment that has
      // only 1 log message and/or metric point.
      if append_log {
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          "some_message_2",
        );
        new_segment_num_log_messages += 1;
      }
      if append_metric_point {
        index.append_metric_point(
          "some_name",
          &HashMap::new(),
          Utc::now().timestamp_millis() as u64,
          1.0,
        );
        new_segment_num_metric_points += 1;
      }

      // Force a commit and refresh.
      index.commit().await.expect("Could not commit index");
      let index = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
        .await
        .expect("Could not refresh index");
      (original_segment, _) = Segment::refresh(&storage, &original_segment_path)
        .await
        .expect("Could not refresh segment");

      let current_segment_log_message_count;
      let current_segment_metric_point_count;
      {
        // Write these in a separate block so that reference of current_segment from all_segments_map
        // does not persist when commit() is called (and all_segments_map is updated).
        // Otherwise, this test may deadlock.
        let current_segment_ref = index.get_current_segment_ref();
        let current_segment = current_segment_ref.value();
        current_segment_log_message_count = current_segment.get_log_message_count();
        current_segment_metric_point_count = current_segment.get_metric_point_count();

        assert_eq!(
          current_segment_log_message_count,
          new_segment_num_log_messages
        );
        assert_eq!(
          current_segment_metric_point_count,
          new_segment_num_metric_points
        );
      }

      assert_eq!(index.memory_segments_map.len(), 2);
      assert_eq!(
        original_segment.get_log_message_count(),
        original_segment_num_log_messages
      );
      assert_eq!(
        original_segment.get_metric_point_count(),
        original_segment_num_metric_points
      );

      // Commit and refresh a few times. The index should not change.
      index.commit().await.expect("Could not commit index");
      let index = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
        .await
        .expect("Could not refresh index");
      index.commit().await.expect("Could not commit index");
      index.commit().await.expect("Could not commit index");
      Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
        .await
        .unwrap();
      let index_final = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
        .await
        .unwrap();
      let index_final_current_segment_ref = index_final.get_current_segment_ref();
      let index_final_current_segment = index_final_current_segment_ref.value();

      assert_eq!(
        index.memory_segments_map.len(),
        index_final.memory_segments_map.len()
      );
      assert_eq!(index.index_dir_path, index_final.index_dir_path);
      assert_eq!(
        current_segment_log_message_count,
        index_final_current_segment.get_log_message_count()
      );
      assert_eq!(
        current_segment_metric_point_count,
        index_final_current_segment.get_metric_point_count()
      );
    }

    Ok(())
  }

  #[tokio::test]
  async fn test_multiple_segments_logs() {
    let storage_type = StorageType::Local;
    let start_time = Utc::now().timestamp_millis() as u64;

    // Create a new index with a low threshold for the segment size.
    let (mut index, index_dir_path) = create_index_with_thresholds(
      "test_multiple_segments_logs",
      &storage_type,
      1024,
      1024 * 1024,
    )
    .await;

    let message_prefix = "message";
    let num_log_messages = 10000;
    let commit_after = 1000;

    // Append log messages.
    let mut num_log_messages_from_last_commit = 0;
    for i in 1..=num_log_messages {
      let message = format!("{} {}", message_prefix, i);
      index.append_log_message(
        Utc::now().timestamp_millis() as u64,
        &HashMap::new(),
        &message,
      );

      // Commit after indexing more than commit_after messages.
      num_log_messages_from_last_commit += 1;
      if num_log_messages_from_last_commit > commit_after {
        index.commit().await.expect("Could not commit index");
        num_log_messages_from_last_commit = 0;
        sleep(Duration::from_millis(1000));
      }
    }

    // Commit and sleep to ensure the index is written to disk.
    index.commit().await.expect("Could not commit index");
    sleep(Duration::from_millis(1000));

    let end_time = Utc::now().timestamp_millis() as u64;

    // Read the index from disk.
    index = match Index::refresh(&storage_type, &index_dir_path, 1024 * 1024).await {
      Ok(index) => index,
      Err(err) => {
        error!("Error refreshing index: {:?}", err);
        return;
      }
    };

    // Ensure that more than 1 segment was created.
    assert!(index.memory_segments_map.len() > 1);

    // The current segment should be empty (i.e., have 0 documents).
    let current_segment_ref = index.get_current_segment_ref();
    let current_segment = current_segment_ref.value();
    assert_eq!(current_segment.get_log_message_count(), 0);

    for item in &index.memory_segments_map {
      let segment_number = item.key();
      let segment = item.value();
      if *segment_number == index.metadata.get_current_segment_number() {
        assert_eq!(segment.get_log_message_count(), 0);
      }
    }

    let query_message = &format!(
      r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{}", "operator" : "AND" }} }} }} }}"#,
      message_prefix
    );

    // Ensure the prefix is in every log message.
    let ast =
      QueryDslParser::parse(query_dsl::Rule::start, query_message).expect("Failed to parse query");
    let results = index
      .search_logs(&ast, start_time, end_time)
      .await
      .expect("Error in search_logs");
    assert_eq!(results.len(), num_log_messages);

    // Ensure the suffix is in exactly one log message.
    for i in 1..=num_log_messages {
      let suffix = &format!(
        r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{}", "operator" : "AND" }} }} }} }}"#,
        i
      );
      let ast =
        QueryDslParser::parse(query_dsl::Rule::start, suffix).expect("Failed to parse query");
      let results = index
        .search_logs(&ast, start_time, end_time)
        .await
        .expect("Error in search_logs");
      assert_eq!(results.len(), 1);
    }

    // Ensure the prefix+suffix is in exactly one log message.
    for i in 1..=num_log_messages {
      let query_message = &format!(
        r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{} {}", "operator" : "AND" }} }} }} }}"#,
        message_prefix, i
      );
      let ast = QueryDslParser::parse(query_dsl::Rule::start, query_message)
        .expect("Failed to parse query");
      let results = index
        .search_logs(&ast, start_time, end_time)
        .await
        .expect("Error in search_logs");

      assert_eq!(results.len(), 1);
    }
  }

  #[tokio::test]
  async fn test_search_logs_count() {
    let storage_type = StorageType::Local;
    let (index, _index_dir_path) =
      create_index_with_thresholds("test_search_logs_count", &storage_type, 1024, 1024 * 1024)
        .await;

    let message_prefix = "message";
    let num_message_suffixes = 20;

    // Create tokens with different numeric message suffixes
    for i in 1..num_message_suffixes {
      let message = &format!("{}{}", message_prefix, i);
      let count = 2u32.pow(i);
      for _ in 0..count {
        index.append_log_message(
          Utc::now().timestamp_millis() as u64,
          &HashMap::new(),
          message,
        );
      }
      index.commit().await.expect("Could not commit index");
    }

    for i in 1..num_message_suffixes {
      let query_message = &format!(
        r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{}{}", "operator" : "AND" }} }} }} }}"#,
        message_prefix, i
      );
      let expected_count = 2u32.pow(i);

      let ast = QueryDslParser::parse(query_dsl::Rule::start, query_message)
        .expect("Failed to parse query");
      let results = index
        .search_logs(&ast, 0, Utc::now().timestamp_millis() as u64)
        .await
        .expect("Error in search_logs");

      assert_eq!(expected_count, results.len() as u32);
    }
  }

  #[tokio::test]
  async fn test_multiple_segments_metric_points() {
    let storage_type = StorageType::Local;
    let (mut index, index_dir_path) = create_index_with_thresholds(
      "test_multiple_segments_metric_points",
      &storage_type,
      1024,
      1024 * 1024,
    )
    .await;

    let num_metric_points = 10000;
    let mut num_metric_points_from_last_commit = 0;
    let commit_after = 1000;

    // Append metric points to the index.
    let start_time = Utc::now().timestamp_millis() as u64;
    let mut label_map = HashMap::new();
    label_map.insert("label_name_1".to_owned(), "label_value_1".to_owned());
    for _ in 1..=num_metric_points {
      index.append_metric_point("metric", &label_map, start_time, 100.0);
      num_metric_points_from_last_commit += 1;

      // Commit after we have indexed more than commit_after messages.
      if num_metric_points_from_last_commit >= commit_after {
        index.commit().await.expect("Could not commit index");
        num_metric_points_from_last_commit = 0;
      }
    }
    // Commit and sleep to make sure the index is written to disk.
    index.commit().await.expect("Could not commit index");
    sleep(Duration::from_millis(10000));

    // Refresh the segment from disk.
    index = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
      .await
      .unwrap();
    let current_segment_ref = index.get_current_segment_ref();
    let current_segment = current_segment_ref.value();

    // Make sure that more than 1 segment got created.
    assert!(index.memory_segments_map.len() > 1);

    // The current segment in the index will be empty (i.e. will have 0 metric points.)
    assert_eq!(current_segment.get_metric_point_count(), 0);
    for item in &index.memory_segments_map {
      let segment_id = item.key();
      let segment = item.value();
      if *segment_id == index.metadata.get_current_segment_number() {
        assert_eq!(segment.get_metric_point_count(), 0);
      }
    }

    // The number of metric points in the index should be equal to the number of metric points we indexed.
    let ast = PromQLParser::parse(promql::Rule::start, "metric{label_name_1=label_value_1}")
      .expect("Failed to parse query");
    let mut results = index
      .search_metrics(&ast, 0, u64::MAX)
      .await
      .expect("Error in get_metrics");

    assert_eq!(
      num_metric_points,
      results.take_vector()[0].get_metric_points().len() as u32
    )
  }

  #[tokio::test]
  async fn test_index_dir_does_not_exist() {
    let index_dir = TempDir::new("index_test").unwrap();
    let storage_type = StorageType::Local;

    // Create a path within index_dir that does not exist.
    let temp_path_buf = index_dir.path().join("doesnotexist");

    let index = Index::new(&storage_type, temp_path_buf.to_str().unwrap())
      .await
      .unwrap();

    // If we don't get any panic/error during commit, that means the commit is successful.
    index.commit().await.expect("Could not commit index");
  }

  #[tokio::test]
  async fn test_refresh_does_not_exist() {
    let index_dir = TempDir::new("index_test").unwrap();
    let temp_path_buf = index_dir.path().join("doesnotexist");
    let storage_type = StorageType::Local;
    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");

    // Expect an error when directory isn't present.
    let mut result =
      Index::refresh(&storage_type, temp_path_buf.to_str().unwrap(), 1024 * 1024).await;
    assert!(result.is_err());

    // Expect an error when metadata file is not present in the directory.
    storage
      .create_dir(temp_path_buf.to_str().expect("Could not create dir path"))
      .expect("Could not create dir");
    result = Index::refresh(&storage_type, temp_path_buf.to_str().unwrap(), 1024 * 1024).await;
    assert!(result.is_err());
  }

  #[tokio::test]
  async fn test_overlap_one_segment() {
    let storage_type = StorageType::Local;
    let (index, _index_dir_path) = create_index("test_overlap_one_segment", &storage_type).await;

    index.append_log_message(1000, &HashMap::new(), "message_1");
    index.append_log_message(2000, &HashMap::new(), "message_2");

    assert_eq!(index.get_overlapping_segments(500, 1500).await.len(), 1);
    assert_eq!(index.get_overlapping_segments(1500, 2500).await.len(), 1);
    assert_eq!(index.get_overlapping_segments(1500, 1600).await.len(), 1);
    assert!(index.get_overlapping_segments(500, 600).await.is_empty());
    assert!(index.get_overlapping_segments(2500, 2600).await.is_empty());
  }

  #[tokio::test]
  async fn test_overlap_multiple_segments() {
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = format!(
      "{}/{}",
      index_dir.path().to_str().unwrap(),
      "test_overlap_multiple_segments"
    );
    let storage_type = StorageType::Local;

    let index = Index::new_with_threshold_params(
      &storage_type,
      &index_dir_path,
      // This size depends on the number of log messages added in each segment in the for loop below.
      (0.0003 * 1024.0 * 1024.0) as u64,
      1024 * 1024,
    )
    .await
    .unwrap();

    // Setting it high to test out that there is no single-threaded deadlock while commiting.
    // Note that if you change this value, some of the assertions towards the end of this test
    // may need to be changed.
    let num_segments = 20;

    for i in 0..num_segments {
      let start = i * 2 * 1000;
      index.append_log_message(start, &HashMap::new(), "message_1");
      index.append_log_message(start + 500, &HashMap::new(), "message_2");
      index.commit().await.expect("Could not commit index");
    }

    // We'll have num_segments segments, plus one empty segment at the end.
    assert_eq!(index.memory_segments_map.len() as u64, num_segments + 1);

    // The first segment will start at time 0 and end at time 1000.
    // The second segment will start at time 2000 and end at time 3000.
    // The third segment will start at time 4000 and end at time 5000.
    // ... and so on.
    assert_eq!(index.get_overlapping_segments(500, 1800).await.len(), 1);
    assert_eq!(index.get_overlapping_segments(500, 2800).await.len(), 2);
    assert_eq!(index.get_overlapping_segments(500, 3800).await.len(), 2);
    assert_eq!(index.get_overlapping_segments(500, 4800).await.len(), 3);
    assert_eq!(index.get_overlapping_segments(500, 5800).await.len(), 3);
    assert_eq!(index.get_overlapping_segments(500, 6800).await.len(), 4);
    assert_eq!(index.get_overlapping_segments(500, 10000).await.len(), 6);

    assert!(index.get_overlapping_segments(1500, 1800).await.is_empty());
    assert!(index.get_overlapping_segments(3500, 3800).await.is_empty());
    assert!(index
      .get_overlapping_segments(num_segments * 1000 * 10, num_segments * 1000 * 20)
      .await
      .is_empty());
  }

  #[test_case(32; "search_memory_budget = 32 * segment_size_threshold")]
  #[test_case(24; "search_memory_budget = 24 * segment_size_threshold")]
  #[test_case(16; "search_memory_budget = 16 * segment_size_threshold")]
  #[test_case(8; "search_memory_budget = 8 * segment_size_threshold")]
  #[test_case(4; "search_memory_budget = 4 * segment_size_threshold")]
  #[tokio::test]
  async fn test_concurrent_append(num_segments_in_memory: u64) {
    let storage_type = StorageType::Local;
    let segment_size_threshold_bytes = 1024;
    let search_memory_budget_bytes = num_segments_in_memory * segment_size_threshold_bytes;

    // Create a new index with a low threshold for the segment size.
    let (index, index_dir_path) = create_index_with_thresholds(
      "test_concurrent_append",
      &storage_type,
      segment_size_threshold_bytes,
      search_memory_budget_bytes,
    )
    .await;

    let arc_index = Arc::new(index);
    let num_threads = 20;
    let num_appends_per_thread = 5000;

    let mut commit_handles = Vec::new();

    // Start a thread to commit the index periodically.
    let arc_index_clone = arc_index.clone();
    let ten_millis = Duration::from_millis(10);
    let handle = thread::spawn(move || {
      let rt = tokio::runtime::Runtime::new().unwrap();
      rt.block_on(async {
        for _ in 0..100 {
          arc_index_clone
            .commit()
            .await
            .expect("Could not commit index");
          sleep(ten_millis);
        }
      });
    });
    commit_handles.push(handle);

    // Start threads to append to the index.
    let mut append_handles = Vec::new();
    for i in 0..num_threads {
      let arc_index_clone = arc_index.clone();
      let start = i * num_appends_per_thread;
      let mut label_map = HashMap::new();
      label_map.insert("label1".to_owned(), "value1".to_owned());

      let handle = thread::spawn(move || {
        for j in 0..num_appends_per_thread {
          let time = start + j;
          arc_index_clone.append_log_message(time as u64, &HashMap::new(), "message");
          arc_index_clone.append_metric_point("metric", &label_map, time as u64, 1.0);
        }
      });
      append_handles.push(handle);
    }

    for handle in append_handles {
      handle.join().unwrap();
    }

    for handle in commit_handles {
      handle.join().unwrap();
    }

    // Commit again to cover the scenario that append threads run for more time than the commit thread
    arc_index.commit().await.expect("Could not commit index");

    let index = Index::refresh(&storage_type, &index_dir_path, 1024 * 1024)
      .await
      .expect("Could not refresh index");
    let expected_len = num_threads * num_appends_per_thread;

    let query_message = r#"{
      "query": {
        "bool": {
          "must": [
            { "match": { "_all" : { "query": "message", "operator" : "AND" } } }
          ]
        }
      }
    }
    "#;

    let ast =
      QueryDslParser::parse(query_dsl::Rule::start, query_message).expect("Failed to parse query");
    let results = index
      .search_logs(&ast, 0, expected_len as u64)
      .await
      .expect("Error in search_logs");
    assert_eq!(expected_len, results.len());

    let ast = PromQLParser::parse(promql::Rule::start, "metric{label_name_1=label_value_1}")
      .expect("Failed to parse query");
    let mut results = index
      .search_metrics(&ast, 0, u64::MAX)
      .await
      .expect("Error in get_metrics");

    let mut tmpvec = results.take_vector();
    let mp = tmpvec[0].get_metric_points();
    assert_eq!(expected_len, mp.len());
  }

  #[tokio::test]
  async fn test_reusing_index_when_available() {
    let storage_type = StorageType::Local;
    let (index, index_dir_path) = create_index_with_thresholds(
      "test_reusing_index_when_available",
      &storage_type,
      1024,
      1024 * 1024,
    )
    .await;

    let start_time = Utc::now().timestamp_millis();

    index.append_log_message(start_time as u64, &HashMap::new(), "some_message_1");
    index.commit().await.expect("Could not commit index");

    // Create one more new index using same dir location
    let index = Index::new_with_threshold_params(&storage_type, &index_dir_path, 1024, 1024 * 1024)
      .await
      .unwrap();

    let query_message = r#"{
        "query": {
          "bool": {
            "must": [
              { "match": { "_all" : { "query": "some_message_1", "operator" : "AND" } } }
            ]
          }
        }
      }
      "#;

    // Call search_logs and handle errors
    let ast =
      QueryDslParser::parse(query_dsl::Rule::start, query_message).expect("Failed to parse query");
    let search_result = index
      .search_logs(
        &ast,
        start_time as u64,
        Utc::now().timestamp_millis() as u64,
      )
      .await
      .expect("Error in search_logs");

    assert_eq!(search_result.len(), 1);
  }

  #[tokio::test]
  async fn test_empty_directory_without_metadata() {
    // Create a new index in an empty directory - this should work.
    let index_dir = TempDir::new("index_test").unwrap();
    let index_dir_path = index_dir.path().to_str().unwrap();
    let storage_type = StorageType::Local;

    let index =
      Index::new_with_threshold_params(&storage_type, index_dir_path, 1024, 1024 * 1024).await;
    assert!(index.is_ok());
  }

  #[test_case(32; "search_memory_budget = 32 * segment_size_threshold")]
  #[test_case(24; "search_memory_budget = 24 * segment_size_threshold")]
  #[test_case(16; "search_memory_budget = 16 * segment_size_threshold")]
  #[test_case(8; "search_memory_budget = 8 * segment_size_threshold")]
  #[test_case(4; "search_memory_budget = 4 * segment_size_threshold")]
  #[tokio::test]
  async fn test_limited_memory(num_segments_in_memory: u64) {
    let storage_type = StorageType::Local;
    let segment_size_threshold_bytes = (0.0003 * 1024.0 * 1024.0) as u64;
    let search_memory_budget_bytes = num_segments_in_memory * segment_size_threshold_bytes;
    let (index, _index_dir_path) = create_index_with_thresholds(
      "test_limited_memory",
      &storage_type,
      // This size depends on the number of log messages added in each segment in the for loop below.
      segment_size_threshold_bytes,
      search_memory_budget_bytes,
    )
    .await;

    // Setting it high to test out that there is no single-threaded deadlock while commiting.
    // Note that if you change this value, some of the assertions towards the end of this test
    // may need to be changed.
    let num_segments = 20;

    for i in 0..num_segments {
      let start = i * 2 * 1000;
      let end = start + 500;
      // Insert unique messages in each segment - these will come handy for testing later.
      let message_start = &format!("message_{}", start);
      let message_end = &format!("message_{}", end);
      index.append_log_message(start, &HashMap::new(), message_start);
      index.append_log_message(end, &HashMap::new(), message_end);
      index.commit().await.expect("Could not commit index");
    }

    // We'll have num_segments segments, plus one empty segment at the end.
    assert_eq!(
      index.all_segments_summaries.read().await.len() as u64,
      num_segments + 1
    );

    // We shouldn't have more than specified segments in memory.
    assert!(index.memory_segments_map.len() as u64 <= num_segments_in_memory);

    // Check the queries return results as expected.
    for i in 0..num_segments {
      let start = i * 2 * 1000;
      let end = start + 500;
      let message_start = &format!("message_{}", start);
      let message_end = &format!("message_{}", end);

      let query_message = &format!(
        r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{}", "operator" : "AND" }} }} }} }}"#,
        message_start
      );

      // Check that the queries for unique messages across the entire time range returns exactly one result.
      let ast = QueryDslParser::parse(query_dsl::Rule::start, query_message)
        .expect("Failed to parse query");
      let results = index
        .search_logs(&ast, 0, u64::MAX)
        .await
        .expect("Error in search_logs");
      assert_eq!(results.len(), 1);

      let query_message = &format!(
        r#"{{ "query": {{ "match": {{ "_all": {{ "query" : "{}", "operator" : "AND" }} }} }} }}"#,
        message_end
      );

      let ast = QueryDslParser::parse(query_dsl::Rule::start, query_message)
        .expect("Failed to parse query");
      let results = index
        .search_logs(&ast, 0, u64::MAX)
        .await
        .expect("Error in search_logs");
      assert_eq!(results.len(), 1);
    }
  }

  #[tokio::test]
  async fn test_delete_segment_in_memory() {
    let storage_type = StorageType::Local;
    let (index, _index_dir_path) =
      create_index("test_delete_segment_in_memory", &storage_type).await;
    let message = "test_message";
    index.append_log_message(
      Utc::now().timestamp_millis() as u64,
      &HashMap::new(),
      message,
    );

    index.commit().await.expect("Could not commit");
    let segment_number = *index.get_current_segment_ref().key(); // Get current cos it has been committed to.

    // try to delete segment
    index
      .delete_segment(segment_number)
      .await
      .expect_err("Segment in memory: 0");
  }

  #[tokio::test]
  async fn test_delete_multiple_segments() {
    // Create 20 segments and keep 4 segments only in memory
    let num_segments_in_memory = 4;
    let segment_size_threshold_bytes = (0.0003 * 1024.0 * 1024.0) as u64;
    let search_memory_budget_bytes = num_segments_in_memory * segment_size_threshold_bytes;
    let storage_type = StorageType::Local;
    let (index, _index_dir_path) = create_index_with_thresholds(
      "test_delete_multiple_segments",
      &storage_type,
      // This size depends on the number of log messages added in each segment in the for loop below.
      segment_size_threshold_bytes,
      search_memory_budget_bytes,
    )
    .await;

    // Setting it high to test out that there is no single-threaded deadlock while commiting.
    // Note that if you change this value, some of the assertions towards the end of this test
    // may need to be changed.
    let num_segments = 20;

    for i in 0..num_segments {
      let start = i * 2 * 1000;
      let end = start + 500;
      // Insert unique messages in each segment - these will come handy for testing later.
      let message_start = &format!("message_{}", start);
      let message_end = &format!("message_{}", end);
      index.append_log_message(start, &HashMap::new(), message_start);
      index.append_log_message(end, &HashMap::new(), message_end);
      index.commit().await.expect("Could not commit index");
    }

    // We'll have num_segments segments, plus one empty segment at the end.
    assert_eq!(
      index.all_segments_summaries.read().await.len() as u64,
      num_segments + 1
    );

    index.shrink_to_fit();
    // We shouldn't have more than specified segments in memory.
    assert!(index.memory_segments_map.len() as u64 <= num_segments_in_memory);

    // Check the deleted segments count
    let mut delete_count = 0;
    for i in 0..num_segments {
      let result = index.delete_segment(i.try_into().unwrap()).await;
      if result.is_ok() {
        delete_count += 1;
      }
    }
    assert!(delete_count >= 16);
  }
}

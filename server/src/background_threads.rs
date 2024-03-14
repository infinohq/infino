use std::sync::Arc;

use chrono::Utc;
use log::{debug, error, info};

use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::AppState;
use crate::IS_SHUTDOWN;

// Returns true if the specified join handle is not none and is not finished.
fn is_join_handle_running(handle: &Option<JoinHandle<()>>) -> bool {
  match handle {
    Some(handle) => {
      if handle.is_finished() {
        // The thread cooresponding to this handle is finished.
        return false;
      }
    }
    None => return false, // Handle isn't initialized yet - so no thread corresponding to
                          // this handle is running.
  }

  // None of the above conditions are true, so the thread corresponding to this handle
  // is running
  true
}

/// Function to flush WAL by starting a new thread as necessory.
fn check_and_start_flush_wal_thread(
  state: Arc<AppState>,
  flush_wal_handle: &mut Option<JoinHandle<()>>,
) {
  if !is_join_handle_running(flush_wal_handle) {
    // The thread to flush WAL isn't started or has finished.
    // Start a new thread for flushing WAL, and update the flush_wal_handle.
    *flush_wal_handle = Some(tokio::spawn(async move {
      state.coredb.flush_wal().await;
    }));
  }
}

/// Function to commit indices by starting a new thread as necessory.
fn check_and_start_commit_thread(
  state: Arc<AppState>,
  commit_handle: &mut Option<JoinHandle<()>>,
  is_shutdown: bool,
) {
  if !is_join_handle_running(commit_handle) {
    // The thread to flush WAL isn't started or has finished.
    // Start a new thread for commit, and update the commit_handle.
    *commit_handle = Some(tokio::spawn(async move {
      let result = state.coredb.commit(is_shutdown).await;

      // Handle the result of the commit operation
      match result {
        Ok(_) => debug!("Commit successful"),
        Err(e) => error!("Commit failed: {}", e),
      }
    }));
  }
}

/// Function to execute retention policy by starting a new thread as necessory.
/// Returns true if a new retention thread was started, returns false otherwise.
fn check_and_start_retention_thread(
  state: Arc<AppState>,
  retention_handle: &mut Option<JoinHandle<()>>,
) -> bool {
  if !is_join_handle_running(retention_handle) {
    // The thread to run retention policy isn't started or has finished.
    // Start a new thread for executing retention policy, and update the retention_handle.
    *retention_handle = Some(tokio::spawn(async move {
      info!("Triggering retention policy on index in coredb");
      let result = state.coredb.trigger_retention().await;
      if let Err(e) = result {
        error!(
          "Error triggering retention policy on index in coredb: {}",
          e
        );
      }
    }));

    return true;
  }

  false
}

/// Periodically commits CoreDB to disk (typically called in a thread so that CoreDB
/// can be asyncronously committed), and triggers retention policy every hour
pub async fn check_and_start_background_threads(state: Arc<AppState>) {
  let mut last_trigger_policy_time = Utc::now().timestamp_millis() as u64;
  let mut flush_wal_handle: Option<JoinHandle<()>> = None;
  let mut commit_handle: Option<JoinHandle<()>> = None;
  let mut retention_handle: Option<JoinHandle<()>> = None;

  // TODO: make trigger policy interval configurable
  let policy_interval_ms = 3600000; // 1hr in ms

  loop {
    // Start flush log thread - if one isn't running already.
    check_and_start_flush_wal_thread(state.clone(), &mut flush_wal_handle);

    // Check if we need to shut down (typically triggered by the user by sending Ctrl-C on Infino server).
    // We can check for this anywhere in the loop, but we check just before commit to avoid extra work
    // (such as running retention policy even after user initiates shutdown).
    let is_shutdown = IS_SHUTDOWN.load();

    // Start commit thread - if one isn't running already.
    check_and_start_commit_thread(state.clone(), &mut commit_handle, is_shutdown);

    // Exit from the loop after shutting down background threads if is_shutdown is set.
    if is_shutdown {
      // Gather the handles of all the background threads.
      let mut join_handles = Vec::new();
      if let Some(handle) = flush_wal_handle {
        join_handles.push(handle);
      }
      if let Some(handle) = commit_handle {
        join_handles.push(handle);
      }
      if let Some(handle) = retention_handle {
        join_handles.push(handle);
      }

      // Wait for the background threads to finish and check for errors.
      for handle in join_handles {
        if let Err(e) = handle.await {
          error!("Error while joining thread: {}", e);
        }
      }

      // break from the loop - as we don't want to start any more background threads.
      break;
    }

    // Start retention thread - if one isn't running already.
    let current_time = Utc::now().timestamp_millis() as u64;
    if current_time - last_trigger_policy_time > policy_interval_ms {
      let new_retention_thread_started =
        check_and_start_retention_thread(state.clone(), &mut retention_handle);
      if new_retention_thread_started {
        // Update last_trigger_policy_time only if a new retention thread was started.
        last_trigger_policy_time = current_time;
      }
    }

    // Sleep for some time before committing again.
    sleep(Duration::from_millis(1000)).await;
  }
} // end loop {..}

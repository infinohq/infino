use std::sync::Arc;

use chrono::Utc;
use log::{debug, error, info};

use tokio::io::Join;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::AppState;
use crate::IS_SHUTDOWN;

/// Function to flush WAL by starting a new thread as necessory.
async fn check_and_start_flush_wal_thread(
  state: Arc<AppState>,
  flush_wal_handle: &mut Option<JoinHandle<()>>,
) {
  // Check if a new thread for flushing WAL should be started. A new thread needs to be started if
  // the flush_wal_handle is not set, or it points to a thread that has finished.
  let mut start_flush_wal_thread = false;
  match flush_wal_handle {
    Some(handle) => {
      if handle.is_finished() {
        start_flush_wal_thread = true;
      }
    }
    None => start_flush_wal_thread = true,
  }

  if start_flush_wal_thread {
    // Start a new thread for flushing WAL, and update the flush_wal_handle.
    *flush_wal_handle = Some(tokio::spawn(async move {
      state.coredb.flush_wal().await;
    }));
  }
}

/// Periodically commits CoreDB to disk (typically called in a thread so that CoreDB
/// can be asyncronously committed), and triggers retention policy every hour
pub async fn commit_in_loop(state: Arc<AppState>) {
  let mut last_trigger_policy_time = Utc::now().timestamp_millis() as u64;
  let policy_interval_ms = 3600000; // 1hr in ms
  let mut flush_wal_handle: Option<JoinHandle<()>> = None;
  let mut commit_handle: Option<JoinHandle<()>> = None;

  loop {
    check_and_start_flush_wal_thread(state.clone(), &mut flush_wal_handle);

    // Check if we need to shut down (typically triggered by the user by sending Ctrl-C on Infino server).
    let is_shutdown = IS_SHUTDOWN.load();

    // Part 2 -
    // Commit the index to object store. Set commit_current_segment to is_shutdown -- i.e.,
    // commit the current segment only when the server is shutting down.

    // TODO-------- start here
    let state_clone = state.clone();
    let is_shutdown_clone = is_shutdown;
    commit_handle = tokio::spawn(async move {
      let result = state_clone.coredb.commit(is_shutdown_clone).await;

      // Handle the result of the commit operation
      match result {
        Ok(_) => debug!("Commit successful"),
        Err(e) => error!("Commit failed: {}", e),
      }
    });

    // Exit from the loop if is_shutdown is set.
    if is_shutdown {
      // Wait for wal flush thread to finish and check for errors.
      let result = flush_wal_handle.await;
      match result {
        Ok(_) => {
          info!("Write ahead log flush thread completed successfully");
        }
        Err(e) => {
          error!("Error while joining write ahead log flush thread {}", e);
        }
      }

      // Wait for commit thread to finish and check for errors.
      let result = commit_handle.await;
      match result {
        Ok(_) => {
          info!("Commit thread completed successfully");
        }
        Err(e) => {
          error!("Error while joining commit thread {}", e);
        }
      }
      break;
    }

    let current_time = Utc::now().timestamp_millis() as u64;
    // TODO: make trigger policy interval configurable
    if current_time - last_trigger_policy_time > policy_interval_ms {
      info!("Triggering retention policy on index in coredb");
      let result = state.coredb.trigger_retention().await;

      if let Err(e) = result {
        error!(
          "Error triggering retention policy on index in coredb: {}",
          e
        );
      }

      last_trigger_policy_time = current_time;
    }

    // Sleep for some time before committing again.
    sleep(Duration::from_millis(1000)).await;
  } // end loop {..}
}

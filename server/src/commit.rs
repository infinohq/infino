use std::sync::Arc;

use chrono::Utc;
use log::{debug, error, info};

use tokio::time::{sleep, Duration};

use crate::AppState;
use crate::IS_SHUTDOWN;

/// Periodically commits CoreDB to disk (typically called in a thread so that CoreDB
/// can be asyncronously committed), and triggers retention policy every hour
pub async fn commit_in_loop(state: Arc<AppState>) {
  let mut last_trigger_policy_time = Utc::now().timestamp_millis() as u64;
  let policy_interval_ms = 3600000; // 1hr in ms
  loop {
    let is_shutdown = IS_SHUTDOWN.load();

    // Commit the indexes to object store. Set commit_current_segment to is_shutdown -- i.e.,
    // commit the current segment only when the server is shutting down.
    let state_clone = state.clone();
    let is_shutdown_clone = is_shutdown;
    let commit_handle = tokio::spawn(async move {
      for index_entry in state_clone.coredb.get_index_map().iter() {
        let index = index_entry.value();
        let index_name = index_entry.key();
        let result = index.commit(is_shutdown_clone).await;
        // Handle the result of the commit operation
        match result {
          Ok(_) => debug!("Commit for index {} successful", index_name),
          Err(e) => error!("Commit failed for index {}: {}", index_name, e),
        }

        let current_time = Utc::now().timestamp_millis() as u64;
        // TODO: make trigger policy interval configurable
        if current_time - last_trigger_policy_time > policy_interval_ms {
          info!("Triggering retention policy on index in coredb");
          let result = state_clone.coredb.trigger_retention(index_name).await;

          if let Err(e) = result {
            error!(
              "Error triggering retention policy on index in coredb: {}",
              e
            );
          }

          last_trigger_policy_time = current_time;
        }
      }
    });

    // Exit from the loop if is_shutdown is set.
    if is_shutdown {
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

    // Sleep for some time before committing again.
    sleep(Duration::from_millis(1000)).await;
  } // end loop {..}
}

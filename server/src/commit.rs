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

    // Commit the index to object store. Set commit_current_segment to is_shutdown -- i.e.,
    // commit the current segment only when the server is shutting down.
    let state_clone = state.clone();
    let is_shutdown_clone = is_shutdown;
    tokio::spawn(async move {
      let result = state_clone.coredb.commit(is_shutdown_clone).await;

      // Handle the result of the commit operation
      match result {
        Ok(_) => debug!("Commit successful"),
        Err(e) => error!("Commit failed: {}", e),
      }
    });

    if is_shutdown {
      info!("Received shutdown in commit thread. Exiting...");
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
    sleep(Duration::from_millis(100)).await;
  } // end loop {..}
}

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use log::info;
use tokio::signal;

/// Handle shuwdown signal.
pub async fn shutdown_signal() {
  let ctrl_c = async {
    signal::ctrl_c()
      .await
      .expect("failed to install Ctrl+C handler");
  };

  #[cfg(unix)]
  let terminate = async {
    signal::unix::signal(signal::unix::SignalKind::terminate())
      .expect("failed to install signal handler")
      .recv()
      .await;
  };

  #[cfg(not(unix))]
  let terminate = std::future::pending::<()>();

  tokio::select! {
      _ = ctrl_c => {},
      _ = terminate => {},
  }

  info!("Shutdown signal received, starting graceful shutdown. This may take several seconds...");
}

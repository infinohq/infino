use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[test]
fn test_cargo_run_infino() {
  // Set the config directory path.
  std::env::set_var("INFINO_CONFIG_DIR_PATH", "../config");

  // Start the Infino server process.
  let mut child = Command::new("cargo")
    .args(["run", "infino"])
    .stdout(Stdio::piped())
    .spawn()
    .expect("Failed to start process");

  // Wait for 30 seconds.
  thread::sleep(Duration::from_secs(30));

  // Check if the process is still running - fail in case the server isn't running.
  match child.try_wait() {
    Ok(Some(status)) => {
      panic!("Process exited prematurely with status: {}", status);
    }
    Ok(None) => {
      println!("Process is still running, proceeding...");
    }
    Err(e) => {
      panic!("Error while checking process status: {}", e);
    }
  }

  // Send SIGTERM to the process for graceful shutdown.
  let _ = Command::new("kill")
    .args(["-s", "TERM", &child.id().to_string()])
    .spawn()
    .expect("Failed to send SIGTERM to server");

  // Wait for 30 seconds.
  thread::sleep(Duration::from_secs(30));

  // Check if the process is still running - fail in case the server is still running.
  match child.try_wait() {
    Ok(Some(status)) => {
      println!("Server process exited with status: {}", status);
    }
    Ok(None) => {
      panic!("Server did not exit event after sending SIGTERM.");
    }
    Err(e) => {
      panic!("Error while checking process status: {}", e);
    }
  }
}

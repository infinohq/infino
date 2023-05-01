use chrono::Utc;
use reqwest::StatusCode;
use std::{
  process::Child,
  thread,
  time::{self, Instant},
};
use sysinfo::{ProcessorExt, System, SystemExt};
use tokio::task::JoinHandle;

use crate::utils::io::run_cargo_in_dir;

pub struct InfinoTsClient {
  process: Child,
  cpu_usage_task: Option<JoinHandle<()>>,
}

impl InfinoTsClient {
  pub fn new() -> InfinoTsClient {
    let dir_path = "../";
    let package_name = "infino";

    // Start infino server on port 3000
    let mut child = run_cargo_in_dir(dir_path, package_name).unwrap();

    println!("Started process with PID {}", child.id());

    // Let the server start
    thread::sleep(time::Duration::from_millis(25000));

    // Start a background task to update the CPU usage gauge
    // Start a background task to update the CPU usage gauge
    let cpu_usage_task = Some(tokio::spawn(async move {
      loop {
        let system = System::new();
        // Get the current CPU usage percentage
        let cpu_usage_percent = system.get_processors()[0].get_cpu_usage();
        // Update the Prometheus Gauge with the CPU usage percentage

        let time = Utc::now().timestamp_millis() as u64;
        let value = cpu_usage_percent as f64;

        let json_str = format!("{{\"date\": {}, \"{}\":{}}}", time, "cpu_usage", value);
        let client = reqwest::Client::new();
        let res = client
          .post(&format!("http://localhost:3000/append_ts"))
          .header("Content-Type", "application/json")
          .body(json_str)
          .send()
          .await;

        match res {
          Ok(response) => {
            if response.status() != StatusCode::OK {
              println!("Error while pushing ts to infino {:?}", response)
            }
          }
          Err(e) => {
            println!("Error while pushing ts to infino {:?}", e)
          }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
      }
    }));

    InfinoTsClient {
      process: child,
      cpu_usage_task,
    }
  }

  pub async fn search(&self) -> u128 {
    let query_url =
      "http://localhost:3000/search_ts?label_name=__name__&&label_value=cpu_usage&start_time=0";
    let now = Instant::now();
    let response = reqwest::get(query_url).await;
    let elapsed = now.elapsed();
    println!(
      "Infino Time series time required for searching {:.2?}",
      elapsed
    );

    // println!("Response {:?}", response);
    match response {
      Ok(res) => {
        let text = res.text().await.unwrap();
        // println!("Result {}", text);
        elapsed.as_nanos()
      }
      Err(err) => {
        println!("Error while fetching from prometheus: {}", err);
        elapsed.as_nanos()
      }
    }
  }

  pub fn stop(mut self) {
    self.process.kill().unwrap();
    if let Some(cpu_usage_task) = self.cpu_usage_task.take() {
      cpu_usage_task.abort();
    }
  }
}

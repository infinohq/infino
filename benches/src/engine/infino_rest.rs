use chrono::Utc;
use reqwest::{StatusCode, Body};
use serde_json::json;
use core::num;
use std::{
  process::Child,
  thread,
  time::{self, Instant}, collections::HashMap,
};
use sysinfo::{ProcessorExt, System, SystemExt};
use tokio::task::JoinHandle;

use crate::utils::io::run_cargo_in_dir;
use crate::utils::io;

pub struct InfinoApiClient {
  process: Child,
}

impl InfinoApiClient {
  pub fn new() -> InfinoApiClient {
    let dir_path = "../";
    let package_name = "infino";

    // Start infino server on port 3000
    let mut child = run_cargo_in_dir(dir_path, package_name).unwrap();

    println!("Started process with PID {}", child.id());

    InfinoApiClient { process: child }
  }

  pub async fn index_lines(&self, input_data_path: &str, max_docs: i32) {
    let mut num_docs = 0;
    if let Ok(lines) = io::read_lines(input_data_path) {
      let mut log = HashMap::new();
      let client = reqwest::Client::new();
      let time = Utc::now().timestamp_millis() as u64;
      log.insert("date", json!(time));
      for line in lines {
        num_docs += 1;

        // If max_docs is less than 0, we index all the documents.
        // Otherwise, do not indexing more than max_docs documents.
        if max_docs > 0 && num_docs > max_docs {
          println!(
            "Already indexed {} documents. Not indexing anymore.",
            max_docs
          );
          break;
        }
        if let Ok(message) = line {
            log.insert("message", json!(message));
        }
      }

      let now = Instant::now();
      let insert = client
        .post(&format!("http://localhost:3000/append_ts"))
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&log).unwrap()))
        .send()
        .await;
      let elapsed = now.elapsed();
      println!("Infino time required for insertion: {:.2?}", elapsed);
      println!("#{} {:?}", num_docs, insert);
    }
  }

  pub async fn search(&self, text:  &str) -> u128 {
    let query_url =
      &format!("http://localhost:3000/search_ts?text={}&start_time=0", text);
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
  }
}

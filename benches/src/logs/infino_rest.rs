// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0

use std::{collections::HashMap, time::Instant};

use chrono::Utc;
use reqwest::Body;
use serde_json::json;

use crate::utils::io;

pub struct InfinoApiClient {
  // No configuration needed-
}

impl InfinoApiClient {
  pub fn new() -> InfinoApiClient {
    InfinoApiClient {}
  }

  /// Indexes input data and returns the time required for insertion as microseconds.
  pub async fn index_lines(&self, input_data_path: &str, max_docs: i32) -> u128 {
    let mut num_docs = 0;

    // This is kept same as ClickHouse to so that equivalent API requests are made across
    // Clickhouse and Infino REST API.
    let num_docs_per_batch = 100;
    let mut num_docs_in_this_batch = 0;
    let mut logs_batch = Vec::new();
    let now = Instant::now();

    if let Ok(lines) = io::read_lines(input_data_path) {
      let client = reqwest::Client::new();
      for line in lines {
        num_docs += 1;
        num_docs_in_this_batch += 1;

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
          let mut log = HashMap::new();
          log.insert("date", json!(Utc::now().timestamp_millis() as u64));
          log.insert("message", json!(message));
          logs_batch.push(log);
          if num_docs_in_this_batch == num_docs_per_batch {
            let _ = client
              .post("http://localhost:3000/default/append_log")
              .header("Content-Type", "application/json")
              .body(Body::from(serde_json::to_string(&logs_batch).unwrap()))
              .send()
              .await;
            logs_batch.clear();
            num_docs_in_this_batch = 0;
          }
        }
      }
    }

    let elapsed = now.elapsed().as_micros();
    println!(
      "Infino REST API - time required for log insertions: {} microseconds",
      elapsed
    );
    elapsed
  }

  /// Searches the given term and returns the time required in microseconds
  pub async fn search_logs(&self, text: &str, range_start_time: u64, range_end_time: u64) -> u128 {
    let query_url = &format!(
      "http://localhost:3000/search_logs?q={}&start_time={}&end_time={}",
      text, range_start_time, range_end_time
    );

    let now = Instant::now();
    let response = reqwest::get(query_url).await;
    let elapsed = now.elapsed().as_micros();
    println!(
      "Infino REST API - time required for logs search {} is : {} microseconds",
      text, elapsed
    );

    //println!("Response {:?}", response);
    match response {
      Ok(res) => {
        #[allow(unused)]
        let text = res.text().await.unwrap();
        //println!("Result {}", text);
      }
      Err(err) => {
        println!("Error while fetching from Infino: {}", err);
      }
    }
    elapsed
  }

  /// Runs multiple queries and returns the sum of time needed to run them in microseconds.
  pub async fn search_multiple_queries(&self, queries: &[&str]) -> u128 {
    let mut time = 0;
    for query in queries {
      time += self.search_logs(query, 0, u64::MAX).await;
    }
    time
  }

  #[allow(unused)]
  pub async fn flush(&self) {
    let client = reqwest::Client::new();

    let _ = client
      .post("http://localhost:3000/flush")
      .body("")
      .send()
      .await;
  }

  #[allow(unused)]
  pub fn get_index_dir_path(&self) -> &str {
    "../index"
  }
}

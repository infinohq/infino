// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0

use std::{time::Instant};

use chrono::Utc;
use reqwest::Body;
use serde_json::json;

use crate::utils::io;

pub struct InfinoOSApiClient {
  // No configuration needed-
}

impl InfinoOSApiClient {
  pub fn new() -> InfinoOSApiClient {
    InfinoOSApiClient {}
  }

  /// Indexes input data and returns the time required for insertion as microseconds.
  pub async fn index_lines(&self, input_data_path: &str, max_docs: i32) -> u128 {
    let mut num_docs = 0;

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
          logs_batch.push(json!(
            {"index": {"_id": num_docs}}
          ));
          logs_batch.push(json!({ 
            "_id": num_docs,
            "date": Utc::now().timestamp_millis() as u64,
            "message": message,
          }));
          if num_docs_in_this_batch == num_docs_per_batch {
            // Join all elements in logs_batch delimitted by \n 
            let body_str = logs_batch.iter()
                              .map(|v| v.to_string())
                              .collect::<Vec<_>>()
                              .join("\n");

            // Prepend and append a newline to the body string
            let mut body_str_newline = String::new();
            body_str_newline.push('\n');
            body_str_newline.push_str(&body_str);
            body_str_newline.push('\n');

            let _ = client
              .post("http://localhost:9200/infino/_bulk")
              .header("Content-Type", "application/json")
              .body(Body::from(body_str_newline))
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
      "Infino OS REST API - time required for log insertions: {} microseconds",
      elapsed
    );
    elapsed
  }

  /// Searches the given term and returns the time required in microseconds
  pub async fn search_logs(&self, text: &str, range_start_time: u64, range_end_time: u64) -> u128 {
    let query_url = &format!(
      "http://localhost:9200/infino/perfindex/logs/_search?text={}&startTime={}&endTime={}",
      text, range_start_time, range_end_time
    );

    let now = Instant::now();
    let response = reqwest::get(query_url).await;
    let elapsed = now.elapsed().as_micros();
    println!(
      "Infino OS REST API - time required for logs search {} is : {} microseconds",
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

/* 
 * flush is not implemented in Infino OS.
 * We'll un-comment this and update it with the right API end point
 * when flush is implemented.
 * 
  #[allow(unused)]
  pub async fn flush(&self) {
    let client = reqwest::Client::new();

    let _ = client
      .post("http://localhost:2999/flush")
      .body("")
      .send()
      .await;
  }
 */

  #[allow(unused)]
  pub fn get_index_dir_path(&self) -> &str {
    "../index"
  }
}

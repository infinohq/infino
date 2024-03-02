use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

use chrono::DateTime;
use clap::{arg, Command};
use reqwest::{self, Body, Client};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::{fs::File, io::BufReader};

use coredb::CoreDB;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Debug, Serialize, Deserialize)]
/// An Apache log entry - for Apache access logs.
struct ApacheLogEntry {
  date: u64,
  ip: String,
  request: String,
  status_code: u16,
  response_size: usize,
  referrer: String,
  user_agent: String,
}

impl ApacheLogEntry {
  fn get_fields_map(&self) -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("ip".to_string(), self.ip.clone());
    map.insert("request".to_string(), self.request.clone());
    map.insert("status_code".to_string(), self.status_code.to_string());
    map.insert("response_size".to_string(), self.response_size.to_string());
    map.insert("referrer".to_string(), self.referrer.clone());
    map.insert("user_agent".to_string(), self.user_agent.clone());

    map
  }
}

/// Parse a datetime string to a timestamp in microseconds.
fn parse_datetime(datetime_str: &str) -> Option<u64> {
  let fmt = "%d/%b/%Y:%H:%M:%S %z"; // Example: "01/Jan/2023:00:00:00 +0530"
  let datetime = DateTime::parse_from_str(datetime_str, fmt).ok()?;
  Some(datetime.timestamp_millis() as u64)
}

/// Parse a log line into an ApacheLogEntry.
fn parse_log_line(line: &str) -> Option<ApacheLogEntry> {
  let parts: Vec<&str> = line.split_whitespace().collect();
  if parts.len() >= 10 {
    let ip = parts[0].to_string();
    let datetime_str =
      parts[3].trim_start_matches('[').to_string() + " " + parts[4].trim_end_matches(']');
    let date = parse_datetime(&datetime_str)?;

    let request = parts[5].trim_start_matches('"').to_string()
      + " "
      + parts[6]
      + " "
      + parts[7].trim_end_matches('"');
    let status_code = parts[8].parse().ok()?;
    let response_size = parts[9].parse().ok()?;
    let referrer = parts[10].trim_matches('"').to_string();
    let user_agent = parts[11..].join(" ").trim_matches('"').to_string();

    Some(ApacheLogEntry {
      ip,
      date,
      request,
      status_code,
      response_size,
      referrer,
      user_agent,
    })
  } else {
    None
  }
}

/// Parse arguments from the command line and return logs file name, count of lines to index, and Infino URL.
fn get_args() -> (String, i64, String, bool) {
  // Parse command line arguments.
  let matches = Command::new("Index Apache Logs")
    .version("1.0")
    .about("Index Apache Logs")
    .arg(
      arg!(--file <VALUE>)
        .required(false)
        .default_value("examples/datasets/apache-tiny.log"),
    )
    .arg(
      arg!(--count <VALUE>)
        .required(false)
        .default_value("100000")
        .value_parser(clap::value_parser!(i64)),
    )
    .arg(
      arg!(--infino_url <VALUE>)
        .required(false)
        .default_value("http://localhost:3000"),
    )
    .arg(
      arg!(--coredb_only)
        .required(false)
        .default_value("false")
        .value_parser(clap::value_parser!(bool)),
    )
    .get_matches();

  let file = matches
    .get_one::<String>("file")
    .expect("Could not get file");
  let count = matches
    .get_one::<i64>("count")
    .expect("Could not get count");
  let infino_url = matches
    .get_one::<String>("infino_url")
    .expect("Could not get infino url");
  let coredb_only = matches
    .get_one::<bool>("coredb_only")
    .expect("Could not get coredb_only flag");

  (file.clone(), *count, infino_url.clone(), *coredb_only)
}

async fn index_logs_batch(client: &Client, append_url: &str, logs_batch: &Vec<ApacheLogEntry>) {
  loop {
    let response = client
      .post(append_url)
      .header("Content-Type", "application/json")
      .body(Body::from(serde_json::to_string(&logs_batch).unwrap()))
      .send()
      .await;
    let status = response.as_ref().unwrap().status();
    if status == 429 {
      println!("Too many requests, sleeping for 1s");
      thread::sleep(std::time::Duration::from_millis(1000));
    } else {
      assert_eq!(response.as_ref().unwrap().status(), 200);
      break;
    }
  }
}

/// Helper function to index max_docs from file using Infino API.
async fn index(
  file_path: &str,
  max_docs: i64,
  infino_url: &str,
  coredb_only: bool,
) -> Result<(), std::io::Error> {
  let mut num_docs = 0;
  let num_docs_per_batch = 1000;
  let mut num_docs_in_this_batch = 0;
  let mut batch_count = 0;
  let mut logs_batch = Vec::new();
  let now = Instant::now();
  let append_url = &format!("{}/append_log", infino_url);
  let client = reqwest::Client::new();

  // User only when coredb_only is set to true.
  let config_dir_path = "../../config";
  let coredb = CoreDB::new(config_dir_path).await.unwrap();

  let file = File::open(file_path).await.unwrap();
  let reader = BufReader::new(file);
  let mut lines = reader.lines();

  while let Some(line) = lines.next_line().await? {
    // If max_docs is less than 0, we index all the documents.
    // Otherwise, do not indexing more than max_docs documents.
    if max_docs > 0 && num_docs >= max_docs {
      println!(
          "Reached max documents limit - {}. Exiting from the indexing loop and indexing last batch (if any).",
          max_docs
        );
      break;
    }
    let log = parse_log_line(&line).unwrap();
    num_docs += 1;

    if coredb_only {
      // Index the log entry using CoreDB.
      let timestamp = log.date;
      let fields = log.get_fields_map();
      let text = fields.values().cloned().collect::<Vec<String>>().join(" ");
      loop {
        let result = coredb.append_log_message(timestamp, &fields, &text).await;
        if result.is_err() {
          println!(
            "Received error from CoreDB, retrying after sleeping: {}",
            result.err().unwrap()
          );
          thread::sleep(Duration::from_secs(1));
        } else {
          break;
        }
      }
    }

    // Index the log entries in batches using Infino REST API.
    num_docs_in_this_batch += 1;

    logs_batch.push(log);
    if num_docs_in_this_batch == num_docs_per_batch {
      index_logs_batch(&client, append_url, &logs_batch).await;
      logs_batch.clear();
      num_docs_in_this_batch = 0;
      println!("Indexed batch {}", batch_count);
      batch_count += 1;
    }
  }

  // Index the last batch if it is not empty.
  if !logs_batch.is_empty() {
    index_logs_batch(&client, append_url, &logs_batch).await;
    println!("Indexed last batch {}", batch_count);
  }

  let elapsed = now.elapsed().as_micros();
  println!(
    "Infino REST time required for insertion: {} microseconds",
    elapsed
  );

  Ok(())
}

#[tokio::main]
async fn main() {
  #[cfg(feature = "dhat-heap")]
  let _profiler = dhat::Profiler::builder().trim_backtraces(None).build();

  // Get the command line arguments.
  let (file, count, infino_url, coredb_only) = get_args();

  println!(
    "Indexing first {} log lines from file {} coredb_only flag {}",
    count, file, coredb_only
  );

  // Index 'count' number of lines from 'file' using 'infino_url' or coredb directly.
  index(&file, count, &infino_url, coredb_only)
    .await
    .expect("Could not index logs");
}

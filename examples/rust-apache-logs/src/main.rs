use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::Instant;

use chrono::DateTime;
use clap::{arg, Command};
use reqwest::{self, Body, Client};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
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
fn get_args() -> (String, i64, String) {
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

  (file.clone(), *count, infino_url.clone())
}

/// The output is wrapped in a Result to allow matching on errors.
/// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
  P: AsRef<Path>,
{
  let file = File::open(filename)?;
  Ok(io::BufReader::new(file).lines())
}

async fn index_logs_batch(client: &Client, append_url: &str, logs_batch: &Vec<ApacheLogEntry>) {
  let response = client
    .post(append_url)
    .header("Content-Type", "application/json")
    .body(Body::from(serde_json::to_string(&logs_batch).unwrap()))
    .send()
    .await;
  assert_eq!(response.as_ref().unwrap().status(), 200);
}

/// Helper function to index max_docs from file using Infino API.
async fn index_using_infino(file: &str, max_docs: i64, infino_url: &str) {
  let mut num_docs = 0;
  let num_docs_per_batch = 1000;
  let mut num_docs_in_this_batch = 0;
  let mut batch_count = 0;
  let mut logs_batch = Vec::new();
  let now = Instant::now();
  let append_url = &format!("{}/append_log", infino_url);
  let client = reqwest::Client::new();

  if let Ok(lines) = read_lines(file) {
    for line in lines {
      // If max_docs is less than 0, we index all the documents.
      // Otherwise, do not indexing more than max_docs documents.
      if max_docs > 0 && num_docs >= max_docs {
        println!(
          "Reached max documents limit - {}. Exiting from the indexing loop and indexing last batch (if any).",
          max_docs
        );
        break;
      }

      num_docs += 1;
      num_docs_in_this_batch += 1;

      if let Ok(message) = line {
        let log = parse_log_line(&message);
        match log {
          Some(log) => {
            logs_batch.push(log);
            if num_docs_in_this_batch == num_docs_per_batch {
              index_logs_batch(&client, append_url, &logs_batch).await;
              logs_batch.clear();
              num_docs_in_this_batch = 0;
              println!("Indexed batch {}", batch_count);
              batch_count += 1;
            }
          }
          None => {
            println!("Could not parse log line: {}", message);
          }
        }
      }
    }

    // Index the last batch if it is not empty.
    if !logs_batch.is_empty() {
      index_logs_batch(&client, append_url, &logs_batch).await;
      println!("Indexed last batch {}", batch_count);
    }
  }

  let elapsed = now.elapsed().as_micros();
  println!(
    "Infino REST time required for insertion: {} microseconds",
    elapsed
  );
}

#[tokio::main]
async fn main() {
  // Get the command line arguments.
  let (file, count, infino_url) = get_args();

  println!(
    "Indexing first {} log lines from file {} using infino url {}",
    count, file, infino_url
  );

  // Index 'count' number of lines from 'file' using 'infino_url'.
  index_using_infino(&file, count, &infino_url).await;
}

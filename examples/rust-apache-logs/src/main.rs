use std::fs::File;
use std::io::{BufRead, BufReader};

use chrono::DateTime;
use clap::{arg, Command};

#[allow(dead_code)]
#[derive(Debug)]
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
  Some(datetime.timestamp_micros() as u64)
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
fn get_args() -> (String, usize, String) {
  // Parse command line arguments.
  let matches = Command::new("Index Apache Logs")
    .version("1.0")
    .about("Index Apache Logs")
    .arg(
      arg!(--file <VALUE>)
        .required(false)
        .default_value("examples/rust-apache-logs/data/apache-tiny.log"),
    )
    .arg(
      arg!(--count <VALUE>)
        .required(false)
        .default_value("100000")
        .value_parser(clap::value_parser!(usize)),
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
    .get_one::<usize>("count")
    .expect("Could not get count");
  let infino_url = matches
    .get_one::<String>("infino_url")
    .expect("Could not get infino url");

  (file.clone(), *count, infino_url.clone())
}

fn index_using_infino(file: &str, count: usize, infino_url: &str) {
  let file = File::open(file).expect("Could not open apache.log");
  let reader = BufReader::new(file);
  let num_docs_per_batch = 1000;

  let mut num_docs_in_this_batch = 0;
  for line in reader.lines().take(count) {
    let line = line.expect("Could not receive line");
    if let Some(log_entry) = parse_log_line(&line) {
      println!("Indexing {:?}", log_entry);
      num_docs_in_this_batch += 1;
    }
  }
}

fn main() {
  // Get the command line arguments.
  let (file, count, infino_url) = get_args();

  println!(
    "Indexing first {} log lines from file {} using infino url {}",
    count, file, infino_url
  );

  // Index 'count' number of lines from 'file' using 'infino_url'.
  index_using_infino(&file, count, &infino_url);
}

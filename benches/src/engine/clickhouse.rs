use std::time::Instant;

use chrono::Utc;
use clickhouse::Client;
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::utils::io;

#[derive(Row, Serialize, Deserialize)]
struct LogRow {
  time: u64,
  text: String,
}

pub struct ClickhouseEngine {
  client: Client,
}

impl ClickhouseEngine {
  pub async fn new() -> ClickhouseEngine {
    let client = Client::default()
      .with_url("http://localhost:8123")
      .with_user("default")
      .with_database("test_logs");

    client
      .query(
        "create table if not exists test_logs.test_logs (id UUID DEFAULT generateUUIDv4(), 
      time Int64, text String, PRIMARY KEY (id)) Engine=MergeTree;",
      )
      .execute()
      .await
      .unwrap();

    ClickhouseEngine { client }
  }

  /// Indexes input data and returns the time required for insertion as microseconds.
  pub async fn index_lines(&mut self, input_data_path: &str, max_docs: i32) -> u128 {
    let mut num_docs = 0;
    let now = Instant::now();
    let mut inserter = self
      .client
      .inserter("test_logs.test_logs")
      .unwrap()
      .with_max_entries(100)
      .with_period(Some(std::time::Duration::from_secs(15)));
    if let Ok(lines) = io::read_lines(input_data_path) {
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
          inserter
            .write(&LogRow {
              time: Utc::now().timestamp_millis() as u64,
              text: message,
            })
            .await
            .unwrap();
          inserter.commit().await.unwrap();
        }
      }
    }
    inserter.end().await.unwrap();

    let elapsed = now.elapsed().as_micros();
    println!(
      "Clickhouse time required for insertion: {} microseconds",
      elapsed
    );

    return elapsed;
  }

  /// Searches the given term and returns the time required in microseconds
  pub async fn search(&self, query: &str, range_start_time: u64, range_end_time: u64) -> u128 {
    let words: Vec<_> = query.split_whitespace().collect();

    // There is no inverted index in Clickhouse, so we need to use the `like` operator for free-text search.
    let words_with_like: Vec<_> = words
      .iter()
      .map(|s| format!("text LIKE '%{}%'", s))
      .collect();
    let like_clause = words_with_like.join(" AND ");
    let clickhouse_query = format!(
      "select * from test_logs.test_logs where {} and time > {} and time < {} order by time desc",
      like_clause, range_start_time, range_end_time
    );
    println!("Clickhouse query is {}", clickhouse_query);

    let now: Instant = Instant::now();
    let cursor = self.client.query(&clickhouse_query).fetch_all::<LogRow>();
    #[allow(unused)]
    let rows = cursor.await;

    // TODO: figure out hot to get count of results from Clickhouse results.
    //let count = rows.unwrap().len();
    let elapsed = now.elapsed().as_micros();

    println!(
      "Clickhouse time required for searching query {} is : {} microseconds",
      query, elapsed
    );
    return elapsed;
  }

  pub fn get_index_dir_path(&self) -> &str {
    "./ch-tmp/data/test_logs/test_logs/"
  }

  /// Runs multiple queries and returns the sum of time needed to run them in microseconds.
  pub async fn search_multiple_queries(&self, queries: &[&str]) -> u128 {
    let mut time = 0;
    for query in queries {
      time += self.search(query, 0, u64::MAX).await;
    }
    return time;
  }
}

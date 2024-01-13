use std::collections::HashMap;
use std::time::Instant;

use chrono::Utc;
use coredb::utils::config::Settings;
use coredb::CoreDB;

use crate::utils::io;

pub struct InfinoEngine {
  index_dir_path: String,
  coredb: CoreDB,
}

impl InfinoEngine {
  pub fn new(config_path: &str) -> InfinoEngine {
    let setting = Settings::new(config_path).unwrap();
    let index_dir_path = String::from(setting.get_coredb_settings().get_index_dir_path());
    let coredb = CoreDB::new(config_path).unwrap();

    InfinoEngine {
      index_dir_path,
      coredb,
    }
  }

  /// Indexes input data and returns the time required for insertion as microseconds.
  pub async fn index_lines(&mut self, input_data_path: &str, max_docs: i32) -> u128 {
    let mut num_docs = 0;
    let now = Instant::now();
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
          self.coredb.append_log_message(
            Utc::now().timestamp_millis() as u64,
            &HashMap::new(),
            message.as_str(),
          );
        }
      }

      self.coredb.commit(false);
    }
    let elapsed = now.elapsed().as_micros();
    println!(
      "Infino time required for insertion: {} microseconds",
      elapsed
    );
    elapsed
  }

  /// Searches the given term and returns the time required in microseconds
  pub fn search_logs(&self, query: &str, range_start_time: u64, range_end_time: u64) -> u128 {
    let now = Instant::now();

    match self
      .coredb
      .search_logs(query, "", range_start_time, range_end_time)
    {
      Ok(result) => {
        let elapsed = now.elapsed().as_micros();
        println!(
          "Infino time required for searching logs {} is : {} microseconds. Num of results {}",
          query,
          elapsed,
          result.len()
        );
        elapsed
      }
      Err(search_logs_error) => {
        eprintln!("Error in search_logs: {:?}", search_logs_error);
        0
      }
    }
  }

  pub fn get_index_dir_path(&self) -> &str {
    self.index_dir_path.as_str()
  }

  /// Runs multiple queries and returns the sum of time needed to run them in microseconds.
  pub fn search_multiple_queries(&self, queries: &[&str]) -> u128 {
    queries
      .iter()
      .map(|query| self.search_logs(query, 0, u64::MAX))
      .sum()
  }
}

use std::collections::HashMap;
use std::time::Instant;

use chrono::Utc;
use tsldb::utils::config::Settings;
use tsldb::Tsldb;

use crate::utils::io;

pub struct InfinoEngine {
  index_dir_path: String,
  tsldb: Tsldb,
}

impl InfinoEngine {
  pub fn new(config_path: &str) -> InfinoEngine {
    let setting = Settings::new(config_path).unwrap();
    let index_dir_path = String::from(setting.get_tsldb_settings().get_index_dir_path());
    let tsldb = Tsldb::new(&config_path).unwrap();

    InfinoEngine {
      index_dir_path,
      tsldb,
    }
  }

  pub async fn index_lines(&mut self, input_data_path: &str, max_docs: i32) {
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
          self.tsldb.append_log_message(
            Utc::now().timestamp_millis() as u64,
            &HashMap::new(),
            message.as_str(),
          );
        }
      }

      self.tsldb.commit(false);
    }
    let elapsed = now.elapsed();
    println!("Infino time required for insertion: {:.2?}", elapsed);
  }

  pub fn search(&self, query: &str, range_start_time: u64, range_end_time: u64) -> usize {
    let num_words = query.split_whitespace().count();
    let now = Instant::now();
    let result = self.tsldb.search(query, range_start_time, range_end_time);
    let elapsed = now.elapsed();
    println!(
      "Infino time required for searching {} word query is : {:.2?}. Num of results {}",
      num_words,
      elapsed,
      result.len()
    );
    return result.len();
  }

  pub fn get_index_dir_path(&self) -> &str {
    self.index_dir_path.as_str()
  }

  pub fn search_multiple_queries(&self, queries: &[&str]) -> usize {
    queries
      .iter()
      .map(|query| self.search(query, 0, u64::MAX))
      .count()
  }
}

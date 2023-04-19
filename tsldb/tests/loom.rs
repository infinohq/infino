// Use loom to iterate through all permutations of thread spawning.
#[cfg(loom)]
mod loom_tests {
  use std::collections::HashMap;
  use std::fs::File;
  use std::io::Write;

  use loom::sync::Arc;
  use loom::thread;

  use tempdir::TempDir;
  use tsldb::utils::io::get_joined_path;
  use tsldb::Tsldb;

  /// Helper function to create a test configuration.
  fn create_test_config(config_dir_path: &str, index_dir_path: &str) {
    // Create a test config in the directory config_dir_path.
    let config_file_path = get_joined_path(config_dir_path, "default.toml");
    println!("config file path = {}", config_file_path);
    {
      let index_dir_path_line = format!("index_dir_path = \"{}\"\n", index_dir_path);
      let mut file = File::create(&config_file_path).unwrap();
      file.write_all(b"[tsldb]\n").unwrap();
      file.write_all(index_dir_path_line.as_bytes()).unwrap();
      file
        .write_all(b"num_log_messages_threshold = 20\n")
        .unwrap();
      file.write_all(b"num_data_points_threshold = 40\n").unwrap();
    }
  }

  #[test]
  fn test_concurrent_append() {
    let mut builder = loom::model::Builder::new();
    builder.max_branches = 10_000;

    builder.check(|| {
      // Create test configuration.
      let config_dir = TempDir::new("config_test").unwrap();
      let index_dir = TempDir::new("index_test").unwrap();
      let config_dir_path = config_dir.path().to_str().unwrap();
      let index_dir_path = index_dir.path().to_str().unwrap();
      create_test_config(config_dir_path, index_dir_path);

      // Create index.
      let index = Tsldb::new(config_dir_path).unwrap();
      let arc_index = Arc::new(index);

      // Start threads to append to the index.
      let num_threads = 3;
      let num_appends_per_thread = 50;
      let mut handles = Vec::new();
      for i in 0..num_threads {
        let arc_index_clone = arc_index.clone();
        let start = i * num_appends_per_thread;
        let handle = thread::spawn(move || {
          for j in 0..num_appends_per_thread {
            let time = start + j;
            arc_index_clone.append_log_message(time as u64, &HashMap::new(), "message");
            let mut labels: HashMap<String, String> = HashMap::new();
            labels.insert("label1".to_string(), "value1".to_string());
            arc_index_clone.append_data_point("some_name", &labels, time as u64, 1.0);
          }
        });
        handles.push(handle);
      }

      for handle in handles {
        handle.join().unwrap();
      }

      // Commit and refresh.
      arc_index.commit(true);
      let tsldb = Tsldb::refresh(config_dir_path);

      let expected_len = num_threads * num_appends_per_thread;

      // Check whether the number of log messages is as expected.
      let results = tsldb.search("message", 0, expected_len as u64);
      let received_logs_len = results.len();
      assert_eq!(expected_len, received_logs_len);

      // Check whether the number of data points is as expected.
      let results = tsldb.get_time_series("label1", "value1", 0, expected_len as u64);
      let received_data_points_len = results.len();
      assert_eq!(expected_len, received_data_points_len);
    })
  }
}

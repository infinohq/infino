use std::fs::{metadata, remove_file, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};

use serde_json::Value;

use crate::utils::error::CoreDBError;

const MAX_ENTRIES: usize = 1000;

/// File based write ahead log. Typically, a write ahead log is created for each segment.
#[derive(Debug)]
pub struct WriteAheadLog {
  /// Path to the write ahead log file.
  file_path: String,

  /// Temporary memory buffer to store write ahead log entries.
  buffer: Vec<Value>,

  /// Write ahead log file writer.
  writer: BufWriter<std::fs::File>,
}

impl WriteAheadLog {
  /// Create a new write ahead log.
  pub fn new(path: &str) -> Result<Self, CoreDBError> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let writer = BufWriter::new(file);
    let buffer = Vec::new();

    Ok(Self {
      file_path: path.to_owned(),
      buffer,
      writer,
    })
  }

  /// Read all entries from the write ahead log.
  pub fn read_all(&self) -> Result<Vec<Value>, CoreDBError> {
    // Create buffered reader to read the file line by line.
    let mut json_values = Vec::new();
    let file = File::open(&self.file_path)?;
    let reader = BufReader::new(file);

    // Read each line and parse it as JSON. Ignore lines that cannot be read or parsed as JSON.
    // This could happen in case of a crash/partial data received. We ignore it to get to last known good state.
    for line in reader.lines() {
      let line = match line {
        Ok(line) => line,
        Err(_) => continue, // Ignore lines that cannot be read.
      };

      let json_value = match serde_json::from_str(&line) {
        Ok(value) => value,
        Err(_) => continue, // Ignore lines that cannot be parsed as JSON.
      };

      json_values.push(json_value);
    }

    Ok(json_values)
  }

  /// Append an entry to the write ahead log. After appending MAX_ENTRIES entries, all the entries
  /// will be flushed to disk.
  pub fn append(&mut self, entry: Value) -> Result<(), CoreDBError> {
    self.buffer.push(entry);

    if self.buffer.len() >= MAX_ENTRIES {
      self.flush()?;
    }
    Ok(())
  }

  /// Flush write ahead log to disk.
  pub fn flush(&mut self) -> Result<(), CoreDBError> {
    if !self.buffer.is_empty() {
      let combined_entries = self
        .buffer
        .iter()
        .map(|entry| serde_json::to_string(entry).unwrap_or_default() + "\n")
        .collect::<String>();

      self.writer.write_all(combined_entries.as_bytes())?;
      self.writer.flush()?;
      self.buffer.clear();
    }

    Ok(())
  }

  /// Delete the write ahead log - typically called when after a segment is committed.
  pub fn remove(&mut self) -> Result<(), CoreDBError> {
    // Delete the file - if it exists.
    if metadata(&self.file_path).is_ok() {
      remove_file(&self.file_path)?
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::fs;
  use std::path::Path;
  use std::sync::Arc;

  use serde_json::json;
  use tempfile::NamedTempFile;
  use tokio::task;

  use crate::utils::sync::TokioMutex;

  #[tokio::test]
  async fn test_wal_serial() {
    // Create a new temporary file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut wal = WriteAheadLog::new(path).unwrap();

    let entry = json!({"time": 1627590000, "message": "Test log entry"});

    // Add 2 entries. wal.flash() is not called yet, so the file should not contain 'Test log entry'.
    wal.append(entry.clone()).unwrap();
    wal.append(entry.clone()).unwrap();
    let contents = fs::read_to_string(Path::new(path)).unwrap();
    assert!(!contents.contains("Test log entry"));

    // Flush wal. File should now contain 'Test log entry' twice.
    wal.flush().unwrap();
    let contents = fs::read_to_string(Path::new(path)).unwrap();
    assert!(contents.contains("Test log entry"));
    assert_eq!(contents.matches('\n').count(), 2); // Each entry should be on a new line

    // Add more entries, upto a total of MAX_ENTRIES.
    for _ in 0..MAX_ENTRIES {
      wal.append(entry.clone()).unwrap();
    }

    // Flush should be called automatically as we reach MAX_ENTRIES limit.

    // Read back the log file. Should noe contain 'Test log entry' MAX_ENTRIES+2 times.
    let contents = fs::read_to_string(Path::new(path)).unwrap();
    assert!(contents.contains("Test log entry"));
    assert_eq!(contents.matches('\n').count(), MAX_ENTRIES + 2); // Each entry should be on a new line

    // Remove the file and check that it does not exist anymore.
    wal.remove().unwrap();
    assert!(std::fs::metadata(path).is_err())
  }

  #[tokio::test]
  async fn test_wal_parallel() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let wal = WriteAheadLog::new(path).unwrap();
    let wal = Arc::new(TokioMutex::new(wal));

    const NUM_APPEND_THREADS: usize = 20;
    const NUM_FLUSH_THREADS: usize = 10;
    const NUM_APPENDS: usize = 50000; // Number of append operations per thread.
    const NUM_FLUSHES: usize = 10; // Number of flush operations per thread.

    // Spawn tasks for concurrent appends.
    let mut append_handles = vec![];
    for _ in 0..NUM_APPEND_THREADS {
      let wal_clone = Arc::clone(&wal);
      let handle = task::spawn(async move {
        for _ in 0..NUM_APPENDS {
          let entry = json!({"time": 1627590000, "message": "Concurrent append"});
          let wal_clone = &mut wal_clone.lock().await;
          wal_clone.append(entry).unwrap();
        }
      });
      append_handles.push(handle);
    }

    // Spawn tasks for concurrent flushes.
    let mut flush_handles = vec![];
    for _ in 0..NUM_FLUSH_THREADS {
      let wal_clone = wal.clone();
      let handle = task::spawn(async move {
        for _ in 0..NUM_FLUSHES {
          let wal_clone = &mut wal_clone.lock().await;
          wal_clone.flush().unwrap();
        }
      });
      flush_handles.push(handle);
    }

    // Await all append and flush tasks to ensure completion.
    for handle in append_handles {
      handle.await.unwrap();
    }
    for handle in flush_handles {
      handle.await.unwrap();
    }

    // Final flush to ensure all entries are written.
    let wal_clone = wal.clone();
    let wal_clone = &mut wal_clone.lock().await;
    wal_clone.flush().unwrap();

    // Verify that entries were written correctly.
    let contents = fs::read_to_string(temp_file.path()).unwrap();
    let num_lines = contents.matches('\n').count();
    assert!(num_lines == NUM_APPENDS * NUM_APPEND_THREADS);
  }
}

use crate::utils::sync::{Arc, TokioMutex};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

use crate::utils::error::CoreDBError;

const MAX_ENTRIES: usize = 10000;

#[derive(Debug)]
pub struct WriteAheadLog {
  buffer: Arc<TokioMutex<Vec<Value>>>,
  writer: Arc<TokioMutex<BufWriter<std::fs::File>>>,
}

impl WriteAheadLog {
  pub fn new(path: &str) -> Result<Self, CoreDBError> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let writer = BufWriter::new(file);

    Ok(Self {
      buffer: Arc::new(TokioMutex::new(Vec::new())),
      writer: Arc::new(TokioMutex::new(writer)),
    })
  }

  pub async fn append(&self, entry: Value) -> Result<(), CoreDBError> {
    let num_entries;

    {
      // Write in a new scope to release buffer lock immediately.
      let mut buffer = self.buffer.lock().await;
      buffer.push(entry);
      num_entries = buffer.len();
    }

    if num_entries >= MAX_ENTRIES {
      self.flush().await?;
    }
    Ok(())
  }

  async fn flush(&self) -> Result<(), CoreDBError> {
    let buffer = &mut *self.buffer.lock().await;
    let writer = &mut *self.writer.lock().await;

    if !buffer.is_empty() {
      let combined_entries = buffer
        .iter()
        .map(|entry| serde_json::to_string(entry).unwrap_or_default() + "\n")
        .collect::<String>();

      writer.write_all(combined_entries.as_bytes())?;
      writer.flush()?;
      buffer.clear();
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;
  use std::fs;
  use std::path::Path;
  use tempfile::NamedTempFile;
  use tokio::task;

  #[tokio::test]
  async fn test_write_and_flush() {
    // Create a new temporary file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let wal = WriteAheadLog::new(path).unwrap();

    let entry = json!({"time": 1627590000, "message": "Test log entry"});

    // Add 2 entries. wal.flash() is not called yet, so the file should not contain 'Test log entry'.
    wal.append(entry.clone()).await.unwrap();
    wal.append(entry.clone()).await.unwrap();
    let contents = fs::read_to_string(Path::new(path)).unwrap();
    assert!(!contents.contains("Test log entry"));

    // Flush wal. File should now contain 'Test log entry' twice.
    wal.flush().await.unwrap();
    let contents = fs::read_to_string(Path::new(path)).unwrap();
    assert!(contents.contains("Test log entry"));
    assert_eq!(contents.matches('\n').count(), 2); // Each entry should be on a new line

    // Add more entries, upto a total of MAX_ENTRIES.
    for _ in 0..MAX_ENTRIES {
      wal.append(entry.clone()).await.unwrap();
    }

    // Flush should be called automatically as we reach MAX_ENTRIES limit.

    // Read back the log file. Should noe contain 'Test log entry' MAX_ENTRIES+2 times.
    let contents = fs::read_to_string(Path::new(path)).unwrap();
    assert!(contents.contains("Test log entry"));
    assert_eq!(contents.matches('\n').count(), MAX_ENTRIES + 2); // Each entry should be on a new line

    fs::remove_file(path).unwrap(); // Clean up after the test
  }

  #[tokio::test]
  async fn test_append_and_flush_parallel() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let wal = Arc::new(WriteAheadLog::new(path).unwrap());

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
          wal_clone.append(entry).await.unwrap();
        }
      });
      append_handles.push(handle);
    }

    // Spawn tasks for concurrent flushes.
    let mut flush_handles = vec![];
    for _ in 0..NUM_FLUSH_THREADS {
      let wal_clone = Arc::clone(&wal);
      let handle = task::spawn(async move {
        for _ in 0..NUM_FLUSHES {
          wal_clone.flush().await.unwrap();
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
    wal.flush().await.unwrap();

    // Verify that entries were written correctly.
    let contents = fs::read_to_string(temp_file.path()).unwrap();
    let num_lines = contents.matches('\n').count();
    assert!(num_lines == NUM_APPENDS * NUM_APPEND_THREADS);
  }
}

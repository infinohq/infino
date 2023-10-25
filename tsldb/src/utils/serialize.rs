use std::fs::File;
use std::io::Write;

use serde::de::DeserializeOwned;
use serde::Serialize;

// Level for zstd compression. Higher level means higher compression ratio, at the expense of speed of compression and decompression.
const COMPRESSION_LEVEL: i32 = 15;

/// Compress and write the specified map to the given file.
pub fn write<T: Serialize>(to_write: &T, file_path: &str, sync_after_write: bool) {
  let input = serde_json::to_string(&to_write).unwrap();
  let mut output = Vec::new();

  zstd::stream::copy_encode(input.as_bytes(), &mut output, COMPRESSION_LEVEL).unwrap();

  let mut file = File::options()
    .create(true)
    .write(true)
    .truncate(true)
    .open(file_path)
    .unwrap();

  // Clippy generates a warning is we don't handle the return amount from file.write(). So,
  // assign it to _ to suppress the warning.
  let _ = file.write(output.as_slice()).unwrap();

  if sync_after_write {
    // Forcibly sync the file contents without relying on the OS to do so. This is usually
    // set in tests that call commit() very aggressively, and generally can be avoided
    // in production for performance reasons.
    file.sync_all().unwrap();
  }
}

/// Read the map from the given file.
pub fn read<T: DeserializeOwned>(file_path: &str) -> T {
  let file = File::open(file_path).unwrap();
  let data = zstd::stream::decode_all(file).unwrap();
  let retval: T = serde_json::from_slice(&data).unwrap();
  retval
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::BTreeMap;
  use tempfile::NamedTempFile;

  #[test]
  fn test_serialize_btree_map() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let num_keys = 8;
    let prefix = "term#";

    let mut expected: BTreeMap<String, u32> = BTreeMap::new();
    for i in 1..=num_keys {
      expected.insert(format!("{prefix}{i}"), i);
    }

    write(&expected, file_path, false);

    let received: BTreeMap<String, u32> = read(file_path);

    for i in 1..=num_keys {
      assert!(received.get(&String::from(format!("{prefix}{i}"))).unwrap() == &i);
    }

    file.close().expect("Could not close temporary file");
  }

  #[test]
  fn test_serialize_vec() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let num_keys = 8;
    let prefix = "term#";

    let mut expected: Vec<String> = Vec::new();
    for i in 1..=num_keys {
      expected.push(format!("{prefix}{i}"));
    }

    write(&expected, file_path, false);

    let received: Vec<String> = read(file_path);

    for i in 1..=num_keys {
      assert!(received.contains(&format!("{prefix}{i}")));
    }

    file.close().expect("Could not close temporary file");
  }

  #[test]
  fn test_empty() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();

    let expected: BTreeMap<String, u32> = BTreeMap::new();
    write(&expected, file_path, false);

    let received: BTreeMap<String, u32> = read(file_path);
    assert!(received.len() == 0);

    file.close().expect("Could not close temporary file");
  }
}

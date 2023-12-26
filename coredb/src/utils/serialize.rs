use std::fs::File;
use std::io::Write;

use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;

// Level for zstd compression. Higher level means higher compression ratio, at the expense of speed of compression and decompression.
pub const COMPRESSION_LEVEL: i32 = 15;

/// Compress and write the specified map to the given file. Returns the number of bytes written after compression.
pub fn write<T: Serialize>(to_write: &T, file_path: &str, sync_after_write: bool) -> (u64, u64) {
  let input = serde_json::to_string(&to_write).unwrap();
  let input = input.as_bytes();
  let uncomepressed_length = input.len() as u64;

  let mut output = Vec::new();

  zstd::stream::copy_encode(
    input,
    &mut output,
    crate::utils::serialize::COMPRESSION_LEVEL,
  )
  .unwrap();

  let mut file = File::options()
    .create(true)
    .write(true)
    .truncate(true)
    .open(file_path)
    .unwrap();

  let compressed_length = file.write(output.as_slice()).unwrap() as u64;

  if sync_after_write {
    // Forcibly sync the file contents without relying on the OS to do so. This is usually
    // set in tests that call commit() very aggressively, and generally can be avoided
    // in production for performance reasons.
    file.sync_all().unwrap();
  }

  (uncomepressed_length, compressed_length)
}

/// Read the map from the given file. Returns the map and the number of bytes read after decompression.
pub fn read<T: DeserializeOwned>(file_path: &str) -> (T, u64) {
  let file = File::open(file_path).unwrap();
  let mmap =
    unsafe { Mmap::map(&file).unwrap_or_else(|_| panic!("Could not map file {}", file_path)) };
  let data = zstd::decode_all(&mmap[..]).unwrap();
  let retval: T = serde_json::from_slice(&data).unwrap();
  (retval, data.len() as u64)
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
    let (uncompressed, compressed) = write(&expected, file_path, false);
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (BTreeMap<String, u32>, _) = read(file_path);
    for i in 1..=num_keys {
      assert!(received.get(&String::from(format!("{prefix}{i}"))).unwrap() == &i);
    }
    // The number of bytes read is uncompressed - so it should be equal to the uncompressed number of bytes written.
    assert!(num_bytes_read == uncompressed);

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

    let (uncompressed, compressed) = write(&expected, file_path, false);
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (Vec<_>, _) = read(file_path);

    for i in 1..=num_keys {
      assert!(received.contains(&format!("{prefix}{i}")));
    }
    // The number of bytes read is uncompressed - so it should be greater than or equal to the number of bytes written uncompressed.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }

  #[test]
  fn test_empty() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();

    let expected: BTreeMap<String, u32> = BTreeMap::new();
    let (uncompressed, compressed) = write(&expected, file_path, false);
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (BTreeMap<String, u32>, _) = read(file_path);
    assert!(received.len() == 0);

    // The number of bytes read is uncompressed - so it should be greater than or equal to the number of bytes written uncompressed.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }
}

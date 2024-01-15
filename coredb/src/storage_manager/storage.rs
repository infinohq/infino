use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::de::DeserializeOwned;
use serde::Serialize;

// Level for zstd compression. Higher level means higher compression ratio, at the expense of speed of compression and decompression.
pub const COMPRESSION_LEVEL: i32 = 15;

#[derive(Debug)]
pub struct Storage {
  object_store: Arc<dyn ObjectStore>,
}

impl Storage {
  pub fn new() -> Self {
    let object_store = Arc::new(LocalFileSystem::new());
    Self { object_store }
  }

  /// Compress and write the specified map to the given file. Returns the number of bytes written after compression.
  pub async fn write<T: Serialize>(
    &self,
    to_write: &T,
    file_path: &str,
    sync_after_write: bool,
  ) -> (u64, u64) {
    let input = serde_json::to_string(&to_write).unwrap();
    let input = input.as_bytes();
    let uncomepressed_length = input.len() as u64;

    let mut output = Vec::new();

    zstd::stream::copy_encode(input, &mut output, COMPRESSION_LEVEL).unwrap();
    let output = Bytes::from(output);

    let compressed_length = output.len() as u64;
    let path = Path::from(file_path);
    self.object_store.put(&path, output).await.unwrap();

    (uncomepressed_length, compressed_length)
  }

  /// Read the map from the given file. Returns the map and the number of bytes read after decompression.
  pub async fn read<T: DeserializeOwned>(&self, file_path: &str) -> (T, u64) {
    let path = Path::from(file_path);
    let get_result = self
      .object_store
      .get(&path)
      .await
      .expect("Could not get file");
    let bytes = get_result.bytes().await.expect("Could not get bytes");
    let data = zstd::decode_all(&bytes[..]).unwrap();
    let retval: T = serde_json::from_slice(&data).unwrap();
    (retval, data.len() as u64)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::BTreeMap;
  use tempfile::NamedTempFile;

  #[tokio::test]
  async fn test_serialize_btree_map() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let num_keys = 8;
    let prefix = "term#";
    let storage = Storage::new();

    let mut expected: BTreeMap<String, u32> = BTreeMap::new();
    for i in 1..=num_keys {
      expected.insert(format!("{prefix}{i}"), i);
    }
    let (uncompressed, compressed) = storage.write(&expected, file_path, false).await;
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (BTreeMap<String, u32>, _) = storage.read(file_path).await;
    for i in 1..=num_keys {
      assert!(received.get(&String::from(format!("{prefix}{i}"))).unwrap() == &i);
    }
    // The number of bytes read is uncompressed - so it should be equal to the uncompressed number of bytes written.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }

  #[tokio::test]
  async fn test_serialize_vec() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let num_keys = 8;
    let prefix = "term#";
    let storage = Storage::new();

    let mut expected: Vec<String> = Vec::new();
    for i in 1..=num_keys {
      expected.push(format!("{prefix}{i}"));
    }

    let (uncompressed, compressed) = storage.write(&expected, file_path, false).await;
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (Vec<_>, _) = storage.read(file_path).await;

    for i in 1..=num_keys {
      assert!(received.contains(&format!("{prefix}{i}")));
    }
    // The number of bytes read is uncompressed - so it should be greater than or equal to the number of bytes written uncompressed.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }

  #[tokio::test]
  async fn test_empty() {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let storage = Storage::new();

    let expected: BTreeMap<String, u32> = BTreeMap::new();
    let (uncompressed, compressed) = storage.write(&expected, file_path, false).await;
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (BTreeMap<String, u32>, _) = storage.read(file_path).await;
    assert!(received.len() == 0);

    // The number of bytes read is uncompressed - so it should be greater than or equal to the number of bytes written uncompressed.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }
}

use std::sync::Arc;

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::utils::error::CoreDBError;

// Level for zstd compression. Higher level means higher compression ratio, at the expense of speed of compression and decompression.
pub const COMPRESSION_LEVEL: i32 = 15;

#[derive(Debug)]
pub enum StorageType {
  Local,
  AWS(String), // AWS storage with bucket name.
}

#[derive(Debug)]
pub struct Storage {
  storage_type: StorageType,
  object_store: Arc<dyn ObjectStore>,
}

impl Storage {
  pub fn new(storage_type: StorageType) -> Self {
    let object_store;
    match storage_type {
      StorageType::AWS(bucket_name) => {
        object_store = AmazonS3Builder::from_env()
          .with_bucket_name(&bucket_name)
          .build();
      }
      StorageType::Local => {
        object_store = Arc::new(LocalFileSystem::new());
      }
    }

    Self {
      storage_type,
      object_store,
    }
  }

  /// Compress and write the specified map to the given file. Returns the number of bytes written after compression.
  pub async fn write<T: Serialize>(
    &self,
    to_write: &T,
    file_path: &str,
    _sync_after_write: bool,
  ) -> Result<(u64, u64), CoreDBError> {
    let input = serde_json::to_string(&to_write).unwrap();
    let input = input.as_bytes();
    let uncomepressed_length = input.len() as u64;

    let mut output = Vec::new();

    zstd::stream::copy_encode(input, &mut output, COMPRESSION_LEVEL).unwrap();
    let output = Bytes::from(output);

    let compressed_length = output.len() as u64;
    let path = Path::from(file_path);
    self.object_store.put(&path, output).await?;

    Ok((uncomepressed_length, compressed_length))
  }

  /// Read the map from the given file. Returns the map and the number of bytes read after decompression.
  pub async fn read<T: DeserializeOwned>(&self, file_path: &str) -> Result<(T, u64), CoreDBError> {
    let path = Path::from(file_path);
    let get_result = self.object_store.get(&path).await?;
    let bytes = get_result.bytes().await?;
    let data = zstd::decode_all(&bytes[..])?;
    let retval: T = serde_json::from_slice(&data).unwrap();
    Ok((retval, data.len() as u64))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::BTreeMap;

  use tempfile::NamedTempFile;
  use test_case::test_case;

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::AWS("unit_test".to_owned()); "with AWS storage")]
  #[tokio::test]
  async fn test_serialize_btree_map(storage_type: StorageType) {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let num_keys = 8;
    let prefix = "term#";
    let storage = Storage::new(storage_type);

    let mut expected: BTreeMap<String, u32> = BTreeMap::new();
    for i in 1..=num_keys {
      expected.insert(format!("{prefix}{i}"), i);
    }
    let (uncompressed, compressed) = storage
      .write(&expected, file_path, false)
      .await
      .expect("Could not write to storage");
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (BTreeMap<String, u32>, _) = storage
      .read(file_path)
      .await
      .expect("Could not read from storage");
    for i in 1..=num_keys {
      assert!(received.get(&format!("{prefix}{i}")).unwrap() == &i);
    }
    // The number of bytes read is uncompressed - so it should be equal to the uncompressed number of bytes written.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }

  #[tokio::test]
  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::AWS("unit_test".to_owned()); "with AWS storage")]
  async fn test_serialize_vec(storage_type: StorageType) {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let num_keys = 8;
    let prefix = "term#";
    let storage = Storage::new(storage_type);

    let mut expected: Vec<String> = Vec::new();
    for i in 1..=num_keys {
      expected.push(format!("{prefix}{i}"));
    }

    let (uncompressed, compressed) = storage
      .write(&expected, file_path, false)
      .await
      .expect("Could not write to storage");
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (Vec<_>, _) = storage
      .read(file_path)
      .await
      .expect("Could not read from storage");

    for i in 1..=num_keys {
      assert!(received.contains(&format!("{prefix}{i}")));
    }
    // The number of bytes read is uncompressed - so it should be greater than or equal to the number of bytes written uncompressed.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }

  #[tokio::test]
  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::AWS("unit_test".to_owned()); "with AWS storage")]
  async fn test_empty(storage_type: StorageType) {
    let file = NamedTempFile::new().expect("Could not create temporary file");
    let file_path = file.path().to_str().unwrap();
    let storage = Storage::new(storage_type);

    let expected: BTreeMap<String, u32> = BTreeMap::new();
    let (uncompressed, compressed) = storage
      .write(&expected, file_path, false)
      .await
      .expect("Could not write to storage");
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let (received, num_bytes_read): (BTreeMap<String, u32>, _) = storage
      .read(file_path)
      .await
      .expect("Could not read from storage");
    assert!(received.is_empty());

    // The number of bytes read is uncompressed - so it should be greater than or equal to the number of bytes written uncompressed.
    assert!(num_bytes_read == uncompressed);

    file.close().expect("Could not close temporary file");
  }
}

use std::sync::Arc;

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage_manager::aws_s3_utils::AWSS3Utils;
use crate::utils::error::CoreDBError;

// Level for zstd compression. Higher level means higher compression ratio, at the expense of speed of compression and decompression.
pub const COMPRESSION_LEVEL: i32 = 15;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageType {
  Local,
  Aws(String), // AWS storage with bucket name.
}

#[derive(Debug)]
pub struct Storage {
  #[allow(dead_code)]
  storage_type: StorageType,
  object_store: Arc<dyn ObjectStore>,
}

impl Storage {
  pub async fn new(storage_type: &StorageType) -> Result<Self, CoreDBError> {
    let object_store: Arc<dyn ObjectStore> = match storage_type {
      // AWS storage. Create the bucket if it doesn't exist.
      StorageType::Aws(bucket_name) => {
        // Check if the bucket exists.
        let aws_s3_utils = AWSS3Utils::new().await?;
        let exists = aws_s3_utils.check_bucket_exists(bucket_name).await;
        if !exists {
          log::info!("Bucket {} doesn't exist. Creating it.", bucket_name);
          aws_s3_utils.create_bucket(bucket_name).await?;
        }

        let s3_store = AmazonS3Builder::from_env()
          .with_bucket_name(bucket_name.to_owned())
          .build()?;
        Arc::new(s3_store)
      }

      // Local storage.
      StorageType::Local => Arc::new(LocalFileSystem::new()),
    };

    Ok(Self {
      // We try to avoid clone() in the service code - but it is
      // - (a) necessary here as we need it to be part of Storage for further operations,
      // - (b) Storage::new() is usually one time per index - so isn't in the critical path of search or insertions.
      storage_type: storage_type.clone(),
      object_store,
    })
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

  #[cfg(test)]
  pub fn get_storage_type(&self) -> &StorageType {
    &self.storage_type
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::BTreeMap;
  use std::env;

  use crate::utils::environment::load_env;

  use tempfile::NamedTempFile;
  use test_case::test_case;

  fn get_temp_file_path(storage_type: &StorageType, test_name: &str) -> String {
    match storage_type {
      StorageType::Local => {
        let file = NamedTempFile::new().expect("Could not create temporary file");
        file.path().to_str().unwrap().to_owned()
      }
      StorageType::Aws(_) => {
        format!("storage-test/{}", test_name)
      }
    }
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws("dev-infino-unit-test".to_owned()); "with AWS storage")]
  #[tokio::test]
  async fn test_serialize_btree_map(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Do not run non-local storage in Github Actions, or if we don't have AWS credentials.
    if storage_type != StorageType::Local
      && (env::var("GITHUB_ACTIONS").is_ok() || env::var("AWS_ACCESS_KEY_ID").is_err())
    {
      return;
    }

    let file_path = &get_temp_file_path(&storage_type, "test-serialize-btree-map");
    let num_keys = 8;
    let prefix = "term#";
    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");
    assert_eq!(storage_type, storage.get_storage_type().clone());

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
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws("dev-infino-unit-test".to_owned()); "with AWS storage")]
  #[tokio::test]
  async fn test_serialize_vec(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Do not run non-local storage in Github Actions, or if we don't have AWS credentials.
    if storage_type != StorageType::Local
      && (env::var("GITHUB_ACTIONS").is_ok() || env::var("AWS_ACCESS_KEY_ID").is_err())
    {
      return;
    }

    let file_path = &get_temp_file_path(&storage_type, "test-serialize-vec");
    let num_keys = 8;
    let prefix = "term#";
    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");
    assert_eq!(storage_type, storage.get_storage_type().clone());

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
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws("dev-infino-unit-test".to_owned()); "with AWS storage")]
  #[tokio::test]
  async fn test_empty(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Do not run non-local storage in Github Actions, or if we don't have AWS credentials.
    if storage_type != StorageType::Local
      && (env::var("GITHUB_ACTIONS").is_ok() || env::var("AWS_ACCESS_KEY_ID").is_err())
    {
      return;
    }

    let file_path = &get_temp_file_path(&storage_type, "test-empty");
    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");
    assert_eq!(storage_type, storage.get_storage_type().clone());

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
  }
}

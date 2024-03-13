// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::storage_manager::aws_s3_utils::AWSS3Utils;
use crate::storage_manager::azure_storage_utils::AzureStorageUtils;
use crate::storage_manager::gcp_storage_utils::GCPStorageUtils;

use crate::utils::error::CoreDBError;

// Level for zstd compression. Higher level means higher compression ratio, at the expense of speed of compression and decompression.
use crate::storage_manager::constants::COMPRESSION_LEVEL;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudStorageConfig {
  pub bucket_name: String,
  pub region: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageType {
  Local,
  Aws(CloudStorageConfig),   // AWS storage with required cloud config.
  Gcp(CloudStorageConfig),   // GCP storage with required cloud config.
  Azure(CloudStorageConfig), // Azure storage with required cloud config.
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
      StorageType::Aws(cloud_storage_config) => {
        // Check if the bucket exists.
        let aws_s3_utils = AWSS3Utils::new(&cloud_storage_config.region).await?;
        let bucket_name = &cloud_storage_config.bucket_name;

        let exists = aws_s3_utils.check_bucket_exists(bucket_name).await;
        if !exists {
          log::info!("Bucket {} doesn't exist. Creating it.", bucket_name);
          aws_s3_utils.create_bucket(bucket_name).await?;
        }

        // Make sure to have AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in env vars.
        let s3_store = AmazonS3Builder::from_env()
          .with_bucket_name(bucket_name.to_owned())
          .build()?;
        Arc::new(s3_store)
      }

      // Local storage.
      StorageType::Local => Arc::new(LocalFileSystem::new()),

      // GCP Cloud Storage, creates a bucket if it does not exist.
      StorageType::Gcp(cloud_storage_config) => {
        let gcp_storage_utils = GCPStorageUtils::new(&cloud_storage_config.region).await?;

        let bucket_name = &cloud_storage_config.bucket_name;
        let exists = gcp_storage_utils.check_bucket_exists(bucket_name).await;
        if !exists {
          log::info!("Bucket {} doesn't exist. Creating it.", bucket_name);
          gcp_storage_utils.create_bucket(bucket_name).await?;
        }

        // Make sure to have path to json file with gcp credentials in SERVICE_ACCOUNT env vars.
        let gcs_store = GoogleCloudStorageBuilder::from_env()
          .with_bucket_name(bucket_name.to_owned())
          .build()?;

        Arc::new(gcs_store)
      }

      // Azure Cloud Storage, creates a container if it does not exist.
      StorageType::Azure(cloud_storage_config) => {
        let azure_storage_utils = AzureStorageUtils::new(&cloud_storage_config.region).await?;

        let bucket_name = &cloud_storage_config.bucket_name;
        let exists = azure_storage_utils
          .check_container_exists(bucket_name)
          .await;
        if !exists {
          log::info!("Bucket {} doesn't exist. Creating it.", bucket_name);
          azure_storage_utils.create_container(bucket_name).await?;
        }

        // Make sure to have AZURE_STORAGE_ACCOUNT_KEY and AZURE_STORAGE_ACCOUNT_NAME in env vars.
        let azure_store = MicrosoftAzureBuilder::from_env()
          .with_container_name(bucket_name.to_owned())
          .build()?;

        Arc::new(azure_store)
      }
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
  ) -> Result<(u64, u64), CoreDBError> {
    // Serialize the to_write input in memory.
    let mut serialized_data = Vec::new(); // Serialize directly to a Vec<u8>
    serde_json::to_writer(&mut serialized_data, &to_write)?;
    let uncompressed_length = serialized_data.len() as u64;

    // Compress the serialized input.
    let compressed_data = zstd::encode_all(&serialized_data[..], COMPRESSION_LEVEL)?;
    let compressed_length = compressed_data.len() as u64;

    // Write the compressed input to object store using multipart upload.
    let path = Path::from(file_path);
    let (_id, mut writer) = self.object_store.put_multipart(&path).await?;
    writer.write_all(&compressed_data[..]).await?;
    writer.flush().await?;
    writer.shutdown().await?;

    Ok((uncompressed_length, compressed_length))
  }

  /// Reads the given file and returns an object of type T after decompression.
  pub async fn read<T: DeserializeOwned>(&self, file_path: &str) -> Result<T, CoreDBError> {
    // Read the compressed data from the object store.
    let path = Path::from(file_path);
    let get_result = self.object_store.get(&path).await?;
    let compressed_data = get_result.bytes().await?;

    // Set up the decoder for streaming decompression.
    let mut decoder = zstd::stream::Decoder::new(&compressed_data[..])?;

    // Deserialize directly from the decompressed stream.
    let retval: T = serde_json::from_reader(&mut decoder)?;

    Ok(retval)
  }

  #[cfg(test)]
  pub fn get_storage_type(&self) -> &StorageType {
    &self.storage_type
  }

  /// Create a directory. Only relevant for StorageType::Local, no-op for the rest storage types.
  pub fn create_dir(&self, dir: &str) -> Result<(), CoreDBError> {
    // We only need to create the directory for local storage. For cloud storage such as
    // AWS, directories are just implied hierarchies by path separator '/'.
    if let StorageType::Local = self.storage_type {
      let dir_path = std::path::Path::new(dir);
      if !dir_path.is_dir() {
        // Directory does not exist. Create it.
        std::fs::create_dir_all(dir_path)?;
      }
    }

    Ok(())
  }

  /// Remove a directory. Deletes the directory for StorageType::Local, deletes all the files
  /// with the given prefix for the remaining storage types.
  pub async fn remove_dir(&self, dir: &str) -> Result<(), CoreDBError> {
    match self.storage_type {
      StorageType::Local => {
        let dir_path = std::path::Path::new(dir);
        std::fs::remove_dir_all(dir_path)?;
      }
      _ => {
        let path = Path::from(dir);

        // Get a list of all the files in the directory.
        let locations = self
          .object_store
          .list(Some(&path))
          .map_ok(|m| m.location)
          .boxed();

        // Delete those files.
        self
          .object_store
          .delete_stream(locations)
          .try_collect::<Vec<Path>>()
          .await?;
      }
    }

    Ok(())
  }

  /// Returns true if the specified path exists.
  pub async fn check_path_exists(&self, path_str: &str) -> bool {
    let path = Path::from(path_str);

    // Perform a head operation to check if the path exists.
    let result = self.object_store.head(&path).await;

    result.is_ok()
  }

  /// Get a list of all files in the specified directory.
  pub async fn read_dir(&self, path_str: &str) -> Result<Vec<String>, CoreDBError> {
    let path = Path::from(path_str);

    // Get a list of all the files in the directory.
    let list_result = self.object_store.list_with_delimiter(Some(&path)).await?;

    let mut retval = Vec::new();
    for prefix in list_result.common_prefixes {
      let dirname = prefix.filename().ok_or(CoreDBError::CannotReadDirectory(
        "Could not get filename from directory path".to_owned(),
      ))?;
      retval.push(dirname.to_string());
    }

    Ok(retval)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::BTreeMap;
  use std::env;

  use crate::utils::environment::load_env;

  use tempfile::tempdir;
  use tempfile::NamedTempFile;
  use test_case::test_case;

  fn get_temp_file_path(storage_type: &StorageType, test_name: &str) -> String {
    match storage_type {
      StorageType::Local => {
        let file = NamedTempFile::new().expect("Could not create temporary file");
        file.path().to_str().unwrap().to_owned()
      }
      // For all other cloud storages like AWS, GCP, etc
      _ => {
        format!("storage-test/{}", test_name)
      }
    }
  }

  fn get_temp_dir_path(storage_type: &StorageType, test_name: &str) -> String {
    match storage_type {
      StorageType::Local => {
        let dir = tempdir().expect("Could not create temporary directory");
        dir.path().to_str().unwrap().to_owned()
      }
      // For all other cloud storages like AWS, GCP, etc
      _ => {
        format!("storage-test/{}", test_name)
      }
    }
  }
  fn run_test(storage_type: &StorageType) -> bool {
    // Do not run non-local storage in Github Actions, or if we don't have AWS/GCP credentials.
    if storage_type != &StorageType::Local
      && (env::var("GITHUB_ACTIONS").is_ok()
        || env::var("AWS_ACCESS_KEY_ID").is_err()
        || env::var("GOOGLE_APPLICATION_CREDENTIALS").is_err()
        || env::var("AZURE_STORAGE_ACCOUNT_KEY").is_err())
    {
      return false;
    }

    true
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws(CloudStorageConfig {
    bucket_name: "dev-infino-unit-test".to_owned(),
    region: "us-east-1".to_owned(),
  }))]
  #[test_case(StorageType::Gcp(CloudStorageConfig {
    bucket_name: "dev-infino-unit-test".to_owned(),
    region: "US-EAST1".to_owned(),
  }))]
  #[test_case(StorageType::Azure(CloudStorageConfig {
    bucket_name: "dev-infino-unit-test".to_owned(),
    region: "US-EAST1".to_owned(),
  }))]
  #[tokio::test]
  async fn test_serialize_btree_map(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Check if this test should be run (typically we don't run cloud tests in Github Actions or if credentials are not set).
    if !run_test(&storage_type) {
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
      .write(&expected, file_path)
      .await
      .expect("Could not write to storage");
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let received: BTreeMap<String, u32> = storage
      .read(file_path)
      .await
      .expect("Could not read from storage");
    for i in 1..=num_keys {
      assert!(received.get(&format!("{prefix}{i}")).unwrap() == &i);
    }
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws(CloudStorageConfig {
    bucket_name: "dev-infino-unit-test".to_owned(),
    region: "us-east-1".to_owned(),
  }))]
  #[test_case(StorageType::Gcp(CloudStorageConfig {
    bucket_name: "dev-infino-unit-test".to_owned(),
    region: "US-EAST1".to_owned(),
  }))]
  #[test_case(StorageType::Azure(CloudStorageConfig {
    bucket_name: "dev-infino-unit-test".to_owned(),
    region: "US-EAST1".to_owned(),
  }))]
  #[tokio::test]
  async fn test_serialize_vec(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Check if this test should be run (typically we don't run cloud tests in Github Actions or if credentials are not set).
    if !run_test(&storage_type) {
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
      .write(&expected, file_path)
      .await
      .expect("Could not write to storage");
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let received: Vec<_> = storage
      .read(file_path)
      .await
      .expect("Could not read from storage");

    for i in 1..=num_keys {
      assert!(received.contains(&format!("{prefix}{i}")));
    }
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "us-east-1".to_owned(),
  }))]
  #[test_case(StorageType::Gcp(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "US-EAST1".to_owned(),
  }))]
  #[test_case(StorageType::Azure(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "US-EAST1".to_owned(),
  }))]
  #[tokio::test]
  async fn test_empty(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Check if this test should be run (typically we don't run cloud tests in Github Actions or if credentials are not set).
    if !run_test(&storage_type) {
      return;
    }

    let file_path = &get_temp_file_path(&storage_type, "test-empty");
    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");
    assert_eq!(storage_type, storage.get_storage_type().clone());

    let expected: BTreeMap<String, u32> = BTreeMap::new();
    let (uncompressed, compressed) = storage
      .write(&expected, file_path)
      .await
      .expect("Could not write to storage");
    assert!(uncompressed > 0);
    assert!(compressed > 0);

    let received: BTreeMap<String, u32> = storage
      .read(file_path)
      .await
      .expect("Could not read from storage");
    assert!(received.is_empty());
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "us-east-1".to_owned(),
  }))]
  #[test_case(StorageType::Gcp(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "US-EAST1".to_owned(),
  }))]
  #[test_case(StorageType::Azure(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "US-EAST1".to_owned(),
  }))]
  #[tokio::test]
  async fn test_create_dir(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Check if this test should be run (typically we don't run cloud tests in Github Actions or if credentials are not set).
    if !run_test(&storage_type) {
      return;
    }

    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");
    match storage_type {
      StorageType::Local => {
        let temp_dir = std::env::temp_dir();
        let test_dir = temp_dir.as_path().join("test-create-dir");
        let test_dir = test_dir.to_str().expect("Could not create path");
        assert!(storage.create_dir(test_dir).is_ok());
        assert!(std::path::Path::new(test_dir).is_dir());
      }
      _ => assert!(storage.create_dir("some-dir").is_ok()),
    };
  }

  #[test_case(StorageType::Local; "with local storage")]
  #[test_case(StorageType::Aws(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "us-east-1".to_owned(),
  }))]
  #[test_case(StorageType::Gcp(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "US-EAST1".to_owned(),
  }))]
  #[test_case(StorageType::Azure(CloudStorageConfig {
      bucket_name: "dev-infino-unit-test".to_owned(),
      region: "US-EAST1".to_owned(),
  }))]
  #[tokio::test]
  async fn test_prefix(storage_type: StorageType) {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Check if this test should be run (typically we don't run cloud tests in Github Actions or if credentials are not set).
    if !run_test(&storage_type) {
      return;
    }

    // Check for non-existing prefix.
    let storage = Storage::new(&storage_type)
      .await
      .expect("Could not create storage");
    assert!(
      !storage
        .check_path_exists("this-prefix-does-not-exist")
        .await
    );

    // Write some data, and check that the prefix with that data exists.
    let file_path = &get_temp_file_path(&storage_type, "test-prefix");
    storage
      .write(&Vec::<String>::new(), file_path)
      .await
      .expect("Could not write to storage");
    assert!(storage.check_path_exists(file_path).await);

    // Write a couple more files in directories.
    let dir_path = &get_temp_dir_path(&storage_type, "test-dir-prefix");
    if storage_type == StorageType::Local {
      // Need to create directories only for local storage.
      storage
        .create_dir(&format!("{}/0", dir_path))
        .expect("Could not create dir");
      storage
        .create_dir(&format!("{}/1", dir_path))
        .expect("Could not create dir");
    }
    storage
      .write(&Vec::<String>::new(), &format!("{}/0/test", dir_path))
      .await
      .expect("Could not write to storage");
    storage
      .write(&Vec::<String>::new(), &format!("{}/1/test", dir_path))
      .await
      .expect("Could not write to storage");
    storage
      .write(&Vec::<String>::new(), &format!("{}/1/test-again", dir_path))
      .await
      .expect("Could not write to storage");

    // Check that the directories are as expected.
    let directories = storage
      .read_dir(dir_path)
      .await
      .expect("Could not read dir");
    assert_eq!(directories.len(), 2);
    assert!(directories.contains(&"0".to_owned()));
    assert!(directories.contains(&"1".to_owned()));
  }
}

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::env;
use std::path::Path;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

use super::error::CoreDBError;
use crate::storage_manager::{
  constants::{
    DEFAULT_CLOUD_REGION_FOR_AWS_S3, DEFAULT_CLOUD_REGION_FOR_AZURE, DEFAULT_CLOUD_REGION_FOR_GCP,
  },
  storage::{CloudStorageConfig, StorageType},
};

/// Helper function to initialize a logger for tests.
pub fn config_test_logger() {
  let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
  let filter_level = if log_level.to_lowercase() == "debug" {
    log::LevelFilter::Debug
  } else {
    log::LevelFilter::Info
  };

  let _ = env_logger::builder()
    .is_test(true)
    .filter_level(filter_level)
    .try_init();
}

const DEFAULT_CONFIG_FILE_NAME: &str = "default.toml";

#[derive(Debug, Deserialize)]
/// Settings for coredb.
pub struct CoreDBSettings {
  index_dir_path: String,
  default_index_name: String,

  log_messages_threshold: u32,
  metric_points_threshold: u32,
  search_memory_budget_megabytes: f32,
  uncommitted_segments_threshold: u32,
  retention_days: u32,
  storage_type: String,
  cloud_storage_bucket_name: Option<String>,
  cloud_storage_region: Option<String>,
  target_segment_size_megabytes: f32,
}

impl CoreDBSettings {
  /// Get the settings for the directory where the index is stored.
  pub fn get_index_dir_path(&self) -> String {
    let path = Path::new(&self.index_dir_path);

    if path.is_absolute() {
      // If the path is already absolute, return it as is.
      self.index_dir_path.clone()
    } else {
      // If the path is relative, concatenate it with the current directory.
      let current_dir = env::current_dir().expect("Could not get current directory");
      let joined_path = current_dir.join(path);
      joined_path
        .as_path()
        .to_str()
        .expect("Could not convert path to string")
        .to_owned()
    }
  }

  /// Get the settings for the default index name.
  pub fn get_default_index_name(&self) -> &str {
    self.default_index_name.as_str()
  }

  pub fn get_default_config_file_name() -> &'static str {
    DEFAULT_CONFIG_FILE_NAME
  }

  pub fn get_log_messages_threshold(&self) -> u32 {
    self.log_messages_threshold
  }

  pub fn get_metric_points_threshold(&self) -> u32 {
    self.metric_points_threshold
  }

  pub fn get_uncommitted_segments_threshold(&self) -> u32 {
    self.uncommitted_segments_threshold
  }

  pub fn get_search_memory_budget_bytes(&self) -> u64 {
    (self.search_memory_budget_megabytes * 1024.0 * 1024.0) as u64
  }

  pub fn get_retention_days(&self) -> u32 {
    self.retention_days
  }

  pub fn get_storage_type(&self) -> Result<StorageType, CoreDBError> {
    match self.storage_type.as_str() {
      "local" => Ok(StorageType::Local),
      "aws" => {
        let aws_bucket_name =
          self
            .cloud_storage_bucket_name
            .to_owned()
            .ok_or(CoreDBError::InvalidConfiguration(
              "AWS bucket name (as cloud_storage_bucket_name) not provided".to_owned(),
            ))?;

        let aws_region = self
          .cloud_storage_region
          .to_owned()
          .unwrap_or_else(|| DEFAULT_CLOUD_REGION_FOR_AWS_S3.to_owned());

        Ok(StorageType::Aws(CloudStorageConfig {
          bucket_name: aws_bucket_name,
          region: aws_region,
        }))
      }
      "gcp" => {
        let gcp_bucket_name =
          self
            .cloud_storage_bucket_name
            .to_owned()
            .ok_or(CoreDBError::InvalidConfiguration(
              "GCP bucket name (as cloud_storage_bucket_name) not provided".to_owned(),
            ))?;

        let gcp_region = self
          .cloud_storage_region
          .to_owned()
          .unwrap_or_else(|| DEFAULT_CLOUD_REGION_FOR_GCP.to_owned());

        Ok(StorageType::Gcp(CloudStorageConfig {
          bucket_name: gcp_bucket_name,
          region: gcp_region,
        }))
      }
      "azure" => {
        let azure_container_name =
          self
            .cloud_storage_bucket_name
            .to_owned()
            .ok_or(CoreDBError::InvalidConfiguration(
              "Azure container name (as cloud_storage_bucket_name) not provided".to_owned(),
            ))?;

        let azure_region = self
          .cloud_storage_region
          .to_owned()
          .unwrap_or_else(|| DEFAULT_CLOUD_REGION_FOR_AZURE.to_owned());

        Ok(StorageType::Azure(CloudStorageConfig {
          bucket_name: azure_container_name,
          region: azure_region,
        }))
      }
      _ => {
        let message = format!("Unknown storage type: {}", self.storage_type);
        Err(CoreDBError::InvalidConfiguration(message))
      }
    }
  }

  pub fn get_target_segment_size_bytes(&self) -> u64 {
    (self.target_segment_size_megabytes * 1024.0 * 1024.0) as u64
  }
}

#[derive(Debug, Deserialize)]
/// Settings for coredb, read from config file.
pub struct Settings {
  coredb: CoreDBSettings,
}

impl Settings {
  /// Create Settings from given configuration directory path.
  pub fn new(config_dir_path: &str) -> Result<Self, ConfigError> {
    let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
    let config_default_file_name = format!("{}/{}", config_dir_path, DEFAULT_CONFIG_FILE_NAME);
    let config_environment_file_name = format!("{}/{}.toml", config_dir_path, run_mode);

    let config = Config::builder()
      // Start off by merging in the "default" configuration file
      .add_source(File::with_name(&config_default_file_name))
      // Add in the current environment file
      // Default to 'development' env
      // Note that this file is _optional_
      .add_source(File::with_name(&config_environment_file_name).required(false))
      // Add in settings from the environment (with a prefix of INFINO)
      // Eg.. `INFINO_DEBUG=1` would set the `debug` key
      .add_source(Environment::with_prefix("INFINO"))
      .build()?;

    // Deserialize (and thus freeze) the entire configuration.
    let settings: Settings = config.try_deserialize()?;

    if settings.coredb.storage_type != "local"
      && settings.coredb.cloud_storage_bucket_name.is_none()
    {
      return Err(ConfigError::Message(
        "Cloud Storage bucket name is not set".to_owned(),
      ));
    }

    Ok(settings)
  }

  /// Get coredb settings.
  pub fn get_coredb_settings(&self) -> &CoreDBSettings {
    &self.coredb
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::fs::File;
  use std::io::Write;

  use tempdir::TempDir;

  use crate::utils::io::get_joined_path;

  #[test]
  fn test_new_settings() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();

    // Reading from an empty directory should be an error.
    assert!(Settings::new(config_dir_path).is_err());

    // Check default settings.
    let config_file_path = get_joined_path(config_dir_path, DEFAULT_CONFIG_FILE_NAME);
    {
      let mut file = File::create(config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file
        .write_all(b"index_dir_path = \"/var/index\"\n")
        .unwrap();
      file
        .write_all(b"default_index_name = \".default\"\n")
        .unwrap();
      file.write_all(b"log_messages_threshold = 1000\n").unwrap();
      file
        .write_all(b"metric_points_threshold = 10000\n")
        .unwrap();
      file
        .write_all(b"uncommitted_segments_threshold = 10\n")
        .unwrap();
      file
        .write_all(b"search_memory_budget_megabytes = 4096\n")
        .unwrap();
      file.write_all(b"retention_days = 30\n").unwrap();
      file.write_all(b"storage_type = \"local\"\n").unwrap();
      file
        .write_all(b"target_segment_size_megabytes = 1024\n")
        .unwrap();
    }

    let settings = Settings::new(config_dir_path).unwrap();
    let coredb_settings = settings.get_coredb_settings();
    assert_eq!(coredb_settings.get_index_dir_path(), "/var/index");
    assert_eq!(coredb_settings.get_default_index_name(), ".default");
    assert_eq!(
      coredb_settings.get_search_memory_budget_bytes(),
      4096 * 1024 * 1024
    );

    assert_eq!(coredb_settings.get_retention_days(), 30);
    assert_eq!(
      coredb_settings.get_target_segment_size_bytes(),
      1024 * 1024 * 1024
    );

    assert_eq!(
      coredb_settings.get_storage_type().unwrap(),
      StorageType::Local
    );

    // Check if we are running this test as part of a GitHub actions. We can't change environment variables
    // in GitHub actions, so don't run rest of the test as part of GitHub actions.
    let github_actions = env::var("GITHUB_ACTIONS").is_ok();
    if !github_actions {
      // Check settings override using RUN_MODE environment variable.
      env::set_var("RUN_MODE", "SETTINGSTEST");
      let config_file_path = get_joined_path(config_dir_path, "settingstest.toml");
      {
        let mut file = File::create(config_file_path).unwrap();
        file.write_all(b"[coredb]\n").unwrap();
        file
          .write_all(b"search_memory_budget_megabytes=4\n")
          .unwrap();
      }
      let settings = Settings::new(config_dir_path).unwrap();
      let coredb_settings = settings.get_coredb_settings();
      assert_eq!(coredb_settings.get_index_dir_path(), "/var/index");
      assert_eq!(coredb_settings.get_log_messages_threshold(), 1000);
      assert_eq!(coredb_settings.get_metric_points_threshold(), 10000);
      assert_eq!(coredb_settings.get_uncommitted_segments_threshold(), 10);
      assert_eq!(
        coredb_settings.get_search_memory_budget_bytes(),
        4 * 1024 * 1024
      );
      assert_eq!(coredb_settings.get_default_index_name(), ".default");
    }
  }

  #[test]
  fn test_settings_error() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();

    // Create a config where a few keys such as 'log_messages_threshold' are missing.
    let config_file_path = get_joined_path(config_dir_path, DEFAULT_CONFIG_FILE_NAME);
    {
      let mut file = File::create(config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file
        .write_all(b"index_dir_path = \"/var/index\"\n")
        .unwrap();
      file
        .write_all(b"default_index_name = \".default\"\n")
        .unwrap();
      file
        .write_all(b"search_memory_budget_megabytes = 2048\n")
        .unwrap();
      file.write_all(b"retention_days = 30\n").unwrap();
      file.write_all(b"storage_type = \"local\"\n").unwrap();
      file
        .write_all(b"target_segment_size_megabytes = 1024\n")
        .unwrap();
    }

    // Make sure this config returns an error.
    let result = Settings::new(config_dir_path);
    assert!(result.is_err());
  }
}

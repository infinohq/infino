// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::env;
use std::path::Path;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

const DEFAULT_CONFIG_FILE_NAME: &str = "default.toml";

#[derive(Debug, Deserialize)]
/// Settings for coredb.
pub struct CoreDBSettings {
  index_dir_path: String,
  default_index_name: String,
  segment_size_threshold_megabytes: f32,

  // This has to be greater than 4*segment_size_threshold_megabytes. Otherwise,
  // a ConfigError is raised while parsing.
  memory_budget_megabytes: f32,
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

  pub fn get_segment_size_threshold_bytes(&self) -> u64 {
    (self.segment_size_threshold_megabytes * 1024.0 * 1024.) as u64
  }

  pub fn get_memory_budget_bytes(&self) -> u64 {
    (self.memory_budget_megabytes * 1024.0 * 1024.0) as u64
  }

  pub fn get_search_memory_budget_bytes(&self) -> u64 {
    // Search memory budget is total memory budget, minus the size of one segment (typically for insertions).
    ((self.memory_budget_megabytes - self.segment_size_threshold_megabytes) * 1024.0 * 1024.0)
      as u64
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

    // Run validation checks.
    if settings.coredb.memory_budget_megabytes
      < 4.0 * settings.coredb.segment_size_threshold_megabytes
    {
      return Err(ConfigError::Message(
        "Memory budget should be at least 4 times the segment size threshold".to_owned(),
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
      file
        .write_all(b"segment_size_threshold_megabytes = 1024\n")
        .unwrap();
      file.write_all(b"memory_budget_megabytes = 4096\n").unwrap();
    }

    let settings = Settings::new(config_dir_path).unwrap();
    let coredb_settings = settings.get_coredb_settings();
    assert_eq!(coredb_settings.get_index_dir_path(), "/var/index");
    assert_eq!(coredb_settings.get_default_index_name(), ".default");
    assert_eq!(
      coredb_settings.get_segment_size_threshold_bytes(),
      1024 * 1024 * 1024
    );
    assert_eq!(
      coredb_settings.get_memory_budget_bytes(),
      4096 * 1024 * 1024
    );
    assert_eq!(
      coredb_settings.get_search_memory_budget_bytes(),
      (4096 - 1024) * 1024 * 1024
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
          .write_all(b"segment_size_threshold_megabytes=1\n")
          .unwrap();
        file.write_all(b"memory_budget_megabytes=4\n").unwrap();
      }
      let settings = Settings::new(config_dir_path).unwrap();
      let coredb_settings = settings.get_coredb_settings();
      assert_eq!(coredb_settings.get_index_dir_path(), "/var/index");
      assert_eq!(
        coredb_settings.get_segment_size_threshold_bytes(),
        1024 * 1024
      );
      assert_eq!(coredb_settings.get_memory_budget_bytes(), 4 * 1024 * 1024);
      assert_eq!(
        coredb_settings.get_search_memory_budget_bytes(),
        3 * 1024 * 1024
      );
      assert_eq!(coredb_settings.get_default_index_name(), ".default");
    }
  }

  #[test]
  fn test_settings_error() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();

    // Create a config where the memory budget is too low.
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
        .write_all(b"segment_size_threshold_megabytes = 1024\n")
        .unwrap();
      file.write_all(b"memory_budget_megabytes = 2048\n").unwrap();
    }

    // Make sure this config returns an error.
    let result = Settings::new(config_dir_path);
    assert!(result.is_err());
  }
}

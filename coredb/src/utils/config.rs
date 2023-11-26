use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

const DEFAULT_CONFIG_FILE_NAME: &str = "default.toml";

#[derive(Debug, Deserialize)]
/// Settings for coredb.
pub struct CoreDBSettings {
  index_dir_path: String,
  default_index_name: String,
  num_log_messages_threshold: u32,
  num_metric_points_threshold: u32,
}

impl CoreDBSettings {
  /// Get the settings for the directory where the index is stored.
  pub fn get_index_dir_path(&self) -> &str {
    self.index_dir_path.as_str()
  }

  /// Get the settings for the default index name.
  pub fn get_default_index_name(&self) -> &str {
    self.default_index_name.as_str()
  }

  /// Get the setting for the threshold number of log messages per segment.
  /// That is, if a commit is called on a segment and it has more than the specified number of log messages,
  /// a new segment will be created.
  pub fn get_num_log_messages_threshold(&self) -> u32 {
    self.num_log_messages_threshold
  }

  /// Get the setting for the threshold number of metric points per segment.
  /// That is, if a commit is called on a segment and it has more than the specified number of metric points,
  /// a new segment will be created.
  pub fn get_num_metric_points_threshold(&self) -> u32 {
    self.num_metric_points_threshold
  }

  pub fn get_default_config_file_name() -> &'static str {
    DEFAULT_CONFIG_FILE_NAME
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
      // Add in settings from the environment (with a prefix of COREDB)
      // Eg.. `COREDB_DEBUG=1` would set the `debug` key
      .add_source(Environment::with_prefix("coredb"))
      .build()?;

    // You can deserialize (and thus freeze) the entire configuration as
    config.try_deserialize()
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
  fn test_settings() {
    let config_dir = TempDir::new("config_test").unwrap();
    let config_dir_path = config_dir.path().to_str().unwrap();

    // Reading from an empty directory should be an error.
    assert!(Settings::new(config_dir_path).is_err());

    // Check default settings.
    let config_file_path = get_joined_path(config_dir_path, DEFAULT_CONFIG_FILE_NAME);
    {
      let mut file = File::create(&config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file
        .write_all(b"index_dir_path = \"/var/index\"\n")
        .unwrap();
      file
        .write_all(b"default_index_name = \".default\"\n")
        .unwrap();
      file
        .write_all(b"num_log_messages_threshold = 1000\n")
        .unwrap();
      file
        .write_all(b"num_metric_points_threshold = 10000\n")
        .unwrap();
    }

    let settings = Settings::new(&config_dir_path).unwrap();
    let coredb_settings = settings.get_coredb_settings();
    assert_eq!(coredb_settings.get_index_dir_path(), "/var/index");
    assert_eq!(coredb_settings.get_num_log_messages_threshold(), 1000);
    assert_eq!(coredb_settings.get_num_metric_points_threshold(), 10000);

    // Check settings override using RUN_MODE environment variable.
    env::set_var("RUN_MODE", "SETTINGSTEST");
    let config_file_path = get_joined_path(config_dir_path, "settingstest.toml");
    {
      let mut file = File::create(&config_file_path).unwrap();
      file.write_all(b"[coredb]\n").unwrap();
      file.write_all(b"num_log_messages_threshold=1\n").unwrap();
    }
    let settings = Settings::new(&config_dir_path).unwrap();
    let coredb_settings = settings.get_coredb_settings();
    assert_eq!(coredb_settings.get_index_dir_path(), "/var/index");
    assert_eq!(coredb_settings.get_num_log_messages_threshold(), 1);
    assert_eq!(coredb_settings.get_num_metric_points_threshold(), 10000);
  }
}
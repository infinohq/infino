// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

const DEFAULT_CONFIG_FILE_NAME: &str = "default.toml";

#[derive(Debug, Deserialize)]
/// Settings for infino server.
pub struct ServerSettings {
  port: u16,
  host: String,
  timestamp_key: String,
  labels_key: String,
}

impl ServerSettings {
  /// Get the port.
  pub fn get_port(&self) -> u16 {
    self.port
  }

  /// Get the host.
  pub fn get_host(&self) -> &str {
    &self.host
  }

  /// Get the key for timestamp in json.
  pub fn get_timestamp_key(&self) -> &str {
    &self.timestamp_key
  }

  /// Get the labels for timestamp in json.
  pub fn get_labels_key(&self) -> &str {
    &self.labels_key
  }
}

#[derive(Debug, Deserialize)]
/// Settings for Core, read from config file.
pub struct Settings {
  server: ServerSettings,
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
      .add_source(Environment::with_prefix("infino"))
      .build()?;

    // You can deserialize (and thus freeze) the entire configuration as
    config.try_deserialize()
  }

  /// Get server settings.
  pub fn get_server_settings(&self) -> &ServerSettings {
    &self.server
  }

  #[cfg(test)]
  /// Get the default config file name.
  pub fn get_default_config_file_name() -> &'static str {
    DEFAULT_CONFIG_FILE_NAME
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_settings() {
    let config_dir_path = "../config";
    let settings = Settings::new(config_dir_path).expect("Could not parse config");

    // Check server settings.
    let server_settings = settings.get_server_settings();
    assert_eq!(server_settings.get_port(), 3000);
    assert_eq!(server_settings.get_host(), "0.0.0.0");
    assert_eq!(server_settings.get_timestamp_key(), "date");
    assert_eq!(server_settings.get_labels_key(), "labels");
  }
}

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

const DEFAULT_CONFIG_FILE_NAME: &str = "default.toml";

#[derive(Debug, Deserialize)]
/// Settings for infino server.
pub struct ServerSettings {
  commit_interval_in_seconds: u32,
}

impl ServerSettings {
  /// Get the commit interval in seconds.
  pub fn get_commit_interval_in_seconds(&self) -> u32 {
    self.commit_interval_in_seconds
  }
}

#[derive(Debug, Deserialize)]
/// Settings for rabbitmq queue.
pub struct RabbitMQSettings {
  container_name: String,
}

impl RabbitMQSettings {
  /// Get comtainer name for rabbitmq docker container.
  pub fn get_container_name(&self) -> &str {
    &self.container_name
  }
}

#[derive(Debug, Deserialize)]
/// Settings for Tsldb, read from config file.
pub struct Settings {
  server: ServerSettings,
  rabbitmq: RabbitMQSettings,
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

  /// Get tsldb settings.
  pub fn get_rabbitmq_settings(&self) -> &RabbitMQSettings {
    &self.rabbitmq
  }

  #[cfg(test)]
  /// Get the default config file name.
  pub fn get_default_config_file_name() -> &'static str {
    &DEFAULT_CONFIG_FILE_NAME
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_settings() {
    let config_dir_path = "config";
    let settings = Settings::new(&config_dir_path).unwrap();

    let server_settings = settings.get_server_settings();
    assert_eq!(server_settings.get_commit_interval_in_seconds(), 30);

    let rabbitmq_settings = settings.get_rabbitmq_settings();
    assert_eq!(rabbitmq_settings.get_container_name(), "infino-queue");
  }
}

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use crate::utils::error::CoreDBError;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::buckets::get::GetBucketRequest;
use google_cloud_storage::http::buckets::insert::{
  BucketCreationConfig, InsertBucketParam, InsertBucketRequest,
};
use google_cloud_storage::http::Error;

// project_id, and region is necessary for bucket creation operation
pub struct GCPStorageUtils {
  client: Client,
  region: String,
  project_id: String,
}

impl GCPStorageUtils {
  pub async fn new(region: &str) -> Result<Self, CoreDBError> {
    // Load configuration from the environment and create the GCP client.
    let config = match ClientConfig::default().with_auth().await {
      Ok(cfg) => cfg,
      Err(err) => {
        return Err(CoreDBError::IOError(err.to_string()));
      }
    };

    let project_id = match config.project_id.clone() {
      Some(id) => id,
      None => {
        // If project_id is not present, return an error
        return Err(CoreDBError::GCPStorageUtilsError(String::from(
          "Project ID is missing",
        )));
      }
    };

    let client = Client::new(config);

    Ok(Self {
      client,
      region: region.to_owned(),
      project_id,
    })
  }

  pub async fn create_bucket(&self, bucket_name: &str) -> Result<String, CoreDBError> {
    let bucket_creation_result = self
      .client
      .insert_bucket(&InsertBucketRequest {
        name: bucket_name.to_owned(),
        bucket: BucketCreationConfig {
          location: self.region.clone(),
          ..Default::default()
        },
        param: InsertBucketParam {
          project: self.project_id.clone(),
          ..Default::default()
        },
      })
      .await;

    if let Err(err) = bucket_creation_result {
      match err {
        Error::Response(response_err) => {
          if response_err.code == 409 {
            // Bucket with same name already exists
            return Ok(bucket_name.to_owned());
          } else {
            return Err(CoreDBError::GCPStorageUtilsError(format!(
              "Google Cloud Storage error: {}",
              response_err.code
            )));
          }
        }
        _ => {
          return Err(CoreDBError::GCPStorageUtilsError(err.to_string()));
        }
      }
    }

    Ok(bucket_name.to_owned())
  }

  pub async fn check_bucket_exists(&self, bucket_name: &str) -> bool {
    // fetch the bucket to check if it exists
    let bucket_result = self
      .client
      .get_bucket(&GetBucketRequest {
        bucket: bucket_name.to_owned(),
        ..Default::default()
      })
      .await;

    // Check if there's an error
    if bucket_result.is_err() {
      return false;
    }

    // If there's no error, bucket is present
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::env;

  use crate::utils::environment::load_env;

  #[tokio::test]
  async fn test_gcp_storage_utils() {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Do not run this test in Github Actions, and do not run it if the GCP credentials are not set.
    if env::var("GITHUB_ACTIONS").is_ok() || env::var("GOOGLE_APPLICATION_CREDENTIALS").is_err() {
      return;
    }

    // Check a bucket that should exist.
    let utils = GCPStorageUtils::new("US-EAST5").await.unwrap();
    assert!(utils.check_bucket_exists("dev-infino-unit-test").await);

    // Check a bucket that does not exist.
    assert!(
      !(utils
        .check_bucket_exists("dev-infino-bucket-does-not-exist")
        .await)
    );

    // Create a bucket that exists - the operation should succeed.
    let response = utils.create_bucket("dev-infino-unit-test-east5").await;
    assert!(response.is_ok());
  }
}

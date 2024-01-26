use aws_config::Region;
use aws_sdk_s3::Client;

use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};

use crate::utils::error::CoreDBError;

pub struct AWSS3Utils {
  client: Client,
  region: Region,
}

impl AWSS3Utils {
  pub async fn new() -> Result<Self, CoreDBError> {
    // Load configuration from the environment and create the S3 client.
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::v2023_11_09()).await;
    let client = Client::new(&config);

    // Determine the region to use for the bucket location constraint.
    let region = config.region().ok_or(CoreDBError::InvalidConfiguration(
      "No AWS region found".to_owned(),
    ))?;

    Ok(Self {
      client,
      region: region.clone(),
    })
  }

  pub async fn create_bucket(&self, bucket_name: &str) -> Result<CreateBucketOutput, CoreDBError> {
    let builder = match self.region.as_ref() {
      "us-east-1" => self.client.create_bucket(),
      _ => {
        // For non us-east-1 regions, the SDK needs a location constraint.
        let constraint = BucketLocationConstraint::from(self.region.as_ref());
        let cfg = CreateBucketConfiguration::builder()
          .location_constraint(constraint)
          .build();
        self.client.create_bucket().create_bucket_configuration(cfg)
      }
    };

    let response = builder
      .bucket(bucket_name)
      .send()
      .await
      .expect("Could not create S3 bucket");

    Ok(response)
  }

  pub async fn check_bucket_exists(&self, bucket_name: &str) -> bool {
    // Return true if the head_bucket operation is successful, return false otherwise.
    self
      .client
      .head_bucket()
      .bucket(bucket_name)
      .send()
      .await
      .is_ok()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::env;

  use crate::utils::environment::load_env;

  #[tokio::test]
  async fn test_aws_s3_utils() {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Do not run this test in Github Actions, and do not run it if the AWS credentials are not set.
    if env::var("GITHUB_ACTIONS").is_ok() || env::var("AWS_ACCESS_KEY_ID").is_err() {
      return;
    }

    // Check a bucket that should exist.
    let utils = AWSS3Utils::new().await.unwrap();
    assert!(utils.check_bucket_exists("dev-infino-unit-test").await);

    // Check a bucket that does not exist.
    assert!(
      !(utils
        .check_bucket_exists("dev-infino-bucket-does-not-exist")
        .await)
    );

    // Create a bucket that exists - the operation should succeed.
    let response = utils.create_bucket("dev-infino-unit-test").await;
    assert!(response.is_ok());
  }
}

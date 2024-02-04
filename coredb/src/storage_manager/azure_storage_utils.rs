use std::env;

use crate::utils::error::CoreDBError;
use azure_core::error::ErrorKind;
use azure_core::Error;

use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use futures::StreamExt;
use log::debug;

#[derive(Debug)]

pub struct AzureStorageUtils {
  client: BlobServiceClient,
}

impl AzureStorageUtils {
  pub async fn new(region: &str) -> Result<Self, CoreDBError> {
    // Load configuration from the environment and create the Azure Storage client.
    let account =
      env::var("AZURE_STORAGE_ACCOUNT_NAME").expect("missing AZURE_STORAGE_ACCOUNT_NAME");
    let access_key =
      env::var("AZURE_STORAGE_ACCOUNT_KEY").expect("missing AZURE_STORAGE_ACCOUNT_KEY");

    let storage_credentials = StorageCredentials::access_key(account.clone(), access_key);

    let service_client = BlobServiceClient::new(account, storage_credentials);

    debug!(
      "Region {} needs to be saved while creating storage account",
      region
    );

    Ok(Self {
      client: service_client,
    })
  }

  pub async fn create_container(&self, container_name: &str) -> Result<(), CoreDBError> {
    let container_client = self.client.container_client(container_name);

    let container_result = container_client
      .create()
      .public_access(PublicAccess::None)
      .await;

    if let Err(err) = container_result {
      let base_error = err.to_string();

      match (err as Error).kind() {
        ErrorKind::HttpResponse {
          status,
          error_code: _,
        } => {
          if *status == 409 {
            // StatusCode 409 (CONFLICT) indicates that the resource already exists
            debug!("Container already exists");
          } else {
            return Err(CoreDBError::AzureStorageUtilsError(base_error));
          }
        }

        _ => {
          return Err(CoreDBError::AzureStorageUtilsError(base_error));
        }
      }
    }

    Ok(())
  }

  pub async fn check_container_exists(&self, bucket_name: &str) -> bool {
    // Fetch the container to check if it exists
    let get_container_contents = self
      .client
      .container_client(bucket_name.to_owned())
      .list_blobs()
      .into_stream()
      .next()
      .await;

    // Check if there's an error
    match get_container_contents {
      Some(result) => {
        // If the OK result exists, it suggests blobs in containers, indicating the container exists.
        // If there is an error, the container does not exist, so return false.
        result.is_ok()
      }
      None => false,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::env;

  use crate::utils::environment::load_env;

  #[tokio::test]
  async fn test_azure_storage_utils() {
    // Load environment variables - esp creds for accessing non-local storage.
    load_env();

    // Do not run this test in Github Actions, and do not run it if the Azure credentials are not set.
    if env::var("GITHUB_ACTIONS").is_ok() || env::var("AZURE_STORAGE_ACCOUNT_KEY").is_err() {
      return;
    }

    // Check a container that should exist.
    let utils = AzureStorageUtils::new("US-EAST5").await.unwrap();
    assert!(utils.check_container_exists("dev-infino-unit-test").await);

    let result = utils
      .check_container_exists("dev-infino-bucket-does-not-exist")
      .await;

    // Check a container that does not exist.
    assert!(!(result));

    // Create a container that exists - the operation should succeed.
    let response = utils.create_container("dev-infino-unit-test").await;
    assert!(response.is_ok());
  }
}

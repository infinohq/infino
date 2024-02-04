# Environment Variables

## Common

| Name | Description | Default |
|------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | The AWS access key ID. Used in Storage Manager| `nil` |
| `AWS_SECRET_ACCESS_KEY` | The AWS secret key access for slack app buildd-health. Used in Storage Manager.| `nil` |
| `AWS_DEFAULT_REGION` | The channel_id specific to "notifications" slack channel. Used in Storage Manager.| `us-east-1` |
| `AZURE_STORAGE_ACCOUNT_KEY` | The Azure storage account key. Used in Storage Manager.| `nil` |
| `AZURE_STORAGE_ACCOUNT_NAME` | The Azure storage account name. Used in Storage Manager.| `infino` |
| `GOOGLE_APPLICATION_CREDENTIALS` | The file path to GCP's credentials.json. Used in Storage Manager by GCPStorageUtils.| `nil` |
| `SERVICE_ACCOUNT` | The file path to GCP's credentials.json. Used in Storage Manager by GoogleCloudStorageBuilder.| `nil` |

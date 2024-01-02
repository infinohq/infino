use thiserror::Error;

#[derive(Debug, Error)]
/// Collection of error messages in Infino.
pub enum InfinoError {
  #[error("IOError with queue")]
  QueueIOError(#[from] std::io::Error),

  #[error("ClientError with queue")]
  QueueClientError(#[from] rabbitmq_stream_client::error::ClientError),

  #[error("Invalid input {0}.")]
  InvalidInput(String),
}

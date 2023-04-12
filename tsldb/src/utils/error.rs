use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
/// Enum for various errors in Tsldb.
pub enum TsldbError {
  #[error("Invalid size. Expected {0}, Received {1}.")]
  InvalidSize(usize, usize),

  #[error("Already at full capcity. Max capacity {0}.")]
  CapacityFull(usize),

  #[error("Cannot read directory {0}.")]
  CannotReadDirectory(String),

  #[error("Cannot find index metadata in directory {0}.")]
  CannotFindIndexMetadataInDirectory(String),

  #[error("Time series block is empty - cannot be compressed.")]
  EmptyTimeSeriesBlock(),

  #[error("Cannot decode time series. {0}")]
  CannotDecodeTimeSeries(String),

  #[error("Invalid configuration. {0}")]
  InvalidConfiguration(String),
}

use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
/// Enum for various errors in coredb.
pub enum CoreDBError {
  #[error("Invalid size. Expected {0}, Received {1}.")]
  InvalidSize(usize, usize),

  #[error("Already at full capcity. Max capacity {0}.")]
  CapacityFull(usize),

  #[error("Cannot read directory {0}.")]
  CannotReadDirectory(String),

  #[error("Cannot find index metadata in directory {0}.")]
  CannotFindIndexMetadataInDirectory(String),

  #[error("The directory {0} is not an index directory.")]
  NotAnIndexDirectory(String),

  #[error("Time series block is empty - cannot be compressed.")]
  EmptyTimeSeriesBlock(),

  #[error("Cannot decode time series. {0}")]
  CannotDecodeTimeSeries(String),

  #[error("Invalid configuration. {0}")]
  InvalidConfiguration(String),

  #[error("Cannot create index. {0}")]
  CannotCreateIndex(String),

  #[error("Cannot delete index. {0}")]
  CannotDeleteIndex(String),

  #[error("Index not found. {0}")]
  IndexNotFound(String),
}

#[derive(Debug)]
pub enum AstError {
  InvalidNode,
  CombinerFailure(String),
  TraverseError(String),
  PostingsListError(String),
  DocMatchingError(String),
}

#[derive(Debug)]
pub enum LogError {
  LogMessageNotFound(u32),
}

#[derive(Debug)]
pub enum SegmentSearchError {
  AstError(AstError),
  LogError(LogError),
}

#[derive(Debug)]
pub enum SegmentError {
  SegmentNotFoundError(u32),
}

#[derive(Debug)]
pub enum SummaryError {
  ExternalSummaryError(String),
  SearchLogsError(SearchLogsError),
}

#[derive(Debug)]
pub enum SearchLogsError {
  JsonParseError(serde_json::Error),
  SegmentSearchError(SegmentSearchError),
  SegmentError(SegmentError),
  NoQueryProvided,
}

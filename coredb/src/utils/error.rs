// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

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

  #[error("Storage Error:  {0}")]
  StorageError(String),

  #[error("IO Error: {0}")]
  IOError(String),

  #[error("Search Logs Error: {0}")]
  SearchLogsError(SearchLogsError),

  #[error("Segment in memory: {0}")]
  SegmentInMemory(u32),

  #[error("Invalid postings block: {0}")]
  InvalicPostingsBlock(String),

  #[error("AWS SDK Error: {0}")]
  AwsSdkError(String),

  #[error("GCP Storage Utils Error: {0}")]
  GCPStorageUtilsError(String),

  #[error("Azure Storage Utils Error: {0}")]
  AzureStorageUtilsError(String),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AstError {
  #[error("Invalid query")]
  InvalidQuery,

  #[error("Combiner failure: {0}")]
  CombinerFailure(String),

  #[error("Traverse error: {0}")]
  TraverseError(String),

  #[error("Postings list error: {0}")]
  PostingsListError(String),

  #[error("Doc matching error: {0}")]
  DocMatchingError(String),

  #[error("Unsupported query: {0}")]
  UnsupportedQuery(String),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum LogError {
  #[error("Log mesage not found error: {0}")]
  LogMessageNotFound(u32),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SegmentSearchError {
  #[error("Ast error: {0}")]
  AstError(AstError),

  #[error("Log error: {0}")]
  LogError(LogError),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SegmentError {
  #[error("Segment not found error: {0}")]
  SegmentNotFoundError(u32),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SummaryError {
  #[error("External summary error: {0}")]
  ExternalSummaryError(String),

  #[error("Search logs error: {0}")]
  SearchLogsError(SearchLogsError),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SearchLogsError {
  #[error("Json parse error: {0}")]
  JsonParseError(String),

  #[error("Segment search error: {0}")]
  SegmentSearchError(SegmentSearchError),

  #[error("Segment error: {0}")]
  SegmentError(SegmentError),

  #[error("No query provided")]
  NoQueryProvided,
}

impl From<object_store::Error> for CoreDBError {
  fn from(error: object_store::Error) -> Self {
    CoreDBError::StorageError(error.to_string())
  }
}

impl From<std::io::Error> for CoreDBError {
  fn from(error: std::io::Error) -> Self {
    CoreDBError::IOError(error.to_string())
  }
}

impl From<SearchLogsError> for CoreDBError {
  fn from(error: SearchLogsError) -> Self {
    CoreDBError::SearchLogsError(error)
  }
}

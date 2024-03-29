// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use std::num::ParseIntError;

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

  #[error("Query Error: {0}")]
  QueryError(QueryError),

  #[error("Segment not found error: {0}")]
  SegmentNotFoundError(u32),

  #[error("Segment in memory: {0}")]
  SegmentInMemory(u32),

  #[error("Invalid postings block: {0}")]
  InvalidPostingsBlock(String),

  #[error("AWS SDK Error: {0}")]
  AwsSdkError(String),

  #[error("GCP Storage Utils Error: {0}")]
  GCPStorageUtilsError(String),

  #[error("Azure Storage Utils Error: {0}")]
  AzureStorageUtilsError(String),

  #[error("Too many append requests, slow down so that commit thread can keep up")]
  TooManyAppendsError(),

  #[error("Segment merge failed")]
  SegmentMergeFailed(),

  #[error("Policy not found")]
  PolicyNotFound(),

  #[error("Invalid policy")]
  InvalidPolicy(),

  #[error("Invalid log Id {0}")]
  InvalidLogId(String),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum QueryError {
  #[error("Invalid query")]
  InvalidQuery,

  #[error("Combiner failure: {0}")]
  CombinerFailure(String),

  #[error("Index not found error: {0}")]
  IndexNotFoundError(String),

  #[error("Traverse error: {0}")]
  TraverseError(String),

  #[error("Postings list error: {0}")]
  PostingsListError(String),

  #[error("Doc matching error: {0}")]
  DocMatchingError(String),

  #[error("Unsupported query: {0}")]
  UnsupportedQuery(String),

  #[error("CoreDB error: {0}")]
  CoreDBError(String),

  #[error("CoreDB error: {0}")]
  TimeOutError(String),

  #[error("Log mesage not found error: {0}")]
  LogMessageNotFound(u32),

  #[error("External summary error: {0}")]
  ExternalQueryError(String),

  #[error("Search logs error: {0}")]
  SearchLogsError(String),

  #[error("Search metrics error: {0}")]
  SearchMetricsError(String),

  #[error("Json parse error: {0}")]
  JsonParseError(String),

  #[error("No query provided")]
  NoQueryProvided,

  #[error("Regexp Error: {0}")]
  RegexpError(String),

  #[error("Unable to search and mark logs as deleted")]
  SearchAndMarkLogsError,
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

impl From<serde_json::Error> for CoreDBError {
  fn from(error: serde_json::Error) -> Self {
    CoreDBError::IOError(error.to_string())
  }
}

impl From<ParseIntError> for CoreDBError {
  fn from(error: ParseIntError) -> Self {
    CoreDBError::IOError(error.to_string())
  }
}

impl From<QueryError> for CoreDBError {
  fn from(error: QueryError) -> Self {
    CoreDBError::QueryError(error)
  }
}

impl From<CoreDBError> for QueryError {
  fn from(error: CoreDBError) -> Self {
    QueryError::CoreDBError(error.to_string())
  }
}

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

/// Number of entries per time series block.
pub(super) static BLOCK_SIZE_FOR_TIME_SERIES: usize = 128;

/// Separator between the label name and label value to create a label term. For example,
/// if the label name is 'method' and the value is 'GET',and the LABEL_SEPARATOR is '~',
/// in the labels map this will be stored as 'method~GET'.
pub const LABEL_SEPARATOR: &str = "~";

/// The label for the metric name when stored in the time series. For exmaple, if the METRIC_NAME_PREFIX
/// is '__name__', the LABEL_SEPARATOR is '~', and the matric name is 'request_count', in the labels map,
/// this will be stored as '__name__~request_count'.
pub const METRIC_NAME_PREFIX: &str = "__name__";

/// Represents a condition to be used in a metrics query.
#[derive(PartialEq, Debug)]
pub enum MetricsQueryCondition {
  Equals,
  NotEquals,
  EqualsRegex,
  NotEqualsRegex,
  Undefined,
}

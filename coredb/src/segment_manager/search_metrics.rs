use std::collections::HashSet;

use crate::metric::metric_point::MetricPoint;
use crate::metric::time_series::TimeSeries;
use crate::metric::time_series::LABEL_SEPARATOR;
use crate::segment_manager::segment::Segment;

impl Segment {
  /// Get the unique label names for this segment.
  pub fn get_label_names(&self) -> Vec<String> {
    let labels = self.get_labels();
    let mut label_names = HashSet::new(); // This set will hold the unique label names.

    // Iterate through all entries in the DashMap.
    for entry in labels.iter() {
      let key = entry.key(); // Get the key from the entry.

      // Split the key into the label name and label value at the LABEL_SEPARATOR
      // The split_once method will return a tuple if the separator is found.
      if let Some((label_name, _)) = key.split_once(LABEL_SEPARATOR) {
        // Add the label name to the HashSet (to maintain unique label names).
        label_names.insert(label_name.to_string());
      }
    }

    // Convert the HashSet of unique label names into a Vec and return it
    label_names.into_iter().collect()
  }

  /// Get the unique label values for given label name for this segment.
  pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
    let labels = self.get_labels();
    let mut label_values = HashSet::new(); // This set will hold the unique label values

    // Iterate through all entries in the DashMap
    for entry in labels.iter() {
      let key = entry.key(); // Get the key from the entry
                             // Split the key into potential label name and value at the LABEL_SEPARATOR
      if let Some((current_label_name, label_value)) = key.split_once(LABEL_SEPARATOR) {
        // Check if the current label name matches the one we're looking for
        if current_label_name == label_name {
          // Add the label value to the HashSet. This ensures uniqueness.
          label_values.insert(label_value.to_string());
        }
      }
    }

    // Convert the HashSet of unique label values into a Vec and return it
    label_values.into_iter().collect()
  }

  // ---
  // TODO: This api currently only supports only label_name=label_value queries.
  // Additionally, the following need to be supported:
  //   - label_name!=label_value,
  //   - label_name=~label_value_regex,
  //   - label_name!=~label_value_regex.
  // ---
  /// Get the time series for the given label name/value, within the given (inclusive) time range.
  pub fn search_metrics(
    &self,
    label_name: &str,
    label_value: &str,
    range_start_time: u64,
    range_end_time: u64,
  ) -> Vec<MetricPoint> {
    let label = TimeSeries::get_label(label_name, label_value);
    let label_id = self.get_labels().get(&label);
    let mut retval = match label_id {
      Some(label_id) => {
        let arc_ts = self
          .get_time_series_map()
          .get_time_series(*label_id)
          .unwrap()
          .clone();
        let ts = &*arc_ts.read().unwrap();
        ts.get_metrics(range_start_time, range_end_time)
      }
      None => Vec::new(),
    };
    // Sort the retrieved time series in chronological order.
    retval.sort();

    retval
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use super::*;
  use crate::metric::time_series::METRIC_NAME_PREFIX;

  #[test]
  pub fn test_basic_metrics_search() {
    let segment = Segment::new();

    // Create a couple of metric points.
    let mut label_set_1 = HashMap::new();
    label_set_1.insert("label_name_1".to_string(), "label_value_1".to_string());
    label_set_1.insert("label_name_2".to_string(), "label_value_1".to_string());
    label_set_1.insert("label_name_3".to_string(), "label_value_3".to_string());
    segment
      .append_metric_point("metric_name_1", &label_set_1, 1, 1.0)
      .expect("Could not append metric point");
    segment
      .append_metric_point("metric_name_2", &label_set_1, 2, 2.0)
      .expect("Could not append metric point");

    let mut label_set_2 = HashMap::new();
    label_set_2.insert("label_name_1".to_string(), "label_value_2".to_string());
    label_set_2.insert("label_name_3".to_string(), "label_value_3".to_string());
    label_set_2.insert("label_name_4".to_string(), "label_value_4".to_string());
    segment
      .append_metric_point("metric_name_1", &label_set_2, 3, 3.0)
      .expect("Could not append metric point");

    // Check label_name retrieval.
    let label_names = segment.get_label_names();
    assert_eq!(label_names.len(), 5);
    assert!(label_names.contains(&METRIC_NAME_PREFIX.to_owned()));
    assert!(label_names.contains(&"label_name_1".to_owned()));
    assert!(label_names.contains(&"label_name_2".to_owned()));
    assert!(label_names.contains(&"label_name_3".to_owned()));
    assert!(label_names.contains(&"label_name_4".to_owned()));

    // Check label_value retrieval for a given label_name.
    let label_values = segment.get_label_values(METRIC_NAME_PREFIX);
    assert_eq!(label_values.len(), 2);
    assert!(label_values.contains(&"metric_name_1".to_owned()));
    assert!(label_values.contains(&"metric_name_2".to_owned()));
    let label_values = segment.get_label_values("label_name_1");
    assert_eq!(label_values.len(), 2);
    assert!(label_values.contains(&"label_value_1".to_owned()));
    assert!(label_values.contains(&"label_value_2".to_owned()));
    let label_values = segment.get_label_values("label_name_2");
    assert_eq!(label_values.len(), 1);
    assert!(label_values.contains(&"label_value_1".to_owned()));
    let label_values = segment.get_label_values("label_name_3");
    assert_eq!(label_values.len(), 1);
    assert!(label_values.contains(&"label_value_3".to_owned()));
    let label_values = segment.get_label_values("label_name_4");
    assert_eq!(label_values.len(), 1);
    assert!(label_values.contains(&"label_value_4".to_owned()));

    // Check basic search operation.
    let results = segment.search_metrics("label_name_1", "label_value_1", 0, u64::MAX);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_time(), 1);
    assert_eq!(results[1].get_time(), 2);
    let results = segment.search_metrics("label_name_1", "label_value_1", 0, 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_time(), 1);
    let results = segment.search_metrics("label_name_3", "label_value_3", 0, u64::MAX);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get_time(), 1);
    assert_eq!(results[1].get_time(), 2);
    assert_eq!(results[2].get_time(), 3);
  }
}

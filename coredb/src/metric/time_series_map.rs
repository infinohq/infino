use std::fmt;

use dashmap::DashMap;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::metric::time_series::TimeSeries;
use crate::utils::error::CoreDBError;
use crate::utils::sync::{Arc, RwLock};

#[derive(Debug)]
/// Represents an time series map - a map of label-id to TimeSeries.
pub struct TimeSeriesMap {
  time_series_map: DashMap<u32, Arc<RwLock<TimeSeries>>>,
}

impl TimeSeriesMap {
  /// Creates a new inverted map.
  pub fn new() -> Self {
    TimeSeriesMap {
      time_series_map: DashMap::new(),
    }
  }

  pub fn is_empty(&self) -> bool {
    self.time_series_map.is_empty()
  }

  pub fn get_time_series(&self, label_id: u32) -> Option<Arc<RwLock<TimeSeries>>> {
    self
      .time_series_map
      .get(&label_id)
      .map(|time_series| time_series.clone())
  }

  pub fn get_time_series_map(&self) -> &DashMap<u32, Arc<RwLock<TimeSeries>>> {
    &self.time_series_map
  }

  pub fn append(&self, label_id: u32, time: u64, value: f64) -> Result<(), CoreDBError> {
    // Access or insert the metric point for the given label id, ensuring thread-safe operation.
    let arc_rwlock_ts = self
      .time_series_map
      .entry(label_id)
      .or_default()
      .value()
      .clone();

    // Acquire a write lock on the time series and append the metric point.
    // The scope of the write lock is minimized to the append operation only.
    {
      let mut ts = arc_rwlock_ts.write(); // Lock is acquired here.
      ts.append(time, value);
    } // Lock is automatically released here as pl goes out of scope.

    Ok(())
  }
}

impl Default for TimeSeriesMap {
  fn default() -> Self {
    Self::new()
  }
}

/// Custom Serialize for TimeSeriesMap
impl Serialize for TimeSeriesMap {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let map = &self.time_series_map;
    let mut map_ser = serializer.serialize_map(Some(map.len()))?;
    for entry in map.iter() {
      let key = entry.key();
      let value_lock = entry.value().read();
      map_ser.serialize_entry(&key, &*value_lock)?;
    }
    map_ser.end()
  }
}

/// Custom Deserialize for TimeSeriesMap
impl<'de> Deserialize<'de> for TimeSeriesMap {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: Deserializer<'de>,
  {
    deserializer.deserialize_map(TimeSeriesMapVisitor)
  }
}

struct TimeSeriesMapVisitor;

impl<'de> Visitor<'de> for TimeSeriesMapVisitor {
  type Value = TimeSeriesMap;

  fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    formatter.write_str("a map of u32 to Arc<RwLock<TimeSeries>>")
  }

  fn visit_map<M>(self, mut access: M) -> Result<TimeSeriesMap, M::Error>
  where
    M: MapAccess<'de>,
  {
    let dash_map = DashMap::new();

    while let Some((key, value)) = access.next_entry::<u32, TimeSeries>()? {
      dash_map.insert(key, Arc::new(RwLock::new(value)));
    }

    Ok(TimeSeriesMap {
      time_series_map: dash_map,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::thread;

  #[test]
  fn test_parallel_append() {
    let time_series_map = Arc::new(TimeSeriesMap::new());

    // New inverted map should be empty.
    assert!(time_series_map.is_empty());

    let label_id = 1; // Using a single label ID for simplicity.

    // Spawn 100 threads to append metrics (1 to 100).
    let mut handles = vec![];
    for i in 1..=100 {
      let time_series_map = time_series_map.clone();
      let handle = thread::spawn(move || {
        time_series_map
          .append(label_id, i, i as f64)
          .expect("Could not append to time series map");
      });
      handles.push(handle);
    }

    // Wait for all threads to complete.
    for handle in handles {
      handle.join().unwrap();
    }

    // Retrieve and check the TimeSeries.
    let time_series_arc = time_series_map.get_time_series(label_id).unwrap();
    let time_series = time_series_arc.read();

    let time_series_vec = time_series.flatten();

    // Ensure the list contains 100 metric points.
    assert_eq!(time_series_vec.len(), 100);

    // Ensure the list is sorted
    let mut last_time = 0;
    for metric_point in time_series_vec {
      let curr_time = metric_point.get_time();
      assert!(
        curr_time > last_time,
        "The list is not sorted. Found {} after {}",
        curr_time,
        last_time
      );
      last_time = curr_time;
    }
  }

  #[test]
  fn serialize_and_deserialize_inverted_map() {
    let time_series_map = TimeSeriesMap::new();

    // Setup - Insert some data into the TimeSeriesMap.
    let mut time_series = TimeSeries::new();
    time_series.append(1, 1_f64);
    time_series.append(2, 2_f64);
    time_series.append(3, 3_f64);

    time_series_map
      .time_series_map
      .insert(1, Arc::new(RwLock::new(time_series)));

    // Serialize the InvertedMap
    let serialized =
      serde_json::to_string(&time_series_map).expect("Failed to serialize InvertedMap");

    // Deserialize the InvertedMap
    let deserialized: TimeSeriesMap =
      serde_json::from_str(&serialized).expect("Failed to deserialize InvertedMap");

    // Verify that deserialized data matches original
    let deserialized_time_series = deserialized.time_series_map.get(&1).unwrap();
    let deserialized_time_series = &*deserialized_time_series.read();

    let deserialized_time_series_vec = deserialized_time_series.flatten();
    assert_eq!(deserialized_time_series_vec.first().unwrap().get_time(), 1);
    assert_eq!(deserialized_time_series_vec.get(1).unwrap().get_time(), 2);
    assert_eq!(deserialized_time_series_vec.get(2).unwrap().get_time(), 3);
  }
}

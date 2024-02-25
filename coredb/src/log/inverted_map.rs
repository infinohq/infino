use std::fmt;

use dashmap::DashMap;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::log::postings_list::PostingsList;
use crate::utils::error::CoreDBError;
use crate::utils::sync::{Arc, RwLock};

#[derive(Debug)]
/// Represents an inverted index - a map of term-id to PostingsList.
pub struct InvertedMap {
  inverted_map: DashMap<u32, Arc<RwLock<PostingsList>>>,
}

impl InvertedMap {
  /// Creates a new inverted map.
  pub fn new() -> Self {
    InvertedMap {
      inverted_map: DashMap::new(),
    }
  }

  pub fn is_empty(&self) -> bool {
    self.inverted_map.is_empty()
  }

  pub fn get_postings_list(&self, term_id: u32) -> Option<Arc<RwLock<PostingsList>>> {
    self
      .inverted_map
      .get(&term_id)
      .map(|postings_list| postings_list.clone())
  }

  pub fn append(&self, term_id: u32, log_message_id: u32) -> Result<(), CoreDBError> {
    // Access or insert the postings list for the given term_id, ensuring thread-safe operation.
    let arc_rwlock_pl = self
      .inverted_map
      .entry(term_id)
      .or_default()
      .value()
      .clone();

    // Acquire a write lock on the postings list and append the log message id.
    // The scope of the write lock is minimized to the append operation only.
    {
      let mut pl = arc_rwlock_pl.write(); // Lock is acquired here.
      pl.append(log_message_id)?;
    } // Lock is automatically released here as pl goes out of scope.

    Ok(())
  }

  #[cfg(test)]
  pub fn insert_unchecked(&self, term_id: u32, postings_list: PostingsList) {
    self
      .inverted_map
      .insert(term_id, Arc::new(RwLock::new(postings_list)));
  }

  #[cfg(test)]
  pub fn clear_inverted_map(&self) {
    self.inverted_map.clear();
  }
}

/// Custom Serialize for InvertedMap
impl Serialize for InvertedMap {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let map = &self.inverted_map;
    let mut map_ser = serializer.serialize_map(Some(map.len()))?;
    for entry in map.iter() {
      let key = entry.key();
      let value_lock = entry.value().read();
      map_ser.serialize_entry(&key, &*value_lock)?;
    }
    map_ser.end()
  }
}

/// Custom Deserialize for InvertedMap
impl<'de> Deserialize<'de> for InvertedMap {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: Deserializer<'de>,
  {
    deserializer.deserialize_map(InvertedMapVisitor)
  }
}

struct InvertedMapVisitor;

impl<'de> Visitor<'de> for InvertedMapVisitor {
  type Value = InvertedMap;

  fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    formatter.write_str("a map of u32 to Arc<RwLock<PostingsList>>")
  }

  fn visit_map<M>(self, mut access: M) -> Result<InvertedMap, M::Error>
  where
    M: MapAccess<'de>,
  {
    let dash_map = DashMap::new();

    while let Some((key, value)) = access.next_entry::<u32, PostingsList>()? {
      dash_map.insert(key, Arc::new(RwLock::new(value)));
    }

    Ok(InvertedMap {
      inverted_map: dash_map,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::sync::thread;

  #[test]
  fn test_parallel_append() {
    let inverted_map = Arc::new(InvertedMap::new());

    // New inverted map should be empty.
    assert!(inverted_map.is_empty());

    let term_id = 1; // Using a single term ID for simplicity.

    // Spawn 100 threads to append log message IDs (1 to 100).
    let mut handles = vec![];
    for log_message_id in 1..=100 {
      let inverted_map = inverted_map.clone();
      let handle = thread::spawn(move || {
        inverted_map
          .append(term_id, log_message_id)
          .expect("Could not append to postings list");
      });
      handles.push(handle);
    }

    // Wait for all threads to complete.
    for handle in handles {
      handle.join().unwrap();
    }

    // Retrieve and check the PostingsList.
    let postings_list_arc = inverted_map.get_postings_list(term_id).unwrap();
    let postings_list = postings_list_arc.read();

    let postings_list_vec = postings_list.flatten();

    // Ensure the list contains 100 IDs.
    assert_eq!(postings_list_vec.len(), 100);

    // Ensure the list is sorted
    let mut last_id = 0;
    for &id in &postings_list_vec {
      assert!(
        id > last_id,
        "The list is not sorted. Found {} after {}",
        id,
        last_id
      );
      last_id = id;
    }
  }

  #[test]
  fn serialize_and_deserialize_inverted_map() {
    let inverted_map = InvertedMap::new();

    // Setup - Insert some data into the InvertedMap
    let mut postings_list = PostingsList::new();
    postings_list
      .append(1)
      .expect("Could not append to postings list");
    postings_list
      .append(2)
      .expect("Could not append to postings list");
    postings_list
      .append(3)
      .expect("Could not append to postings list");

    inverted_map
      .inverted_map
      .insert(1, Arc::new(RwLock::new(postings_list)));

    // Serialize the InvertedMap
    let serialized = serde_json::to_string(&inverted_map).expect("Failed to serialize InvertedMap");

    // Deserialize the InvertedMap
    let deserialized: InvertedMap =
      serde_json::from_str(&serialized).expect("Failed to deserialize InvertedMap");

    // Verify that deserialized data matches original
    let deserialized_postings_list = deserialized.inverted_map.get(&1).unwrap();
    let deserialized_postings_list = &*deserialized_postings_list.read();

    let deserialized_postings_list_vec = deserialized_postings_list.flatten();
    assert_eq!(deserialized_postings_list_vec, vec![1, 2, 3]);
  }
}

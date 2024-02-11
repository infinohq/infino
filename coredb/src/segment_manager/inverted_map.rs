use std::fmt;
use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::log::postings_list::PostingsList;

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

  pub fn append(&self, term_id: u32, log_message_id: u32) {
    // Need to lock the shard that contains the term, so that some other thread doesn't insert the same term.
    // Use the entry api - https://github.com/xacrimon/dashmap/issues/169#issuecomment-1009920032
    let arc_rwlock_pl = self.inverted_map.entry(term_id).or_default().clone();
    let pl = &mut *arc_rwlock_pl.write().unwrap();
    pl.append(log_message_id);
  }

  #[cfg(test)]
  pub fn insert_unchecked(&self, term_id: u32, postings_list: PostingsList) {
    self
      .inverted_map
      .insert(term_id, Arc::new(RwLock::new(postings_list)));
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
      let value_lock = entry.value().read().unwrap();
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

// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

/// Custom serde serialize and deserialize implementation for RwLock.
pub mod rwlock_serde {
  use crate::utils::sync::RwLock;
  use serde::de::Deserializer;
  use serde::ser::Serializer;
  use serde::{Deserialize, Serialize};

  /// Serialize the type wrapped in RwLock.
  pub fn serialize<S, T>(val: &RwLock<T>, s: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
    T: Serialize,
  {
    let inner = &*val.read().unwrap();
    T::serialize(inner, s)
  }

  /// Deserialize the type and wrap it in RwLock.
  pub fn deserialize<'de, D, T>(d: D) -> Result<RwLock<T>, D::Error>
  where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
  {
    Ok(RwLock::new(T::deserialize(d)?))
  }
}

/// Custom serde serialize and deserialize implementation for Arc<RwLock>.
pub mod arc_rwlock_serde {
  use crate::utils::sync::{Arc, RwLock};
  use serde::de::Deserializer;
  use serde::ser::Serializer;
  use serde::{Deserialize, Serialize};

  /// Serialize the type wrapped in RwLock.
  #[allow(dead_code)]
  pub fn serialize<S, T>(val: &Arc<RwLock<T>>, s: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
    T: Serialize,
  {
    let cloned = val.clone();
    let inner = &cloned.read().unwrap();
    T::serialize(inner, s)
  }

  /// Deserialize the type and wrap it in RwLock.
  #[allow(dead_code)]
  pub fn deserialize<'de, D, T>(d: D) -> Result<Arc<RwLock<T>>, D::Error>
  where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
  {
    let rwlock_val = Arc::new(RwLock::new(T::deserialize(d)?));
    Ok(rwlock_val)
  }
}

/// Custom serde serialize and deserialize implementation for AtomicCell.
pub mod atomic_cell_serde {
  use crossbeam::atomic::AtomicCell;
  use serde::de::Deserializer;
  use serde::ser::Serializer;
  use serde::{Deserialize, Serialize};

  /// Serialize the type wrapped in AtomicCell.
  pub fn serialize<S, T>(val: &AtomicCell<T>, s: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
    T: Serialize,
    T: std::marker::Copy,
  {
    T::serialize(&(val.load()), s)
  }

  /// Deserialize the type and wrap in AtomicCell.
  pub fn deserialize<'de, D, T>(d: D) -> Result<AtomicCell<T>, D::Error>
  where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
    T: std::marker::Copy,
  {
    Ok(AtomicCell::new(T::deserialize(d).unwrap()))
  }
}

/*
/// Custom serde serialize and deserialize implementation for Arc<RwLock<[T]>>.
pub mod arc_rwlock_array_serde {
  use std::fmt::Formatter;

  use serde::de::{Deserializer, SeqAccess};
  use serde::ser::SerializeTuple;
  use serde::ser::Serializer;
  use serde::{Deserialize, Serialize};
  use serde_with::de::DeserializeAsWrap;
  use serde_with::DeserializeAs;

  use crate::utils::sync::{Arc, RwLock};

  /// Serialize the array type wrapped in Arc<RwLock>.
  fn serialize<const N: usize, S, T>(
    t: Arc<RwLock<&[T; N]>>,
    serializer: S,
  ) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
    T: Serialize,
  {
    let cloned = t.clone();
    let value = *cloned.read().unwrap();
    let mut ser_tuple = serializer.serialize_tuple(N)?;
    for elem in value {
      ser_tuple.serialize_element(elem)?;
    }
    ser_tuple.end()
  }

  pub fn deserialize<'de, D, T, N>(d: D) -> Result<Arc<RwLock<[T; N]>>, D::Error>
  where
    D: Deserializer<'de>,
    T: Deserialize<'de> + std::marker::Copy,
    N: usize;
  {
    let val = T::deserialize(d)?;
    let rwlock_val = Arc::new(RwLock::new([val; 128]));
    Ok(rwlock_val)
  }
}
*/

use crate::utils::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
pub struct AtomicVector<T> {
  #[serde(skip_serializing, skip_deserializing)]
  data: Arc<RwLock<Vec<T>>>,
}
impl<T> Default for AtomicVector<T> {
  fn default() -> Self {
    Self::new()
  }
}
impl<T> AtomicVector<T> {
  pub fn new() -> Self {
    AtomicVector {
      // Initialize with RwLock and Arc for shared, mutable access
      data: Arc::new(RwLock::new(vec![])),
    }
  }
  pub fn get(&self) -> Vec<T>
  where
    T: Clone,
  {
    // Direct access without unwrapping
    self.data.read().clone()
  }

  pub fn push(&self, value: T) {
    // Direct access without unwrapping
    self.data.write().push(value);
  }

  pub fn extend<I>(&self, iter: I)
  where
    I: IntoIterator<Item = T>,
  {
    // Direct access, no need for unwrap
    let mut data = self.data.write();
    data.extend(iter);
  }

  pub fn empty(&self) {
    // Directly clearing the vector without unwrap
    let mut data = self.data.write();
    data.clear();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::{sync::Arc, thread};

  #[tokio::test]
  async fn test_atomic_vector() {
    let atomic_vector = AtomicVector::new();
    atomic_vector.push(1);
    atomic_vector.push(2);
    atomic_vector.push(3);
    assert_eq!(atomic_vector.get(), vec![1, 2, 3]);
  }

  #[tokio::test]
  async fn test_new() {
    let atomic_vec: AtomicVector<u32> = AtomicVector::new();
    assert_eq!(atomic_vec.get(), Vec::<u32>::new());
  }

  #[tokio::test]
  async fn test_push_and_get() {
    let av = AtomicVector::<u32>::new();
    av.push(5);
    assert_eq!(av.get(), vec![5]);
  }

  #[tokio::test]
  async fn test_extend() {
    let av = AtomicVector::new();
    av.extend(vec![1, 2, 3]);
    assert_eq!(av.get(), vec![1, 2, 3]);
  }

  #[tokio::test]
  async fn test_push() {
    let atomic_vec = Arc::new(AtomicVector::<u32>::new());
    let atomic_vec_clone = Arc::clone(&atomic_vec);

    thread::spawn(move || {
      atomic_vec_clone.push(1);
      atomic_vec_clone.push(2);
      atomic_vec_clone.push(3);
    })
    .join()
    .unwrap();

    assert_eq!(atomic_vec.get(), vec![1, 2, 3]);
  }

  #[tokio::test]
  async fn test_multiple_threads() {
    let atomic_vec = Arc::new(AtomicVector::<u32>::new());
    let atomic_vec_clones = (0..10).map(|_| Arc::clone(&atomic_vec)).collect::<Vec<_>>();

    let handles = atomic_vec_clones
      .into_iter()
      .map(|atomic_vec_clone| {
        thread::spawn(move || {
          for i in 0..100 {
            atomic_vec_clone.push(i);
          }
        })
      })
      .collect::<Vec<_>>();

    for handle in handles {
      handle.join().unwrap();
    }

    let final_vec = atomic_vec.get();
    assert_eq!(final_vec.len(), 1000);
  }

  #[tokio::test]
  async fn test_with_complex_type() {
    let atomic_vec = Arc::new(AtomicVector::<String>::new());
    let atomic_vec_clone = Arc::clone(&atomic_vec);

    thread::spawn(move || {
      atomic_vec_clone.push("1".to_string());
      atomic_vec_clone.push(2.to_string());
      atomic_vec_clone.extend(vec!["3".to_string(), "4".to_string()]);
    })
    .join()
    .unwrap();

    assert_eq!(
      atomic_vec.get(),
      vec![
        "1".to_string(),
        "2".to_string(),
        "3".to_string(),
        "4".to_string()
      ]
    );
  }

  #[tokio::test]
  async fn test_empty() {
    let atomic_vec = Arc::new(AtomicVector::<u32>::new());
    atomic_vec.push(1);
    atomic_vec.push(2);
    atomic_vec.push(3);
    atomic_vec.empty();
    assert_eq!(atomic_vec.get(), Vec::<u32>::new());
  }
}

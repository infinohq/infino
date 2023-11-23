#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Mutex};
#[cfg(not(loom))]
pub(crate) use std::thread;

#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Mutex};
#[cfg(loom)]
pub(crate) use loom::thread;

// Note that as of 03/2023, the loom library has an issue where in certain scenarios,
// RwLock blocks even if all the locks are held by readers. See
// https://github.com/tokio-rs/loom/blob/16e5e9a0e562cf754ced04ac1a82802b8e492178/src/rt/notify.rs#L113
// Once this is fixed, we can use loom::sync::RwLock when configuration is loom (and use std::sync::RwLock
// when the configuration isn't loom).
pub(crate) use std::sync::RwLock;

// A call to this function will compile only if T is Send + Sync.
#[cfg(test)]
pub fn is_sync<T: Send + Sync>() {}

#[test]
fn test_u32_is_sync() {
  is_sync::<u32>();
}

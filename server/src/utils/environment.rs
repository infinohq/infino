// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use lazy_static::lazy_static;
use std::sync::Mutex;

#[cfg(test)]
use std::env::VarError;

#[cfg(test)]
use std::panic::{RefUnwindSafe, UnwindSafe};

#[cfg(test)]
use std::{env, panic};

lazy_static! {
  static ref SERIAL_TEST: Mutex<()> = Default::default();
}

/// Sets environment variables to the given value for the duration of the closure.
/// Restores the previous values when the closure completes or panics, before unwinding the panic.
/// Typically used in tests.
#[cfg(test)]
pub fn with_env_vars<F>(kvs: Vec<(&str, Option<&str>)>, closure: F)
where
  F: Fn() + UnwindSafe + RefUnwindSafe,
{
  let guard = SERIAL_TEST.lock().unwrap();
  let mut old_kvs: Vec<(&str, Result<String, VarError>)> = Vec::new();
  for (k, v) in kvs {
    let old_v = env::var(k);
    old_kvs.push((k, old_v));
    match v {
      None => env::remove_var(k),
      Some(v) => env::set_var(k, v),
    }
  }

  match panic::catch_unwind(|| {
    closure();
  }) {
    Ok(_) => {
      for (k, v) in old_kvs {
        reset_env(k, v);
      }
    }
    Err(err) => {
      for (k, v) in old_kvs {
        reset_env(k, v);
      }
      drop(guard);
      panic::resume_unwind(err);
    }
  };
}

#[cfg(test)]
fn reset_env(k: &str, old: Result<String, VarError>) {
  if let Ok(v) = old {
    env::set_var(k, v);
  } else {
    env::remove_var(k);
  }
}

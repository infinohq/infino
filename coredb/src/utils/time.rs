use std::time::SystemTime as Time;

pub fn get_current_time_in_seconds(time: Time) -> u64 {
  let duration = time.duration_since(std::time::UNIX_EPOCH).unwrap();
  duration.as_secs()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_get_current_time_in_seconds() {
    let time = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1700000000);
    let time_in_seconds = get_current_time_in_seconds(time);
    assert_eq!(time_in_seconds, 1700000000);
  }
}

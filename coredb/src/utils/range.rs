/// Returns true if the two ranges overlap (inclusive).
pub fn is_overlap(
  range_1_start: u64,
  range_1_end: u64,
  range_2_start: u64,
  range_2_end: u64,
) -> bool {
  if range_1_end >= range_2_start && range_2_end >= range_1_start {
    return true;
  }
  false
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_is_overlap() {
    // Check for overlap.
    assert!(is_overlap(100, 200, 150, 180));
    assert!(is_overlap(100, 200, 150, 250));
    assert!(is_overlap(100, 200, 50, 150));

    // The ranges are inclusive.
    assert!(is_overlap(100, 200, 50, 100));
    assert!(is_overlap(100, 200, 200, 250));

    // Check for ranges that do not overlap.
    assert!(!is_overlap(100, 200, 50, 80));
    assert!(!is_overlap(100, 200, 250, 550));
  }
}

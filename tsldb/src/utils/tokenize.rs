use unicode_segmentation::UnicodeSegmentation;

pub fn tokenize(input: &str) -> Vec<&str> {
  let words = input.unicode_words().collect::<Vec<&str>>();

  words
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_tokenize() {
    let words = tokenize("a quick brown");
    assert_eq!(words, vec!["a", "quick", "brown"]);

    let words = tokenize("[notice]: this is last boarding call.");
    assert_eq!(
      words,
      vec!["notice", "this", "is", "last", "boarding", "call"]
    );

    let words = tokenize("He said, \"Hi, there!\"");
    assert_eq!(words, vec!["He", "said", "Hi", "there"]);

    let words = tokenize("sky:1 test:2 test:345");
    assert_eq!(words, vec!["sky:1", "test:2", "test:345"]);
  }
}

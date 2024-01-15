// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use unicode_segmentation::UnicodeSegmentation;

pub const FIELD_DELIMITER: char = '~';

// Tokenize a given string. Note that FIELD_DELIMITER is a special character that we
// use a field separator in queries, and so we do not tokenize on "~".
pub fn tokenize(input: &str) -> Vec<String> {
  // First, create different segments based on FIELD_DELIMITER separator.
  let segments: Vec<&str> = input.split(FIELD_DELIMITER).collect();

  let mut tokens: Vec<String> = Vec::new();
  for segment in segments {
    // Unicode tokenize each segment.
    let words: Vec<&str> = segment.unicode_words().collect();

    // Merge each segment, making sure to add the special delimiter FIELD_DELIMITER back at the end of each segment.
    // As an example, if the first segment is vec!["a", "b"], and the next segment is vec!["c", "d"], and the
    // field delimiter is "~", the tokens will be vec!["a", "b~c", "d"]
    for (pos, word) in words.iter().enumerate() {
      if pos == 0 && !tokens.is_empty() {
        let word_with_tilde = format!("{}{}", FIELD_DELIMITER, word);
        tokens.last_mut().unwrap().push_str(&word_with_tilde);
      } else {
        tokens.push(word.to_string());
      }
    }
  }

  tokens
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_tokenize() {
    let words = tokenize("a quick brown fox jumped 5.2 feet");
    assert_eq!(
      words,
      vec!["a", "quick", "brown", "fox", "jumped", "5.2", "feet"]
    );

    let words = tokenize("[notice]: this is last boarding call.");
    assert_eq!(
      words,
      vec!["notice", "this", "is", "last", "boarding", "call"]
    );

    let words = tokenize("He said, \"Hi, there!\"");
    assert_eq!(words, vec!["He", "said", "Hi", "there"]);

    // "~" appearing at the beginning of the text does get treated a seperator,
    // while the one appearing in between doesn't.
    let words = tokenize(&format!(
      "~sky:1 test{}2 thisisaword test{}345",
      FIELD_DELIMITER, FIELD_DELIMITER
    ));
    assert_eq!(
      words,
      vec![
        "sky",
        "1",
        &format!("test{}2", FIELD_DELIMITER),
        "thisisaword",
        &format!("test{}345", FIELD_DELIMITER)
      ]
    );

    let words = tokenize(&format!(
      "sky:1 test{}2 test{}345",
      FIELD_DELIMITER, FIELD_DELIMITER
    ));
    assert_eq!(
      words,
      vec![
        "sky",
        "1",
        &format!("test{}2", FIELD_DELIMITER),
        &format!("test{}345", FIELD_DELIMITER)
      ]
    );
  }
}

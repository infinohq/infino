// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use unicode_segmentation::UnicodeSegmentation;

pub const FIELD_DELIMITER: char = '~';

/// Tokenize input string, and add the tokens to the output vector.
/// The input string is assumed to not contain FIELD_DELIMITER, or the tokenization
/// is expected to not do any special processing on FIELD_DELIMITER.
///
/// This is an optimization typically useful during insertions (see LogMessage::get_terms),
/// where we do not want to tokenize on FIELD_DELIMITER.
pub fn tokenize_without_field_delimiter<'a>(input: &'a str, output: &mut Vec<&'a str>) {
  let tokens = input.unicode_words();
  output.extend(tokens);
}

/// Tokenize a given string. Note that FIELD_DELIMITER is a special character that we
/// use a field separator in queries, and so we do not tokenize on "~".
///
/// Note that for input such as "a~b c"
pub fn tokenize<'a>(input: &'a str, output: &mut Vec<&'a str>) {
  if !input.contains(FIELD_DELIMITER) {
    // Input does not have FIELD_DELIMITER, so just do unicode_words().
    // This improves performance in this hot method, as we do not need to split
    // the input on FIELD_DELIMITER.
    let tokens: Vec<&str> = input.unicode_words().collect();
    output.extend(tokens);
    return;
  }

  // Temporary storage for combining tokens with '~'
  let mut current_token: Option<&'a str> = None;

  // Iterate through the unicode words
  for word in input.unicode_words() {
    let start = word.as_ptr() as usize - input.as_ptr() as usize;
    let end = start + word.len();

    // Check if current_token is Some and should be combined with the current word
    if let Some(ct) = current_token {
      // Check if there's a '~' between the current token and the word
      if &input[start - 1..start] == "~" {
        let combined_start = ct.as_ptr() as usize - input.as_ptr() as usize;
        output.push(&input[combined_start..end]);
        current_token = None; // Reset current_token
        continue;
      } else {
        // Push the previous token if it's not followed by '~'
        output.push(ct);
        current_token = None;
      }
    }

    // If there's no '~' directly before this word, set current_token to Some
    if current_token.is_none() {
      current_token = Some(word);
    }
  }

  // Handle the last token
  if let Some(ct) = current_token {
    output.push(ct);
  }

  // Special handling for when the input itself ends with '~'
  if input.ends_with('~') && !output.is_empty() {
    let last = output.pop().unwrap(); // Safe because output is not empty
    let combined_end = last.as_ptr() as usize - input.as_ptr() as usize + last.len() + 1; // +1 for '~'
    output.push(&input[last.as_ptr() as usize - input.as_ptr() as usize..combined_end]);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_tokenize() {
    let mut words = Vec::new();
    tokenize("a quick brown fox jumped 5.2 feet", &mut words);
    assert_eq!(
      words,
      vec!["a", "quick", "brown", "fox", "jumped", "5.2", "feet"]
    );

    let mut words = Vec::new();
    tokenize("[notice]: this is last boarding call.", &mut words);
    assert_eq!(
      words,
      vec!["notice", "this", "is", "last", "boarding", "call"]
    );

    let mut words = Vec::new();
    tokenize("He said, \"Hi, there!\"", &mut words);
    assert_eq!(words, vec!["He", "said", "Hi", "there"]);

    // "~" appearing at the beginning of the text does get treated a seperator,
    // while the one appearing in between doesn't.
    let mut words = Vec::new();
    let input = &format!(
      "~sky:1 test{}2 thisisaword test{}345",
      FIELD_DELIMITER, FIELD_DELIMITER
    );
    tokenize(input, &mut words);
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

    let mut words = Vec::new();
    let input = &format!("sky:1 test{}2 test{}345", FIELD_DELIMITER, FIELD_DELIMITER);
    tokenize(input, &mut words);
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

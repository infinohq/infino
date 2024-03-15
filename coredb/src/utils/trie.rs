use std::collections::HashMap;

use super::tokenize::FIELD_DELIMITER;

/// TrieNode represents a node in the Trie data structure.
#[derive(Default, Debug)]
struct TrieNode {
  /// Indicates whether the node marks the end of a word in the Trie.
  is_end_of_word: bool,

  /// Stores the children nodes of the current node, indexed by character.
  children: HashMap<char, TrieNode>,
}

/// Trie is a trie data structure supporting insertion, containment check, and prefix search.
#[derive(Default, Debug)]
pub struct Trie {
  root: TrieNode,
}

#[allow(clippy::only_used_in_recursion)]
impl Trie {
  pub fn new() -> Self {
    Trie {
      root: TrieNode::default(),
    }
  }

  /// Inserts a word into the Trie.
  pub fn insert(&mut self, word: &str) {
    let mut current_node = &mut self.root;

    // Traverse the Trie, creating nodes for each character in the word.
    for c in word.chars() {
      current_node = current_node.children.entry(c).or_default();
    }

    // Mark the end of the word in the Trie.

    current_node.is_end_of_word = true;
  }

  /// Checks if a word is contained in the Trie.
  pub fn contains(&self, word: &str) -> bool {
    let mut current_node = &self.root;

    for c in word.chars() {
      match current_node.children.get(&c) {
        Some(node) => current_node = node,
        None => return false,
      }
    }

    current_node.is_end_of_word
  }

  /// Retrieves all terms in the Trie with a given prefix.
  ///
  /// # Arguments
  ///
  /// - `prefix`: The prefix to search for in the Trie.
  /// - `case_insensitive`: A flag indicating whether the search should be case-insensitive.
  ///
  /// # Logic:
  ///
  /// - If `case_insensitive` is false:
  ///   - Perform a regular search, traversing the Trie based on the characters in the prefix.
  ///
  /// - If `case_insensitive` is true:
  ///   - Check if the `prefix` contains FIELD_DELIMITER.
  ///     - If yes:
  ///       - Perform a case-sensitive search until the FIELD_DELIMITER is encountered, because fields cannot be case-insensitive
  ///       - After encountering FIELD_DELIMITER, the search becomes case-insensitive for the remaining characters.
  ///     - If no:
  ///       - Perform a case-insensitive search for the entire prefix.
  ///
  pub fn get_terms_with_prefix(&self, prefix: &str, case_insensitive: bool) -> Vec<String> {
    let mut result = Vec::new();
    let mut current_node = &self.root;

    if case_insensitive {
      // Check if the prefix contains FIELD_DELIMITER for case-insensitive search
      if let Some(index) = prefix.chars().position(|c| c == FIELD_DELIMITER) {
        // Traverse the trie until the FIELD_DELIMITER
        for c in prefix[..=index].chars() {
          match current_node.children.get(&c) {
            Some(node) => current_node = node,
            None => return result,
          }
        }

        // Call search_case_insensitive_prefix with remaining characters post FIELD_DELIMITER
        self.search_case_insensitive_prefix(
          current_node,
          &prefix[index + 1..],
          &prefix[..=index],
          &mut result,
        );

        return result;
      } else {
        // If Delimiter is not present, then perform regular prefix search
        self.search_case_insensitive_prefix(current_node, prefix, "", &mut result);

        return result;
      }
    }

    // If case_insensitive is false, perform a regular search
    for c in prefix.chars() {
      match current_node.children.get(&c) {
        Some(node) => current_node = node,
        None => return result,
      }
    }

    // Directly call collect_terms_with_prefix for the remaining trie
    self.collect_terms_with_prefix(current_node, prefix, &mut result);

    result
  }

  /// Recursively searches the Trie for terms with a case-insensitive prefix.
  ///
  /// This method is specifically designed for case-insensitive searches until
  /// the entire given prefix is traversed. It explores both lowercase and uppercase
  /// paths in the Trie to accommodate case variations in the search prefix.
  ///
  /// # Arguments
  ///
  /// - `node`: The current TrieNode being traversed during recursion.
  /// - `remaining_chars`: The remaining characters in the search prefix.
  /// - `current_prefix`: The prefix accumulated during the recursive traversal.
  /// - `result`: The vector to collect terms that match the case-insensitive prefix.

  fn search_case_insensitive_prefix(
    &self,
    node: &TrieNode,
    remaining_chars: &str,
    current_prefix: &str,
    result: &mut Vec<String>,
  ) {
    if remaining_chars.is_empty() {
      // If no more characters in the prefix, collect terms with the current prefix.
      self.collect_terms_with_prefix(node, current_prefix, result);
      return;
    }

    let current_char = remaining_chars.chars().next().unwrap();

    // Convert the current character to lowercase and uppercase for case-insensitive search.
    let lowercase_char = current_char.to_ascii_lowercase();
    let uppercase_char = current_char.to_ascii_uppercase();

    // Check and traverse the Trie for both lowercase and uppercase characters.
    if let Some(lowercase_node) = node.children.get(&lowercase_char) {
      self.search_case_insensitive_prefix(
        lowercase_node,
        &remaining_chars[1..],
        &(current_prefix.to_string() + &lowercase_char.to_string()),
        result,
      );
    }

    if let Some(uppercase_node) = node.children.get(&uppercase_char) {
      self.search_case_insensitive_prefix(
        uppercase_node,
        &remaining_chars[1..],
        &(current_prefix.to_string() + &uppercase_char.to_string()),
        result,
      );
    }
  }

  /// Recursively collects all terms in the Trie with a given prefix.
  fn collect_terms_with_prefix(&self, node: &TrieNode, prefix: &str, result: &mut Vec<String>) {
    if node.is_end_of_word {
      // If the node marks the end of a word, add it to the result.

      result.push(prefix.to_string());
    }

    // Recursively traverse child nodes and collect terms with the updated prefix.
    for (child_char, child_node) in &node.children {
      // Skip collecting terms if the child_char is FIELD_DELIMITER.
      if *child_char != FIELD_DELIMITER {
        let mut next_prefix = prefix.to_string();
        next_prefix.push(*child_char);
        self.collect_terms_with_prefix(child_node, &next_prefix, result);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Helper function to assert that two vectors contain the same elements, regardless of order.
  fn assert_contains_same_elements(actual: Vec<String>, expected: Vec<&str>) {
    assert_eq!(actual.len(), expected.len(), "Vector lengths do not match");

    for element in expected {
      assert!(
        actual.contains(&element.to_string()),
        "Element {} not found in vector",
        element
      );
    }
  }

  #[test]
  fn test_insert_and_contains() {
    let mut trie = Trie::new();

    // Insert words into the trie
    trie.insert("apple");
    trie.insert("app");
    trie.insert("banana");

    // Test word containment
    assert!(trie.contains("apple"));
    assert!(trie.contains("app"));
    assert!(trie.contains("banana"));

    // Test non-existent words
    assert!(!trie.contains("apricot"));
    assert!(!trie.contains("ban"));
  }

  #[test]
  fn test_prefix_search_case_sensitive() {
    let mut trie = Trie::new();

    // Insert words into the trie
    trie.insert("apple");
    trie.insert("aPple");
    trie.insert("app");
    trie.insert("banana");
    trie.insert("apricot");
    trie.insert("app~delimiter");

    // Test prefix search
    let result: Vec<String> = trie.get_terms_with_prefix("app", false);
    let expected_result = vec!["app", "apple"];

    assert_contains_same_elements(result, expected_result);
  }

  #[test]
  fn test_prefix_search_case_insensitive() {
    let mut trie = Trie::new();

    // Insert words into the trie
    trie.insert("apple");
    trie.insert("app");
    trie.insert("aPpLe");
    trie.insert("banana");
    trie.insert("apricot");

    // Test case-insensitive prefix search
    let result: Vec<String> = trie.get_terms_with_prefix("Ap", true);
    let expected_result = vec!["app", "apple", "apricot", "aPpLe"];

    assert_contains_same_elements(result, expected_result);
  }

  #[test]
  fn test_prefix_search_case_insensitive_and_delimiter() {
    let mut trie = Trie::new();

    // Insert words into the trie with Delimiter
    trie.insert("fruit~apple");
    trie.insert("FRUIT~app");
    trie.insert("fruit~aPpLe");
    trie.insert("fruit~banana");
    trie.insert("FRUIT~apricot");

    // Test case-insensitive prefix search with Delimiter
    let result: Vec<String> = trie.get_terms_with_prefix("fruit~Ap", true);
    let expected_result = vec!["fruit~apple", "fruit~aPpLe"];

    assert_contains_same_elements(result, expected_result);
  }
}

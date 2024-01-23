// This code is licensed under Elastic License 2.0
// https://www.elastic.co/licensing/elastic-license

use object_store::path::DELIMITER;

/// Join a directory path with file path. For example, if the
/// directory path is "a/b", delimiter is "/", and file path is "c",
/// this will return "a/b/c".
pub fn get_joined_path(dir_path: &str, file_path: &str) -> String {
  let joined_path = format!("{}{}{}", dir_path, DELIMITER, file_path);
  joined_path
}

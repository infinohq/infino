use std::path::Path;

/// Join a directory path with file path. For example, if the
/// directory path is "a/b" and file path is "c", this will return
/// "a/b/c" on Linux.
pub fn get_joined_path(dir_path: &str, file_path: &str) -> String {
  let path_buf = Path::new(dir_path).join(file_path);
  let joined_path = String::from(path_buf.to_str().unwrap());
  joined_path
}

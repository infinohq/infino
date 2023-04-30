use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::process::{Child, Command};

use fs_extra::dir::get_size;

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
  P: AsRef<Path>,
{
  let file = File::open(filename)?;
  Ok(io::BufReader::new(file).lines())
}

// The function takes directory path and return the fize of the directory
pub fn get_directory_size(directory_path: &str) -> u64 {
  let output = Command::new("ls")
    .arg("-R")
    .arg("-sh")
    .arg(directory_path)
    .output()
    .expect("failed to execute ls command");
  println!("Output of ls on tantivy index directory {:?}", output);

  let folder_size = get_size(directory_path).unwrap();
  return folder_size;
}

pub fn run_cargo_in_dir(dir_path: &str, package_name: &str) -> std::io::Result<Child> {
  Command::new("cargo")
    .arg("run")
    .arg("-r")
    .arg("-p")
    .arg(package_name)
    .current_dir(dir_path)
    .spawn()
}

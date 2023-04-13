use std::process::Command;

/// Start a docker container with the given name and image.
pub fn start_docker_container(
  name: &str,
  image_name: &str,
  image_tag: &str,
  extra_args: &[&str],
) -> Result<(), std::io::Error> {
  // Build the Docker run command
  let mut command = Command::new("docker");
  command
    .arg("run")
    .arg("-d")
    .arg("--name")
    .arg(name)
    .args(extra_args)
    .arg(format!("{}:{}", image_name, image_tag));

  println!("Running command: {:?}", command);

  // Run the Docker command
  let output = command.output()?;
  if !output.status.success() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      format!("Failed to start Docker container: {:?}", output),
    ));
  }

  Ok(())
}

/// Execute given command in the container with the given name.
pub fn exec_command(name: &str, args: &[&str]) -> Result<(), std::io::Error> {
  let mut command = Command::new("docker");
  command.arg("exec").arg(name).args(args);

  let output = command.output()?;
  if !output.status.success() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      format!("Failed to run docker exec command: {:?}", output),
    ));
  }

  Ok(())
}

/// Stop the docker container of given name.
pub fn stop_docker_container(name: &str) -> Result<(), std::io::Error> {
  // Build the Docker run command
  let mut command = Command::new("docker");
  command.arg("stop").arg(name);

  // Run the Docker command
  let output = command.output()?;
  if !output.status.success() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      format!("Failed to stop Docker container: {:?}", output),
    ));
  }

  Ok(())
}

/// Remove the docker container of the given name.
pub fn remove_docker_container(name: &str) -> Result<(), std::io::Error> {
  // Build the Docker run command
  let mut command = Command::new("docker");
  command.arg("container").arg("rm").arg(name);

  // Run the Docker command
  let output = command.output()?;
  if !output.status.success() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      format!("Failed to stop Docker container: {:?}", output),
    ));
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_commands() {
    let container_name = "infino-docker-test";

    // Previous failed test runs may leave this container around. Remove the docker container and ignore
    // any error as it is ok to proceed if the removal failed (likely due to container not existing already).
    let _ = remove_docker_container(container_name);

    let start_output = start_docker_container(container_name, "rabbitmq", "latest", &[]);
    if start_output.is_err() {
      println!("Dokcer start output: {:?}", start_output);
    }
    assert!(start_output.is_ok());

    let exec_output = exec_command(
      container_name,
      &["rabbitmq-plugins", "enable", "rabbitmq_stream"],
    );
    if exec_output.is_err() {
      println!("Dokcer start output: {:?}", exec_output);
    }
    assert!(exec_output.is_ok());

    let stop_output = stop_docker_container(container_name);
    if stop_output.is_err() {
      println!("Dokcer stop output: {:?}", stop_output);
    }
    assert!(stop_output.is_ok());

    let remove_output = remove_docker_container(container_name);
    if remove_output.is_err() {
      println!("Dokcer remove output: {:?}", remove_output);
    }
    assert!(remove_output.is_ok());
  }

  #[test]
  fn test_start_error() {
    let start_output =
      start_docker_container("does-not-exist", "__does-not-exist__", "latest", &[]);
    assert!(start_output.is_err());
  }

  #[test]
  fn test_stop_error() {
    let start_output = stop_docker_container("does-not-exist");
    assert!(start_output.is_err());
  }

  #[test]
  fn test_remove_error() {
    let remove_output = remove_docker_container("does-not-exist");
    assert!(remove_output.is_err());
  }
}

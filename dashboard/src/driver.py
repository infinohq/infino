from datetime import datetime
import os
import time
import re
import docker

from infinopy import InfinoClient


def start_infino():
    """
    Starts Infino container on port 3000.
    """

    # Set up the Docker client
    docker_client = docker.from_env()

    # Start the Infino server container
    container = docker_client.containers.run(
        "infinohq/infino:latest",
        detach=True,
        ports={"3000/tcp": 3000},
    )

    # Wait for the server to start
    time.sleep(10)

    # Set the base URL for the client
    os.environ["INFINO_BASE_URL"] = "http://localhost:3000"

    return container


def stop_infino(container):
    """
    Stops and removes Infino container.
    """
    container.stop()
    container.remove()


def publish_logs(client):
    """
    Publishes Apache logs to Infino
    """
    log_file = "../benches/data/Apache.log"
    with open(log_file, "r") as file:
        lines = file.readlines()

    logs_batch = []
    batch_size = 10000

    for i, line in enumerate(lines):
        # Define a regular expression pattern for parsing log lines
        log_pattern = re.compile(r"\[(.+?)\] \[(.+?)\] (.+)")

        # Use the pattern to match the log line
        match = log_pattern.match(line)

        if match:
            # Extract matched groups
            log_datetime_str, log_level, log_message = match.groups()

            # Parse the datetime string into a datetime object
            log_datetime = datetime.strptime(log_datetime_str, "%a %b %d %H:%M:%S %Y")
            timestamp = log_datetime.timestamp()
            date_isoformat = log_datetime.date().isoformat()

            # Create a dictionary representing the log entry
            log_entry = {
                "date": timestamp,
                "date_isoformat": date_isoformat,
                "log_level": log_level,
                "message": log_message,
            }

            # Append the log entry to the list
            logs_batch.append(log_entry)

        if (i + 1) % batch_size == 0 or i == len(lines) - 1:
            response = client.append_log(logs_batch)
            assert response.status_code == 200
            logs_batch = []


if __name__ == "__main__":
    # Start Infino server
    print("Starting Infino server...")
    container = start_infino()

    # Set up the Infino client
    print("Creating client...")
    client = InfinoClient()

    # Publish Apache log data
    print("Publishing logs...")
    publish_logs(client)

    # Display dashboard

    # Shutdown Infino server
    input(
        "Press enter when you are done viewing the dashboard and want to shut down Infino..."
    )
    print("Now shutting down Infino...")
    stop_infino(container)

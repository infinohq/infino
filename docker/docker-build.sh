#!/bin/bash
DOCKER_PATH=$(which docker)
if [ -z "$DOCKER_PATH" ]; then
  echo "Docker not found. Make sure Docker is installed and available in the PATH."
  exit 1
fi

$DOCKER_PATH build -t infinohq/infino:latest -f docker/infino.dockerfile .

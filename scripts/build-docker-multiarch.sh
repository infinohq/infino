#!/bin/bash

# Check if we are in the project root while executing this script
if [ ! -f './docker/infino.dockerfile' ]; then
    echo "Execute this script from the root of the project."
    exit 1
fi


# Default values
docker_img_tag="infinohq/infino:latest"

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --docker-img-tag) docker_img_tag="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo -n "Do you have push access to $docker_img_tag? (y/n) "
read answer

# Convert response to lowercase
answer=$(echo "$answer" | tr '[:upper:]' '[:lower:]')

if [ "$answer" != "y" ]; then
    echo "You need push access to the repo $docker_img_tag to build multi-arch images. Exiting."
    exit 0
else
    docker buildx create --use
    if [ "$?" -eq 0 ]; then
        #docker buildx build --platform linux/arm64,linux/amd64 --push -t $docker_img_tag -f docker/infino.dockerfile .
        status=$(docker buildx build --platform linux/arm64,linux/amd64 --push -t $docker_img_tag -f docker/infino.dockerfile .)
        docker buildx rm
        exit $status
    else
        echo "Failed to create docker buildx instance. Exiting."
        exit 1
    fi
fi
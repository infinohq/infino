#!/bin/bash

# Function to compare versions
version_lt() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1"; }

# Path to your Java source files
java_source_path="src/main/org/opensearch/infino/*.java"
test_source_path="src/test/org/opensearch/infino/*.java"

# List of supported OpenSearch versions
# TODO: Support these versions "2.5.0" "2.4.1" "2.4.0" "2.3.0" "2.2.1" "2.2.0" "2.1.0" "2.0.1" "2.0.0" "1.3.14" "1.3.13" "1.3.12" "1.3.11" "1.3.10" "1.3.9" "1.3.8" "1.3.7" "1.3.6" "1.3.5" "1.3.4" "1.3.3" "1.3.2" "1.3.1" "1.3.0" "1.2.4" "1.2.3" "1.2.0" "1.1.0" "1.0.1" "1.0.0")
versions=("2.11.1" "2.11.0" "2.10.0" "2.9.0" "2.8.0" "2.7.0" "2.6.0")

for version in "${versions[@]}"
do
    echo "Building for OpenSearch version $version"

    # Run Gradle build
    ./gradlew build -Dopensearch.version=$version

    # Export the version as an environment variable
    export OPENSEARCH_VERSION=$version

    # Run Gradle test
    ./gradlew test -Dopensearch.version=$version
done

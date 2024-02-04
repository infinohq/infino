# CoreDB

CoreDB is a telemetry database designed for efficient storage and retrieval of time-series data. It utilizes time-sharded segments and compressed blocks for data storage, implementing indexing for fast data retrieval.

## Components
- **Index Manager**: Manages indexing for fast data retrieval.

- **Log Module**: Handles log messages and provides functionality for appending and searching logs.

- **Metric Module**: Manages metric points, allowing the addition and retrieval of metric data.

- **Storage Manager**: Handles storage-related operations, including reading and writing to storage.

- **Policy Manager**: Manages retention policies for data stored in CoreDB.

- **Request Manager**: Manages incoming requests and coordinates various components for seamless operation.

- **Segment Manager**: Handles the organization and retrieval of time-sharded segments.

## Setting up

To set up CoreDB, follow these steps:

1. **Configuration**:
    - Create a configuration file using the provided [template](../config/default.toml).
    - Specify the directory path for storing indices, default index name, segment size threshold, memory budget, retention period, and storage type.

2. **Environment Variables**:
    - Refer to the [environment variables documentation](./env-vars.md) for a list of required environment variables and their descriptions.

3. **Dependencies**:
    - Ensure that required dependencies are installed.

4. **Build**:
    - Use the provided [Makefile](./Makefile) to build CoreDB. The Makefile supports both debug and release modes.
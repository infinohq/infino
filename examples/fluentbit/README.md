# FluentBit Example

This example shows how to use FluentBit to publish log messages and time series data points to Infino.

## Log Messages

### Using FluentBit docker image

We'll be using four shell windows: (1) for running the Infino server, (2) for running a `fluentbit` docker container to push logs to Infino server, (3) for running `fluentbit` docker container to push metrics to Infino server, and (4) for executing searches against Infino server.

* Start Infino server in debug mode in one shell window - run in the infino directory `RUST_LOG=debug make run`

* In the second shell window, run the following command to push **log messages** to Infino. This command starts fluentbit, parses `datasets/Apache_2k.log` and sends the log messages to the Infino server for indexing.
```docker run -ti --mount type=bind,source=`pwd`/examples/fluentbit/apache-log.conf,target=/fluent-bit/etc/fluent-bit.conf --mount type=bind,source=`pwd`/examples/fluentbit/parsers.conf,target=/fluent-bit/etc/parsers.conf --mount type=bind,source=`pwd`/examples/datasets/Apache_2k.log,target=/fluent-bit/etc/Apache_2k.log fluent/fluent-bit```

As this command is running, you'll see the log messages being indexed by Infino in the Infino server window.

* [TODO: add details about using fluentbit for time series]

* Shutdown the Infino server gracefully by pressing `Ctrl-C` in the Infino server window, and shutdown the fluentbit docker container by pressing `Ctrl-C` in the both the fluentbit shell windows.

# FluentBit Example

This example shows how to use FluentBit to publish log messages and time series to Infino.

## Using FluentBit docker image

We'll be using four shell windows: (1) for running the Infino server, (2) for running a `fluentbit` docker container to push logs to Infino server, (3) for running `fluentbit` docker container to push metrics to Infino server, and (4) for executing searches against Infino server.

* Start Infino server in debug mode in one shell window - run in the infino directory `RUST_LOG=debug make run`

* In the second shell window, run the following command to push **log messages** to Infino. This command starts fluentbit, parses `datasets/Apache_2k.log` and sends the log messages to the Infino server for indexing. The various mounts in the docker command are needed so that the container has access to the fluentbit configuration and the log dataset. As this command is running, you'll see the log messages being indexed by Infino in the Infino server window.

  ```docker run -ti --mount type=bind,source=`pwd`/examples/fluentbit/apache-log.conf,target=/fluent-bit/etc/fluent-bit.conf --mount type=bind,source=`pwd`/examples/fluentbit/parsers.conf,target=/fluent-bit/etc/parsers.conf --mount type=bind,source=`pwd`/examples/datasets/Apache_2k.log,target=/fluent-bit/etc/Apache_2k.log fluent/fluent-bit```


* In the third shell window, run the following command to put **time series** to Infino. This command pushes the CPU metrics from docker host to Infino
for indexing. The mount in the docker command are needed so that the container has access to the fluentbit configuration. As this command is running, you'll see the CPU metrics being indexed by Infino in the Infino server window.

  ```docker run -ti --mount type=bind,source=`pwd`/examples/fluentbit/time-series-cpu.conf,target=/fluent-bit/etc/fluent-bit.conf fluent/fluent-bit```


* In the fourth shell window, try searching for logs and metrics:
  * search logs by calling `search_log` api, example below. You can change the `text` parameter to try different queries. The end time,
if not specified, is assumed to be the current time.
  ```curl "http://localhost:3000/search_log?text=workerenv%20error&start_time=0"```
  * search time series by calling `search_ts` api, example below. You can change the `label_value` parameter to try different metrics.
The end time, if not specified, is assumed to be the current time.
  ```curl "http://localhost:3000/search_ts?label_name=__name__&&label_value=cpu_p&start_time=0"```

* Shutdown the Infino server gracefully by pressing `Ctrl-C` in the Infino server window, and shutdown the fluentbit docker container by pressing `Ctrl-C` in the both the fluentbit shell windows.

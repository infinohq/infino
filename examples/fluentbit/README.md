# FluentBit Example

This example shows how to use FluentBit to publish log messages and time series data points to Infino.

## Log Messages

### Using FluentBit docker image

* Start infino server
* Run the following command to push log messages to Infino:

```docker run -ti --mount type=bind,source=`pwd`/examples/fluentbit/apache-log.conf,target=/fluent-bit/etc/fluent-bit.conf --mount type=bind,source=`pwd`/examples/fluentbit/parsers.conf,target=/fluent-bit/etc/parsers.conf --mount type=bind,source=`pwd`/examples/datasets/Apache_2k.log,target=/fluent-bit/etc/Apache_2k.log fluent/fluent-bit```

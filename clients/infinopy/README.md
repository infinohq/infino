
<h1 align="center">
   InfinoPy
</h1>
<p align="center">
  Python Client for Infino, a scalable telemetry store. </br>
</p>


## Quick Start

`infinopy` is a python client for Infino. Infino is a scalable telemetry store to reduce the cost and complexity of observability. If you haven't explored Infino yet, please refer to the [Infino git repo](https://github.com/infinohq/infino).


### Install InfinoPy Client
```bash
pip install infinopy
```

### Notes for testing
You should ensure your environment is up-to-date if you want to edit/test the client: [OpenSSL3](https://ports.macports.org/port/openssl/), [Python3](https://www.python.org/downloads/macos/), Virtualenv, Setuptools. If you've changed any of the core Infino code you'll also need to run **make docker-build** to build the docker image that the infino server needs to run in for unit tests.

### Example

The documentation is still in progress. In the meantime, [this test](https://github.com/infinohq/infino/blob/python-client/clients/python/infino/tests/test_infino.py) illustrates how to use InfinoPy - the Python client for Infino.

<h1 align="center">
    InfinoPy - Infino Python Client
</h1>
<p align="center">
    InfinoPy - Python Client for Infino, a scalable telemetry store.
</p>

If you haven't explored Infino yet, please refer to:
 - [Infino git repo](https://github.com/infinohq/infino)

## Quick Start

`infinopy` is a python client for Infino. Infino is a scalable telemetry store to reduce the cost and complexity of observability. 


### Notes for MacOS 
If you're on MacOS you might want to ensure your environment is up-to-date.

**Install [OpenSSL3](https://ports.macports.org/port/openssl/)**
**Install [Python3](https://www.python.org/downloads/macos/)**

**install virtualenv and setuptools**
```bash
pip install virtualenv
pip install setuptools
```

**Remove the public version of infinopy from virtual env**
cd venv/lib/pythonXX/site-packages
pip uninstall infinopy
pip install -e /path/to/infino/clients/infinopy directory (allows for editing via links)


### Installation
Once you are sure you environment is up-to-date, do the following:
```bash
pip install infinopy
```

### Example

The documentation is still in progress. In the meantime, [this test](https://github.com/infinohq/infino/blob/python-client/clients/python/infino/tests/test_infino.py) illustrates how to use InfinoPy - the Python client for Infino.
import os

from infinopy import InfinoClient


def test_base_url():
    # Check default base url
    client = InfinoClient()
    assert client.get_base_url() == "http://localhost:3000"

    # Check the base url when env var INFINO_BASE_URL is set
    value = "http://SOME_ENV_URL"
    os.environ["INFINO_BASE_URL"] = value
    client = InfinoClient()
    assert client.get_base_url() == value

    # Check the base url when client is explicitly passed one
    value = "http://SOME_OTHER_URL"
    client = InfinoClient(value)
    assert client.get_base_url() == value

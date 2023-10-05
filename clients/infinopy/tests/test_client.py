import os
import unittest
import time
import docker
from infinopy import InfinoClient


class InfinoClientTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # Set up the Docker client
        self.docker_client = docker.from_env()

        # Start the Infino server container
        self.container = self.docker_client.containers.run(
            "infinohq/infino:latest",
            detach=True,
            ports={"3000/tcp": 3000},
        )

        # Wait for the server to start
        time.sleep(10)

        # Set the base URL for the client
        os.environ["INFINO_BASE_URL"] = "http://localhost:3000"

        # Set up the client for testing
        self.client = InfinoClient()

    @classmethod
    def tearDownClass(self):
        # Stop and remove the server container
        self.container.stop()
        self.container.remove()

    def test_ping(self):
        # Test the ping method
        response = self.client.ping()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "OK")

    def test_log(self):
        current_time = int(time.time())

        # Test the append_log method
        payload = {"date": current_time, "message": "my message one"}
        response = self.client.append_log(payload)
        self.assertEqual(response.status_code, 200)

        payload = {"date": current_time, "message": "my message two"}
        response = self.client.append_log(payload)
        self.assertEqual(response.status_code, 200)

        # Test the search_log method.
        # Query for text that is present in both the payloads.
        response = self.client.search_log(
            text="my message", start_time=current_time - 10, end_time=current_time + 10
        )
        self.assertEqual(response.status_code, 200)
        results = response.json()
        self.assertEqual(len(results), 2)

        # Query for text that is present in only one payload.
        response = self.client.search_log(
            text="two", start_time=current_time - 10, end_time=current_time + 10
        )
        self.assertEqual(response.status_code, 200)
        results = response.json()
        self.assertEqual(len(results), 1)

        # Test that default values for start and end time work.
        response = self.client.search_log(text="my message")
        self.assertEqual(response.status_code, 200)
        results = response.json()
        self.assertEqual(len(results), 2)

    def test_ts(self):
        current_time = int(time.time())

        # Test the append_ts method.
        payload = {"date": current_time, "some_metric_name": 1.0}
        response = self.client.append_ts(payload)
        self.assertEqual(response.status_code, 200)

        payload = {"date": current_time + 1, "some_metric_name": 0.0}
        response = self.client.append_ts(payload)
        self.assertEqual(response.status_code, 200)

        # Test the search_ts method.
        response = self.client.search_ts(
            label_name="__name__",
            label_value="some_metric_name",
            start_time=current_time - 10,
            end_time=current_time + 10,
        )
        self.assertEqual(response.status_code, 200)
        results = response.json()
        self.assertEqual(len(results), 2)

        # Test that default values for start and end time work.
        response = self.client.search_ts(
            label_name="__name__",
            label_value="some_metric_name",
        )
        self.assertEqual(response.status_code, 200)
        results = response.json()
        self.assertEqual(len(results), 2)

    def test_get_index_dir(self):
        # Test the get_index_dir method.
        response = self.client.get_index_dir()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "index")

    def test_base_url(self):
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


if __name__ == "__main__":
    unittest.main()

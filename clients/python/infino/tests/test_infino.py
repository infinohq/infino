import os
import unittest
import time
import docker
from infino.client import InfinoClient

class InfinoClientTestCase(unittest.TestCase):
    def setUp(self):
        # Set up the Docker client
        self.docker_client = docker.from_env()

        # Start the Infino server container
        self.container = self.docker_client.containers.run(
            "infinohq/infino:latest",
            detach=True,
            ports={"3000": 3000},
        )

        # Wait for the server to start
        time.sleep(10)

        # Set the base URL for the client
        os.environ["INFINO_BASE_URL"] = "http://localhost:3000"

        # Set up the client for testing
        self.client = InfinoClient()

    def tearDown(self):
        # Stop and remove the server container
        self.container.stop()
        self.container.remove()
    
    def test_append_log(self):
        # Test the append_log method
        payload = {"data": "example"}
        response = self.client.append_log(payload)
        self.assertEqual(response, {"message": "Log appended successfully"})

    def test_search_log(self):
        # Test the search_log method
        query = "example"
        response = self.client.search_log(query)
        self.assertEqual(response, {"results": [...]})  # Assert expected results

    def test_get_index_dir(self):
        # Test the get_index_dir method
        response = self.client.get_index_dir()
        self.assertEqual(response, {"index_dir": "/path/to/index"})

if __name__ == "__main__":
    unittest.main()

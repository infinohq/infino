import os
import requests

class InfinoClient:
    def __init__(self):
        self.base_url = os.environ.get("INFINO_BASE_URL", "http://localhost:3000")

    def _request(self, method, path, params=None, json=None):
        url = self.base_url + path
        response = requests.request(method, url, params=params, json=json)
        return response.json()

    def append_log(self, payload):
        path = "/append_log"
        return self._request("POST", path, json=payload)

    def append_ts(self, payload):
        path = "/append_ts"
        return self._request("POST", path, json=payload)

    def search_log(self, query):
        path = "/search_log"
        params = {"query": query}
        return self._request("GET", path, params=params)

    def search_ts(self, query):
        path = "/search_ts"
        params = {"query": query}
        return self._request("GET", path, params=params)

    def get_index_dir(self):
        path = "/get_index_dir"
        return self._request("GET", path)

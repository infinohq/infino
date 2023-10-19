import os
import requests


class InfinoClient:
    def __init__(self, base_url=None):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = os.environ.get("INFINO_BASE_URL", "http://localhost:3000")

    def _request(self, method, path, params=None, json=None):
        url = self.base_url + path
        response = requests.request(method, url, params=params, json=json)
        return response

    def ping(self):
        path = "/ping"
        return self._request("GET", path)

    def append_log(self, payload):
        path = "/append_log"
        return self._request("POST", path, json=payload)

    def append_ts(self, payload):
        path = "/append_ts"
        return self._request("POST", path, json=payload)

    def search_log(self, text, start_time=None, end_time=None):
        path = "/search_log"
        params = {"text": text, "start_time": start_time, "end_time": end_time}
        response = self._request("GET", path, params=params)
        return response

    def search_ts(self, label_name, label_value, start_time=None, end_time=None):
        path = "/search_ts"
        params = {
            "label_name": label_name,
            "label_value": label_value,
            "start_time": start_time,
            "end_time": end_time,
        }
        return self._request("GET", path, params=params)

    def get_index_dir(self):
        path = "/get_index_dir"
        return self._request("GET", path)

    def get_base_url(self):
        return self.base_url

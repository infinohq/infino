import os
import requests

class InfinoClient:
    def __init__(self, base_url=None):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = os.environ.get("INFINO_BASE_URL", "https://localhost:3000")

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

    # Deprecated. Kept here for backwards compatibility. TODO: Remove by Jan 2024.
    def append_ts(self, payload):
        return self.append_metric(payload)
    
    def append_metric(self, payload):
        path = "/append_metric"
        return self._request("POST", path, json=payload)
    
    # Deprecated. Kept here for backwards compatibility. TODO: Remove by Jan 2024.
    def search_log(self, text, start_time=None, end_time=None):
        return self.search_logs(text, start_time, end_time)
    
    def search_logs(self, text, start_time=None, end_time=None):
        path = "/search_logs"
        params = {"text": text, "start_time": start_time, "end_time": end_time}
        response = self._request("GET", path, params=params)
        return response

    def summarize(self, text, start_time=None, end_time=None):
        path = "/summarize"
        params = {"text": text, "start_time": start_time, "end_time": end_time}
        response = self._request("GET", path, params=params)
        return response

    # Deprecated. Kept here for backwards compatibility. TODO: Remove by Jan 2024.
    def search_ts(self, label_name, label_value, start_time=None, end_time=None):
        return self.search_metrics(label_name, label_value, start_time, end_time)
    
    def search_metrics(self, label_name, label_value, start_time=None, end_time=None):
        path = "/search_metrics"
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

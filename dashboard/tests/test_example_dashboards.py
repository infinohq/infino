import subprocess
import time

import pytest


@pytest.fixture(
    params=["src/example_logs_dashboard.py", "src/example_metrics_dashboard.py"]
)
def dashboard_file(request):
    return request.param


def test_dashboard(dashboard_file):
    """
    This test only checks that the dashboard script runs without any errors. The actual output on the dashboard isn't checked.
    """
    PORT = "8699"

    p = subprocess.Popen(
        [
            "streamlit",
            "run",
            dashboard_file,
            "--server.port",
            PORT,
            "--server.headless",
            "true",
        ],
    )

    # Wait for the Streamlit app to start (you might need to adjust this time)
    time.sleep(2)

    # Assert that the streamlit process is still running
    process_is_running = p.poll() is None
    assert process_is_running

    # Terminate the Streamlit process
    p.terminate()

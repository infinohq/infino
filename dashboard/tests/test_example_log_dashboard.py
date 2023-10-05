import subprocess
import time


def test_logs_dashboard():
    """
    This test only checks that the dashboard script runs without any errors. The actual output on the dashboard isn't checked.
    """
    PORT = "8699"

    p = subprocess.Popen(
        [
            "streamlit",
            "run",
            "src/example_log_dashboard.py",
            "--server.port",
            PORT,
            "--server.headless",
            "true",
        ],
    )

    # Wait for the Streamlit app to start (you might need to adjust this time)
    time.sleep(2)

    # Terminate the Streamlit process
    p.terminate()

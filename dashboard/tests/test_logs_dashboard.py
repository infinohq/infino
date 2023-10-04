import subprocess


def test_logs_dashboard():
    PORT = "8699"

    p = subprocess.Popen(
        [
            "streamlit",
            "run",
            "src/logs_dashboard.py",
            "--server.port",
            PORT,
            # "--server.headless",
            # "true",
        ]
    )

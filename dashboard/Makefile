venv:
	python3 -m venv venv

build:
	. venv/bin/activate; pip-compile -o requirements.txt; pip install -r requirements.txt

check:
	black src tests

logs-dashboard:
	. venv/bin/activate; streamlit run src/example_logs_dashboard.py

metrics-dashboard:
	. venv/bin/activate; streamlit run src/example_metrics_dashboard.py

run-driver: build
	. venv/bin/activate; python src/driver.py

test: build
	. venv/bin/activate; export PYTHONPATH=./src:${PYTHONPATH}; pytest -o log_cli=true tests

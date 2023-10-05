venv:
	python3 -m venv venv

build:
	. venv/bin/activate; pip-compile -o requirements.txt; pip install -r requirements.txt

test: build
	. venv/bin/activate; export PYTHONPATH=./src:${PYTHONPATH}; pytest -o log_cli=true tests

check:
	black src tests

logs-dashboard:
	. venv/bin/activate; streamlit run src/example_log_dashboard.py

run-driver: build
	. venv/bin/activate; python src/driver.py

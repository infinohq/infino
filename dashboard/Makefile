venv:
	python3 -m venv venv

build:
	. venv/bin/activate; pip-compile -o requirements.txt; pip install -r requirements.txt

test:
	. venv/bin/activate; export PYTHONPATH=./src:${PYTHONPATH}; pytest -o log_cli=true tests

check:
	black src tests

run_driver:
	. venv/bin/activate; python src/driver.py

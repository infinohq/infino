venv:
	python3 -m venv venv

build:
	. venv/bin/activate; cd venv; pip-compile -o ../requirements.txt; pip install -r ../requirements.txt; cd ..

check:
	black src tests

logs-dashboard:
	. venv/bin/activate; streamlit run src/example_logs_dashboard.py

metrics-dashboard:
	. venv/bin/activate; streamlit run src/example_metrics_dashboard.py

run-driver: build
	@ if [ "${OPENAI_API_KEY}" = "" ]; then \
		echo "Please set OPENAI_API_KEY environment variable"; \
		exit 1; \
	fi
	. venv/bin/activate; python src/driver.py

run-driver-do-not-start-infino: build
	. venv/bin/activate; python src/driver.py --do-not-start-infino

test: build
	. venv/bin/activate; export PYTHONPATH=./src:${PYTHONPATH}; pytest -o log_cli=true tests

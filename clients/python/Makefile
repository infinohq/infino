venv: venv/touchfile

venv/touchfile:
	test -d venv || virtualenv venv
	. venv/bin/activate; pip install -Ur infino/requirements.txt
	touch venv/touchfile

test: venv
	. venv/bin/activate; python -m unittest

clean:
	rm -rf venv
	find . -name "*.pyc" -delete

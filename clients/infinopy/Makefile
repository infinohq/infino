venv: venv/touchfile

venv/touchfile:
	test -d venv || virtualenv venv
	. venv/bin/activate; pip install -Ur requirements.txt
	touch venv/touchfile

build: venv
	. venv/bin/activate; python setup.py sdist bdist_wheel

test: venv
	. venv/bin/activate; python -m unittest

clean:
	rm -rf venv build dist __pycache__ infinopy.egg-info
	find . -name "*.pyc" -delete

package: clean build

upload: package
	twine upload dist/*

all: install

install:
	python3 ./setup.py install --user

build-for-pypi:
	python3 setup.py sdist bdist_wheel

test-deploy: build-for-pypi
	python3 setup.py sdist bdist_wheel
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

deploy: build-for-pypi
	twine upload dist/*

all: install

install:
	poetry install

clean-dist:
	rm -rf dist/

build-for-pypi:
	poetry build

deploy: clean-dist build-for-pypi
	poetry publish --username __token__ --password ${PYPI_TOKEN}

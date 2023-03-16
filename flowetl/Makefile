#
# Makefile for managing FlowETL dependencies and virtual environment.
#
# Requires pyenv to be installed, to ensure the correct python version is used for locking dependencies.
#
# The virtual environment directory can be configured by setting the variable VENVDIR (default is .venv)
#
# targets:
#   lock (default):
#       Compile requirements.txt and dev-requirements.txt from requirements.in and dev-requirements.in
#   requirements.txt:
#       Compile requirements.txt from requirements.in
#   dev-requirements.txt:
#       Compile dev-requirements.txt from dev-requirements.in
#   upgrade:
#       Re-compile requirements.txt and dev-requirements.txt, upgrading all dependencies
#   sync:
#       Sync virtual environment with requirements.txt
#   dev-sync:
#       Sync virtual environment with requirements.txt _and_ dev-requirements.txt, and editable-install flowetl
#   unit-tests:
#       Run unit tests in virtual environment with dev requirements installed
#   integration-tests:
#       Run integration tests in virtual environment with dev requirements installed
#   tests:
#       Run all tests (unit and integration)
#   clean:
#       Remove virtual environment
#   deep-clean:
#       Remove virtual environment and compiled requirements files

VENVDIR ?= .venv
ACTIVATE := source $(VENVDIR)/bin/activate;
PIP_COMPILE := pip-compile --resolver=backtracking --strip-extras --generate-hashes
PIP_SYNC := pip-sync --pip-args "--no-deps"
# Recursive variable because versions should be read when a target is made, not before
VERSION_ENVS = AIRFLOW_VERSION=$(shell cat .airflow-version) PYTHON_VERSION=$(shell cat .python-version)

.PHONY: lock upgrade sync dev-sync unit-tests integration-tests clean deep-clean

lock: requirements.txt dev-requirements.txt

# Create virtual env if it doesn't exist, and install pip-tools
$(VENVDIR):
	pyenv local # Ensure pyenv is installed
	python -m venv $(VENVDIR)
	$(ACTIVATE) pip install --upgrade pip pip-tools

requirements.txt: requirements.in .python-version .airflow-version flowetl/setup.py | $(VENVDIR)
	$(ACTIVATE) $(VERSION_ENVS) $(PIP_COMPILE) --unsafe-package flowetl requirements.in

dev-requirements.txt: dev-requirements.in requirements.txt .python-version .airflow-version | $(VENVDIR)
	$(ACTIVATE) $(VERSION_ENVS) $(PIP_COMPILE) --allow-unsafe dev-requirements.in

upgrade: requirements.in dev-requirements.in .python-version .airflow-version flowetl/setup.py | $(VENVDIR)
	$(ACTIVATE) $(VERSION_ENVS) $(PIP_COMPILE) --upgrade --unsafe-package flowetl requirements.in
	$(ACTIVATE) $(VERSION_ENVS) $(PIP_COMPILE) --upgrade --allow-unsafe dev-requirements.in

sync: requirements.txt $(VENVDIR)
	$(ACTIVATE) $(PIP_SYNC) requirements.txt

dev-sync: requirements.txt dev-requirements.txt flowetl/setup.py $(VENVDIR)
	$(ACTIVATE) $(PIP_SYNC) requirements.txt dev-requirements.txt && pip install --no-deps -e ./flowetl

unit-tests: dev-sync
	$(ACTIVATE) pytest tests/unit

integration-tests: dev-sync
	docker build -t flowminder/flowdb:test -f ../flowdb.Dockerfile ..
	docker build -t flowminder/flowetl:test -f ../flowetl.Dockerfile ..
	docker pull postgres:11.0
	$(ACTIVATE) FLOWETL_TESTS_CONTAINER_TAG=test FLOWETL_INTEGRATION_TESTS_DISABLE_PULLING_DOCKER_IMAGES=TRUE pytest tests/integration

tests: unit-tests integration-tests

clean:
	rm -rf "$(VENVDIR)"

deep-clean: clean
	rm -f requirements.txt dev-requirements.txt

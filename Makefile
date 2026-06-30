PYTHON ?= python3
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python
DOCS_DIR := docs

.PHONY: venv deps docs lint test

venv:
	@test -x "$(VENV_PYTHON)" || "$(PYTHON)" -m venv "$(VENV)"

deps: venv
	"$(VENV_PYTHON)" -m pip install -e '.[test]'

docs: venv
	"$(VENV_PYTHON)" -m pip install -e '.[docs]'
	"$(VENV_PYTHON)" -m pdoc pyftdc --output-directory "$(DOCS_DIR)"

lint: deps
	"$(VENV_PYTHON)" -m ruff check src tests
	"$(VENV_PYTHON)" -m pylint src tests
	"$(VENV_PYTHON)" -m pyright

test: deps
	"$(VENV_PYTHON)" -m pytest

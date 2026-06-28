PYTHON ?= python3
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python

.PHONY: venv deps test

venv:
	@test -x "$(VENV_PYTHON)" || "$(PYTHON)" -m venv "$(VENV)"

deps: venv
	"$(VENV_PYTHON)" -m pip install -e '.[test]'

test: deps
	"$(VENV_PYTHON)" -m pytest

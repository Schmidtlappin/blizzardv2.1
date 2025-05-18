# Makefile for Blizzard 2.0

# Variables
PYTHON = python3
PIP = $(PYTHON) -m pip
PYTEST = pytest
PYTEST_ARGS = --verbose
BLACK = black
FLAKE8 = flake8
MYPY = mypy

# Directories
SRC_DIR = src
TEST_DIR = test
CLI_DIR = cli
CONFIG_DIR = config

# Targets
.PHONY: help install dev-install clean test lint format types check

# Default target
help:
	@echo "Available targets:"
	@echo "  install      Install the package"
	@echo "  dev-install  Install the package in development mode"
	@echo "  clean        Remove build artifacts and __pycache__ directories"
	@echo "  test         Run tests"
	@echo "  lint         Run linter"
	@echo "  format       Format code with Black"
	@echo "  types        Run type checker"
	@echo "  check        Run all checks (lint, types, test)"

# Installation
install:
	$(PIP) install -e .

dev-install:
	$(PIP) install -e . -r requirements-dev.txt

# Cleaning
clean:
	rm -rf build/ dist/ *.egg-info/ __pycache__/ .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Testing
test:
	$(PYTEST) $(PYTEST_ARGS) $(TEST_DIR)

# Linting
lint:
	$(FLAKE8) $(SRC_DIR) $(CLI_DIR) $(TEST_DIR)

# Formatting
format:
	$(BLACK) $(SRC_DIR) $(CLI_DIR) $(TEST_DIR)

# Type checking
types:
	$(MYPY) $(SRC_DIR) $(CLI_DIR)

# Run all checks
check: lint types test

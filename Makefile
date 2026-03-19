.PHONY: install test lint run-daily migrate setup-db load-deals

VENV      := .venv
PYTHON    := $(VENV)/bin/python
PIP       := $(VENV)/bin/pip
PYTEST    := $(VENV)/bin/pytest
RUFF      := $(VENV)/bin/ruff

# install — create a virtual environment (if absent) and install all dependencies.
# Uses pip with a local .venv so the system Python is never modified.
install:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip setuptools
	$(PIP) install -e ".[dev]"

# test — run the full test suite with pytest.
test:
	$(PYTEST) tests/ -v

# lint — check code quality with ruff.
# ruff is chosen over flake8 because it handles linting, import sorting, and style
# checks in a single pass and is significantly faster on larger codebases.
lint:
	$(RUFF) check src/ tests/

# setup-db — one-time privilege grant that must be run as a Postgres superuser.
# Requires PGPASSWORD and PGUSER env vars pointing to a superuser, or run manually:
#   psql -h 127.0.0.1 -U postgres -d dealflow -f db/setup_dev_db.sql
setup-db:
	@echo "Running one-time database privilege setup as superuser..."
	@echo "If this fails, run manually: psql -U postgres -d dealflow -f db/setup_dev_db.sql"
	psql -h 127.0.0.1 -U $${PGUSER:-postgres} -d dealflow -f db/setup_dev_db.sql

# migrate — apply all pending SQL migrations to the database configured in DATABASE_URL.
migrate:
	$(PYTHON) -m src.db.runner

# load-deals — upsert deals from a CSV file into the deals table.
# Usage: make load-deals FILE=data/deals_template.csv
load-deals:
	$(PYTHON) -m src.ingestion.deal_loader $(FILE)

# run-daily — placeholder for the daily pipeline entry point (implemented in Slice 11).
run-daily:
	@echo "run-daily is not implemented yet (Slice 11)"

# FHIR2OMOP — convenience targets
#
# Required env vars:
#   GCP_PROJECT   e.g. chile-omop-prod
#   GCP_REGION    e.g. southamerica-west1
#
# Optional:
#   HAPI_BASE_URL   e.g. https://hapi.internal/fhir
#   GCS_LANDING     e.g. gs://chile-omop-prod-fhir-landing

GCP_REGION ?= southamerica-west1
GCS_LANDING ?= gs://$(GCP_PROJECT)-fhir-landing
RUNBOOK_VENV := tools/runbook/.venv

ifeq ($(OS),Windows_NT)
PYTHON ?= py -3
RUNBOOK_VENV_BIN := Scripts
RUNBOOK_PY := $(RUNBOOK_VENV)/$(RUNBOOK_VENV_BIN)/python.exe
else
PYTHON ?= python3
RUNBOOK_VENV_BIN := bin
RUNBOOK_PY := $(RUNBOOK_VENV)/$(RUNBOOK_VENV_BIN)/python
endif

.PHONY: check datasets buckets vocab ingest load dbt-parse dbt-build dbt-test dqd all clean-raw runbook runbook-dry-run runbook-resume runbook-check-hashing runbook-install runbook-clean runbook-check-connectivity

check:
	@test -n "$(GCP_PROJECT)" || (echo "ERROR: GCP_PROJECT not set" && exit 1)
	@echo "project=$(GCP_PROJECT) region=$(GCP_REGION)"

datasets: check
	bash infra/bigquery/create_datasets.sh $(GCP_PROJECT) $(GCP_REGION)

buckets: check
	gsutil mb -p $(GCP_PROJECT) -l $(GCP_REGION) -b on $(GCS_LANDING) || true

vocab: check
	@test -f ./vocabulary_download_v5.zip || (echo "Download vocab from https://athena.ohdsi.org/ to ./vocabulary_download_v5.zip first" && exit 1)
	python vocab/load_athena_vocab.py \
	  --zip ./vocabulary_download_v5.zip \
	  --project $(GCP_PROJECT) \
	  --dataset omop_vocab \
	  --location $(GCP_REGION)

ingest: check
	python ingest/hapi_export.py \
	  --hapi-base-url $(HAPI_BASE_URL) \
	  --gcs-landing $(GCS_LANDING) \
	  --run-date $$(date +%Y-%m-%d)

load: check
	python ingest/ndjson_to_bq.py \
	  --project $(GCP_PROJECT) \
	  --dataset fhir_raw \
	  --location $(GCP_REGION) \
	  --gcs-landing $(GCS_LANDING) \
	  --run-date $$(date +%Y-%m-%d)

dbt-parse:
	cd dbt && dbt deps && dbt parse

dbt-build:
	cd dbt && dbt deps && dbt seed && dbt build --select tag:omop

dbt-test:
	cd dbt && dbt test

dqd: check
	Rscript quality/run_dqd.R $(GCP_PROJECT) omop_cdm omop_vocab

all: datasets buckets vocab ingest load dbt-build dbt-test

clean-raw: check
	@echo "This will DROP $(GCP_PROJECT):fhir_raw — Ctrl+C to abort"
	@sleep 5
	bq rm -r -f -d $(GCP_PROJECT):fhir_raw

# -----------------------------------------------------------------------------
# Interactive warehouse-validation runbook (TUI)
# See docs/WAREHOUSE_VALIDATION_RUNBOOK.md and tools/runbook/
#
# We ship a dedicated venv at tools/runbook/.venv so the TUI's three deps
# (rich, questionary, python-dotenv) don't collide with system Python.
# Ubuntu 24.04 / PEP 668 forbids `pip install` against system Python
# without --break-system-packages, so a venv is the right call anyway.
# -----------------------------------------------------------------------------

runbook-install:
	$(PYTHON) -m venv $(RUNBOOK_VENV)
	$(RUNBOOK_PY) -m pip install --upgrade pip
	$(RUNBOOK_PY) -m pip install -r tools/runbook/requirements.txt
	@echo ""
	@echo "runbook TUI installed into $(RUNBOOK_VENV)"
	@echo "venv python: $(RUNBOOK_PY)"
	@echo "Next: make runbook-dry-run   (preview)"
	@echo "      make runbook           (real interactive run)"
	@echo "      make runbook-check-hashing  (hashing preflight)"

runbook:
	@test -x $(RUNBOOK_PY) || (echo "runbook venv missing — run 'make runbook-install' first" && exit 1)
	$(RUNBOOK_PY) -m tools.runbook

runbook-dry-run:
	@test -x $(RUNBOOK_PY) || (echo "runbook venv missing — run 'make runbook-install' first" && exit 1)
	$(RUNBOOK_PY) -m tools.runbook --dry-run

runbook-resume:
	@test -x $(RUNBOOK_PY) || (echo "runbook venv missing — run 'make runbook-install' first" && exit 1)
	$(RUNBOOK_PY) -m tools.runbook --resume

runbook-check-hashing:
	@test -x $(RUNBOOK_PY) || (echo "runbook venv missing — run 'make runbook-install' first" && exit 1)
	$(RUNBOOK_PY) -m tools.runbook --check-hashing

runbook-check-connectivity:
	@test -x $(RUNBOOK_PY) || (echo "runbook venv missing — run 'make runbook-install' first" && exit 1)
	$(RUNBOOK_PY) -m tools.runbook --check-connectivity

runbook-clean:
	rm -rf $(RUNBOOK_VENV)

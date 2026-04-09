#!/usr/bin/env bash
# Create the four BigQuery datasets used by the pipeline, pinned to a region.
# Idempotent: re-running is safe.
#
# Usage: create_datasets.sh <project> <region>
set -euo pipefail

PROJECT="${1:?project required}"
REGION="${2:?region required}"

create() {
  local ds="$1"
  local desc="$2"
  if bq --project_id="$PROJECT" show --format=prettyjson "${PROJECT}:${ds}" >/dev/null 2>&1; then
    echo "[=] ${ds} already exists"
  else
    echo "[+] creating ${ds} in ${REGION}"
    bq --project_id="$PROJECT" --location="$REGION" mk \
      --dataset \
      --description "$desc" \
      "${PROJECT}:${ds}"
  fi
}

create fhir_raw   "Raw FHIR resources (Analytics V2 schema or NDJSON-loaded)"
create omop_stg   "Staging models produced by dbt"
create omop_cdm   "OMOP CDM v5.4 — production tables"
create omop_vocab "OMOP vocabulary loaded from OHDSI Athena"

echo "[done] datasets ready in ${PROJECT}"

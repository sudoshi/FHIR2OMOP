# FHIR2OMOP — HAPI FHIR to OMOP CDM on BigQuery

Opinionated starter kit implementing **Composition A** from the research brief
(`HAPI-FHIR-to-OMOP-on-BigQuery-Research-Brief.md`): GCP-managed ingestion from
HAPI FHIR + a dbt-bigquery project that transforms raw FHIR (Analytics V2
schema) into OMOP CDM v5.4.

Audience: the Chile team (Roche LIMS → HAPI FHIR → OMOP on BigQuery).

## What this repo gives you

| Component | What it does | Where |
|---|---|---|
| Runbook TUI | Interactive wizard that drives the first warehouse-validation run end-to-end (11 stages, pre-flight connectivity checks, resume support) | `tools/runbook/` · [docs](docs/RUNBOOK_TUI.md) |
| BigQuery bootstrap | Creates `fhir_raw`, `omop_stg`, `omop_cdm`, `omop_vocab` datasets in `southamerica-west1` | `infra/bigquery/` |
| Vocabulary loader | Loads an Athena vocabulary bundle into `omop_vocab` | `vocab/` |
| HAPI ingestion | Triggers HAPI `$export`, polls, lands NDJSON in GCS | `ingest/hapi_export.py` |
| Raw loader | Loads GCS NDJSON into `fhir_raw.*` (schema-on-read JSON) | `ingest/ndjson_to_bq.py` |
| dbt project | Staging + marts for `PERSON`, `VISIT_OCCURRENCE`, `VISIT_DETAIL`, `MEASUREMENT`, `OBSERVATION`, `SPECIMEN`, `CARE_SITE`, `LOCATION`, `CDM_SOURCE`, `FACT_RELATIONSHIP` | `dbt/` |
| Airflow DAG | Nightly orchestration of the whole chain | `orchestration/airflow/dags/fhir2omop_nightly.py` |
| DQD runner | Skeleton for running OHDSI DataQualityDashboard against BigQuery | `quality/` |

## Quickstart

The recommended path is the interactive TUI at `tools/runbook/`. It wraps
every step of `docs/WAREHOUSE_VALIDATION_RUNBOOK.md`, collects every input
with sensible defaults, runs pre-flight connectivity checks against GCP
and HAPI before touching anything, and persists state so you can resume
after a failure.

```bash
# 0. Prereqs: gcloud SDK, python 3.11+, dbt-bigquery, bq CLI, a GCP project.
#    See docs/PREREQUISITES.md for macOS/Linux/Windows install instructions.

# 1. One-time install (creates tools/runbook/.venv with rich + questionary)
make runbook-install

# 2. Preview what the wizard will ask and what will run — no side effects
make runbook-dry-run

# 3. Verify gcloud, bq, HAPI, dbt, and the vocab zip are actually reachable
make runbook-check-connectivity

# 4. Run the interactive wizard end-to-end
make runbook
```

If a stage fails, fix the issue and `make runbook-resume` to pick up from
where it stopped. Full operator docs: [`docs/RUNBOOK_TUI.md`](docs/RUNBOOK_TUI.md).

### Manual path (if you prefer running the stages yourself)

The same work can be done step-by-step against the Make targets. Use this
if you're scripting in CI or want to understand what the TUI is actually
doing under the hood.

```bash
export GCP_PROJECT=chile-omop-prod
export GCP_REGION=southamerica-west1

# 1. Create datasets + landing bucket
make datasets
make buckets

# 2. Load OMOP vocabulary (after downloading from https://athena.ohdsi.org/)
#    Choose at least: SNOMED, LOINC, UCUM, Gender, Race, Ethnicity, Visit,
#    Visit Type, Domain, Concept Class, Vocabulary, Relationship, Type Concept
python vocab/load_athena_vocab.py \
  --zip ./vocabulary_download_v5.zip \
  --project $GCP_PROJECT \
  --dataset omop_vocab

# 3. Install dbt deps, seed, and build (after raw FHIR data is loaded)
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml    # edit paths
dbt deps
dbt seed
dbt build --select tag:omop
```

## First validation run

The source of truth for the first real warehouse-backed run is
[`docs/WAREHOUSE_VALIDATION_RUNBOOK.md`](docs/WAREHOUSE_VALIDATION_RUNBOOK.md).
It walks through:

- loading a small raw FHIR batch first
- validating `fhir_raw` before dbt
- building dbt in stages
- checking OMOP row counts and unknown concept rates
- running dbt tests and DQD in that order

The runbook TUI (`make runbook`) drives that document as 11 interactive
stages, runs the validation queries from §4 and §7 automatically, and
produces an exit-criteria report against §10. Reusable helper queries
live in:

- `dbt/analyses/raw_resource_counts.sql`
- `dbt/analyses/omop_validation_summary.sql`
- `dbt/analyses/inventory_source_codes.sql`

## Pipeline topology

```
HAPI FHIR ──$export──▶ GCS ──ndjson_to_bq──▶ BQ fhir_raw ──dbt──▶ omop_stg ──dbt──▶ omop_cdm
                                                                                    │
                                                                omop_vocab (Athena) ┘
```

See the research brief, §3, for the full architecture diagram and the
rationale for each layer.

## What this code *does not* do (yet)

- **Sensitive data handling.** You must hash `PERSON.person_source_value`
  in production. `dbt/macros/hash_mrn.sql` now supports this via
  `hash_person_source_value: true` plus either a
  `person_source_value_pepper` var or `$DBT_PEPPER`; keep it disabled in
  local/dev.
- **Mirror to Cloud Healthcare API FHIR store.** Optional Layer 2 from the
  brief. If you go that route you can skip `ingest/ndjson_to_bq.py`
  entirely and point `sources.yml` at the Analytics V2 tables the
  Healthcare API streams.
- **DRUG_EXPOSURE, CONDITION_OCCURRENCE, DEATH.** LIMS-dominant feeds rarely
  have these. Add staging models under `dbt/models/staging/` when you do.
- **Incremental DQD report landing in Looker Studio.** `quality/run_dqd.R`
  produces the JSON report; wiring it to Looker Studio is downstream.

## Where to start reading the code

1. `dbt/models/marts/omop/measurement.sql` — this is the single most
   important file in the repo. Everything else is plumbing.
2. `dbt/models/staging/stg_fhir__observation_lab.sql` — shows how the
   Analytics V2 schema gets flattened.
3. `dbt/seeds/seed_unit_source_to_concept.csv` — the one file you will
   hand-edit most (Roche units → UCUM concept_ids).
4. `orchestration/airflow/dags/fhir2omop_nightly.py` — read this to
   understand the nightly flow.

## Ties to the brief

| Brief section | Code |
|---|---|
| §3 Layer 1 (HAPI `$export`) | `ingest/hapi_export.py` |
| §3 Layer 3 (raw BigQuery) | `infra/bigquery/`, `ingest/ndjson_to_bq.py` |
| §3 Layer 4 (dbt transform) | `dbt/` |
| §3 Layer 5 (vocabulary) | `vocab/load_athena_vocab.py` |
| §3 Layer 6 (DQD / Achilles) | `quality/` |
| §4 Observation → MEASUREMENT | `dbt/models/marts/omop/measurement.sql` + `dbt/seeds/` |
| §6 Phase 3 (core ETL) | `dbt/` |
| §9 Next steps (1) count distinct codes | `dbt/analyses/inventory_source_codes.sql` |

## License

Internal scaffolding for the Chile team. Borrow freely.

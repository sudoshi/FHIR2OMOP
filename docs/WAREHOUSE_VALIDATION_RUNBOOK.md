# First Warehouse-Backed Validation Runbook

This runbook is for the first real execution of the pipeline against a
BigQuery project with representative FHIR data.

It is intentionally opinionated:

- Use a small, known export window first, not a full historical load.
- Validate each layer before moving on.
- Treat unknown concepts and missing joins as mapping work items, not as
  reasons to widen scope immediately.

## Goal

Exit the first validation run with:

1. Raw FHIR resources loaded into `fhir_raw`.
2. dbt models built successfully into `omop_stg` and `omop_cdm`.
3. A small set of row-count, mapping, and relationship checks reviewed.
4. A short list of code-system, unit, and source-data gaps to fix next.

## Recommended first-run scope

Run a narrow batch first:

- 1 day of data if the feed is small.
- 2 to 7 days if one day is too sparse.
- Prefer a date window that includes:
  - at least one patient
  - at least one encounter
  - at least one diagnostic report
  - multiple lab observations with numeric values
  - at least one specimen reference

Do not start with a backfill.

## 1. Prerequisites

Tools:

- `gcloud`
- `bq`
- Python 3.11+
- `dbt-bigquery`
- R plus OHDSI DQD dependencies if you want to run DQD on day one

Environment:

```bash
export GCP_PROJECT=chile-omop-prod
export GCP_REGION=southamerica-west1
export GCS_LANDING=gs://$GCP_PROJECT-fhir-landing
export HAPI_BASE_URL=https://hapi.internal/fhir
```

dbt profile:

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
```

For the first validation run, prefer the `dev` target with OAuth unless you
already have the production service account flow working.

## 2. Bootstrap the project

Create datasets and the landing bucket:

```bash
make datasets
make buckets
```

Load the Athena vocabulary bundle:

```bash
python vocab/load_athena_vocab.py \
  --zip ./vocabulary_download_v5.zip \
  --project $GCP_PROJECT \
  --dataset omop_vocab \
  --location $GCP_REGION
```

Sanity-check the vocabulary load:

```bash
bq query --use_legacy_sql=false \
'select table_name, row_count
 from `'"$GCP_PROJECT"'.omop_vocab.INFORMATION_SCHEMA.TABLES`
 order by table_name'
```

Expected:

- `concept`, `concept_relationship`, `concept_ancestor`, `vocabulary`
  populated
- row counts far above zero

## 3. Land a small raw FHIR batch

If exporting from HAPI directly:

```bash
python ingest/hapi_export.py \
  --hapi-base-url $HAPI_BASE_URL \
  --gcs-landing $GCS_LANDING \
  --run-date 2026-04-09 \
  --since 2026-04-08T00:00:00Z
```

Then load to BigQuery:

```bash
python ingest/ndjson_to_bq.py \
  --project $GCP_PROJECT \
  --dataset fhir_raw \
  --location $GCP_REGION \
  --gcs-landing $GCS_LANDING \
  --run-date 2026-04-09
```

If the data is already in `fhir_raw`, skip to the raw-layer checks below.

## 4. Validate the raw layer before dbt

Compile the helper analysis:

```bash
cd dbt
dbt deps
dbt compile --select raw_resource_counts
```

Run the compiled SQL in BigQuery, or use this direct query:

```bash
bq query --use_legacy_sql=false \
"select table_name, row_count
 from \`$GCP_PROJECT.fhir_raw.INFORMATION_SCHEMA.TABLES\`
 order by table_name"
```

Expected:

- `Patient`, `Observation`, `DiagnosticReport`, `Specimen` tables exist
- row counts are non-zero for the resources you expect in the sample

Investigate before proceeding if:

- `Observation` is empty
- all `last_updated` values are null
- one resource dominates unexpectedly because the export filter is wrong

## 5. Parse dbt before building

```bash
make dbt-parse
```

If you need the production hashing behavior on the first real build, pass:

```bash
DBT_PEPPER='...' dbt parse --vars '{hash_person_source_value: true}'
```

Expected:

- parse succeeds with no compilation errors

## 6. Seed and build in stages

Seed the hand-maintained mappings:

```bash
cd dbt
dbt seed --target dev
```

Build staging and intermediate models first:

```bash
dbt build --target dev --select tag:staging+ tag:intermediate+
```

Then build the OMOP layer:

```bash
dbt build --target dev --select tag:omop
```

If you want the single-command version after the first successful staged run:

```bash
make dbt-build
```

## 7. Validate the OMOP layer

Compile the helper summary:

```bash
cd dbt
dbt compile --select omop_validation_summary inventory_source_codes
```

Review these checks:

1. Row counts by table
2. Unknown concept counts
3. Missing unit counts in `measurement`
4. Missing visit links where you expected them
5. Source-code coverage from `inventory_source_codes`

Recommended direct SQL checks:

```bash
bq query --use_legacy_sql=false \
"select table_name, row_count
 from \`$GCP_PROJECT.omop_cdm.INFORMATION_SCHEMA.TABLES\`
 order by table_name"
```

```bash
bq query --use_legacy_sql=false \
"select
   count(*) as measurement_rows,
   countif(measurement_concept_id = 0) as unknown_measurement_concept_rows,
   countif(unit_concept_id = 0) as unknown_unit_rows,
   countif(person_id is null) as null_person_rows
 from \`$GCP_PROJECT.omop_cdm.measurement\`"
```

```bash
bq query --use_legacy_sql=false \
"select
   count(*) as observation_rows,
   countif(observation_concept_id = 0) as unknown_observation_concept_rows
 from \`$GCP_PROJECT.omop_cdm.observation\`"
```

Use `dbt/analyses/inventory_source_codes.sql` to identify the source codes
that still need explicit mapping in `seed_test_source_to_concept.csv`.

## 8. Run relational tests

```bash
make dbt-test
```

Expected:

- uniqueness tests pass on primary keys
- relationship tests pass from facts to `person`

If tests fail, inspect whether the issue is:

- a bad join key in the source data
- a missing reference resource
- a mapping problem that zeroed a concept id but should not have

## 9. Run DQD after dbt tests are green

```bash
Rscript quality/run_dqd.R $GCP_PROJECT omop_cdm omop_vocab 2026-04-09
```

Focus first on:

- field-level nullability failures
- invalid concept ids
- date-ordering problems
- rows dropped because person linkage failed upstream

Do not try to clear every DQD warning in the first run. The first goal is to
separate structural failures from expected mapping backlog.

## 10. Exit criteria for the first run

Call the first warehouse-backed validation successful if all of these are true:

1. Raw FHIR tables loaded for the expected resource types.
2. `dbt parse`, `dbt build`, and `dbt test` all succeed.
3. `measurement` and `person` contain non-zero rows.
4. Unknown concept rates are understood and documented.
5. The remaining gaps are mostly seed-mapping or source-data issues, not
   broken model logic.

## Common failure signatures

`Observation` rows built but `measurement` is empty:
- Patient linkage likely failed.
- Check whether `Observation.subject.reference` lines up with `Patient.id`.

`measurement_concept_id = 0` is very high:
- LOINC coverage is low.
- Run `inventory_source_codes.sql` and expand
  `seed_test_source_to_concept.csv`.

`unit_concept_id = 0` is very high:
- Units are not normalized enough.
- Expand `seed_unit_source_to_concept.csv`.

`visit_occurrence` is empty but measurements loaded:
- This can be acceptable for LIMS-heavy feeds with weak encounter coverage.
- Treat it as a data-shape decision, not automatically as a defect.

`person_source_value` looks hashed in prod but joins still work:
- That is expected now.
- Downstream joins use `person_id`, not `person_source_value`.

## Suggested follow-up after first success

1. Expand `seed_test_source_to_concept.csv` from real source-code inventory.
2. Expand `seed_unit_source_to_concept.csv` from actual unit strings.
3. Capture one manually reviewed patient round-trip:
   FHIR Patient/Observation/Specimen -> OMOP PERSON/MEASUREMENT/SPECIMEN.
4. Move from local/manual execution to Composer once the single-run path is
   stable.

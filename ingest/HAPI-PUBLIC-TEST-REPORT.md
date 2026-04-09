# HAPI FHIR Ingestion — Public Server Test Report

**Date:** 2026-04-09
**Target:** `https://hapi.fhir.org/baseR4` (public HAPI test server)
**Scope:** Smoke-test + debug of `ingest/hapi_export.py` and `ingest/ndjson_to_bq.py` against a live HAPI FHIR R4 server.
**Outcome:** One critical bug found and fixed. Full end-to-end ingest (kickoff → poll → download → rewrite → schema-validate) passes against the public server. GCS/BigQuery paths still require a GCP environment to validate.

---

## 1. Context

The FHIR2OMOP repo implements **Composition A** from `HAPI-FHIR-to-OMOP-on-BigQuery-Research-Brief.md` — DIY ingestion plus dbt-in-BigQuery transform. The ingestion half consists of two Python scripts:

| File | Role |
|---|---|
| `ingest/hapi_export.py` | FHIR Bulk Data Access (`$export`) kickoff, polling, and streaming of NDJSON output files into GCS. |
| `ingest/ndjson_to_bq.py` | GCS NDJSON → `fhir_raw.*` BigQuery tables (schema-on-read JSON with bookkeeping columns). |

Neither had been run against a real HAPI server in this repo. Neither had unit or integration tests. The goal of this test pass was to:

1. Confirm the scripts actually work against a vanilla HAPI server.
2. Catch obvious protocol or parsing bugs before the Chile team tries to run them against their self-hosted HAPI.
3. Leave behind a repeatable local test harness that doesn't require GCS/BigQuery.

---

## 2. Environment

| Component | Version / Detail |
|---|---|
| HAPI base URL | `https://hapi.fhir.org/baseR4` |
| HAPI server build | `HAPI FHIR 8.9.4-SNAPSHOT/eee190b153/2026-02-26` |
| FHIR version | `4.0.1` (R4) |
| Python | 3.9 (macOS system) |
| `requests` | 2.32.5 |
| `tenacity` | 9.1.2 |
| `google-cloud-storage` | 3.9.0 (import-only; no live GCS calls) |

`requests`, `tenacity`, and `google-cloud-storage` were installed ad-hoc via `pip3 install` to match `ingest/requirements.txt`.

The README states Python 3.11+. Python 3.9 was sufficient here because `ingest/hapi_export.py` and `ingest/ndjson_to_bq.py` both start with `from __future__ import annotations`, which defers evaluation of the `str | None` / `tuple[str, str]` annotations. No runtime issue encountered.

---

## 3. Phase 1 — Capability statement and `$export` support

**Command:**
```python
GET https://hapi.fhir.org/baseR4/metadata
Accept: application/fhir+json
```

**Result:** `HTTP 200`. Capability statement confirmed:

- `software.name` = `HAPI FHIR Server`
- `fhirVersion` = `4.0.1`
- Two bulk-data operations advertised:
  - `export` → `http://hl7.org/fhir/uv/bulkdata/OperationDefinition/export`
  - `export-poll-status` → HAPI-specific status polling endpoint

**Conclusion:** Server speaks FHIR Bulk Data Access, which is what `hapi_export.py` is built against. No need to fall back to `$everything` or resource-level paging.

Sanity-checked with a `GET /Patient?_count=3` that returned a normal FHIR searchset bundle with three Patients — server is not rate-limiting us.

---

## 4. Phase 2 — `$export` kickoff and polling

### Kickoff

```python
GET https://hapi.fhir.org/baseR4/$export
    ?_outputFormat=application/fhir+ndjson
    &_type=Patient
Accept: application/fhir+json
Prefer: respond-async
```

**Result:** `HTTP 202` with `Content-Location` header:
```
https://hapi.fhir.org/baseR4/$export-poll-status?_jobId=<uuid>
```

This matches the FHIR Bulk Data Access spec and what `kickoff_export()` expects at `hapi_export.py:104`. No bug here.

### Polling

Polled with 5–15s intervals. State transitions observed:

```
QUEUED → IN_PROGRESS → HTTP 200 (manifest)
```

`X-Progress` header populated correctly so `poll_until_complete()` at `hapi_export.py:127` logs a useful progress string on each loop.

**Polling latency on public HAPI (single-Patient export):** ~3m30s end-to-end for the small Patient-only export, and ~3m30s again for the three-type (Patient, Encounter, Observation) export. These numbers are characteristic of the public test server's backlog and will be much faster on a dedicated HAPI instance, but they're the reason the default `poll_timeout_s = 3600` in `hapi_export.py:76` is correct for production — don't lower it just because local tests seem slow.

### Manifest inspection

On completion, the poll URL returned a standard bulk-data manifest with an `output` array:

**Patient-only export (first run):**
- 25 files, all type `Patient`, URLs of form `https://hapi.fhir.org/baseR4/Binary/<id>`

**Three-type export (second run, with `Patient,Encounter,Observation`):**
- 95 total files
  - `Patient`: 25
  - `Encounter`: 12
  - `Observation`: 58

`iter_manifest_files()` at `hapi_export.py:146` yields `(type, url)` pairs correctly. The per-type counter in `main()` at `hapi_export.py:197` generated unique `<Type>-NNNN.ndjson` blob names with no collisions.

---

## 5. Phase 3 — The bug

### Symptom

Downloading the first manifest file with the default session headers produced **unparseable "NDJSON"**:

```
{
  "resourceType": "Binary",
  "id": "hUxhVvh58sDgaNhRQwUpsvDE7oZQGDPw",
  "meta": {
    "extension": [ {
      "url": "https://hapifhir.org/NamingSystem/bulk-export-job-id",
      "valueString": "2c7e75d0-..."
  ...
```

That's a pretty-printed FHIR `Binary` resource envelope — not the raw NDJSON content that `rewrite_ndjson()` in `ndjson_to_bq.py` expects. If this reached production unchanged, every "NDJSON" file would have been a single FHIR JSON object, BigQuery load would either fail outright or load exactly one row per file with a `resourceType = "Binary"`, and all downstream dbt models would see zero Patient/Observation/etc. rows.

### Root cause

`make_session()` at `hapi_export.py:93` sets a session-wide `Accept: application/fhir+json` header because that's what the `$export` kickoff and status poll require. `stream_to_gcs()` at `hapi_export.py:154` then reused the same session — and therefore the same `Accept` header — to download the binary manifest files.

HAPI sees `Accept: application/fhir+json` on a `Binary/<id>` GET and does exactly what FHIR says: it wraps the Binary resource (whose `content` would normally be the NDJSON bytes) into a FHIR JSON envelope. Without the `application/fhir+ndjson` Accept, you never actually get the NDJSON bytes — you get the metadata wrapper that describes them.

### Before / after verification

Ran the same `Binary/<id>` URL twice on the same live session:

| Attempt | `Accept` header | `Content-Type` returned | First 3 lines parse as `Patient` NDJSON? |
|---|---|---|---|
| Before (session default) | `application/fhir+json` | `application/fhir+json;charset=utf-8` | **No** — returns `{"resourceType":"Binary", …}` envelope |
| After (per-request override) | `application/fhir+ndjson` | `application/fhir+ndjson` | **Yes** — 3/3 lines parse, e.g. `Patient/69c2792581e99ae3d62b9d41` |

### Fix

`ingest/hapi_export.py:152-168` — override the Accept header for the file download specifically, leaving the session default intact for the kickoff/poll flow:

```python
@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=2, max=30))
def stream_to_gcs(session: requests.Session, file_url: str,
                  bucket: storage.Bucket, blob_name: str) -> int:
    """Stream a single NDJSON file from HAPI to GCS. Returns bytes uploaded."""
    LOG.info("downloading %s -> gs://%s/%s", file_url, bucket.name, blob_name)
    # Override Accept header for binary download — the session default
    # (application/fhir+json) causes HAPI to wrap the Binary in a FHIR
    # JSON envelope instead of returning the raw NDJSON content.
    headers = {"Accept": "application/fhir+ndjson"}
    with session.get(file_url, stream=True, headers=headers) as r:
        r.raise_for_status()
        blob = bucket.blob(blob_name)
        with blob.open("wb") as out:
            total = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
                    total += len(chunk)
            return total
```

The fix is minimal and deliberately scoped to the one function that actually downloads Binary content. The kickoff/poll functions continue to use the session default because those endpoints really do want `application/fhir+json`.

### Notes on the bug class

Some HAPI deployments configure their bulk-data sink to write directly to S3/GCS and return *pre-signed URLs* in the manifest instead of `Binary/<id>` URLs served by HAPI itself. In that case the Accept header wouldn't matter — you'd be hitting an object store, not HAPI. The public HAPI server uses the Binary-served path, as will any vanilla self-hosted HAPI without a custom bulk export sink, so this fix is necessary for the default deployment topology the Chile team is most likely to encounter.

---

## 6. Phase 4 — Local end-to-end test harness

To make this repeatable and to avoid hitting GCS/BigQuery during development, I added a standalone harness at `ingest/test_hapi_public.py`.

### What it does

1. Builds an `ExportArgs` and `requests.Session` via `hapi_export.make_session()` — **exactly the same** session used in production, so Accept-header regressions like the one in §5 would be caught here.
2. Calls the real `hapi_export.kickoff_export()` and `hapi_export.poll_until_complete()` against whichever HAPI base URL you give it (defaults to `https://hapi.fhir.org/baseR4`).
3. For each file in the manifest, calls a local `download_to_local()` that mirrors `stream_to_gcs()` (same Accept-header override, same chunked streaming pattern) but writes to a `tempfile.mkdtemp()` directory instead of GCS.
4. For each downloaded file, calls a local `rewrite_ndjson_local()` that is a byte-for-byte port of `ndjson_to_bq.rewrite_ndjson()` — same column extraction (`resource_id`, `resource_type`, `last_updated` from `meta.lastUpdated`, `raw`, `_ingest_run_date`, `_ingest_file_uri`), same error handling.
5. Calls `validate_rewritten()` which enforces the `fhir_raw.*` BigQuery schema contract from `ndjson_to_bq.py:85-92`:
   - `resource_id` REQUIRED STRING, non-empty
   - `resource_type` REQUIRED STRING, must match filename stem
   - `raw` REQUIRED JSON, non-empty object
   - `last_updated` NULLABLE TIMESTAMP (tracked but not enforced)
6. Prints a per-file and per-type summary. Exits non-zero if any row would violate the schema.
7. `--keep` flag leaves the temp directory for inspection.

### Why import from `hapi_export` instead of reimplementing

Importing `kickoff_export`, `poll_until_complete`, and `make_session` from the production module means the test harness actually covers the production code path. Reimplementing the kickoff/poll logic in the test would have let the Accept-header bug slip through again — which is literally what happens when tests are a parallel implementation of the thing they're testing.

`rewrite_ndjson` and `stream_to_gcs` are mirrored locally instead of imported because they take `google.cloud.storage.Blob` / `Bucket` parameters and we want to avoid wiring up fake GCS objects. The mirrors are short enough that drift is obvious in a diff.

### Running it

```bash
# Default: Patient + Encounter against public HAPI
python ingest/test_hapi_public.py

# Just Patient (fastest)
python ingest/test_hapi_public.py --types Patient

# All the LIMS-relevant types
python ingest/test_hapi_public.py \
  --types Patient,Encounter,Observation,DiagnosticReport,Specimen \
  --poll-interval-s 15 \
  --poll-timeout-s 1800

# Against a different HAPI (e.g. Chile's self-hosted one)
python ingest/test_hapi_public.py \
  --hapi-base-url https://hapi.internal/fhir \
  --keep
```

Exit codes:
- `0` — all files passed schema validation
- `1` — at least one file had schema violations (reported per-file)
- `2` — manifest was empty

---

## 7. Phase 5 — Test results

### Run 1: Patient-only (`--types Patient`)

```
kickoff_export:        HTTP 202, poll URL returned
poll_until_complete:   ~3m30s QUEUED→IN_PROGRESS→200
manifest:              25 files, all Patient
download + rewrite:    all 25 files pulled and validated

Per-file summary:
  Patient-0000.ndjson: 1000 rows  empty_id=0 empty_type=0 wrong_type=0 empty_raw=0 empty_last_updated=0  OK
  Patient-0001.ndjson:  210 rows  empty_id=0 empty_type=0 wrong_type=0 empty_raw=0 empty_last_updated=0  OK
  Patient-0002.ndjson: 1000 rows  empty_id=0 empty_type=0 wrong_type=0 empty_raw=0 empty_last_updated=0  OK
  ... (22 more files, all 1000 rows, all OK)
  Patient-0024.ndjson: 1000 rows  empty_id=0 empty_type=0 wrong_type=0 empty_raw=0 empty_last_updated=0  OK

Summary:
  Patient: 24,210 rows across 25 files
  PASS
```

### Run 2: Multi-resource (`--types Patient,Encounter,Observation`)

```
kickoff_export:        HTTP 202
poll_until_complete:   ~3m25s
manifest:              95 files
  - Patient: 25 files
  - Encounter: 12 files
  - Observation: 58 files

Spot-checks on first file of each type (100 rows sampled):
  Observation-0000.ndjson:  100/1000 sampled, Valid=100, Invalid=0, missing_id=0, missing_lastUpdated=0
  Patient-0000.ndjson:      100/1000 sampled, Valid=100, Invalid=0, missing_id=0, missing_lastUpdated=0
  Encounter-0000.ndjson:    100/1000 sampled, Valid=100, Invalid=0, missing_id=0, missing_lastUpdated=0

PASS
```

### Schema-contract results

For every row that made it through `rewrite_ndjson`, every `fhir_raw.*` table's REQUIRED columns were populated:

| Column | Required? | Empty rows (Run 1, 24,210 total) |
|---|---|---|
| `resource_id` | REQUIRED STRING | 0 |
| `resource_type` | REQUIRED STRING (matches filename) | 0 |
| `raw` | REQUIRED JSON | 0 |
| `_ingest_run_date` | REQUIRED DATE (set by loader) | n/a — provided by loader |
| `_ingest_file_uri` | REQUIRED STRING (set by loader) | n/a — provided by loader |
| `last_updated` | NULLABLE TIMESTAMP | 0 (every row had `meta.lastUpdated`) |

That last point is noteworthy: the public HAPI server populates `meta.lastUpdated` on every Patient resource, so the dbt staging `row_number() over (partition by resource_id order by last_updated desc)` dedup logic will work without a fallback. A self-hosted HAPI will also set `meta.lastUpdated` on writes by default, so this should hold in Chile too.

### Artifact sizes (Run 2, for capacity planning)

- Patient NDJSON: ~740 bytes/row average, max 2,546 bytes
- Encounter NDJSON: ~1,054 bytes/row average, max 1,599 bytes
- Observation NDJSON: ~816 bytes/row average, max 1,667 bytes

At 500k lab results/day (brief §7), that's roughly 400 MB/day of Observation NDJSON before BigQuery compression. Negligible for storage; worth knowing for GCS egress if you ever export out of region.

---

## 8. What was NOT tested

These paths require a live GCP environment and were explicitly out of scope:

| Area | Why it's not covered |
|---|---|
| `stream_to_gcs` actual GCS upload | Needs a real `google.cloud.storage.Bucket`. The mirror in `test_hapi_public.py:download_to_local` exercises the same request/Accept/streaming logic but writes to a local path. The GCS-specific call is the one line `bucket.blob(blob_name).open("wb")`. |
| `ndjson_to_bq.load_file` — BigQuery load | Needs BigQuery credentials, a `fhir_raw` dataset in `southamerica-west1`, and a staged blob in a real GCS bucket. The loader's schema definition, job config, and `WRITE_APPEND` disposition were reviewed statically and look correct. |
| `ndjson_to_bq.ensure_table` table creation | Same — needs BQ access. Schema definition (`resource_id STRING REQUIRED`, partitioning on `_ingest_run_date`, clustering on `resource_id`) matches what the dbt sources expect. |
| Airflow DAG wiring | Read `orchestration/airflow/dags/fhir2omop_nightly.py` statically. Task dependencies look correct; BashOperators pass `{{ ds }}` as `--run-date`, which matches both scripts' expectations. |
| dbt models | Separate workstream — see §9. |
| Vocabulary loader (`vocab/load_athena_vocab.py`) | Not exercised; no Athena bundle available locally. |
| OHDSI DataQualityDashboard runner | R-based; not in this pass. |

---

## 9. Observations flagged during the review (not bugs, but follow-ups)

These surfaced while reading adjacent code during debugging. None of them prevented the public-HAPI tests from passing, but they're worth tracking before the pipeline runs end-to-end in GCP.

### 9.1 dbt `json_lax.sql` macros may be misnamed for BigQuery semantics

`dbt/macros/json_lax.sql` defines:

```sql
{% macro json_string(col, path) %}
  safe.string({{ col }}.{{ path }})
{% endmacro %}

{% macro json_number(col, path) %}
  safe_cast(safe.string({{ col }}.{{ path }}) as numeric)
{% endmacro %}
```

BigQuery's `STRING()` / `SAFE.STRING()` JSON conversion functions are **strict**: they return the value only when the underlying JSON type is already a string, and return `NULL` (via `SAFE.`) when the JSON value is a number or a boolean. The lax family (`LAX_STRING`, `LAX_INT64`, `LAX_FLOAT64`, `LAX_BOOL`) are the ones that stringify / coerce across JSON types.

If that reading of the docs is right, every use of `json_number(...)` in the staging models — e.g. `stg_fhir__observation_lab.value_number`, `range_low`, `range_high` — would silently resolve to `NULL`, because `Observation.valueQuantity.value` is a JSON number, `SAFE.STRING` on it returns `NULL`, and `SAFE_CAST(NULL AS NUMERIC)` is `NULL`. Same class of issue for `safe_cast(json_string('raw', 'deceasedBoolean') as bool)` in `stg_fhir__patient.sql` — the deceasedBoolean is a JSON boolean and would also null out.

I attempted to confirm this against the live BigQuery JSON function reference but the Google docs pages redirect and are heavily client-rendered, so I couldn't get an authoritative quote via WebFetch. **The file name `json_lax.sql` strongly suggests the author's intent was lax conversion**, but the implementation uses `SAFE.STRING` which is the strict variant in BigQuery's JSON API. This needs to be confirmed by running one dbt model against BigQuery and spot-checking a `MEASUREMENT.value_as_number` row — if it comes out `NULL` when `valueQuantity.value` is populated, the macros need to be rewritten to use `LAX_FLOAT64(raw.valueQuantity.value)` / `LAX_STRING(raw.gender)` / `LAX_BOOL(raw.deceasedBoolean)` / etc.

**This is not a HAPI test finding** — the `ndjson_to_bq.py` JSON load is unaffected — but it's the next thing that will break when the pipeline runs end-to-end in BigQuery, and it would be caught on the very first dbt run by the existing `measurement.measurement_concept_id` not-null test (since a NULL `value_number` propagates into `value_source_value` via `coalesce(cast(value_number as string), ...)` but does *not* fail the concept_id test — meaning the pipeline would run green but produce useless MEASUREMENT rows). I'd prioritize fixing this before the first dbt run.

### 9.2 Observation category filter is narrow

`stg_fhir__observation_lab.sql:35-40` classifies an observation as "lab" only if the category coding code is in `('laboratory', 'lab')`. Against the public HAPI data this was fine, but some feeds emit category as a *display* string without a `.code` field, or use the FHIR observation-category system with different casing. Worth a spot check against the Chile HAPI before assuming the split is clean — any missed lab observation will land in `OBSERVATION` instead of `MEASUREMENT` and silently depress the MEASUREMENT row count.

### 9.3 `ndjson_to_bq.py` re-load is not idempotent

On a second run for the same `--run-date`, `ndjson_to_bq.py` rewrites the same staged file to the same path and issues another `WRITE_APPEND` load. That doubles rows in `fhir_raw.<Resource>` for the run date. It's not a HAPI test finding, but it's a footgun for ops: a rerun after a partial failure will silently duplicate. A `WRITE_TRUNCATE` on the partition (or at least a delete-before-load guarded by `_ingest_run_date = @run_date`) would be safer. dbt's dedup on `row_number() over (partition by resource_id order by last_updated desc)` will paper over it downstream, but the duplication is still real in the raw layer and inflates BQ storage.

### 9.4 Default poll timeout vs. public server

`poll_timeout_s = 3600` is fine for a real HAPI. On the public server at the time of testing, single-type exports took ~3–4 minutes each. Anyone running `test_hapi_public.py` for the first time should not lower `--poll-timeout-s` below 900 or they'll get spurious timeouts before HAPI's job scheduler picks up their request.

---

## 10. Summary

| # | Item | Status |
|---|---|---|
| 1 | `hapi_export.py` reaches `https://hapi.fhir.org/baseR4` | ✅ |
| 2 | `$export` kickoff returns `HTTP 202` with `Content-Location` | ✅ |
| 3 | Polling correctly traverses `QUEUED → IN_PROGRESS → 200` | ✅ |
| 4 | Manifest parsed into `(type, url)` tuples with unique per-type indexing | ✅ |
| 5 | **Binary download returns valid NDJSON** | ❌ → ✅ (bug fixed at `ingest/hapi_export.py:152-168`) |
| 6 | `rewrite_ndjson` produces rows that satisfy the `fhir_raw.*` schema contract | ✅ (24,210 / 24,210 rows clean on Patient-only run) |
| 7 | Multi-resource export (Patient + Encounter + Observation) | ✅ (95 files, spot-checked 100 rows/type, zero invalid) |
| 8 | Local test harness that reuses production kickoff/poll code | ✅ (`ingest/test_hapi_public.py`) |
| 9 | GCS upload path | ⏭️ Not tested — needs GCP credentials |
| 10 | BigQuery load path | ⏭️ Not tested — needs GCP credentials |
| 11 | dbt JSON extraction correctness | ⚠️ Flagged for follow-up (see §9.1) |

**Net state of the ingestion half of Composition A:** the Accept-header bug is fixed and the pipeline flows cleanly end-to-end against a vanilla R4 HAPI server. The bug was reachable by literally the first production run — before this test pass, the very first nightly Airflow run against Chile's HAPI would have landed 25 or so `Binary`-envelope rows per resource type in BigQuery and every downstream dbt model would have returned zero records. That risk is now gone for `hapi_export.py`.

The next highest-priority item is §9.1 (dbt `json_lax.sql` macros), which will manifest as silently-NULL numeric measurement values on the first dbt run and is best caught before the Chile team wastes a day debugging their vocabulary mappings against zero data.

---

## Appendix A — Commands used

```bash
# Install dependencies (Python 3.9)
pip3 install requests tenacity google-cloud-storage

# Run the single-type smoke test against public HAPI
cd /Users/sudoshi/Github/FHIR2OMOP
python3 ingest/test_hapi_public.py \
  --types Patient \
  --poll-interval-s 10 \
  --poll-timeout-s 900

# Run the multi-type test
python3 ingest/test_hapi_public.py \
  --types Patient,Encounter,Observation \
  --poll-interval-s 15 \
  --poll-timeout-s 1200

# Keep the temp dir to inspect rewritten NDJSON manually
python3 ingest/test_hapi_public.py --types Patient --keep
```

## Appendix B — Files touched

| File | Change |
|---|---|
| `ingest/hapi_export.py` | Fixed Accept-header bug at `stream_to_gcs()` (lines 152–168). No other changes. |
| `ingest/test_hapi_public.py` | New file. Local end-to-end test harness that imports `kickoff_export` / `poll_until_complete` / `make_session` / `iter_manifest_files` from the production module and exercises download + rewrite + schema-validate locally without GCS/BigQuery. |
| `ingest/HAPI-PUBLIC-TEST-REPORT.md` | This file. |

No changes to `ingest/ndjson_to_bq.py`, `ingest/requirements.txt`, the dbt project, or any other component.

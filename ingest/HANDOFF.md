# Handoff prompt — FHIR2OMOP debug session

**Read this file and then continue the work.** It exists because the user is moving machines and the previous Claude Code session's context is not carrying over.

---

## What this repo is

`/Users/sudoshi/Github/FHIR2OMOP` — a starter kit implementing **Composition A** from `HAPI-FHIR-to-OMOP-on-BigQuery-Research-Brief.md` (in project root, read it first if you haven't). The pipeline is: HAPI FHIR → `$export` NDJSON → GCS → `fhir_raw.*` BigQuery → dbt → `omop_cdm.*`. Target deployment: the Chile team, Roche LIMS, `southamerica-west1`.

Two Python ingest scripts (`ingest/hapi_export.py`, `ingest/ndjson_to_bq.py`) plus a dbt project under `dbt/` plus an Airflow DAG under `orchestration/airflow/dags/`.

## What the previous session did

The user asked: *"Can you test and debug this against the public HAPI server?"*

1. **Tested `hapi_export.py` against `https://hapi.fhir.org/baseR4`** (public HAPI R4 test server, build `HAPI FHIR 8.9.4-SNAPSHOT`).
2. **Found and fixed one critical bug** in `ingest/hapi_export.py:152-168` (`stream_to_gcs`): the function inherited the session-level `Accept: application/fhir+json` header from `make_session()`, which caused HAPI to wrap the Binary NDJSON output in a FHIR JSON envelope instead of returning raw NDJSON. Fix: override `Accept: application/fhir+ndjson` on the per-file download. Without this fix every nightly run would have landed ~25 `Binary`-envelope rows per resource type in BigQuery and every downstream dbt model would return zero.
3. **Created `ingest/test_hapi_public.py`** — a local end-to-end test harness that imports `kickoff_export` / `poll_until_complete` / `make_session` / `iter_manifest_files` from `hapi_export.py` (exercising the real production code path) and mirrors `stream_to_gcs` / `rewrite_ndjson` locally so GCS and BigQuery are not required.
4. **Ran the harness successfully against the public HAPI server**:
   - Patient-only run: 25 files, 24,210 rows, 0 schema violations → PASS
   - Patient + Encounter + Observation run: 95 manifest files, spot-checked 100 rows/type, 0 invalid → PASS
5. **Wrote `ingest/HAPI-PUBLIC-TEST-REPORT.md`** — detailed report of everything above, including before/after tables, per-file results, artifact sizes, what was NOT tested (all GCS/BQ/dbt/Airflow/vocab/DQD paths), and §9 follow-ups.

## Files touched by the previous session

| File | What changed |
|---|---|
| `ingest/hapi_export.py` | Accept-header fix in `stream_to_gcs` (lines ~152–168). No other changes. |
| `ingest/test_hapi_public.py` | New file (test harness). |
| `ingest/HAPI-PUBLIC-TEST-REPORT.md` | New file (detailed test report). |
| `ingest/HANDOFF.md` | This file. |

**Nothing else was modified** — `ndjson_to_bq.py`, the dbt project, the Airflow DAG, vocab loader, Makefile, README, etc. are all untouched.

## Current state

- ✅ Ingestion half of Composition A (HAPI → NDJSON → schema-shaped rows) works end-to-end against a vanilla HAPI R4 server.
- ⏭️ GCS upload, BigQuery load, dbt transform, Airflow DAG execution, vocabulary loader, and DQD runner are **not yet tested** — they all require a live GCP environment.
- ⚠️ Several follow-ups flagged in the test report §9, **most importantly §9.1** — see "Highest-priority next task" below.

## Highest-priority next task

**Verify and fix `dbt/macros/json_lax.sql`.** The file is named `json_lax.sql` suggesting the author's intent was BigQuery's LAX JSON conversion family (`LAX_STRING`, `LAX_INT64`, `LAX_FLOAT64`, `LAX_BOOL`) which coerces across JSON types. But the implementation uses `SAFE.STRING(...)`, which is the **strict** variant — it returns `NULL` when the underlying JSON value is a number or boolean rather than stringifying it.

If that reading of the BigQuery docs is correct (the previous session could not fully confirm via WebFetch because `cloud.google.com/bigquery/docs/...` is heavily JS-rendered and returns nav-only), then the following staging fields are silently `NULL`-ing out on every run and nobody would notice until a clinician asks why the MEASUREMENT table has no numbers:

- `stg_fhir__observation_lab.value_number` — via `json_number('raw', 'valueQuantity.value')`
- `stg_fhir__observation_lab.range_low` / `range_high` — via `safe.string(rr.low.value)` inside the unnest
- `stg_fhir__patient.deceased_bool` — via `safe_cast(json_string('raw', 'deceasedBoolean') as bool)`
- Possibly others — grep for `json_number(` and `json_string(` in `dbt/models/staging/`

**What to do:**

1. **Confirm the semantic first.** Don't blindly rewrite the macros. Either:
   - Get a conclusive quote from the BigQuery JSON functions reference (try `https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions` — if WebFetch returns nav-only, try the mirror at `datatovalue.blog` / `medium.com/data-engineers-notes` / `owox.com/blog` which were surfaced via WebSearch and contain examples), **or**
   - Ask the user whether they have BigQuery access to run one validation query: `SELECT SAFE.STRING(JSON '42'), LAX_STRING(JSON '42')` — the first should return `NULL`, the second should return `"42"`. That one query settles it.
2. **If confirmed buggy**, rewrite the macros in `dbt/macros/json_lax.sql` to use the LAX family:
   - `json_string(col, path)` → `LAX_STRING({{ col }}.{{ path }})`
   - `json_number(col, path)` → `LAX_FLOAT64({{ col }}.{{ path }})` (returns FLOAT64, cast downstream if NUMERIC precision matters — `SAFE_CAST(LAX_FLOAT64(...) AS NUMERIC)` is fine)
   - `json_ts(col, path)` → `SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', LAX_STRING({{ col }}.{{ path }}))`
   - `json_date(col, path)` → `SAFE.PARSE_DATE('%Y-%m-%d', LAX_STRING({{ col }}.{{ path }}))`
3. **Then grep the staging models for hand-rolled `safe.string(x.low.value)`** patterns (e.g. the `range_low` / `range_high` extraction in `stg_fhir__observation_lab.sql` lines 95–104) and fix those too — they have the same bug.
4. **Do not add a dbt test** for this until you can actually run dbt against BQ — there's no point asserting semantics you can't execute.

## Other lower-priority follow-ups (from test report §9)

- **§9.2 Observation category filter is narrow.** `stg_fhir__observation_lab.sql:35-40` only matches category codes `('laboratory', 'lab')`. Some feeds use different casing or only a display string. Worth a spot-check against the Chile HAPI before trusting the lab/non-lab split — missed lab observations silently drop into `OBSERVATION` instead of `MEASUREMENT`.
- **§9.3 `ndjson_to_bq.py` re-load is not idempotent.** Rerunning for the same `--run-date` doubles rows via `WRITE_APPEND`. Fix would be a partition truncate or a delete-before-load guarded by `_ingest_run_date`. dbt dedup papers over it downstream but the raw layer duplication is real.
- **§9.4 Poll timeout default (3600s) is correct for production** — don't lower it based on local testing.

## Environment notes

- **Repo is at `/Users/sudoshi/Github/FHIR2OMOP`** on the previous machine. Path may differ on the new machine — adjust commands accordingly.
- **Python 3.9 worked** on the previous machine even though `README.md` says "python 3.11+", because `hapi_export.py` and `ndjson_to_bq.py` both start with `from __future__ import annotations`. If you need to run `dbt-bigquery`, check its minimum Python version — it may require ≥3.8 or ≥3.10 depending on the release.
- **Dependencies**: on the previous machine, `requests`, `tenacity`, and `google-cloud-storage` were installed ad-hoc via `pip3 install`. If the new machine is fresh, run `pip3 install -r ingest/requirements.txt` first. `google-cloud-bigquery` is also in that file but is only needed if you're touching `ndjson_to_bq.py`.
- **No GCP credentials on the previous machine.** If the user has `gcloud auth application-default login` set up on the new machine, you may finally be able to test the BigQuery load path in `ndjson_to_bq.py`. Check with the user before assuming credentials are present — the ingest scripts fail loud if they aren't.
- **Public HAPI server is slow.** Single-type `$export` takes ~3–4 minutes to progress from QUEUED → IN_PROGRESS → 200 because of backlog from other users. Use `--poll-timeout-s 900` minimum. Not a bug in the scripts — Chile's self-hosted HAPI will be much faster.
- **`CLAUDE.md` says the current date is 2026-04-09** and invokes "GSD Mode" — act decisively, don't narrate, use parallel tool calls, commit only when asked.

## Key files to read (in order)

1. `HAPI-FHIR-to-OMOP-on-BigQuery-Research-Brief.md` (project root) — the design spec. §4 is the lab mapping table, §6 is the phase plan.
2. `ingest/HAPI-PUBLIC-TEST-REPORT.md` — what the previous session did, with exact numbers and a summary table at §10.
3. `ingest/hapi_export.py` and `ingest/test_hapi_public.py` — the fixed file and the harness that proves the fix.
4. `dbt/macros/json_lax.sql` — the macro file for the §9.1 follow-up.
5. `dbt/models/staging/stg_fhir__observation_lab.sql` — the critical staging model where `value_number` / `range_low` / `range_high` live.
6. `dbt/models/marts/omop/measurement.sql` — the heaviest mart model per brief §6; reads the staging model above.

## How to reproduce the passing tests on the new machine

```bash
cd /path/to/FHIR2OMOP                # adjust for new machine
pip3 install -r ingest/requirements.txt

# Fastest smoke test (~5 min, most of it waiting on HAPI's job queue)
python3 ingest/test_hapi_public.py \
  --types Patient \
  --poll-interval-s 10 \
  --poll-timeout-s 900

# Expected output tail:
#   === summary ===
#     Patient: ~24,000 rows across ~25 files
#   PASS
```

If the harness fails with `"Not valid NDJSON"` or `"Got FHIR Binary envelope"`, the Accept-header fix was somehow reverted — re-check `stream_to_gcs` at `ingest/hapi_export.py` around line 160 for the `headers = {"Accept": "application/fhir+ndjson"}` override.

## What the user is most likely to ask next

- "Continue with the dbt debugging" → start with §9.1 above.
- "Test against BigQuery" → need GCP credentials; ask first.
- "Deploy to Composer" → read `orchestration/airflow/README.md` and the DAG at `orchestration/airflow/dags/fhir2omop_nightly.py`; static review only on the previous machine.
- "Run the whole pipeline end-to-end" → not possible without GCP; clarify which slice they mean.

## Things the previous session explicitly did NOT do

- Commit anything. Per `CLAUDE.md`: commit only when asked.
- Push to any remote.
- Touch the dbt project files.
- Modify `ndjson_to_bq.py`, the Makefile, or the Airflow DAG.
- Save project state to auto-memory — the memory system at `/Users/sudoshi/.claude/projects/-Users-sudoshi-Github-FHIR2OMOP/memory/` is empty and that's fine. If you learn durable preferences or surprising facts from the user, save them per the auto-memory instructions in your system prompt.

---

**Proceed.** Start by reading the research brief and the test report, then ask the user whether they want to (a) continue with the §9.1 dbt macro fix, (b) pivot to testing something that needs GCP credentials, or (c) do something else entirely.

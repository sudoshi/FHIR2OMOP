# FHIR2OMOP DEVLOG

Running journal of development work on the FHIR2OMOP starter kit. Most
recent entries at the top. Each entry should answer: what changed, why,
and what's still open.

---

## 2026-04-09 — §9.1 / §9.2 / §9.3 follow-ups from the public HAPI test report

Cleared the three highest-value items from `ingest/HAPI-PUBLIC-TEST-REPORT.md`
§9. None of this has been validated against BigQuery yet — no credentials on
this machine — so these are paper fixes until the Chile HAPI integration is
wired up. All three were identified by the previous session but deferred.

### §9.1 — LAX JSON extractors in `dbt/macros/json_lax.sql`

**Problem.** The macro file was named `json_lax.sql` but the implementation
used `SAFE.STRING(...)`, which is the *strict* variant of BigQuery's JSON
extractors: it returns NULL when the underlying JSON type is anything other
than a JSON string. `SAFE.` only changes "error" → "NULL", not "coerce".

In FHIR, `Observation.valueQuantity.value` is a JSON number and
`Patient.deceasedBoolean` is a JSON boolean — so every row on every run was
silently nulling these columns:

- `stg_fhir__observation_lab.value_number`
- `stg_fhir__observation_lab.range_low` / `range_high`
- `stg_fhir__observation_nonlab.value_number`
- `stg_fhir__patient.deceased_bool`

Nobody would notice until a clinician asked why the MEASUREMENT table had no
numbers.

**Fix.** Rewrote the macros to use the LAX family (`LAX_STRING`,
`LAX_FLOAT64`, `LAX_BOOL`), which coerce across JSON scalar types. Added a
new `json_bool` macro backed by `LAX_BOOL` and switched
`stg_fhir__patient.deceased_bool` to use it directly (instead of the awkward
`SAFE_CAST(<string> AS BOOL)` chain). Also fixed the hand-rolled
`range_low` / `range_high` subqueries in `stg_fhir__observation_lab.sql`
which had the same bug outside the macro path.

Header comment in `json_lax.sql` now spells out the strict-vs-lax semantics
so the trap doesn't recur.

**Files:**
- `dbt/macros/json_lax.sql`
- `dbt/models/staging/stg_fhir__observation_lab.sql` (lines 94–107)
- `dbt/models/staging/stg_fhir__patient.sql` (line 38)

**Audit.** Every other hand-rolled `safe.string(...)` callsite across the
staging layer operates on fields that are JSON strings per the FHIR R4 spec
(Reference.reference, Identifier.value, Address.*, CodeableConcept.coding[]
fields, Extension.url, etc.). Left alone — they're correct today.

### §9.2 — Broadened Observation lab / non-lab category filter

**Problem.** `with_category` CTE in `stg_fhir__observation_lab.sql` (and a
duplicate in `stg_fhir__observation_nonlab.sql`) matched only on
`category[].coding[].code in ('laboratory', 'lab')`. Feeds that populate
`coding[].display` or `CodeableConcept.text` but leave the code blank (or
use a non-standard code) were silently dropping their labs into
OBSERVATION instead of MEASUREMENT.

**Fix.** Extracted into a new shared macro
`dbt/macros/is_observation_lab.sql` so the two staging models can't drift
out of sync on what counts as a lab. Broadened the match to include
`coding[].display` and `CodeableConcept.text`, all case-insensitive, with
the same `('laboratory', 'lab')` target set. Uses `lax_string` for
consistency with the §9.1 rewrite.

Filter is intentionally permissive: a missed lab silently dropping into
OBSERVATION is harder to notice than an extra row landing in MEASUREMENT.
Spot-check against real Chile HAPI data and tighten here if needed.

**Files:**
- `dbt/macros/is_observation_lab.sql` (new)
- `dbt/models/staging/stg_fhir__observation_lab.sql`
- `dbt/models/staging/stg_fhir__observation_nonlab.sql`

### §9.3 — `ndjson_to_bq.py` is now idempotent per `--run-date`

**Problem.** `load_file()` uses `WRITE_APPEND` because a single resource
type normally spans many NDJSON files and each is loaded in its own job.
Without any guard, rerunning for the same `--run-date` doubled every row in
`fhir_raw.<Resource>`. dbt staging dedupes by `(resource_id, last_updated)`
so the marts layer papered over it, but the raw layer duplication was real
and would break any row-count audit.

**Fix.** Added `delete_existing_run()` which runs a parameterized
`DELETE FROM <table> WHERE _ingest_run_date = @run_date` before the *first*
append for each resource type. The main loop tracks already-cleared types
in a `cleared: set[str]` so subsequent files for the same type still
append correctly — clearing mid-run would wipe what we just loaded. The
table is partitioned on `_ingest_run_date`, so the DELETE touches exactly
one partition and is a cheap no-op on a first-ever run.

**File:** `ingest/ndjson_to_bq.py` (new function + main-loop wiring)

### Explicitly not done

- **§9.4** — poll timeout default (3600s) is correct for production; the
  handoff flagged this as "don't lower it based on local testing."
- **No dbt tests added** for §9.1 — can't execute against BigQuery without
  credentials, so there's no point asserting semantics we can't run.
- **No GCP-side validation** — all of these are paper fixes until someone
  runs them against a real BigQuery dataset. The public HAPI harness at
  `ingest/test_hapi_public.py` still passes end-to-end for the ingestion
  half of Composition A.

### Open follow-ups

- Wire up GCP application-default credentials on this machine so the
  BigQuery load + dbt transform path can finally be exercised.
- Once credentials are in place: run the full loader against a real dataset
  and verify that `value_number` / `range_low` / `range_high` /
  `deceased_bool` actually populate (the §9.1 fix).
- Spot-check the lab / non-lab split against a real feed (the §9.2
  broadening).
- Run `ndjson_to_bq.py` twice for the same `--run-date` and confirm the
  row count stays flat (the §9.3 fix).

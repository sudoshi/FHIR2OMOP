# FHIR2OMOP DEVLOG

Running journal of development work on the FHIR2OMOP starter kit. Most
recent entries at the top. Each entry should answer: what changed, why,
and what's still open.

---

## 2026-04-09 — Runbook TUI: connectivity checks, hashing smoke test, behavioural tests

Second pass on `tools/runbook/` the same day. Three related changes:

### Pre-flight connectivity checks (`tools/runbook/connectivity.py`, new)

The original TUI collected ~15 inputs and then started executing
stages. If the HAPI URL was wrong, or gcloud hadn't been auth'd, or
the dbt profile pointed at a project the user couldn't see, you'd
only find out minutes into the run, inside stage 1 or 2. The first
real Chile run is not the time to discover that.

Added a `run_all_checks(cfg)` pre-flight phase that runs before any
stage and returns a `ConnectivityReport` of 10 checks, each with a
level (`pass` / `warn` / `fail` / `skip`), a one-line summary, and a
remediation hint:

| # | Check | What it verifies |
|---|---|---|
| 1 | `gcloud_auth` | `gcloud auth list` has an active account |
| 2 | `gcloud_adc` | `gcloud auth application-default print-access-token` works |
| 3 | `bq_project` | `bq ls --project_id=…` succeeds |
| 4 | `gcs_bucket` | `gcloud storage ls gs://…` — WARN if bucket missing (bootstrap will create it), FAIL on permission errors |
| 5 | `hapi_metadata` | HTTP GET `{base}/metadata`, parses as CapabilityStatement, reports fhirVersion; 401 is WARN not FAIL |
| 6 | `athena_vocab` | Zip exists, is a real zip, and contains `CONCEPT.csv` / `CONCEPT_RELATIONSHIP.csv` / `VOCABULARY.csv` |
| 7 | `dbt_version` | `dbt --version` works and mentions a BigQuery adapter |
| 8 | `dbt_debug` (slow) | `dbt debug --target …` — exercises profile + OAuth + real BQ query |
| 9 | `pepper_source` | Resolves the configured pepper source without logging the value (WARN if < 8 chars) |
| 10 | `rscript` | `Rscript --version` on `$PATH` (only if DQD is enabled) |

Checks run automatically after the wizard and before the stage loop.
Three new CLI flags:

- `--check-connectivity` — standalone mode; run only the checks and
  exit 0 (all pass) or 3 (any fail). Bypasses the "Reuse this
  config?" prompt so `make runbook-check-connectivity` is
  scriptable.
- `--skip-connectivity` — explicit opt-out on a real run. Discouraged.
- `--skip-slow-checks` — skip `dbt_debug` only; everything else still
  runs.

Failure handling: on any FAIL, the TUI prompts `abort` /
`rerun_wizard` / `proceed` (default abort). `rerun_wizard` re-walks
the wizard with current answers as defaults, re-saves, and re-runs
the checks before continuing.

Two real bugs caught and fixed while testing against the public
`hapi.fhir.org/baseR4`:

1. **HAPI body truncation.** First run got "non-JSON response" on a
   request that was clearly valid JSON — I was only reading 8192
   bytes of a CapabilityStatement that can exceed 100KB. Bumped to a
   4MB read cap.
2. **Config validation blocked the mode.** A local-only issue like
   "vocab zip missing" short-circuited before connectivity ran —
   pointless in a mode whose whole job is to test network/system
   reachability independent of local files. Downgraded config
   issues to a yellow warnings panel in `--check-connectivity` mode
   so the checks still execute.

Rendering lives in `ui.py::render_connectivity_report` so
`connectivity.py` stays free of `rich`/`questionary` imports. The
runner accepts a `progress_cb` so the UI can emit a live `· running
check: …` line per check — matters for the `dbt debug` case, which
can take 10–30s and would otherwise look hung.

Integration points:

- `tools/runbook/connectivity.py` — new module (~450 lines)
- `tools/runbook/__main__.py` — new flags, wiring, failure-prompt loop
- `tools/runbook/ui.py` — `render_connectivity_report`,
  `render_connectivity_progress`, `ask_connectivity_failure`
- `Makefile` — new `runbook-check-connectivity` target
- `docs/RUNBOOK_TUI.md` — "Pre-flight connectivity checks" section
  with check table, failure-handling description, skip modes, and
  sample output block

### Hashing: cleaner pepper wiring and a dedicated smoke test

The first TUI revision passed the pepper to dbt via
`--vars '{hash_person_source_value: true, person_source_value_pepper: "{{ env_var(\'DBT_PEPPER\') }}"}'`
which worked but was ugly and risked leaking the Jinja reference into
logs or the dry-run preview. Refactored so:

- `dbt/macros/hash_mrn.sql` now reads
  `var('person_source_value_pepper', env_var('DBT_PEPPER', ''))` —
  dbt picks up the pepper from `$DBT_PEPPER` as a fallback when the
  var isn't explicitly set. Error message updated to mention both.
- `tools/runbook/stages.py::_dbt_vars_args()` now returns just
  `["--vars", "{hash_person_source_value: true}"]` when hashing is
  enabled. No pepper reference in argv, ever. The macro picks it up
  from the environment via `env_var()`.
- `docs/WAREHOUSE_VALIDATION_RUNBOOK.md` §5 sample command updated to
  `DBT_PEPPER='...' dbt parse --vars '{hash_person_source_value: true}'`.
- `README.md` reflects the new `$DBT_PEPPER`-or-var option.

New `--check-hashing` flag and `_run_hashing_smoke_test()` in
`__main__.py`: forces `hash_person_source_value=true` (even if the
saved config has it off), resolves the pepper, runs just `dbt deps` +
`dbt parse` from the `dbt_parse` stage, and exits. Lets you verify
secret wiring end-to-end without committing to a full run.

Corresponding Make target: `make runbook-check-hashing`.

### Behavioural test suite (`tools/runbook/test_runbook.py`, new)

Six unittest tests covering the pieces that would silently drift if
someone edited them wrong:

1. `run_command` returns 127 with a helpful log line when the binary
   is missing (the FileNotFoundError path added during the pepper
   refactor).
2. A `check_only` stage correctly reports failure when any validator
   fails but records all validator results regardless.
3. `RunbookConfig.validate(base_dir=…)` is stable across cwd changes
   so the `dbt_project_dir` check doesn't depend on where the TUI
   was launched from.
4. `exit_criteria_statuses` reads results by name (`raw_tables`,
   `final_person_measurement`, `measurement_gaps`, `observation_gaps`)
   so an old stale result can't contaminate the criterion table.
5. `_dbt_vars_args` with hashing enabled returns the plain
   `{hash_person_source_value: true}` vars string — no pepper
   reference.
6. `_run_hashing_smoke_test` does not mutate the caller's config
   (uses `dataclasses.replace` for the forced-hashing copy).

All 6 pass against the venv at `tools/runbook/.venv/`.

### Verified

- `tools/runbook/.venv/bin/python -m unittest tools.runbook.test_runbook -v`
  — 6/6 pass in 0.004s
- `make runbook-check-connectivity` on a machine with no gcloud auth —
  correctly reports 1 pass (HAPI public server), 7 fails (gcloud,
  bq, gcs, dbt, pepper), 2 skips (vocab, Rscript); Python exits 3,
  Make wraps that as its own "Error 3" and exits 2
- HAPI connectivity check against `https://hapi.fhir.org/baseR4` —
  PASS, "HAPI FHIR Server, FHIR 4.0.1"
- `--dry-run` with a saved config — zero mentions of "CONNECTIVITY" in
  output (dry-run stays network-free)
- `--skip-connectivity --dry-run` — also zero CONNECTIVITY output
- `--list-stages` — renders the 11-stage table and exits 0
- `--check-connectivity` now bypasses the "Reuse this config?" prompt

### Open follow-ups

- Still not verified against a real GCP project. Every bug caught so
  far has been either local or against public HAPI.
- The `dbt_debug` check shells out and blocks ~10–30s; consider
  running it in a background thread with a spinner, but not worth
  it for a one-shot pre-flight.
- The `gcs_bucket` check currently treats "bucket missing" as WARN
  because bootstrap creates it. Could tighten to actually try a
  dry-run create (`gcloud storage buckets create --dry-run`) to
  verify the IAM permission. Not done — Chile's service account
  policy is still being worked out.

---

## 2026-04-09 — Interactive TUI wrapper for the warehouse validation runbook

Added `tools/runbook/`, a `rich` + `questionary` TUI that walks the user
through every step of `docs/WAREHOUSE_VALIDATION_RUNBOOK.md`. The runbook
is long, opinionated, and has ~15 distinct inputs (GCP project/region,
landing bucket, HAPI URL, vocab zip path, dbt target, run window,
person_source_value pepper, etc.). For a first real pipeline run against
Chile's HAPI instance, the odds of fumbling an env var or skipping a
validation query are high enough that a guided wrapper is cheaper than
the inevitable re-run.

### What the TUI does

- **Wizard:** collects every input with sensible defaults, hides HAPI
  prompts when the user says data is already in `fhir_raw`, hides vocab
  prompts when `omop_vocab` is already loaded, and only asks about
  hashing/pepper when production hashing is enabled.
- **Secret resolution:** the pepper is never written to the config file.
  The config stores a `pepper_source` (one of `prompt`, `env`, `dotenv`,
  `pass`, `gcloud`) plus an optional `pepper_ref` (env var name, dotenv
  key, `pass` path, or `gcloud secrets` name). At run start, the pepper
  is resolved once, placed in `$DBT_PEPPER`, and read by the dbt hashing
  macro via `env_var()` when hashing is enabled — so it never appears
  in argv, logs, or dry-run previews.
- **Dry-run:** `python -m tools.runbook --dry-run` loads or collects the
  config and prints the full list of inputs + every shell command + every
  BigQuery validator that would run, with no side effects. Useful before
  ever pointing it at a real project.
- **Resume:** stage progress is persisted to `.runbook_state.json`.
  `--resume` picks up after the last completed stage. On failure the
  user gets retry / continue / abort prompts.
- **Validators:** the BigQuery checks from `§4` and `§7` of the runbook
  (raw table presence, OMOP row counts, `measurement_concept_id = 0`
  rate, `unit_concept_id = 0` rate, `person_id IS NULL` count, etc.) are
  executed via `bq --format=json` and rendered as PASS/FAIL rows with a
  summary line. Judgment calls ("is 12% unknown-concept acceptable?")
  stay with the human — the TUI just surfaces the numbers.
- **Exit report:** at the end, a rich table maps the §10 exit criteria
  onto stage outcomes and tells the user which failed, then points them
  at `logs/runbook_<ts>.log` and the next-action items from the runbook
  itself (expanding seed CSVs, manually reviewed patient round-trip).

### Structure

```
tools/runbook/
  __init__.py
  __main__.py          # argparse + orchestration (355 lines)
  config.py            # RunbookConfig + SecretResolver (259 lines)
  state.py             # .runbook_state.json resume (127 lines)
  stages.py            # 11 stages + BQ validators + runner (688 lines)
  ui.py                # rich + questionary wizard (404 lines)
  requirements.txt     # rich, questionary, python-dotenv
```

Runbook Make targets now include `runbook`, `runbook-dry-run`,
`runbook-resume`, `runbook-check-connectivity`,
`runbook-check-hashing`, and `runbook-install`. Stage list is
data-driven from `stages.STAGES`, so
adding a §11 or splitting a stage means editing one list.

### Deliberate non-goals

- **No judgment automation.** The TUI surfaces unknown-concept rates and
  row counts; it does not try to decide whether they're acceptable. The
  runbook is explicit that the first-run goal is "separate structural
  failures from expected mapping backlog" — that needs a human.
- **No state machine across runs.** The resume support is scoped to a
  single run; if you change the config mid-run, you rerun affected
  stages. This keeps the state file dumb and inspectable.
- **Not a replacement for the runbook itself.** The Markdown runbook
  stays authoritative. The TUI's stage descriptions all reference their
  `§N` section so reviewers can trace them back.

### Files

- `tools/runbook/*` (new package)
- `tools/__init__.py` (new, empty)
- `Makefile` — added `runbook`, `runbook-dry-run`, `runbook-resume`,
  `runbook-install` targets
- `.gitignore` — added `runbook_config.json`, `.runbook_state.json`,
  `logs/runbook_*.log`
- `docs/RUNBOOK_TUI.md` — end-user guide (this entry is the dev-facing
  narrative; `RUNBOOK_TUI.md` is the operator-facing "how to use it")

### Verified locally

- `python -m tools.runbook --list-stages` renders the 11-stage table
- `python -m tools.runbook --dry-run --config <path>` loads a saved
  config, renders the input summary and secret-source panel, and prints
  every command across all 11 stages without executing anything
- Broken config (missing `gcp_project`, `pepper_ref`, nonexistent
  `dbt_project_dir`, etc.) surfaces all 5 errors in a red panel instead
  of crashing
- When production hashing is enabled, the runbook places the pepper in
  `$DBT_PEPPER` and the dbt hashing macro reads it via `env_var()`, so
  the pepper does not appear in argv or subprocess logs

### Open follow-ups

- Point it at a real GCP project once application-default credentials
  are configured on this machine. Until then everything is still paper.
- Consider adding a `--yes` / non-interactive batch flag for CI once the
  single-user flow is stable. Not needed for the first-run use case.
- The `prereq` stage's `dbt --version` check will fail on a machine
  without dbt-bigquery installed — deliberately, so the user fixes it
  before continuing, but the error message could point them at
  `pip install dbt-bigquery`.

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

# How to use the Warehouse Validation Runbook TUI

`tools/runbook/` is an interactive Python wizard that walks you through
the first warehouse-backed validation run described in
[`WAREHOUSE_VALIDATION_RUNBOOK.md`](./WAREHOUSE_VALIDATION_RUNBOOK.md).

The Markdown runbook is still authoritative. This document only covers
how to drive the TUI that wraps it.

---

## What the TUI does for you

- Collects every input the runbook needs (GCP project, region, HAPI URL,
  vocab zip path, dbt target, run date, etc.) with sensible defaults.
- Saves your inputs to a config file so you can re-run or resume without
  re-answering.
- Resolves the `person_source_value` pepper from one of five sources
  (`prompt`, `env`, `dotenv`, `pass`, `gcloud secrets`) without ever
  writing it to disk.
- Runs each runbook stage end-to-end: subprocess output is teed to the
  console and to `logs/runbook_<timestamp>.log`.
- Executes the BigQuery validation queries from §4 and §7 of the
  runbook and renders PASS/FAIL rows.
- Resumes from where you left off if a stage fails or you Ctrl-C.
- Offers a `--dry-run` mode that shows every input and every command
  without executing anything.

It does **not** try to make judgment calls for you. When a validator
says "unknown-concept rate is 12.4%," it's still on you to decide
whether that's acceptable for the first run.

---

## Prerequisites

On the machine that will run the TUI:

- Python 3.11+
- `gcloud`, `bq`, and application-default credentials already set up
  (`gcloud auth application-default login`)
- `dbt-bigquery` installed and importable
- If you plan to run the DQD stage: `Rscript` + OHDSI DQD dependencies
- Network access to the HAPI FHIR server (unless data is already in
  `fhir_raw`)
- Write access to the target GCP project

Install the TUI's Python dependencies:

```bash
make runbook-install
```

This creates a dedicated venv at `tools/runbook/.venv/` and installs
`rich`, `questionary`, and `python-dotenv` into it. A venv is used
(instead of installing into system Python) because Ubuntu 24.04 and
other modern distros enforce PEP 668 — bare `pip install` against
system Python is refused.

All `make runbook*` targets invoke `tools/runbook/.venv/bin/python`
directly, so the TUI does not depend on `python`/`python3` being on
your `$PATH` in any particular state. To tear the venv down and
reinstall, run `make runbook-clean && make runbook-install`.

---

## Quick start

```bash
# 1. Dry-run first — collects inputs, previews commands, runs nothing.
make runbook-dry-run

# 2. Real run once you're happy with the preview.
make runbook

# 3. If it stops mid-run, resume from the last completed stage.
make runbook-resume
```

That's the whole day-to-day surface. Everything below is detail.

---

## The full CLI

```bash
python -m tools.runbook [options]
```

| Option | Purpose |
|---|---|
| `--dry-run` | Collect inputs, render the summary and full command preview, then exit. No subprocesses run. |
| `--resume` | Skip the wizard and resume from `.runbook_state.json`. Requires an existing config. |
| `--list-stages` | Print the 11-stage table and exit. Non-interactive. |
| `--config PATH` | Override the config file location (default: `runbook_config.json` at repo root). |
| `--state PATH` | Override the state file location (default: `.runbook_state.json` at repo root). |
| `--no-save-config` | Don't write the collected config back to disk after the wizard. |
| `-h`, `--help` | argparse help. |

---

## The stages

The TUI runs 11 stages, each mapped to a section of
`WAREHOUSE_VALIDATION_RUNBOOK.md`:

| # | Section | Stage | Notes |
|---|---|---|---|
| 1 | §1 | Prerequisites | gcloud/bq/python/dbt presence + active gcloud account |
| 2 | §2 | Bootstrap datasets & buckets | `make datasets`, `make buckets` |
| 3 | §2 | Load Athena vocabulary | skippable if `omop_vocab` already loaded |
| 4 | §3 | Land raw FHIR batch | skippable if data is already in `fhir_raw` |
| 5 | §4 | Validate raw layer | BigQuery row-count + presence checks |
| 6 | §5 | Parse dbt | `dbt deps` + `dbt parse` |
| 7 | §6 | Seed and build dbt | staging → intermediate → omop, in order |
| 8 | §7 | Validate OMOP layer | row counts, unknown-concept rates, null-person checks |
| 9 | §8 | Run relational tests | `dbt test` |
| 10 | §9 | Run DQD | skippable; OHDSI Data Quality Dashboard |
| 11 | §10 | Exit criteria report | summary table, next-steps panel |

See the stage list yourself:

```bash
python -m tools.runbook --list-stages
```

---

## The wizard (what it will ask you)

On first run (or when you re-run the wizard), you'll be asked for:

**GCP / BigQuery**
- GCP project ID (e.g. `chile-omop-prod`)
- GCP region (default: `southamerica-west1`)
- GCS landing bucket (default: `gs://<project>-fhir-landing`)

**Source FHIR server**
- "Is data already loaded in `fhir_raw`?" — yes skips HAPI export + load
- HAPI FHIR base URL (only if not skipping)
- HAPI basic-auth user (optional)

**Run window**
- Run date (`YYYY-MM-DD`, empty = today)
- `--since` timestamp for the export (RFC3339, empty = no filter)

**Vocabulary**
- "Is the Athena vocab already loaded into `omop_vocab`?" — yes skips
  the load
- Path to `vocabulary_download_v5.zip` (only if not skipping)
- Vocab dataset name (default: `omop_vocab`)

**dbt**
- dbt project dir (default: `./dbt`)
- dbt target — `dev` or `prod`
- `DBT_PROFILES_DIR` (empty = `~/.dbt/profiles.yml`)

**Hashing and pepper**
- "Hash `person_source_value`?" — yes enables production behavior
- Pepper source — one of:
  - **Prompt me each run** (never stored, asked with masked input)
  - **Environment variable** (you supply the var name)
  - **dotenv file** (default path `.env.local`, you supply the key name)
  - **pass** (password-store path)
  - **gcloud secrets** (secret name)

**DQD**
- "Run DQD at the end?" — yes runs `quality/run_dqd.R` after `dbt test`

Your answers (minus any secrets) are written to
`runbook_config.json`. Edit that file directly to tweak later runs, or
delete it to get a fresh wizard.

---

## Secret handling

The TUI is strict about not persisting the pepper:

- The config file never contains the pepper value — it only contains
  the **source** (`prompt` / `env` / `dotenv` / `pass` / `gcloud`) and
  the **reference** (env var name, dotenv key, pass path, or gcloud
  secret name).
- At run start, the pepper is resolved once from the configured source,
  placed in the subprocess environment as `$DBT_PEPPER`, and referenced
  from dbt via `{{ env_var('DBT_PEPPER') }}` inside the `--vars`
  argument. This means the pepper never appears in `argv`, in the
  dry-run preview, or in `logs/runbook_*.log`.
- If the configured source fails (env var unset, `pass` entry missing,
  gcloud secret inaccessible) the run fails fast — before any stage
  executes — so you can't end up in a half-loaded state.
- The dry-run preview shows *where* the pepper will come from but never
  *what* it is.

### Source cheat sheet

| Source | What to configure | How it's resolved |
|---|---|---|
| `prompt` | — | `questionary.password()` at run start |
| `env` | env var name (e.g. `FHIR2OMOP_PEPPER`) | `os.environ[name]` |
| `dotenv` | path + key name | `python-dotenv` reads the key from the file |
| `pass` | entry path (e.g. `fhir2omop/pepper`) | `pass show <entry>` |
| `gcloud` | secret name | `gcloud secrets versions access latest --secret <name>` |

Choose `prompt` if you're running interactively and don't want a
persistent secret anywhere. Choose `pass` or `gcloud` if you want to
stay unattended without a plaintext file. Choose `dotenv` if you're
already using `.env.local` for other project secrets.

---

## Dry-run preview

```bash
python -m tools.runbook --dry-run
# or: make runbook-dry-run
```

Behavior:

1. Loads `runbook_config.json` if it exists. If it doesn't, runs the
   wizard interactively.
2. Renders a full table of the collected inputs (with derived values
   like `gcs_landing (effective)` and `run_date (effective)`).
3. If hashing is enabled, shows a "Secret source" panel describing
   where the pepper will come from — without resolving or printing it.
4. Walks all 11 stages and, for each, prints:
   - The shell commands that would execute (with env vars inlined in
     a `dim` style so they're visible but secondary).
   - The BigQuery validator descriptions (not the SQL — the SQL is in
     the logs when real-run).
5. If `cfg.validate()` finds any problems (missing project, nonexistent
   dbt project dir, unresolvable vocab zip, etc.) it prints them at the
   top in a red panel.
6. Never runs a subprocess.

Dry-run is also scriptable: if you pass `--config /path/to/config.json`
and the file exists, the wizard is skipped entirely and the preview is
rendered non-interactively.

---

## Resume semantics

Stage progress is persisted to `.runbook_state.json` after every state
change. The file contains:

- When the run started and when it was last updated
- Per-stage records: status (`pending` / `in_progress` / `completed` /
  `failed` / `skipped`), start and finish timestamps, and the last
  error message if any
- The path to the log file and the path to the config file in use

If `.runbook_state.json` becomes corrupt (e.g. a half-written JSON), the
next run renames it to `.runbook_state.json.bak` and starts fresh.

Resuming:

```bash
make runbook-resume
# or: python -m tools.runbook --resume
```

This skips the wizard entirely and restarts the stage loop. Stages
already marked `completed` or `skipped` are reported and skipped;
everything else is re-run in order.

On failure you're asked:

- **Retry** — re-run the same stage (e.g. after fixing a permission
  issue in another terminal)
- **Continue** — mark the stage failed and move on (useful for the
  skippable DQD stage)
- **Abort** — stop the run; state is persisted so you can `--resume`
  later

---

## Logs and output

Every subprocess's combined stdout/stderr is teed to
`logs/runbook_<UTC-timestamp>.log` under the repo root. The TUI prints
the log file path at the start of a real run.

The log also contains:

- A header line before each command with the UTC timestamp and the
  rendered command
- The full SQL text of every BigQuery validator (so you can copy/paste
  into the BigQuery console to reproduce)

`logs/runbook_*.log` is gitignored.

---

## Exit criteria report

At the end of a real run, the TUI prints a table mapping the §10 exit
criteria onto observed stage outcomes:

| Criterion | Status |
|---|---|
| Raw FHIR tables loaded | PASS / FAIL |
| dbt parse + build + test succeeded | PASS / FAIL |
| person and measurement non-zero | PASS / FAIL |
| Unknown-concept rates reviewed | PASS / FAIL |

Followed by a "Next steps" panel pointing at
`seed_test_source_to_concept.csv`,
`seed_unit_source_to_concept.csv`, and the manually-reviewed
patient-round-trip task from the runbook itself.

---

## Troubleshooting

**"Failed to resolve pepper secret: env var 'FHIR2OMOP_PEPPER' is not
set or empty"**
The TUI resolves the pepper before any stage runs. Either export the
env var in the current shell, re-run the wizard and pick a different
source, or pick `prompt` and enter it interactively.

**"bq CLI not found" during validation**
The TUI shells out to `bq` (not the Python BigQuery client) so that
its auth matches what the user sees in the runbook. Install the
Google Cloud SDK and make sure `bq` is on `$PATH`.

**dbt version mismatch in the prereq stage**
Install `dbt-bigquery` into the same Python environment the TUI is
running under. The `runbook-install` Make target only installs the TUI
deps — not dbt itself.

**"athena_vocab_zip not found"**
Either put the Athena download at the path the wizard asks for, or
answer "yes" to "Is the Athena vocabulary already loaded?"

**Stage hangs forever on `dbt build`**
That's dbt, not the TUI. Output is being streamed in real time; check
`logs/runbook_*.log` for the last dbt line and BigQuery console for
the running job.

**I want to start from scratch**
Delete `runbook_config.json` and `.runbook_state.json`. The next run
will re-run the wizard.

---

## Relationship to the Markdown runbook

The Markdown runbook at
[`WAREHOUSE_VALIDATION_RUNBOOK.md`](./WAREHOUSE_VALIDATION_RUNBOOK.md) is
still the source of truth for what should happen and why. The TUI's
stage descriptions all cite their `§N` section so you can trace any
step back. If you find a divergence between the two, the Markdown wins
and the TUI is wrong — open a task to fix `tools/runbook/stages.py`.

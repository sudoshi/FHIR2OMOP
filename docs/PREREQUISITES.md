# Prerequisites

This document collects the prerequisites for running the FHIR2OMOP
pipeline and the warehouse-validation runbook across macOS, Linux, and
Windows.

For the runbook workflow itself, also see
[`RUNBOOK_TUI.md`](./RUNBOOK_TUI.md).

## Common Requirements

These apply on every OS:

- Python `3.11+`
- Google Cloud SDK with `gcloud` and `bq` on `PATH`
- Application Default Credentials configured:
  `gcloud auth application-default login`
- `dbt-bigquery` installed in the Python environment you use for dbt
- Network access to the HAPI FHIR server unless raw data is already in
  `fhir_raw`
- Write access to the target GCP project
- Optional: `Rscript` plus OHDSI DQD dependencies if you want the DQD
  stage

The runbook TUI itself installs only three Python packages:

- `rich`
- `questionary`
- `python-dotenv`

Those are listed in
[`tools/runbook/requirements.txt`](/home/smudoshi/Github/FHIR2OMOP/tools/runbook/requirements.txt).

## Platform Notes

### macOS

- Install Python `3.11+`
- Install Google Cloud SDK so `gcloud` and `bq` are available
- Install `dbt-bigquery`
- Optional: install `Rscript` if you want the DQD stage

The documented `make runbook-install`, `make runbook-dry-run`, and
`make runbook` flow should work well on macOS.

### Linux

- Install Python `3.11+`
- Install Google Cloud SDK so `gcloud` and `bq` are available
- Install `dbt-bigquery`
- Optional: install `Rscript` if you want the DQD stage

Linux is the most direct fit for this repo’s current shell and tooling
assumptions.

### Windows

- Install Python `3.11+`
- Install Google Cloud SDK so `gcloud` and `bq` are available
- Install `dbt-bigquery`
- Optional: install `Rscript` if you want the DQD stage

The runbook Python code is portable, but there are two practical ways to
run it on Windows:

1. Use WSL.
2. Use native Windows Python and invoke `python -m tools.runbook`
   directly.

The Makefile now understands Windows venv layout
(`tools/runbook/.venv/Scripts/python.exe`), but using `make` on Windows
still assumes you have a Make-compatible shell environment available.
If you do not, use the direct Python commands below instead of `make`.

## Recommended Setup Commands

### macOS / Linux

```bash
make runbook-install
make runbook-dry-run
make runbook-check-connectivity
make runbook-check-hashing
```

### Windows PowerShell

```powershell
py -3 -m venv tools/runbook/.venv
.\tools\runbook\.venv\Scripts\python.exe -m pip install --upgrade pip
.\tools\runbook\.venv\Scripts\python.exe -m pip install -r tools/runbook/requirements.txt
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --dry-run
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --check-connectivity
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --check-hashing
```

## Secret Source Notes

Supported runbook pepper sources are:

- `prompt`
- `env`
- `dotenv`
- `pass`
- `gcloud`

On Windows, `pass` is usually the least portable option. In practice,
`prompt`, `env`, `dotenv`, or `gcloud` are the better choices there.

## What The Runbook Checks For

The runbook’s pre-flight checks verify:

- active `gcloud` auth
- Application Default Credentials
- BigQuery project access
- landing bucket visibility
- HAPI `/metadata` reachability
- Athena vocabulary zip validity
- `dbt-bigquery` availability
- `dbt debug` connectivity, unless slow checks are skipped
- pepper resolvability when hashing is enabled
- `Rscript` presence when DQD is enabled

That logic lives in
[`connectivity.py`](/home/smudoshi/Github/FHIR2OMOP/tools/runbook/connectivity.py).

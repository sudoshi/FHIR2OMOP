"""
Stage definitions for the runbook wizard.

Each Stage wraps a section of WAREHOUSE_VALIDATION_RUNBOOK.md. A stage has:

    precheck   - cheap sanity check before running (optional)
    command(s) - shell invocations (for real execution) - shown in dry-run too
    validator  - optional follow-up that runs BigQuery checks and rates the result

Stages are plain dataclasses so --dry-run can render them without any side
effects.
"""
from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import RunbookConfig


# =============================================================================
# Command descriptors
# =============================================================================


@dataclass
class Command:
    """A single shell command. `cwd` is relative to the repo root."""

    argv: list[str]
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    description: str = ""

    def display(self) -> str:
        parts = [shlex.quote(a) for a in self.argv]
        rendered = " ".join(parts)
        if self.cwd:
            return f"(cd {self.cwd} && {rendered})"
        return rendered


# =============================================================================
# Query descriptors (used by validation stages)
# =============================================================================


@dataclass
class BqCheck:
    """A single BigQuery validation query + how to interpret it."""

    name: str
    sql: str
    description: str
    # Optional predicate: (rows) -> (ok, summary_line). Default: show raw rows.
    interpret: Callable[[list[dict[str, Any]]], tuple[bool, str]] | None = None


# =============================================================================
# Stage descriptor
# =============================================================================


@dataclass
class Stage:
    key: str
    title: str
    runbook_section: str
    description: str
    # Callable that, given a config, returns the list of commands to run.
    commands: Callable[[RunbookConfig], list[Command]]
    # Optional pre-run sanity checks (raise or return list[str] of errors)
    precheck: Callable[[RunbookConfig], list[str]] | None = None
    # Optional BQ validators that run after commands
    validators: Callable[[RunbookConfig], list[BqCheck]] | None = None
    # Can the user skip this stage entirely from the wizard?
    skippable: bool = False
    # Is this stage a pure check with no commands? (display hint)
    check_only: bool = False


# =============================================================================
# Stage builders
# =============================================================================

# ----- Stage 0: prereqs ------------------------------------------------------


def _stage_prereq_commands(cfg: RunbookConfig) -> list[Command]:
    return [
        Command(["gcloud", "--version"], description="gcloud CLI present"),
        Command(["bq", "version"], description="bq CLI present"),
        Command(["python", "--version"], description="Python 3.11+"),
        Command(
            ["dbt", "--version"],
            description="dbt-bigquery installed",
        ),
        Command(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            description="gcloud has an active account",
        ),
    ]


# ----- Stage 1: datasets + buckets ------------------------------------------


def _stage_bootstrap_commands(cfg: RunbookConfig) -> list[Command]:
    return [
        Command(
            ["make", "datasets"],
            env={"GCP_PROJECT": cfg.gcp_project, "GCP_REGION": cfg.gcp_region},
            description="Create BigQuery datasets (fhir_raw, omop_stg, omop_cdm, omop_vocab)",
        ),
        Command(
            ["make", "buckets"],
            env={
                "GCP_PROJECT": cfg.gcp_project,
                "GCP_REGION": cfg.gcp_region,
                "GCS_LANDING": cfg.effective_gcs_landing(),
            },
            description="Create landing GCS bucket (idempotent)",
        ),
    ]


# ----- Stage 2: load Athena vocab --------------------------------------------


def _stage_vocab_commands(cfg: RunbookConfig) -> list[Command]:
    if cfg.skip_vocab_load:
        return []
    return [
        Command(
            [
                "python",
                "vocab/load_athena_vocab.py",
                "--zip",
                cfg.athena_vocab_zip,
                "--project",
                cfg.gcp_project,
                "--dataset",
                cfg.vocab_dataset,
                "--location",
                cfg.gcp_region,
            ],
            description="Load Athena vocabulary bundle into omop_vocab",
        )
    ]


def _vocab_validators(cfg: RunbookConfig) -> list[BqCheck]:
    sql = (
        f"SELECT table_name, row_count "
        f"FROM `{cfg.gcp_project}.{cfg.vocab_dataset}.__TABLES__` "
        f"ORDER BY table_name"
    )

    def interpret(rows: list[dict[str, Any]]) -> tuple[bool, str]:
        needed = {"concept", "concept_relationship", "concept_ancestor", "vocabulary"}
        found = {r["table_name"].lower(): int(r.get("row_count", 0) or 0) for r in rows}
        missing = [t for t in needed if t not in found]
        if missing:
            return False, f"missing tables: {', '.join(missing)}"
        zero = [t for t in needed if found[t] == 0]
        if zero:
            return False, f"tables exist but empty: {', '.join(zero)}"
        return True, f"concept={found['concept']:,} relationships={found['concept_relationship']:,}"

    return [BqCheck("vocab_tables", sql, "Vocabulary tables present and non-empty", interpret)]


# ----- Stage 3: HAPI export + NDJSON load -----------------------------------


def _stage_raw_ingest_commands(cfg: RunbookConfig) -> list[Command]:
    if cfg.skip_hapi_export:
        return []
    cmds: list[Command] = []
    hapi_args = [
        "python",
        "ingest/hapi_export.py",
        "--hapi-base-url",
        cfg.hapi_base_url,
        "--gcs-landing",
        cfg.effective_gcs_landing(),
        "--run-date",
        cfg.effective_run_date(),
    ]
    if cfg.since:
        hapi_args += ["--since", cfg.since]
    if cfg.hapi_http_user:
        hapi_args += ["--http-user", cfg.hapi_http_user]
    cmds.append(Command(hapi_args, description="HAPI FHIR $export -> NDJSON -> GCS"))

    cmds.append(
        Command(
            [
                "python",
                "ingest/ndjson_to_bq.py",
                "--project",
                cfg.gcp_project,
                "--dataset",
                "fhir_raw",
                "--location",
                cfg.gcp_region,
                "--gcs-landing",
                cfg.effective_gcs_landing(),
                "--run-date",
                cfg.effective_run_date(),
            ],
            description="Load NDJSON from GCS -> fhir_raw.*",
        )
    )
    return cmds


# ----- Stage 4: raw layer validation -----------------------------------------


def _stage_raw_validate_commands(cfg: RunbookConfig) -> list[Command]:
    return []  # validators only


def _raw_validators(cfg: RunbookConfig) -> list[BqCheck]:
    sql = (
        f"SELECT table_name, row_count "
        f"FROM `{cfg.gcp_project}.fhir_raw.__TABLES__` "
        f"ORDER BY table_name"
    )

    def interpret(rows: list[dict[str, Any]]) -> tuple[bool, str]:
        required = {"Patient", "Observation", "DiagnosticReport", "Specimen"}
        found = {r["table_name"]: int(r.get("row_count", 0) or 0) for r in rows}
        missing = [t for t in required if t not in found]
        if missing:
            return False, f"missing raw tables: {', '.join(missing)}"
        empty = [t for t in required if found[t] == 0]
        if empty:
            return False, f"raw tables empty: {', '.join(empty)}"
        obs = found["Observation"]
        pat = found["Patient"]
        return True, f"Patient={pat:,} Observation={obs:,}"

    return [BqCheck("raw_tables", sql, "Raw FHIR tables populated", interpret)]


# ----- Stage 5: dbt parse ----------------------------------------------------


_DBT_HASH_VARS = (
    "{hash_person_source_value: true}"
)


def _stage_dbt_parse_commands(cfg: RunbookConfig) -> list[Command]:
    env = _dbt_env(cfg)
    parse_cmd: list[str] = ["dbt", "parse"]
    if cfg.hash_person_source_value:
        parse_cmd += ["--vars", _DBT_HASH_VARS]
    return [
        Command(
            ["dbt", "deps"],
            cwd=cfg.dbt_project_dir,
            env=env,
            description="Install dbt packages",
        ),
        Command(
            parse_cmd,
            cwd=cfg.dbt_project_dir,
            env=env,
            description="Parse dbt project (no build)"
            + (" (uses $DBT_PEPPER via dbt env_var)" if cfg.hash_person_source_value else ""),
        ),
    ]


# ----- Stage 6: seed + build -------------------------------------------------


def _dbt_vars_args(cfg: RunbookConfig) -> list[str]:
    return ["--vars", _DBT_HASH_VARS] if cfg.hash_person_source_value else []


def _stage_dbt_build_commands(cfg: RunbookConfig) -> list[Command]:
    env = _dbt_env(cfg)
    v = _dbt_vars_args(cfg)
    return [
        Command(
            ["dbt", "seed", "--target", cfg.dbt_target, *v],
            cwd=cfg.dbt_project_dir,
            env=env,
            description="Load hand-maintained mapping seeds",
        ),
        Command(
            [
                "dbt", "build", "--target", cfg.dbt_target,
                "--select", "tag:staging+", "tag:intermediate+",
                *v,
            ],
            cwd=cfg.dbt_project_dir,
            env=env,
            description="Build staging + intermediate models",
        ),
        Command(
            ["dbt", "build", "--target", cfg.dbt_target, "--select", "tag:omop", *v],
            cwd=cfg.dbt_project_dir,
            env=env,
            description="Build OMOP CDM layer",
        ),
    ]


# ----- Stage 7: OMOP validation ----------------------------------------------


def _stage_omop_validate_commands(cfg: RunbookConfig) -> list[Command]:
    return []  # validators only


def _omop_validators(cfg: RunbookConfig) -> list[BqCheck]:
    p = cfg.gcp_project

    def interpret_counts(rows: list[dict[str, Any]]) -> tuple[bool, str]:
        found = {r["table_name"]: int(r.get("row_count", 0) or 0) for r in rows}
        person = found.get("person", 0)
        meas = found.get("measurement", 0)
        if person == 0:
            return False, "person table is empty"
        if meas == 0:
            return False, "measurement table is empty"
        return True, f"person={person:,} measurement={meas:,}"

    def interpret_measurement(rows: list[dict[str, Any]]) -> tuple[bool, str]:
        if not rows:
            return False, "no rows returned"
        r = rows[0]
        total = int(r.get("measurement_rows", 0) or 0)
        unk_c = int(r.get("unknown_measurement_concept_rows", 0) or 0)
        unk_u = int(r.get("unknown_unit_rows", 0) or 0)
        null_p = int(r.get("null_person_rows", 0) or 0)
        if total == 0:
            return False, "measurement table is empty"
        pct_c = (unk_c / total) * 100 if total else 0
        pct_u = (unk_u / total) * 100 if total else 0
        ok = null_p == 0
        line = (
            f"rows={total:,} unknown_concept={unk_c:,} ({pct_c:.1f}%)"
            f" unknown_unit={unk_u:,} ({pct_u:.1f}%) null_person={null_p}"
        )
        return ok, line

    def interpret_observation(rows: list[dict[str, Any]]) -> tuple[bool, str]:
        if not rows:
            return False, "no rows returned"
        r = rows[0]
        total = int(r.get("observation_rows", 0) or 0)
        unk = int(r.get("unknown_observation_concept_rows", 0) or 0)
        pct = (unk / total) * 100 if total else 0
        return True, f"rows={total:,} unknown_concept={unk:,} ({pct:.1f}%)"

    return [
        BqCheck(
            name="omop_row_counts",
            sql=(
                f"SELECT table_name, row_count "
                f"FROM `{p}.omop_cdm.__TABLES__` "
                f"ORDER BY table_name"
            ),
            description="OMOP CDM row counts by table",
            interpret=interpret_counts,
        ),
        BqCheck(
            name="measurement_gaps",
            sql=(
                "SELECT\n"
                "  COUNT(*) AS measurement_rows,\n"
                "  COUNTIF(measurement_concept_id = 0) AS unknown_measurement_concept_rows,\n"
                "  COUNTIF(unit_concept_id = 0) AS unknown_unit_rows,\n"
                "  COUNTIF(person_id IS NULL) AS null_person_rows\n"
                f"FROM `{p}.omop_cdm.measurement`"
            ),
            description="measurement unknown-concept / null-person gaps",
            interpret=interpret_measurement,
        ),
        BqCheck(
            name="observation_gaps",
            sql=(
                "SELECT\n"
                "  COUNT(*) AS observation_rows,\n"
                "  COUNTIF(observation_concept_id = 0) AS unknown_observation_concept_rows\n"
                f"FROM `{p}.omop_cdm.observation`"
            ),
            description="observation unknown-concept rate",
            interpret=interpret_observation,
        ),
    ]


# ----- Stage 8: dbt test -----------------------------------------------------


def _stage_dbt_test_commands(cfg: RunbookConfig) -> list[Command]:
    return [
        Command(
            ["dbt", "test", "--target", cfg.dbt_target, *_dbt_vars_args(cfg)],
            cwd=cfg.dbt_project_dir,
            env=_dbt_env(cfg),
            description="Run dbt uniqueness and relationship tests",
        )
    ]


# ----- Stage 9: DQD ----------------------------------------------------------


def _stage_dqd_commands(cfg: RunbookConfig) -> list[Command]:
    if not cfg.run_dqd:
        return []
    return [
        Command(
            [
                "Rscript",
                "quality/run_dqd.R",
                cfg.gcp_project,
                "omop_cdm",
                cfg.vocab_dataset,
                cfg.effective_run_date(),
            ],
            description="Run OHDSI Data Quality Dashboard",
        )
    ]


# ----- Stage 10: exit criteria summary --------------------------------------


def _stage_exit_commands(cfg: RunbookConfig) -> list[Command]:
    return []


def _exit_validators(cfg: RunbookConfig) -> list[BqCheck]:
    # Re-run the cheapest checks the user cares about most.
    p = cfg.gcp_project
    return [
        BqCheck(
            name="final_person_measurement",
            sql=(
                "SELECT\n"
                f"  (SELECT COUNT(*) FROM `{p}.omop_cdm.person`) AS person_rows,\n"
                f"  (SELECT COUNT(*) FROM `{p}.omop_cdm.measurement`) AS measurement_rows"
            ),
            description="Exit criterion: person and measurement are non-empty",
            interpret=lambda rows: (
                (
                    int((rows[0].get("person_rows") or 0)) > 0
                    and int((rows[0].get("measurement_rows") or 0)) > 0,
                    f"person={int(rows[0].get('person_rows', 0) or 0):,}"
                    f" measurement={int(rows[0].get('measurement_rows', 0) or 0):,}",
                )
                if rows
                else (False, "no rows returned")
            ),
        )
    ]


# =============================================================================
# Utilities
# =============================================================================


def _dbt_env(cfg: RunbookConfig) -> dict[str, str]:
    env: dict[str, str] = {}
    if cfg.dbt_profiles_dir:
        env["DBT_PROFILES_DIR"] = cfg.dbt_profiles_dir
    return env


# =============================================================================
# Stage registry
# =============================================================================


STAGES: list[Stage] = [
    Stage(
        key="prereq",
        title="Prerequisites",
        runbook_section="§1",
        description="Verify gcloud, bq, python, dbt, and gcloud auth.",
        commands=_stage_prereq_commands,
    ),
    Stage(
        key="bootstrap",
        title="Bootstrap datasets and buckets",
        runbook_section="§2",
        description="Create BigQuery datasets and the GCS landing bucket.",
        commands=_stage_bootstrap_commands,
    ),
    Stage(
        key="vocab",
        title="Load Athena vocabulary",
        runbook_section="§2",
        description="Load the Athena OMOP vocabulary bundle and sanity-check counts.",
        commands=_stage_vocab_commands,
        validators=_vocab_validators,
        skippable=True,
    ),
    Stage(
        key="raw_ingest",
        title="Land a small raw FHIR batch",
        runbook_section="§3",
        description="Export from HAPI and load NDJSON into fhir_raw.*",
        commands=_stage_raw_ingest_commands,
        skippable=True,
    ),
    Stage(
        key="raw_validate",
        title="Validate the raw layer",
        runbook_section="§4",
        description="Row-count + presence checks on fhir_raw.*",
        commands=_stage_raw_validate_commands,
        validators=_raw_validators,
        check_only=True,
    ),
    Stage(
        key="dbt_parse",
        title="Parse dbt",
        runbook_section="§5",
        description="dbt deps + dbt parse (no build).",
        commands=_stage_dbt_parse_commands,
    ),
    Stage(
        key="dbt_build",
        title="Seed and build dbt in stages",
        runbook_section="§6",
        description="Seed mappings, build staging+intermediate, then OMOP layer.",
        commands=_stage_dbt_build_commands,
    ),
    Stage(
        key="omop_validate",
        title="Validate the OMOP layer",
        runbook_section="§7",
        description="Row counts, unknown-concept rates, and null-person checks.",
        commands=_stage_omop_validate_commands,
        validators=_omop_validators,
        check_only=True,
    ),
    Stage(
        key="dbt_test",
        title="Run relational tests",
        runbook_section="§8",
        description="dbt test — uniqueness and relationship assertions.",
        commands=_stage_dbt_test_commands,
    ),
    Stage(
        key="dqd",
        title="Run Data Quality Dashboard",
        runbook_section="§9",
        description="OHDSI DQD over omop_cdm + omop_vocab.",
        commands=_stage_dqd_commands,
        skippable=True,
    ),
    Stage(
        key="exit_criteria",
        title="Evaluate exit criteria",
        runbook_section="§10",
        description="Summarize whether the first-run exit criteria are met.",
        commands=_stage_exit_commands,
        validators=_exit_validators,
        check_only=True,
    ),
]


def get_stage(key: str) -> Stage:
    for s in STAGES:
        if s.key == key:
            return s
    raise KeyError(f"unknown stage {key!r}")


# =============================================================================
# Execution helpers
# =============================================================================


class StageExecutionError(RuntimeError):
    def __init__(self, stage: Stage, command: Command, returncode: int, tail: str):
        super().__init__(
            f"Stage {stage.key!r} failed on command: {command.display()}"
            f" (exit {returncode}). Last output:\n{tail}"
        )
        self.stage = stage
        self.command = command
        self.returncode = returncode
        self.tail = tail


def run_command(
    cmd: Command,
    *,
    repo_root: Path,
    log_path: Path,
    extra_env: dict[str, str] | None = None,
) -> int:
    """Run a single command, teeing stdout/stderr to the log and to the console.

    Returns the exit code. Does NOT raise on non-zero exit.
    """
    import os
    import sys

    env = os.environ.copy()
    env.update(cmd.env)
    if extra_env:
        env.update(extra_env)

    cwd = repo_root / cmd.cwd if cmd.cwd else repo_root

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as logf:
        now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
        logf.write(f"\n\n===== {now_utc} :: {cmd.display()} =====\n")
        logf.flush()

        try:
            proc = subprocess.Popen(
                cmd.argv,
                cwd=str(cwd),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            message = f"Command not found: {cmd.argv[0]}\n"
            sys.stdout.write(message)
            sys.stdout.flush()
            logf.write(message)
            logf.flush()
            return 127
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            logf.write(line)
            logf.flush()
        proc.wait()
        return proc.returncode


def run_bq_json(sql: str, *, project: str, log_path: Path) -> list[dict[str, Any]]:
    """Run a BigQuery query via the `bq` CLI and parse the JSON result."""
    argv = [
        "bq",
        "--project_id",
        project,
        "query",
        "--use_legacy_sql=false",
        "--format=json",
        "--quiet",
        sql,
    ]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as logf:
        now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
        logf.write(f"\n\n===== {now_utc} :: bq query =====\n")
        logf.write(sql + "\n")
    try:
        proc = subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError as exc:
        raise StageExecutionError(
            STAGES[0], Command(argv), 127, f"bq CLI not found: {exc}"
        ) from exc
    except subprocess.CalledProcessError as exc:
        tail = (exc.stderr or exc.stdout or "").strip()[-2000:]
        raise RuntimeError(f"bq query failed: {tail}") from exc
    text = proc.stdout.strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"bq query returned non-JSON output: {text[:200]}") from exc

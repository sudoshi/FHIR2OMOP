"""
Pre-flight connectivity checks for the runbook TUI.

After the wizard collects inputs, these checks run against the real
systems (GCP, HAPI FHIR, local tools) to verify the config is workable
*before* any stage executes. Each check returns a CheckResult with a
level (pass / warn / fail / skip), a one-line summary, and an optional
remediation hint.

Checks are grouped by what they depend on:

- always runnable once `gcp_project` is set (gcloud auth, ADC, BQ access)
- requires `gcs_landing` (bucket visibility)
- requires `hapi_base_url` unless `skip_hapi_export` (HAPI /metadata)
- requires `athena_vocab_zip` unless `skip_vocab_load` (zip sanity)
- requires `dbt_project_dir` (dbt --version + dbt debug)
- requires `hash_person_source_value` (pepper resolution)
- requires `run_dqd` (Rscript present)

Dry-run mode intentionally skips all of this because it's network-free
preview. `--check-connectivity` runs *just* this phase and exits.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib import error as urlerror
from urllib import request as urlrequest

from .config import RunbookConfig, SecretError, SecretResolver


LOG = logging.getLogger("runbook.connectivity")


# =============================================================================
# Result types
# =============================================================================


PASS = "pass"
WARN = "warn"
FAIL = "fail"
SKIP = "skip"


@dataclass
class CheckResult:
    level: str  # PASS | WARN | FAIL | SKIP
    summary: str
    hint: str = ""


@dataclass
class ConnectivityCheck:
    name: str
    description: str
    applies: Callable[[RunbookConfig], bool]
    run: Callable[[RunbookConfig], CheckResult]
    slow: bool = False


# =============================================================================
# Low-level subprocess helper
# =============================================================================


def _run(argv: list[str], *, timeout: int = 30) -> subprocess.CompletedProcess | None:
    """Run a short command and return the CompletedProcess, or None if the
    binary is missing / timed out."""
    try:
        return subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        return None


# =============================================================================
# Check implementations
# =============================================================================


def check_gcloud_auth(cfg: RunbookConfig) -> CheckResult:
    r = _run(
        [
            "gcloud",
            "auth",
            "list",
            "--filter=status:ACTIVE",
            "--format=value(account)",
        ]
    )
    if r is None:
        return CheckResult(
            FAIL,
            "gcloud CLI not found on $PATH",
            "Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install",
        )
    account = (r.stdout or "").strip()
    if r.returncode != 0 or not account:
        return CheckResult(
            FAIL,
            "no active gcloud account",
            "Run: gcloud auth login",
        )
    return CheckResult(PASS, f"active account: {account}")


def check_gcloud_adc(cfg: RunbookConfig) -> CheckResult:
    # Don't actually print the token — we only care about the exit code.
    r = _run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        timeout=20,
    )
    if r is None:
        return CheckResult(FAIL, "gcloud CLI not found on $PATH")
    if r.returncode != 0:
        err = (r.stderr or "").strip().splitlines()[-1] if r.stderr else ""
        return CheckResult(
            FAIL,
            "application-default credentials not set",
            err or "Run: gcloud auth application-default login",
        )
    return CheckResult(PASS, "application-default credentials present")


def check_bq_project_access(cfg: RunbookConfig) -> CheckResult:
    if not cfg.gcp_project:
        return CheckResult(FAIL, "gcp_project is empty")
    r = _run(
        [
            "bq",
            "ls",
            f"--project_id={cfg.gcp_project}",
            "--max_results=1",
            "--format=none",
        ],
        timeout=30,
    )
    if r is None:
        return CheckResult(
            FAIL,
            "bq CLI not found on $PATH",
            "Install Google Cloud SDK (bq is part of gcloud)",
        )
    if r.returncode != 0:
        tail = ((r.stderr or "") + (r.stdout or "")).strip().splitlines()[-3:]
        hint = "\n".join(tail)[:500] if tail else ""
        return CheckResult(
            FAIL,
            f"bq ls failed for project {cfg.gcp_project}",
            hint or "Check that the project exists and you have bigquery.datasets.list permission",
        )
    return CheckResult(PASS, f"BigQuery access OK for {cfg.gcp_project}")


def check_gcs_bucket(cfg: RunbookConfig) -> CheckResult:
    bucket = cfg.effective_gcs_landing()
    if not bucket:
        return CheckResult(SKIP, "no landing bucket configured")
    r = _run(["gcloud", "storage", "ls", bucket], timeout=20)
    if r is None:
        return CheckResult(WARN, "gcloud not found — can't verify bucket")
    if r.returncode == 0:
        return CheckResult(PASS, f"{bucket} exists and is readable")
    err_text = (r.stderr or "").lower()
    # Bucket doesn't exist yet is a WARN, not a FAIL — the bootstrap stage
    # will create it. Only fail on actual permission / auth problems.
    if any(s in err_text for s in ("not found", "does not exist", "notfound")):
        return CheckResult(
            WARN,
            f"{bucket} does not exist yet",
            "Bootstrap stage will create it; ensure you have storage.buckets.create on the project",
        )
    if any(s in err_text for s in ("permission", "forbidden", "403")):
        return CheckResult(
            FAIL,
            f"no permission to list {bucket}",
            "Check storage.buckets.get / storage.objects.list",
        )
    tail = (r.stderr or r.stdout or "").strip().splitlines()[-2:]
    return CheckResult(FAIL, f"gcs check failed for {bucket}", "\n".join(tail)[:300])


def check_hapi_metadata(cfg: RunbookConfig) -> CheckResult:
    if cfg.skip_hapi_export:
        return CheckResult(SKIP, "skip_hapi_export=true")
    if not cfg.hapi_base_url:
        return CheckResult(FAIL, "hapi_base_url not set")

    metadata_url = cfg.hapi_base_url.rstrip("/") + "/metadata"
    headers = {"Accept": "application/fhir+json"}
    # If basic auth user is set, include it with an empty password. Many
    # HAPI deployments require *some* user header for metadata even when
    # the endpoint is otherwise anonymous; others require the real
    # password which we don't have at wizard time.
    if cfg.hapi_http_user:
        token = base64.b64encode(f"{cfg.hapi_http_user}:".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    req = urlrequest.Request(metadata_url, headers=headers)
    try:
        with urlrequest.urlopen(req, timeout=15) as resp:
            status = getattr(resp, "status", 200)
            if status != 200:
                return CheckResult(FAIL, f"HAPI /metadata returned HTTP {status}")
            # CapabilityStatements can be >100KB on busy servers.
            # Cap at 4MB so a runaway response can't OOM the TUI.
            body = resp.read(4 * 1024 * 1024).decode("utf-8", errors="replace")
    except urlerror.HTTPError as exc:
        if exc.code == 401:
            return CheckResult(
                WARN,
                "HAPI /metadata returned 401",
                "auth required — can't fully verify without a password; HAPI is at least reachable",
            )
        return CheckResult(
            FAIL,
            f"HAPI /metadata HTTP {exc.code}",
            f"URL: {metadata_url}",
        )
    except urlerror.URLError as exc:
        return CheckResult(
            FAIL,
            f"HAPI unreachable: {exc.reason}",
            f"URL: {metadata_url}",
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(FAIL, f"HAPI check error: {exc}")

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return CheckResult(FAIL, "HAPI /metadata returned non-JSON", body[:200])
    if data.get("resourceType") != "CapabilityStatement":
        return CheckResult(
            FAIL,
            f"/metadata is not a CapabilityStatement (got {data.get('resourceType')!r})",
        )
    fhir_version = data.get("fhirVersion", "?")
    software = (data.get("software") or {}).get("name", "FHIR server")
    return CheckResult(PASS, f"{software}, FHIR {fhir_version}")


def check_athena_vocab_zip(cfg: RunbookConfig) -> CheckResult:
    if cfg.skip_vocab_load:
        return CheckResult(SKIP, "skip_vocab_load=true")
    path_str = cfg.athena_vocab_zip
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        # Caller resolves against repo root; we take the config value as-is
        # and fall back to cwd. For the wizard's purposes that's fine — we
        # also recommend an absolute path in the docs.
        pass
    if not path.exists():
        return CheckResult(FAIL, f"not found: {path_str}")
    if not zipfile.is_zipfile(path):
        return CheckResult(FAIL, f"not a zip: {path_str}")
    try:
        with zipfile.ZipFile(path) as z:
            names = set(z.namelist())
    except zipfile.BadZipFile:
        return CheckResult(FAIL, f"corrupt zip: {path_str}")

    required = {"CONCEPT.csv", "CONCEPT_RELATIONSHIP.csv", "VOCABULARY.csv"}
    missing = sorted(required - names)
    if missing:
        return CheckResult(
            FAIL,
            f"zip missing: {', '.join(missing)}",
            "Re-download the Athena vocabulary bundle from https://athena.ohdsi.org/",
        )
    return CheckResult(PASS, f"{len(names)} files, required tables present")


def check_dbt_version(cfg: RunbookConfig) -> CheckResult:
    r = _run(["dbt", "--version"], timeout=10)
    if r is None:
        return CheckResult(
            FAIL,
            "dbt not found on $PATH",
            "Install into your active Python env: pip install dbt-bigquery",
        )
    if r.returncode != 0:
        first = ((r.stderr or r.stdout) or "").strip().splitlines()[:1]
        return CheckResult(FAIL, "dbt --version failed", first[0] if first else "")
    output = (r.stdout or r.stderr or "").strip()
    first = output.splitlines()[0] if output else "dbt"
    has_bigquery = "bigquery" in output.lower()
    if not has_bigquery:
        return CheckResult(
            WARN,
            f"{first} (dbt-bigquery adapter not detected)",
            "Run: pip install dbt-bigquery",
        )
    return CheckResult(PASS, first)


def check_dbt_debug(
    cfg: RunbookConfig,
    *,
    repo_root: Path | None = None,
) -> CheckResult:
    """The heavyweight check. Runs `dbt debug --target <target>` inside the
    dbt project dir, which exercises the profile, OAuth/service-account
    credentials, and a real BigQuery query. Slow (~10–30s)."""
    dbt_dir_str = cfg.dbt_project_dir
    dbt_dir = Path(dbt_dir_str).expanduser()
    if not dbt_dir.is_absolute() and repo_root is not None:
        dbt_dir = repo_root / dbt_dir
    if not dbt_dir.exists():
        return CheckResult(FAIL, f"dbt_project_dir not found: {dbt_dir}")

    argv = ["dbt", "debug", "--target", cfg.dbt_target]
    env = os.environ.copy()
    if cfg.dbt_profiles_dir:
        env["DBT_PROFILES_DIR"] = cfg.dbt_profiles_dir
    # If hashing is enabled we don't need a real pepper for `dbt debug` —
    # debug doesn't parse the models. Leave DBT_PEPPER unset.

    try:
        r = subprocess.run(
            argv,
            cwd=str(dbt_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
    except FileNotFoundError:
        return CheckResult(FAIL, "dbt not found on $PATH")
    except subprocess.TimeoutExpired:
        return CheckResult(
            FAIL,
            "dbt debug timed out after 90s",
            "Check network, profile, and BigQuery reachability",
        )

    combined = (r.stdout or "") + (r.stderr or "")
    if r.returncode == 0 and "All checks passed" in combined:
        return CheckResult(PASS, "dbt debug — all checks passed")
    if r.returncode == 0:
        return CheckResult(PASS, "dbt debug — success")

    # Build a tail hint from the last few meaningful lines.
    lines = [ln for ln in combined.strip().splitlines() if ln.strip()]
    tail = "\n".join(lines[-4:])[:600] if lines else "no output"
    return CheckResult(FAIL, "dbt debug failed", tail)


def check_pepper_source(cfg: RunbookConfig) -> CheckResult:
    if not cfg.hash_person_source_value:
        return CheckResult(SKIP, "hash_person_source_value=false")
    if cfg.pepper_source == "prompt":
        return CheckResult(SKIP, "prompt source — will ask at run time")
    try:
        value = SecretResolver.resolve(
            cfg.pepper_source,
            cfg.pepper_ref,
            dotenv_path=cfg.pepper_dotenv_path,
            prompt_fn=lambda: "",  # should not be called for non-prompt sources
        )
    except SecretError as exc:
        return CheckResult(
            FAIL,
            f"could not resolve pepper from {cfg.pepper_source}",
            str(exc),
        )
    length = len(value)
    if length < 8:
        return CheckResult(
            WARN,
            f"pepper resolved from {cfg.pepper_source} but is only {length} chars",
            "Use a longer pepper (>=16 chars recommended)",
        )
    return CheckResult(
        PASS,
        f"pepper resolved from {cfg.pepper_source} ({length} chars, value not shown)",
    )


def check_rscript(cfg: RunbookConfig) -> CheckResult:
    if not cfg.run_dqd:
        return CheckResult(SKIP, "run_dqd=false")
    r = _run(["Rscript", "--version"], timeout=10)
    if r is None:
        return CheckResult(
            FAIL,
            "Rscript not found on $PATH",
            "Install R; DQD stage will fail without it",
        )
    first = (r.stdout or r.stderr or "").strip().splitlines()[:1]
    return CheckResult(PASS, first[0] if first else "Rscript present")


# =============================================================================
# Check registry
# =============================================================================


CHECKS: list[ConnectivityCheck] = [
    ConnectivityCheck(
        name="gcloud_auth",
        description="gcloud has an active account",
        applies=lambda c: True,
        run=check_gcloud_auth,
    ),
    ConnectivityCheck(
        name="gcloud_adc",
        description="application-default credentials work",
        applies=lambda c: True,
        run=check_gcloud_adc,
    ),
    ConnectivityCheck(
        name="bq_project",
        description="BigQuery project is accessible",
        applies=lambda c: bool(c.gcp_project),
        run=check_bq_project_access,
    ),
    ConnectivityCheck(
        name="gcs_bucket",
        description="GCS landing bucket visible (or creatable)",
        applies=lambda c: True,
        run=check_gcs_bucket,
    ),
    ConnectivityCheck(
        name="hapi_metadata",
        description="HAPI FHIR /metadata is reachable",
        applies=lambda c: not c.skip_hapi_export,
        run=check_hapi_metadata,
    ),
    ConnectivityCheck(
        name="athena_vocab",
        description="Athena vocabulary zip is valid",
        applies=lambda c: not c.skip_vocab_load,
        run=check_athena_vocab_zip,
    ),
    ConnectivityCheck(
        name="dbt_version",
        description="dbt-bigquery is installed",
        applies=lambda c: True,
        run=check_dbt_version,
    ),
    ConnectivityCheck(
        name="dbt_debug",
        description="dbt debug (profile + BigQuery auth)",
        applies=lambda c: True,
        run=lambda c: check_dbt_debug(c),  # repo_root patched in run_all_checks
        slow=True,
    ),
    ConnectivityCheck(
        name="pepper_source",
        description="person_source_value pepper is resolvable",
        applies=lambda c: c.hash_person_source_value,
        run=check_pepper_source,
    ),
    ConnectivityCheck(
        name="rscript",
        description="Rscript is available for DQD stage",
        applies=lambda c: c.run_dqd,
        run=check_rscript,
    ),
]


# =============================================================================
# Runner
# =============================================================================


@dataclass
class ConnectivityReport:
    results: list[tuple[ConnectivityCheck, CheckResult]]

    def any_failed(self) -> bool:
        return any(r.level == FAIL for _, r in self.results)

    def any_warned(self) -> bool:
        return any(r.level == WARN for _, r in self.results)

    def count_by_level(self) -> dict[str, int]:
        out = {PASS: 0, WARN: 0, FAIL: 0, SKIP: 0}
        for _, r in self.results:
            out[r.level] = out.get(r.level, 0) + 1
        return out


def run_all_checks(
    cfg: RunbookConfig,
    *,
    repo_root: Path | None = None,
    skip_slow: bool = False,
    progress_cb: Callable[[ConnectivityCheck], None] | None = None,
) -> ConnectivityReport:
    """Run every applicable check. `progress_cb` is called with each check
    just before it executes so the UI can render a live status line."""
    results: list[tuple[ConnectivityCheck, CheckResult]] = []
    for chk in CHECKS:
        if not chk.applies(cfg):
            results.append((chk, CheckResult(SKIP, "not applicable")))
            continue
        if skip_slow and chk.slow:
            results.append((chk, CheckResult(SKIP, "slow check skipped")))
            continue
        if progress_cb is not None:
            try:
                progress_cb(chk)
            except Exception:  # noqa: BLE001
                pass
        try:
            if chk.name == "dbt_debug":
                result = check_dbt_debug(cfg, repo_root=repo_root)
            else:
                result = chk.run(cfg)
        except Exception as exc:  # noqa: BLE001
            LOG.exception("check %s crashed", chk.name)
            result = CheckResult(FAIL, f"check crashed: {exc}")
        results.append((chk, result))
    return ConnectivityReport(results=results)

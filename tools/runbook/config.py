"""
Config + secret resolution for the runbook wizard.

RunbookConfig holds every non-secret input needed to drive the 10 validation
stages. It is serialized to JSON so the user can resume or reuse between runs.

Secrets (today: person_source_value_pepper) are NEVER written into the config
file. Instead the config stores a `pepper_source` enum and, for sources that
need it, a `pepper_ref` (e.g. an env var name, a pass entry, a gcloud secret
name). SecretResolver.resolve() turns that pair into a live value at run time.
"""
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass, field, fields
from datetime import date
from pathlib import Path
from typing import Any

try:
    from dotenv import dotenv_values
except ImportError:  # pragma: no cover - dep is declared in requirements.txt
    dotenv_values = None  # type: ignore[assignment]


CONFIG_SCHEMA_VERSION = 1

PEPPER_SOURCES = ("prompt", "env", "dotenv", "pass", "gcloud")


@dataclass
class RunbookConfig:
    """Everything the runbook needs except live secrets."""

    # --- GCP / BigQuery ---
    gcp_project: str = ""
    gcp_region: str = "southamerica-west1"
    gcs_landing: str = ""  # derived from gcp_project if empty

    # --- Source FHIR server (HAPI) ---
    hapi_base_url: str = ""
    hapi_http_user: str = ""  # optional basic-auth user; password via secret
    skip_hapi_export: bool = False  # data already landed in fhir_raw

    # --- Vocabulary ---
    athena_vocab_zip: str = "./vocabulary_download_v5.zip"
    vocab_dataset: str = "omop_vocab"
    skip_vocab_load: bool = False  # already loaded

    # --- dbt ---
    dbt_project_dir: str = "./dbt"
    dbt_target: str = "dev"
    dbt_profiles_dir: str = ""  # empty -> use ~/.dbt/profiles.yml

    # --- Run window ---
    run_date: str = ""  # YYYY-MM-DD, default today
    since: str = ""     # e.g. 2026-04-08T00:00:00Z

    # --- Hashing of person_source_value ---
    hash_person_source_value: bool = False
    pepper_source: str = "prompt"  # one of PEPPER_SOURCES
    pepper_ref: str = ""            # env var name / pass path / gcloud secret name
    pepper_dotenv_path: str = ".env.local"  # where dotenv source reads from

    # --- DQD (optional stage) ---
    run_dqd: bool = True

    # --- Logging ---
    log_dir: str = "logs"

    # --- Schema version (for forward compat) ---
    schema_version: int = CONFIG_SCHEMA_VERSION

    # -------------------------------------------------------------------------
    # Derived values
    # -------------------------------------------------------------------------
    def effective_gcs_landing(self) -> str:
        if self.gcs_landing:
            return self.gcs_landing
        if self.gcp_project:
            return f"gs://{self.gcp_project}-fhir-landing"
        return ""

    def effective_run_date(self) -> str:
        return self.run_date or date.today().isoformat()

    # -------------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------------
    def validate(self) -> list[str]:
        """Return a list of human-readable validation errors. Empty == OK."""
        errs: list[str] = []
        if not self.gcp_project:
            errs.append("gcp_project is required")
        if self.pepper_source not in PEPPER_SOURCES:
            errs.append(
                f"pepper_source must be one of {PEPPER_SOURCES}, got {self.pepper_source!r}"
            )
        if (
            self.hash_person_source_value
            and self.pepper_source != "prompt"
            and not self.pepper_ref
        ):
            errs.append(
                f"pepper_ref is required when pepper_source={self.pepper_source!r}"
            )
        if not self.skip_hapi_export and not self.hapi_base_url:
            errs.append("hapi_base_url is required unless skip_hapi_export=true")
        if not self.skip_vocab_load and not Path(self.athena_vocab_zip).expanduser().exists():
            errs.append(
                f"athena_vocab_zip not found: {self.athena_vocab_zip}"
                " (set skip_vocab_load=true if already loaded)"
            )
        if not Path(self.dbt_project_dir).expanduser().exists():
            errs.append(f"dbt_project_dir not found: {self.dbt_project_dir}")
        return errs

    # -------------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------------
    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")

    @classmethod
    def load(cls, path: Path) -> "RunbookConfig":
        raw = json.loads(Path(path).read_text())
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in raw.items() if k in known}
        cfg = cls(**filtered)
        if raw.get("schema_version", CONFIG_SCHEMA_VERSION) != CONFIG_SCHEMA_VERSION:
            # Forward-compat hook: if we ever bump the schema, migrate here.
            pass
        return cfg

    def to_display(self) -> dict[str, Any]:
        """Render-friendly view that hides empty derived fields."""
        d = asdict(self)
        d["gcs_landing (effective)"] = self.effective_gcs_landing()
        d["run_date (effective)"] = self.effective_run_date()
        return d


# =============================================================================
# Secret resolution
# =============================================================================


class SecretError(RuntimeError):
    pass


class SecretResolver:
    """Resolve a secret from one of several sources.

    Supported sources:
      - prompt   : ask the user interactively (handled by caller)
      - env      : os.environ[ref]
      - dotenv   : read ref from a .env-style file (default .env.local)
      - pass     : `pass show <ref>`
      - gcloud   : `gcloud secrets versions access latest --secret <ref>`
    """

    @staticmethod
    def resolve(
        source: str,
        ref: str,
        *,
        dotenv_path: str | Path = ".env.local",
        prompt_fn=None,
    ) -> str:
        source = source.strip().lower()
        if source == "prompt":
            if prompt_fn is None:
                raise SecretError("prompt source requires a prompt_fn")
            value = prompt_fn()
            if not value:
                raise SecretError("Empty value entered at prompt")
            return value

        if source == "env":
            value = os.environ.get(ref, "")
            if not value:
                raise SecretError(f"env var {ref!r} is not set or empty")
            return value

        if source == "dotenv":
            if dotenv_values is None:
                raise SecretError(
                    "python-dotenv is not installed; cannot read dotenv source"
                )
            path = Path(dotenv_path).expanduser()
            if not path.exists():
                raise SecretError(f"dotenv file not found: {path}")
            values = dotenv_values(path)
            if ref not in values or not values[ref]:
                raise SecretError(f"key {ref!r} not found in {path}")
            return str(values[ref])

        if source == "pass":
            return _run_capture(["pass", "show", ref], source="pass", ref=ref)

        if source == "gcloud":
            return _run_capture(
                [
                    "gcloud",
                    "secrets",
                    "versions",
                    "access",
                    "latest",
                    "--secret",
                    ref,
                ],
                source="gcloud",
                ref=ref,
            )

        raise SecretError(f"unknown secret source: {source!r}")

    @staticmethod
    def describe(source: str, ref: str) -> str:
        """Return a human-readable description of where the secret will come from."""
        if source == "prompt":
            return "will be asked interactively (never stored)"
        if source == "env":
            return f"from environment variable ${ref}"
        if source == "dotenv":
            return f"from dotenv entry {ref}"
        if source == "pass":
            return f"from `pass show {ref}`"
        if source == "gcloud":
            return f"from `gcloud secrets versions access latest --secret {ref}`"
        return f"unknown source {source!r}"


def _run_capture(argv: list[str], *, source: str, ref: str) -> str:
    try:
        proc = subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError as exc:
        raise SecretError(f"{source} CLI not found on PATH: {exc}") from exc
    except subprocess.CalledProcessError as exc:
        raise SecretError(
            f"{source} failed for ref={ref!r}: {exc.stderr.strip() or exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise SecretError(f"{source} timed out for ref={ref!r}") from exc
    value = proc.stdout.strip()
    if not value:
        raise SecretError(f"{source} returned empty value for ref={ref!r}")
    return value

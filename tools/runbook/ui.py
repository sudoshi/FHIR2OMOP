"""
Rich + questionary helpers for the runbook TUI.

Kept intentionally thin: rendering + prompts only. No subprocess, no BQ, no
state. The calling code composes these with config, state, and stages.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import questionary
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Confirm
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .config import PEPPER_SOURCES, RunbookConfig, SecretResolver
from .stages import STAGES, Stage


# =============================================================================
# Headers and panels
# =============================================================================


def render_header(console: Console) -> None:
    title = Text("FHIR2OMOP — Warehouse Validation Runbook", style="bold cyan")
    subtitle = Text(
        "Interactive wizard for the first real pipeline run against BigQuery",
        style="dim",
    )
    console.print(Panel.fit(Text.assemble(title, "\n", subtitle), border_style="cyan"))


def render_stage_header(console: Console, stage: Stage, index: int, total: int) -> None:
    console.print()
    console.print(
        Rule(
            f"[bold]{index}/{total}[/bold]  {stage.runbook_section}  "
            f"[bold cyan]{stage.title}[/bold cyan]",
            style="cyan",
        )
    )
    console.print(f"[dim]{stage.description}[/dim]")


def render_stage_list(console: Console) -> None:
    table = Table(title="Runbook stages", show_lines=False, border_style="dim")
    table.add_column("#", justify="right", style="dim")
    table.add_column("Section", style="cyan")
    table.add_column("Stage", style="bold")
    table.add_column("Type", style="dim")
    table.add_column("Description")
    for i, s in enumerate(STAGES, start=1):
        kind = "check" if s.check_only else "run"
        if s.skippable:
            kind += " (skippable)"
        table.add_row(str(i), s.runbook_section, s.title, kind, s.description)
    console.print(table)


# =============================================================================
# Config rendering
# =============================================================================


_SECRET_HINT = "person_source_value pepper"


def render_config_summary(console: Console, cfg: RunbookConfig, *, title: str = "Configuration") -> None:
    table = Table(title=title, border_style="cyan")
    table.add_column("Field", style="bold")
    table.add_column("Value")

    for k, v in cfg.to_display().items():
        if isinstance(v, bool):
            v = "yes" if v else "no"
        if v in ("", None):
            v = "[dim](unset)[/dim]"
        table.add_row(k, str(v))

    console.print(table)

    if cfg.hash_person_source_value:
        describe = SecretResolver.describe(cfg.pepper_source, cfg.pepper_ref)
        console.print(
            Panel.fit(
                f"[bold]{_SECRET_HINT}[/bold]: {describe}",
                title="Secret source",
                border_style="magenta",
            )
        )


def render_command_preview(console: Console, cfg: RunbookConfig) -> None:
    for i, stage in enumerate(STAGES, start=1):
        cmds = stage.commands(cfg)
        console.print()
        console.print(
            f"[bold cyan]{i}. {stage.runbook_section} {stage.title}[/bold cyan]"
            f"  [dim]({'check-only' if stage.check_only else 'commands'})[/dim]"
        )
        if not cmds and not stage.check_only:
            console.print("  [dim](no commands — skipped per config)[/dim]")
        for c in cmds:
            console.print(f"  [green]$[/green] {c.display()}")
            if c.description:
                console.print(f"    [dim]{c.description}[/dim]")
            if c.env:
                env_repr = " ".join(f"{k}={v}" for k, v in c.env.items())
                console.print(f"    [dim]env: {env_repr}[/dim]")
        if stage.validators:
            for v in stage.validators(cfg):
                console.print(f"  [yellow]?[/yellow] BQ check: {v.description}")


def render_dry_run_report(console: Console, cfg: RunbookConfig) -> None:
    console.print()
    console.print(Rule("DRY-RUN PREVIEW", style="yellow"))
    errors = cfg.validate()
    if errors:
        console.print(Panel("\n".join(f"• {e}" for e in errors), title="Validation errors", border_style="red"))
    else:
        console.print("[green]Config validates.[/green]")

    render_config_summary(console, cfg, title="Inputs the wizard will use")
    console.print()
    console.print(Rule("Commands that would be executed", style="dim"))
    render_command_preview(console, cfg)
    console.print()
    console.print(
        Panel.fit(
            "No commands were run. Re-invoke without --dry-run to execute.",
            border_style="yellow",
        )
    )


# =============================================================================
# Wizard
# =============================================================================


def collect_config_wizard(
    existing: RunbookConfig | None = None,
    *,
    console: Console | None = None,
) -> RunbookConfig:
    """Walk the user through every input. Returns a fully populated config."""
    console = console or Console()
    cfg = existing or RunbookConfig()

    console.print()
    console.print(Rule("Wizard — collect inputs", style="cyan"))
    console.print(
        "[dim]Press enter to accept defaults shown in parentheses. "
        "Ctrl-C aborts without saving.[/dim]"
    )
    console.print()

    # --- GCP ---
    cfg.gcp_project = questionary.text(
        "GCP project ID",
        default=cfg.gcp_project or "chile-omop-prod",
        validate=_nonempty,
    ).ask()
    cfg.gcp_region = questionary.text(
        "GCP region",
        default=cfg.gcp_region or "southamerica-west1",
    ).ask()
    default_bucket = cfg.gcs_landing or f"gs://{cfg.gcp_project}-fhir-landing"
    cfg.gcs_landing = questionary.text(
        "GCS landing bucket (gs://...)",
        default=default_bucket,
    ).ask()

    # --- HAPI ---
    cfg.skip_hapi_export = questionary.confirm(
        "Is data already loaded in fhir_raw? (skip HAPI export + NDJSON load)",
        default=cfg.skip_hapi_export,
    ).ask()
    if not cfg.skip_hapi_export:
        cfg.hapi_base_url = questionary.text(
            "HAPI FHIR base URL",
            default=cfg.hapi_base_url or "https://hapi.internal/fhir",
            validate=_nonempty,
        ).ask()
        cfg.hapi_http_user = questionary.text(
            "HAPI basic-auth user (optional, leave blank for none)",
            default=cfg.hapi_http_user,
        ).ask()

    cfg.run_date = questionary.text(
        "Run date (YYYY-MM-DD, empty = today)",
        default=cfg.run_date,
    ).ask()
    cfg.since = questionary.text(
        "Export --since timestamp (RFC3339, empty = full small export)",
        default=cfg.since,
    ).ask()

    # --- Vocab ---
    cfg.skip_vocab_load = questionary.confirm(
        "Is the Athena vocabulary already loaded into omop_vocab?",
        default=cfg.skip_vocab_load,
    ).ask()
    if not cfg.skip_vocab_load:
        cfg.athena_vocab_zip = questionary.text(
            "Path to Athena vocabulary zip",
            default=cfg.athena_vocab_zip or "./vocabulary_download_v5.zip",
        ).ask()
    cfg.vocab_dataset = questionary.text(
        "Vocabulary dataset name",
        default=cfg.vocab_dataset or "omop_vocab",
    ).ask()

    # --- dbt ---
    cfg.dbt_project_dir = questionary.text(
        "dbt project directory (relative to repo root)",
        default=cfg.dbt_project_dir or "./dbt",
    ).ask()
    cfg.dbt_target = questionary.select(
        "dbt target",
        choices=["dev", "prod"],
        default=cfg.dbt_target or "dev",
    ).ask()
    cfg.dbt_profiles_dir = questionary.text(
        "DBT_PROFILES_DIR (empty = ~/.dbt/profiles.yml)",
        default=cfg.dbt_profiles_dir,
    ).ask()

    # --- Hashing / pepper ---
    cfg.hash_person_source_value = questionary.confirm(
        "Hash person_source_value? (production behavior)",
        default=cfg.hash_person_source_value,
    ).ask()
    if cfg.hash_person_source_value:
        cfg.pepper_source = questionary.select(
            "Where should the pepper come from?",
            choices=[
                questionary.Choice("Prompt me each run (never stored)", value="prompt"),
                questionary.Choice("Environment variable", value="env"),
                questionary.Choice("dotenv file (.env.local)", value="dotenv"),
                questionary.Choice("pass (password-store)", value="pass"),
                questionary.Choice("gcloud secrets", value="gcloud"),
            ],
            default=cfg.pepper_source if cfg.pepper_source in PEPPER_SOURCES else "prompt",
        ).ask()
        if cfg.pepper_source == "env":
            cfg.pepper_ref = questionary.text(
                "Env var name",
                default=cfg.pepper_ref or "FHIR2OMOP_PEPPER",
                validate=_nonempty,
            ).ask()
        elif cfg.pepper_source == "dotenv":
            cfg.pepper_dotenv_path = questionary.text(
                "Path to dotenv file",
                default=cfg.pepper_dotenv_path or ".env.local",
            ).ask()
            cfg.pepper_ref = questionary.text(
                "Key name inside the dotenv file",
                default=cfg.pepper_ref or "FHIR2OMOP_PEPPER",
                validate=_nonempty,
            ).ask()
        elif cfg.pepper_source == "pass":
            cfg.pepper_ref = questionary.text(
                "pass entry (e.g. fhir2omop/pepper)",
                default=cfg.pepper_ref or "fhir2omop/pepper",
                validate=_nonempty,
            ).ask()
        elif cfg.pepper_source == "gcloud":
            cfg.pepper_ref = questionary.text(
                "gcloud secret name",
                default=cfg.pepper_ref or "fhir2omop-pepper",
                validate=_nonempty,
            ).ask()
        # prompt: no ref

    # --- DQD ---
    cfg.run_dqd = questionary.confirm(
        "Run OHDSI Data Quality Dashboard at the end?",
        default=cfg.run_dqd,
    ).ask()

    return cfg


def _nonempty(value: str) -> bool | str:
    if value is None or value.strip() == "":
        return "Value is required"
    return True


# =============================================================================
# Per-stage prompts (used during real execution)
# =============================================================================


def confirm(prompt: str, *, default: bool = True) -> bool:
    return Confirm.ask(prompt, default=default)


def ask_next_action(stage: Stage) -> str:
    return questionary.select(
        f"What would you like to do for stage {stage.title!r}?",
        choices=[
            questionary.Choice("Run", value="run"),
            questionary.Choice("Skip", value="skip"),
            questionary.Choice("Abort", value="abort"),
        ],
        default="run",
    ).ask()


def ask_on_failure(stage: Stage) -> str:
    return questionary.select(
        f"Stage {stage.title!r} failed. What now?",
        choices=[
            questionary.Choice("Retry", value="retry"),
            questionary.Choice("Mark failed and continue", value="continue"),
            questionary.Choice("Abort the run", value="abort"),
        ],
        default="retry",
    ).ask()


# =============================================================================
# Validator rendering
# =============================================================================


def render_validator_result(
    console: Console,
    check_name: str,
    description: str,
    ok: bool,
    summary: str,
    rows: list[dict[str, Any]],
) -> None:
    marker = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
    console.print(f"  {marker} {description}")
    console.print(f"       [dim]{summary}[/dim]")

    if rows:
        keys = list(rows[0].keys())
        t = Table(show_header=True, border_style="dim", pad_edge=False)
        for k in keys:
            t.add_column(k)
        for r in rows[:20]:
            t.add_row(*[str(r.get(k, "")) for k in keys])
        console.print(t)


# =============================================================================
# Final exit-criteria report
# =============================================================================


def render_exit_report(
    console: Console,
    cfg: RunbookConfig,
    completed: list[str],
    failed: list[str],
    check_results: list[tuple[str, bool, str]],
) -> None:
    console.print()
    console.print(Rule("EXIT CRITERIA", style="cyan"))
    table = Table(border_style="cyan")
    table.add_column("Criterion")
    table.add_column("Status")

    criteria: list[tuple[str, bool]] = [
        ("Raw FHIR tables loaded", "raw_validate" in completed),
        ("dbt parse + build + test succeeded", all(s in completed for s in ("dbt_parse", "dbt_build", "dbt_test"))),
        ("person and measurement non-zero", any(ok for name, ok, _ in check_results if "person" in name or "final" in name)),
        ("Unknown-concept rates reviewed", "omop_validate" in completed),
    ]
    for name, ok in criteria:
        table.add_row(name, "[green]PASS[/green]" if ok else "[red]FAIL[/red]")

    console.print(table)

    if failed:
        console.print(
            Panel(
                "\n".join(f"• {f}" for f in failed),
                title="Failed stages",
                border_style="red",
            )
        )
    console.print(
        Panel.fit(
            "Full log: see logs/runbook_*.log\n"
            "Next: expand seed_test_source_to_concept.csv and "
            "seed_unit_source_to_concept.csv based on inventory_source_codes.sql.",
            title="Next steps",
            border_style="green",
        )
    )

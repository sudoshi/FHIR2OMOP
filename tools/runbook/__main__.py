"""
Entry point for the runbook TUI.

Usage:
    python -m tools.runbook                        # full interactive run
    python -m tools.runbook --dry-run              # collect inputs, preview commands, exit
    python -m tools.runbook --resume               # resume from saved state
    python -m tools.runbook --list-stages          # print the stage list and exit
    python -m tools.runbook --check-hashing        # run dbt deps + parse with hashing enabled, then exit
    python -m tools.runbook --check-connectivity   # run pre-flight checks only and exit
    python -m tools.runbook --skip-connectivity    # skip the pre-flight phase on a real run
    python -m tools.runbook --skip-slow-checks     # skip dbt debug (the slow check)
    python -m tools.runbook --config PATH          # override config path
    python -m tools.runbook --state PATH           # override state path

All non-secret inputs are persisted to --config (default runbook_config.json)
so they can be reused or version-controlled. Secrets (pepper) are never
written; they are resolved at runtime from the configured source.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from .config import RunbookConfig, SecretError, SecretResolver
from .connectivity import ConnectivityReport, run_all_checks
from .stages import (
    STAGES,
    BqCheck,
    Command,
    Stage,
    run_bq_json,
    run_command,
)
from .state import RunState
from .ui import (
    ask_connectivity_failure,
    ask_next_action,
    ask_on_failure,
    collect_config_wizard,
    confirm,
    render_config_summary,
    render_connectivity_progress,
    render_connectivity_report,
    render_dry_run_report,
    render_exit_report,
    render_header,
    render_stage_header,
    render_stage_list,
    render_validator_result,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "runbook_config.json"
DEFAULT_STATE_PATH = REPO_ROOT / ".runbook_state.json"


# =============================================================================
# Arg parsing
# =============================================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m tools.runbook",
        description="Interactive TUI for the FHIR2OMOP warehouse validation runbook.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect inputs and print a preview of every command + validator, then exit. Nothing executes.",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Skip the wizard and resume from the saved state file.",
    )
    p.add_argument(
        "--list-stages",
        action="store_true",
        help="Print the stage list and exit.",
    )
    p.add_argument(
        "--check-hashing",
        action="store_true",
        help="Resolve the pepper and run only dbt deps + dbt parse with hashing enabled, then exit.",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config file (default: {DEFAULT_CONFIG_PATH.name})",
    )
    p.add_argument(
        "--state",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help=f"Path to state file (default: {DEFAULT_STATE_PATH.name})",
    )
    p.add_argument(
        "--no-save-config",
        action="store_true",
        help="Do not write the collected config back to --config.",
    )
    p.add_argument(
        "--check-connectivity",
        action="store_true",
        help="Run only the pre-flight connectivity checks (no wizard changes, no stages) and exit.",
    )
    p.add_argument(
        "--skip-connectivity",
        action="store_true",
        help="Skip the pre-flight connectivity checks during a real run (not recommended).",
    )
    p.add_argument(
        "--skip-slow-checks",
        action="store_true",
        help="Skip slow connectivity checks like `dbt debug`. Fast checks still run.",
    )
    return p.parse_args(argv)


# =============================================================================
# Secret resolution (wraps SecretResolver with an interactive prompt fallback)
# =============================================================================


def _resolve_pepper(cfg: RunbookConfig) -> str | None:
    if not cfg.hash_person_source_value:
        return None
    try:
        import questionary

        def prompt() -> str:
            return questionary.password("Enter the person_source_value pepper:").ask() or ""

        return SecretResolver.resolve(
            cfg.pepper_source,
            cfg.pepper_ref,
            dotenv_path=cfg.pepper_dotenv_path,
            prompt_fn=prompt,
        )
    except SecretError as exc:
        raise SystemExit(f"Failed to resolve pepper secret: {exc}") from exc


# =============================================================================
# Stage execution
# =============================================================================


def _log_path(cfg: RunbookConfig) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_dir = REPO_ROOT / cfg.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"runbook_{ts}.log"


def _run_connectivity(
    cfg: RunbookConfig,
    *,
    console: Console,
    skip_slow: bool,
) -> ConnectivityReport:
    console.print()
    console.print("[bold cyan]Running pre-flight connectivity checks…[/bold cyan]")
    if skip_slow:
        console.print("[dim](slow checks like `dbt debug` are being skipped)[/dim]")

    def progress(chk) -> None:  # ConnectivityCheck, kept loose to avoid circular import
        render_connectivity_progress(console, chk.name, chk.description)

    return run_all_checks(
        cfg,
        repo_root=REPO_ROOT,
        skip_slow=skip_slow,
        progress_cb=progress,
    )


def _execute_stage(
    stage: Stage,
    cfg: RunbookConfig,
    *,
    console: Console,
    log_path: Path,
    extra_env: dict[str, str] | None,
) -> tuple[bool, list[tuple[str, bool, str]]]:
    """Run commands + validators for one stage. Returns (ok, validator_results)."""
    commands = stage.commands(cfg)

    if stage.precheck:
        errors = stage.precheck(cfg)
        if errors:
            console.print("[red]Precheck failed:[/red]")
            for e in errors:
                console.print(f"  • {e}")
            return False, []

    for cmd in commands:
        console.print(f"[green]$[/green] {cmd.display()}")
        if cmd.description:
            console.print(f"  [dim]{cmd.description}[/dim]")
        rc = run_command(cmd, repo_root=REPO_ROOT, log_path=log_path, extra_env=extra_env)
        if rc != 0:
            console.print(f"[red]Exit code {rc}[/red]")
            return False, []

    validator_results: list[tuple[str, bool, str]] = []
    if stage.validators:
        console.print("[dim]Running BigQuery validators…[/dim]")
        for check in stage.validators(cfg):
            try:
                rows = run_bq_json(check.sql, project=cfg.gcp_project, log_path=log_path)
            except RuntimeError as exc:
                console.print(f"  [red]FAIL[/red] {check.description}: {exc}")
                validator_results.append((check.name, False, str(exc)))
                continue
            if check.interpret:
                ok, summary = check.interpret(rows)
            else:
                ok, summary = True, f"{len(rows)} rows"
            render_validator_result(console, check.name, check.description, ok, summary, rows)
            validator_results.append((check.name, ok, summary))
        if validator_results and not all(ok for _, ok, _ in validator_results):
            return False, validator_results

    return True, validator_results


# =============================================================================
# Top-level flow
# =============================================================================


def _load_or_collect_config(
    args: argparse.Namespace, console: Console
) -> RunbookConfig:
    existing: RunbookConfig | None = None
    if args.config.exists():
        try:
            existing = RunbookConfig.load(args.config)
        except Exception as exc:  # noqa: BLE001 — show any load error
            console.print(f"[yellow]Could not load existing config {args.config}: {exc}[/yellow]")

    if args.resume:
        if existing is None:
            raise SystemExit(f"--resume requires an existing config at {args.config}")
        console.print(f"[dim]Resuming with config from {args.config}[/dim]")
        return existing

    # In dry-run or one-shot check modes with an existing config, reuse it
    # non-interactively so the commands stay scriptable.
    if (args.dry_run or args.check_connectivity or args.check_hashing) and existing is not None:
        render_config_summary(console, existing, title=f"Loaded config from {args.config.name}")
        if args.dry_run:
            mode = "Dry-run"
        elif args.check_connectivity:
            mode = "Connectivity check"
        else:
            mode = "Hashing check"
        console.print(f"[dim]{mode}: reusing existing config without prompting.[/dim]")
        return existing

    if existing is not None:
        render_config_summary(console, existing, title=f"Loaded config from {args.config.name}")
        if confirm("Reuse this config? (No = re-run the wizard)", default=True):
            return existing

    cfg = collect_config_wizard(existing, console=console)
    if not args.no_save_config:
        cfg.save(args.config)
        console.print(f"[green]Saved config to {args.config}[/green]")
    return cfg


def _decide_resume(
    state: RunState,
    console: Console,
    *,
    auto_resume: bool,
) -> RunState:
    completed = state.completed_names()
    if not completed:
        return state
    console.print(
        f"[dim]State file shows {len(completed)} completed stages: "
        f"{', '.join(completed)}[/dim]"
    )
    if auto_resume:
        return state
    if confirm("Resume from where you left off?", default=True):
        return state
    state.reset()
    return state


def _run_hashing_smoke_test(cfg: RunbookConfig, console: Console) -> int:
    smoke_cfg = replace(cfg, hash_person_source_value=True)
    errors = smoke_cfg.validate(base_dir=REPO_ROOT)
    if errors:
        console.print(
            Panel(
                "\n".join(f"• {e}" for e in errors),
                title="[red]Config errors[/red]",
                border_style="red",
            )
        )
        return 2

    if not cfg.hash_person_source_value:
        console.print(
            "[yellow]Hashing is disabled in the saved config; "
            "forcing it on for this smoke test only.[/yellow]"
        )

    pepper = _resolve_pepper(smoke_cfg)
    extra_env = {"DBT_PEPPER": pepper} if pepper else None
    if pepper:
        console.print("[dim]Pepper resolved successfully (not shown).[/dim]")

    log_path = _log_path(smoke_cfg)
    console.print(f"[dim]Logging to {log_path}[/dim]")
    console.print(
        Panel.fit(
            "Hashing smoke test: running only dbt deps + dbt parse with "
            "hash_person_source_value=true.",
            border_style="cyan",
        )
    )

    stage = next(s for s in STAGES if s.key == "dbt_parse")
    ok, _ = _execute_stage(
        stage,
        smoke_cfg,
        console=console,
        log_path=log_path,
        extra_env=extra_env,
    )
    if ok:
        console.print("[green]Hashing smoke test passed.[/green]")
        return 0

    console.print("[red]Hashing smoke test failed.[/red]")
    return 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    console = Console()
    render_header(console)

    if args.list_stages:
        render_stage_list(console)
        return 0

    cfg = _load_or_collect_config(args, console)

    if args.check_hashing:
        return _run_hashing_smoke_test(cfg, console)

    errors = cfg.validate(base_dir=REPO_ROOT)
    if errors:
        # In --check-connectivity mode, local-only issues like "vocab zip
        # missing" don't affect network reachability — downgrade to a
        # warnings panel and keep going. The connectivity checks themselves
        # will surface anything that matters for this mode.
        title = (
            "[yellow]Config warnings[/yellow]"
            if args.check_connectivity
            else "[red]Config errors[/red]"
        )
        border = "yellow" if args.check_connectivity else "red"
        console.print(
            Panel(
                "\n".join(f"• {e}" for e in errors),
                title=title,
                border_style=border,
            )
        )
        if not args.dry_run and not args.check_connectivity:
            return 2

    if args.dry_run:
        render_dry_run_report(console, cfg)
        return 0

    # -- Pre-flight connectivity phase --
    # Verify that the configured servers/resources are actually reachable
    # before we start running stages. This catches auth/network/URL
    # mistakes in seconds instead of minutes into the run.
    if args.check_connectivity:
        report = _run_connectivity(cfg, console=console, skip_slow=args.skip_slow_checks)
        render_connectivity_report(console, report)
        return 0 if not report.any_failed() else 3

    if not args.skip_connectivity:
        report = _run_connectivity(cfg, console=console, skip_slow=args.skip_slow_checks)
        render_connectivity_report(console, report)
        if report.any_failed():
            choice = ask_connectivity_failure()
            if choice == "abort":
                return 3
            if choice == "rerun_wizard":
                # Force the wizard, re-check, then continue if clean.
                cfg = collect_config_wizard(cfg, console=console)
                if not args.no_save_config:
                    cfg.save(args.config)
                    console.print(f"[green]Saved config to {args.config}[/green]")
                errors = cfg.validate(base_dir=REPO_ROOT)
                if errors:
                    console.print(
                        Panel(
                            "\n".join(f"• {e}" for e in errors),
                            title="[red]Config errors[/red]",
                            border_style="red",
                        )
                    )
                    return 2
                report = _run_connectivity(cfg, console=console, skip_slow=args.skip_slow_checks)
                render_connectivity_report(console, report)
                if report.any_failed():
                    console.print(
                        "[red]Connectivity still failing after wizard — aborting.[/red]"
                    )
                    return 3
            # else: proceed — user explicitly overrode

    # -- Resolve pepper up-front so we fail fast if the source is broken --
    pepper = _resolve_pepper(cfg)
    extra_env: dict[str, str] = {}
    if pepper:
        extra_env["DBT_PEPPER"] = pepper  # consumed by the dbt hashing macro via env_var()
        console.print("[dim]Pepper resolved successfully (not shown).[/dim]")

    # -- State + log setup --
    state = RunState.load_or_new(args.state)
    state = _decide_resume(state, console, auto_resume=args.resume)
    log_path = _log_path(cfg)
    state.config_path = str(args.config)
    state.log_file = str(log_path)
    state.save(args.state)
    console.print(f"[dim]Logging to {log_path}[/dim]")

    total = len(STAGES)
    latest_validator_results: dict[str, tuple[bool, str]] = {}

    for idx, stage in enumerate(STAGES, start=1):
        if state.is_completed(stage.key):
            console.print(
                f"[dim]✓ {idx}/{total} {stage.title} — already completed, skipping[/dim]"
            )
            continue

        render_stage_header(console, stage, idx, total)

        if stage.skippable and not confirm(f"Run stage '{stage.title}'?", default=True):
            state.mark_skipped(stage.key)
            state.save(args.state)
            continue

        state.mark_started(stage.key)
        state.save(args.state)

        while True:
            try:
                ok, results = _execute_stage(
                    stage,
                    cfg,
                    console=console,
                    log_path=log_path,
                    extra_env=extra_env,
                )
            except KeyboardInterrupt:
                console.print("[yellow]Interrupted.[/yellow]")
                state.mark_failed(stage.key, "interrupted")
                state.save(args.state)
                return 130
            for name, ok, summary in results:
                latest_validator_results[name] = (ok, summary)

            if ok:
                state.mark_completed(stage.key)
                state.save(args.state)
                break

            state.mark_failed(stage.key, "command or validator failed")
            state.save(args.state)
            action = ask_on_failure(stage)
            if action == "retry":
                state.mark_started(stage.key)
                state.save(args.state)
                continue
            if action == "continue":
                break
            return 1

    render_exit_report(
        console,
        cfg,
        completed=state.completed_names(),
        failed=state.failed_names(),
        check_results=latest_validator_results,
    )
    return 0 if not state.failed_names() else 1


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

from rich.console import Console

from tools.runbook import __main__ as runbook_main
from tools.runbook.config import RunbookConfig
from tools.runbook.stages import BqCheck, Command, Stage, _dbt_vars_args, run_command
from tools.runbook.ui import exit_criteria_statuses


class RunbookBehaviorTests(unittest.TestCase):
    def test_run_command_returns_127_when_executable_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "runbook.log"
            with contextlib.redirect_stdout(io.StringIO()):
                rc = run_command(
                    Command(["definitely-not-a-real-executable"]),
                    repo_root=Path(tmpdir),
                    log_path=log_path,
                )
            log_text = log_path.read_text()

        self.assertEqual(rc, 127)
        self.assertIn("Command not found", log_text)

    def test_check_only_stage_fails_when_any_validator_fails(self) -> None:
        cfg = RunbookConfig(
            gcp_project="demo",
            skip_hapi_export=True,
            skip_vocab_load=True,
            dbt_project_dir="./dbt",
        )
        stage = Stage(
            key="demo",
            title="Demo",
            runbook_section="§x",
            description="demo",
            commands=lambda _: [],
            validators=lambda _: [
                BqCheck("failing", "sql-1", "should fail", interpret=lambda rows: (False, "bad")),
                BqCheck("passing", "sql-2", "should pass", interpret=lambda rows: (True, "good")),
            ],
            check_only=True,
        )
        original = runbook_main.run_bq_json
        runbook_main.run_bq_json = lambda sql, *, project, log_path: [{"sql": sql}]
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                console = Console(file=io.StringIO())
                ok, results = runbook_main._execute_stage(
                    stage,
                    cfg,
                    console=console,
                    log_path=Path(tmpdir) / "runbook.log",
                    extra_env=None,
                )
        finally:
            runbook_main.run_bq_json = original

        self.assertFalse(ok)
        self.assertEqual(results[0], ("failing", False, "bad"))
        self.assertEqual(results[1], ("passing", True, "good"))

    def test_validate_uses_repo_root_for_relative_paths(self) -> None:
        cfg = RunbookConfig(
            gcp_project="demo",
            skip_hapi_export=True,
            skip_vocab_load=True,
            dbt_project_dir="./dbt",
        )
        original_cwd = Path.cwd()
        try:
            # The validation base should be stable even if the caller's cwd changes.
            os.chdir("/tmp")
            self.assertEqual(
                cfg.validate(base_dir="/home/smudoshi/Github/FHIR2OMOP"),
                [],
            )
        finally:
            os.chdir(original_cwd)

    def test_exit_criteria_use_named_latest_results(self) -> None:
        criteria = dict(
            exit_criteria_statuses(
                completed=["dbt_parse", "dbt_build", "dbt_test"],
                check_results={
                    "some_person_check": (True, "misleading old pass"),
                    "raw_tables": (True, "ok"),
                    "final_person_measurement": (False, "empty"),
                    "measurement_gaps": (True, "reviewed"),
                    "observation_gaps": (True, "reviewed"),
                },
            )
        )

        self.assertTrue(criteria["Raw FHIR tables loaded"])
        self.assertTrue(criteria["dbt parse + build + test succeeded"])
        self.assertFalse(criteria["person and measurement non-zero"])
        self.assertTrue(criteria["Unknown-concept rates reviewed"])

    def test_dbt_hash_vars_only_toggle_hashing(self) -> None:
        cfg = RunbookConfig(hash_person_source_value=True)
        self.assertEqual(_dbt_vars_args(cfg), ["--vars", "{hash_person_source_value: true}"])

    def test_hashing_smoke_test_does_not_mutate_original_config(self) -> None:
        cfg = RunbookConfig(
            gcp_project="demo",
            skip_hapi_export=True,
            skip_vocab_load=True,
            dbt_project_dir="./dbt",
            hash_person_source_value=False,
        )
        original_resolve = runbook_main._resolve_pepper
        original_execute = runbook_main._execute_stage
        runbook_main._resolve_pepper = lambda smoke_cfg: "pepper-value"

        def fake_execute(stage, smoke_cfg, **kwargs):
            self.assertEqual(stage.key, "dbt_parse")
            self.assertTrue(smoke_cfg.hash_person_source_value)
            self.assertFalse(cfg.hash_person_source_value)
            self.assertEqual(kwargs["extra_env"], {"DBT_PEPPER": "pepper-value"})
            return True, []

        runbook_main._execute_stage = fake_execute
        try:
            rc = runbook_main._run_hashing_smoke_test(cfg, Console(file=io.StringIO()))
        finally:
            runbook_main._resolve_pepper = original_resolve
            runbook_main._execute_stage = original_execute

        self.assertEqual(rc, 0)
        self.assertFalse(cfg.hash_person_source_value)


if __name__ == "__main__":
    unittest.main()

"""
Resume state for the runbook wizard.

One state file per run, default path `.runbook_state.json`. Records which
stages completed, which failed, timestamps, and the associated config path
+ log file so --resume can pick up where we left off.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATE_SCHEMA_VERSION = 1


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class StageRecord:
    name: str
    status: str  # pending | in_progress | completed | failed | skipped
    started_at: str = ""
    finished_at: str = ""
    error: str = ""


@dataclass
class RunState:
    config_path: str = ""
    log_file: str = ""
    started_at: str = field(default_factory=_utcnow)
    last_updated_at: str = field(default_factory=_utcnow)
    stages: dict[str, StageRecord] = field(default_factory=dict)
    schema_version: int = STATE_SCHEMA_VERSION

    # ------------------------------------------------------------------
    # Stage helpers
    # ------------------------------------------------------------------
    def mark_started(self, name: str) -> None:
        rec = self.stages.get(name) or StageRecord(name=name, status="pending")
        rec.status = "in_progress"
        rec.started_at = _utcnow()
        rec.finished_at = ""
        rec.error = ""
        self.stages[name] = rec
        self._touch()

    def mark_completed(self, name: str) -> None:
        rec = self.stages.get(name) or StageRecord(name=name, status="pending")
        rec.status = "completed"
        rec.finished_at = _utcnow()
        rec.error = ""
        self.stages[name] = rec
        self._touch()

    def mark_failed(self, name: str, error: str) -> None:
        rec = self.stages.get(name) or StageRecord(name=name, status="pending")
        rec.status = "failed"
        rec.finished_at = _utcnow()
        rec.error = error
        self.stages[name] = rec
        self._touch()

    def mark_skipped(self, name: str) -> None:
        rec = self.stages.get(name) or StageRecord(name=name, status="pending")
        rec.status = "skipped"
        rec.finished_at = _utcnow()
        self.stages[name] = rec
        self._touch()

    def is_completed(self, name: str) -> bool:
        rec = self.stages.get(name)
        return bool(rec and rec.status in ("completed", "skipped"))

    def completed_names(self) -> list[str]:
        return [n for n, r in self.stages.items() if r.status in ("completed", "skipped")]

    def failed_names(self) -> list[str]:
        return [n for n, r in self.stages.items() if r.status == "failed"]

    def reset(self) -> None:
        self.stages.clear()
        self.started_at = _utcnow()
        self._touch()

    def _touch(self) -> None:
        self.last_updated_at = _utcnow()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = asdict(self)
        payload["stages"] = {k: asdict(v) for k, v in self.stages.items()}
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    @classmethod
    def load(cls, path: Path) -> "RunState":
        raw = json.loads(Path(path).read_text())
        stages_raw = raw.pop("stages", {}) or {}
        state = cls(**{k: v for k, v in raw.items() if k in {
            "config_path", "log_file", "started_at", "last_updated_at", "schema_version",
        }})
        state.stages = {
            k: StageRecord(**v) for k, v in stages_raw.items()
        }
        return state

    @classmethod
    def load_or_new(cls, path: Path) -> "RunState":
        path = Path(path)
        if path.exists():
            try:
                return cls.load(path)
            except (json.JSONDecodeError, TypeError):
                # Corrupt state file — back it up and start fresh.
                backup = path.with_suffix(path.suffix + ".bak")
                path.rename(backup)
        return cls()

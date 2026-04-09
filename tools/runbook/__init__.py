"""
Interactive TUI wizard that walks the user through the first warehouse-backed
validation run described in WAREHOUSE_VALIDATION_RUNBOOK.md.

Run with:
    python -m tools.runbook               # full interactive run
    python -m tools.runbook --dry-run     # collect inputs, preview commands, exit
    python -m tools.runbook --resume      # resume from saved state
    python -m tools.runbook --check-hashing  # validate dbt hashing + pepper wiring
"""

__version__ = "0.1.1"

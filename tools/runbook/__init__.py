"""
Interactive TUI wizard that walks the user through the first warehouse-backed
validation run described in WAREHOUSE_VALIDATION_RUNBOOK.md.

Run with:
    python -m tools.runbook               # full interactive run
    python -m tools.runbook --dry-run     # collect inputs, preview commands, exit
    python -m tools.runbook --resume      # resume from saved state
"""

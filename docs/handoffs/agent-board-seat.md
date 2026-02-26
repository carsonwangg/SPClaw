# Agent Handoff - Board Seat

## Branch
- `codex/agent-board-seat`

## Suggested Worktree
- `/Users/carsonwang/worktrees/coatue-claw/board-seat`

## Ownership Scope
- `src/coatue_claw/board_seat_daily.py`
- `src/coatue_claw/launchd_runtime.py` (board-seat schedule only)
- `tests/test_board_seat_daily.py`
- `tests/test_launchd_runtime.py` (board-seat schedule assertions)
- Board-seat runtime docs/handoffs

## Current Baseline
- Board Seat v1 rebuild is implemented (no longer reset scaffold).
- `board_seat_daily.py` now includes:
  - weekday-noon schedule gating
  - channel auto-discovery (`company_match`)
  - LLM-first target seeding + web verification/enrichment (`brave,serp`)
  - high-confidence new-target gate + 20-day cooldown (default)
  - repitch significance checks
  - concise 5-section output
  - funding confidence model + cache
  - memory-only rewrite fallback with warning thread
  - exhaustive candidate loop with rejection telemetry + candidate decision audit table
- Funding commands are live:
  - `refresh-funding`
  - `funding-quality-report`

## Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py`
- `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py`

## Merge Notes
- Rebase onto `origin/main` before merge.
- Role branch should not restart runtime directly; integrator handles deploy on `main`.

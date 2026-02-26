# Agent Handoff - Board Seat

## Branch
- `codex/agent-board-seat`

## Suggested Worktree
- `/Users/carsonwang/worktrees/coatue-claw/board-seat`

## Ownership Scope
- `src/coatue_claw/board_seat_daily.py`
- `tests/test_board_seat_daily.py`
- Board-seat runtime docs/handoffs

## Current Baseline (Reset)
- Board Seat has been intentionally scrapped and reset to scaffold as of commit `806e21b`.
- Current `board_seat_daily.py` is a reset baseline (`format_version: v0_reset_scaffold`), with live posting disabled by default.
- Rebuild should start from scratch from this scaffold, not from any prior quality/critic pipeline.
- Required env posture during rebuild:
  - `COATUE_CLAW_BOARD_SEAT_RESET_MODE=1`
  - `COATUE_CLAW_BOARD_SEAT_ENABLED=0`

## Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py`

## Merge Notes
- Rebase onto `origin/main` before merge.
- Do not restart runtime from this branch; integrator handles deploy on `main`.

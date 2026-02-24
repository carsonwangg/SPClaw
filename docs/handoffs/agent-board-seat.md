# Agent Handoff - Board Seat

## Branch
- `codex/agent-board-seat`

## Suggested Worktree
- `/Users/carsonwang/worktrees/coatue-claw/board-seat`

## Ownership Scope
- `src/coatue_claw/board_seat_daily.py`
- `tests/test_board_seat_daily.py`
- Board-seat runtime docs/handoffs

## Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py`

## Merge Notes
- Rebase onto `origin/main` before merge.
- Do not restart runtime from this branch; integrator handles deploy on `main`.

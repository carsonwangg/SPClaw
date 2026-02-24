# Agent Handoff - Market Daily

## Branch
- `codex/agent-market-daily`

## Suggested Worktree
- `/Users/carsonwang/worktrees/coatue-claw/market-daily`

## Ownership Scope
- `src/coatue_claw/market_daily.py`
- `tests/test_market_daily.py`
- Market-daily runtime docs/handoffs

## Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py`

## Merge Notes
- Rebase onto `origin/main` before merge.
- Do not restart runtime from this branch; integrator handles deploy on `main`.

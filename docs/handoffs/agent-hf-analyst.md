# Agent Handoff - Hedge Fund Analyst

## Branch
- `codex/agent-hf-analyst`

## Suggested Worktree
- `/Users/carsonwang/worktrees/coatue-claw/hf-analyst`

## Ownership Scope
- New HF analyst module(s) under `src/coatue_claw/`
- HF routing in `src/coatue_claw/slack_bot.py`
- HF command surface in `src/coatue_claw/cli.py`
- HF tests under `tests/`

## Validation
- Run targeted HF tests once created.
- Run routing regression tests when `slack_bot.py` changes.

## Merge Notes
- Rebase onto `origin/main` before merge.
- Do not restart runtime from this branch; integrator handles deploy on `main`.

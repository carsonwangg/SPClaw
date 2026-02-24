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

## Current Ship Status (2026-02-24)
- HFA V1 implementation is complete in this branch:
  - `src/coatue_claw/hf_document_extract.py`
  - `src/coatue_claw/hf_prompt_contract.py`
  - `src/coatue_claw/hf_store.py`
  - `src/coatue_claw/hf_analyst.py`
  - CLI wiring in `src/coatue_claw/cli.py`
  - Slack wiring in `src/coatue_claw/slack_bot.py`
  - memory helper extension in `src/coatue_claw/memory_runtime.py`
- Tests added:
  - `tests/test_hf_document_extract.py`
  - `tests/test_hf_prompt_contract.py`
  - `tests/test_hf_analyst.py`
- Validation baseline:
  - `PYTHONPATH=src python3 -m pytest -q` -> `240 passed`

## Immediate Integrator Checklist
1. Merge `codex/agent-hf-analyst` into `main`.
2. Restart OpenClaw and verify Slack command path:
   - `hfa analyze` in a thread with uploaded docs.
3. Verify DM auto-run dedupe:
   - first upload auto-runs once; repeated events for same file set do not rerun.

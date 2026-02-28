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
  - simplified LLM pipeline (active runtime path) with only two hard guards:
    - real company sanity check on web
    - 20-day no-repeat target lock
  - deeper simple-mode scraping with source page-content extraction for richer LLM grounding
  - high-confidence new-target gate + 20-day cooldown (default)
  - repitch significance checks
  - concise 5-section output
  - funding confidence model + cache
  - memory-only rewrite fallback with warning thread
  - exhaustive candidate loop with rejection telemetry + candidate decision audit table
  - simple-mode candidate schema trimmed to `name` only (no `one_line_fit` / `why_now`)
  - simple-mode draft prompt uses fetched source text only (no snippet claims), with funding inferred by LLM from those sources
- Funding commands are live:
  - `refresh-funding`
  - `funding-quality-report`

## Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py`
- `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py`

## Latest Update (2026-02-27)
- Conversational Slack command path now routes board-seat runs to `board_seat_daily`:
  - implemented `bs help`, `bs status`, `bs now` in `src/coatue_claw/slack_bot.py`.
  - `bs now` runs `board_seat_daily.run_once(force=True, dry_run=False)` scoped to current channel/company mapping.
  - this ensures on-demand posts use the same board-seat formatter and thread-source behavior as scheduler runs.

## Latest Update (2026-02-27, legacy v7 default restored)
- `src/coatue_claw/board_seat_daily.py` now defaults to legacy v7 formatter contract:
  - `BOARD_SEAT_FORMAT_VERSION=v7_legacy_with_target_line`
  - simple mode path is disabled; legacy v7 is the only active runtime path.
  - deterministic/LLM draft contract and quality gate aligned to v7 labeled structure with explicit `What target does` line under `Thesis`.
- `.env.example` updated to document `COATUE_CLAW_BOARD_SEAT_SIMPLE_MODE` as deprecated/no-op.
- `AGENTS.md` board-seat section updated from v1 5-section contract to v7 labeled-line contract.

## Merge Notes
- Rebase onto `origin/main` before merge.
- Role branch should not restart runtime directly; integrator handles deploy on `main`.

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

## Latest Update (2026-02-28, fail-closed board-seat command routing)
- Updated `/Users/carsonwang/worktrees/coatue-claw/board-seat/src/coatue_claw/slack_routing.py`:
  - added `is_explicit_board_seat_command(text)` to detect explicit `bs ...` / `board seat ...` commands after Slack mention stripping.
- Updated `/Users/carsonwang/worktrees/coatue-claw/board-seat/src/coatue_claw/slack_bot.py`:
  - explicit board-seat commands are now fail-closed near the HFA fast-path:
    - if `_handle_board_seat_command` handles it, return immediately.
    - if not handled, post a deterministic routing error with valid `bs` commands and return immediately (no conversational fallthrough).
  - `bs status` response now emits canonical structured fields:
    - `format_version`, `status`, `enabled`, `schedule_time`, `target_lock_days`, `portcos`.
- Updated tests:
  - `/Users/carsonwang/worktrees/coatue-claw/board-seat/tests/test_slack_routing.py` adds explicit board-seat command detector coverage.
  - `/Users/carsonwang/worktrees/coatue-claw/board-seat/tests/test_slack_bot_board_seat_routing.py` adds fail-closed routing regression test (auto-skips when `slack_bolt` is unavailable in local interpreter).

## Latest Update (2026-02-27)
- Conversational Slack command path now routes board-seat runs to `board_seat_daily`:
  - implemented `bs help`, `bs status`, `bs now` in `src/coatue_claw/slack_bot.py`.
  - `bs now` runs `board_seat_daily.run_once(force=True, dry_run=False)` scoped to current channel/company mapping.
  - this ensures on-demand posts use the same board-seat formatter and thread-source behavior as scheduler runs.

## Latest Update (2026-02-27, warning-only quality handling)
- Removed memory rewrite fallback from `src/coatue_claw/board_seat_daily.py`.
- When quality gates fail after web rewrite attempts:
  - no fallback pitch is posted
  - run returns `draft_quality_failed`
  - diagnostic warning is posted with failure codes for debugging.

## Latest Update (2026-02-27, v8 concise full-context contract)
- `src/coatue_claw/board_seat_daily.py` now uses `BOARD_SEAT_FORMAT_VERSION=v8_full_context_concise`.
- Drafting now uses full source extracts for target/company/funding evidence and produces concise sections:
  - `Thesis`
  - `What target does`
  - `Why now`
  - `Fit + value creation`
  - `Risks / kill criteria`
  - `Funding snapshot`

## Latest Update (2026-02-27, v8 guardrail relaxation)
- Quality gate softened to reduce false skips:
  - section/bullet count mismatches are warning-level unless major structure is missing.
  - main-draft `Sources` blocks are auto-removed, not hard-failed.
  - run still preserves quality fail telemetry for operator diagnostics.

## Merge Notes
- Rebase onto `origin/main` before merge.
- Role branch should not restart runtime directly; integrator handles deploy on `main`.

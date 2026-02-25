# Live Session Handoff (Coatue Claw)

## Objective
Ship valuation charting into the OpenClaw-native Slack workflow.

## Update (2026-02-25, board-seat candidate quality recovery: medium+new + broad weighted scoring)
- Implemented candidate quality recovery in `/Users/carsonwang/worktrees/coatue-claw/board-seat/src/coatue_claw/board_seat_daily.py`:
  - target confidence model now defaults to `broad_weighted_v1` with deterministic score bands:
    - `COATUE_CLAW_BOARD_SEAT_CONFIDENCE_HIGH_MIN` (default `2.40`)
    - `COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MEDIUM_MIN` (default `1.35`)
  - gate policy now allows **new + High/Medium** when enabled:
    - `COATUE_CLAW_BOARD_SEAT_ALLOW_MEDIUM_NEW_TARGET=1` (default)
  - conceptual target rejection expanded via `_is_conceptual_target_name(...)` and shared validation:
    - blocks generic targets like `LLMs`, `ROI`, `workflow`, `platform`
    - preserves concrete startup names like `Browserbase`, `Scale AI`
  - target gate payload now emits debug fields:
    - `target_confidence_score`
    - `target_confidence_reasons`
    - `target_validation_reason`
  - dry-run/live `sent` + `skipped` rows now include the confidence debug fields for diagnosis.
- Added/updated regression coverage in `/Users/carsonwang/worktrees/coatue-claw/board-seat/tests/test_board_seat_daily.py`:
  - conceptual `LLMs` target rejected and retargeted to concrete company
  - medium-confidence new target allowed under broad weighted model
  - low-score target still rejected
  - non-new target still rejected even with high confidence
  - run-once payload includes new confidence debug fields
  - run-once dry-run retarget path confirms no conceptual target leakage

## Update (2026-02-25, board-seat API health + Brave key alias support)
- Diagnosed OpenAI board-seat quality degradation causes:
  - `COATUE_CLAW_BRAVE_API_KEY` was set, but resolver only read `BRAVE_SEARCH_API_KEY`; Brave rows were effectively disabled.
  - SerpAPI endpoint currently responds `HTTP 429 Too Many Requests` for board-seat queries in this environment, yielding zero Google rows.
- Implemented key resolver compatibility in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py`:
  - `_brave_search_api_key()` now accepts both:
    - `COATUE_CLAW_BRAVE_API_KEY`
    - `BRAVE_SEARCH_API_KEY`
- Added regression test:
  - `/Users/carsonwang/CoatueClaw/tests/test_board_seat_daily.py::test_brave_api_key_accepts_coatue_claw_alias`
- Updated env sample:
  - `/Users/carsonwang/CoatueClaw/.env.example` now lists `COATUE_CLAW_BRAVE_API_KEY`.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `46 passed`
  - `PYTHONPATH=src python3 -m pytest -q` -> `310 passed`

## Update (2026-02-25, strict new-target gate: skip when no high-confidence new target)
- Implemented a strict board-seat post gate in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py`:
  - posts now require both:
    - a **new target** (not already in target memory for that company)
    - **High confidence** target evidence (`High` from source-confidence scoring)
  - otherwise run result is skipped with:
    - `reason=no_high_confidence_new_target`
    - `gate_reason` in `{invalid_target,target_not_new,target_confidence_not_high}`
- Added env control (default enabled):
  - `COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET=1`
- Status payload now surfaces the gate mode:
  - `require_high_conf_new_target`
- Added regression coverage in `/Users/carsonwang/CoatueClaw/tests/test_board_seat_daily.py`:
  - low-confidence target gets rejected by gate
  - non-new target gets rejected even when source confidence is high
  - run-once dry-run skips when gate is not satisfied
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `45 passed`
  - `PYTHONPATH=src python3 -m pytest -q` -> `309 passed`

## Update (2026-02-25, board-seat target hardening to reject conceptual/non-company targets)
- Fixed board-seat target selection in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py` so conceptual labels do not pass as acquisition targets:
  - added `aifirst` to `ACQ_PLACEHOLDER_TARGETS`
  - added `ai-first` / `ai first` to `ACQ_INVALID_TARGET_TERMS`
  - added `roi` to `TARGET_TOKEN_STOPWORDS`
  - introduced `_canonical_target_key(...)` and used it in target validation/candidate filtering to reject possessive/pluralized self-target variants (for example `OpenAIs` for `OpenAI`)
- Added regression coverage in `/Users/carsonwang/CoatueClaw/tests/test_board_seat_daily.py`:
  - `test_is_valid_target_name_rejects_ai_first_placeholder`
  - `test_is_valid_target_name_rejects_possessive_company_variant`
  - `test_is_valid_target_name_rejects_metric_token`
  - existing rewrite test confirms fallback to a concrete target (`Browserbase`) when seed target is conceptual (`AI-first`)
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `42 passed`
  - `PYTHONPATH=src python3 -m pytest -q` -> `306 passed`

## Update (2026-02-24, board-seat sqlite connection lifecycle fix for ledger FD exhaustion)
- Fixed file-descriptor leak in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py`:
  - `BoardSeatStore._connect()` is now a context manager that always commits/rolls back and closes the sqlite connection.
  - previous behavior relied on sqlite connection context semantics, which do not close the connection and can accumulate open descriptors under repeated board-seat operations.
- Added regression coverage in `/Users/carsonwang/CoatueClaw/tests/test_board_seat_daily.py`:
  - `test_store_connect_context_closes_connection` validates commit + close behavior for `_connect()` context lifecycle.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `38 passed`
  - `PYTHONPATH=src python3 -m pytest -q` -> `257 passed`

### Runtime verification (Mac mini)
1. Pulled latest `main` (`6a62458`) to `/opt/coatue-claw`, restarted gateway, and re-ran board-seat dry-run:
   - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.board_seat_daily run-once --force --dry-run`
2. Confirmed ledger payload no longer reports FD exhaustion:
   - `ledger_error=None`
   - `ledger` now includes expected output paths:
     - `csv_path`
     - `json_path`
     - `mirror_csv_path`
     - `mirror_json_path`
3. Post-restart Slack probe recovered healthy (`make openclaw-slack-status` -> `probe.ok=true`).

## Update (2026-02-24, integrator merge + deploy of board-seat funding hardening / strict repitch governance)
- Integrated `origin/codex/agent-board-seat` into `main`:
  - merge commit: `fd8a942` (`Merge board-seat funding hardening and strict repitch governance`)
  - merge conflicts resolved in:
    - `/Users/carsonwang/CoatueClaw/Makefile`
    - `/Users/carsonwang/CoatueClaw/docs/handoffs/current-plan.md`
    - `/Users/carsonwang/CoatueClaw/docs/handoffs/live-session.md`
- Integrator validation on merged tree:
  - `PYTHONPATH=src python3 -m pytest -q` -> `256 passed`
- Mac mini deploy verification (`/opt/coatue-claw`):
  - pulled latest `main` and restarted runtime via `make openclaw-restart`
  - `make openclaw-slack-status` recovered to healthy probe (`ok=true`) after restart settle
  - `make openclaw-board-seat-status` confirms:
    - `target_lock_days=14`
    - funding quality/verification payloads present
  - `make openclaw-board-seat-refresh-funding` completed (`entities_refreshed=23`)
  - `make openclaw-board-seat-funding-report` completed:
    - `/opt/coatue-claw-data/artifacts/board-seat/funding-quality-report-2026-02-24.md`
  - `make openclaw-board-seat-run-once DRY_RUN=1 FORCE=1` executed:
    - strict repitch gate active (`repitch_not_significant_enough` paths surfaced)
    - hard 14-day no-repeat behavior active
  - observed runtime warning in dry-run payload:
    - ledger export reported `[Errno 24] Too many open files` on target ledger write path (non-fatal for run result; follow-up candidate).

## Update (2026-02-24, launchd 24x7 enable resilience for transient bootstrap errors)
- Hardened `/Users/carsonwang/CoatueClaw/src/coatue_claw/launchd_runtime.py` against intermittent launchctl bootstrap failures seen in `make openclaw-24x7-enable`:
  - `_bootstrap(...)` now retries transient `Input/output error` failures (configurable via `COATUE_CLAW_LAUNCHCTL_BOOTSTRAP_RETRIES`, default `3`).
  - `enable_services(...)` now raises label-specific errors (`failed enabling <label>: ...`) so failing service is explicit.
- Added regression coverage in `/Users/carsonwang/CoatueClaw/tests/test_launchd_runtime.py`:
  - retry path succeeds after first transient bootstrap failure
  - enable error includes failing service label
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
  - `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py tests/test_launchd_runtime.py` -> `44 passed`

### Runtime verification (Mac mini)
1. Pulled `main` to `/opt/coatue-claw` (`da144cd`) and restarted gateway:
   - `make openclaw-restart`
2. Re-ran 24x7 enable:
   - `make openclaw-24x7-enable` -> success
3. Verified scheduler load state:
   - `make openclaw-24x7-status` shows all service labels loaded, including:
     - `com.coatueclaw.market-daily`
     - `com.coatueclaw.market-daily-earnings-recap`
4. Verified Slack health:
   - `make openclaw-slack-status` -> `probe.ok=true`

## Update (2026-02-24, SSH host alias memory for future Codex sessions)
- Added operator memory to `AGENTS.md`:
  - Mac mini SSH host alias is `mini` (`ssh mini`)
  - explicit guardrail added to avoid storing SSH passwords/secrets in git
- Integrator deploy/health check was executed via `ssh mini`:
  - pulled latest `main` in `/opt/coatue-claw` (now at `e5ffeb7`)
  - `make openclaw-restart` succeeded
  - `make openclaw-slack-status` probe now reports `ok: true`
  - `make openclaw-slack-logs` confirms Slack socket mode connected

## Update (2026-02-24, HFA V1 docs-first workflow shipped on `codex/agent-hf-analyst`)
- Implemented HFA V1 modules:
  - `src/coatue_claw/hf_document_extract.py`
  - `src/coatue_claw/hf_prompt_contract.py`
  - `src/coatue_claw/hf_store.py`
  - `src/coatue_claw/hf_analyst.py`
- Implemented docs-first extraction for:
  - PDF (`pypdf`)
  - DOCX (`python-docx`)
  - PPTX (`python-pptx`)
  - TXT/MD/CSV (native read path)
- Added HFA run persistence and audit tables in `/opt/coatue-claw-data/db/hf_analyst.sqlite`:
  - `hf_runs`
  - `hf_run_inputs`
  - `hf_run_sections`
  - `hf_dm_autoruns`
- Added CLI surface in `src/coatue_claw/cli.py`:
  - `claw hfa analyze --channel <id> --thread-ts <ts> [--question "..."] [--dry-run]`
  - `claw hfa status [--channel <id>] [--thread-ts <ts>]`
- Added Slack HFA workflow in `src/coatue_claw/slack_bot.py`:
  - explicit `hfa analyze [optional question]`
  - explicit `hfa status`
  - DM auto-run on new file sets only (deduped by file-set hash in `hf_dm_autoruns`)
- Added HFA memory writeback helper in `src/coatue_claw/memory_runtime.py`:
  - `ingest_hfa_facts(...)` persists thesis/catalysts/risks/score/artifact pointer
  - no full memo body writeback
- Added tests:
  - `tests/test_hf_document_extract.py`
  - `tests/test_hf_prompt_contract.py`
  - `tests/test_hf_analyst.py`
- Dependency updates in `pyproject.toml`:
  - `pypdf`
  - `python-docx`
  - `python-pptx`

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_hf_document_extract.py tests/test_hf_prompt_contract.py tests/test_hf_analyst.py` -> `12 passed`
- `PYTHONPATH=src python3 -m pytest -q tests/test_memory_runtime.py tests/test_slack_routing.py tests/test_slack_file_ingest.py` -> `9 passed`
- `PYTHONPATH=src python3 -m pytest -q` -> `240 passed`
- `python3 -m compileall -q src/coatue_claw/hf_document_extract.py src/coatue_claw/hf_prompt_contract.py src/coatue_claw/hf_store.py src/coatue_claw/hf_analyst.py src/coatue_claw/cli.py src/coatue_claw/slack_bot.py src/coatue_claw/memory_runtime.py` -> pass

### Integrator Runtime Steps (after merge to `main`)
1. `cd /opt/coatue-claw && git pull origin main`
2. `make openclaw-restart`
3. `make openclaw-slack-status`
4. In Slack thread with uploaded docs, run:
   - `hfa analyze`
5. In DM with uploaded docs (no command), verify single auto-run and no duplicate rerun on same file set.
## Update (2026-02-24, MD catalyst + link relevance hardening for AMD/INTC regression)
- Implemented in `/Users/carsonwang/worktrees/coatue-claw/market-daily/src/coatue_claw/market_daily.py`:
  - Added deterministic `_is_low_signal_x_post(...)` gate and applied it before X evidence candidate scoring.
  - Added direct evidence fallback path for catalyst generation:
    - if cluster confirmation/decisive-primary fails, high-quality Yahoo/Web causal headlines can still generate specific reason lines.
  - Added cause-aware link rendering:
    - fallback line hides `[X]`
    - fallback keeps only quality `[News]/[Web]`
    - specific lines include `[X]` only when stricter relevance checks pass.
  - Added `CatalystEvidence` trace fields:
    - `cause_mode`, `cause_source_type`, `cause_source_url`
  - Added artifact/debug trace output for cause fields.
- Tests updated in `/Users/carsonwang/worktrees/coatue-claw/market-daily/tests/test_market_daily.py`:
  - promo X spam rejection
  - direct evidence fallback without cluster match
  - fallback link policy (`[X]` suppressed)
  - specific-line relevant X inclusion
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py` -> `40 passed`
  - `PYTHONPATH=src python3 -m pytest -q` -> `238 passed`

### Immediate next steps
1. Merge role branch into integrator queue.
2. Restart runtime on Mac mini and verify next MD post no longer shows random `[X]` on fallback lines.
3. Run `md debug AMD` / `md debug INTC` after next cycle and verify `cause_mode` + `cause_source_*` fields in debug output.

## Update (2026-02-24, Market Daily earnings preview + 7PM recap)
- Implemented in `/Users/carsonwang/worktrees/coatue-claw/market-daily/src/coatue_claw/market_daily.py`:
  - Morning `open` slot MD posts now append `Earnings After Close Today` using final MD universe members.
  - Added nightly earnings recap runner:
    - command: `python -m coatue_claw.market_daily run-earnings-recap`
    - default schedule time: `19:00` local runtime (`COATUE_CLAW_MD_EARNINGS_RECAP_TIME`)
    - recap selection: top 4 same-day reporters by absolute move since regular close
    - output contract: 2-4 bullet recap per ticker with source links; deterministic fallback when LLM unavailable.
- CLI wiring in `/Users/carsonwang/worktrees/coatue-claw/market-daily/src/coatue_claw/cli.py`:
  - `claw market-daily run-earnings-recap --manual|--force|--dry-run|--channel`
- Slack wiring in `/Users/carsonwang/worktrees/coatue-claw/market-daily/src/coatue_claw/slack_bot.py`:
  - `md earnings now`
  - `md earnings now force`
- launchd/runtime wiring in `/Users/carsonwang/worktrees/coatue-claw/market-daily/src/coatue_claw/launchd_runtime.py`:
  - new service label: `com.coatueclaw.market-daily-earnings-recap`
  - included in `marketdaily` service selector and `all` bundle.
- ops/runbook updates:
  - `Makefile`: `openclaw-market-daily-earnings-recap-run-once`
  - `docs/openclaw-runtime.md`: command/scheduler/env additions
  - `.env.example`: added recap-time env sample
- validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_market_daily.py tests/test_launchd_runtime.py` -> `42 passed`

### Immediate next steps
1. Integrator merge this branch to `main`.
2. On Mac mini, restart runtime and enable 24x7 services:
   - `make openclaw-restart`
   - `make openclaw-24x7-enable`
   - `make openclaw-24x7-status`
3. Smoke check:
   - `make openclaw-market-daily-run-once DRY_RUN=1`
   - `make openclaw-market-daily-earnings-recap-run-once DRY_RUN=1`
   - Slack: `md status` should show `earnings_recap_time`.
## Update (2026-02-24, Board Seat hard 14-day no-repeat + strict repitch evidence gate)
- Implemented strict repitch governance in `/Users/carsonwang/worktrees/coatue-claw/board-seat/src/coatue_claw/board_seat_daily.py`:
  - hard no-repeat rule: same target cannot be re-pitched within `14` days (non-bypassable).
  - default target lock updated to `14` days (`COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS`, minimum enforced 14).
  - `COATUE_CLAW_BOARD_SEAT_ALLOW_REPEAT_TARGETS=1` now only bypasses configurable lock window beyond 14 days; it cannot bypass the hard 14-day rule.
- Added continuous promising-target event tracking:
  - new DB table: `board_seat_target_events`
  - each run tracks event flow for promising targets (current target + rotation + target memory) and stores normalized event rows with:
    - `event_type`
    - `impact_score`
    - `evidence_quality`
    - source metadata and timestamps
- Added strict post-lock repitch assessment:
  - new DB table: `board_seat_repitch_assessments`
  - repeated ideas after lock window are only allowed when events meet exceptional thresholds (critical bias against repeats).
  - rejected resurfacing is explicit with `repitch_not_significant_enough`.
- Added explicit repitch disclosure when resurfacing is allowed:
  - message includes:
    - `Repitch note`
    - `New evidence`
  - pitch audit fields now persisted in `board_seat_pitches`:
    - `is_repitch`
    - `repitch_of_pitch_id`
    - `repitch_prev_posted_at_utc`
    - `repitch_similarity`
    - `repitch_new_evidence_json`

### Validation
- `python3 -m compileall -q src/coatue_claw/board_seat_daily.py` -> pass
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `37 passed`
- `PYTHONPATH=src python3 -m pytest -q` -> `236 passed`

## Update (2026-02-24, Board Seat funding accuracy hardening: web-first + warning-mode)
- Implemented funding accuracy hardening in `/Users/carsonwang/worktrees/coatue-claw/board-seat/src/coatue_claw/board_seat_daily.py`:
  - `FundingSnapshot` expanded with:
    - `evidence_count`
    - `distinct_domains`
    - `conflict_flags`
    - `verification_status`
  - `board_seat_funding_cache` schema expanded with persisted columns for the above fields plus migration-safe `ALTER TABLE` add-on logic.
  - funding evidence pipeline now normalizes and scores web rows before extraction:
    - canonical URL normalization + dedupe
    - low-signal row rejection
    - top-N evidence selection (`COATUE_CLAW_BOARD_SEAT_FUNDING_WEB_TOP_ROWS`, default `8`)
    - conflict flag detection (`major_round_mismatch`, `major_amount_mismatch`, `minor_date_variance`)
  - funding confidence/verification contract added:
    - env controls:
      - `COATUE_CLAW_BOARD_SEAT_FUNDING_MIN_DOMAINS` (default `2`)
      - `COATUE_CLAW_BOARD_SEAT_FUNDING_LOW_CONF_THRESHOLD` (default `0.55`)
      - `COATUE_CLAW_BOARD_SEAT_FUNDING_WARNING_MODE` (default `1`)
    - low-confidence funding now appends warning line:
      - `Warning: Funding data is low-confidence; verify before action.`
  - status telemetry expanded:
    - `funding_verification_by_company`
    - `funding_quality_metrics` (`verified_pct`, `low_confidence_pct`, `oldest_cache_age_days`, counts)
  - new board-seat CLI commands shipped:
    - `refresh-funding --all-portcos`
    - `funding-quality-report --all-portcos`
    - report artifact: `funding-quality-report-YYYY-MM-DD.md` under board-seat artifact dir
  - Make targets added:
    - `openclaw-board-seat-refresh-funding`
    - `openclaw-board-seat-funding-report`

### Validation
- `python3 -m compileall -q src/coatue_claw/board_seat_daily.py` -> pass
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `35 passed`

### Tests added/updated
- canonical URL funding-evidence dedupe
- funding conflict detection (round/amount mismatch)
- funding confidence band mapping (`high`/`medium`/`low`)
- low-confidence warning line rendering in board-seat message
- web refresh domain/evidence metrics path
- status funding quality metric payload coverage

### Immediate runtime steps (Mac mini integrator)
1. `cd /opt/coatue-claw && git pull origin main`
2. Set runtime keys in `/opt/coatue-claw/.env.prod` for web-first funding:
   - `BRAVE_SEARCH_API_KEY`
   - `COATUE_CLAW_BOARD_SEAT_GOOGLE_SERP_API_KEY` (or `SERPAPI_API_KEY`)
   - keep `COATUE_CLAW_BOARD_SEAT_CRUNCHBASE_ENABLED=0` until Crunchbase key is provisioned
3. `make openclaw-restart`
4. `make openclaw-board-seat-status`
5. `make openclaw-board-seat-refresh-funding`
6. `make openclaw-board-seat-funding-report`

## Update (2026-02-24, parallel Codex branch/worktree protocol + agent handoff docs)
- Added parallel-branch operating protocol to `/Users/carsonwang/CoatueClaw/AGENTS.md`:
  - branch naming standard: `codex/agent-board-seat`, `codex/agent-chart-day`, `codex/agent-hf-analyst`, `codex/agent-market-daily`
  - integrator-only deploy gate preserved (`main` merge -> restart + runtime verification).
- Added per-agent continuity docs:
  - `/Users/carsonwang/CoatueClaw/docs/handoffs/agent-board-seat.md`
  - `/Users/carsonwang/CoatueClaw/docs/handoffs/agent-chart-day.md`
  - `/Users/carsonwang/CoatueClaw/docs/handoffs/agent-hf-analyst.md`
  - `/Users/carsonwang/CoatueClaw/docs/handoffs/agent-market-daily.md`
- Worktree bootstrap target paths standardized:
  - `/Users/carsonwang/worktrees/coatue-claw/board-seat`
  - `/Users/carsonwang/worktrees/coatue-claw/chart-day`
  - `/Users/carsonwang/worktrees/coatue-claw/hf-analyst`
  - `/Users/carsonwang/worktrees/coatue-claw/market-daily`
- Created and pushed parallel role branches:
  - `origin/codex/agent-board-seat`
  - `origin/codex/agent-chart-day`
  - `origin/codex/agent-hf-analyst`
  - `origin/codex/agent-market-daily`
- Verified active worktrees and branch mapping:
  - `/Users/carsonwang/CoatueClaw` -> `main`
  - `/Users/carsonwang/worktrees/coatue-claw/board-seat` -> `codex/agent-board-seat`
  - `/Users/carsonwang/worktrees/coatue-claw/chart-day` -> `codex/agent-chart-day`
  - `/Users/carsonwang/worktrees/coatue-claw/hf-analyst` -> `codex/agent-hf-analyst`
  - `/Users/carsonwang/worktrees/coatue-claw/market-daily` -> `codex/agent-market-daily`

### Immediate next steps
1. Start one Codex session per worktree and keep edits within role-owned files.
2. Use only role-owned files on each branch to minimize merge collisions.
3. Merge to `main` via integrator queue only; restart runtime once per merged batch.

## Update (2026-02-24, Board Seat V6 structured headers + target funding reliability)
- Implemented Board Seat V6 in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py`.
- Output contract changes now live:
  - removed `Idea confidence` line from thesis.
  - added required `*Target does:*` directly under `*Idea:*`.
  - `Why now` is validated for monthly trend framing; explicit `last 24 hours` phrasing is rejected.
- Slack delivery now supports section-header rich text:
  - bold + underlined headers for:
    - `Thesis`
    - `{Company} context`
    - `Funding snapshot`
    - `Sources`
  - plaintext message remains canonical for storage, memory parsing, repeat guard, and backfill.
  - if Slack rejects rich-text blocks (`invalid_blocks`/`invalid_arguments`), posting auto-falls back to plaintext contract.
- Funding reliability updates:
  - default funding scope is target company (`COATUE_CLAW_BOARD_SEAT_FUNDING_SCOPE=target`).
  - Crunchbase API is primary funding provider when key is set (`COATUE_CLAW_CRUNCHBASE_API_KEY`).
  - web funding fallback merges Brave + Google SERP evidence when Crunchbase is unavailable or sparse.
  - unknown funding fallback remains explicit:
    - `Target funding data is limited; verify via Crunchbase/PitchBook before action.`
- Additional guardrails:
  - low-signal fallback phrasing updated to monthly trend language.
  - target token stopwords expanded to avoid malformed target extraction from temporal words (for example `Over`).

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `28 passed`
- `python3 -m compileall -q src/coatue_claw/board_seat_daily.py` -> pass

### Tests added/updated
- rejects 24h `Why now` phrasing at draft validation.
- verifies rich-text headers are bold + underlined.
- verifies target line is required in message structure.
- verifies funding resolution prefers Crunchbase primary when available.

### Immediate runtime steps (Mac mini)
1. `cd /opt/coatue-claw && git pull origin main`
2. `make openclaw-restart`
3. `make openclaw-slack-status`
4. Dry-run one channel:
   - `COATUE_CLAW_BOARD_SEAT_PORTCOS='OpenAI:openai' /opt/coatue-claw/.venv/bin/python -m coatue_claw.board_seat_daily run-once --force --dry-run`
5. Live-post one channel and verify rendering:
   - header styling visible
   - `Target does` present
   - no `Idea confidence`
   - funding snapshot is target-scoped

## Update (2026-02-24, X-chart scheduled slot miss fix for Morning/Afternoon/Evening posts)
- Root cause identified in `src/coatue_claw/x_chart_daily.py`:
  - launchd runs scout on `StartInterval=3600`, which drifts by minute offset from service load time.
  - slot detection previously only posted when runtime minute was within ±20 of `COATUE_CLAW_X_CHART_WINDOWS` (`09:00,12:00,18:00`), so an offset runtime (for example `:34`) could miss all slots.
- Fixed `_slot_key(...)` behavior:
  - scheduled runs now map to the most recent elapsed configured window on that local day.
  - dedupe (`was_slot_posted`) still guarantees one post per slot/day.
  - effect: if run happens at `09:34`, it still posts `09:00` slot once; same pattern for `12:00` and `18:00`.
- Test updates in `tests/test_x_chart_daily.py`:
  - added direct coverage for elapsed-window mapping.
  - adjusted pool/scout tests to use true pre-first-window times for non-posting behavior.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `74 passed`
  - `PYTHONPATH=src python3 -m pytest -q tests/test_launchd_runtime.py` -> `5 passed`

### Runtime verification (Mac mini)
1. Deployed latest main (`bb0472f`) and restarted runtime:
   - `cd /opt/coatue-claw && git pull origin main`
   - `make openclaw-restart`
2. Verified scheduler/service health:
   - `make openclaw-24x7-status` -> `com.coatueclaw.x-chart-daily` loaded
   - `make openclaw-x-chart-status` showed stale scheduled pointer pre-fix (`last_scheduled_posted_at_utc=2026-02-21...`)
3. Live validation at `Tue Feb 24 10:55:57 PST 2026`:
   - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-once`
   - result posted scheduled slot `2026-02-24-09:00` with convention `Coatue Chart of the Morning`
   - `make openclaw-x-chart-status` now shows refreshed scheduled post timestamp (`2026-02-24T18:56:04.178971+00:00`)

## Update (2026-02-24, Board Seat V6 target-memory lock + ledger + format guard)
- Implemented hard target-memory controls in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py`:
  - new SQLite table: `board_seat_target_memory`
  - per-company target lock window via `COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS` (default `30`)
  - repeat-target bypass toggle only by explicit override: `COATUE_CLAW_BOARD_SEAT_ALLOW_REPEAT_TARGETS=1`
- Repeat-prevention now checks target key before text-similarity fallback:
  - if a target was recently pitched (for example `Epirus` for `Anduril`), board-seat auto-retargets to the next candidate.
  - if no alternate target is available, post is skipped with explicit reason `repeat_target_within_lock_window`.
- Added persistent target ledger exports:
  - artifact paths:
    - `/opt/coatue-claw-data/artifacts/board-seat/board-seat-target-ledger.csv`
    - `/opt/coatue-claw-data/artifacts/board-seat/board-seat-target-ledger.json`
  - Google Drive mirror support:
    - default mirror path: `/Users/spclaw/Documents/SPClaw Database/Companies/Board-Seat`
    - configurable via `COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_PATH`
- Backfill parser broadened to include legacy numbered board-seat messages (for example `1. Idea title`, `Target(s) / sector`) so historical targets enter memory.
- Added deterministic format contract validator before post:
  - rejects numbered templates and missing labeled fields
  - fallback regeneration attempted once; if still invalid, run skips with `invalid_format_contract`.
- Follow-up parser hardening:
  - added `boardseat`/`board` placeholder blocks in target normalization to prevent legacy header text from being stored as a target company.
- New CLI surface in `board_seat_daily`:
  - `seed-target --company <name> --target <target>`
  - `target-memory [--company ...] [--limit ...]`
  - `export-ledger [--company ...]`

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `25 passed`
- `PYTHONPATH=src python3 -m pytest -q` -> `217 passed`

### Immediate runtime steps
1. On Mac mini, seed Epirus into target memory:
   - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.board_seat_daily seed-target --company Anduril --target Epirus`
2. Restart + health check:
   - `make -C /opt/coatue-claw openclaw-restart`
   - `make -C /opt/coatue-claw openclaw-slack-status`
3. Verify ledger mirror files exist under:
   - `/Users/spclaw/Documents/SPClaw Database/Companies/Board-Seat/`

## Update (2026-02-24, Board Seat V5 target-first sources + confidence)
- Implemented Board Seat V5 in `/Users/carsonwang/CoatueClaw/src/coatue_claw/board_seat_daily.py`.
- `format_version` bumped to:
  - `v5_target_first_confidence_sources`
- New deterministic source policy path:
  - default policy: `target_first_3_1`
  - source classification buckets: `target_direct`, `target_proxy`, `parent_context`, `funding_context`
  - enforced mix: up to 3 target refs + up to 1 parent-context ref
  - funding links are excluded from `Sources` by default (funding remains in Funding snapshot text)
- New target-first evidence plumbing:
  - target token extraction from idea line
  - target-focused Brave retrieval (`_target_search_rows`) merged with model refs + acquisition rows
  - quality-domain weighting + low-quality downranking
  - URL/title-fingerprint dedupe
- New confidence output:
  - `BoardSeatDraft` now carries `idea_confidence`
  - rendered line under Thesis:
    - `*Idea confidence:* High|Medium|Low`
  - confidence is deterministic from selected source composition (not model-only)
- LLM draft prompt updated:
  - requires `idea_confidence` field
  - asks for target-company/idea-specific source refs
  - explicitly avoids parent funding links in source refs
- Fallback source behavior updated:
  - when refs are missing, fallback search links are target-oriented (not funding-oriented)

### Public config added
- `COATUE_CLAW_BOARD_SEAT_SOURCE_POLICY` (default `target_first_3_1`)
- `COATUE_CLAW_BOARD_SEAT_INCLUDE_FUNDING_LINKS` (default `0`)
- `COATUE_CLAW_BOARD_SEAT_TARGET_MIN_QUALITY_SOURCES` (default `1`)
- `COATUE_CLAW_BOARD_SEAT_TARGET_MIN_TOTAL_SOURCES` (default `2`)
- `COATUE_CLAW_BOARD_SEAT_LOW_SIGNAL_MODE` (default `candidate_with_confidence`)

### Tests
- Updated `/Users/carsonwang/CoatueClaw/tests/test_board_seat_daily.py` for V5.
- Added coverage for:
  - source classification (target/parent/funding)
  - target-first composer mix and funding-link exclusion
  - low-signal confidence behavior
  - confidence line rendering in message output
- Validation run:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py`
  - Result: `19 passed`

### Runtime verification (Mac mini)
- Pulled and deployed commit `8a1a72a` to `/opt/coatue-claw`.
- Restarted gateway:
  - `make openclaw-restart`
- Verified Slack health:
  - `make openclaw-slack-status` -> `probe.ok=true`
- Verified board-seat module state:
  - `make openclaw-board-seat-status` -> `format_version: v5_target_first_confidence_sources`
- Dry-run verification (OpenAI):
  - `COATUE_CLAW_BOARD_SEAT_PORTCOS='OpenAI:openai' /opt/coatue-claw/.venv/bin/python -m coatue_claw.board_seat_daily run-once --force --dry-run`
  - preview includes `*Idea confidence:* ...` and v5 source block.

### Immediate next steps
1. Validate one live `#openai` post:
   - has `Idea confidence` line
   - `Sources` are target-first (Browserbase-style evidence), no generic parent funding links.

## Update (2026-02-24, Board Seat V5 fallback target hardening)
- Fixed low-signal fallback behavior so `Idea` always names a concrete company target.
- Removed placeholder fallback target output (`Stealth AI Systems` class).
- Added deterministic default target map by portco:
  - OpenAI defaults to `Browserbase` in low-signal conditions.
- Strengthened target extraction filters to reject placeholder/generic captures:
  - blocks `stealth` class terms
  - strips leading acquire/acquihire artifacts
  - rejects generic one-token company fragments (for example `Systems`).
- Resulting behavior:
  - even with `Idea confidence: Low`, idea line still emits a specific company.

### Validation
- `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `20 passed`
- `PYTHONPATH=src python3 -m pytest -q` -> `212 passed`

## Update (2026-02-24, Board Seat Readability V3 labeled hierarchy)
- Implemented Board Seat readability V3 in `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`.
- Format is now deterministic labeled-line hierarchy (no bullet subheaders):
  - `*Thesis*` with fixed lines:
    - `*Why now:* ...`
    - `*What's different:* ...`
    - `*MOS/risks:* ...`
    - `*Bottom line:* ...`
  - `*{Company} context*` with fixed lines:
    - `*Current efforts:* ...`
    - `*Domain fit/gaps:* ...`
  - `*Funding snapshot*` with fixed lines:
    - `*History:* ...`
    - `*Latest round/backers:* ...`
- `format_version` bumped from `v2_thesis_context_funding` to `v3_labeled_hierarchy`.
- `BoardSeatDraft` moved from list-based bullet fields to fixed labeled fields:
  - `why_now`, `whats_different`, `mos_risks`, `bottom_line`
  - `context_current_efforts`, `context_domain_fit_gaps`
  - `funding_history`, `funding_latest_round_backers`
- Added per-line sanitization and validation:
  - each required line must be present and non-empty
  - each line is capped to `MAX_LINE_WORDS=18`
  - strips accidental leading bullet markers (`-`, `•`) and trims noisy suffix punctuation
- Extended parsing for repeat-guardrail compatibility:
  - supports V3 labeled lines
  - still supports V2 bullet sections and legacy 5-line posts (`Signal`, `Board lens`, `Watchlist`)
  - repeat signature continues to focus on thesis/context core signal
- Funding resolver/cache behavior kept unchanged (manual seed -> fresh cache -> web refresh -> unknown fallback).
- Unknown funding now renders explicitly in both funding lines:
  - `History: Funding details are currently unavailable.`
  - `Latest round/backers: Funding details are currently unavailable.`
- Runtime bugfix retained: Slack `SlackResponse` parsing in channel/history fetch paths.

### Validation (this session)
- Targeted board-seat tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py`
  - Result: `12 passed`
- Full suite smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` unrelated/pre-existing failures in Spencer change label defaults:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`
- Dry-run check for hierarchy output:
  - `COATUE_CLAW_BOARD_SEAT_PORTCOS='OpenAI:openai' ... run-once --force --dry-run`
  - Confirmed labeled-line hierarchy and spacing in preview output.
- Live Anduril run check:
  - `COATUE_CLAW_BOARD_SEAT_PORTCOS='Anduril:anduril' ... run-once --force`
  - Expected skip preserved: `repeat_investment_without_significant_change`.

## Update (2026-02-24, live Slack conversational path mismatch)
- Root cause for format mismatch in ad-hoc thread replies:
  - those replies were generated by the OpenClaw conversational agent path (gateway workspace prompt), not by `board_seat_daily` renderer.
  - evidence: live bot messages in `#anduril` included freeform sections (`New idea`, bullet subheaders) while `board_seat_daily` V3 renderer emits labeled lines only.
- Runtime fix applied in OpenClaw workspace prompt:
  - updated `/Users/spclaw/.openclaw/workspace/AGENTS.md` with a mandatory “Portco Idea Format Rule” enforcing labeled-line hierarchy for portco idea requests.
  - template now requires:
    - `*Thesis*` -> `*Why now:*`, `*What's different:*`, `*MOS/risks:*`, `*Bottom line:*`
    - `*{Company} context*` -> `*Current efforts:*`, `*Domain fit/gaps:*`
    - `*Funding snapshot*` -> `*History:*`, `*Latest round/backers:*`
  - explicitly disallows bulletized labels (`• Why now`, etc.).
- Gateway restarted after prompt update:
  - `make -C /opt/coatue-claw openclaw-restart`
  - health re-verified with `openclaw-status` + `openclaw-slack-status` (`probe.ok=true`).

### Important operational note
- This specific fix lives in OpenClaw runtime workspace prompt (`~/.openclaw/workspace/AGENTS.md`) and is not repo-tracked by default.
- For portability, keep this section in handoff so future sessions can re-apply after machine/profile resets.

## Update (2026-02-24, Board Seat V2 format + funding snapshot)
- Implemented Board Seat V2 in `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`:
  - deterministic sectioned output for all portcos:
    - `*Board Seat as a Service — {Company}*`
    - `*Thesis*` (max 2 bullets)
    - `*{Company} context*` (max 2 bullets)
    - `*Funding snapshot*` (max 2 bullets)
  - hard formatting guardrails:
    - max 6 bullets total
    - max 20 words per bullet
    - legacy labels (`Signal`, `Board lens`, `Watchlist`, `Team ask`) removed from generation/fallback paths
- Added structured generation and rendering:
  - new dataclasses: `FundingSnapshot`, `BoardSeatDraft`
  - model path now generates JSON draft bullets, then deterministic sanitizer/renderer enforces final shape
- Added funding data pipeline with additive DB cache table:
  - new SQLite table: `board_seat_funding_cache`
  - resolver order:
    1) manual seed JSON (`COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH`)
    2) fresh cache (`COATUE_CLAW_BOARD_SEAT_FUNDING_TTL_DAYS`, default 14)
    3) Brave web refresh + extraction
    4) explicit unknown snapshot fallback
  - source tracking emits `manual_seed` / `cache` / `web_refresh` / `unknown`
- Preserved and extended repeat guardrail behavior:
  - repeat detection still blocks stale repeats (`repeat_investment_without_significant_change`)
  - investment parsing now supports V2 sections; core repeat signal remains thesis/context-focused
- Added run/status diagnostics:
  - `run_once(...).sent[*]` now includes:
    - `format_version`
    - `funding_source_type`
    - `funding_as_of_utc`
    - `funding_unknown`
  - `status()` now includes:
    - `format_version`
    - `funding_cache_age_days_by_company`
    - `funding_data_source_by_company`

### Validation (this session)
- Targeted:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py`
  - Result: `13 passed`
- Full smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` failures outside board-seat module (pre-existing Spencer label defaults):
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

### Next Steps
1. Run live `board seat run-once --force` and verify Slack output in `#anduril` matches V2 structure and skim budget.
2. Add/seed `COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH` JSON for top portcos to improve funding quality and reduce web variance.
3. Separately fix Spencer change-tracker default user mapping to restore full-suite green.

## Update (2026-02-24, runtime follow-up)
- Runtime rollout executed:
  - `make openclaw-restart`
  - `make openclaw-slack-status` (probe `ok`, Slack account healthy)
  - `board_seat_daily status` confirms `format_version: v2_thesis_context_funding`
- Fixed a live-runtime bug in `board_seat_daily` Slack pagination/parsing:
  - `SlackResponse` objects were being treated as plain `dict` in channel/history fetch paths.
  - channel resolution now works with `channel_ref=anduril` and resolves `channel_id` correctly.
- Live Anduril run currently skips posting due to preserved repeat guardrail:
  - reason: `repeat_investment_without_significant_change`
  - this is expected behavior with unchanged recent context.

## Update (2026-02-23, title/takeaway role + sentence integrity)
- Fixed title/takeaway inversion class in X chart posts:
  - title is now enforced as the concise sentence
  - takeaway is enforced as the fuller contextual sentence
  - deterministic role correction added (`title_takeaway_role_swapped`)
- Removed post/render-time takeaway clipping that was stripping punctuation and causing fragments.
- Added single-sentence takeaway guardrails:
  - new validator check for one complete sentence (`takeaway_single_sentence`)
  - malformed comparative fragments (`to lowest`/`to highest`) are rejected and rewritten
- Added renderer takeaway fit behavior:
  - wrap up to 2 lines
  - shrink font to floor
  - one semantic shorten pass if needed
  - fail publish if still not fit
- Added diagnostics in outputs/reviews:
  - `title_takeaway_role_swapped`
  - `takeaway_single_sentence`
  - `takeaway_wrapped_line_count`
- Explicit URL flow improved:
  - if strict candidate parse filters out the post, fallback now builds a candidate directly from the fetched tweet payload before using vxtwitter fallback

### Validation (this session)
- Targeted tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py`
  - Result: `74 passed`
- Live post check:
  - `run-post-url https://x.com/KobeissiLetter/status/2026040229535047769`
  - posted successfully with `copy_rewrite_reason=title_takeaway_role_swapped` and review checks passed (`takeaway_single_sentence=true`, `title_takeaway_role_ok=true`)
- Full suite smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` pre-existing Spencer change-tracker failures (unchanged):
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

## Update (2026-02-23, headline truncation guardrails)
- Implemented headline truncation fix for X chart posts by removing hard character clipping from headline generation and shifting to layout-based fitting in render paths.
- Headline policy is now a complete sentence (terminal punctuation + action verb), with locked finance term integrity checks (for example `market cap`, `enterprise value`, `free cash flow`).
- Renderers now fit headlines with wrap + font-size adaptation (up to 3 lines) and perform one semantic rewrite pass before marking overflow unpublishable.
- Added explicit title override support for URL-post flows:
  - CLI: `run-post-url <url> --title "<full sentence>"`
  - Slack compound URL flow: supports `title: ...`
- Added/updated diagnostics and review checks in chart run outputs:
  - `copy_rewrite_applied`
  - `copy_rewrite_reason`
  - `candidate_fallback_used`
  - `headline_complete_sentence`
  - `headline_wrapped_line_count`
  - `headline_complete_phrase` retained for backward compatibility.
- Quality gates now reject incomplete headline sentences and missing locked terms before publishing.

### Validation (this session)
- Targeted tests:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py`
  - Result: `70 passed`.
- Full suite smoke:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q`
  - Result: `3` pre-existing failures in Spencer change tracker modules:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`
  - These failures are outside the X chart files touched in this patch.

### Next Steps
1. Run a live Slack post test in `#charting` with a known long headline containing locked terms (`market cap`) and confirm no truncation.
2. Run explicit URL post with `title:` override in Slack and verify override passes/fails deterministically under sentence + locked-term validation.
3. If desired, normalize Spencer change-tracker defaults in a separate patch to restore full-suite green.

## Current Status (2026-02-23)
- OpenAI model policy updated to premium defaults (no frugal mode):
  - `COATUE_CLAW_BOARD_SEAT_MODEL` default -> `gpt-5.2-chat-latest`
  - `COATUE_CLAW_X_CHART_TITLE_MODEL` default -> `gpt-5.2-chat-latest`
  - `COATUE_CLAW_X_CHART_VISION_MODEL` default -> `gpt-4.1`
  - `COATUE_CLAW_MEMORY_EMBED_MODEL` default -> `text-embedding-3-large`
  - `COATUE_CLAW_MD_MODEL` default -> `gpt-5.2-chat-latest`
  - Mac mini `.env.prod` set to these premium runtime values and OpenClaw restarted.
- MD BKNG catalyst miss fix is now shipped with Google-first evidence:
  - web retrieval is now `google_serp` primary with `ddg_html` fallback (instead of DDG-only)
  - Google payload parsing now includes `organic_results`, `news_results`, and `answer_box` snippets
  - retrieval depth increased (`COATUE_CLAW_MD_WEB_MAX_RESULTS=20` default)
  - BKNG-specific query templates + aliases added (`Booking Holdings`, `Booking.com`, `OTA`)
  - new BKNG causal clusters:
    - `ota_ai_disruption`
    - `travel_demand_outlook`
  - scoring now upweights snippet-level causal phrases and only blocks generic wrappers when they lack specific event content
  - debug output now shows:
    - top 5 evidence candidates
    - selected cluster + cluster scoring diagnostics
    - web backend used (`google_serp` vs `ddg_html`)
  - decisive defaults tuned to reduce over-fallback:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE=0.60`
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN=0.03`
  - tests:
    - `PYTHONPATH=src pytest -q tests/test_market_daily.py` => `28 passed`
    - `PYTHONPATH=src pytest -q` => `162 passed`
- MD cause wording is now less conservative by default (decisive-primary mode):
  - if one high-quality source clearly dominates the evidence cluster, MD now states the primary reason directly
  - strict generic-wrapper blocklist still applies (no `stock down today` / `news today` style lines)
  - fallback line remains only when evidence is weak/ambiguous:
    - `Likely positioning/flow; no single confirmed catalyst.`
  - new knobs:
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED` (default `1`)
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE` (default `0.60`)
    - `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN` (default `0.03`)
  - test added:
    - `test_single_strong_quality_source_can_drive_decisive_primary_reason`
- MD basket-cause coherence now handles the NET/CRWD Anthropic case:
  - if one cybersecurity mover has confirmed `anthropic_claude_cyber`, peer cybersecurity selloff movers in the same run inherit that same cause phrase
  - directional scoring now penalizes upbeat partnership headlines for down-move cause selection
  - tests added:
    - `test_cyber_basket_carries_anthropic_cause_to_net`
- ORCL/generic-cause wording fix:
  - removed generic `deal_contract` fallback phrase (`a major deal or contract update changed sentiment`)
  - deal/contract lines now use the concrete selected headline event text
  - Yahoo headline relevance filter now requires ticker/alias mention, preventing unrelated cross-company headlines in ORCL/NET lines
  - decisive-primary logic now allows strong explicit event headlines (for example lawsuits) even when the top-vs-second cluster score gap is small
  - fixed cross-mover phrase contamination bug:
    - generic clusters like `deal_contract` no longer reuse phrases across movers
    - cluster reuse is now limited to high-specific clusters (for example `anthropic_claude_cyber`)
  - expanded wrapper blocklist for weak templates like:
    - `why ... shares ... today`
    - `shares ... trading lower today`
- MD specific-cause enforcement is now shipped for selloffs (NET/CRWD Anthropic case class):
  - cause naming now requires corroboration gate: at least 2 independent sources + 2 distinct domains + at least one quality domain
  - evidence is now normalized/deduped by canonical URL + title fingerprint, including DDG absolute redirect unwrapping
  - generic wrappers are blocked from final lines (`why ... stock down today`, `news today`, ticker-only fragments)
  - added explicit cause cluster `anthropic_claude_cyber` with deterministic event phrase:
    - `Anthropic launched Claude Code Security.`
  - final reason lines now use deterministic template when corroborated:
    - negative: `Shares fell after <event>.`
    - positive: `Shares rose after <event>.`
  - when corroboration fails, fallback is now:
    - `Likely positioning/flow; no single confirmed catalyst.`
  - shared basket-event reuse is enabled across movers in the same run (for example NET + CRWD share the same Anthropic cause phrase)
  - debug payload now includes:
    - `confirmed_cluster`, `confirmed_cause_phrase`, `corroborated_sources`, `corroborated_domains`
  - tests added/updated in `tests/test_market_daily.py`:
    - generic wrapper blocking
    - Anthropic cluster extraction mapping
    - corroboration gate behavior
    - NET/CRWD shared-cluster reason reuse
    - single-source fallback behavior
  - local validation:
    - `PYTHONPATH=src pytest -q` => `155 passed`
- MD catalyst reliability fix for NET/Anthropic-class misses is now implemented in repo:
  - source coverage expanded from X+Yahoo-only to X + Yahoo + Google SERP primary + DDG fallback (when confidence is weak)
  - Yahoo parser now supports both legacy yfinance fields and nested `content.*` schema (`pubDate`, `title`, `clickThroughUrl.url`/`canonicalUrl.url`)
  - evidence windows are now session-anchored instead of fixed-hour:
    - open slot starts at previous regular-session close
    - close slot starts at same-day session open
    - lookback cap enforced by `COATUE_CLAW_MD_MAX_LOOKBACK_HOURS` (default `96`)
  - X retrieval now uses richer ticker+alias query and configurable depth (`COATUE_CLAW_MD_X_MAX_RESULTS`, default `50`)
  - evidence scoring now includes source quality, recency, mention strength, driver keywords, and directional move-aware ranking
  - fallback reason line now uses concise default:
    - `Likely positioning/flow; no single confirmed catalyst.`
  - Slack output style remains unchanged (📈/📉 + concise reason + source links), with optional `[Web]` link when web fallback is used
  - markdown artifact now includes evidence diagnostics per mover:
    - confidence, chosen source, driver keywords, top evidence considered, and reject reasons
  - new debug surfaces shipped:
    - CLI: `claw market-daily debug-catalyst <TICKER> [--slot open|close]`
    - Slack: `md debug <TICKER> [open|close]`
  - tests added/updated in `tests/test_market_daily.py`:
    - nested Yahoo schema parsing
    - Monday-open previous-close session window behavior
    - DDG fallback parsing
    - short-ticker alias relevance behavior
    - debug output shape
  - local validation:
    - `PYTHONPATH=src pytest -q tests/test_market_daily.py tests/test_launchd_runtime.py tests/test_slack_channel_access.py tests/test_slack_pipeline.py tests/test_slack_routing.py`
    - `26 passed`
  - local dry-run to `/opt/coatue-claw-data` cannot execute from laptop sandbox due path permission; runtime verification is required on Mac mini.
- Follow-on MD tuning shipped after initial reliability patch:
  - fallback now triggers on weak directional signal and low source diversity (not only low score)
  - catalyst selection now applies move-direction-aware ranking (down movers prefer down-cause evidence)
  - reason-line fallback now respects the chosen evidence source instead of blindly preferring Yahoo headline order
  - added regression tests for direction-aware fallback and negative-move evidence selection
  - full suite validation after tuning: `PYTHONPATH=src pytest -q` => `149 passed`

## Current Status (2026-02-20)
- MD Slack output style tightened for readability and user-facing copy:
  - mover lines now include only directional emoji:
    - up movers: `📈`
    - down movers: `📉`
  - removed universe line from Slack post body
  - replaced with slot-aware opener:
    - `3 biggest movers this morning:`
    - `3 biggest movers this afternoon:`
  - catalyst line sanitization now strips hashtags, cashtags, handles, URLs, and non-MD emoji
  - catalyst generation now enforces causal wording (`after/on/as/amid`) so each line explains why the stock moved
  - retained source links only (`[X]` / `[News]`) per mover
  - added relevance filtering for X evidence on ambiguous tickers:
    - short symbols (3 chars or less) now require cashtag usage (for example `$NET`)
    - finance-keyword gate blocks non-market chatter
    - noise-term guard filters sports/context collisions (for example `run rate`, `match`)
  - catalyst coherence guard added:
    - truncates to a single readable sentence
    - for vague X-only snippets, falls back to a clean company-specific-driver fallback line
  - tests updated:
    - `tests/test_market_daily.py::test_build_message_format`
    - `tests/test_market_daily.py::test_catalyst_sanitization_removes_tags_urls_and_extra_emoji`
    - `tests/test_market_daily.py::test_relevant_ticker_post_filters_ambiguous_short_tickers`

- MD (Market Daily) 2x/day feature is now implemented in repo (`main`) with Slack/CLI/runtime wiring:
  - new module: `src/coatue_claw/market_daily.py`
  - SQLite store: `/opt/coatue-claw-data/db/market_daily.sqlite`
    - tables: `md_runs`, `md_universe_snapshots`, `md_coatue_holdings`, `md_overrides`, `md_cusip_ticker_cache`
  - universe flow:
    - seed file: `config/md_tmt_seed_universe.csv`
    - top-40 ranking by yfinance market cap
    - Coatue 13F overlay + manual include/exclude overrides
  - move ranking:
    - `% move = (last - prev_close) / prev_close`
    - top movers by abs % move with market-cap tie-breaker
  - catalyst flow:
    - X recent search + Yahoo news evidence
    - per-mover concise one-line catalyst with fallback when no clear signal
  - Slack command surface added:
    - `md now`, `md now force`
    - `md status`
    - `md holdings refresh`
    - `md holdings show`
    - `md include <TICKER>`
    - `md exclude <TICKER>`
  - CLI command surface added:
    - `claw market-daily run-once [--manual] [--force] [--dry-run] [--channel ...]`
    - `claw market-daily status`
    - `claw market-daily holdings`
    - `claw market-daily refresh-coatue-holdings`
    - `claw market-daily include <TICKER>` / `exclude <TICKER>`
  - launchd service added:
    - label: `com.coatueclaw.market-daily`
    - schedule: weekdays at `07:00` and `14:15` local time
    - program: `python -m coatue_claw.market_daily run-once`
  - Make targets added:
    - `openclaw-market-daily-status`
    - `openclaw-market-daily-run-once`
    - `openclaw-market-daily-refresh-holdings`
  - docs updated:
    - `docs/openclaw-runtime.md` includes MD runtime + env contract
  - tests added:
    - `tests/test_market_daily.py`
    - `tests/test_launchd_runtime.py` updated for market-daily service/schedule
  - validation:
    - `PYTHONPATH=src pytest -q` => `140 passed`
- Immediate runtime verification required on Mac mini:
  - `make openclaw-market-daily-status`
  - `make openclaw-market-daily-run-once DRY_RUN=1`
  - `make openclaw-24x7-enable` (loads new market-daily launchd plist)
  - `make openclaw-24x7-status` (confirm `com.coatueclaw.market-daily` loaded)

- Change tracker now captures both Spencer + Carson requests with requester attribution:
  - default tracked users include `Spencer Peterson` + `Carson Wang`
  - list output now labels each item with requester name
  - daily digest now labels each open request with requester name
  - tracker commands accept:
    - `spencer changes`
    - `change requests`
    - `tracked changes`
  - optional override via env:
    - `COATUE_CLAW_CHANGE_TRACKER_USERS` (format `user_id:label,...`)
- Board Seat-as-a-Service daily scheduler shipped for portco channels:
  - new runtime module: `src/coatue_claw/board_seat_daily.py`
  - daily post per company/channel with duplicate guard (one post per company per local day)
  - default portco channel map:
    - `anduril`, `anthropic`, `cursor`, `neuralink`, `openai`, `physical-intelligence`, `ramp`, `spacex`, `stripe`, `sunday-robotics`
  - post format follows the Anduril board-seat frame and is tailored by company:
    - `Signal`, `Board lens`, `Watchlist`, `Team ask`
  - channel context source: recent Slack history in each channel (default 24h lookback); fallback template when no high-signal context exists
  - Slack scope fallback: if `conversations:read` is unavailable (`missing_scope`), service posts by channel name directly and skips history-context enrichment
  - launchd service added: `com.coatueclaw.board-seat-daily` (`COATUE_CLAW_BOARD_SEAT_TIME`, default `08:30`)
  - Make targets added:
    - `openclaw-board-seat-status`
    - `openclaw-board-seat-run-once`
  - tests added:
    - `tests/test_board_seat_daily.py`
    - `tests/test_launchd_runtime.py` updated for new service
- X chart pipeline now runs as hourly scout + windowed post:
  - every run ingests candidates into a persistent `observed_candidates` pool in SQLite
  - scheduled posting windows still use `COATUE_CLAW_X_CHART_WINDOWS` (`09:00,12:00,18:00` default)
  - at post time, winner is ranked from pooled candidates observed since the previous scheduled post (not just the current fetch)
  - non-window runs now return `reason: scouted_pool_updated` after refreshing the pool
  - pool retention/pruning controls:
    - `COATUE_CLAW_X_CHART_POOL_KEEP_DAYS` (default `10`)
    - `COATUE_CLAW_X_CHART_POOL_LIMIT` (default `600`)
- Naming convention updated from “Chart of the Day” to slot naming:
  - `Coatue Chart of the Morning`
  - `Coatue Chart of the Afternoon`
  - `Coatue Chart of the Evening`
  - naming is applied to Slack initial comment + uploaded file title
- launchd scheduler updated for hourly scout cadence:
  - `com.coatueclaw.x-chart-daily` now runs every `3600s` by default (`COATUE_CLAW_X_CHART_SCOUT_INTERVAL_SECONDS`)
  - posting still occurs only when the hourly run lands in an allowed window
- tests added/updated:
  - `test_run_chart_scout_outside_window_updates_pool`
  - `test_run_chart_scout_window_uses_hourly_pool_since_last_slot`
  - `test_convention_name_uses_morning_afternoon_evening_windows`
  - `tests/test_launchd_runtime.py` expectations updated for hourly scheduler

## Previous Status Snapshot (2026-02-19)
- X chart source variety control shipped:
  - winner selection still starts from highest scores, but now applies source diversity in a near-top-score pool
  - default behavior:
    - lookback: last `6` posted charts
    - diversity pool: candidates within `90%` of top score
    - pick least-recently-used source from that pool; otherwise keep top score winner
  - env controls:
    - `COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK` (default `6`)
    - `COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR` (default `0.90`)
  - tests added:
    - `test_pick_winner_prefers_variety_within_score_floor`
    - `test_pick_winner_keeps_top_when_alternative_too_low`
- X chart rendering reverted to source-snip mode (latest):
  - Slack chart posts now use source-snip-card mode:
    - raw X chart image is embedded as-is inside a Coatue header/footer card
    - no numeric reconstruction or chart redraw
  - URL-triggered chart runs (`run-post-url`) no longer fail on numeric reconstruction checks
  - if no source image is available, bot returns explicit Slack error
  - retained title quality instructions in Slack output:
    - `Title` (narrative headline)
    - `Chart label` (what the chart shows)
    - `Key takeaway` (concise readout)
  - source-snip-card title readability guard added:
    - headline/subheading now auto-fit to card width
    - if text is too long, renderer force-shortens to concise one-line copy (no clipping)
  - style-copy sanitizer added for low-signal phrasing:
    - rewrites generic/opening phrases (for example “It’s official…”) into concise trend language
    - applies keyword override for tariff/customs charts
    - trims trailing stopwords to avoid broken headline endings
  - added chart-image hint fallback for low-signal tweet copy:
    - reads chart title cue from image (vision) to synthesize concise headline
    - prevents broken fragments like “It’s official: In ...”
  - takeaway rewrite guard now runs independently of headline quality
    - prevents generic/clipped takeaway copy even when headline is already concise
  - Slack post summary now uses the sanitized style takeaway (same as chart card), not raw excerpt fallback
  - `x chart status` now shows `render_mode: source-snip` for runtime verification
  - updated tests cover source-snip posting path and no-rebuild requirement
- Spencer change-review + daily digest shipped:
  - Spencer requests are auto-captured from Slack (tracked user IDs: `spcoatue` + `spencermpeter` by default)
  - persisted DB: `/opt/coatue-claw-data/db/spencer_changes.sqlite`
  - statuses: `captured`, `handled`, `implemented`, `blocked`, `needs_followup`
  - review commands in Slack:
    - `spencer changes`
    - `spencer changes open`
    - `spencer changes last 50`
  - daily DM digest runtime added:
    - service label: `com.coatueclaw.spencer-change-digest`
    - schedule env: `COATUE_CLAW_SPENCER_CHANGE_DIGEST_TIME` (default `18:00`)
    - recipients env: `COATUE_CLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS`
  - Make targets added:
    - `openclaw-spencer-digest-status`
    - `openclaw-spencer-digest-run-once`
  - tests added:
    - `tests/test_spencer_change_log.py`
    - `tests/test_spencer_change_digest.py`
  - Mac mini runtime validation:
    - pulled latest (`5b26ea9`), restarted OpenClaw, and enabled scheduler service
    - `make openclaw-24x7-status` confirms `com.coatueclaw.spencer-change-digest` is loaded
    - forced send test succeeded:
      - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.spencer_change_digest run-once --force`
      - Slack post delivered to user channel `U0AGD28QSQG` (`ts=1771557014.620259`)
  - reliability hardening:
    - digest sender now retries across Slack token sources (env + `~/.openclaw/openclaw.json`)
    - if `conversations.open` lacks scope, it falls back to App Home DM posting (`channel=<user_id>`)
- Spencer-request governance log shipped:
  - bot now auto-captures change requests from Spencer accounts (`spcoatue`/`spencermpeter` user IDs) when messages look like bot-change asks
  - each request is persisted in SQLite (`/opt/coatue-claw-data/db/spencer_changes.sqlite`) with status lifecycle:
    - `captured`, `handled`, `implemented`, `blocked`, `needs_followup`
  - request status is auto-updated inline as bot workflows execute (settings/pipeline/chart/x digest/universe/diligence paths)
  - new Slack review commands:
    - `spencer changes`
    - `spencer changes open`
    - `spencer changes last 50`
  - env override for tracked Spencer IDs: `COATUE_CLAW_SPENCER_USER_IDS`
  - tests added: `tests/test_spencer_change_log.py`
- Slack channel access hardening shipped for new channels:
  - bot now auto-joins newly created public channels via `channel_created` handler + `conversations.join`
  - bot also performs startup bootstrap over existing public channels and joins channels where it is not yet a member
  - env flag: `COATUE_CLAW_SLACK_AUTOJOIN_PUBLIC_CHANNELS` (default `1`, set `0` to disable)
  - private channels still require manual invite (Slack platform limitation)
  - tests added: `tests/test_slack_channel_access.py`
- X Chart destination channel updated to `#charting` for scheduled/manual chart posts.
  - Mac mini runtime env set: `COATUE_CLAW_X_CHART_SLACK_CHANNEL=C0AFXM2MWAV` (`#charting`)
- X Chart post-publish checklist loop shipped:
  - each Slack-posted chart is now reviewed against a persisted checklist (`post_reviews` in `x_chart_daily.sqlite`)
  - checklist coverage includes copy limits, US relevance, axis labels, grouped-series validity, and artifact-size integrity
  - learning feedback now downranks sources when checklist failures occur
  - Mac mini validation: `make openclaw-x-chart-run-once` posted to `C0AFXM2MWAV` and recorded review pass (`review_summary.pass_count=1`)
- Drive/share taxonomy simplification prepared for deployment:
  - Drive root path changed to `/Users/spclaw/Documents/SPClaw Database`
  - category taxonomy simplified to `Universes`, `Companies`, `Industries`
  - ingest classifier now maps legacy category words (for example `filings`, `themes`, `macro`) into the simplified three-folder set
- Grouped-bar QA + rendering correctness update shipped (`main`):
  - employee/robot tweet charts now require two reconstructed bar series before posting
  - rejected cases:
    - single-series grouped output
    - normalized/index-only grouped output
    - non-year / placeholder x-axis labels for grouped employee/robot charts
  - grouped bar semantics normalized before render:
    - labels: `Employees` + `Robots`
    - colors: navy + purple
    - y-axis unit fallback: `Number (thousands)`
  - fixed immutable dataclass handling in grouped-series normalization (no in-place mutation)
  - added CV fallback for employee/robot charts when vision extraction is unavailable:
    - detects dark/purple bar pairs
    - scales to unit values using latest employee/robot figures from post text
  - style-copy QA enforced pre-render (headline/chart label/takeaway constraints)
  - non-normalized bar charts now always render y-axis numeric tick labels
  - added hard fail if reconstructed bar chart has missing y-axis tick labels
  - employee/robot takeaway copy now emits a complete short sentence (avoids clipped line endings)
  - validation: `PYTHONPATH=src pytest -q` => `105 passed`
- OpenClaw gateway handoff hardening shipped (`main`):
  - added deterministic CLI entrypoint for tweet URL chart requests:
    - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-post-url <x-url> [--channel <id>]`
  - this allows gateway-side natural-language Slack handling to call the strict rebuild-only chart path instead of ad hoc screenshot replies
  - runtime note: Mac mini workspace guidance at `~/.openclaw/workspace/AGENTS.md` now explicitly instructs the gateway agent to use `run-post-url` for tweet URL chart requests and avoid relative media paths
  - test added: `test_cli_run_post_url_command`
  - validation: `PYTHONPATH=src pytest -q` => `100 passed`
- X Chart readability + reconstruction fidelity update shipped (`main`):
  - takeaway line is now much shorter by default (feed-safe and chart-safe) to prevent clipping in Slack/file preview
  - renderer screenshot fallback is now hard-disabled (independent of env flag)
  - bar reconstruction now supports grouped/two-series bar structures (for example employees vs robots) and renders native Coatue-style bars with legend
  - vision extractor schema now accepts multi-series bar outputs (`series`) in addition to single-series (`values`)
  - reconstruction gate logic now mirrors rendering mode:
    - bar-mode requires bar reconstruction
    - line-mode requires line reconstruction
  - regression test added to guarantee no screenshot fallback path
  - validation: `PYTHONPATH=src pytest -q` => `99 passed`
- X Chart semantic-title + true-rebuild enforcement shipped (`main`):
  - style copy generation improved for title/subheading fidelity:
    - optional LLM title synthesis (`gpt-4o-mini`) now generates:
      - narrative headline (big title)
      - technical chart label (small subheading)
      - concise takeaway
    - fallback heuristic still applies if LLM synthesis is unavailable
  - URL-specific chart requests now require numeric reconstruction before posting:
    - `run_chart_for_post_url(...)` now pre-validates extracted bars/series
    - if reconstruction fails, bot returns a clear failure instead of posting a screenshot-style fallback
  - added fallback candidate source for URL requests when X API misses media:
    - `api.vxtwitter.com` fallback fetch for text/media on specific post URLs
  - renderer now attempts vision bar extraction first (before mode heuristics), improving reconstruction rate on tweet charts that lack explicit bar keywords
  - title heuristics now include explicit employees-vs-robots narrative handling (for posts like `$AMZN ... employees ... robots`) to avoid raw/fragmented headline/subheading text
  - global post gate added (`COATUE_CLAW_X_CHART_REQUIRE_REBUILD`, default on):
    - if numeric reconstruction is not reliable, Slack posting is skipped with explicit error (no screenshot fallback output)
  - validation: `PYTHONPATH=src pytest -q` => `98 passed`
- Slack natural-language compound X-post handling shipped (`main`):
  - single message with X post URL can now do both:
    - add poster handle to X scout source list
    - generate/post Coatue-style chart from that exact post URL
  - this closes the gap where non-command phrasing fell through and triggered generic executor/browser-relay behavior
  - examples now supported:
    - `Please make a chart of the day from this post <x-url> and add this guy to our twitter list`
    - `Output a coatue style chart from this post <x-url>`
  - implemented in:
    - `src/coatue_claw/slack_x_chart_intent.py`
    - `src/coatue_claw/slack_bot.py` (compound handler wired before strict `x chart ...` parser)
    - `src/coatue_claw/x_chart_daily.py` (`run_chart_for_post_url`)
  - validation: `PYTHONPATH=src pytest -q` => `95 passed`
- X Chart readability pass shipped (`main`):
  - removed generated timestamp line from top-left chart header (less clutter)
  - bar reconstructions now always render x-axis labels (no blank x-axis)
  - added fallback label strategy:
    - use parsed years when available
    - otherwise infer year range ending at post year
    - finally fall back to `P1..Pn`
  - added pre-save readability guardrail: if reconstructed bar chart has insufficient x-axis labels, renderer falls back to source image instead of posting an unreadable rebuild
  - vision extractor now uses inline image bytes (data URL) to improve extraction reliability vs remote URL fetch edge cases
  - validation: `PYTHONPATH=src pytest -q` => `91 passed`
- X Chart-of-the-Day title and bar reconstruction pass shipped (`main`):
  - title framing now explicitly follows Coatue pattern:
    - big narrative headline (theme/takeaway)
    - small chart label (what the graph shows)
  - bar reconstruction no longer emits placeholder-style `G1..G10` labels
  - stricter reconstruction gates now reject low-quality bar parses instead of rendering misleading synthetic bars
  - optional OpenAI vision extraction added for bar charts (`OPENAI_API_KEY` + `COATUE_CLAW_X_CHART_VISION_ENABLED=1`) to recover real bar labels/values when possible
  - renderer now supports non-normalized bar values (including negatives) and dynamic y-axis labels when vision extraction returns concrete units
  - test suite updated and green (`PYTHONPATH=src pytest -q` => `90 passed`)
- Repo is synced on `main` and used as the cross-device source of truth.
- Slack channel/user policy is open in OpenClaw (`groupPolicy=open`, `dmPolicy=open`, `allowFrom=["*"]`).
- Natural-language chart intent parsing is implemented:
  - detects plot/chart/graph requests
  - defaults y-axis to YoY revenue growth unless user specifies otherwise
  - supports configurable axis metrics (EV/LTM multiple, YoY growth, LTM revenue, market cap, enterprise value, debt, cash, latest quarter revenue)
- Chart footer branding text (`COATUE CLAW`) has been removed; only footnote/citation text remains.
- CSV-backed universe workflow is implemented:
  - storage path: `/opt/coatue-claw-data/db/universes/*.csv`
  - Slack natural commands: create/list/show/add/remove universes
  - chart requests with missing or underspecified tickers now prompt for source:
    - `online` discovery
    - saved `universe` CSV
  - post-chart feedback loop asks for include/exclude tickers and can rerun chart in-thread
- Chart pre-output follow-ups are now only asked when strictly necessary:
  - bot first tries auto universe match and online discovery
  - if a valid ticker set is found, it renders immediately with no extra question
  - if not, it asks for `online` vs `use universe NAME`
- Chart titles now infer context from user prompt/source (example: `Defense Stocks`) and use that as headline.
- Footer citation/footnote text now sits at the left corner since logo text was removed.
- Coatue-style median dotted line + callout has been restored for chart outputs after configurable-axis refactor.
- Category guide/key is now auto-placed inside plot whitespace (dotted `Category Guide` box) using a density-aware heuristic.
- Default guide placement behavior now optimizes for low point density and distance from datapoints while avoiding `R^2` and median-callout zones (and de-prioritizing trendline overlap).
- Post-chart Slack follow-up prompt is now sent via resilient thread posting (`chat_postMessage` with retry on rate limits, fallback to `say`) so the adjustments question is consistently delivered.
- OpenClaw charting skill contract now explicitly requires a final post-chart follow-up question (stock screen/data/formatting adjustments) after successful chart output.
- Added a dedicated laptop/Codex/OpenClaw operations runbook at `docs/laptop-codex-openclaw-workflow.md` and mirrored key guardrails into `AGENTS.md` (canonical repo path, ship loop, restart/verify loop).
- Expanded runtime spec in `docs/openclaw-runtime.md` with execution model, job classes, artifact contract, and incident triage runbook.
- Added explicit OpenClaw operator targets in `Makefile` for `openclaw-dev`, `openclaw-bot-status`, `openclaw-bot-logs`, and `openclaw-schedulers-status`.
- Hardened `Makefile` OpenClaw targets for non-login SSH shells:
  - prepends `/opt/homebrew/bin` to PATH (so `node` + `openclaw` resolve)
  - auto-resolves OpenClaw binary path (`openclaw` on PATH or `/opt/homebrew/bin/openclaw`)
- Added plain-English Slack settings workflow:
  - `show my settings` / `how are you configured`
  - conversational setting updates (peer count target, default x/y axes, post-chart follow-up wording)
  - `promote current settings` to auto-commit/push runtime defaults to `main`
  - `undo last promotion` to auto-`git revert` the last settings promotion commit
- Added runtime settings persistence and audit modules:
  - `src/coatue_claw/runtime_settings.py`
  - `src/coatue_claw/slack_config_intent.py`
  - defaults file tracked in git: `config/runtime-defaults.json`
- Added Slack deploy pipeline workflow (admin-gated, single-job lock):
  - `deploy latest` -> pull + restart + Slack probe status
  - `undo last deploy` -> revert last deploy target + push + restart + probe
  - `run checks` -> `PYTHONPATH=src pytest -q`
  - `show pipeline status` / `show deploy history`
  - `build: <request>` -> runs Codex CLI on runtime host by default (or custom command via env)
- Added deploy pipeline modules:
  - `src/coatue_claw/slack_pipeline.py`
  - `src/coatue_claw/slack_pipeline_intent.py`
  - deploy history file: `/opt/coatue-claw-data/db/deploy-history.json`
- Replaced diligence template output with deep neutral memo generation:
  - new module: `src/coatue_claw/diligence_report.py`
  - `claw diligence TICKER` now outputs the 8-section neutral investment memo format with evidence-based citations
  - data sources in memo: company profile, statements, valuation/balance sheet metrics, and recent reporting metadata (via Yahoo Finance/yfinance)
  - Slack mention parsing now accepts both `diligence` and common typo `dilligence`
  - diligence now performs a database-first local report check before online fetch:
    - local sources: `/opt/coatue-claw-data/db/file_ingest.sqlite` + `/opt/coatue-claw-data/artifacts/packets/*.md`
    - memo now includes matched local report references and local-check timestamp in sources
- Added hybrid memory subsystem (structured-first + semantic fallback):
  - SQLite + FTS5 memory DB: `/opt/coatue-claw-data/db/memory.sqlite`
  - auto fact extraction from Slack mentions (`profile`, `relationship`, `decision`, `convention`)
  - decay tiers with TTL refresh-on-access (`permanent`, `stable`, `active`, `session`, `checkpoint`)
  - memory CLI commands: `claw memory status|query|prune|extract-daily|checkpoint`
  - Slack memory interactions:
    - `remember ...`
    - `memory status`
    - `memory prune`
    - `memory extract daily [days N]`
    - natural lookup (`what is my ...`, `when is my ...`, `do you remember ...`)
  - pre-flight pipeline checkpoints now auto-write before `deploy_latest`, `undo_last_deploy`, and `build_request`
  - optional semantic retrieval path via LanceDB/OpenAI embeddings when configured
- Added file management bridge (local-first + shared mirror):
  - module: `src/coatue_claw/file_bridge.py`
  - config: `config/file-bridge.json`
  - runbook: `docs/file-management-system.md`
  - Make targets:
    - `openclaw-files-init`
    - `openclaw-files-status`
    - `openclaw-files-sync-pull`
    - `openclaw-files-sync-push`
    - `openclaw-files-sync`
    - `openclaw-files-index`
  - published index artifacts generated to `published/index.json` and `published/index.md`
  - Drive mirror root is now set to `/Users/spclaw/Documents/SPClaw Database` for Mac mini sync
  - Spencer-facing category subfolders are provisioned under `01_DROP_HERE_Incoming`, `02_READ_ONLY_Latest_AUTO`, and `03_READ_ONLY_Archive_AUTO` (`Universes`, `Companies`, `Industries`)
  - `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` auto-mirrors Latest for visibility and is excluded from pull ingestion
  - Slack file uploads are now auto-ingested (download + category routing + SQLite audit + Drive mirror):
    - module: `src/coatue_claw/slack_file_ingest.py`
    - DB: `/opt/coatue-claw-data/db/file_ingest.sqlite`
    - wired in `src/coatue_claw/slack_bot.py` (`message`, `file_shared`, and `app_mention` with file attachments)
- Added email interaction channel (optional poller):
  - module: `src/coatue_claw/email_gateway.py`
  - runbook: `docs/email-integration.md`
  - Make targets:
    - `openclaw-email-status`
    - `openclaw-email-run-once`
    - `openclaw-email-serve`
  - command support via email subject/body:
    - `diligence TICKER` / `dilligence TICKER`
    - `memory status`
    - `memory query <phrase>`
    - `files status`
    - `help`
  - email attachments auto-ingest into local + Drive category folders with audit DB:
    - `/opt/coatue-claw-data/db/email_gateway.sqlite`
  - diligence parsing now uses context-aware ticker extraction (body-first, stopword filtering) so phrases like `Testing Dilligence` + `Diligence SNOW please` resolve to ticker `SNOW` instead of `DILIGENCE`
- Added 24/7 runtime supervision for email + memory pruning:
  - module: `src/coatue_claw/launchd_runtime.py`
  - launchd services:
    - `com.coatueclaw.email-gateway` (always-on `email_gateway serve` with KeepAlive)
    - `com.coatueclaw.memory-prune` (hourly `claw memory prune` via StartInterval)
  - launchctl domain fallback support for remote SSH operations (`gui/<uid>` then `user/<uid>`)
  - Make targets:
    - `openclaw-24x7-enable`
    - `openclaw-24x7-status`
    - `openclaw-24x7-disable`
  - `openclaw-schedulers-status` now reports actual launchd service status instead of placeholder text
- Chart outputs remain PNG + CSV + JSON + raw provider payload.
- Session shipping protocol is codified in `AGENTS.md` and templated in `docs/handoffs/ship-template.md`.

## What Was Implemented
- Added valuation chart engine in `/opt/coatue-claw/src/coatue_claw/valuation_chart.py`.
- Added CLI command in `/opt/coatue-claw/src/coatue_claw/cli.py`:
  - `claw valuation-chart SNOW,MDB,DDOG`
- Added Slack command handling in `/opt/coatue-claw/src/coatue_claw/slack_bot.py`:
  - mention pattern for graph/chart + EV/LTM/growth tickers
  - uploads PNG + CSV + JSON + raw provider JSON artifacts
- Added unit tests in `/opt/coatue-claw/tests/test_valuation_chart.py`.
- Added workspace skill for OpenClaw runtime:
  - `/Users/spclaw/.openclaw/workspace/skills/valuation-charting/SKILL.md`

## Current Data Behavior
- Default metric orientation is **EV/LTM revenue on x-axis** and **YoY revenue growth on y-axis**.
- User can override x/y metrics in natural language (`x axis ...`, `y axis ...`, or `A vs B` phrasing).
- LTM revenue is **sum of last 4 reported quarters**.
- Provider preference is `google` then `yahoo`.
- In this build, Google adapter is unavailable for required EV + LTM inputs, so run falls back to Yahoo.
- Quality gates include:
  - `missing_ltm_revenue`
  - `missing_debt`
  - `currency_mismatch`
  - `stale_market_data`
  - `stale_fundamentals`

## Artifacts
- Charts/data write to `/opt/coatue-claw-data/artifacts/charts/`:
  - `valuation-scatter-*.png`
  - `valuation-scatter-*.csv`
  - `valuation-scatter-*.json`
  - `valuation-scatter-*-raw.json`

## Validation Completed
- `pytest`: `20 passed` (valuation chart + chart-intent parser tests).
- CLI smoke run (latest):
  - provider used: `yahoo`
  - included/excluded counts returned
  - PNG/CSV/JSON/raw generated
- OpenClaw skill recognized:
  - `openclaw skills info valuation-charting` => ready, source `openclaw-workspace`
- Repo-session validation (this session):
  - `PYTHONPATH=src pytest -q` => `58 passed`
  - `PYTHONPATH=src pytest -q tests/test_launchd_runtime.py tests/test_email_gateway.py` => `6 passed`
  - `make openclaw-restart` failed locally with `openclaw: No such file or directory`; runtime restart/status validation must be executed on Mac mini runtime host.
  - Mac mini runtime validation succeeded after pull (`d5099bb`): gateway running, Slack probe `ok=true`; root cause of earlier SSH failure was minimal PATH in non-login shell.
  - Makefile PATH hardening validated on Mac mini after pull (`0862aa0`): non-login SSH `make openclaw-restart` and `make openclaw-slack-status` now execute with resolved `openclaw` + `node`.
  - Hybrid memory runtime deployed on Mac mini (`33650f2`): structured memory active; semantic fallback currently disabled until `OPENAI_API_KEY` is set in runtime env.
  - File bridge Drive root configured + validated on Mac mini (`9db4643` + latest pull): `make openclaw-files-init`, `make openclaw-files-sync`, and `make openclaw-files-status` pass using `/Users/spclaw/Documents/SPClaw Database`.
  - Recursive subfolder sync validation passed on Mac mini: file dropped into `01_DROP_HERE_Incoming/Companies` mirrored to local `incoming/Companies`, then cleaned up.
- Slack file ingest tests added/validated: classification, routing, dedupe, and SQLite write path.
- Diligence tests extended/validated for local-report precheck behavior before external research.
- Email gateway tests added/validated: command parsing, disabled-mode safety, and attachment ingest routing.
- 24/7 runtime service tests added/validated:
  - local: `PYTHONPATH=src pytest -q tests/test_launchd_runtime.py` => `4 passed`
  - Mac mini: `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py tests/test_email_gateway.py` => `7 passed`
- Mac mini runtime validation (2026-02-19):
  - pulled `a49f887` then `95fb26d` in `/opt/coatue-claw`
  - `make openclaw-24x7-enable` succeeded for:
    - `com.coatueclaw.email-gateway`
    - `com.coatueclaw.memory-prune`
  - `make openclaw-24x7-status` reports:
    - email service: `loaded=true`, `state=running`
    - memory prune service: `loaded=true`, `last_exit_code=0` (idle between scheduled runs is expected)
  - `make openclaw-slack-status` probe returns `ok=true`
  - `make openclaw-email-status` confirms email channel enabled with expected allowlist senders
- Email diligence parsing hardening validation (commit `19b2099`):
  - local tests: `PYTHONPATH=src pytest -q tests/test_email_gateway.py` => `3 passed`
  - Mac mini tests: `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_email_gateway.py` => `3 passed`
  - Mac mini direct parser check:
    - `parse_email_command('Testing Dilligence', 'Diligence SNOW please')`
    - result: `EmailCommand(kind='diligence', arg='SNOW')`
  - runtime health after deploy:
    - `make openclaw-24x7-status`: email service loaded/running; memory-prune loaded with clean `last_exit_code=0`
    - `make openclaw-slack-status`: probe `ok=true`
- Email diligence response formatting upgrade (`c862c48` + `a58c0f8`):
  - replaced raw markdown preview dump with readable executive summary body
  - added HTML email alternative for cleaner rendering in Gmail
  - attached full memo as readable `.pdf` for consumer-facing delivery
  - removed local filesystem path lines from user-facing email output
  - stripped long `[Source: ...]` tails from summary bullets for readability while preserving full citations in the attachment
  - escaped literal `$` values during PDF rendering to avoid matplotlib mathtext parse failures on financial figures
  - replaced raw full-memo PDF text dump with a professionally sectioned diligence brief layout (clean headings, wrapped bullets, readable spacing)
  - tests added for readable summary + attachment contract (`tests/test_email_gateway.py`)
  - Mac mini validation:
    - `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_email_gateway.py` => `4 passed`
    - direct runtime check confirms:
      - `has_local_path=False`
      - attachment filename ends with `.pdf`
      - attachment content type `application/pdf`
      - attachment payload starts with `%PDF`
- Professional PDF brief rendering upgrade (`fb3692c`):
  - replaced dense raw-memo PDF pages with a clean sectioned brief format:
    - Key Takeaways
    - Business Overview
    - Financial Snapshot
    - Company Strengths
    - Key Risks
    - Open Questions
  - improved typography, bullet wrapping, spacing, and page footer layout for readability
  - Mac mini validation:
    - `tests/test_email_gateway.py` => `4 passed`
    - generated diligence attachment remains valid PDF (`%PDF`) with consumer-facing summary body
    - services healthy: `make openclaw-24x7-status`, `make openclaw-slack-status`
- PDF layout style refinement (this session):
  - upgraded to an executive-style template with centered report title, metadata row, blue divider, and backdrop callout box
  - improved section hierarchy and typography to match a professional investment brief feel
  - report title now stays generic to diligence topic/company (example: `SNOW Diligence Report`) and avoids third-party naming
  - Mac mini deployment validation (`26fc61d`):
    - `tests/test_email_gateway.py` => `4 passed`
    - generated attachment metadata:
      - filename: `SNOW-...pdf`
      - content_type: `application/pdf`
      - payload begins with `%PDF`
    - user-facing summary title now reports `Title: SNOW Diligence Report`
- Slack default routing upgrade (this session):
  - added routing helper module: `src/coatue_claw/slack_routing.py`
  - plain Slack messages now run through OpenClaw request handling by default
  - explicit `@user` mentions are excluded from default routing
  - `app_mention` flow remains supported and now shares common request handler logic with default-routed messages
  - tests: `tests/test_slack_routing.py`
  - Mac mini deployment validation (`86bce9d`):
    - `/opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_slack_routing.py` => `3 passed`
    - `make openclaw-restart` + `make openclaw-slack-status` => Slack probe `ok=true`
    - `make openclaw-24x7-status` => email/memory launchd services still healthy
  - deployed on Mac mini via `0173404` and validated:
    - `tests/test_email_gateway.py` => `4 passed` on runtime host
    - direct runtime check confirms `has_source_tail=False` in diligence summary body
    - email 24/7 service restored and running (`make openclaw-24x7-status`)
- X digest feature (this session):
  - implemented official X API digest path (no browser scraping dependency):
    - new digest engine: `src/coatue_claw/x_digest.py`
    - new Slack intent parser: `src/coatue_claw/slack_x_intent.py`
    - Slack commands wired:
      - `x digest <query> [last Nh] [limit N]`
      - `x status`
    - CLI command wired:
      - `claw x-digest "QUERY" --hours 24 --limit 50`
    - artifact contract:
      - writes markdown digest to `/opt/coatue-claw-data/artifacts/x-digest` (override via `COATUE_CLAW_X_DIGEST_DIR`)
    - env contract:
      - `COATUE_CLAW_X_BEARER_TOKEN` (required; runtime only, never in git)
      - optional `COATUE_CLAW_X_API_BASE`
  - tests added:
    - `tests/test_slack_x_intent.py`
    - `tests/test_x_digest.py`
  - local validation:
    - `PYTHONPATH=src pytest -q` => `69 passed`
  - operator note:
    - token was provided in chat; rotate/regenerate it in X Developer portal and set the new token in runtime `.env.prod` only
  - Mac mini deploy + runtime verification completed:
    - pulled `main` to `/opt/coatue-claw` at commit `5dfdd03`
    - set `COATUE_CLAW_X_BEARER_TOKEN` in `/opt/coatue-claw/.env.prod`
    - restarted runtime: `make openclaw-restart`
    - verified Slack health: `make openclaw-slack-status` probe `ok=true`
    - verified live X API path using runtime env:
      - `build_x_digest("snowflake", hours=24, max_results=10)` succeeded
      - output written to `/opt/coatue-claw-data/artifacts/x-digest/snowflake-20260219-061237.md`
- Slack no-mention routing hotfix on Mac mini:
  - root cause from runtime logs:
    - `slack-auto-reply ... reason=\"no-mention\" skipping channel message`
  - config fix applied in `~/.openclaw/openclaw.json`:
    - `channels.slack.requireMention=false`
    - `channels.slack.channels.C0AFGMRFWP8.requireMention=false`
  - restarted runtime and verified Slack probe health:
    - `make openclaw-restart`
    - `make openclaw-slack-status` => probe `ok=true`
  - expected behavior after fix:
    - plain channel messages (e.g., `x status`) are routed to OpenClaw without requiring `@Coatue Claw`
- X chart scout daily winner feature (this session):
  - implemented automated chart scouting + posting engine:
    - module: `src/coatue_claw/x_chart_daily.py`
    - persists source trust/ranking + post history in SQLite:
      - `/opt/coatue-claw-data/db/x_chart_daily.sqlite`
    - writes per-slot markdown artifacts:
      - `/opt/coatue-claw-data/artifacts/x-chart-daily/*.md`
  - source strategy:
    - seeded prioritized source list with `@fiscal_AI` and additional high-signal accounts
    - auto-discovers and promotes new X sources based on engagement
    - ingests supplemental candidates from Visual Capitalist feed (`https://www.visualcapitalist.com/feed/`)
  - scheduling:
    - added launchd service `com.coatueclaw.x-chart-daily`
    - default windows: `09:00,12:00,18:00` with configurable timezone (default `America/Los_Angeles`)
  - Slack controls added:
    - `x chart now`
    - `x chart status`
    - `x chart sources`
    - `x chart add @handle priority 1.2`
  - CLI controls added:
    - `claw x-chart run-once --manual`
    - `claw x-chart status`
    - `claw x-chart list-sources`
    - `claw x-chart add-source HANDLE --priority 1.2`
  - Makefile controls added:
    - `make openclaw-x-chart-status`
    - `make openclaw-x-chart-run-once`
    - `make openclaw-x-chart-sources`
    - `make openclaw-x-chart-add-source HANDLE=fiscal_AI PRIORITY=1.6`
  - tests added/updated:
    - `tests/test_x_chart_daily.py`
    - `tests/test_launchd_runtime.py`
  - local validation:
    - `PYTHONPATH=src pytest -q` => `72 passed`
  - robustness patch:
    - X source scanning now handles invalid/renamed usernames gracefully (skips invalid handles instead of failing the entire run)
    - Slack post token resolution now falls back to `~/.openclaw/openclaw.json` (`channels.slack.botToken`) when `SLACK_BOT_TOKEN` env is missing/inactive
    - Slack posting retries with fallback token when env token is rejected (`account_inactive`, `invalid_auth`, `token_revoked`)
  - Mac mini deploy/verification:
    - pulled `main` to `/opt/coatue-claw` at commit `c3f64d0`
    - runtime env configured:
      - `COATUE_CLAW_X_CHART_SLACK_CHANNEL=#charting`
      - `COATUE_CLAW_X_CHART_TIMEZONE=America/Los_Angeles`
      - `COATUE_CLAW_X_CHART_WINDOWS=09:00,12:00,18:00`
    - launchd scheduler enabled and healthy:
      - `make openclaw-24x7-enable`
      - `make openclaw-24x7-status` shows `com.coatueclaw.x-chart-daily` loaded
    - manual proof-of-life post succeeded:
      - `make openclaw-x-chart-run-once` posted winner to `#charting`
      - sample winner: `@Barchart` post `https://x.com/Barchart/status/2024274150118859021`
      - artifact: `/opt/coatue-claw-data/artifacts/x-chart-daily/manual-20260218-224336-x.md`
  - Chart-of-the-day style formatter (this session):
    - integrated a new Coatue-style rendered image card into x-chart posting flow
    - renderer uses design cues from Coatue `C:\\Takes` + valuation-chart skill style language:
      - navy headline, metadata row, blue divider
      - light gray canvas with bordered content rails
      - executive summary bullets and backdrop context callout
      - source footer + score marker
      - Avenir/Avenir Next/Helvetica Neue fallback typography
    - Slack delivery now posts:
      - summary text message in channel
      - threaded `Coatue Chart of the Day` styled PNG upload
    - output artifact path:
      - `/opt/coatue-claw-data/artifacts/x-chart-daily/<slot>-styled.png`
  - chart-quality filter update:
    - X candidates now must pass chart-like text/data signal checks before ranking
    - reduces false positives (e.g., headline photos without chart context)
  - US scope + readability/style refinement (this session):
    - added US relevance classifier gate for both X and Visual Capitalist candidates (blocks non-US FX-only trends such as Turkish lira posts)
    - added render-safe normalization for outbound chart text (`_normalize_render_text`) to remove unsupported glyph/emoji artifacts
    - simplified chart card format to a graph-first layout (no left text panel; headline + large chart + minimal footer only)
    - added iterative post-generation style audit (`StyleDraft`) with checks for:
      - US relevance
      - explicit trend signal
      - concise headline/takeaway
      - plain language (non slide-jargon)
      - clean characters
      - graph-first copy density (short, feed-readable framing)
    - style audit metadata now persists in:
      - markdown artifact (`style_iteration`, `style_score`, checks)
      - Slack post payload (`style_audit`)
    - Slack delivery now uploads the styled graph as the initial message attachment (`files_upload_v2` with `initial_comment`) instead of posting image in a thread reply
  - chart reconstruction upgrade (this session):
    - chart pipeline now attempts to reconstruct line-series directly from source chart images (color/dark-line extraction + normalization) and re-plots the chart in Coatue style rather than screenshot framing by default
    - when reconstruction succeeds, output is graph-first with normalized axes and redrawn series; original image embed is fallback-only
    - bar-mode support added:
      - if the post references bar/histogram language, or bar-like structure is detected in source chart image, renderer outputs rebuilt bar chart instead of line chart
      - if bar mode is selected but bar reconstruction confidence is insufficient, renderer now falls back to source image (never fake line output for bar-cue posts)
    - chart headlines are auto-shortened with no `...`; long titles are rephrased to concise header text
    - title/takeaway builders and Slack summary text now avoid ellipsis output
    - two-level Coatue-style title synthesis added:
      - small chart label describes what is being measured
      - big narrative headline expresses thematic takeaway
    - headline synthesis now strips raw news prefixes (for example `BREAKING:`) and rewrites into narrative language
    - removed source-handle callout and score badge from chart image output
    - render includes pre-save overlap checks that adjust header/plot/footer spacing to avoid text/graphics collisions
  - local validation for this refinement:
    - `PYTHONPATH=src pytest -q tests/test_x_chart_daily.py tests/test_launchd_runtime.py` => `22 passed`
    - `PYTHONPATH=src pytest -q` => `88 passed`

## Next Step to Validate in Slack
Send in `#charting`:
- `@Coatue Claw plot EV/Revenue multiples and revenue growth for SNOW,MDB,DDOG,NOW,CRWD`
- `@Coatue Claw graph SNOW,MDB,DDOG with x axis market cap and y axis ltm revenue`
- `@Coatue Claw create universe defense with PLTR,LMT,RTX,NOC,GD,LDOS`
- `@Coatue Claw make me a valuation chart for defense stocks` then reply `@Coatue Claw use universe defense` or `@Coatue Claw online`
- Confirm rendered title headline is prompt-relevant (`Defense Stocks` / similar) and footnote is left-aligned.
- Confirm category guide appears inside unused plot whitespace and does not consume a dedicated right gutter.
- Confirm each successful chart post includes the in-thread adjustments follow-up prompt right after artifact upload.

Then confirm bot returns:
- as-of timestamps
- provider used + fallback reason
- chart image with line of best fit
- CSV/JSON/raw attachments

## Immediate Next Steps
1. Ensure Slack app permissions for cross-channel behavior:
   - bot scopes: `channels:read`, `channels:join`, `chat:write`, `chat:write.public`
   - event subscriptions: add `channel_created`
   - reinstall app after scope/event changes
2. Run all Slack validation prompts above in `#charting`.
3. Validate plain-English settings commands in Slack:
   - `@Coatue Claw show my settings`
   - `@Coatue Claw going forward look for 12 peers`
   - `@Coatue Claw use market cap as the default x-axis`
   - `@Coatue Claw when you finish a chart, ask us if we want ticker changes`
4. Validate `@Coatue Claw promote current settings` commits/pushes to `main` and reports commit hash in-thread.
5. Validate `@Coatue Claw undo last promotion` produces a revert commit and restarts runtime.
6. Validate Slack deploy pipeline in `#claw-lab`:
   - `@Coatue Claw deploy latest`
   - `@Coatue Claw run checks`
   - `@Coatue Claw show pipeline status`
   - `@Coatue Claw show deploy history`
7. Validate diligence memo output in Slack:
    - `@Coatue Claw diligence SNOW`
    - `@Coatue Claw dilligence MDB` (typo alias path)
    - confirm memo includes all required neutral sections plus source/timestamp attribution
    - confirm memo includes local database precheck summary and local report references when available
8. Validate memory flows in Slack:
   - `@Coatue Claw remember my daughter's birthday is June 3rd`
   - `@Coatue Claw what is my daughter's birthday?`
   - `@Coatue Claw memory status`
   - `@Coatue Claw memory checkpoint`
9. Configure `SLACK_PIPELINE_ADMINS` and optional `COATUE_CLAW_SLACK_BUILD_COMMAND` in runtime env for production permissions/runner control.
9. Validate 24/7 persistence after next Mac mini reboot:
   - run `make openclaw-24x7-status`
   - confirm both services auto-restart as `loaded=true` and `state=running`
10. Confirm Spencer has Drive access to `/Users/spclaw/Documents/SPClaw Database` and can drag/drop by category under `01_DROP_HERE_Incoming/{Universes|Companies|Industries}`.
11. Validate end-to-end category workflow with Spencer:
    - Spencer drops a file into `01_DROP_HERE_Incoming/{Universes|Companies|Industries}`
    - run `make openclaw-files-sync-pull`
    - confirm file appears in `/opt/coatue-claw-data/files/incoming/{Universes|Companies|Industries}`
12. Validate Slack upload ingestion with Spencer:
    - Spencer uploads a file in Slack (without bot mention)
    - confirm bot thread ack shows routed category
    - confirm file appears in `/opt/coatue-claw-data/files/incoming/{Universes|Companies|Industries}`
    - confirm record exists in `/opt/coatue-claw-data/db/file_ingest.sqlite`
13. Configure email env vars in `/opt/coatue-claw/.env.prod` and validate:
    - `make openclaw-email-status`
    - `make openclaw-email-run-once`
    - send test email `diligence SNOW` and confirm reply
14. On Mac mini, set chart scout env in `/opt/coatue-claw/.env.prod`:
    - `COATUE_CLAW_X_CHART_SLACK_CHANNEL=<channel-id>`
    - `COATUE_CLAW_X_CHART_TIMEZONE=America/Los_Angeles`
    - `COATUE_CLAW_X_CHART_WINDOWS=09:00,12:00,18:00`
15. Run:
    - `make openclaw-24x7-enable`
    - `make openclaw-24x7-status` (confirm `com.coatueclaw.x-chart-daily` loaded)
16. Dry-run source quality and posting:
    - `make openclaw-x-chart-sources`
    - `make openclaw-x-chart-run-once`
17. Tune style details after first day of live posts (font sizing, backdrop length, and summary density).
15. If response fails, capture first failing line with `openclaw channels logs --channel slack --lines 300`.

## 2026-02-19 - Slack Build Runner rg Fallback
- Issue observed: Slack `build:`/behavior-refine flow failed inside `codex exec` when generated command used `rg` on hosts without ripgrep (`zsh: command not found: rg`).
- Code update: `src/coatue_claw/slack_pipeline.py`
  - Added explicit build-runner prompt guidance: if `rg` is unavailable, use `grep -R` (or install ripgrep) instead of failing.
- Tests:
  - Added `test_run_build_request_prompt_includes_rg_fallback` in `tests/test_slack_pipeline.py`.
  - Validation run: `PYTHONPATH=src pytest -q tests/test_slack_pipeline.py` (pass).

### Immediate Next Steps
1. On Mac mini runtime host, install ripgrep for best performance: `brew install ripgrep`.
2. Pull latest `main`, restart runtime, and retry the same Slack behavior-refine request.
3. If still failing, capture `make openclaw-slack-logs` output and confirm `PATH` seen by the runtime process includes `/opt/homebrew/bin`.

### 2026-02-19 Ship Status
- rg fallback prompt patch is ready to ship on `main` in this session.
- Next operator action after pull: `make openclaw-restart` then re-run Slack refine request.

## 2026-02-21 - X Chart Title Coherence Hardening
- Issue fixed: Chart-of-the-* titles could become ungrammatical from noisy/truncated source copy (example observed in Slack: `Institutional investors sold a are at an extreme`).
- Root cause:
  - subject extraction occasionally preserved a partial action phrase (`sold a`), then narrative template appended `are at an extreme`.
- Code updates in `/Users/carsonwang/CoatueClaw/src/coatue_claw/x_chart_daily.py`:
  - added subject cleanup before narrative synthesis (`_clean_subject_for_headline`)
  - added singular/plural copula selection (`_subject_is_plural`) so templates use `is/are` correctly
  - added explicit institutional net-seller/net-buyer narrative rules
  - added incoherent-headline detector (`_has_incoherent_headline`)
  - added final grammar-repair fallback inside `_sanitize_style_copy`
  - added style quality/checklist guardrails so ungrammatical headlines fail QA
- Tests:
  - added regression test `test_style_draft_rewrites_incoherent_institutional_selling_headline`
  - validation run: `PYTHONPATH=src pytest -q tests/test_x_chart_daily.py` -> `44 passed`

### Immediate Next Steps
1. Deploy latest `main` to `/opt/coatue-claw` and restart runtime (`make openclaw-restart`).
2. Run manual post-url smoke tests with known noisy titles and verify coherent final headline in Slack.
3. Monitor next scheduled morning/afternoon/evening chart posts for title quality regressions.

## 2026-02-21 - Remove Chart Label From Source-Snip Output
- User request: remove chart-label line from chart-of-the-* output now that final render is a direct X source snip card.
- Changes:
  - removed chart-label subtitle line from source snip card renderer (`_render_source_snip_card`)
  - removed `- Chart label: ...` from Slack initial comment payload in `_post_winner_to_slack`
  - retained internal style-draft `chart_label` field for QA/scoring only (not user-facing in snip mode)
- Tests:
  - updated `tests/test_x_chart_daily.py` to assert Slack initial comment no longer includes `Chart label:`
  - validation run: `PYTHONPATH=src pytest -q tests/test_x_chart_daily.py` -> `44 passed`

### Immediate Next Steps
1. Deploy/restart on mini and run one `run-post-url` smoke test.
2. Confirm Slack output in `#charting` includes title + takeaway + source link, with no chart-label line.

## 2026-02-23 - MD Catalyst Fix For APP SEC-Probe Attribution
- Issue observed: APP mover line defaulted to generic/fallback wording and missed clear SEC-probe narrative visible in top web coverage.
- Root cause:
  - no dedicated `regulatory/probe` cause cluster;
  - Barron's (`barrons.com`) was not treated as a quality cause domain;
  - APP-specific web retrieval queries were too generic.
- Code updates in `/Users/carsonwang/CoatueClaw/src/coatue_claw/market_daily.py`:
  - added APP alias overrides (`AppLovin`, `AppLovin Corporation`, `AppLovin Corp`);
  - added `regulatory_probe` driver cluster + event phrase (`reports of an active SEC probe.`);
  - added cluster priority bonus for `regulatory_probe`;
  - added `barrons.com` to both domain weights and quality-domain allowlist;
  - expanded APP web queries (`sec probe`, `regulatory probe`, `short seller report`);
  - boosted directional/decisive recognition for probe/investigation/regulatory wording.
- Tests added in `/Users/carsonwang/CoatueClaw/tests/test_market_daily.py`:
  - `test_regulatory_probe_cluster_extraction_maps_keywords`
  - `test_barrons_domain_counts_as_quality_source`
  - `test_app_regulatory_probe_cluster_outputs_specific_reason`
- Validation run:
  - `PYTHONPATH=src python3 -m pytest tests/test_market_daily.py` -> `31 passed`.

### Immediate Next Steps
1. Pull latest `main` on Mac mini and restart runtime (`make openclaw-restart`).
2. Run `md debug APP` (or CLI `claw market-daily debug-catalyst APP --slot open`) and confirm selected cluster `regulatory_probe`.
3. Run `md now --force` in Slack and verify APP reason line explicitly names SEC-probe overhang when corroborated evidence is present.

## 2026-02-23 - X Chart Takeaway Sentence Completeness + Candidate Fallback
- Issue fixed: X chart posts could emit incomplete takeaway fragments (example class: `...fell to lowest`) due to hard clipping without sentence validation.
- Root cause:
  - takeaway text was length-constrained but not validated for sentence completeness;
  - degenerate one-token LLM outputs (`U.S`) could survive as headline/chart label;
  - scout winner selection did not switch candidates when top copy quality was poor.
- Code updates in `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`:
  - added deterministic copy-quality helpers:
    - `_is_complete_sentence`
    - `_finalize_takeaway_sentence`
    - `_rewrite_takeaway_from_candidate`
    - `_is_degenerate_copy_value`
  - upgraded `_sanitize_style_copy(...)` to:
    - enforce complete takeaway sentences;
    - rewrite low-quality/fragment takeaways from candidate context;
    - rebuild degenerate headline/chart-label values;
    - return rewrite diagnostics (`copy_rewrite_applied`, `copy_rewrite_reason`).
  - added style checks:
    - `takeaway_complete_sentence`
    - `headline_non_degenerate`
    - `chart_label_non_degenerate`
  - added scout fallback selection in `run_chart_scout_once(...)`:
    - evaluates candidate pool in score order;
    - skips top candidate when copy quality fails and picks next valid candidate;
    - returns `candidate_fallback_used` + rewrite diagnostics in result payload.
  - kept explicit URL behavior strict in `run_chart_for_post_url(...)`:
    - preserves requested URL;
    - rewrites copy as needed;
    - returns diagnostics (`copy_rewrite_applied`, `copy_rewrite_reason`, `candidate_fallback_used=false`).
  - expanded post-publish review checklist and persisted these checks in review payload.
- Tests added in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_takeaway_sentence_validator_rejects_fragment_fell_to_lowest`
  - `test_takeaway_sentence_finalizer_returns_complete_sentence`
  - `test_style_draft_rewrites_degenerate_fields_and_fragment_takeaway`
  - `test_run_chart_scout_falls_back_when_top_candidate_copy_is_bad`
  - `test_run_chart_for_post_url_rewrites_takeaway_but_keeps_requested_url`
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `49 passed`.
  - full suite smoke: `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> 3 pre-existing unrelated failures in `test_spencer_change_digest` / `test_spencer_change_log`.
- Runtime smoke checks completed:
  - `make -C /opt/coatue-claw openclaw-x-chart-run-once`
  - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-post-url https://x.com/Barchart/status/2025715989384663396`
  - both posted successfully and emitted rewrite diagnostics; review checks reported complete-sentence and non-degenerate copy.

### Immediate Next Steps
1. Pull latest `main` on Mac mini runtime and restart (`make openclaw-restart`).
2. In Slack `#charting`, run `@Coatue Claw x chart now` and confirm takeaway is a complete sentence with no clipped ending.
3. Run one explicit URL request and confirm posted source URL is unchanged while takeaway is rewritten when needed.
4. Triage unrelated full-suite failures in `tests/test_spencer_change_digest.py` and `tests/test_spencer_change_log.py` separately from X-chart pipeline.

## 2026-02-23 - X Chart Headline Completeness Guardrail
- Issue fixed: source-snip chart posts could still ship malformed titles (example observed in Slack: `U.S. Housing Market Home Sellers now is`).
- Root cause:
  - title pipeline had length + degenerate checks but no deterministic headline-phrase completeness gate;
  - `U.S.` abbreviation splitting in first-sentence extraction could collapse subject parsing and degrade headline quality.
- Code updates in `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`:
  - added headline-quality helpers:
    - `_is_complete_headline_phrase`
    - `_finalize_headline_phrase`
    - `_rewrite_headline_from_candidate`
    - `_strip_trailing_headline_dangling_endings`
  - added headline dangling ending rules (copulas/prepositions and fragment tails like `now is`).
  - integrated headline finalization + rewrite into `_sanitize_style_copy(...)`:
    - no generic forced fallback title on unrecoverable headline;
    - sets `copy_rewrite_reason=headline_unrecoverable` when title cannot be repaired.
  - fixed `_extract_first_sentence(...)` to avoid `U.S.` abbreviation truncation when the first segment is too short.
  - made headline completeness publish-critical:
    - `_style_copy_quality_errors` includes `headline incomplete phrase`
    - `_style_copy_publish_issues` includes `headline_incomplete_phrase`
    - style checks include `headline_complete_phrase`
    - post-review checks include `headline_complete_phrase`
  - scout behavior update:
    - if all candidates fail publish copy quality (including headline completeness), scout run returns non-post result:
      - `reason=no_publishable_candidate_available`
      - `publish_issues=[...]`
    - scout no longer force-posts top invalid candidate in this failure mode.
  - explicit URL behavior remains strict:
    - requested URL is preserved
    - run errors if headline remains invalid after rewrite.
- Tests added/updated in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_headline_phrase_validator_rejects_fragment_home_sellers_now_is`
  - `test_headline_phrase_finalizer_returns_empty_for_dangling_copula`
  - `test_style_draft_rewrites_broken_headline_phrase`
  - `test_run_chart_scout_falls_back_when_top_candidate_headline_is_incomplete`
  - `test_run_chart_scout_returns_non_post_when_no_publishable_candidate`
  - `test_run_chart_for_post_url_errors_when_headline_unrecoverable`
  - expanded explicit-URL rewrite regression to assert headline validity.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `55 passed`.
  - full smoke: `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

### Immediate Next Steps
1. Pull latest `main` on Mac mini and restart runtime (`make openclaw-restart`).
2. In Slack `#charting`, run one `x chart now` and confirm title is a complete grammatical phrase (no clipped ending).
3. Run one explicit URL command for the same source class and confirm:
   - URL is unchanged
   - title is either repaired or request fails with publish-copy error (no invalid title post).
4. Triage the unrelated Spencer-change identity defaults failures in a separate patch.

## 2026-02-23 - Board Seat Non-Repeat Guardrail (Anduril)
- User signal captured from `#anduril`: Spencer explicitly flagged repeated investment pitches (example callout: repeated Epirus idea) and requested net-new ideas unless material change.
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`
  - added persistent pitch-history store table: `board_seat_pitches` in `/opt/coatue-claw-data/db/board_seat_daily.sqlite`
  - pitch-history schema stores:
    - company/channel metadata
    - message text + extracted investment text
    - investment signature/hash
    - context signature/snippets
    - significant-change flag
  - existing `board_seat_runs` rows are auto-seeded into `board_seat_pitches` on startup (`legacy_run_seed`) to bootstrap historical memory.
  - added best-effort Slack history backfill path for each channel (`slack_history_backfill`) when history scope is available.
  - added deterministic repeat detection:
    - exact hash repeat block
    - high signal-line similarity repeat block
    - special repeat block for repeated “no high signal updates” theme
  - added significant-change gating:
    - allows prior idea only when context changed materially (token novelty/event-term or numeric delta)
  - if repeated idea detected without significant change:
    - tries one net-new fallback rewrite
    - if still repeated, skips posting with explicit reason `repeat_investment_without_significant_change`
  - LLM prompt now receives prior investment theses and is instructed not to repeat absent clear context change.
  - status payload now includes `pitch_counts` by company.
- Tests added/updated in `/opt/coatue-claw/tests/test_board_seat_daily.py`:
  - extraction test for structured investment text
  - repeat-without-change skip behavior
  - allow-post when significant change exists
  - history-backfill parsing test
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `9 passed`
  - full smoke remains unchanged with 3 unrelated pre-existing failures in Spencer-change tests.
- Live runtime verification:
  - forced dry-run for Anduril now returns skip reason `repeat_investment_without_significant_change`
  - forced live run also skipped repeat post (no duplicate idea pushed)
  - current seeded Anduril pitch history count: `3`
- Slack history scope note:
  - direct `conversations_history` backfill for `#anduril` currently scans `0` messages under present bot scopes/token permissions.
  - existing historical posts are still captured via `legacy_run_seed` from `board_seat_runs`.

### Immediate Next Steps
1. Add/confirm Slack scopes for deep backfill (`channels:history` and private-channel equivalent as needed), reinstall app, then re-run history backfill.
2. Re-run board-seat force run in `#anduril` after new Anduril signals appear and confirm post is allowed only when significant change is detected.
3. Expand repeat detector from textual similarity to named-investment entity tracking (e.g., Epirus/Shield AI/Saronic) if repeated-name risk remains high.

## 2026-02-23 - X Chart Fragment-Tail Guardrails (Title + Takeaway)
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - added deterministic tail-fragment dictionaries:
    - `TRAILING_DETERMINERS`
    - `TRAILING_QUALIFIERS`
  - added new validators:
    - `_has_fragment_tail(words)`
    - `_tail_complete(text)`
  - title/takeaway completeness now rejects clipped tails like:
    - `... in their`
    - `... in their initial`
    - `... in early`
  - title/takeaway finalizers now fail fast on fragment tails after clipping and force rewrite path instead of posting incomplete phrases.
  - headline/takeaway rewrite flow updated to apply fragment-tail rewrites and diagnostic reasons:
    - `headline_tail_fragment_rewritten`
    - `takeaway_tail_fragment_rewritten`
    - `headline_unrecoverable` (unchanged for hard failures)
  - style quality and publish gates now include tail-fragment checks:
    - `_style_copy_quality_errors(...)`
    - `_style_copy_publish_issues(...)`
  - style draft checks include:
    - `headline_tail_complete`
    - `takeaway_tail_complete`
  - post-review metadata now includes:
    - `_post_publish_checklist(...).checks.headline_tail_complete`
    - `_post_publish_checklist(...).checks.takeaway_tail_complete`
    - `_post_winner_to_slack(...).review.checks.headline_tail_complete`
    - `_post_winner_to_slack(...).review.checks.takeaway_tail_complete`
- Tests updated in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_headline_validator_rejects_trailing_possessive_fragment`
  - `test_takeaway_validator_rejects_trailing_possessive_fragment`
  - `test_headline_finalizer_returns_empty_for_clipped_their_fragment`
  - `test_takeaway_finalizer_returns_empty_for_clipped_initial_fragment`
  - `test_style_draft_rewrites_fragmented_kobeissi_copy`
  - `test_run_chart_scout_falls_back_when_top_candidate_has_fragment_tail`
  - updated explicit URL rewrite regression to assert fragment-tail rewrite behavior and diagnostics.
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py` -> `61 passed`
  - full smoke: `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
    - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
    - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
    - `tests/test_spencer_change_log.py::test_requester_label_defaults`

### Immediate Next Steps
1. Pull latest `main` on the Mac mini and restart runtime (`make openclaw-restart`).
2. Trigger `x chart now` in Slack and confirm title/takeaway no longer end in clipped tails.
3. Trigger one explicit URL post for a fragment-prone source and confirm URL is preserved with rewritten coherent copy.

## 2026-02-23 - X Chart Run-On Clause Fix (Takeaway + Role Stability)
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - added deterministic unjoined-clause detection for takeaway copy:
    - `_has_unjoined_clause_boundary(text)`
    - `_first_unjoined_clause_boundary_index(text)`
    - `_tokenize_clause_words(text)`
  - added deterministic repair path:
    - `_repair_takeaway_clause_boundary(text)` rewrites run-ons into one coherent sentence with connector (`while`) and re-validates.
  - takeaway validation/finalization tightened:
    - `_is_single_sentence_takeaway(...)` now rejects unjoined clause boundaries.
    - `_finalize_takeaway_sentence(...)` now repairs run-on clause boundaries before accepting.
  - sanitize flow updated:
    - `_sanitize_style_copy(...)` marks rewrite diagnostic `takeaway_clause_rewritten` when this recovery path is applied.
  - publish gates tightened:
    - `_style_copy_quality_errors(...)` now adds `takeaway clause boundary invalid`.
    - `_style_copy_publish_issues(...)` now adds `takeaway_clause_boundary_invalid`.
  - review/style checks expanded:
    - `takeaway_clause_boundary_ok` added to style/review check payloads.
  - role-stability hardening:
    - `_enforce_title_takeaway_roles(...)` now compacts run-on headlines to concise locked-term-safe core sentence when role order is otherwise valid.
- Tests updated in `/opt/coatue-claw/tests/test_x_chart_daily.py`:
  - `test_takeaway_validator_rejects_market_cap_ai_runon`
  - `test_takeaway_finalizer_repairs_unjoined_clause_boundary`
  - `test_role_enforcement_compacts_runon_headline_when_role_order_is_valid`
  - updated Kobeissi regression expected takeaway to coherent one-sentence form:
    - `US stocks erase nearly -$800 billion in market cap while AI disruption fears spread and trade war headlines return.`
- Validation:
  - targeted:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py` -> `77 passed`
  - full smoke:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
      - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
      - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
      - `tests/test_spencer_change_log.py::test_requester_label_defaults`
- Live validation:
  - explicit URL reposted:
    - `https://x.com/KobeissiLetter/status/2026040229535047769`
  - result payload showed:
    - `copy_rewrite_reason = takeaway_clause_rewritten`
    - `takeaway_clause_boundary_ok = true`
    - `takeaway_single_sentence = true`
    - `title_takeaway_role_ok = true`
    - all post-review checks passed.

### Immediate Next Steps
1. In Slack `#charting`, visually confirm the latest Kobeissi repost shows:
   - concise title ending with `market cap.`
   - one-sentence takeaway using connector (`while`) and terminal punctuation.
2. Monitor the next 3 manual `run-post-url` uses for `takeaway_clause_rewritten` rate; if frequent, add pre-LLM prompt guidance to avoid unjoined clause patterns earlier.
3. Keep Spencer-change identity failures out of this charting branch and patch separately.

## 2026-02-23 - X Chart Post Copy Cleanup + #charting Purge Attempt
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - removed `- Render: ...` line from Slack chart post `initial_comment` output.
  - post summary now includes only:
    - Source
    - Title
    - Key takeaway
    - Link
- Test updated: `/opt/coatue-claw/tests/test_x_chart_daily.py`
  - `test_post_winner_preserves_takeaway_punctuation_in_slack_comment` now asserts `- Render:` is absent.
- Validation:
  - targeted tests:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py` -> `77 passed`
- Slack channel cleanup run (`#charting`, channel id `C0AFXM2MWAV`):
  - API delete pass results:
    - `messages_seen: 48`
    - `deleted: 36`
    - `cant_delete_message: 12`
  - follow-up pass:
    - `messages_seen: 11`
    - `deleted_now: 0`
    - `cant_delete_now: 11`
  - remaining messages are non-bot/user or channel-system events (join/rename/user posts), which the current bot token cannot delete.

### Immediate Next Steps
1. If full channel wipe is still required, use a user/admin token with delete rights (or manual Slack admin UI deletion for non-bot messages).
2. Keep current bot deletion helper for future bot-authored cleanup runs.

## 2026-02-23 - DM Routing Fix for SPClaw
- Runtime modules updated:
  - `/opt/coatue-claw/src/coatue_claw/slack_routing.py`
    - added `should_route_message_event(text, channel_type)`:
      - always routes non-empty direct-message events (`channel_type=im`), even if the DM includes `<@SPClaw>` mention markup.
      - preserves existing mention-based default routing behavior for channel messages.
  - `/opt/coatue-claw/src/coatue_claw/slack_bot.py`
    - `handle_message` now uses `should_route_message_event(...)` instead of only `should_default_route_message(...)`.
    - DM events are tagged as `source_event=slack-message-dm` for logging/memory source traceability.
- Tests updated:
  - `/opt/coatue-claw/tests/test_slack_routing.py`
    - added `test_should_route_message_event_im_always_routes_nonempty`
    - added `test_should_route_message_event_non_im_follows_default_rules`
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_slack_routing.py /opt/coatue-claw/tests/test_x_chart_daily.py /opt/coatue-claw/tests/test_slack_x_chart_intent.py` -> `82 passed`
  - runtime restart:
    - `make openclaw-restart` (gateway restarted)
    - `make openclaw-slack-status` -> Slack probe `ok=true`, bot `coatue_claw` connected.

### Immediate Next Steps
1. DM `SPClaw` a plain message and a second message including `@SPClaw` to verify both trigger replies.
2. If DM still does not trigger, check Slack app Event Subscriptions include DM `message` events (`message.im`) and reinstall app.

## 2026-02-23 - Board Seat Styling + Source Links (Global Default)
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`
  - BoardSeat draft schema now carries source links:
    - `BoardSeatDraft.source_urls`
  - Added deterministic source-link normalization/fallback helpers:
    - `_normalize_source_url(...)`
    - `_normalize_source_urls(...)`
    - `_fallback_source_urls(company)`
    - `_message_source_urls(company, draft)`
  - Renderer now always appends a clickable sources block:
    - `*Sources*`
    - `<https://...|Source 1>`
    - `<https://...|Source 2>`
  - Source-link behavior:
    - prefer funding/LLM-provided URLs when available
    - if missing, auto-add deterministic Google reference queries so links are always present
  - Sanitizer now preserves/normalizes source URLs from draft first, then funding snapshot fallback.
- Tests updated: `/opt/coatue-claw/tests/test_board_seat_daily.py`
  - `test_v3_message_structure_uses_labeled_lines_not_bullets` now asserts `*Sources*` exists.
  - `test_v3_context_and_funding_use_labeled_lines` now asserts clickable source links are rendered.
  - `test_run_once_dry_run_v3_contains_hierarchy_sections` now asserts sources section exists in preview.
  - added `test_render_board_seat_message_uses_fallback_sources_when_missing`.
- Validation:
  - targeted:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `13 passed`
  - full smoke:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
      - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
      - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
      - `tests/test_spencer_change_log.py::test_requester_label_defaults`
- Runtime prompt update (non-repo, live OpenClaw workspace):
  - `/Users/spclaw/.openclaw/workspace/AGENTS.md`
  - Portco idea template switched to markdown bold markers (`**...**`) and now explicitly requires a clickable `Sources` section with at least two links.
  - Gateway restarted and health re-checked:
    - `make -C /opt/coatue-claw openclaw-restart`
    - `make -C /opt/coatue-claw openclaw-slack-status` -> probe `ok=true`

### Immediate Next Steps
1. In Slack `#anduril` (or any portco channel), request a fresh board-seat idea and confirm section labels render as bold and include `Sources` links.
2. If any ad-hoc conversational reply still shows italics, inspect the specific outgoing text path (OpenClaw conversational formatter vs `board_seat_daily`) and enforce bold markers in that path as well.

## 2026-02-23 - Slack `#openai` No-Response Incident (Operational Fix)
- Symptom:
  - user reported no reply after posting in `#openai`.
- Root cause found:
  - bot membership check showed `openai` channel existed but bot was not a member:
    - `openai id=C0AFZ2YSQN9 is_member=False`
  - OpenClaw runtime channel map in `/Users/spclaw/.openclaw/openclaw.json` only had:
    - `C0AFGMRFWP8` (general)
- Fix applied (runtime-side, non-repo):
  - Joined bot to `#openai` via Slack API:
    - `conversations_join(channel=C0AFZ2YSQN9)` -> success.
  - Updated OpenClaw config channel map to include `#openai`:
    - added `C0AFZ2YSQN9: { enabled: true, requireMention: false }`
  - Restarted gateway and verified health:
    - `make -C /opt/coatue-claw openclaw-restart`
    - `make -C /opt/coatue-claw openclaw-slack-status` -> probe `ok=true`
  - Posted a heartbeat message directly to `#openai` as delivery check:
    - `chat_postMessage(channel=C0AFZ2YSQN9, text='SPClaw runtime check...')` -> success.

### Immediate Next Steps
1. Ask user to send one fresh message in `#openai` to confirm end-to-end reply behavior.
2. If still no auto-reply, capture exact message text and timestamp, then inspect `openclaw channels logs --channel slack --lines 300` around that timestamp.

## 2026-02-23 - Slack Bot Channel Join Sweep (All Visible Channels)
- User request: join every channel with the bot.
- Action taken:
  - executed Slack membership sweep via bot token from `/Users/spclaw/.openclaw/openclaw.json`.
  - listed all non-archived public/private channels visible to the bot and attempted `conversations.join` for non-member channels.
- Result:
  - `total_channels_seen: 15`
  - `already_member: 5`
  - `joined_now: 10`
  - `failures: 0`
  - post-check: `not_member_count: 0`
- Operational note:
  - this confirms the bot is now in every channel currently visible/listable to the token.
  - if new channels are created later, either re-run the sweep or keep auto-join bootstrap enabled.

### Immediate Next Steps
1. Ask user to test in any previously non-responsive channel (for example `#openai`) and confirm response path is live.
2. If a newly created channel is missed in the future, run another join sweep or verify `COATUE_CLAW_SLACK_AUTOJOIN_PUBLIC_CHANNELS=1`.

## 2026-02-23 - Board Seat V4 (Acquisition/Acquihire + Named Citations)
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/board_seat_daily.py`
  - format version bumped:
    - `BOARD_SEAT_FORMAT_VERSION = "v4_acq_acquihire_named_sources"`
  - `BoardSeatDraft` schema expanded:
    - `idea_line` (mandatory explicit acquisition/acquihire line)
    - `source_refs` (`SourceRef` objects with publisher/title/url)
  - added `SourceRef` dataclass and source-normalization/render helpers.
  - rendering now enforces V4 structure:
    - `Idea` line is first under `Thesis`
    - `Sources` block uses named source + title + clickable link
    - numeric labels (`Source 1/2/3`) removed.
  - acquisition-idea enforcement:
    - validator rejects non-acquisition idea lines
    - validator rejects placeholder target patterns
    - sanitizer rewrites invalid ideas to best-effort acquisition form
    - fallback behavior remains “always post best-effort candidate”.
  - repeat guardrail extraction updated:
    - `Idea:` is parsed as part of thesis/core investment signal.
  - acquisition evidence collection added:
    - `_acquisition_search_rows(...)` uses Brave search (when key exists) for acquisition/acquihire-target evidence rows.
- Tests updated: `/opt/coatue-claw/tests/test_board_seat_daily.py`
  - migrated to V4 expectations and added coverage for:
    - explicit `Idea` line
    - invalid non-acquisition thesis rejection
    - best-effort acquisition fallback behavior
    - named citation rendering and ban on numeric source labels
    - V4 repeat-guardrail behavior
    - legacy parse compatibility.
- Validation:
  - targeted:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `15 passed`
  - full smoke:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q` -> unchanged unrelated failures:
      - `tests/test_spencer_change_digest.py::test_run_once_dry_run_includes_carson_label`
      - `tests/test_spencer_change_log.py::test_is_spencer_user_defaults`
      - `tests/test_spencer_change_log.py::test_requester_label_defaults`
- Interactive prompt path update (non-repo):
  - `/Users/spclaw/.openclaw/workspace/AGENTS.md`
  - Portco idea template now requires:
    - acquisition/acquihire-only primary idea
    - explicit `Idea` labeled line
    - named source+title citation lines (no Source 1/2/3)
  - runtime restarted and verified healthy:
    - `make -C /opt/coatue-claw openclaw-restart`
    - `make -C /opt/coatue-claw openclaw-slack-status` (`probe.ok=true`)

### Immediate Next Steps
1. In `#openai`, send: `give me a new board seat idea` and verify first thesis line is `Idea: Acquire/Acquihire ...`.
2. Confirm `Sources` now render as `Publisher — Article title: <link>` and never as numeric source labels.

## 2026-02-23 - Board Seat V4 Fallback Target Hardening
- Issue found during dry-run validation:
  - low-signal fallback produced invalid placeholder target text (`Idea: Acquire No ...`) for `OpenAI`.
- Fix shipped in `src/coatue_claw/board_seat_daily.py`:
  - added `no` to acquisition placeholder target blocklist.
  - tightened target candidate extraction to skip ultra-short candidates (`len < 3`).
- Tests:
  - added regression in `tests/test_board_seat_daily.py`:
    - `test_best_effort_idea_line_avoids_placeholder_no_target`
  - targeted suite:
    - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_board_seat_daily.py` -> `16 passed`
- Runtime validation:
  - dry run now yields valid best-effort idea line:
    - `Idea: Acquire Stealth AI Systems ...`
  - restarted runtime and confirmed Slack probe `ok=true`.

### Immediate Next Steps
1. Live-test `#openai` with `give me a new board seat idea` and confirm no placeholder targets in `Idea`.
2. If a fallback idea still looks weak, add a deterministic company-specific fallback target map before posting.

## 2026-02-24 - Memory-to-Git Reconciliation Policy v1 (Hybrid Auto)
- Scope shipped to `main`:
  - explicit Slack prefix trigger: `git-memory:`
  - queueing for git reconciliation using existing change tracker.
- Code changes:
  - `src/coatue_claw/slack_bot.py`
    - detects `git-memory:` at message start
    - captures queue items as:
      - `request_kind=memory_git`
      - `trigger_mode=git_memory_prefix`
      - `source_ref=slack://... | memory:/Users/spclaw/.openclaw/workspace/memory/YYYY-MM-DD.md`
    - preserves runtime memory ingestion and replies with queue id/status in-thread.
  - `src/coatue_claw/spencer_change_log.py`
    - schema migration adds: `request_kind`, `trigger_mode`, `source_ref`, `related_commit`
    - new tracker methods:
      - `reconcile_status()`
      - `export_memory_git_queue(...)`
      - `reconcile_link(...)`
    - `list_changes(...)` now supports `request_kind` and `open_only` filters.
  - `src/coatue_claw/cli.py`
    - added:
      - `claw memory reconcile-status`
      - `claw memory reconcile-export --limit N`
      - `claw memory reconcile-link --ids 1,2 --commit <hash> [--resolved-by ...]`
- Repo audit artifacts:
  - `docs/memory-inbox/queue.md`
  - `docs/memory-inbox/reconciliation-ledger.csv`
- Slack governance UX (reused existing surface):
  - `spencer changes memory`
  - `change requests memory`
- Validation:
  - `PYTHONPATH=/opt/coatue-claw/src /opt/coatue-claw/.venv/bin/python -m pytest -q /opt/coatue-claw/tests/test_spencer_change_log.py /opt/coatue-claw/tests/test_spencer_change_digest.py` -> `14 passed`

### Immediate Next Steps
1. Restart runtime and verify Slack probe healthy.
2. Post one live `git-memory:` message in Slack and confirm ack + queue id.
3. Run `spencer changes memory` and `claw memory reconcile-export --limit 200` to verify end-to-end queue visibility.

## 2026-02-24 - Scheduled Memory Reconcile Export Enabled
- User request: schedule memory-git queue export so laptop/git view stays fresh without manual steps.
- Code shipped:
  - `src/coatue_claw/launchd_runtime.py`
    - added service `com.coatueclaw.memory-reconcile-export`
    - runs `python -m coatue_claw.cli memory reconcile-export --limit <N>`
    - default cadence: 15 minutes (`COATUE_CLAW_MEMORY_RECONCILE_INTERVAL_SECONDS=900`)
    - queue limit env: `COATUE_CLAW_MEMORY_RECONCILE_EXPORT_LIMIT` (default `200`)
    - new selector: `--service memoryreconcile`
  - `Makefile`
    - `openclaw-memory-reconcile-status`
    - `openclaw-memory-reconcile-export`
  - `docs/openclaw-runtime.md`
    - documented new launchd plist + commands + env controls
  - `tests/test_launchd_runtime.py`
    - service spec / all-services / selector coverage updated.
- Runtime action on Mac mini:
  - enabled service directly:
    - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.launchd_runtime enable --service memoryreconcile`
  - status now shows:
    - `com.coatueclaw.memory-reconcile-export` loaded in `gui/501`, `last_exit_code=0`.
  - manual checks passed:
    - `make -C /opt/coatue-claw openclaw-memory-reconcile-status`
    - `make -C /opt/coatue-claw openclaw-memory-reconcile-export`

### Immediate Next Steps
1. Send one `git-memory:` message in Slack and confirm queue file refreshes automatically within 15 minutes.
2. If needed, tune interval by setting `COATUE_CLAW_MEMORY_RECONCILE_INTERVAL_SECONDS` in `.env.prod` and rerun `make openclaw-24x7-enable`.

## 2026-02-24 - Auto DM Alert for Behavior Change Requests
- User request: for every Slack behavior-change ask, auto-notify Carson and confirm:
  - A) request added to memory markdown
  - B) request queued for git reconciliation upload path.
- Shipped in `src/coatue_claw/slack_bot.py`:
  - detects behavior-change asks in plain language (not just `git-memory:` prefix)
  - captures as `memory_git` queue item with trigger mode:
    - `git_memory_prefix` or `auto_behavior_request`
  - appends request line to `MEMORY.md` (path env-configurable)
  - refreshes queue snapshot (`docs/memory-inbox/queue.md`)
  - sends immediate DM to Carson (or env-configured notify users)
  - posts in-thread acknowledgement with queue ID.
- Tracker update:
  - `src/coatue_claw/spencer_change_log.py` now accepts `auto_behavior_request` trigger mode.
- Config knobs:
  - `COATUE_CLAW_CHANGE_NOTIFY_USER_IDS` (default Carson ID)
  - `COATUE_CLAW_CHANGE_MEMORY_MD_PATH` (default `/Users/spclaw/.openclaw/workspace/MEMORY.md`)
- Validation:
  - targeted tests: `20 passed`
  - full suite: `223 passed`

### Immediate Next Steps
1. In Slack, post a plain request like “can you change the bot so chart follow-up is shorter”.
2. Confirm:
   - Carson receives DM immediately
   - queue shows item under `spencer changes memory`
   - request line appears in configured `MEMORY.md`.

## 2026-02-24 - X Chart Winner Selection: 3-Day Source Repeat Cooldown
- User preference implemented: same source account may repeat, but not within 3 days of its most recent posted chart when alternatives exist.
- Runtime module updated: `/opt/coatue-claw/src/coatue_claw/x_chart_daily.py`
  - added `_source_repeat_days()` reading `COATUE_CLAW_X_CHART_SOURCE_REPEAT_DAYS` (default `3`)
  - `_pick_winner(...)` now:
    - builds `source_last_posted` from recent posted slots
    - filters candidates from sources posted within cooldown window
    - applies existing score-floor source-variety ranking on cooled pool
    - falls back to original score pool if cooldown would otherwise empty the set
- Tests updated: `/opt/coatue-claw/tests/test_x_chart_daily.py`
  - `test_pick_winner_enforces_source_repeat_cooldown_with_alternative`
  - `test_pick_winner_allows_recent_source_when_no_alternative`
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py -k "pick_winner"` -> `4 passed`
  - `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `76 passed`

### Immediate Next Steps
1. Observe next scheduled chart windows and confirm no same-source repeats within 3 days unless no viable alternative exists.
2. If too restrictive in live flow, reduce `COATUE_CLAW_X_CHART_SOURCE_REPEAT_DAYS` to `2` in `.env.prod`.

## 2026-02-24 - X Chart Reinforcement: Preferred Sources + Style-Quality Score
- User input: reinforce chart-of-day scout toward Spencer-preferred chart style and source accounts.
- Shipped in `src/coatue_claw/x_chart_daily.py`:
  - added preferred default source seeds:
    - `stock_unlock` priority `1.45`
    - `stripe` priority `1.4`
  - added deterministic `_style_quality_score(...)` integrated into `_score_candidate(...)`:
    - boosts institutional/data-dense language (YoY/QoQ/CAGR, guidance/consensus, trend framing, quantitative context)
    - penalizes promo CTA patterns (`discord`, `chatroom`, `link below`, `join`, `free`, etc.)
    - penalizes cashtag-heavy spam bursts, especially with CTA wording
- Tests updated in `tests/test_x_chart_daily.py`:
  - `test_store_seeds_priority_sources`
  - `test_score_candidate_boosts_institutional_chart_language`
  - `test_score_candidate_penalizes_cashtag_spam_with_cta`
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `78 passed`

### Immediate Next Steps
1. Watch next scheduled posts to confirm stronger preference for institutional chart posts over promo-style posts with similar engagement.
2. If selection is too sensitive, tune the two new source priorities by ±0.1 and re-run `tests/test_x_chart_daily.py`.

## 2026-02-24 - X Chart Reinforcement: Preferred Topic Tags + Source Handles
- User confirmed preferred examples and requested reinforcement on both topic coverage and account inclusion.
- Shipped in `src/coatue_claw/x_chart_daily.py`:
  - added topic-keyword tags across theme/signal/style dictionaries for:
    - backlog inflection
    - AI data-center power-demand narratives
    - market breadth/rotation/dispersion regime signals
    - positioning/underallocation and stock-picker regimes
  - added preferred default source handles:
    - `MikeZaccardi` (`1.3`)
    - `oguzerkan` (`1.25`)
- Tests updated in `tests/test_x_chart_daily.py`:
  - source seeding now asserts `mikezaccardi` and `oguzerkan`
  - added `test_score_candidate_boosts_preferred_topic_tags`
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_x_chart_daily.py` -> `79 passed`

### Immediate Next Steps
1. Let scheduler run for 2-3 windows and inspect winners for higher hit-rate on preferred topics.
2. If source concentration climbs, trim handle priorities by small increments while preserving topic-tag weights.

## Update (2026-02-24, integrator deploy: Market Daily no-X patch on main)
- Integrated `origin/codex/agent-market-daily` into `main` (merge commit: `d6c2bdd`).
- Phase A validation/deploy completed on `/opt/coatue-claw`:
  - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_market_daily.py` -> `40 passed`
  - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
  - `make openclaw-restart`
  - `make openclaw-slack-status` -> probe recovered healthy (`ok=true`, `status=200`)
  - `make openclaw-market-daily-run-once FORCE=1` -> posted (`run_id=7`)
  - `make openclaw-market-daily-earnings-recap-run-once FORCE=1` -> `no_reporters` cleanly (`run_id=8`)
- No-X checks confirmed from artifact/debug output:
  - `/opt/coatue-claw-data/artifacts/market-daily/md-close-20260225-022458.md` footer now reads:
    - `Sources: Yahoo fast_info + Yahoo news + web search`
  - mover lines include `[News]` / `[Web]` only (no `[X]`).
  - `debug-catalyst INTC --slot close` returns links map with only `news`/`web` fields (no `x`).
- Follow-up required (Phase B): reject quote-directory wrapper titles from catalyst phrase selection (INTC currently still picks Yahoo quote-page style wrapper title).

## Update (2026-02-24, Market Daily Intel headline-quality hardening)
- Implemented deterministic wrapper-title rejection in `src/coatue_claw/market_daily.py` for quote-directory style evidence text.
- Added wrapper-title guardrails at all required gates:
  - `_normalize_evidence_candidates(...)` now tags wrapper titles with `reject_reason="generic_wrapper"`.
  - `_pick_direct_cause_candidate(...)` now skips wrapper-title candidates.
  - `_cluster_event_phrase(...)` returns `None` for wrapper-title candidate text.
  - `_build_reason_line_from_phrase(...)` now falls back for wrapper-title phrases.
- Added/updated tests in `tests/test_market_daily.py`:
  - quote-page wrapper title rejected as generic wrapper.
  - direct-evidence path skips wrapper title when a better causal headline exists.
  - INTC-like wrapper phrase path falls back (never emits wrapper text in rendered reason line).
- Validation:
  - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_market_daily.py` -> `43 passed`
  - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Runtime verification:
  - `make openclaw-restart` + `make openclaw-slack-status` probe healthy (`ok=true`).
  - `make openclaw-market-daily-run-once FORCE=1` artifact: `/opt/coatue-claw-data/artifacts/market-daily/md-close-20260225-023157.md`.
  - INTC line now: `Likely positioning/flow; no single confirmed catalyst.` (no quote-directory wrapper phrase).

## Update (2026-02-24, integrator merge/deploy of latest Market Daily role-branch fixes)
- Merged `origin/codex/agent-market-daily` into `main` with merge commit `c5f6eca`.
  - confirms inclusion of role-branch head `e9ae7d8` (`market-daily: reject quote-directory wrapper headlines for catalysts`).
- Validation:
  - exact checklist commands using system python failed because `pytest` is not installed in `/usr/bin/python3` (`No module named pytest`).
  - equivalent venv validations passed:
    - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_market_daily.py` -> `47 passed`
    - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Runtime checks:
  - `make openclaw-restart`
  - `make openclaw-slack-status` -> probe `ok=true`, `status=200`
  - `make openclaw-market-daily-run-once FORCE=1` -> posted (`artifact: md-close-20260225-023700.md`)
  - `make openclaw-market-daily-earnings-recap-run-once FORCE=1` -> clean `no_reporters`
- Acceptance snippets (artifact/debug):
  - footer: `Data UTC: ... | Sources: Yahoo fast_info + Yahoo news + web search`
  - INTC line: `- 📈 INTC +5.7% — Likely positioning/flow; no single confirmed catalyst.`
  - debug (`md debug INTC close` equivalent):
    - `links_keys: ['news', 'web']`
    - no `x` link key present
- Follow-up hardening applied post-merge on main:
  - expanded quote-wrapper rejection to include `why ... stock/shares ... today|now` phrasing for reason-line safety.

## Update (2026-02-24, integrator merge/deploy of Market Daily grammar hardening)
- Merged `origin/codex/agent-market-daily` into `main` at merge commit `6c5f51d`, including role commit `94b8180`.
- Integrated scope:
  - no-X MD output policy
  - quote-directory wrapper rejection for catalyst phrases
  - grammar hardening with hybrid polish + aggressive fallback diagnostics
- Validation:
  - checklist `python3 -m pytest` commands failed on mini (`No module named pytest`)
  - venv validation passed:
    - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_market_daily.py` -> `53 passed`
    - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Runtime verification:
  - `make openclaw-restart`
  - `make openclaw-slack-status` -> probe `ok=true`
  - `make openclaw-market-daily-run-once FORCE=1` -> posted (`md-close-20260225-031016.md`)
  - `make openclaw-market-daily-earnings-recap-run-once FORCE=1` -> `no_reporters`
- Acceptance snippets:
  - footer: `Sources: Yahoo fast_info + Yahoo news + web search`
  - INTC line: `Likely positioning/flow; no single confirmed catalyst.`
  - AMD line: `Shares rose after Meta inks deal with AMD for chips—and equity.`
  - `debug-catalyst INTC --slot close` keys include `cause_render_mode=fallback`, `cause_raw_phrase`, `cause_final_phrase`; links are only `news`/`web`.
- Rollback flag:
  - `COATUE_CLAW_MD_REASON_QUALITY_MODE=deterministic` was **not** required.

## Update (2026-02-24, integrator deploy of Market Daily simple catalyst synthesis)
- Merged `origin/codex/agent-market-daily` into `main` at merge commit `c862aaf` (includes `f71eaa1`).
- Feature now active by default: `simple_synthesis` catalyst mode (Google+Yahoo top-5 evidence with one-line LLM cause and aggressive best-guess fallback).
- Validation:
  - checklist `python3 -m pytest` failed on mini due missing global pytest / Python 3.9 compatibility (`datetime.UTC` import).
  - venv validation passed:
    - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_market_daily.py` -> `56 passed`
    - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Status confirms new defaults:
  - `catalyst_mode='simple_synthesis'`
  - `synth_max_results=5`
  - `synth_source_mode='google_plus_yahoo'`
  - `synth_domain_gate='soft'`
  - `synth_force_best_guess=True`
- Runtime verification:
  - `make openclaw-restart` and `make openclaw-slack-status` probe healthy (`ok=true`).
  - forced MD close post succeeded (`md-close-20260225-041445.md`).
  - forced earnings recap returned clean `no_reporters`.
- Acceptance snippets:
  - footer: `Sources: Yahoo fast_info + Yahoo news + web search`
  - INTC line is causal sentence (no quote-directory wrapper text).
  - no `[X]` links or X footer text.
  - debug INTC includes synth diagnostics:
    - `synth_generation_mode='simple_synthesis'`
    - `synth_candidates_considered` / `synth_candidates_used` / `synth_chosen_urls`
    - `cause_render_mode='simple_best_guess'`
- Rollback flag was not needed. Legacy rollback remains available via:
  - `COATUE_CLAW_MD_CATALYST_MODE=legacy_heuristic`

## Update (2026-02-24, Market Daily time-integrity guardrails merged/deployed)
- Integrated `origin/codex/agent-market-daily` into `main` via merge commit `d59ca97` (includes `43c84ed`).
- Scope now active:
  - strict in-window evidence requirement
  - historical callback rejection
  - publish-time enrichment + timeout guardrails
  - link emission constrained to time-valid evidence
- Status verification (runtime interpreter):
  - `require_in_window_dates=True`
  - `allow_undated_fallback=False`
  - `reject_historical_callback=True`
  - `publish_time_enrich_enabled=True`
  - `publish_time_enrich_timeout_ms=1200`
- Validation:
  - checklist `python3 -m pytest` fails on mini (missing pytest + Python 3.9 runtime mismatch for `datetime.UTC` imports).
  - venv tests pass:
    - `tests/test_market_daily.py` -> `63 passed`
    - `tests/test_launchd_runtime.py` -> `8 passed`
- Runtime verification:
  - `make openclaw-restart` + `make openclaw-slack-status` healthy
  - forced close run posted (`md-close-20260225-044033.md`)
  - forced earnings recap run returned `no_reporters`
- INTC debug checks passed:
  - stale Morgan callback rejected (`historical_callback_reject` present)
  - chosen synthesis URLs include in-window Intel Yahoo `.../why-intel-intc-stock-soaring-210238819.html` plus another in-window source
  - debug arrays are non-empty:
    - `publish_time_rejections`
    - `historical_callback_rejections`
    - `candidate_publish_times`
  - links payload only contains `news`/`web`.
- Integrator follow-up applied post-merge:
  - removed over-broad quote-wrapper rule that rejected valid in-window `why ... stock soaring today` candidates.

## Update (2026-02-25, market-daily catalyst quality recovery merge + deploy)
- Integrated role branch `origin/codex/agent-market-daily` into `main` using:
  - merge commit: `bee2d68` (`Merge market-daily catalyst quality recovery`)
  - included role patch: `c8736fa`
- Validation run:
  - checklist commands with system `python3` failed (`No module named pytest` on mini)
  - runtime-equivalent venv checks passed:
    - `tests/test_market_daily.py` -> `69 passed`
    - `tests/test_launchd_runtime.py` -> `8 passed`
- Runtime/ops:
  - `make openclaw-restart`
  - `make openclaw-slack-status` probe healthy (`ok=true`)
  - forced runs posted:
    - close artifact: `/opt/coatue-claw-data/artifacts/market-daily/md-close-20260225-051636.md`
    - recap artifact: `/opt/coatue-claw-data/artifacts/market-daily/md-earnings-recap-20260225-051342.md`
- Slack verification results:
  - no `[X]` links and no `X recent search` footer
  - sources footer remains no-X (`Yahoo fast_info + Yahoo news + web search`)
  - stale Morgan callback is rejected for INTC (`historical_callback_reject`)
  - with SERP key missing simulation:
    - `rejected_reasons` includes `web:google_serp_required_missing`
    - no DDG fallback note in simple synthesis
  - recap footer uses `Google web + Yahoo news evidence` (no X)
- Post-merge hardening on `main`:
  - simple synthesis now prefers linking the top selected in-window evidence URL for the chosen source, so low-quality roundup links do not outrank cleaner causal explainers in rendered links.
  - this produced INTC web link to Yahoo in-window explainer (`.../why-intel-intc-stock-soaring-210238819.html`) in latest debug checks.
- Quick fallback simulation (LLM unavailable + weak evidence):
  - `OPENAI_API_KEY=""` + missing SERP key => line falls back to `Likely positioning/flow; no single confirmed catalyst.`

## Update (2026-02-25, MD anchor-first free-sentence catalyst labeling runbook)
- Main already contains role-branch merge commit `d8bf379` (`Merge MD anchor-first free-sentence catalyst labeling`), including role patch `8193c58`.
- Validation on Mac mini:
  - runbook `python3 -m pytest` commands fail on host interpreter (`No module named pytest`).
  - runtime-equivalent checks pass via venv:
    - `tests/test_market_daily.py` -> `71 passed`
    - `tests/test_launchd_runtime.py` -> `8 passed`
- Runtime orchestration:
  - `make openclaw-restart` completed.
  - first Slack probe returned transient gateway close (`1006`), subsequent probe healthy (`ok=true`, `status=200`).
  - forced runs posted:
    - close artifact: `/opt/coatue-claw-data/artifacts/market-daily/md-close-20260225-062530.md`
    - earnings recap artifact: `/opt/coatue-claw-data/artifacts/market-daily/md-earnings-recap-20260225-062849.md`
- Slack/diagnostic verification:
  - close mover lines are free-sentence style and do not contain malformed `after According to ...` fragments in this run.
  - no `[X]` links and no X footer text.
  - close footer: `Sources: Yahoo fast_info + Yahoo news + web search`.
  - `debug-catalyst INTC --slot close` includes:
    - `cause_anchor_url`
    - `cause_support_urls`
    - `generation_format=free_sentence`
    - `generation_policy=post_as_is`
  - INTC stale Morgan callback is rejected under time-integrity gates.
- Production env defaults confirmed in `.env.prod`:
  - `COATUE_CLAW_MD_REASON_OUTPUT_MODE=free_sentence`
  - `COATUE_CLAW_MD_SYNTH_SUPPORT_COUNT=2`
  - `COATUE_CLAW_MD_POST_AS_IS=1`
- Follow-up note:
  - debug output still shows INTC rendered `links.web` on a support URL while anchor/source URL is Yahoo in-window; quality is improved but link-to-anchor strictness may need one more deterministic tie-break rule.

## Update (2026-02-25, Market Daily consensus-event + no-attribution catalyst correction)
- Implemented INTC catalyst correction in `src/coatue_claw/market_daily.py` on `main`:
  - replaced anchor-first sentence behavior with consensus-first winner selection (`_pick_consensus_winner`) over top synthesis candidates.
  - added deterministic event-family classification (`deal_partnership`, `pricing`, `guidance`, `analyst_move`, `regulatory`, `earnings`, `other`).
  - enforced no-publisher-attribution sentence output via `_strip_publisher_attribution`.
  - added sentence-family consistency guard: if generated line conflicts with consensus family, fallback to deterministic consensus-anchor sentence.
  - aligned support links to consensus family and preserved broad source ingestion.
- New debug observability fields shipped:
  - `consensus_event_family`
  - `consensus_winner_url`
  - `attribution_stripped`
- Tests:
  - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_market_daily.py` -> `72 passed`
  - `PYTHONPATH=src /opt/coatue-claw/.venv/bin/python -m pytest -q tests/test_launchd_runtime.py` -> `8 passed`
- Runtime checks:
  - `make openclaw-restart`
  - `make openclaw-slack-status` healthy (`ok=true`, status `200`) after restart
  - `make openclaw-market-daily-run-once FORCE=1` posted close artifact:
    - `/opt/coatue-claw-data/artifacts/market-daily/md-close-20260225-071156.md`
  - `make openclaw-market-daily-earnings-recap-run-once FORCE=1` posted recap artifact:
    - `/opt/coatue-claw-data/artifacts/market-daily/md-earnings-recap-20260225-071334.md`
- INTC verification snippets (post-fix):
  - close line now uses catalyst-only partnership framing (no Reuters attribution):
    - `Intel shares rose about 5.7% after the company announced a multiyear AI partnership with SambaNova ...`
  - close footer remains no-X:
    - `Sources: Yahoo fast_info + Yahoo news + web search`
  - debug INTC close:
    - `consensus_event_family=deal_partnership`
    - `consensus_winner_url=https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/`
    - `cause_anchor_url` matches consensus winner URL
    - `generation_format=free_sentence`, `generation_policy=post_as_is`
- Recap footer remains no-X:
  - `Sources: Yahoo earnings calendar/history + Yahoo fast_info + Google web + Yahoo news evidence`

## Update (2026-02-25, integrator cherry-pick: market-daily earnings recap anchor-first rewrite)
- Integrator action used market-daily-only path (no full branch merge):
  - `git cherry-pick 58c8258`
  - resulting commit on `main`: `0f78583` (`market-daily: rewrite earnings recap to anchor-first end-to-end`)
- Cherry-pick conflict resolution summary:
  - kept `main` continuity in handoff docs during conflict resolution.
  - resolved `src/coatue_claw/market_daily.py` conflict and retained current mover consensus/no-attribution behavior while accepting recap rewrite.
  - follow-up fix applied post-cherry-pick so recap keeps anchor+support citation ordering (without applying mover-family filter to recap rows).
- Validation status:
  - checklist host `python3` commands failed on mini (`No module named pytest`; Python 3.9 `datetime.UTC` import mismatch).
  - runtime-equivalent validation passed on venv:
    - `tests/test_market_daily.py` -> `75 passed`
    - `tests/test_launchd_runtime.py` -> `8 passed`
- Runtime ops:
  - `make openclaw-restart`
  - `make openclaw-slack-status` healthy after restart (`ok=true`, status `200`)
  - `make openclaw-market-daily-earnings-recap-run-once FORCE=1` posted:
    - `/opt/coatue-claw-data/artifacts/market-daily/md-earnings-recap-20260225-180006.md`
  - `make openclaw-slack-logs` captured current channel runtime logs.
- Acceptance check snapshot (latest forced recap run):
  - Recap posted only when reporters existed (`reporters=3`).
  - Each ticker had 3 bullets (within required 2–4).
  - No X links/sources appeared in footer or content.
  - Deterministic backup remained coherent when LLM unavailable (`recap_generation_mode=deterministic_backup`).
  - Limitation in this run: no source evidence rows were available (`evidence: none`), so `[S1]/[S2]` citation handles and `Sources:` URL-handle mapping were not present for ticker bullets.
- Integrator note:
  - dry-run recap invocation in this environment can stall intermittently; force-run path completed and was used for production verification.

## Update (2026-02-25, board-seat company-only target enforcement)
- Implemented global company-only target resolution in `src/coatue_claw/board_seat_daily.py` on `codex/agent-board-seat`.
- New behavior:
  - `Idea` targets are resolved as company entities before gating/render.
  - deterministic alias mapping defaults include `next.js -> Vercel` (extendable via env JSON).
  - non-company product shapes are rejected/retargeted via alias, then rotation/default fallback.
  - governance gates remain unchanged (new-target requirement, confidence policy, 14-day no-repeat, repitch significance).
- New observability in run payload rows:
  - `target_original`
  - `target_resolution_reason` (`as_extracted`, `alias_mapped`, `fallback_rotation`, `fallback_default`, `invalid_after_resolution`)
- Additional implementation details:
  - company-target requirement env reader: `COATUE_CLAW_BOARD_SEAT_REQUIRE_COMPANY_TARGET` (default on)
  - alias map env reader: `COATUE_CLAW_BOARD_SEAT_TARGET_COMPANY_ALIAS_JSON`
  - source selection in sanitize path now uses resolved `idea_line` so target-confidence/source classification aligns with final company target.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `55 passed`

## Update (2026-02-25, board-seat line truncation cleanup)
- Fixed Board Seat line normalization so strict word-cap clipping no longer leaves partial second-sentence fragments (example fixed: `... Vercel. Migrate`).
- Implementation:
  - added sentence-tail cleanup in `_limit_words` to trim incomplete short trailing sentence fragments.
  - preserved existing word-cap behavior and all governance gates.
- Validation:
  - `PYTHONPATH=src python3 -m pytest -q tests/test_board_seat_daily.py` -> `57 passed`

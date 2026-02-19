# Live Session Handoff (Coatue Claw)

## Objective
Ship valuation charting into the OpenClaw-native Slack workflow.

## Current Status (2026-02-19)
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
  - Drive mirror root is now set to `/Users/spclaw/Documents/Google Drive Local` for Mac mini sync
  - Spencer-facing category subfolders are provisioned under `01_DROP_HERE_Incoming`, `02_READ_ONLY_Latest_AUTO`, and `03_READ_ONLY_Archive_AUTO` (Companies, Sectors, Themes, Earnings, Filings, Transcripts, Decks, Models, Notes, Calls, Macro, Admin, Misc)
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
  - File bridge Drive root configured + validated on Mac mini (`9db4643` + latest pull): `make openclaw-files-init`, `make openclaw-files-sync`, and `make openclaw-files-status` pass using `/Users/spclaw/Documents/Google Drive Local`.
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
- Email diligence response formatting upgrade (commit pending in this session):
  - replaced raw markdown preview dump with readable executive summary body
  - added HTML email alternative for cleaner rendering in Gmail
  - attached full memo as `.md` file to preserve complete report
  - tests added for readable summary + attachment contract (`tests/test_email_gateway.py`)

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
1. Run all Slack validation prompts above in `#charting`.
2. Validate plain-English settings commands in Slack:
   - `@Coatue Claw show my settings`
   - `@Coatue Claw going forward look for 12 peers`
   - `@Coatue Claw use market cap as the default x-axis`
   - `@Coatue Claw when you finish a chart, ask us if we want ticker changes`
3. Validate `@Coatue Claw promote current settings` commits/pushes to `main` and reports commit hash in-thread.
4. Validate `@Coatue Claw undo last promotion` produces a revert commit and restarts runtime.
5. Validate Slack deploy pipeline in `#claw-lab`:
   - `@Coatue Claw deploy latest`
   - `@Coatue Claw run checks`
   - `@Coatue Claw show pipeline status`
   - `@Coatue Claw show deploy history`
6. Validate diligence memo output in Slack:
    - `@Coatue Claw diligence SNOW`
    - `@Coatue Claw dilligence MDB` (typo alias path)
    - confirm memo includes all required neutral sections plus source/timestamp attribution
    - confirm memo includes local database precheck summary and local report references when available
7. Validate memory flows in Slack:
   - `@Coatue Claw remember my daughter's birthday is June 3rd`
   - `@Coatue Claw what is my daughter's birthday?`
   - `@Coatue Claw memory status`
   - `@Coatue Claw memory checkpoint`
8. Configure `SLACK_PIPELINE_ADMINS` and optional `COATUE_CLAW_SLACK_BUILD_COMMAND` in runtime env for production permissions/runner control.
9. Validate 24/7 persistence after next Mac mini reboot:
   - run `make openclaw-24x7-status`
   - confirm both services auto-restart as `loaded=true` and `state=running`
10. Confirm Spencer has Drive access to `/Users/spclaw/Documents/Google Drive Local` and can drag/drop by category under `01_DROP_HERE_Incoming/*`.
11. Validate end-to-end category workflow with Spencer:
    - Spencer drops a file into `01_DROP_HERE_Incoming/<Category>`
    - run `make openclaw-files-sync-pull`
    - confirm file appears in `/opt/coatue-claw-data/files/incoming/<Category>`
12. Validate Slack upload ingestion with Spencer:
    - Spencer uploads a file in Slack (without bot mention)
    - confirm bot thread ack shows routed category
    - confirm file appears in `/opt/coatue-claw-data/files/incoming/<Category>`
    - confirm record exists in `/opt/coatue-claw-data/db/file_ingest.sqlite`
13. Configure email env vars in `/opt/coatue-claw/.env.prod` and validate:
    - `make openclaw-email-status`
    - `make openclaw-email-run-once`
    - send test email `diligence SNOW` and confirm reply
14. Wire first scheduled jobs (weekly idea scan + X digest).
15. If response fails, capture first failing line with `openclaw channels logs --channel slack --lines 300`.

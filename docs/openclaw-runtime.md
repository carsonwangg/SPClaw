# OpenClaw Runtime Guide

## Purpose
Define the runtime contract for Coatue Claw on OpenClaw, including process roles, operational controls, storage boundaries, and validation expectations.

## Runtime Source of Truth
- Gateway config: `~/.openclaw/openclaw.json`
- Gateway service: `~/Library/LaunchAgents/ai.openclaw.gateway.plist`
- Gateway logs: `/tmp/openclaw/openclaw-YYYY-MM-DD.log`
- App repo: `/opt/coatue-claw`
- Runtime data: `/opt/coatue-claw-data`
- File management runbook: `docs/file-management-system.md`

## Execution Model
- OpenClaw gateway is the long-running control plane and Slack channel transport.
- Slack bot handling is event-driven in `src/coatue_claw/slack_bot.py` and runs under OpenClaw channel delivery.
- CLI entrypoint is `claw` (`src/coatue_claw/cli.py`) for on-demand operations (valuation charts, diligence packets).
- Chart generation engine is `src/coatue_claw/valuation_chart.py`.

## Job Classes
- Long-running:
  - OpenClaw gateway process
  - Slack channel event handling
- On-demand:
  - `claw valuation-chart ...`
  - `claw diligence ...`
  - `claw memory status|query|prune|extract-daily|checkpoint`
- Scheduled (planned but not yet wired in this repo):
  - Weekly idea scan
  - X digest generation
  - Hourly memory prune

Diligence output contract:
- `claw diligence TICKER` generates a neutral, evidence-first 8-section investment memo with source/timestamp attribution.
- Memo source baseline is Yahoo Finance via yfinance (profile, financial statements, valuation/balance-sheet metrics, and recent reporting metadata).

Memory output contract:
- Primary memory store: SQLite + FTS5 (`/opt/coatue-claw-data/db/memory.sqlite`).
- Structured facts are stored as `category/entity/key/value/rationale/source/timestamp` rows with decay tiers.
- Decay tiers:
  - `permanent` (no expiry)
  - `stable` (90 days, refresh on access)
  - `active` (14 days, refresh on access)
  - `session` (24 hours)
  - `checkpoint` (4 hours)
- Checkpoints are written before risky pipeline operations (`deploy_latest`, `undo_last_deploy`, `build_request`).
- Semantic fallback is optional via LanceDB/OpenAI embeddings when configured.

## Secrets and Environment Contract
- Production secrets live only in `/opt/coatue-claw/.env.prod`.
- Do not commit secrets to git.
- Slack runtime requires:
  - `SLACK_BOT_TOKEN`
  - `SLACK_SIGNING_SECRET`
  - `SLACK_APP_TOKEN`
  - bot OAuth scope `files:read` for automatic Slack file ingest

## Operational Commands
- OpenClaw binary resolution:
  - `Makefile` prepends `/opt/homebrew/bin` to PATH for non-login SSH shells.
  - Targets auto-resolve `openclaw` from PATH, with fallback to `/opt/homebrew/bin/openclaw`.
- Runtime health:
  - `make openclaw-dev` (gateway + Slack status check)
  - `make openclaw-status`
  - `make openclaw-restart`
  - `make openclaw-logs`
- Bot-specific:
  - `make openclaw-bot-status`
  - `make openclaw-bot-logs`
- Scheduler status:
  - `make openclaw-schedulers-status`
  - `make openclaw-memory-status`
  - `make openclaw-memory-prune`
  - `make openclaw-memory-extract-daily DAYS=14`
  - `make openclaw-files-init`
  - `make openclaw-files-status`
  - `make openclaw-files-sync-pull`
  - `make openclaw-files-sync-push`
  - `make openclaw-files-sync`
  - `make openclaw-files-index`
- Slack diagnostics:
  - `make openclaw-slack-status`
  - `make openclaw-slack-probe`
  - `make openclaw-slack-logs`
  - `make openclaw-slack-audit`

## Artifact Contract
- Chart/data artifacts are written to `/opt/coatue-claw-data/artifacts/charts/`:
  - `valuation-scatter-*.png`
  - `valuation-scatter-*.csv`
  - `valuation-scatter-*.json`
  - `valuation-scatter-*-raw.json`
- Every generated insight must retain source attribution and timestamps.

File bridge contract:
- Local canonical bot-managed paths:
  - `/opt/coatue-claw-data/files/working`
  - `/opt/coatue-claw-data/files/archive`
  - `/opt/coatue-claw-data/files/published`
  - `/opt/coatue-claw-data/files/incoming`
- Shared mirror paths (Google Drive or local fallback) are configured in `config/file-bridge.json`.
- `openclaw-files-sync` performs pull (`01_DROP_HERE_Incoming` -> local incoming), push (`published/archive` -> shared paths), and index regeneration.
- `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` is auto-mirrored from published outputs and excluded from pull ingestion.
- Published index artifacts are generated at:
  - `/opt/coatue-claw-data/files/published/index.json`
  - `/opt/coatue-claw-data/files/published/index.md`
- Slack file ingest contract:
  - Slack message/app_mention events with file attachments trigger automatic file intake.
  - Files are downloaded with bot token auth and categorized into `incoming/<Category>`.
  - Files are mirrored into Drive `01_DROP_HERE_Incoming/<Category>` for shared visibility.
  - Intake metadata is stored in `/opt/coatue-claw-data/db/file_ingest.sqlite`.

## Runtime Settings Contract
- Live Slack-configurable settings are stored outside git:
  - `/opt/coatue-claw-data/db/runtime-settings.json`
- Change audits are written to:
  - `/opt/coatue-claw-data/artifacts/config-audit/*.md`
- Git-tracked baseline defaults are stored in:
  - `/opt/coatue-claw/config/runtime-defaults.json`
- Promotion ledger is stored in:
  - `/opt/coatue-claw-data/db/settings-promotions.json`

Slack conversational controls:
1. `@Coatue Claw show my settings`
2. `@Coatue Claw going forward look for 12 peers`
3. `@Coatue Claw use market cap as the default x-axis`
4. `@Coatue Claw promote current settings` (direct commit/push to `main`)
5. `@Coatue Claw undo last promotion` (auto-revert of last promoted settings commit)

Slack deploy pipeline controls:
1. `@Coatue Claw deploy latest` (pull + restart + Slack health probe)
2. `@Coatue Claw undo last deploy` (revert last deploy target + push + restart + probe)
3. `@Coatue Claw run checks` (`PYTHONPATH=src pytest -q`)
4. `@Coatue Claw show pipeline status`
5. `@Coatue Claw show deploy history`
6. `@Coatue Claw build: <request>` (runs Codex CLI by default if installed, otherwise requires custom runner)

Pipeline environment controls:
- `SLACK_PIPELINE_ADMINS`: optional comma-separated Slack user IDs allowed to run pipeline commands
- `COATUE_CLAW_SLACK_BUILD_COMMAND`: optional custom build command template with `{request}` placeholder
- `COATUE_CLAW_DEPLOY_HISTORY_PATH`: optional deploy history JSON path (default under `/opt/coatue-claw-data/db/`)

Memory environment controls:
- `COATUE_CLAW_MEMORY_DB_PATH`: optional SQLite memory DB path
- `COATUE_CLAW_MEMORY_VECTOR_DIR`: optional LanceDB directory path
- `COATUE_CLAW_MEMORY_EMBED_MODEL`: optional embedding model (default `text-embedding-3-small`)
- `OPENAI_API_KEY`: required only for semantic memory fallback

File ingest environment controls:
- `COATUE_CLAW_FILE_INGEST_DB_PATH`: optional override for Slack file ingest SQLite path
- `COATUE_CLAW_SLACK_FILE_MAX_MB`: optional max Slack file size in MB for auto-ingest (default `50`)

## Slack Validation Checklist
1. `make openclaw-slack-status` reports `running=true` and successful probe status.
2. `make openclaw-slack-logs` shows active Slack connection.
3. `lastInboundAt` updates after a real Slack mention.
4. `lastOutboundAt` updates after a bot reply.
5. Chart requests return as-of timestamps, provider used/fallback reason, and expected artifact uploads.

## Incident Triage
1. Restart runtime: `make openclaw-restart`.
2. Re-check health: `make openclaw-dev`.
3. Capture failure evidence: `openclaw channels logs --channel slack --lines 300`.
4. Record issue and next action in both handoff docs before ending session.

## Important Notes
- Do not run parallel Slack Socket Mode consumers with the same app token.
- Keep OpenClaw Slack channel as the primary production delivery path.
- If `lastInboundAt` remains `null`, verify Slack event subscriptions and workspace installation.

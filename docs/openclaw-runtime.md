# OpenClaw Runtime Guide

## Purpose
Define the runtime contract for Coatue Claw on OpenClaw, including process roles, operational controls, storage boundaries, and validation expectations.

## Runtime Source of Truth
- Gateway config: `~/.openclaw/openclaw.json`
- Gateway service: `~/Library/LaunchAgents/ai.openclaw.gateway.plist`
- Coatue 24/7 services:
  - `~/Library/LaunchAgents/com.coatueclaw.email-gateway.plist`
  - `~/Library/LaunchAgents/com.coatueclaw.memory-prune.plist`
  - `~/Library/LaunchAgents/com.coatueclaw.memory-reconcile-export.plist`
  - `~/Library/LaunchAgents/com.coatueclaw.x-chart-daily.plist`
  - `~/Library/LaunchAgents/com.coatueclaw.board-seat-daily.plist`
  - `~/Library/LaunchAgents/com.coatueclaw.spencer-change-digest.plist`
  - `~/Library/LaunchAgents/com.coatueclaw.market-daily.plist`
- Gateway logs: `/tmp/openclaw/openclaw-YYYY-MM-DD.log`
- App repo: `/opt/coatue-claw`
- Runtime data: `/opt/coatue-claw-data`
- File management runbook: `docs/file-management-system.md`
- Email integration runbook: `docs/email-integration.md`

## Execution Model
- OpenClaw gateway is the long-running control plane and Slack channel transport.
- Slack bot handling is event-driven in `src/coatue_claw/slack_bot.py` and runs under OpenClaw channel delivery.
- CLI entrypoint is `claw` (`src/coatue_claw/cli.py`) for on-demand operations (valuation charts, diligence packets).
- Chart generation engine is `src/coatue_claw/valuation_chart.py`.

## Job Classes
- Long-running:
  - OpenClaw gateway process
  - Slack channel event handling
  - Optional email poller (`coatue_claw.email_gateway`)
- On-demand:
  - `claw valuation-chart ...`
  - `claw diligence ...`
  - `claw x-digest "QUERY" --hours 24 --limit 50`
  - `claw x-chart run-once --manual`
  - `claw x-chart status|list-sources|add-source`
  - `python -m coatue_claw.board_seat_daily run-once|status|target-memory|seed-target|export-ledger`
  - `claw market-daily run-once --manual|--force|--dry-run`
  - `claw market-daily status|holdings|refresh-coatue-holdings|debug-catalyst`
  - `claw memory status|query|prune|extract-daily|checkpoint`
- Scheduled (planned but not yet wired in this repo):
  - Weekly idea scan
- Scheduled (wired):
  - Hourly memory prune via `launchd` (`com.coatueclaw.memory-prune`)
  - Memory reconcile queue export via `launchd` (`com.coatueclaw.memory-reconcile-export`) every 15 minutes by default (`COATUE_CLAW_MEMORY_RECONCILE_INTERVAL_SECONDS=900`)
  - Chart scout via `launchd` (`com.coatueclaw.x-chart-daily`) every hour (`StartInterval=3600` default), with posting gated by `COATUE_CLAW_X_CHART_WINDOWS` (default `09:00,12:00,18:00`)
  - Board Seat daily post via `launchd` (`com.coatueclaw.board-seat-daily`) at `COATUE_CLAW_BOARD_SEAT_TIME` (default `08:30`)
  - Daily Spencer change-request digest DM via `launchd` (`com.coatueclaw.spencer-change-digest`) at `COATUE_CLAW_SPENCER_CHANGE_DIGEST_TIME` (default `18:00`)
  - MD (Market Daily) via `launchd` (`com.coatueclaw.market-daily`) on US weekdays at `COATUE_CLAW_MD_TIMES` (default `07:00,14:15`, local PT runtime)
  - Chart scout posts upload the source chart image snip from the selected X post (no redraw/reconstruction step)

Diligence output contract:
- `claw diligence TICKER` generates a neutral, evidence-first 8-section investment memo with source/timestamp attribution.
- Memo source baseline is Yahoo Finance via yfinance (profile, financial statements, valuation/balance-sheet metrics, and recent reporting metadata).
- Before external fetches, diligence checks local research artifacts first (`file_ingest.sqlite` + prior packet markdowns) and includes matches in memo sources.

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
  - `make openclaw-24x7-enable`
  - `make openclaw-24x7-status`
  - `make openclaw-24x7-disable`
  - `make openclaw-memory-status`
  - `make openclaw-memory-prune`
  - `make openclaw-memory-extract-daily DAYS=14`
  - `make openclaw-memory-reconcile-status`
  - `make openclaw-memory-reconcile-export LIMIT=200`
  - `make openclaw-files-init`
  - `make openclaw-files-status`
  - `make openclaw-files-sync-pull`
  - `make openclaw-files-sync-push`
  - `make openclaw-files-sync`
  - `make openclaw-files-index`
  - `make openclaw-email-status`
  - `make openclaw-email-run-once`
  - `make openclaw-email-serve`
  - `make openclaw-x-chart-status`
  - `make openclaw-x-chart-run-once`
  - `make openclaw-x-chart-sources`
  - `make openclaw-spencer-digest-status`
  - `make openclaw-spencer-digest-run-once`
  - `make openclaw-market-daily-status`
  - `make openclaw-market-daily-run-once`
  - `make openclaw-market-daily-refresh-holdings`

24/7 runtime bootstrap on Mac mini:
1. `cd /opt/coatue-claw`
2. `git pull --ff-only origin main`
3. `make openclaw-restart`
4. `make openclaw-24x7-enable`
5. `make openclaw-24x7-status`
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
- Default Mac mini Drive root is `/Users/spclaw/Documents/SPClaw Database`.
- `openclaw-files-sync` performs pull (`01_DROP_HERE_Incoming` -> local incoming), push (`published/archive` -> shared paths), and index regeneration.
- `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` is auto-mirrored from published outputs and excluded from pull ingestion.
- Canonical category set is simplified to `Universes`, `Companies`, and `Industries`.
- Published index artifacts are generated at:
  - `/opt/coatue-claw-data/files/published/index.json`
  - `/opt/coatue-claw-data/files/published/index.md`
- Slack file ingest contract:
  - Slack message/app_mention events with file attachments trigger automatic file intake.
  - Files are downloaded with bot token auth and categorized into `incoming/{Universes|Companies|Industries}`.
  - Files are mirrored into Drive `01_DROP_HERE_Incoming/{Universes|Companies|Industries}` for shared visibility.
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
7. `@Coatue Claw x digest SNOW last 24h limit 80`
8. `@Coatue Claw x status`
9. `x chart now`
10. `x chart sources`
11. `x chart add @fiscal_AI priority 1.6`
12. `x chart help`

Pipeline environment controls:
- `SLACK_PIPELINE_ADMINS`: optional comma-separated Slack user IDs allowed to run pipeline commands
- `COATUE_CLAW_SLACK_BUILD_COMMAND`: optional custom build command template with `{request}` placeholder
- `COATUE_CLAW_DEPLOY_HISTORY_PATH`: optional deploy history JSON path (default under `/opt/coatue-claw-data/db/`)

Memory environment controls:
- `COATUE_CLAW_MEMORY_DB_PATH`: optional SQLite memory DB path
- `COATUE_CLAW_MEMORY_VECTOR_DIR`: optional LanceDB directory path
- `COATUE_CLAW_MEMORY_EMBED_MODEL`: optional embedding model (default `text-embedding-3-large`)
- `COATUE_CLAW_MEMORY_RECONCILE_INTERVAL_SECONDS`: scheduler interval for memory queue export (default `900`, min `300`, max `86400`)
- `COATUE_CLAW_MEMORY_RECONCILE_EXPORT_LIMIT`: queue export limit used by scheduler (default `200`, min `1`, max `1000`)
- `OPENAI_API_KEY`: required only for semantic memory fallback

File ingest environment controls:
- `COATUE_CLAW_FILE_INGEST_DB_PATH`: optional override for Slack file ingest SQLite path
- `COATUE_CLAW_SLACK_FILE_MAX_MB`: optional max Slack file size in MB for auto-ingest (default `50`)

X digest environment controls:
- `COATUE_CLAW_X_BEARER_TOKEN`: required for X API requests
- `COATUE_CLAW_X_API_BASE`: optional API base URL override (default `https://api.x.com`)
- `COATUE_CLAW_X_DIGEST_DIR`: optional digest markdown output dir (default `/opt/coatue-claw-data/artifacts/x-digest`)

X chart scout environment controls:
- `COATUE_CLAW_X_CHART_SLACK_CHANNEL`: required Slack destination (channel id like `C...` or channel name like `#charting`)
- `COATUE_CLAW_X_CHART_TIMEZONE`: posting timezone (default `America/Los_Angeles`)
- `COATUE_CLAW_X_CHART_WINDOWS`: comma-separated daily times (default `09:00,12:00,18:00`)
- `COATUE_CLAW_X_CHART_SOURCE_LIMIT`: number of tracked source handles to scan each run (default `25`)
- `COATUE_CLAW_X_CHART_DISCOVERY_MIN_ENGAGEMENT`: minimum engagement for auto-discovered source promotion (default `120`)
- `COATUE_CLAW_X_CHART_DB_PATH`: optional SQLite store path (default `/opt/coatue-claw-data/db/x_chart_daily.sqlite`)
- `COATUE_CLAW_X_CHART_DIR`: optional markdown artifact output dir (default `/opt/coatue-claw-data/artifacts/x-chart-daily`)
- `COATUE_CLAW_VISUALCAPITALIST_FEED_URL`: optional feed override (default `https://www.visualcapitalist.com/feed/`)

MD (Market Daily) environment controls:
- `COATUE_CLAW_MD_SLACK_CHANNEL`: Slack destination channel id/name (default `general`)
- `COATUE_CLAW_MD_TZ`: slot timezone (default `America/Los_Angeles`)
- `COATUE_CLAW_MD_TIMES`: local run times (`HH:MM,HH:MM`, default `07:00,14:15`)
- `COATUE_CLAW_MD_TOP_N`: mover count (default `3`)
- `COATUE_CLAW_MD_TMT_TOP_K`: top-ranked seed members to keep before overlay (default `40`)
- `COATUE_CLAW_MD_CANDIDATE_SEED_PATH`: CSV universe seed path (default `/opt/coatue-claw/config/md_tmt_seed_universe.csv`)
- `COATUE_CLAW_MD_DB_PATH`: SQLite path (default `/opt/coatue-claw-data/db/market_daily.sqlite`)
- `COATUE_CLAW_MD_ARTIFACT_DIR`: markdown artifact output dir (default `/opt/coatue-claw-data/artifacts/market-daily`)
- `COATUE_CLAW_MD_COATUE_CIK`: Coatue CIK for auto 13F refresh
- `COATUE_CLAW_MD_OPENFIGI_API_KEY`: optional OpenFIGI key for stronger CUSIP->ticker resolution
- `COATUE_CLAW_MD_MODEL`: optional catalyst summarizer model (default `gpt-5.2-chat-latest`)
- `COATUE_CLAW_MD_MAX_LOOKBACK_HOURS`: max evidence lookback cap for session windows (default `96`)
- `COATUE_CLAW_MD_X_MAX_RESULTS`: X search depth for catalyst retrieval (default `50`)
- `COATUE_CLAW_MD_WEB_SEARCH_ENABLED`: enable web fallback retrieval (`1`/`0`, default `1`)
- `COATUE_CLAW_MD_WEB_SEARCH_BACKEND`: web backend (`google_serp` primary with `ddg_html` fallback, default `google_serp`)
- `COATUE_CLAW_MD_GOOGLE_SERP_API_KEY`: SERP API key for Google-backed evidence retrieval
- `COATUE_CLAW_MD_GOOGLE_SERP_ENDPOINT`: optional SERP endpoint override (default `https://serpapi.com/search.json`)
- `COATUE_CLAW_MD_WEB_MAX_RESULTS`: max web evidence links per ticker (default `20`)
- `COATUE_CLAW_MD_MIN_EVIDENCE_CONFIDENCE`: confidence threshold before fallback reason line (default `0.55`)
- `COATUE_CLAW_MD_MIN_CAUSE_SOURCES`: minimum independent corroborating sources required to name a specific cause (default `2`)
- `COATUE_CLAW_MD_MIN_CAUSE_DOMAINS`: minimum distinct corroborating domains required to name a specific cause (default `2`)
- `COATUE_CLAW_MD_ENABLE_CAUSE_CLUSTER_REUSE`: reuse one confirmed basket cause phrase across multiple movers in the same run (`1`/`0`, default `1`)
- `COATUE_CLAW_MD_GENERIC_HEADLINE_BLOCKLIST_ENABLED`: block generic wrappers (for example "stock is down today") from final catalyst lines (`1`/`0`, default `1`)
- `COATUE_CLAW_MD_REASON_MODE`: catalyst reasoning mode (default `best_effort`)
- `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED`: allow decisive single-source event phrasing when one high-quality source is dominant (`1`/`0`, default `1`)
- `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE`: minimum effective evidence score for decisive-primary override (default `0.60`)
- `COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN`: minimum top-cluster score gap vs runner-up for decisive-primary override (default `0.03`)
- Basket coherence rule: if a confirmed `anthropic_claude_cyber` cause is present for one cybersecurity mover in a run, peer cybersecurity selloff movers reuse that same cause phrase.
- MD fallback line (when corroboration gate fails): `Likely positioning/flow; no single confirmed catalyst.`

Board Seat daily environment controls:
- `COATUE_CLAW_BOARD_SEAT_PORTCOS`: comma-separated `Company:channel` mappings (default includes anduril/anthropic/cursor/neuralink/openai/physical-intelligence/ramp/spacex/stripe/sunday-robotics)
- `COATUE_CLAW_BOARD_SEAT_TIME`: local daily runtime time (`HH:MM`, default `08:30`)
- `COATUE_CLAW_BOARD_SEAT_TZ`: timezone for run date (default `America/Los_Angeles`)
- `COATUE_CLAW_BOARD_SEAT_LOOKBACK_HOURS`: Slack history window used for context (default `24`)
- `COATUE_CLAW_BOARD_SEAT_MAX_MESSAGES`: max context messages fetched per channel (default `160`)
- `COATUE_CLAW_BOARD_SEAT_DB_PATH`: optional SQLite path for daily run ledger
- `COATUE_CLAW_BOARD_SEAT_MODEL`: optional LLM model for synthesis (default `gpt-5.2-chat-latest`)
- `COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS`: hard target-memory lock window to prevent re-pitching same target (default `30`)
- `COATUE_CLAW_BOARD_SEAT_ALLOW_REPEAT_TARGETS`: set `1` to bypass target lock (default `0`)
- `COATUE_CLAW_BOARD_SEAT_LEDGER_DIR`: board-seat target ledger artifact directory (default `/opt/coatue-claw-data/artifacts/board-seat`)
- `COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_ENABLED`: mirror ledger to Google Drive local path (default `1`)
- `COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_PATH`: mirror destination path (default `/Users/spclaw/Documents/SPClaw Database/Companies/Board-Seat`)

Spencer change-digest environment controls:
- `COATUE_CLAW_CHANGE_TRACKER_USERS`: optional comma-separated `user_id:label` mappings for tracked requesters (example: `U0AGD28QSQG:Carson Wang,U0AFJ5RS31C:Spencer Peterson`)
- `COATUE_CLAW_SPENCER_USER_IDS`: comma-separated Slack user IDs treated as Spencer request sources
- `COATUE_CLAW_SPENCER_CHANGE_DB_PATH`: optional SQLite path for tracked Spencer requests
- `COATUE_CLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS`: comma-separated Slack user IDs to DM with daily open-request digest
- `COATUE_CLAW_SPENCER_CHANGE_DIGEST_TIME`: local daily digest time (`HH:MM`, default `18:00`)
- `COATUE_CLAW_SPENCER_CHANGE_DIGEST_TZ`: timezone used in digest header text (default `America/Los_Angeles`)

Email integration environment controls:
- `COATUE_CLAW_EMAIL_ENABLED`: set `true` to enable email processing
- `COATUE_CLAW_EMAIL_IMAP_HOST|PORT|USER|PASSWORD|MAILBOX`
- `COATUE_CLAW_EMAIL_SMTP_HOST|PORT|USER|PASSWORD`
- `COATUE_CLAW_EMAIL_FROM`: sender address used for replies
- `COATUE_CLAW_EMAIL_ALLOWED_SENDERS`: optional comma-separated allowlist
- `COATUE_CLAW_EMAIL_POLL_SECONDS`: poll cadence for `serve` mode (default `60`)
- `COATUE_CLAW_EMAIL_MAX_ATTACHMENT_MB`: max attachment size to ingest (default `25`)
- `COATUE_CLAW_EMAIL_DB_PATH`: optional SQLite DB path for email gateway logs

## Slack Validation Checklist
1. `make openclaw-slack-status` reports `running=true` and successful probe status.
2. `make openclaw-slack-logs` shows active Slack connection.
3. `lastInboundAt` updates after a real Slack mention.
4. `lastOutboundAt` updates after a bot reply.
5. Chart requests return as-of timestamps, provider used/fallback reason, and expected artifact uploads.
6. Each posted Chart of the Day includes a post-publish checklist review persisted in `x_chart_daily.sqlite` (`post_reviews` table).

## Incident Triage
1. Restart runtime: `make openclaw-restart`.
2. Re-check health: `make openclaw-dev`.
3. Capture failure evidence: `openclaw channels logs --channel slack --lines 300`.
4. Record issue and next action in both handoff docs before ending session.

## Important Notes
- Do not run parallel Slack Socket Mode consumers with the same app token.
- Keep OpenClaw Slack channel as the primary production delivery path.
- Slack default routing is enabled: plain channel messages are treated as OpenClaw requests unless they include an explicit `@user` mention.
- OpenClaw transport must also allow no-mention routing:
  - in `~/.openclaw/openclaw.json`, set `channels.slack.requireMention=false`
  - for channel overrides, set `channels.slack.channels.<channel_id>.requireMention=false`
- If `lastInboundAt` remains `null`, verify Slack event subscriptions and workspace installation.

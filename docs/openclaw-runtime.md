# OpenClaw Runtime Guide

## Purpose
Define the runtime contract for SPClaw on OpenClaw, including process roles, operational controls, storage boundaries, and validation expectations.

## Runtime Source of Truth
- Gateway config: `~/.openclaw/openclaw.json`
- Gateway service: `~/Library/LaunchAgents/ai.openclaw.gateway.plist`
- SPClaw 24/7 services:
  - `~/Library/LaunchAgents/com.spclaw.email-gateway.plist`
  - `~/Library/LaunchAgents/com.spclaw.memory-prune.plist`
  - `~/Library/LaunchAgents/com.spclaw.memory-reconcile-export.plist`
  - `~/Library/LaunchAgents/com.spclaw.x-chart-daily.plist`
  - `~/Library/LaunchAgents/com.spclaw.board-seat-daily.plist`
  - `~/Library/LaunchAgents/com.spclaw.spencer-change-digest.plist`
  - `~/Library/LaunchAgents/com.spclaw.market-daily.plist`
- Gateway logs: `/tmp/openclaw/openclaw-YYYY-MM-DD.log`
- App repo: `/opt/spclaw`
- Runtime data: `/opt/spclaw-data`
- File management runbook: `docs/file-management-system.md`
- Email integration runbook: `docs/email-integration.md`

## Execution Model
- OpenClaw gateway is the long-running control plane and Slack channel transport.
- Slack bot handling is event-driven in `src/spclaw/slack_bot.py` and runs under OpenClaw channel delivery.
- CLI entrypoint is `claw` (`src/spclaw/cli.py`) for on-demand operations (valuation charts, diligence packets).
- Chart generation engine is `src/spclaw/valuation_chart.py`.

## Job Classes
- Long-running:
  - OpenClaw gateway process
  - Slack channel event handling
  - Optional email poller (`spclaw.email_gateway`)
- On-demand:
  - `claw valuation-chart ...`
  - `claw diligence ...`
  - `claw x-digest "QUERY" --hours 24 --limit 50`
  - `claw x-chart run-once --manual`
  - `claw x-chart status|list-sources|add-source`
  - `python -m spclaw.board_seat_daily run-once|status|target-memory|seed-target|export-ledger|refresh-funding|funding-quality-report`
  - `claw market-daily run-once --manual|--force|--dry-run`
  - `claw market-daily run-earnings-recap --manual|--force|--dry-run`
  - `claw market-daily status|holdings|refresh-holdings|debug-catalyst`
  - `claw memory status|query|prune|extract-daily|checkpoint`
- Scheduled (planned but not yet wired in this repo):
  - Weekly idea scan
- Scheduled (wired):
  - Hourly memory prune via `launchd` (`com.spclaw.memory-prune`)
  - Memory reconcile queue export via `launchd` (`com.spclaw.memory-reconcile-export`) every 15 minutes by default (`SPCLAW_MEMORY_RECONCILE_INTERVAL_SECONDS=900`)
  - Chart scout via `launchd` (`com.spclaw.x-chart-daily`) every hour (`StartInterval=3600` default), with posting gated by `SPCLAW_X_CHART_WINDOWS` (default `07:00,12:00,18:00`)
  - Board Seat daily post via `launchd` (`com.spclaw.board-seat-daily`) at `SPCLAW_BOARD_SEAT_TIME` (default `08:30`)
  - Daily Spencer change-request digest DM via `launchd` (`com.spclaw.spencer-change-digest`) at `SPCLAW_SPENCER_CHANGE_DIGEST_TIME` (default `18:00`)
  - MD (Market Daily) via `launchd` (`com.spclaw.market-daily`) on US weekdays at `SPCLAW_MD_TIMES` (default `07:00,14:15`, local PT runtime)
  - MD earnings recap via `launchd` (`com.spclaw.market-daily-earnings-recap`) on US weekdays at `SPCLAW_MD_EARNINGS_RECAP_TIME` (default `19:00`, local PT runtime)
  - Chart scout posts upload the source chart image snip from the selected X post (no redraw/reconstruction step)

Diligence output contract:
- `claw diligence TICKER` generates a neutral, evidence-first 8-section investment memo with source/timestamp attribution.
- Memo source baseline is Yahoo Finance via yfinance (profile, financial statements, valuation/balance-sheet metrics, and recent reporting metadata).
- Before external fetches, diligence checks local research artifacts first (`file_ingest.sqlite` + prior packet markdowns) and includes matches in memo sources.

Memory output contract:
- Primary memory store: SQLite + FTS5 (`/opt/spclaw-data/db/memory.sqlite`).
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
- Production secrets live only in `/opt/spclaw/.env.prod`.
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
  - `make openclaw-market-daily-earnings-recap-run-once`

24/7 runtime bootstrap on Mac mini:
1. `cd /opt/spclaw`
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
- Chart/data artifacts are written to `/opt/spclaw-data/artifacts/charts/`:
  - `valuation-scatter-*.png`
  - `valuation-scatter-*.csv`
  - `valuation-scatter-*.json`
  - `valuation-scatter-*-raw.json`
- Every generated insight must retain source attribution and timestamps.

File bridge contract:
- Local canonical bot-managed paths:
  - `/opt/spclaw-data/files/working`
  - `/opt/spclaw-data/files/archive`
  - `/opt/spclaw-data/files/published`
  - `/opt/spclaw-data/files/incoming`
- Shared mirror paths (Google Drive or local fallback) are configured in `config/file-bridge.json`.
- Default Mac mini Drive root is `/Users/spclaw/Documents/SPClaw Database`.
- `openclaw-files-sync` performs pull (`01_DROP_HERE_Incoming` -> local incoming), push (`published/archive` -> shared paths), and index regeneration.
- `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` is auto-mirrored from published outputs and excluded from pull ingestion.
- Canonical category set is simplified to `Universes`, `Companies`, and `Industries`.
- Published index artifacts are generated at:
  - `/opt/spclaw-data/files/published/index.json`
  - `/opt/spclaw-data/files/published/index.md`
- Slack file ingest contract:
  - Slack message/app_mention events with file attachments trigger automatic file intake.
  - Files are downloaded with bot token auth and categorized into `incoming/{Universes|Companies|Industries}`.
  - Files are mirrored into Drive `01_DROP_HERE_Incoming/{Universes|Companies|Industries}` for shared visibility.
  - Intake metadata is stored in `/opt/spclaw-data/db/file_ingest.sqlite`.

## Runtime Settings Contract
- Live Slack-configurable settings are stored outside git:
  - `/opt/spclaw-data/db/runtime-settings.json`
- Change audits are written to:
  - `/opt/spclaw-data/artifacts/config-audit/*.md`
- Git-tracked baseline defaults are stored in:
  - `/opt/spclaw/config/runtime-defaults.json`
- Promotion ledger is stored in:
  - `/opt/spclaw-data/db/settings-promotions.json`

Slack conversational controls:
1. `@SPClaw show my settings`
2. `@SPClaw going forward look for 12 peers`
3. `@SPClaw use market cap as the default x-axis`
4. `@SPClaw promote current settings` (direct commit/push to `main`)
5. `@SPClaw undo last promotion` (auto-revert of last promoted settings commit)

Slack deploy pipeline controls:
1. `@SPClaw deploy latest` (pull + restart + Slack health probe)
2. `@SPClaw undo last deploy` (revert last deploy target + push + restart + probe)
3. `@SPClaw run checks` (`PYTHONPATH=src pytest -q`)
4. `@SPClaw show pipeline status`
5. `@SPClaw show deploy history`
6. `@SPClaw build: <request>` (runs Codex CLI by default if installed, otherwise requires custom runner)
7. `@SPClaw x digest SNOW last 24h limit 80`
8. `@SPClaw x status`
9. `x chart now`
10. `x chart sources`
11. `x chart add @fiscal_AI priority 1.6`
12. `x chart help`

Pipeline environment controls:
- `SLACK_PIPELINE_ADMINS`: optional comma-separated Slack user IDs allowed to run pipeline commands
- `SPCLAW_SLACK_BUILD_COMMAND`: optional custom build command template with `{request}` placeholder
- `SPCLAW_DEPLOY_HISTORY_PATH`: optional deploy history JSON path (default under `/opt/spclaw-data/db/`)

Memory environment controls:
- `SPCLAW_MEMORY_DB_PATH`: optional SQLite memory DB path
- `SPCLAW_MEMORY_VECTOR_DIR`: optional LanceDB directory path
- `SPCLAW_MEMORY_EMBED_MODEL`: optional embedding model (default `text-embedding-3-large`)
- `SPCLAW_MEMORY_RECONCILE_INTERVAL_SECONDS`: scheduler interval for memory queue export (default `900`, min `300`, max `86400`)
- `SPCLAW_MEMORY_RECONCILE_EXPORT_LIMIT`: queue export limit used by scheduler (default `200`, min `1`, max `1000`)
- `OPENAI_API_KEY`: required only for semantic memory fallback

File ingest environment controls:
- `SPCLAW_FILE_INGEST_DB_PATH`: optional override for Slack file ingest SQLite path
- `SPCLAW_SLACK_FILE_MAX_MB`: optional max Slack file size in MB for auto-ingest (default `50`)

X digest environment controls:
- `SPCLAW_X_BEARER_TOKEN`: required for X API requests
- `SPCLAW_X_API_BASE`: optional API base URL override (default `https://api.x.com`)
- `SPCLAW_X_DIGEST_DIR`: optional digest markdown output dir (default `/opt/spclaw-data/artifacts/x-digest`)

X chart scout environment controls:
- `SPCLAW_X_CHART_SLACK_CHANNEL`: required Slack destination (channel id like `C...` or channel name like `#charting`)
- `SPCLAW_X_CHART_TIMEZONE`: posting timezone (default `America/Los_Angeles`)
- `SPCLAW_X_CHART_WINDOWS`: comma-separated daily times (default `07:00,12:00,18:00`)
- `SPCLAW_X_CHART_SOURCE_LIMIT`: number of tracked source handles to scan each run (default `25`)
- `SPCLAW_X_CHART_DISCOVERY_MIN_ENGAGEMENT`: minimum engagement for auto-discovered source promotion (default `120`)
- `SPCLAW_X_CHART_DB_PATH`: optional SQLite store path (default `/opt/spclaw-data/db/x_chart_daily.sqlite`)
- `SPCLAW_X_CHART_DIR`: optional markdown artifact output dir (default `/opt/spclaw-data/artifacts/x-chart-daily`)
- `SPCLAW_VISUALCAPITALIST_FEED_URL`: optional feed override (default `https://www.visualcapitalist.com/feed/`)

MD (Market Daily) environment controls:
- `SPCLAW_MD_SLACK_CHANNEL`: Slack destination channel id/name (default `general`)
- `SPCLAW_MD_TZ`: slot timezone (default `America/Los_Angeles`)
- `SPCLAW_MD_TIMES`: local run times (`HH:MM,HH:MM`, default `07:00,14:15`)
- `SPCLAW_MD_EARNINGS_RECAP_TIME`: local recap run time (`HH:MM`, default `19:00`)
- `SPCLAW_MD_TOP_N`: mover count (default `3`)
- `SPCLAW_MD_TMT_TOP_K`: top-ranked seed members to keep before overlay (default `40`)
- `SPCLAW_MD_CANDIDATE_SEED_PATH`: CSV universe seed path (default `/opt/spclaw/config/md_tmt_seed_universe.csv`)
- `SPCLAW_MD_DB_PATH`: SQLite path (default `/opt/spclaw-data/db/market_daily.sqlite`)
- `SPCLAW_MD_ARTIFACT_DIR`: markdown artifact output dir (default `/opt/spclaw-data/artifacts/market-daily`)
- `SPCLAW_MD_COATUE_CIK`: Coatue CIK for auto 13F refresh
- `SPCLAW_MD_OPENFIGI_API_KEY`: optional OpenFIGI key for stronger CUSIP->ticker resolution
- `SPCLAW_MD_MODEL`: optional catalyst summarizer model (default `gpt-5.2-chat-latest`)
- `SPCLAW_MD_CATALYST_MODE`: catalyst engine mode (`simple_synthesis` default, `legacy_heuristic` rollback)
- `SPCLAW_MD_SYNTH_MAX_RESULTS`: max evidence candidates passed to simple synthesis (default `5`)
- `SPCLAW_MD_SYNTH_SOURCE_MODE`: simple synthesis source mix (`google_plus_yahoo` default, `google_only`, `yahoo_only`)
- `SPCLAW_MD_SYNTH_DOMAIN_GATE`: synthesis domain filter (`soft` default, `quality_only`, `off`)
- `SPCLAW_MD_SYNTH_SUPPORT_COUNT`: max support links passed alongside the anchor evidence in simple synthesis (default `2`)
- `SPCLAW_MD_SYNTH_FORCE_BEST_GUESS`: legacy compatibility toggle from earlier phrase-based path (`1` default)
- `SPCLAW_MD_RELEVANCE_MODE`: anchor/support selection mode (`llm_first` default, `deterministic` fallback mode)
- `SPCLAW_MD_REASON_OUTPUT_MODE`: simple reason rendering mode (`free_sentence` default, `wrapper` optional rollback)
- `SPCLAW_MD_POST_AS_IS`: in simple mode, accept non-empty LLM sentence with minimal normalization (`1` default)
- `SPCLAW_MD_RECAP_SUPPORT_COUNT`: earnings recap support evidence count (defaults to `SPCLAW_MD_SYNTH_SUPPORT_COUNT`; default effective value `2`)
- `SPCLAW_MD_RECAP_POST_AS_IS`: earnings recap post-as-is policy (defaults to `SPCLAW_MD_POST_AS_IS`; default effective value `1`)
- `SPCLAW_MD_REQUIRE_IN_WINDOW_DATES`: require candidate publish timestamps to fall within the active session window (`1` default)
- `SPCLAW_MD_ALLOW_UNDATED_FALLBACK`: allow undated candidates when timestamp validation fails (`0` default)
- `SPCLAW_MD_REJECT_HISTORICAL_CALLBACK`: reject headlines/summaries that cite materially older event dates (for example “On January 26 ...”) (`1` default)
- `SPCLAW_MD_PUBLISH_TIME_ENRICH_ENABLED`: attempt publish-time enrichment from article metadata when feed/search timestamp is missing (`1` default)
- `SPCLAW_MD_PUBLISH_TIME_ENRICH_TIMEOUT_MS`: per-url publish-time enrichment timeout in milliseconds (default `1200`)
- `SPCLAW_MD_ARTICLE_CONTEXT_ENABLED`: enable article-body context enrichment for LLM relevance + one-line catalyst writing (`1` default)
- `SPCLAW_MD_ARTICLE_CONTEXT_TIMEOUT_MS`: per-url article fetch timeout for context enrichment (default `3500`)
- `SPCLAW_MD_ARTICLE_CONTEXT_MAX_CHARS`: max context chars extracted per article before prompting (default `6000`)
- `SPCLAW_MD_ARTICLE_CONTEXT_LIMIT`: max candidate articles to enrich with full body context per ticker in LLM steps (default `4`)
- `SPCLAW_MD_MAX_LOOKBACK_HOURS`: max evidence lookback cap for session windows (default `96`)
- `SPCLAW_MD_WEB_SEARCH_ENABLED`: enable web fallback retrieval (`1`/`0`, default `1`)
- `SPCLAW_MD_WEB_SEARCH_BACKEND`: web backend (`google_serp` primary with `ddg_html` fallback, default `google_serp`)
- `SPCLAW_MD_GOOGLE_SERP_API_KEY`: SERP API key for Google-backed evidence retrieval
- `SPCLAW_MD_GOOGLE_SERP_ENDPOINT`: optional SERP endpoint override (default `https://serpapi.com/search.json`)
- `SPCLAW_MD_WEB_MAX_RESULTS`: max web evidence links per ticker (default `20`)
- `SPCLAW_MD_MIN_EVIDENCE_CONFIDENCE`: confidence threshold before fallback reason line (default `0.55`)
- `SPCLAW_MD_MIN_CAUSE_SOURCES`: minimum independent corroborating sources required to name a specific cause (default `2`)
- `SPCLAW_MD_MIN_CAUSE_DOMAINS`: minimum distinct corroborating domains required to name a specific cause (default `2`)
- `SPCLAW_MD_ENABLE_CAUSE_CLUSTER_REUSE`: reuse one confirmed basket cause phrase across multiple movers in the same run (`1`/`0`, default `1`)
- `SPCLAW_MD_GENERIC_HEADLINE_BLOCKLIST_ENABLED`: block generic wrappers (for example "stock is down today") from final catalyst lines (`1`/`0`, default `1`)
- `SPCLAW_MD_REASON_MODE`: catalyst reasoning mode (default `best_effort`)
- `SPCLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED`: allow decisive single-source event phrasing when one high-quality source is dominant (`1`/`0`, default `1`)
- `SPCLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE`: minimum effective evidence score for decisive-primary override (default `0.60`)
- `SPCLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN`: minimum top-cluster score gap vs runner-up for decisive-primary override (default `0.03`)
- `SPCLAW_MD_REASON_QUALITY_MODE`: reason rendering mode (`hybrid` default, `deterministic` optional)
- `SPCLAW_MD_REASON_POLISH_ENABLED`: enable optional LLM polish pass for awkward phrases (`1`/`0`, default `1`)
- `SPCLAW_MD_REASON_POLISH_MODEL`: optional model override for reason polish (default follows `SPCLAW_MD_MODEL`)
- `SPCLAW_MD_REASON_POLISH_MAX_CHARS`: max chars for polished reason phrase (default `90`)
- `simple_synthesis` mode behavior:
  - collects top Google web + Yahoo ticker evidence
  - if Google SERP key is missing, skips DDG fallback for catalyst selection and relies on Yahoo evidence + safe fallback behavior
  - rejects quote-directory/generic wrapper items before synthesis
  - enforces strict time-integrity filtering for candidate links and catalyst selection
  - applies soft penalties to technical-analysis and multi-ticker roundup headlines
  - chooses one anchor evidence candidate plus supports using `SPCLAW_MD_RELEVANCE_MODE`:
    - `llm_first`: LLM selects anchor/support from ranked candidates
    - `deterministic`: code-only anchor/support selector
  - in `llm_first`, includes richer article-body context (not just title/snippet) for top candidates before relevance selection and sentence drafting
  - synthesizes one free-sentence catalyst line (not forced `Shares rose/fell after ...` wrapper)
  - aligns `[News]/[Web]` links to the anchor/support evidence used for that generated sentence
  - with LLM unavailable/error, uses deterministic anchor-based sentence backup before generic fallback
  - falls back only when no usable time-valid candidates remain
- `run-earnings-recap` behavior:
  - uses the same anchor-first evidence policy as simple-synthesis mover reasoning
  - recap bullets are now generated end-to-end from anchor + support evidence (2–4 bullets total)
  - every recap bullet is citation-tagged (`[S1]`, `[S2]`, ...) and must align with row `Sources:` links
  - deterministic recap backup path generates coherent bullets when LLM is unavailable/fails
  - recap artifacts include observability fields:
    - `recap_anchor_url`
    - `recap_support_urls`
    - `recap_generation_mode`
    - `recap_quality_rejections`
- Basket coherence rule: if a confirmed `anthropic_claude_cyber` cause is present for one cybersecurity mover in a run, peer cybersecurity selloff movers reuse that same cause phrase.
- MD fallback line (when corroboration gate fails): `Likely positioning/flow; no single confirmed catalyst.`

Board Seat daily environment controls:
- `SPCLAW_BOARD_SEAT_PORTCOS`: comma-separated `Company:channel` mappings (default includes anduril/anthropic/cursor/neuralink/openai/physical-intelligence/ramp/spacex/stripe/sunday-robotics)
- `SPCLAW_BOARD_SEAT_TIME`: local daily runtime time (`HH:MM`, default `08:30`)
- `SPCLAW_BOARD_SEAT_TZ`: timezone for run date (default `America/Los_Angeles`)
- `SPCLAW_BOARD_SEAT_THEME_LOOKBACK_DAYS`: thematic context horizon used for monthly trend framing (default `30`)
- `SPCLAW_BOARD_SEAT_LOOKBACK_HOURS`: optional direct override of Slack history window; defaults to `theme_lookback_days * 24`
- `SPCLAW_BOARD_SEAT_MAX_MESSAGES`: max context messages fetched per channel (default `160`)
- `SPCLAW_BOARD_SEAT_DB_PATH`: optional SQLite path for daily run ledger
- `SPCLAW_BOARD_SEAT_MODEL`: optional LLM model for synthesis (default `gpt-5.2-chat-latest`)
- `SPCLAW_BOARD_SEAT_HEADER_STYLE`: Slack rendering style (`richtext` default; falls back to plaintext on block errors)
- `SPCLAW_BOARD_SEAT_SPECIFICITY_MODE`: draft specificity guard (`moderate` default)
- `SPCLAW_BOARD_SEAT_FUNDING_SCOPE`: funding entity scope (`target` default, `company` optional)
- `SPCLAW_BOARD_SEAT_CRUNCHBASE_ENABLED`: enable Crunchbase funding resolver (`1` default)
- `SPCLAW_CRUNCHBASE_API_KEY`: Crunchbase API key used for target funding lookup
- `SPCLAW_BOARD_SEAT_GOOGLE_SERP_API_KEY`: optional SERP API key for funding/source fallback retrieval (falls back to `SERPAPI_API_KEY`)
- `SPCLAW_BOARD_SEAT_FUNDING_WEB_TOP_ROWS`: max normalized funding evidence rows retained before extraction (default `8`)
- `SPCLAW_BOARD_SEAT_FUNDING_MIN_DOMAINS`: minimum corroborating domains required for `verified` funding status (default `2`)
- `SPCLAW_BOARD_SEAT_FUNDING_LOW_CONF_THRESHOLD`: confidence threshold below which funding is treated as `low` band (default `0.55`)
- `SPCLAW_BOARD_SEAT_FUNDING_WARNING_MODE`: include explicit low-confidence funding warning line in Board Seat output (`1` default)
- `SPCLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET`: require each post to be both a new target and `High` confidence; otherwise skip with `no_high_confidence_new_target` (`1` default)
- `SPCLAW_BOARD_SEAT_TARGET_LOCK_DAYS`: target-memory lock window (default `14`, minimum enforced `14`)
- `SPCLAW_BOARD_SEAT_ALLOW_REPEAT_TARGETS`: set `1` to bypass only the configurable lock window above 14 days; cannot bypass the hard 14-day no-repeat rule (default `0`)
- `SPCLAW_BOARD_SEAT_EVENT_TRACK_TARGETS_PER_COMPANY`: number of promising targets to track event flow for per company run (default `4`)
- `SPCLAW_BOARD_SEAT_EVENT_TRACK_ROWS_PER_TARGET`: max candidate event rows pulled per tracked target per run (default `8`)
- `SPCLAW_BOARD_SEAT_LEDGER_DIR`: board-seat target ledger artifact directory (default `/opt/spclaw-data/artifacts/board-seat`)
- `SPCLAW_BOARD_SEAT_LEDGER_MIRROR_ENABLED`: mirror ledger to Google Drive local path (default `1`)
- `SPCLAW_BOARD_SEAT_LEDGER_MIRROR_PATH`: mirror destination path (default `/Users/spclaw/Documents/SPClaw Database/Companies/Board-Seat`)

Board Seat V6 message contract:
- plaintext canonical format remains persisted in DB/artifacts for memory/repeat-guard/backfill.
- Slack rich-text headers are bold + underlined for:
  - `Thesis`
  - `{Company} context`
  - `Funding snapshot`
  - `Sources`
- thesis lines:
  - `Idea`
  - `Target does`
  - `Why now` (monthly trend; no last-24h phrasing)
  - `What's different`
  - `MOS/risks`
  - `Bottom line`
- `Idea confidence` is removed from rendered output.
- low-confidence funding snapshots append:
  - `Warning: Funding data is low-confidence; verify before action.`
- Re-pitch policy:
  - same target cannot be re-pitched within 14 days (hard rule)
  - after 14 days, resurfacing requires exceptional new evidence with explicit disclosure lines:
    - `Repitch note`
    - `New evidence`

Spencer change-digest environment controls:
- `SPCLAW_CHANGE_TRACKER_USERS`: optional comma-separated `user_id:label` mappings for tracked requesters (example: `U0AGD28QSQG:Carson Wang,U0AFJ5RS31C:Spencer Peterson`)
- `SPCLAW_SPENCER_USER_IDS`: comma-separated Slack user IDs treated as Spencer request sources
- `SPCLAW_SPENCER_CHANGE_DB_PATH`: optional SQLite path for tracked Spencer requests
- `SPCLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS`: comma-separated Slack user IDs to DM with daily open-request digest
- `SPCLAW_SPENCER_CHANGE_DIGEST_TIME`: local daily digest time (`HH:MM`, default `18:00`)
- `SPCLAW_SPENCER_CHANGE_DIGEST_TZ`: timezone used in digest header text (default `America/Los_Angeles`)
- `SPCLAW_CHANGE_NOTIFY_USER_IDS`: comma-separated Slack user IDs for immediate DM notifications when behavior-change requests are captured (default `U0AGD28QSQG`)
- `SPCLAW_CHANGE_MEMORY_MD_PATH`: path to memory markdown file appended on change capture (default `/Users/spclaw/.openclaw/workspace/MEMORY.md`)

Email integration environment controls:
- `SPCLAW_EMAIL_ENABLED`: set `true` to enable email processing
- `SPCLAW_EMAIL_IMAP_HOST|PORT|USER|PASSWORD|MAILBOX`
- `SPCLAW_EMAIL_SMTP_HOST|PORT|USER|PASSWORD`
- `SPCLAW_EMAIL_FROM`: sender address used for replies
- `SPCLAW_EMAIL_ALLOWED_SENDERS`: optional comma-separated allowlist
- `SPCLAW_EMAIL_POLL_SECONDS`: poll cadence for `serve` mode (default `60`)
- `SPCLAW_EMAIL_MAX_ATTACHMENT_MB`: max attachment size to ingest (default `25`)
- `SPCLAW_EMAIL_DB_PATH`: optional SQLite DB path for email gateway logs

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

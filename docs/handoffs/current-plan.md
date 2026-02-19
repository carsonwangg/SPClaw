# Coatue Claw - Current Plan (OpenClaw Native)

## Objective
Build a 24/7 equity research bot (Slack-first) that runs natively on OpenClaw as the primary runtime and control plane.

## V1 Scope
- SEC + transcript + macro ingestion
- Diligence packets (bull/bear + peer comp + charts)
- Weekly idea scan
- X-only digest (digest-first)
- Memory layer (SQLite + LanceDB + thesis notes)

## Platform Target
- Repo: GitHub (`CoatueClaw`)
- Runtime: OpenClaw-native workflows and agents
- Dev machine: Mac mini (local dev + fallback runtime only)
- Control: laptop via OpenClaw
- Runtime data dirs: `/opt/coatue-claw-data/{db,cache,logs,artifacts,backups}`

## Delivery Phases
1. OpenClaw Foundation
- Define OpenClaw execution model (entrypoints, long-running jobs, scheduled jobs)
- Define secrets model for Slack/OpenAI keys in OpenClaw
- Define logging/alerts and incident visibility in OpenClaw
- Define artifact persistence paths and retention

2. Runtime Integration
- Wire Slack bot into OpenClaw process model
- Validate mention events, replies, and retries end-to-end
- Add health checks and restart policy

3. Product Core
- Implement real diligence pipeline (replace template output)
- Implement ingestion jobs (SEC/transcripts/macro)
- Implement memory layer writes/reads

4. Product Loops
- Weekly idea scan automation
- X-only digest generation + posting path
- Operator workflows for review/approval

## Current Status
- AGENTS and initial scaffold are complete
- Basic CLI + Slack bot skeleton are implemented
- Bot mention delivery is working with open Slack access policy
- Slack default routing is now enabled:
  - plain messages are treated as OpenClaw requests by default
  - messages with explicit `@user` mentions are not default-routed
  - deployed/validated on Mac mini (`86bce9d`): Slack probe healthy after restart
  - runtime transport config on Mac mini now explicitly disables mention gating:
    - `~/.openclaw/openclaw.json` -> `channels.slack.requireMention=false`
    - channel override for `#general` (`C0AFGMRFWP8`) also set to `requireMention=false`
- Natural-language chart requests now route into valuation charting with configurable axes
- CSV-backed universe management is implemented for Slack-driven create/edit/reuse flows
- Missing-ticker chart prompts now ask for source choice (`online` discovery vs saved universe CSV)
- Post-chart feedback loop is implemented for include/exclude reruns
- Post-chart feedback prompt delivery now uses resilient thread posting (retry + fallback) for higher Slack reliability
- OpenClaw valuation-charting skill now requires a post-chart adjustments follow-up question after each successful chart response
- Chart headline context now follows prompt theme; citation/footer is left-aligned for cleaner layout
- Category guide placement now defaults to adaptive in-plot whitespace positioning to reduce wasted space while avoiding key chart overlays
- Laptop/Codex/OpenClaw runbook now exists in-repo (`docs/laptop-codex-openclaw-workflow.md`) and AGENTS includes explicit canonical-path + ship/restart workflow rules
- OpenClaw runtime contract is now codified in `docs/openclaw-runtime.md` (execution model, job classes, ops + triage checklist)
- Make targets now include explicit `dev`, `bot`, and `schedulers` runtime controls for operator workflows
- Makefile OpenClaw targets now prepend `/opt/homebrew/bin` to PATH and use binary fallback detection so remote non-login SSH sessions can restart/status without manual PATH export
- Plain-English Slack settings controls are now implemented (`show settings`, conversational default updates, promote-to-main, undo last promotion)
- Runtime settings now persist under `/opt/coatue-claw-data/db/runtime-settings.json` with markdown audit logs in `/opt/coatue-claw-data/artifacts/config-audit/`
- Slack deploy pipeline controls are implemented (`deploy latest`, `undo last deploy`, `run checks`, `show pipeline status`, `show deploy history`, `build: ...`) with one-job-at-a-time locking and admin gating
- Deploy history now persists to `/opt/coatue-claw-data/db/deploy-history.json`
- Diligence command now generates a structured neutral investment memo (deep data pull from company profile, financials, valuation, balance sheet, and recent reporting headlines) instead of template placeholders
- Diligence now runs a local database-first report lookup before external research:
  - checks `/opt/coatue-claw-data/db/file_ingest.sqlite` and prior packet markdowns in `/opt/coatue-claw-data/artifacts/packets/`
  - includes local match references directly in memo output for continuity and auditability
- Hybrid memory system is implemented:
  - SQLite + FTS5 structured memory store in `/opt/coatue-claw-data/db/memory.sqlite`
  - auto extraction of profile facts, decisions, and conventions from Slack messages
  - decay tiers (`permanent`, `stable`, `active`, `session`, `checkpoint`) with TTL refresh-on-access
  - pre-flight pipeline checkpoints for deploy/build/undo operations
  - optional LanceDB/OpenAI semantic fallback
  - CLI ops: `claw memory status|query|prune|extract-daily|checkpoint`
- File management bridge is implemented:
  - local-first canonical storage in `/opt/coatue-claw-data/files/{working,archive,published,incoming}`
  - share mirror sync to configurable Drive root via `config/file-bridge.json`
  - Drive mirror root is configured on Mac mini as `/Users/spclaw/Documents/Google Drive Local`
  - category subfolders provisioned for Spencer-facing workflows under `01_DROP_HERE_Incoming/02_READ_ONLY_Latest_AUTO/03_READ_ONLY_Archive_AUTO`
  - `01_DROP_HERE_Incoming/_Latest_Reference_READ_ONLY` auto-mirrors Latest and is excluded from pull ingestion
  - Slack file uploads now auto-ingest into knowledge folders with SQLite audit tracking (`/opt/coatue-claw-data/db/file_ingest.sqlite`) via `message` + `file_shared` + `app_mention` event handlers
  - operations via `make openclaw-files-{init,status,sync-pull,sync-push,sync,index}`
  - published index artifacts generated to `published/index.{json,md}`
- Email channel integration is implemented (optional):
  - IMAP poll + SMTP reply runtime in `src/coatue_claw/email_gateway.py`
  - email commands: diligence, memory status/query, files status, help
  - context-aware diligence email parsing now prioritizes body intent and filters filler tokens so ticker extraction is robust in natural phrasing
  - diligence email response format is now consumer-friendly (executive summary in body + full memo attached as readable `.pdf`, with summary citation tails removed for readability)
  - local filesystem paths are removed from user-facing diligence email output
  - PDF rendering now escapes literal `$` symbols so finance values render reliably
  - diligence attachment PDF now renders as a sectioned, consumer-readable brief (not raw memo text)
  - professional PDF styling now uses clean section headers, readable bullet spacing, and page footers for Spencer-facing consumption
  - report title is generic to the diligence topic/company (no third-party/borrowed brand title text)
  - latest template upgrade adds centered title + metadata row + backdrop callout to align with professional memo aesthetics
  - email attachments auto-ingest to knowledge folders with audit DB (`/opt/coatue-claw-data/db/email_gateway.sqlite`)
  - operations via `make openclaw-email-{status,run-once,serve}`
  - Mac mini validation confirms `Testing Dilligence` + `Diligence SNOW please` resolves to ticker `SNOW`
  - Mac mini validation confirms summary citation tails are removed in email body while full-citation memo remains attached
  - Mac mini validation confirms diligence attachment is now readable PDF (`application/pdf`) and local paths are removed from user-facing email output
- X digest (official API path) is implemented for on-demand use:
  - Slack commands:
    - `x digest <query> [last Nh] [limit N]`
    - `x status`
  - CLI command:
    - `claw x-digest "QUERY" --hours 24 --limit 50`
  - digest artifact output:
    - `/opt/coatue-claw-data/artifacts/x-digest` (override with `COATUE_CLAW_X_DIGEST_DIR`)
  - runtime env contract:
    - `COATUE_CLAW_X_BEARER_TOKEN` required
    - `COATUE_CLAW_X_API_BASE` optional (default `https://api.x.com`)
  - tests:
    - `tests/test_slack_x_intent.py`
    - `tests/test_x_digest.py`
  - Mac mini runtime status:
    - deployed on `/opt/coatue-claw` at commit `5dfdd03`
    - bearer token configured in `.env.prod`
    - Slack probe healthy after restart (`make openclaw-slack-status` => `ok=true`)
    - live digest smoke test succeeded and wrote artifact to `/opt/coatue-claw-data/artifacts/x-digest/`
- X chart scout is now implemented for daily winner posting:
  - prioritized source list seeded with `@fiscal_AI` and other high-signal accounts
  - auto-discovery/promotion of new sources based on engagement
  - supplemental ingestion from Visual Capitalist feed (`https://www.visualcapitalist.com/feed/`)
  - Slack commands:
    - `x chart now`
    - `x chart status`
    - `x chart sources`
    - `x chart add @handle priority 1.2`
  - CLI commands:
    - `claw x-chart run-once --manual`
    - `claw x-chart status`
    - `claw x-chart list-sources`
    - `claw x-chart add-source HANDLE --priority 1.2`
  - scheduled runtime service:
    - `com.coatueclaw.x-chart-daily` via launchd
    - windows default to `09:00,12:00,18:00` (timezone default `America/Los_Angeles`)
  - artifacts and state:
    - sqlite store: `/opt/coatue-claw-data/db/x_chart_daily.sqlite`
    - markdown artifacts: `/opt/coatue-claw-data/artifacts/x-chart-daily`
  - tests:
    - `tests/test_x_chart_daily.py`
    - `tests/test_launchd_runtime.py` (updated for new service)
  - resilience:
    - invalid/renamed X handles are skipped without failing the full scout run
    - Slack posting can use `~/.openclaw/openclaw.json` token fallback if env token is unavailable
    - Slack posting automatically retries against fallback token when primary env token is rejected
  - Mac mini runtime status:
    - deployed and validated at commit `c3f64d0`
    - scheduler service `com.coatueclaw.x-chart-daily` loaded via launchd
    - proof-of-life manual run posted successfully to `#general` channel id `C0AFGMRFWP8`
  - presentation layer:
    - winners are now rendered into a Coatue-style “Chart of the Day” visual card before Slack upload
    - style cues align with C:\\Takes design language and valuation-chart skill guidance
  - quality gate:
    - candidate selection now enforces chart-like text/data signals to suppress non-chart image picks
- 24/7 runtime supervision is implemented:
  - launchd-managed services in `src/coatue_claw/launchd_runtime.py`
  - services: `com.coatueclaw.email-gateway` (always-on poller), `com.coatueclaw.memory-prune` (hourly prune)
  - launchctl domain fallback (`gui/<uid>` then `user/<uid>`) for reliable control over SSH and local sessions
  - operations via `make openclaw-24x7-{enable,status,disable}`
  - scheduler status target now reports real launchd state (`make openclaw-schedulers-status`)
  - deployed and validated on Mac mini (`a49f887` + `95fb26d`): email poller is running; memory-prune service is loaded with clean `last_exit_code=0` between hourly runs
- Git shipping protocol is now explicit: every Codex change ships to `origin` with handoff updates

## Immediate Next Actions
1. Validate Slack deploy pipeline commands in `#claw-lab` (`deploy latest`, `undo last deploy`, `run checks`, `build: ...`)
2. Configure `SLACK_PIPELINE_ADMINS` on runtime host and validate permission boundaries
3. Validate hybrid memory behavior in Slack:
   - `remember ...` capture
   - `what is my ...` retrieval
   - `memory status`
   - `memory checkpoint`
4. Confirm Google Drive desktop client is syncing `/Users/spclaw/Documents/Google Drive Local` to Spencer-shared Drive
5. Validate category-based file flow with Spencer (`01_DROP_HERE_Incoming/<Category>` -> local incoming mirror -> `02_READ_ONLY_Latest_AUTO/<Category>`)
6. Validate Slack file upload auto-ingest (`Slack upload` -> categorized `incoming/<Category>` + DB record in `file_ingest.sqlite`)
7. Validate launchd service persistence after next Mac mini reboot (`make openclaw-24x7-status`)
8. Validate daily backfill flow (`claw memory extract-daily --dry-run --days 14`)
9. Validate new diligence memo output in Slack (`diligence TICKER`) and confirm section completeness/citations + local database-first precheck behavior
10. Configure email env vars in `/opt/coatue-claw/.env.prod` and validate `make openclaw-email-status` + `make openclaw-email-run-once`
11. Deploy and enable `com.coatueclaw.x-chart-daily` on Mac mini with:
    - `COATUE_CLAW_X_CHART_SLACK_CHANNEL`
    - `COATUE_CLAW_X_CHART_WINDOWS=09:00,12:00,18:00`
    - `COATUE_CLAW_X_CHART_TIMEZONE=America/Los_Angeles`
12. Validate three scheduled daily posts in Slack (9am/12pm/6pm PT) and tune source priority list after first day.
13. Tune style/briefing density after observing 1-2 days of live scheduled posts.

## 2026-02-19 Update - Build Request Runtime Robustness
- Added a near-term reliability guard for Slack `build:` execution:
  - `codex exec` prompt now instructs fallback to `grep -R` when `rg` is missing.
- Added test coverage:
  - `tests/test_slack_pipeline.py::test_run_build_request_prompt_includes_rg_fallback`.
- Operational recommendation remains to install ripgrep on runtime host for speed and consistency.

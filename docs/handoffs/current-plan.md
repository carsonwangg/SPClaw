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
  - email attachments auto-ingest to knowledge folders with audit DB (`/opt/coatue-claw-data/db/email_gateway.sqlite`)
  - operations via `make openclaw-email-{status,run-once,serve}`
  - Mac mini validation confirms `Testing Dilligence` + `Diligence SNOW please` resolves to ticker `SNOW`
  - Mac mini validation confirms summary citation tails are removed in email body while full-citation memo remains attached
  - Mac mini validation confirms diligence attachment is now readable PDF (`application/pdf`) and local paths are removed from user-facing email output
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
11. Wire first scheduled jobs (weekly idea scan + X digest)

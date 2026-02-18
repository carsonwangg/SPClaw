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
- Git shipping protocol is now explicit: every Codex change ships to `origin` with handoff updates

## Immediate Next Actions
1. Validate universe commands and online-vs-universe prompt flow in Slack (`#charting`)
2. Validate plain-English settings commands in Slack (`show my settings`, `going forward look for 12 peers`, `promote current settings`, `undo last promotion`)
3. Validate post-chart follow-up prompt delivery in-thread across multiple chart runs with updated settings
4. Wire first scheduled jobs (weekly idea scan + X digest) and replace scheduler status placeholder target

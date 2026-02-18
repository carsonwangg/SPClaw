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
- Bot mention delivery is under active debugging
- OpenClaw-native runtime spec is not yet fully codified
- Git shipping protocol is now explicit: every Codex change ships to `origin` with handoff updates

## Immediate Next Actions
1. Write OpenClaw runtime spec in repo (`docs/openclaw-runtime.md`)
2. Add OpenClaw run targets (dev, bot, schedulers) in `Makefile`
3. Finalize Slack event delivery in OpenClaw runtime path
4. Convert skill stubs into runnable procedures

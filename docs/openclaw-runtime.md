# OpenClaw Runtime Guide

## Purpose
Define the runtime contract for Coatue Claw on OpenClaw, including process roles, operational controls, storage boundaries, and validation expectations.

## Runtime Source of Truth
- Gateway config: `~/.openclaw/openclaw.json`
- Gateway service: `~/Library/LaunchAgents/ai.openclaw.gateway.plist`
- Gateway logs: `/tmp/openclaw/openclaw-YYYY-MM-DD.log`
- App repo: `/opt/coatue-claw`
- Runtime data: `/opt/coatue-claw-data`

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
- Scheduled (planned but not yet wired in this repo):
  - Weekly idea scan
  - X digest generation

## Secrets and Environment Contract
- Production secrets live only in `/opt/coatue-claw/.env.prod`.
- Do not commit secrets to git.
- Slack runtime requires:
  - `SLACK_BOT_TOKEN`
  - `SLACK_SIGNING_SECRET`
  - `SLACK_APP_TOKEN`

## Operational Commands
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

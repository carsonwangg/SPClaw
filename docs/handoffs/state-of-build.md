# Coatue Claw - State of Build

## Snapshot Date
- 2026-02-17

## What Is Implemented
- Python package scaffold using `src/` layout and `claw` CLI entrypoint.
- CLI command in `src/coatue_claw/cli.py`:
  - `claw diligence TICKER`
  - Generates markdown packet to `/opt/coatue-claw-data/artifacts/packets/<TICKER>-<timestamp>.md`.
- Slack bot in `src/coatue_claw/slack_bot.py` using Slack Bolt Socket Mode:
  - Handles `app_mention` events.
  - Parses `diligence TICKER` from mention text (supports `<@...>` mention tokens and `$TICKER` forms).
  - Runs diligence packet generation and replies in thread.
  - Returns explicit usage text if command is malformed.
  - Logs inbound Slack requests and mention payload details for debugging.
- Runtime dependencies declared in `pyproject.toml`:
  - `slack-bolt`
  - `python-dotenv`

## Operational Setup
- Production env file present at `/opt/coatue-claw/.env.prod`.
- Bot runs on Mac mini as a user `launchd` agent:
  - `~/Library/LaunchAgents/com.coatue.claw.slack-bot.plist`
  - logs to `/opt/coatue-claw-data/logs/slack-bot.log`

## Current Known Issue
- Bot process connects to Slack Socket Mode successfully.
- Mention events are not observed in logs (`app_mention` not arriving), indicating Slack app configuration / workspace delivery mismatch rather than handler crash.

## What Is Still Stubbed
- `README.md` is empty.
- `Makefile` is empty.
- Skill docs are placeholders:
  - `skills/new-signal/SKILL.md`
  - `skills/diligence-packet/SKILL.md`
  - `skills/memory-ingest/SKILL.md`

## Immediate Next Steps
1. In Slack app config, verify Event Subscriptions includes bot event `app_mention` and Socket Mode is enabled.
2. Reinstall app to workspace after any permission/event changes.
3. Ensure bot is added to target channel, then test `@coatue_claw diligence SNOW`.
4. Expand CLI output from template to real research pipeline inputs.

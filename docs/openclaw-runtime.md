# OpenClaw Runtime Guide

## Runtime Source of Truth
- Gateway config: `~/.openclaw/openclaw.json`
- Gateway service: `~/Library/LaunchAgents/ai.openclaw.gateway.plist`
- Gateway logs: `/tmp/openclaw/openclaw-YYYY-MM-DD.log`

## Operational Commands
- Gateway status: `make openclaw-status`
- Gateway restart: `make openclaw-restart`
- Tail logs: `make openclaw-logs`
- Slack channel probe: `make openclaw-slack-probe`
- Slack channel logs: `make openclaw-slack-logs`
- Slack audit trail: `make openclaw-slack-audit`

## Valuation Charting Command
- CLI: `claw valuation-chart SNOW,MDB,DDOG,NOW,CRWD`
- Slack mention: `@Coatue Claw graph ev ntm growth SNOW,MDB,DDOG,NOW,CRWD`
- Artifacts are written to `/opt/coatue-claw-data/artifacts/charts/`.

## Slack Integration Checklist
1. `openclaw channels status --probe --json` shows `running=true` and `probe.ok=true`.
2. `openclaw channels logs --channel slack --lines 300` shows `slack socket mode connected`.
3. `lastInboundAt` updates after a real Slack mention.
4. `lastOutboundAt` updates after a bot reply.

## Important Notes
- Do not run separate Slack Socket Mode consumers with the same app token.
- For OpenClaw-native runtime, keep only the OpenClaw Slack channel provider active.
- If `lastInboundAt` stays `null`, Slack is not delivering events to this app (usually event subscription/app installation mismatch).

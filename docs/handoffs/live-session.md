# Live Session Handoff (Coatue Claw)

## Current Status
- OpenClaw installed on Mac mini
- Gateway running on mini (port 18789)
- Slack app created ("Coatue Claw")
- Bot invited to channel: #all-coatue-claw

## Blocker
- Bot not replying to mentions yet

## What was already checked
- SSH + Tailscale working
- GitHub SSH key added on mini
- Repo cloned at /opt/coatue-claw
- Secrets in /opt/coatue-claw/.env.prod

## Next Commands (run on mini)
1. `openclaw logs`
2. `openclaw gateway stop`
3. `set -a; source /opt/coatue-claw/.env.prod; set +a`
4. `openclaw gateway start`
5. `openclaw gateway status`

## Slack Settings to verify
- Socket Mode ON
- OAuth scopes: app_mentions:read, chat:write, channels:history
- Event Subscriptions ON
- Bot events: app_mention
- Reinstall to Workspace after any change


# Laptop + Codex + OpenClaw Workflow

This is the simple operating guide for Spencer/Carson.

## System Map
- GitHub (`origin/main`) is the shared source of truth.
- Mac mini runs the live bot + OpenClaw runtime.
- Laptop is for Codex sessions, review, and triggering changes.

## Where the Real Repo Is
- Production repo on Mac mini: `/opt/coatue-claw`
- Runtime data: `/opt/coatue-claw-data`

If you see `/Users/spclaw/Documents/Coatue Claw` and it looks empty or has no commits, do not use it. Use `/opt/coatue-claw`.

## Daily Change Loop
1. Ask Codex for the change.
2. Codex edits `/opt/coatue-claw`, validates, commits, and pushes to `origin/main`.
3. On Mac mini, pull and restart runtime:

```bash
cd /opt/coatue-claw
git pull --ff-only origin main
make openclaw-restart
make openclaw-slack-status
```

4. Test in Slack by mentioning `@Coatue Claw`.
5. If needed, inspect logs:

```bash
cd /opt/coatue-claw
make openclaw-slack-logs
```

## How to Operate From Personal Laptop
Use SSH to run runtime commands on Mac mini:

```bash
ssh <mac-mini-user>@<mac-mini-host>
cd /opt/coatue-claw
make openclaw-status
make openclaw-slack-status
```

Current setup uses local loopback gateway binding, so SSH is the standard remote-control method.

## New Codex Session Prompt
Use this exact prompt to resume safely:

```text
Read /opt/coatue-claw/AGENTS.md and /opt/coatue-claw/docs/handoffs/live-session.md and continue from there.
Use /opt/coatue-claw as the active repo.
Ship every change to git with handoff updates.
```

## Definition of Done (Every Task)
- Change is implemented and validated.
- Commit is pushed to `origin/main`.
- Handoff docs are updated:
  - `docs/handoffs/live-session.md`
  - `docs/handoffs/current-plan.md`
- OpenClaw runtime is restarted and Slack status is healthy.

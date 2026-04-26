# Laptop + Codex + OpenClaw Workflow

This is the simple operating guide for Spencer/Carson.

## System Map
- GitHub (`origin/main`) is the shared source of truth.
- Mac mini runs the live bot + OpenClaw runtime.
- Laptop is for Codex sessions, review, and triggering changes.

## Where the Real Repo Is
- Production repo on Mac mini: `/opt/spclaw`
- Runtime data: `/opt/spclaw-data`

If you see `/Users/spclaw/Documents/SPClaw` and it looks empty or has no commits, do not use it. Use `/opt/spclaw`.

## Daily Change Loop
1. Ask Codex for the change.
2. Codex edits `/opt/spclaw`, validates, commits, and pushes to `origin/main`.
3. On Mac mini, pull and restart runtime:

```bash
cd /opt/spclaw
git pull --ff-only origin main
make openclaw-restart
make openclaw-slack-status
make openclaw-24x7-status
```

4. Test in Slack by mentioning `@SPClaw`.
5. If needed, inspect logs:

```bash
cd /opt/spclaw
make openclaw-slack-logs
```

## Memory-to-Git Queue Loop (`git-memory:`)
Use this when Slack writes runtime memory that should become repo-tracked behavior.

1. In Slack, either:
   - start with `git-memory: <change request>`, or
   - ask for a bot behavior change in plain language (auto-detected).
2. Bot auto-actions:
   - queues request as `memory_git`
   - appends it to runtime `MEMORY.md`
   - DMs Carson with request details
   - refreshes queue snapshot.
3. Check queue in Slack:
   - `spencer changes memory`
4. In Codex session, export queue snapshot:
   - `claw memory reconcile-export --limit 200`
5. Implement accepted items, then ship one batch commit.
6. Link resolved queue IDs to commit:
   - `claw memory reconcile-link --ids 12,13 --commit <hash> --resolved-by codex`

Artifacts:
- queue snapshot: `/opt/spclaw/docs/memory-inbox/queue.md`
- append-only ledger: `/opt/spclaw/docs/memory-inbox/reconciliation-ledger.csv`

## How to Operate From Personal Laptop
Use SSH to run runtime commands on Mac mini:

```bash
ssh <mac-mini-user>@<mac-mini-host>
cd /opt/spclaw
make openclaw-status
make openclaw-slack-status
```

Current setup uses local loopback gateway binding, so SSH is the standard remote-control method.

## New Codex Session Prompt
Use this exact prompt to resume safely:

```text
Read /opt/spclaw/AGENTS.md and /opt/spclaw/docs/handoffs/live-session.md and continue from there.
Use /opt/spclaw as the active repo.
Ship every change to git with handoff updates.
```

## Definition of Done (Every Task)
- Change is implemented and validated.
- Commit is pushed to `origin/main`.
- Handoff docs are updated:
  - `docs/handoffs/live-session.md`
  - `docs/handoffs/current-plan.md`
- OpenClaw runtime is restarted and Slack status is healthy.

## One-Time 24/7 Setup (Mac mini)
Run once to keep email + memory prune alive continuously:

```bash
cd /opt/spclaw
make openclaw-24x7-enable
make openclaw-24x7-status
```

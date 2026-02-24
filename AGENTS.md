# Coatue Claw - AGENTS Rules

## Mission
Build and operate a 24/7 equity research system for Spencer + Carson, with Slack-first interaction, durable memory, and auditable outputs.

## System Roles
- MonitorAgent: filings, macro, earnings calendar, watchlist changes
- DiligenceAgent: ticker packets, bull/bear cases, peer comps
- GraphAgent: generate chart artifacts for packets/digests
- IdeaAgent: weekly idea ranking and conviction scoring
- MemoryAgent: thesis memory, retrieval, and writeback policies
- SocialSignalAgent: X-only digest ranking and narrative clustering

## Communication
- Primary interface: Slack
- Secondary interface: CLI for development/ops
- All major outputs also written as markdown artifacts

## Operator Workflow (Laptop + Codex + Mac mini)
- Canonical production repo path on Mac mini: `/opt/coatue-claw`
- Runtime control path on Mac mini: `openclaw` CLI + `make` targets in `/opt/coatue-claw/Makefile`
- If Codex opens an empty/local mirror repo (for example `/Users/spclaw/Documents/Coatue Claw` with no commits), switch immediately to `/opt/coatue-claw` before making changes.

Required end-to-end loop for every Codex task:
1. Edit code/docs in `/opt/coatue-claw`.
2. Run validation (tests and/or compile checks appropriate to the change).
3. Commit and push to `origin/main`.
4. Update handoff docs:
   - `docs/handoffs/live-session.md`
   - `docs/handoffs/current-plan.md`
5. Restart and verify runtime on Mac mini:
   - `make openclaw-restart`
   - `make openclaw-slack-status`
   - `make openclaw-slack-logs` (if needed)

New Codex session boot prompt (recommended):
- "Read `/opt/coatue-claw/AGENTS.md` and `/opt/coatue-claw/docs/handoffs/live-session.md`, then continue from there. Use `/opt/coatue-claw` as the active repo and ship all changes to git."

Detailed runbook for humans and Codex:
- `docs/laptop-codex-openclaw-workflow.md`

## Data/Storage Contracts
- Code path: /opt/coatue-claw
- Runtime path: /opt/coatue-claw-data
- Keep runtime data out of git:
  - /opt/coatue-claw-data/db
  - /opt/coatue-claw-data/logs
  - /opt/coatue-claw-data/artifacts
- Every generated insight must include source + timestamp

## Quality Bar
- No silent failures: log and surface errors
- Every new feature needs:
  - clear input/output contract
  - basic test coverage
  - update to docs/handoffs/current-plan.md
  - update to docs/handoffs/live-session.md with current status + next steps
- Prefer small, reversible PR-sized changes

## Safety/Compliance
- Do not store secrets in git
- Use .env.prod on Mac mini only
- Keep source attribution for every claim
- Do not delete data/artifacts without explicit instruction

## Delivery Rules
- Diligence command must produce:
  - summary
  - bull case
  - bear case
  - peer comparison
  - at least 2 charts
- X digest is digest-first (not real-time alert-first)
- Slack is primary delivery channel for shared workflow

## Git Shipping Protocol (Codex Sessions)
- Every code or docs change made through Codex must be shipped to git before the session ends.
- "Shipped" means:
  - committed with a clear message
  - pushed to `origin` so the laptop can pull immediately
- Do not leave local-only deltas unless explicitly requested.
- Every ship must include handoff continuity updates:
  - `docs/handoffs/live-session.md`: current status + immediate next steps
  - `docs/handoffs/current-plan.md`: plan/status changes when scope or priority changes

## Slack X-URL Chart Rule
- For Slack requests that include an X/Twitter status URL and ask for chart-of-the-day output, use the deterministic CLI path:
  - `/opt/coatue-claw/.venv/bin/python -m coatue_claw.x_chart_daily run-post-url "<x-url>" [--channel "<channel-id>"]`
- Final chart artifact should be the source chart image snip from the X post (no Coatue-style redraw/reconstruction required).
- If source image is unavailable, return the explicit error to Slack.

## Board Seat V4 Rule (Acquisition/Acquihire)
- Applies to both:
  - scheduled board-seat runs (`board_seat_daily`)
  - conversational board-seat replies in Slack portco channels.
- Required structure:
  - `Board Seat as a Service — {Company}`
  - `Thesis` with explicit first line:
    - `Idea: Acquire/Acquihire {Target} — {one-line rationale}`
  - `Why now`, `What's different`, `MOS/risks`, `Bottom line`
  - `{Company} context` and `Funding snapshot`
  - `Sources`
- Primary recommendation must be acquisition/acquihire-oriented (no internal build-first thesis).
- Citations must be named and human-readable:
  - `Publisher/Source — Article title: <url>`
- Numeric labels like `Source 1/2/3` are disallowed.

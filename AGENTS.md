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

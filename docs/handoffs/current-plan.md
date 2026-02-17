# Coatue Claw - Current Plan

## Objective
Build a 24/7 equity research bot (Slack-first) running on Mac mini, controlled from laptop.

## V1 Scope
- SEC + transcript + macro ingestion
- Diligence packets (bull/bear + peer comp + charts)
- Weekly idea scan
- X-only digest (digest-first)
- Memory layer (SQLite + LanceDB + thesis notes)

## Infrastructure
- Repo: GitHub (CoatueClaw)
- Laptop: authoring/control
- Mac mini: runtime
- Runtime dirs: /opt/coatue-claw-data/{db,cache,logs,artifacts,backups}

## Next Actions
1. Fill AGENTS.md with roles + rules
2. Add skill stubs (new-signal, diligence-packet, memory-ingest)
3. Add basic CLI commands
4. Add Slack bot skeleton

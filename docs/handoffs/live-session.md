# Live Session Handoff (Coatue Claw)

## Objective
Ship valuation charting into the OpenClaw-native Slack workflow.

## Current Status (2026-02-18)
- Repo is synced on `main` and used as the cross-device source of truth.
- Valuation chart feature is implemented and unit-tested.
- Remaining validation is Slack mention-to-reply behavior in channel.
- Session shipping protocol is now codified in `AGENTS.md` and templated in `docs/handoffs/ship-template.md`.

## What Was Implemented
- Added valuation chart engine in `/opt/coatue-claw/src/coatue_claw/valuation_chart.py`.
- Added CLI command in `/opt/coatue-claw/src/coatue_claw/cli.py`:
  - `claw valuation-chart SNOW,MDB,DDOG`
- Added Slack command handling in `/opt/coatue-claw/src/coatue_claw/slack_bot.py`:
  - mention pattern for graph/chart + EV/LTM/growth tickers
  - uploads PNG + CSV + JSON + raw provider JSON artifacts
- Added unit tests in `/opt/coatue-claw/tests/test_valuation_chart.py`.
- Added workspace skill for OpenClaw runtime:
  - `/Users/spclaw/.openclaw/workspace/skills/valuation-charting/SKILL.md`

## Current Data Behavior
- Metric is now **EV/LTM revenue vs YoY growth**.
- LTM revenue is **sum of last 4 reported quarters**.
- Provider preference is `google` then `yahoo`.
- In this build, Google adapter is unavailable for required EV + LTM inputs, so run falls back to Yahoo.
- Quality gates include:
  - `missing_ltm_revenue`
  - `missing_debt`
  - `currency_mismatch`
  - `stale_market_data`
  - `stale_fundamentals`

## Artifacts
- Charts/data write to `/opt/coatue-claw-data/artifacts/charts/`:
  - `valuation-scatter-*.png`
  - `valuation-scatter-*.csv`
  - `valuation-scatter-*.json`
  - `valuation-scatter-*-raw.json`

## Validation Completed
- `pytest`: `6 passed` for valuation-chart unit tests.
- CLI smoke run (latest):
  - provider used: `yahoo`
  - included/excluded counts returned
  - PNG/CSV/JSON/raw generated
- OpenClaw skill recognized:
  - `openclaw skills info valuation-charting` => ready, source `openclaw-workspace`

## Next Step to Validate in Slack
Send in `#all-coatue-claw`:
- `@Coatue Claw graph ev ltm growth SNOW,MDB,DDOG,NOW,CRWD`

Then confirm bot returns:
- as-of timestamps
- provider used + fallback reason
- chart image with line of best fit
- CSV/JSON/raw attachments

## Immediate Next Steps
1. Run the Slack mention test command above in `#all-coatue-claw`.
2. Capture first failing log line via `make openclaw-slack-logs` if no reply is posted.
3. Ship fix to git with updated handoff status and next steps.

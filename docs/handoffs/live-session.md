# Live Session Handoff (Coatue Claw)

## Objective
Ship valuation charting into the OpenClaw-native Slack workflow.

## Current Status (2026-02-18)
- Repo is synced on `main` and used as the cross-device source of truth.
- Slack channel/user policy is open in OpenClaw (`groupPolicy=open`, `dmPolicy=open`, `allowFrom=["*"]`).
- Natural-language chart intent parsing is implemented:
  - detects plot/chart/graph requests
  - defaults y-axis to YoY revenue growth unless user specifies otherwise
  - supports configurable axis metrics (EV/LTM multiple, YoY growth, LTM revenue, market cap, enterprise value, debt, cash, latest quarter revenue)
- Chart footer branding text (`COATUE CLAW`) has been removed; only footnote/citation text remains.
- CSV-backed universe workflow is implemented:
  - storage path: `/opt/coatue-claw-data/db/universes/*.csv`
  - Slack natural commands: create/list/show/add/remove universes
  - chart requests with missing or underspecified tickers now prompt for source:
    - `online` discovery
    - saved `universe` CSV
  - post-chart feedback loop asks for include/exclude tickers and can rerun chart in-thread
- Chart pre-output follow-ups are now only asked when strictly necessary:
  - bot first tries auto universe match and online discovery
  - if a valid ticker set is found, it renders immediately with no extra question
  - if not, it asks for `online` vs `use universe NAME`
- Chart titles now infer context from user prompt/source (example: `Defense Stocks`) and use that as headline.
- Footer citation/footnote text now sits at the left corner since logo text was removed.
- Coatue-style median dotted line + callout has been restored for chart outputs after configurable-axis refactor.
- Category legend/key is now moved out of the plot area into the grey header zone in a dotted guide box (`Category Guide`).
- Chart outputs remain PNG + CSV + JSON + raw provider payload.
- Session shipping protocol is codified in `AGENTS.md` and templated in `docs/handoffs/ship-template.md`.

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
- Default metric orientation is **EV/LTM revenue on x-axis** and **YoY revenue growth on y-axis**.
- User can override x/y metrics in natural language (`x axis ...`, `y axis ...`, or `A vs B` phrasing).
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
- `pytest`: `13 passed` (valuation chart + chart-intent parser tests).
- CLI smoke run (latest):
  - provider used: `yahoo`
  - included/excluded counts returned
  - PNG/CSV/JSON/raw generated
- OpenClaw skill recognized:
  - `openclaw skills info valuation-charting` => ready, source `openclaw-workspace`

## Next Step to Validate in Slack
Send in `#charting`:
- `@Coatue Claw plot EV/Revenue multiples and revenue growth for SNOW,MDB,DDOG,NOW,CRWD`
- `@Coatue Claw graph SNOW,MDB,DDOG with x axis market cap and y axis ltm revenue`
- `@Coatue Claw create universe defense with PLTR,LMT,RTX,NOC,GD,LDOS`
- `@Coatue Claw make me a valuation chart for defense stocks` then reply `@Coatue Claw use universe defense` or `@Coatue Claw online`
- Confirm rendered title headline is prompt-relevant (`Defense Stocks` / similar) and footnote is left-aligned.

Then confirm bot returns:
- as-of timestamps
- provider used + fallback reason
- chart image with line of best fit
- CSV/JSON/raw attachments

## Immediate Next Steps
1. Run all Slack validation prompts above in `#charting`.
2. Validate universe CRUD commands write/read expected CSVs under `/opt/coatue-claw-data/db/universes/`.
3. If response fails, capture first failing line with `openclaw channels logs --channel slack --lines 300`.

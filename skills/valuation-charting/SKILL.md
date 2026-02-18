---
name: valuation-charting
description: Implement and operate an equity scatter chart of EV/LTM revenue vs YoY revenue growth. Use when the user asks to plan, build, validate, or troubleshoot EV/LTM vs growth charts, including metric definitions, provider standardization (Google Finance preferred, Yahoo fallback), as-of timestamp checks, data quality gates, Slack command behavior, and audit artifacts.
---

# Valuation Charting

Follow this workflow to deliver a reliable EV/LTM revenue vs YoY revenue growth chart with a line of best fit.

## Metric Definitions

Use these definitions exactly.

- YoY revenue growth: `(latest reported quarter revenue / same quarter prior year revenue) - 1`.
- LTM revenue: `sum of last 4 reported quarterly revenues`.
- Enterprise value (EV): `market cap + total debt + preferred equity + minority interest - cash and equivalents`.
- EV/LTM revenue: `EV / LTM revenue`.

Reject rows where `LTM revenue <= 0`.

## Provider Standardization

Use one provider for the full run.

- Prefer Google Finance for all required fields.
- Use Yahoo Finance only if Google cannot provide required fields.
- Do not mix providers in one run.
- If fallback occurs, state it explicitly.

## As-Of and Quality Gates

- Record `request_received_at`.
- Record `market_data_as_of`.
- Record `fundamentals_as_of`.
- Record `provider_used`.

Reject rows for:

- missing required fields
- `currency_mismatch`
- non-positive denominator
- stale market/fundamentals data

## Coatue Visual Style Prompting

When rendering chart visuals, enforce this style block:

- Typeface should mimic Coatue decks: prefer Avenir Next/Avenir/Helvetica Neue (fallback Arial/DejaVu Sans).
- Do not use emoji callout bubbles or playful stickers in chart annotations.

- Narrative headline above chart, sentence case, dark navy text.
- Subtitle under headline + strong horizontal divider line.
- Light gray canvas and slightly lighter chart panel.
- X-axis = `EV/LTM (x)`; Y-axis = `YoY growth (%)`.
- Two-color point system:
  - base universe in muted gray
  - focus regime in Coatue blue
- Add a subtle blue-shaded regime box on right-side valuation region.
- Add dashed green vertical line for current valuation marker with label:
  - `EV/LTM today = {value}x`
- Keep a linear best-fit line and print `R^2` in top-right.
- Minimal axes: no heavy grid, thin neutral axis lines, zero baseline on growth axis.
- Footer branding + source/as-of timestamp.

## OpenClaw Runtime (Required)

When asked from Slack/OpenClaw, run:

- `/opt/coatue-claw/.venv/bin/python -m coatue_claw.cli valuation-chart TICKER1,TICKER2,...`

Use artifacts from `/opt/coatue-claw-data/artifacts/charts/`.

## Slack Delivery Contract (Required)

To ensure the chart image appears in Slack:

1. Copy PNG to allowed OpenClaw media root:
   - `/Users/spclaw/.openclaw/media/charts/`
2. Return media via payload `mediaUrl` or a `MEDIA:` line pointing to copied PNG.
3. Include concise text with provider, as-of dates, and included/excluded counts.

Do not return text-only scatter values when chart output is requested.

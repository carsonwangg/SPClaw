---
name: valuation-charting
description: Implement and operate an equity scatter chart of EV/NTM revenue vs YoY revenue growth. Use when the user asks to plan, build, validate, or troubleshoot EV/NTM vs growth charts, including strict metric definitions, provider standardization (Google Finance preferred, Yahoo fallback), as-of timestamp checks, data quality gates, Slack command behavior, and audit artifacts.
---

# Valuation Charting

Follow this workflow to deliver a reliable EV/NTM revenue vs YoY revenue growth chart with a line of best fit.

## Freeze Metric Definitions

Use these definitions exactly.

- YoY revenue growth: `(latest reported quarter revenue / same quarter prior year revenue) - 1`.
- NTM revenue: `sum of next 4 quarterly revenue estimates`.
- Enterprise value (EV): `market cap + total debt + preferred equity + minority interest - cash and equivalents`.
- EV/NTM revenue: `EV / NTM revenue`.

Reject rows where `NTM revenue <= 0`.

## Enforce Provider Standardization

Use one provider for the full run.

- Prefer Google Finance for all required fields.
- Use Yahoo Finance only if Google cannot provide required fields.
- Do not mix Google and Yahoo fields in the same run.
- If provider fallback occurs, state it explicitly in output.

## Capture As-Of Dates and Freshness

Stamp all outputs with exact dates/times.

- Record `request_received_at`.
- Record `market_data_as_of` for price/EV fields.
- Record `estimates_as_of` for NTM estimate fields.
- Record `provider_used`.

When data is stale, flag clearly and include the stale reason per ticker.

## Produce Deterministic Data Contract

Emit an artifact row per ticker with at least:

- `ticker`
- `provider`
- `currency`
- `market_data_as_of`
- `estimates_as_of`
- `latest_quarter_end`
- `revenue_q`
- `revenue_q_1y`
- `yoy_growth_pct`
- `ntm_revenue`
- `market_cap`
- `total_debt`
- `preferred_equity`
- `minority_interest`
- `cash_eq`
- `enterprise_value`
- `ev_ntm_revenue`
- `quality_flags`

Treat missing required inputs as explicit exclusions, not silent fills.

## Apply Quality Gates

Gate each ticker before plotting.

- Reject if required fields are missing.
- Reject if currencies are inconsistent for EV numerator vs NTM denominator.
- Reject if denominator is non-positive.
- Reject if freshness policy fails.

Return exclusion reasons such as:

- `missing_ntm_estimates`
- `missing_debt`
- `currency_mismatch`
- `stale_market_data`
- `stale_estimates`


## Execute in OpenClaw Runtime

When asked to produce this chart from Slack/OpenClaw, execute:

- `cd /opt/coatue-claw && /opt/coatue-claw/.venv/bin/python -m coatue_claw.cli valuation-chart TICKER1,TICKER2,...`

Then return:

- PNG chart path
- CSV/JSON artifact paths
- provider used and fallback reason
- as-of timestamps
- included/excluded counts

## Render and Return Chart

Render a scatter plot where:

- x-axis: YoY revenue growth (%).
- y-axis: EV/NTM revenue (x).
- point label: ticker.
- include a linear regression line of best fit across included points.

Always return:

- Chart image.
- Provider used.
- As-of timestamps.
- Included/excluded counts.
- Exclusion list with reasons.
- Audit artifact link or attachment (`csv`/`json`).

## Implement in Small Slices

Build in this order.

1. Metric engine with pure functions and unit tests.
2. Provider adapters with run-level provider lock.
3. Dataset assembler with quality flags and timestamps.
4. Chart renderer.
5. Slack command wiring.

Keep each change small and reversible.

## Validate Accuracy Before Rollout

Use this acceptance routine.

1. Run on a 5-ticker basket.
2. Manually reconcile at least 1-2 tickers against provider pages.
3. Add regression tests from reconciled cases.
4. Launch command only after reconciliations pass.

Do not claim production readiness without passing the reconciliation step.

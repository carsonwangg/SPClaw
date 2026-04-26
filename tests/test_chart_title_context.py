from __future__ import annotations

from spclaw.chart_title_context import infer_chart_title_context


def test_infer_chart_title_context_from_prompt_theme():
    title = infer_chart_title_context("@SPClaw make me a valuation chart for defense stocks")
    assert title == "Defense Stocks"


def test_infer_chart_title_context_from_prompt_phrase():
    title = infer_chart_title_context("@SPClaw plot market cap vs growth for cloud software companies")
    assert title == "Cloud Software Companies"


def test_infer_chart_title_context_from_universe_source_label():
    title = infer_chart_title_context("", source_label="universe:defense")
    assert title == "Defense Universe"


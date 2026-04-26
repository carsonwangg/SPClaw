from __future__ import annotations

from spclaw.chart_intent import parse_chart_intent


def test_parse_chart_intent_natural_ev_rev_growth():
    intent = parse_chart_intent("@SPClaw plot EV/Revenue multiples and revenue growth for SNOW, MDB, DDOG")
    assert intent is not None
    assert intent.tickers == ["SNOW", "MDB", "DDOG"]
    assert intent.x_metric == "ev_ltm_revenue"
    assert intent.y_metric == "yoy_revenue_growth_pct"


def test_parse_chart_intent_defaults_y_to_yoy_when_unspecified():
    intent = parse_chart_intent("@SPClaw chart market cap for SNOW, MDB")
    assert intent is not None
    assert intent.tickers == ["SNOW", "MDB"]
    assert intent.x_metric == "market_cap"
    assert intent.y_metric == "yoy_revenue_growth_pct"


def test_parse_chart_intent_respects_explicit_axes():
    intent = parse_chart_intent("@SPClaw graph SNOW,MDB with x axis market cap and y axis ltm revenue")
    assert intent is not None
    assert intent.tickers == ["SNOW", "MDB"]
    assert intent.x_metric == "market_cap"
    assert intent.y_metric == "ltm_revenue"


def test_parse_chart_intent_uses_vs_orientation_when_user_specifies_pair():
    intent = parse_chart_intent("@SPClaw plot ltm revenue vs market cap for SNOW,MDB")
    assert intent is not None
    assert intent.x_metric == "market_cap"
    assert intent.y_metric == "ltm_revenue"


def test_parse_chart_intent_detects_request_even_without_tickers():
    intent = parse_chart_intent("@SPClaw make me a chart of EV/Revenue multiple vs growth")
    assert intent is not None
    assert intent.tickers == []
    assert intent.x_metric == "ev_ltm_revenue"
    assert intent.y_metric == "yoy_revenue_growth_pct"


def test_parse_chart_intent_uses_runtime_default_axes_when_unspecified():
    intent = parse_chart_intent(
        "@SPClaw chart SNOW,MDB",
        default_x_metric="market_cap",
        default_y_metric="ltm_revenue",
    )
    assert intent is not None
    assert intent.x_metric == "market_cap"
    assert intent.y_metric == "ltm_revenue"

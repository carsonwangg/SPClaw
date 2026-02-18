from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricSpec:
    metric_id: str
    label: str
    axis_kind: str


METRIC_SPECS: dict[str, MetricSpec] = {
    "ev_ltm_revenue": MetricSpec("ev_ltm_revenue", "EV / LTM Revenue", "multiple"),
    "yoy_revenue_growth_pct": MetricSpec("yoy_revenue_growth_pct", "YoY Revenue Growth", "percent"),
    "ltm_revenue": MetricSpec("ltm_revenue", "LTM Revenue", "usd"),
    "revenue_q": MetricSpec("revenue_q", "Latest Quarter Revenue", "usd"),
    "enterprise_value": MetricSpec("enterprise_value", "Enterprise Value", "usd"),
    "market_cap": MetricSpec("market_cap", "Market Cap", "usd"),
    "total_debt": MetricSpec("total_debt", "Total Debt", "usd"),
    "cash_eq": MetricSpec("cash_eq", "Cash & Equivalents", "usd"),
}

DEFAULT_X_METRIC = "ev_ltm_revenue"
DEFAULT_Y_METRIC = "yoy_revenue_growth_pct"


def metric_label(metric_id: str) -> str:
    return METRIC_SPECS[metric_id].label


def metric_axis_kind(metric_id: str) -> str:
    return METRIC_SPECS[metric_id].axis_kind


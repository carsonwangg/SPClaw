from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class ConfigIntent:
    kind: str
    key: str | None = None
    value: str | int | None = None


_METRIC_ALIASES: list[tuple[str, tuple[str, ...]]] = [
    ("ev_ltm_revenue", ("ev ltm revenue", "ev/revenue", "ev to revenue", "valuation multiple", "revenue multiple")),
    ("yoy_revenue_growth_pct", ("yoy revenue growth", "revenue growth", "yoy growth", "growth")),
    ("ltm_revenue", ("ltm revenue", "trailing twelve month revenue", "trailing 12 month revenue")),
    ("revenue_q", ("latest quarter revenue", "quarterly revenue", "current quarter revenue")),
    ("enterprise_value", ("enterprise value",)),
    ("market_cap", ("market cap", "market capitalization")),
    ("total_debt", ("total debt", "debt")),
    ("cash_eq", ("cash and equivalents", "cash equivalents", "cash")),
]


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def _resolve_metric_id(text: str) -> str | None:
    lower = text.lower()
    for metric_id, aliases in _METRIC_ALIASES:
        for phrase in aliases:
            if phrase in lower:
                return metric_id
    return None


def _parse_axis_default(lower: str, axis: str) -> str | None:
    if f"{axis}-axis" not in lower and f"{axis} axis" not in lower:
        return None
    if not re.search(r"\b(default|going forward|from now on|future|should|use)\b", lower):
        return None
    return _resolve_metric_id(lower)


def _parse_followup_prompt(stripped: str) -> str | None:
    lower = stripped.lower()
    direct = re.search(r"\b(set|change|update)\s+(?:the\s+)?follow(?:\s|-)?up\s+prompt\s*(?:to|as)?\s*(.+)", stripped, re.IGNORECASE)
    if direct:
        value = direct.group(2).strip().strip('"')
        return value if value else None

    if re.search(r"\b(when you finish a chart|after (?:each\s+)?chart|post-chart)\b", lower) and "ask" in lower:
        ask = re.search(r"\bask\b\s*(.+)", stripped, re.IGNORECASE)
        if ask:
            prompt = ask.group(1).strip().rstrip(".")
            if prompt:
                if not prompt.endswith("?"):
                    prompt = f"{prompt}?"
                return prompt
    return None


def parse_config_intent(text: str) -> ConfigIntent | None:
    stripped = _strip_slack_mentions(text)
    lower = stripped.lower()

    if re.search(r"\b(show|what are|what's|list)\b", lower) and re.search(r"\b(settings|config|configuration|defaults)\b", lower):
        return ConfigIntent(kind="show")
    if "how are you configured" in lower:
        return ConfigIntent(kind="show")

    if re.search(r"\b(promote current settings|promote these settings|make these my new defaults|ship current settings|embed .*code ?base)\b", lower):
        return ConfigIntent(kind="promote")

    if re.search(r"\b(show|list)\s+promotion\s+history\b", lower):
        return ConfigIntent(kind="history")

    if re.search(r"\b(undo|rollback|roll back|revert)\s+(?:the\s+)?last\s+promotion\b", lower):
        return ConfigIntent(kind="undo_promotion")

    peer_match = re.search(r"\b(\d{1,2})\s+(?:relevant\s+)?(?:peer|peers|comp|comps|comparables)\b", lower)
    if peer_match and re.search(r"\b(going forward|from now on|future|default|instead|look for|find|target|use)\b", lower):
        return ConfigIntent(kind="set", key="peer_discovery_limit", value=int(peer_match.group(1)))

    x_metric = _parse_axis_default(lower, "x")
    if x_metric is not None:
        return ConfigIntent(kind="set", key="default_x_metric", value=x_metric)

    y_metric = _parse_axis_default(lower, "y")
    if y_metric is not None:
        return ConfigIntent(kind="set", key="default_y_metric", value=y_metric)

    followup = _parse_followup_prompt(stripped)
    if followup is not None:
        return ConfigIntent(kind="set", key="followup_prompt", value=followup)

    if re.search(r"\b(going forward|from now on|future|defaults?)\b", lower) and re.search(r"\b(settings|config|pleas|please)\b", lower):
        return ConfigIntent(kind="help")

    return None

from __future__ import annotations

from dataclasses import dataclass
import re

from spclaw.chart_metrics import DEFAULT_X_METRIC, DEFAULT_Y_METRIC, METRIC_SPECS


@dataclass(frozen=True)
class ChartIntent:
    tickers: list[str]
    x_metric: str
    y_metric: str


_CHART_REQUEST_RE = re.compile(r"\b(plot|chart|graph|scatter|visuali[sz]e|valuation)\b", re.IGNORECASE)

_METRIC_PATTERNS: list[tuple[str, list[re.Pattern[str]]]] = [
    (
        "ev_ltm_revenue",
        [
            re.compile(r"\bev\s*/\s*(?:ltm\s*)?(?:revenue|sales)\b", re.IGNORECASE),
            re.compile(r"\bev\s*to\s*(?:ltm\s*)?(?:revenue|sales)\b", re.IGNORECASE),
            re.compile(r"\bev\s*(?:ltm\s*)?(?:revenue|sales)\s*multiple", re.IGNORECASE),
            re.compile(r"\bvaluation multiples?\b", re.IGNORECASE),
            re.compile(r"\brevenue multiples?\b", re.IGNORECASE),
            re.compile(r"\bev\s*ltm\b", re.IGNORECASE),
        ],
    ),
    (
        "yoy_revenue_growth_pct",
        [
            re.compile(r"\byoy\b", re.IGNORECASE),
            re.compile(r"\by\/y\b", re.IGNORECASE),
            re.compile(r"\brevenue growth\b", re.IGNORECASE),
            re.compile(r"\bsales growth\b", re.IGNORECASE),
            re.compile(r"\bgrowth\b", re.IGNORECASE),
        ],
    ),
    (
        "ltm_revenue",
        [
            re.compile(r"\bltm revenue\b", re.IGNORECASE),
            re.compile(r"\btrailing (?:twelve|12)\s*month revenue\b", re.IGNORECASE),
        ],
    ),
    (
        "revenue_q",
        [
            re.compile(r"\bquarterly revenue\b", re.IGNORECASE),
            re.compile(r"\bcurrent quarter revenue\b", re.IGNORECASE),
        ],
    ),
    (
        "enterprise_value",
        [
            re.compile(r"\benterprise value\b", re.IGNORECASE),
            re.compile(r"\bev\b(?!\s*/|\s*to\s*(?:revenue|sales)|\s*(?:revenue|sales)|\s*ltm)", re.IGNORECASE),
        ],
    ),
    (
        "market_cap",
        [
            re.compile(r"\bmarket cap\b", re.IGNORECASE),
            re.compile(r"\bmarket capitalization\b", re.IGNORECASE),
        ],
    ),
    (
        "total_debt",
        [
            re.compile(r"\btotal debt\b", re.IGNORECASE),
            re.compile(r"\bdebt\b", re.IGNORECASE),
        ],
    ),
    (
        "cash_eq",
        [
            re.compile(r"\bcash(?:\s*&\s*equivalents?)?\b", re.IGNORECASE),
        ],
    ),
]

_COMPANY_ALIAS_TO_TICKER: dict[str, str] = {
    "palantir": "PLTR",
    "snowflake": "SNOW",
    "mongodb": "MDB",
    "datadog": "DDOG",
    "servicenow": "NOW",
    "crowdstrike": "CRWD",
    "lockheed": "LMT",
    "raytheon": "RTX",
    "northrop": "NOC",
    "general dynamics": "GD",
    "leidos": "LDOS",
}

_TICKER_STOPWORDS = {
    "COATUE",
    "CLAW",
    "PLOT",
    "CHART",
    "GRAPH",
    "SCATTER",
    "VALUATION",
    "REVENUE",
    "GROWTH",
    "YOY",
    "EV",
    "LTM",
    "SALES",
    "MARKET",
    "CAP",
    "VS",
    "VERSUS",
    "AGAINST",
    "AND",
    "WITH",
    "FOR",
    "THE",
    "OF",
    "ON",
    "X",
    "Y",
    "AXIS",
    "MULTIPLE",
    "MULTIPLES",
    "MAKE",
    "SHOW",
    "ME",
    "A",
    "AN",
    "TO",
    "OTHER",
    "RELEVANT",
    "STOCK",
    "STOCKS",
}


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def _looks_like_chart_request(text: str) -> bool:
    lower = text.lower()
    if _CHART_REQUEST_RE.search(lower):
        return True
    if "multiple" in lower and ("growth" in lower or "revenue" in lower):
        return True
    return False


def _match_metric(text: str) -> str | None:
    for metric_id, patterns in _METRIC_PATTERNS:
        if any(p.search(text) for p in patterns):
            return metric_id
    return None


def _all_metrics(text: str) -> list[str]:
    out: list[str] = []
    for metric_id, patterns in _METRIC_PATTERNS:
        if any(p.search(text) for p in patterns):
            out.append(metric_id)
    return out


def _extract_axis_metric(text: str, axis: str) -> str | None:
    patterns = [
        rf"\b{axis}\s*axis\b(?:\s*(?:is|=|:|to|as))?\s*(.+)",
        rf"\bon the {axis}\s*axis\b(?:\s*(?:is|=|:|to|as))?\s*(.+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        snippet = match.group(1)
        snippet = re.split(r"\band\s+[xy]\s*axis\b", snippet, maxsplit=1, flags=re.IGNORECASE)[0]
        snippet = re.split(r"[,.;\n]", snippet, maxsplit=1)[0]
        metric_id = _match_metric(snippet)
        if metric_id:
            return metric_id
    return None


def _extract_vs_pair(text: str) -> tuple[str, str] | None:
    lower = text.lower()
    for sep in (" vs ", " versus ", " against ", " relative to "):
        if sep not in lower:
            continue
        left, right = lower.split(sep, 1)
        left_metric = _match_metric(left[-100:])
        right_metric = _match_metric(right[:100])
        if left_metric and right_metric:
            # "A vs B" is interpreted as y=A, x=B.
            return right_metric, left_metric
    return None


def _extract_tickers(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    lower = text.lower()

    for alias, ticker in _COMPANY_ALIAS_TO_TICKER.items():
        if re.search(rf"\b{re.escape(alias)}\b", lower):
            if ticker not in seen:
                seen.add(ticker)
                out.append(ticker)

    candidates = re.findall(r"\$?[A-Za-z][A-Za-z.\-]{0,9}", text)
    for candidate in candidates:
        ticker = candidate.upper().lstrip("$").strip(".,;:!?)]}")
        core = ticker.replace(".", "").replace("-", "")
        if not core:
            continue
        if not core.isalpha():
            continue
        if len(core) > 5:
            continue
        if ticker in _TICKER_STOPWORDS:
            continue
        if ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def parse_chart_intent(
    text: str,
    *,
    default_x_metric: str = DEFAULT_X_METRIC,
    default_y_metric: str = DEFAULT_Y_METRIC,
) -> ChartIntent | None:
    stripped = _strip_slack_mentions(text)
    if not _looks_like_chart_request(stripped):
        return None

    lower = stripped.lower()
    tickers = _extract_tickers(stripped)

    if default_x_metric not in METRIC_SPECS:
        default_x_metric = DEFAULT_X_METRIC
    if default_y_metric not in METRIC_SPECS:
        default_y_metric = DEFAULT_Y_METRIC

    x_metric = default_x_metric
    y_metric = default_y_metric
    y_specified = False
    pair_applied = False

    x_axis_metric = _extract_axis_metric(lower, "x")
    y_axis_metric = _extract_axis_metric(lower, "y")
    if x_axis_metric:
        x_metric = x_axis_metric
    if y_axis_metric:
        y_metric = y_axis_metric
        y_specified = True

    pair = _extract_vs_pair(stripped)
    if pair is not None:
        if not x_axis_metric:
            x_metric = pair[0]
        if not y_axis_metric:
            y_metric = pair[1]
        y_specified = True
        pair_applied = True

    # If user did not explicitly set y-axis, keep YoY on y-axis when present.
    if not y_axis_metric and default_y_metric in {x_metric, y_metric} and x_metric == default_y_metric and y_metric != default_y_metric:
        x_metric, y_metric = y_metric, x_metric

    metrics_found = _all_metrics(lower)
    if not x_axis_metric and not pair_applied and metrics_found:
        first_non_yoy = next((m for m in metrics_found if m != default_y_metric), None)
        if first_non_yoy:
            x_metric = first_non_yoy

    # User preference: default YoY on y-axis unless they specify otherwise.
    if not y_specified:
        y_metric = default_y_metric
        if x_metric == y_metric:
            fallback_x = next((m for m in metrics_found if m != default_y_metric), None)
            if fallback_x:
                x_metric = fallback_x

    return ChartIntent(tickers=tickers, x_metric=x_metric, y_metric=y_metric)

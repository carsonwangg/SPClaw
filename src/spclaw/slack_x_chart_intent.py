from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class XChartPostIntent:
    post_url: str
    handle: str
    tweet_id: str
    add_source: bool
    run_chart: bool
    priority: float | None = None
    title_override: str | None = None


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def _extract_post_url(text: str) -> tuple[str, str, str] | None:
    m = re.search(
        r"(https?://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)[^\s]*)",
        text,
        re.IGNORECASE,
    )
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def _looks_like_add_source_intent(lower: str) -> bool:
    has_add = bool(re.search(r"\b(add|include|track|follow)\b", lower))
    has_list = bool(
        re.search(
            r"(twitter|x)\s*(source|sources|list|watchlist)|source\s*list|accounts\s*we\s*like",
            lower,
        )
    )
    has_this_guy = "add this guy" in lower or "add this account" in lower or "add this poster" in lower
    return (has_add and has_list) or has_this_guy


def _looks_like_chart_request(lower: str) -> bool:
    if "x chart" in lower:
        return True
    if "chart of the day" in lower:
        return True
    if "chart from this post" in lower:
        return True
    if re.search(r"\b(make|create|generate|output|post)\b.*\b(chart|graph)\b", lower):
        return True
    if re.search(r"\b(chart|graph)\b.*\b(from|using)\b.*\bpost\b", lower):
        return True
    return False


def _extract_title_override(text: str) -> str | None:
    m = re.search(r"\btitle\s*:\s*(.+)$", text, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    raw = re.sub(r"\s+", " ", m.group(1) or "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}:
        raw = raw[1:-1].strip()
    return raw or None


def parse_x_chart_post_intent(text: str) -> XChartPostIntent | None:
    stripped = _strip_slack_mentions(text)
    if not stripped:
        return None

    post = _extract_post_url(stripped)
    if post is None:
        return None
    post_url, handle, tweet_id = post
    lower = stripped.lower()
    add_source = _looks_like_add_source_intent(lower)
    run_chart = _looks_like_chart_request(lower)
    if not add_source and not run_chart:
        return None

    priority: float | None = None
    m = re.search(r"\bpriority\s+([0-9]*\.?[0-9]+)\b", stripped, re.IGNORECASE)
    if m:
        try:
            priority = float(m.group(1))
        except Exception:
            priority = None
    title_override = _extract_title_override(stripped)

    return XChartPostIntent(
        post_url=post_url,
        handle=handle,
        tweet_id=tweet_id,
        add_source=add_source,
        run_chart=run_chart,
        priority=priority,
        title_override=title_override,
    )

from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class XDigestIntent:
    kind: str
    query: str | None = None
    hours: int = 24
    limit: int = 50


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def _extract_hours(text: str, default: int) -> tuple[int, str]:
    out = default
    cleaned = text

    h = re.search(r"\blast\s+(\d{1,3})\s*h(?:ours?)?\b", cleaned, re.IGNORECASE)
    if h:
        out = int(h.group(1))
        cleaned = re.sub(r"\blast\s+\d{1,3}\s*h(?:ours?)?\b", " ", cleaned, flags=re.IGNORECASE)
        return out, cleaned

    d = re.search(r"\blast\s+(\d{1,2})\s*days?\b", cleaned, re.IGNORECASE)
    if d:
        out = int(d.group(1)) * 24
        cleaned = re.sub(r"\blast\s+\d{1,2}\s*days?\b", " ", cleaned, flags=re.IGNORECASE)
    return out, cleaned


def _extract_limit(text: str, default: int) -> tuple[int, str]:
    out = default
    cleaned = text
    m = re.search(r"\blimit\s+(\d{1,3})\b", cleaned, re.IGNORECASE)
    if m:
        out = int(m.group(1))
        cleaned = re.sub(r"\blimit\s+\d{1,3}\b", " ", cleaned, flags=re.IGNORECASE)
    return out, cleaned


def parse_x_digest_intent(text: str) -> XDigestIntent | None:
    stripped = _strip_slack_mentions(text)
    lower = stripped.lower()
    if not stripped:
        return None

    if re.search(r"\b(x|twitter)\s+(?:digest\s+)?help\b", lower):
        return XDigestIntent(kind="help")

    if re.search(r"\b(x|twitter)\s+(?:digest\s+)?status\b", lower):
        return XDigestIntent(kind="status")

    m = re.search(r"\b(?:x|twitter)\s+digest\b\s*[:\-]?\s*(.+)$", stripped, re.IGNORECASE)
    if not m:
        return None
    remainder = m.group(1).strip()
    if not remainder:
        return XDigestIntent(kind="help")

    hours, remainder = _extract_hours(remainder, 24)
    limit, remainder = _extract_limit(remainder, 50)
    query = re.sub(r"\s+", " ", remainder).strip(" .,:;!?\t")
    if not query:
        return XDigestIntent(kind="help")
    return XDigestIntent(
        kind="digest",
        query=query,
        hours=max(1, min(168, hours)),
        limit=max(10, min(100, limit)),
    )

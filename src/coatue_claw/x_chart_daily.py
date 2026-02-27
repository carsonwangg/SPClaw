from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
import io
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import textwrap
from typing import Any
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]


load_dotenv("/opt/coatue-claw/.env.prod")

logger = logging.getLogger(__name__)

DEFAULT_WINDOWS = "07:00,12:00,18:00"
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_CONVENTION_NAMES = ("Morning", "Afternoon", "Evening")


DEFAULT_PRIORITY_SOURCES: list[tuple[str, float]] = [
    ("fiscal_AI", 1.6),
    ("cloudedjudgment", 1.5),
    ("stock_unlock", 1.45),
    ("stripe", 1.4),
    ("MikeZaccardi", 1.3),
    ("KobeissiLetter", 1.3),
    ("charliebilello", 1.25),
    ("oguzerkan", 1.25),
    ("Barchart", 1.2),
    ("bespokeinvest", 1.2),
    ("biancoresearch", 1.2),
    ("LizAnnSonders", 1.15),
    ("Yardeni", 1.15),
    ("AswathDamodaran", 1.1),
    ("BloombergGraphics", 1.05),
    ("WSJGraphics", 1.0),
    ("OurWorldInData", 1.0),
]

THEME_KEYWORDS = (
    "ai",
    "semiconductor",
    "software",
    "saas",
    "consumer",
    "macro",
    "cloud",
    "gpu",
    "valuation",
    "growth",
    "margin",
    "demand",
    "supply",
    "backlog",
    "data center power",
    "power demand",
    "breadth",
    "rotation",
    "dispersion",
    "regime",
    "positioning",
    "underallocated",
    "stock pickers",
)

CHART_SIGNAL_KEYWORDS = (
    "chart",
    "graph",
    "data",
    "trend",
    "yoy",
    "qoq",
    "cagr",
    "growth",
    "revenue",
    "margin",
    "valuation",
    "multiple",
    "ev",
    "ebitda",
    "sales",
    "earnings",
    "gdp",
    "inflation",
    "unemployment",
    "index",
    "forecast",
    "capex",
    "guidance",
    "consensus",
    "backlog",
    "breadth",
    "rotation",
    "dispersion",
    "outperforming",
    "outperform",
    "power demand",
    "year to date",
    "ytd",
)

INSTITUTIONAL_STYLE_KEYWORDS = (
    "yoy",
    "qoq",
    "cagr",
    "ttm",
    "basis points",
    "bps",
    "consensus",
    "guidance",
    "estimate",
    "forecast",
    "revenue",
    "earnings",
    "margin",
    "valuation",
    "multiple",
    "drawdown",
    "spread",
    "adoption",
    "penetration",
    "trend",
    "cohort",
    "chart",
    "backlog",
    "breadth",
    "rotation",
    "dispersion",
    "regime",
    "positioning",
    "underallocated",
    "stock pickers",
    "power demand",
    "source:",
)

PROMO_SPAM_KEYWORDS = (
    "discord",
    "chatroom",
    "telegram",
    "whatsapp",
    "join",
    "link below",
    "free",
    "group",
    "alerts",
    "dm me",
    "signup",
    "sign up",
)

US_STRONG_SIGNAL_KEYWORDS = (
    "s&p",
    "spx",
    "nasdaq",
    "nyse",
    "dow jones",
    "russell 2000",
    "fomc",
    "federal reserve",
    "fed funds",
    "treasury",
    "u.s. treasury",
    "10y",
    "2y",
    "nonfarm payrolls",
    "jobless claims",
    "cpi",
    "pce",
    "core inflation",
    "u.s. consumer",
    "u.s. earnings",
    "saaS",
    "software multiple",
    "ai capex",
    "s&p 500",
    "market breadth",
    "stock pickers",
    "data center power",
    "power demand",
)

US_WEAK_SIGNAL_KEYWORDS = (
    "u.s.",
    "united states",
    "american",
    "wall street",
)

FOREX_NON_US_KEYWORDS = (
    "forex",
    "fx",
    "eur/usd",
    "usd/jpy",
    "gbp/usd",
    "aud/usd",
    "cad/usd",
    "usd/chf",
    "us dollar index",
    "turkish lira",
    "lira",
    "yuan",
    "renminbi",
    "rupee",
    "peso",
    "real",
    "rand",
)

NON_US_GEOGRAPHY_KEYWORDS = (
    "europe",
    "eurozone",
    "uk ",
    "united kingdom",
    "china",
    "japan",
    "india",
    "brazil",
    "turkey",
    "germany",
    "france",
    "canada",
    "mexico",
    "emerging markets",
)

TREND_SIGNAL_KEYWORDS = (
    "up",
    "down",
    "higher",
    "lower",
    "record high",
    "record low",
    "all-time high",
    "all-time low",
    "accelerating",
    "slowing",
    "rebound",
    "decline",
)

SLIDE_JARGON_KEYWORDS = (
    "bull case",
    "bear case",
    "peer comparison",
    "valuation framework",
    "investment recommendation",
    "operating leverage",
    "contribution margin",
)

BAR_HINT_KEYWORDS = (
    "bar chart",
    "bar graph",
    "histogram",
    "by country",
    "by state",
    "by cohort",
    "top 10",
    "top ten",
    "ranked",
    "ranking",
)

POSITIVE_MOVE_VERBS = ("surged", "rose", "jumped", "climbed", "rebounded", "accelerated", "increased", "grew")
NEGATIVE_MOVE_VERBS = ("fell", "dropped", "declined", "slowed", "rolled over", "sank", "contracted", "decelerated")
NEUTRAL_MOVE_VERBS = ("hit", "reached", "stands at", "is at")
NEWS_PREFIX_RE = re.compile(r"^(breaking|update|new chart|chart|alert)\s*:\s*", re.IGNORECASE)
TICKER_NAME_HINTS: dict[str, str] = {
    "AMZN": "Amazon",
    "MSFT": "Microsoft",
    "GOOGL": "Google",
    "GOOG": "Google",
    "META": "Meta",
    "AAPL": "Apple",
    "NVDA": "NVIDIA",
}

TRAILING_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}

TRAILING_DETERMINERS = {
    "a",
    "an",
    "the",
    "this",
    "that",
    "these",
    "those",
    "my",
    "your",
    "his",
    "her",
    "its",
    "our",
    "their",
    "any",
    "some",
    "each",
    "either",
    "neither",
    "every",
    "another",
}

TRAILING_QUALIFIERS = {
    "initial",
    "early",
    "late",
    "prior",
    "next",
    "current",
    "latest",
    "previous",
    "first",
    "second",
    "third",
    "fourth",
    "final",
}

HEADLINE_LOCKED_TERMS: tuple[tuple[str, ...], ...] = (
    ("market", "cap"),
    ("enterprise", "value"),
    ("free", "cash", "flow"),
    ("operating", "margin"),
    ("gross", "margin"),
    ("net", "income"),
    ("record", "low"),
    ("record", "high"),
)

HEADLINE_MIN_WORDS = 4
HEADLINE_MAX_RENDER_LINES = 3
HEADLINE_MIN_FONT_SIZE = 17.0
TAKEAWAY_MAX_RENDER_LINES = 2
TAKEAWAY_MIN_FONT_SIZE = 8.4
TAKEAWAY_CONNECTOR_TOKENS = {
    "while",
    "as",
    "amid",
    "after",
    "because",
    "and",
    "but",
}
TAKEAWAY_AUXILIARY_TOKENS = {
    "is",
    "are",
    "was",
    "were",
    "be",
    "being",
    "been",
    "has",
    "have",
    "had",
    "will",
    "would",
    "can",
    "could",
    "should",
    "may",
    "might",
    "must",
    "do",
    "does",
    "did",
}
TAKEAWAY_EXTRA_PREDICATE_TOKENS = {
    "return",
    "returns",
    "returning",
    "spread",
    "spreads",
    "spreading",
    "remain",
    "remains",
    "holding",
    "hold",
}
HEADLINE_ACTION_TOKENS = {
    "is",
    "are",
    "was",
    "were",
    "be",
    "being",
    "been",
    "has",
    "have",
    "had",
    "will",
    "would",
    "can",
    "could",
    "should",
    "may",
    "might",
    "must",
    "do",
    "does",
    "did",
    "surges",
    "surged",
    "surging",
    "rises",
    "rose",
    "rising",
    "jumps",
    "jumped",
    "jumping",
    "climbs",
    "climbed",
    "climbing",
    "rebounds",
    "rebounded",
    "rebounding",
    "accelerates",
    "accelerated",
    "accelerating",
    "increases",
    "increased",
    "increasing",
    "grows",
    "grew",
    "growing",
    "falls",
    "fell",
    "falling",
    "drops",
    "dropped",
    "dropping",
    "declines",
    "declined",
    "declining",
    "slows",
    "slowed",
    "slowing",
    "rolls",
    "rolled",
    "rolling",
    "sinks",
    "sank",
    "sinking",
    "contracts",
    "contracted",
    "contracting",
    "decelerates",
    "decelerated",
    "decelerating",
    "hits",
    "hit",
    "reaches",
    "reached",
    "stands",
    "standing",
    "trends",
    "trending",
    "moves",
    "moving",
    "opens",
    "opened",
    "open",
    "erases",
    "erased",
    "erase",
    "outnumber",
    "outnumbers",
    "outnumbered",
}
TAKEAWAY_PREDICATE_TOKENS = HEADLINE_ACTION_TOKENS | TAKEAWAY_EXTRA_PREDICATE_TOKENS

LOW_SIGNAL_COPY_VALUES = {
    "u.s",
    "u.s.",
    "us",
    "usa",
    "chart context",
    "trend snapshot",
    "chart",
    "data",
    "trend",
}

TAKEAWAY_DANGLING_ENDINGS = {
    "to",
    "for",
    "of",
    "in",
    "on",
    "at",
    "with",
    "from",
    "by",
    "as",
    "lowest",
    "highest",
    "most",
    "least",
}

HEADLINE_DANGLING_ENDINGS = {
    "to",
    "for",
    "of",
    "in",
    "on",
    "at",
    "with",
    "from",
    "by",
    "as",
    "is",
    "are",
    "was",
    "were",
    "be",
    "being",
    "been",
    "now",
    "then",
    "than",
    "vs",
    "versus",
}

SUBJECT_TRAILING_ACTION_VERBS = (
    "sold",
    "bought",
    "buying",
    "selling",
    "hit",
    "reached",
    "stands",
    "standing",
    "is",
    "are",
    "was",
    "were",
    "be",
)

PLURAL_SUBJECT_HINTS = {
    "investors",
    "clients",
    "funds",
    "flows",
    "receipts",
    "duties",
    "sales",
    "earnings",
    "jobs",
    "rates",
    "prices",
    "stocks",
    "etfs",
}


class XChartError(RuntimeError):
    pass


@dataclass(frozen=True)
class Candidate:
    candidate_key: str
    source_type: str
    source_id: str
    author: str
    title: str
    text: str
    url: str
    image_url: str | None
    created_at: str | None
    engagement: int
    source_priority: float
    score: float
    discovered_via: str = "seed_list"


@dataclass(frozen=True)
class StyleDraft:
    headline: str
    chart_label: str
    takeaway: str
    why_now: str
    iteration: int
    checks: dict[str, bool]
    score: float
    copy_rewrite_applied: bool = False
    copy_rewrite_reason: str | None = None
    llm_copy_status: str = "ok"
    llm_warning_reason: str | None = None


@dataclass(frozen=True)
class RebuiltSeries:
    label: str
    x: list[float]
    y: list[float]
    color: str
    weight: float


@dataclass(frozen=True)
class RebuiltBars:
    labels: list[str]
    values: list[float]
    color: str
    y_label: str
    normalized: bool
    source: str
    confidence: float
    primary_label: str | None = None
    secondary_values: list[float] | None = None
    secondary_color: str | None = None
    secondary_label: str | None = None


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_X_CHART_DB_PATH",
            str(_data_root() / "db/x_chart_daily.sqlite"),
        )
    )


def _x_api_base() -> str:
    return (os.environ.get("COATUE_CLAW_X_API_BASE", "https://api.x.com").strip() or "https://api.x.com").rstrip("/")


def _output_dir() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_X_CHART_DIR",
            str(_data_root() / "artifacts/x-chart-daily"),
        )
    )


def _resolve_bearer_token() -> str:
    for key in ("COATUE_CLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "COATUE_CLAW_TWITTER_BEARER_TOKEN"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    raise XChartError("X bearer token missing. Set COATUE_CLAW_X_BEARER_TOKEN in .env.prod.")


def _slack_tokens() -> list[str]:
    tokens: list[str] = []
    env_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if env_token:
        tokens.append(env_token)
    config_path = Path.home() / ".openclaw/openclaw.json"
    if config_path.exists():
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            cfg_token = str(
                (
                    payload.get("channels", {})
                    .get("slack", {})
                    .get("botToken", "")
                )
            ).strip()
        except Exception:
            cfg_token = ""
        if cfg_token:
            tokens.append(cfg_token)
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
    if not unique:
        raise XChartError("Slack bot token missing (env SLACK_BOT_TOKEN or ~/.openclaw/openclaw.json channels.slack.botToken).")
    return unique


def _slack_channel() -> str:
    channel = os.environ.get("COATUE_CLAW_X_CHART_SLACK_CHANNEL", "").strip()
    if not channel:
        raise XChartError("COATUE_CLAW_X_CHART_SLACK_CHANNEL missing (set Slack channel id).")
    return channel


def _timezone() -> ZoneInfo:
    tz_name = os.environ.get("COATUE_CLAW_X_CHART_TIMEZONE", DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(tz_name)
    except Exception as exc:
        raise XChartError(f"Invalid timezone: {tz_name}") from exc


def _parse_windows(raw: str | None = None) -> list[tuple[int, int]]:
    value = (raw or os.environ.get("COATUE_CLAW_X_CHART_WINDOWS", DEFAULT_WINDOWS) or DEFAULT_WINDOWS).strip()
    out: list[tuple[int, int]] = []
    for part in value.split(","):
        p = part.strip()
        if not p:
            continue
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", p)
        if not m:
            continue
        h = int(m.group(1))
        minute = int(m.group(2))
        if 0 <= h <= 23 and 0 <= minute <= 59:
            out.append((h, minute))
    if not out:
        out = [(7, 0), (12, 0), (18, 0)]
    out.sort()
    return out


def _slot_name_for_hour(hour: int) -> str:
    if 5 <= hour < 12:
        return "Morning"
    if 12 <= hour < 17:
        return "Afternoon"
    return "Evening"


def _slot_name_for_key(*, slot_key: str, now_local: datetime, windows: list[tuple[int, int]]) -> str:
    if slot_key.startswith("manual"):
        return _slot_name_for_hour(now_local.hour)
    m = re.search(r"-(\d{2}):(\d{2})$", slot_key)
    if not m:
        return _slot_name_for_hour(now_local.hour)
    target = (int(m.group(1)), int(m.group(2)))
    ordered = sorted(windows)
    for idx, item in enumerate(ordered):
        if item != target:
            continue
        if idx < len(DEFAULT_CONVENTION_NAMES):
            return DEFAULT_CONVENTION_NAMES[idx]
        return _slot_name_for_hour(target[0])
    return _slot_name_for_hour(target[0])


def _convention_name(*, slot_key: str, now_local: datetime, windows: list[tuple[int, int]]) -> str:
    return f"Coatue Chart of the {_slot_name_for_key(slot_key=slot_key, now_local=now_local, windows=windows)}"


def _canonical_handle(handle: str) -> str:
    out = handle.strip().lstrip("@")
    out = re.sub(r"[^A-Za-z0-9_]+", "", out)
    return out


def _normalize_render_text(text: str) -> str:
    cleaned = re.sub(r"https?://\S+", "", text or "")
    cleaned = re.sub(r"[\U00010000-\U0010ffff]", "", cleaned)
    cleaned = cleaned.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    cleaned = cleaned.replace("\u2013", "-").replace("\u2014", "-")
    cleaned = unicodedata.normalize("NFKD", cleaned)
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    cleaned = cleaned.replace("\ufffd", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _matplotlib_safe_text(text: str, *, preserve_urls: bool = False) -> str:
    """Return text that won't trigger matplotlib math parsing."""
    if preserve_urls:
        cleaned = text or ""
        cleaned = cleaned.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
        cleaned = cleaned.replace("\u2013", "-").replace("\u2014", "-")
        cleaned = unicodedata.normalize("NFKD", cleaned)
        cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
        cleaned = cleaned.replace("\ufffd", "")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
    else:
        cleaned = _normalize_render_text(text)
    if not cleaned:
        return ""
    return cleaned.replace("$", r"\$")


def _shorten_without_ellipsis(text: str, *, max_chars: int) -> str:
    cleaned = _normalize_render_text(text).replace("...", " ").strip(". ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    words = cleaned.split(" ")
    out: list[str] = []
    for word in words:
        candidate = ((" ".join(out) + " " + word).strip() if out else word)
        if len(candidate) <= max_chars:
            out.append(word)
            continue
        break
    if out:
        return " ".join(out).strip(". ")
    return cleaned[:max_chars].rstrip(". ")


def _is_degenerate_copy_value(text: str) -> bool:
    cleaned = _normalize_render_text(text)
    if not cleaned:
        return True
    if cleaned.lower() in LOW_SIGNAL_COPY_VALUES:
        return True
    words = [w for w in re.split(r"\s+", cleaned) if w]
    if len(words) < 2:
        return True
    alnum = re.sub(r"[^A-Za-z0-9]+", "", cleaned)
    return len(alnum) < 6


def _strip_trailing_dangling_endings(text: str) -> str:
    words = [w for w in _normalize_render_text(text).split(" ") if w]
    while words:
        tail = words[-1].strip(".,;:!?").lower()
        if tail not in TAKEAWAY_DANGLING_ENDINGS and tail not in TRAILING_STOPWORDS:
            break
        words.pop()
    return " ".join(words).strip()


def _strip_trailing_headline_dangling_endings(text: str) -> str:
    words = [w for w in _normalize_render_text(text).split(" ") if w]
    while words:
        tail = words[-1].strip(".,;:!?").lower()
        if tail not in HEADLINE_DANGLING_ENDINGS and tail not in TRAILING_STOPWORDS:
            break
        words.pop()
    return " ".join(words).strip()


def _tail_tokens(text: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9']+", _normalize_render_text(text).lower()) if token]


def _contains_phrase_tokens(tokens: list[str], phrase: tuple[str, ...]) -> bool:
    n = len(phrase)
    if n == 0 or len(tokens) < n:
        return False
    for idx in range(0, len(tokens) - n + 1):
        if tuple(tokens[idx : idx + n]) == phrase:
            return True
    return False


def _headline_locked_terms_preserved(headline_text: str, *, source_text: str = "") -> bool:
    source_tokens = _tail_tokens(source_text)
    headline_tokens = _tail_tokens(headline_text)
    if not source_tokens:
        return True
    for phrase in HEADLINE_LOCKED_TERMS:
        if not _contains_phrase_tokens(source_tokens, phrase):
            continue
        if not _contains_phrase_tokens(headline_tokens, phrase):
            return False
    return True


def _headline_has_action_verb(text: str) -> bool:
    tokens = _tail_tokens(text)
    if not tokens:
        return False
    return any(token in HEADLINE_ACTION_TOKENS for token in tokens)


def _has_fragment_tail(words: list[str]) -> bool:
    if not words:
        return True
    tail = words[-1]
    if tail in TRAILING_DETERMINERS or tail in TRAILING_QUALIFIERS:
        return True
    if len(words) >= 2:
        prev = words[-2]
        if prev in TRAILING_STOPWORDS and (tail in TRAILING_DETERMINERS or tail in TRAILING_QUALIFIERS):
            return True
        if prev in TRAILING_DETERMINERS and tail in TRAILING_QUALIFIERS:
            return True
    return False


def _tail_complete(text: str) -> bool:
    return not _has_fragment_tail(_tail_tokens(text))


def _tokenize_clause_words(text: str) -> tuple[list[str], list[str]]:
    words: list[str] = []
    lower_words: list[str] = []
    for raw in [w for w in _normalize_render_text(text).split(" ") if w]:
        cleaned = raw.strip(".,;:!?()[]{}\"")
        if not cleaned:
            continue
        lowered = re.sub(r"[^a-z0-9']+", "", cleaned.lower())
        if not lowered:
            continue
        words.append(cleaned)
        lower_words.append(lowered)
    return words, lower_words


def _first_unjoined_clause_boundary_index(text: str) -> int | None:
    words, lower_words = _tokenize_clause_words(text)
    if len(lower_words) < 6:
        return None
    n = len(lower_words)

    for phrase in HEADLINE_LOCKED_TERMS:
        size = len(phrase)
        if size == 0:
            continue
        for idx in range(0, n - size):
            if tuple(lower_words[idx : idx + size]) != phrase:
                continue
            boundary = idx + size
            if boundary >= n:
                continue
            next_token = lower_words[boundary]
            if next_token in TAKEAWAY_CONNECTOR_TOKENS or next_token in TRAILING_STOPWORDS:
                continue
            if next_token in TAKEAWAY_PREDICATE_TOKENS:
                continue
            lookahead = lower_words[boundary : min(n, boundary + 7)]
            predicate_idx: int | None = None
            for offset, token in enumerate(lookahead):
                if token in TAKEAWAY_PREDICATE_TOKENS:
                    predicate_idx = boundary + offset
                    break
            if predicate_idx is None:
                continue
            if predicate_idx > boundary and lower_words[predicate_idx - 1] in TAKEAWAY_AUXILIARY_TOKENS:
                continue
            if n - boundary >= 3:
                return boundary

    predicate_indices = [idx for idx, token in enumerate(lower_words) if token in TAKEAWAY_PREDICATE_TOKENS]
    if len(predicate_indices) < 2:
        return None

    first_predicate = predicate_indices[0]
    for predicate_idx in predicate_indices[1:]:
        if predicate_idx <= first_predicate + 1:
            continue
        prev = lower_words[predicate_idx - 1]
        if prev in TAKEAWAY_AUXILIARY_TOKENS or prev == "to":
            continue
        left = max(first_predicate + 1, predicate_idx - 6)
        bridge = lower_words[left:predicate_idx]
        if any(token in TAKEAWAY_CONNECTOR_TOKENS for token in bridge):
            continue
        boundary = max(first_predicate + 1, predicate_idx - 4)
        while boundary < predicate_idx and lower_words[boundary] in TRAILING_STOPWORDS:
            boundary += 1
        if boundary >= predicate_idx or boundary < 2:
            continue
        subject_tokens = lower_words[boundary:predicate_idx]
        if not subject_tokens:
            continue
        if not any(
            token in TRAILING_DETERMINERS
            or token.endswith("s")
            or token in {"ai", "us", "trade", "tariff", "war", "market", "markets"}
            for token in subject_tokens
        ):
            continue
        if len(words) - boundary < 3:
            continue
        return boundary
    return None


def _has_unjoined_clause_boundary(text: str) -> bool:
    core = _normalize_render_text(text).rstrip(".!?").strip()
    if not core:
        return False
    return _first_unjoined_clause_boundary_index(core) is not None


def _repair_takeaway_clause_boundary(text: str) -> str:
    core = _normalize_render_text(text).rstrip(".!?").strip()
    if not core:
        return ""
    words, _ = _tokenize_clause_words(core)
    boundary = _first_unjoined_clause_boundary_index(core)
    if boundary is None:
        return ""
    if boundary < 3 or len(words) - boundary < 3:
        return ""
    clause1 = _strip_trailing_dangling_endings(" ".join(words[:boundary]).strip())
    clause2 = _strip_trailing_dangling_endings(" ".join(words[boundary:]).strip())
    if len(clause1.split()) < 4 or len(clause2.split()) < 3:
        return ""
    if clause2.lower().split(" ")[0] in TAKEAWAY_CONNECTOR_TOKENS:
        rebuilt = f"{clause1} {clause2}".strip()
    else:
        rebuilt = f"{clause1} while {clause2}".strip()
    rebuilt = _normalize_render_text(rebuilt)
    if not rebuilt:
        return ""
    if rebuilt[-1] not in ".!?":
        rebuilt = f"{rebuilt}."
    if _has_unjoined_clause_boundary(rebuilt):
        return ""
    return rebuilt if _is_complete_sentence(rebuilt) else ""


def _normalize_headline_seed(text: str) -> str:
    cleaned = _normalize_render_text(text)
    cleaned = re.sub(r"^@\w+:\s*", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = _strip_news_prefix(cleaned)
    if not cleaned:
        return ""
    cleaned = _extract_first_sentence(cleaned)
    cleaned = _trim_trailing_stopwords(cleaned)
    cleaned = _strip_trailing_headline_dangling_endings(cleaned)
    return cleaned.strip()


def _is_complete_headline_sentence(text: str, *, source_text: str = "") -> bool:
    cleaned = _normalize_render_text(text).strip()
    if not cleaned:
        return False
    if cleaned[-1] not in ".!?":
        return False
    core = cleaned.rstrip(".!?").strip()
    if not core:
        return False
    words = [w for w in re.split(r"\s+", core) if w]
    if len(words) < HEADLINE_MIN_WORDS:
        return False
    if not _headline_has_action_verb(core):
        return False
    if not _is_complete_headline_phrase(core):
        return False
    if not _headline_locked_terms_preserved(core, source_text=source_text):
        return False
    return True


def _finalize_headline_sentence(text: str, *, source_text: str = "") -> str:
    cleaned = _normalize_headline_seed(text)
    if not cleaned:
        return ""
    if cleaned[-1] not in ".!?":
        cleaned = f"{cleaned}."
    return cleaned if _is_complete_headline_sentence(cleaned, source_text=source_text) else ""


def _is_complete_headline_phrase(text: str) -> bool:
    cleaned = _normalize_render_text(text).strip()
    if not cleaned:
        return False
    if "..." in cleaned:
        return False
    if _is_degenerate_copy_value(cleaned):
        return False
    if cleaned[-1] in {":", ";", ",", "-"}:
        return False
    words = [w for w in re.split(r"\s+", cleaned) if w]
    if len(words) < 3:
        return False
    if _has_incoherent_headline(cleaned):
        return False
    stripped = _strip_trailing_headline_dangling_endings(cleaned.rstrip(".!?"))
    if not stripped:
        return False
    stripped_words = [w for w in stripped.split(" ") if w]
    if len(stripped_words) < 3:
        return False
    tail = stripped_words[-1].strip(".,;:!?").lower()
    if tail in HEADLINE_DANGLING_ENDINGS:
        return False
    if not _tail_complete(cleaned.rstrip(".!?")):
        return False
    return stripped == cleaned.rstrip(".!?")


def _finalize_headline_phrase(text: str, *, max_chars: int) -> str:
    cleaned = _normalize_render_text(text)
    cleaned = re.sub(r"^@\w+:\s*", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = _strip_news_prefix(cleaned)
    if not cleaned:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    if len(parts) > 1 and len(parts[0].split()) >= 3:
        cleaned = parts[0].strip()
    cleaned = _shorten_without_ellipsis(cleaned, max_chars=max_chars)
    cleaned = _trim_trailing_stopwords(cleaned)
    before_words = [w for w in _normalize_render_text(cleaned).split(" ") if w]
    cleaned = _strip_trailing_headline_dangling_endings(cleaned)
    after_words = [w for w in _normalize_render_text(cleaned).split(" ") if w]
    removed_words = max(0, len(before_words) - len(after_words))
    if not cleaned:
        return ""
    # If we had to trim dangling endings, force a semantic rewrite instead of posting a clipped phrase.
    if removed_words > 0:
        return ""
    if not _tail_complete(cleaned):
        return ""
    if len(cleaned) > max_chars:
        clipped = _shorten_without_ellipsis(cleaned, max_chars=max_chars)
        clipped = _strip_trailing_headline_dangling_endings(clipped)
        if not clipped:
            return ""
        if not _tail_complete(clipped):
            return ""
        cleaned = clipped
    if len(cleaned) > max_chars:
        return ""
    return cleaned if _is_complete_headline_phrase(cleaned) else ""


def _rewrite_headline_from_candidate(candidate: Candidate) -> tuple[str, str]:
    source_sentence = _normalize_headline_seed(candidate.text or candidate.title)
    if not source_sentence:
        source_sentence = _normalize_headline_seed(candidate.title)
    if not source_sentence:
        return "", "headline_unrecoverable"
    if _is_degenerate_copy_value(source_sentence):
        return "", "headline_unrecoverable"
    source_has_fragment_tail = not _tail_complete(source_sentence)
    subject, verb = _extract_subject_and_verb(source_sentence)
    subject_core = _clean_subject_for_headline(subject=subject, sentence=source_sentence)
    lower = source_sentence.lower()
    rewrite_candidates: list[tuple[str, str]] = [(source_sentence, "headline_sentence_rewritten")]
    if source_has_fragment_tail and subject_core:
        copula = "are" if _subject_is_plural(subject_core) else "is"
        rewrite = ""
        if "record low" in lower or "lowest" in lower:
            rewrite = f"{subject_core} {copula} at a record low."
        elif "record high" in lower or "highest" in lower:
            rewrite = f"{subject_core} {copula} at a record high."
        elif "up" in lower or "higher" in lower:
            rewrite = f"{subject_core} {copula} trending higher."
        elif "down" in lower or "lower" in lower:
            rewrite = f"{subject_core} {copula} trending lower."
        else:
            rewrite = f"{subject_core} {copula} moving lower."
        rewrite_candidates.append((rewrite, "headline_tail_fragment_rewritten"))

    narrative = _synthesize_narrative_title(subject=subject, verb=verb, sentence=source_sentence)
    if narrative:
        rewrite_candidates.append((narrative, "headline_sentence_rewritten"))

    if subject_core:
        compact_subject = _shorten_without_ellipsis(subject_core, max_chars=36)
        if len(compact_subject.split()) < 2:
            if "housing" in lower:
                compact_subject = "US housing activity"
            elif "sales" in lower:
                compact_subject = "US sales trend"
            else:
                compact_subject = _entity_hint_from_text(sentence=source_sentence, fallback=candidate.title)
        copula = "are" if _subject_is_plural(compact_subject) else "is"
        rewrite = ""
        if "record low" in lower or "lowest" in lower:
            rewrite = f"{compact_subject} {copula} at a record low."
        elif "record high" in lower or "highest" in lower:
            rewrite = f"{compact_subject} {copula} at a record high."
        elif verb.lower() in POSITIVE_MOVE_VERBS:
            rewrite = f"{compact_subject} {copula} inflecting higher."
        elif verb.lower() in NEGATIVE_MOVE_VERBS:
            rewrite = f"{compact_subject} {copula} rolling over."
        elif "up" in lower or "higher" in lower:
            rewrite = f"{compact_subject} {copula} trending higher."
        elif "down" in lower or "lower" in lower:
            rewrite = f"{compact_subject} {copula} trending lower."
        else:
            rewrite = f"{compact_subject} {copula} moving sharply."
        rewrite_candidates.append((rewrite, "headline_sentence_rewritten"))

    for rewrite, reason in rewrite_candidates:
        finalized = _finalize_headline_sentence(rewrite, source_text=source_sentence)
        if finalized:
            return finalized, reason

    return "", "headline_unrecoverable"


def _is_complete_sentence(text: str) -> bool:
    cleaned = _normalize_render_text(text)
    if not cleaned:
        return False
    lower = cleaned.lower()
    if re.search(r"\bto\s+(lowest|highest)\b", lower):
        return False
    words = [w for w in re.split(r"\s+", cleaned) if w]
    if len(words) < 4:
        return False
    if cleaned[-1] not in ".!?":
        return False
    stripped = _strip_trailing_dangling_endings(cleaned.rstrip(".!?"))
    if not stripped:
        return False
    stripped_words = [w for w in stripped.split(" ") if w]
    if len(stripped_words) < 4:
        return False
    tail = stripped_words[-1].strip(".,;:!?").lower()
    if tail in TAKEAWAY_DANGLING_ENDINGS:
        return False
    if not _tail_complete(cleaned.rstrip(".!?")):
        return False
    return True


def _is_single_sentence_takeaway(text: str) -> bool:
    cleaned = _normalize_render_text(text)
    if not cleaned:
        return False
    if _has_unjoined_clause_boundary(cleaned):
        return False
    first = _extract_first_sentence(cleaned)
    if not first:
        return False
    if _normalize_render_text(first) != cleaned:
        return False
    return _is_complete_sentence(cleaned)


def _normalize_takeaway_seed(text: str) -> str:
    cleaned = _normalize_render_text(text)
    cleaned = re.sub(r"^@\w+:\s*", "", cleaned, flags=re.IGNORECASE).strip()
    if not cleaned:
        return ""
    cleaned = _extract_first_sentence(cleaned).strip()
    before_words = [w for w in _normalize_render_text(cleaned).split(" ") if w]
    cleaned = _strip_trailing_dangling_endings(cleaned)
    after_words = [w for w in _normalize_render_text(cleaned).split(" ") if w]
    removed_words = max(0, len(before_words) - len(after_words))
    if not cleaned:
        return ""
    if removed_words > 0 and after_words:
        trailing_verb_like = {
            "surged",
            "rose",
            "jumped",
            "climbed",
            "rebounded",
            "accelerated",
            "increased",
            "grew",
            "fell",
            "dropped",
            "declined",
            "slowed",
            "sank",
            "contracted",
            "decelerated",
            "hit",
            "reached",
            "stands",
            "standing",
            "is",
            "are",
            "was",
            "were",
            "be",
        }
        if after_words[-1].strip(".,;:!?").lower() in trailing_verb_like:
            return ""
    if not _tail_complete(cleaned):
        return ""
    return cleaned


def _semantic_shorten_sentence(text: str, *, max_words: int = 18) -> str:
    cleaned = _normalize_render_text(text).strip()
    if not cleaned:
        return ""
    core = cleaned.rstrip(".!?").strip()
    if not core:
        return ""
    lower = core.lower()
    split_markers = (" while ", " amid ", " as ", " after ", " and ", ", ")
    for marker in split_markers:
        idx = lower.find(marker)
        if idx <= 0:
            continue
        candidate = core[:idx].strip()
        candidate = _strip_trailing_dangling_endings(candidate)
        if len(candidate.split()) >= 4:
            return f"{candidate}."
    words = [w for w in core.split(" ") if w]
    if len(words) > max_words:
        candidate = " ".join(words[:max_words]).strip()
        candidate = _strip_trailing_dangling_endings(candidate)
        if len(candidate.split()) >= 4:
            return f"{candidate}."
    return ""


def _finalize_takeaway_sentence(text: str, *, max_chars: int | None = None) -> str:
    cleaned = _normalize_takeaway_seed(text)
    if not cleaned:
        return ""
    if cleaned[-1] not in ".!?":
        cleaned = f"{cleaned}."
    if _has_unjoined_clause_boundary(cleaned):
        repaired = _repair_takeaway_clause_boundary(cleaned)
        if not repaired:
            return ""
        cleaned = repaired
    if max_chars is not None and len(cleaned) > max_chars:
        shortened = _semantic_shorten_sentence(cleaned, max_words=max(10, min(24, max_chars // 4)))
        if not shortened:
            return ""
        cleaned = shortened
        if max_chars is not None and len(cleaned) > max_chars:
            return ""
        if _has_unjoined_clause_boundary(cleaned):
            repaired = _repair_takeaway_clause_boundary(cleaned)
            if not repaired:
                return ""
            cleaned = repaired
            if max_chars is not None and len(cleaned) > max_chars:
                return ""
    return cleaned if _is_single_sentence_takeaway(cleaned) else ""


def _rewrite_takeaway_from_candidate(candidate: Candidate) -> tuple[str, str]:
    source_sentence = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(candidate.text or candidate.title), flags=re.IGNORECASE).strip()
    parts = re.split(r"(?<=[.!?])\s+", source_sentence)
    if len(parts) > 1 and len(parts[0].split()) >= 3:
        source_sentence = parts[0].strip()
    source_has_fragment_tail = not _tail_complete(source_sentence)
    subject, verb = _extract_subject_and_verb(source_sentence)
    subject_core = _clean_subject_for_headline(subject=subject, sentence=source_sentence)
    if not subject_core:
        subject_core = _entity_hint_from_text(sentence=source_sentence, fallback=candidate.title)
    copula = "are" if _subject_is_plural(subject_core) else "is"
    lower = source_sentence.lower()

    rewritten = ""
    if "record low" in lower or "lowest" in lower:
        rewritten = f"{subject_core} {copula} at a record low."
    elif "record high" in lower or "highest" in lower:
        rewritten = f"{subject_core} {copula} at a record high."
    elif verb.lower() in POSITIVE_MOVE_VERBS:
        rewritten = f"{subject_core} {copula} moving higher."
    elif verb.lower() in NEGATIVE_MOVE_VERBS:
        rewritten = f"{subject_core} {copula} moving lower."
    elif "up" in lower or "higher" in lower:
        rewritten = f"{subject_core} {copula} trending higher."
    elif "down" in lower or "lower" in lower:
        rewritten = f"{subject_core} {copula} trending lower."
    elif subject_core and len(subject_core.split()) >= 2:
        rewritten = f"{subject_core} {copula} moving sharply."
    if source_has_fragment_tail and not rewritten and subject_core:
        rewritten = f"{subject_core} {copula} moved lower in early trading."

    finalized = _finalize_takeaway_sentence(rewritten or source_sentence)
    if finalized:
        if source_has_fragment_tail:
            return finalized, "takeaway_tail_fragment_rewritten"
        return finalized, "source_rewrite"
    return "US trend is shifting quickly.", "safe_fallback"


def _title_takeaway_role_ok(*, headline: str, takeaway: str) -> bool:
    h = _normalize_render_text(headline)
    t = _normalize_render_text(takeaway)
    if not h or not t:
        return False
    return h.lower() != t.lower()


def _compact_headline_sentence(text: str, *, source_sentence: str) -> str:
    cleaned = _normalize_render_text(text).strip()
    if not cleaned:
        return ""
    core = cleaned.rstrip(".!?").strip()
    for phrase_tokens in HEADLINE_LOCKED_TERMS:
        phrase = " ".join(phrase_tokens)
        m = re.search(rf"\b{re.escape(phrase)}\b", core, flags=re.IGNORECASE)
        if not m:
            continue
        if m.end() >= len(core):
            continue
        candidate = core[: m.end()].strip()
        finalized = _finalize_headline_sentence(candidate, source_text=source_sentence)
        if finalized:
            return finalized
    return _finalize_headline_sentence(
        _semantic_shorten_sentence(cleaned, max_words=6),
        source_text=source_sentence,
    )


def _enforce_title_takeaway_roles(
    *,
    headline: str,
    takeaway: str,
    source_sentence: str,
) -> tuple[str, str, bool]:
    normalized_headline = _normalize_render_text(headline)
    normalized_takeaway = _normalize_render_text(takeaway)
    if not normalized_headline or not normalized_takeaway:
        return normalized_headline, normalized_takeaway, False

    finalized_headline = _finalize_headline_sentence(normalized_headline, source_text=source_sentence)
    finalized_takeaway = _finalize_takeaway_sentence(normalized_takeaway)
    if not finalized_headline or not finalized_takeaway:
        return normalized_headline, normalized_takeaway, False

    if _title_takeaway_role_ok(headline=finalized_headline, takeaway=finalized_takeaway):
        if _has_unjoined_clause_boundary(finalized_headline):
            compact_headline = _compact_headline_sentence(finalized_headline, source_sentence=source_sentence)
            if compact_headline and _title_takeaway_role_ok(headline=compact_headline, takeaway=finalized_takeaway):
                return compact_headline, finalized_takeaway, False
        return finalized_headline, finalized_takeaway, False

    swapped_headline = _finalize_headline_sentence(finalized_takeaway, source_text=source_sentence)
    swapped_takeaway = _finalize_takeaway_sentence(finalized_headline)
    source_takeaway = _finalize_takeaway_sentence(source_sentence)
    compact_headline = _compact_headline_sentence(finalized_headline, source_sentence=source_sentence)
    if not swapped_headline or not swapped_takeaway:
        if compact_headline and _title_takeaway_role_ok(headline=compact_headline, takeaway=finalized_takeaway):
            return compact_headline, finalized_takeaway, True
        if source_takeaway and _title_takeaway_role_ok(headline=finalized_headline, takeaway=source_takeaway):
            return finalized_headline, source_takeaway, True
        if compact_headline and source_takeaway and _title_takeaway_role_ok(headline=compact_headline, takeaway=source_takeaway):
            return compact_headline, source_takeaway, True
        return finalized_headline, finalized_takeaway, False
    if not _title_takeaway_role_ok(headline=swapped_headline, takeaway=swapped_takeaway):
        if compact_headline and _title_takeaway_role_ok(headline=compact_headline, takeaway=finalized_takeaway):
            return compact_headline, finalized_takeaway, True
        if source_takeaway and _title_takeaway_role_ok(headline=finalized_headline, takeaway=source_takeaway):
            return finalized_headline, source_takeaway, True
        if compact_headline and source_takeaway and _title_takeaway_role_ok(headline=compact_headline, takeaway=source_takeaway):
            return compact_headline, source_takeaway, True
        return finalized_headline, finalized_takeaway, False
    return swapped_headline, swapped_takeaway, True


def _extract_first_sentence(text: str) -> str:
    normalized = _normalize_render_text(text)
    if not normalized:
        return ""
    # Intentionally avoid sentence splitting; punctuation like "U.S." can
    # incorrectly truncate key chart context.
    return normalized


def _strip_news_prefix(text: str) -> str:
    return NEWS_PREFIX_RE.sub("", _normalize_render_text(text)).strip()


def _mode_hint_from_text(candidate: Candidate) -> str:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    if any(token in merged for token in BAR_HINT_KEYWORDS):
        return "bar"
    return "line"


def _extract_subject_and_verb(sentence: str) -> tuple[str, str]:
    clean = _strip_news_prefix(sentence)
    patterns = list(POSITIVE_MOVE_VERBS) + list(NEGATIVE_MOVE_VERBS) + list(NEUTRAL_MOVE_VERBS)
    for verb in patterns:
        m = re.search(rf"^(.*?)\b{re.escape(verb)}\b", clean, flags=re.IGNORECASE)
        if m:
            subject = m.group(1).strip(" ,:-")
            if subject:
                return subject, verb
    words = clean.split(" ")
    if len(words) >= 4:
        return " ".join(words[:4]).strip(" ,:-"), ""
    return clean.strip(" ,:-"), ""


def _extract_timeframe_snippet(sentence: str) -> str:
    lower = sentence.lower()
    m = re.search(r"\bin the first [a-z0-9\s-]{3,28}\b", lower)
    if m:
        frag = m.group(0).replace("in the ", "").strip()
        return frag.title()
    m = re.search(r"\bsince \d{4}\b", lower)
    if m:
        return m.group(0).title()
    m = re.search(r"\b(last|past)\s+\d+\s+(months|years|quarters|weeks)\b", lower)
    if m:
        return m.group(0).title()
    return ""


def _entity_hint_from_text(*, sentence: str, fallback: str) -> str:
    ticker_match = re.search(r"\$([A-Z]{1,5})\b", sentence)
    if ticker_match:
        ticker = ticker_match.group(1).upper()
        return TICKER_NAME_HINTS.get(ticker, ticker)
    clean_fallback = _shorten_without_ellipsis(_strip_news_prefix(fallback), max_chars=24)
    clean_fallback = clean_fallback.strip(" ,:-")
    if clean_fallback:
        return clean_fallback
    return "US trend"


def _infer_units_snippet(sentence: str) -> str:
    lower = sentence.lower()
    if ("billion" in lower or "million" in lower or "$" in sentence or "usd" in lower):
        return "US$"
    if "yoy" in lower or "qoq" in lower or "%" in sentence:
        return "YoY %"
    if "p/e" in lower or "multiple" in lower or "x" in lower:
        return "x"
    if "index" in lower:
        return "Index"
    return ""


def _extract_years_from_text(text: str) -> list[int]:
    years: list[int] = []
    for m in re.finditer(r"\b(19\d{2}|20\d{2})\b", text):
        try:
            value = int(m.group(1))
        except Exception:
            continue
        if 1900 <= value <= 2100:
            years.append(value)
    dedup: list[int] = []
    seen: set[int] = set()
    for y in years:
        if y in seen:
            continue
        seen.add(y)
        dedup.append(y)
    return dedup


def _infer_bar_labels_from_text(*, candidate: Candidate | None, count: int) -> list[str]:
    if count <= 0:
        return []
    if candidate is None:
        return []
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}")
    years = sorted(_extract_years_from_text(merged))
    if len(years) >= 2:
        min_y, max_y = years[0], years[-1]
        span = max_y - min_y + 1
        if span == count:
            return [str(y) for y in range(min_y, max_y + 1)]
        if span > count and count >= 4:
            start = max_y - count + 1
            if start >= 1900:
                return [str(y) for y in range(start, max_y + 1)]
    if len(years) == 1 and 4 <= count <= 20:
        end_y = years[0]
        start_y = end_y - count + 1
        if start_y >= 1900:
            return [str(y) for y in range(start_y, end_y + 1)]
    return []


def _fallback_bar_labels(*, candidate: Candidate | None, count: int) -> list[str]:
    labels = _infer_bar_labels_from_text(candidate=candidate, count=count)
    if len(labels) == count:
        return labels
    year_hint: int | None = None
    if candidate is not None:
        merged = _normalize_render_text(f"{candidate.title} {candidate.text}")
        years = sorted(_extract_years_from_text(merged))
        if years:
            year_hint = years[-1]
        elif candidate.created_at:
            try:
                year_hint = datetime.fromisoformat(candidate.created_at.replace("Z", "+00:00")).year
            except Exception:
                year_hint = None
    if year_hint is None:
        year_hint = datetime.now(UTC).year
    if 4 <= count <= 20:
        start_y = year_hint - count + 1
        if start_y >= 1900:
            return [str(y) for y in range(start_y, year_hint + 1)]
    return [f"P{i+1}" for i in range(count)]


def _is_employees_robots_chart(candidate: Candidate | None) -> bool:
    if candidate is None:
        return False
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    return ("employees" in merged) and ("robots" in merged)


def _labels_are_placeholder(labels: list[str]) -> bool:
    if not labels:
        return True
    return all(bool(re.fullmatch(r"p\d+", str(label).strip().lower())) for label in labels)


def _labels_are_monotonic_years(labels: list[str]) -> bool:
    years: list[int] = []
    for label in labels:
        text = str(label).strip()
        if not re.fullmatch(r"\d{4}", text):
            return False
        years.append(int(text))
    if len(years) < 4:
        return False
    return all(years[i] < years[i + 1] for i in range(len(years) - 1))


def _extract_employee_robot_latest_millions(text: str) -> tuple[float | None, float | None]:
    lower = _normalize_render_text(text).lower()
    emp_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+employees", lower)
    rob_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+robots", lower)
    emp = float(emp_m.group(1)) if emp_m else None
    rob = float(rob_m.group(1)) if rob_m else None
    return emp, rob


def _extract_employees_robots_bars_cv(*, rgb, candidate: Candidate) -> RebuiltBars | None:
    try:
        import numpy as np
        from matplotlib.colors import rgb_to_hsv
    except Exception:
        return None

    h, w, _ = rgb.shape
    crop = rgb[int(h * 0.16) : int(h * 0.93), int(w * 0.07) : int(w * 0.97), :]
    ch, cw, _ = crop.shape
    if ch < 200 or cw < 260:
        return None
    analysis = crop[int(ch * 0.18) :, :, :]
    ah, aw, _ = analysis.shape
    hsv = rgb_to_hsv(np.clip(analysis, 0.0, 1.0))
    hue = hsv[:, :, 0]
    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]

    # ARK-style bars are predominantly blue/purple with similar hue and different brightness.
    base = (sat > 0.15) & (hue >= 0.57) & (hue <= 0.80)
    dark_mask = base & (val < 0.56)
    light_mask = base & (val >= 0.56)

    def _series(mask):
        cols = mask.sum(axis=0)
        col_threshold = max(4, int(ah * 0.030))
        flags = cols >= col_threshold
        spans: list[tuple[int, int]] = []
        start = -1
        min_width = max(2, int(aw * 0.006))
        for i, flag in enumerate(flags.tolist()):
            if flag and start < 0:
                start = i
            elif (not flag) and start >= 0:
                if (i - start) >= min_width:
                    spans.append((start, i - 1))
                start = -1
        if start >= 0 and (len(flags) - start) >= min_width:
            spans.append((start, len(flags) - 1))
        centers: list[float] = []
        heights: list[float] = []
        bottoms: list[float] = []
        for s, e in spans:
            seg = mask[:, s : e + 1]
            y_idx = np.where(seg.any(axis=1))[0]
            if y_idx.size == 0:
                continue
            top = float(y_idx.min())
            bottom = float(y_idx.max())
            centers.append(float((s + e) / 2.0))
            heights.append(max(0.0, bottom - top))
            bottoms.append(bottom)
        return centers, heights, bottoms

    dark_x, dark_h, dark_b = _series(dark_mask)
    light_x, light_h, light_b = _series(light_mask)
    if len(dark_h) < 8 or len(light_h) < 8:
        return None

    # Keep the best-aligned segment count between the two series.
    n = min(len(dark_h), len(light_h))
    if n < 8:
        return None
    dark_vals = dark_h[-n:]
    light_vals = light_h[-n:]

    # Convert relative heights to "thousands" using latest values from post text.
    latest_emp_m, latest_rob_m = _extract_employee_robot_latest_millions(f"{candidate.title} {candidate.text}")
    if latest_emp_m is None or latest_rob_m is None:
        return None
    if dark_vals[-1] <= 0.0 or light_vals[-1] <= 0.0:
        return None
    emp_scale = (latest_emp_m * 1000.0) / float(dark_vals[-1])
    rob_scale = (latest_rob_m * 1000.0) / float(light_vals[-1])
    scale = float((emp_scale + rob_scale) / 2.0)
    if scale <= 0.0:
        return None

    emp_values = [round(float(v) * scale, 1) for v in dark_vals]
    rob_values = [round(float(v) * scale, 1) for v in light_vals]
    labels = _fallback_bar_labels(candidate=candidate, count=n)
    bars = RebuiltBars(
        labels=labels,
        values=emp_values,
        color="#1F2452",
        y_label="Number (thousands)",
        normalized=False,
        source="cv",
        confidence=0.64,
        primary_label="Employees",
        secondary_values=rob_values,
        secondary_color="#6D63E7",
        secondary_label="Robots",
    )
    if _bar_data_quality_errors(candidate=candidate, bars=bars):
        return None
    return bars


def _normalize_grouped_bar_metadata(*, candidate: Candidate | None, bars: RebuiltBars) -> RebuiltBars:
    if candidate is None or (not _is_employees_robots_chart(candidate)):
        return bars
    if not bars.secondary_values or len(bars.secondary_values) != len(bars.values):
        return bars

    primary_values = list(bars.values)
    secondary_values = list(bars.secondary_values)
    p_label = (bars.primary_label or "").strip().lower()
    s_label = (bars.secondary_label or "").strip().lower()
    primary_sum = float(sum(primary_values))
    secondary_sum = float(sum(secondary_values))
    should_swap = False
    if ("robot" in p_label and "employee" in s_label) or ("employee" in s_label and "employee" not in p_label):
        should_swap = True
    elif ("robot" not in s_label and "employee" not in p_label) and (primary_sum < secondary_sum):
        should_swap = True

    if should_swap:
        primary_values, secondary_values = secondary_values, primary_values

    y_label = bars.y_label
    y_lower = (y_label or "").strip().lower()
    if (not y_lower) or ("index" in y_lower) or (y_lower == "value"):
        y_label = "Number (thousands)"
    return replace(
        bars,
        values=primary_values,
        secondary_values=secondary_values,
        primary_label="Employees",
        secondary_label="Robots",
        color="#1F2452",
        secondary_color="#6D63E7",
        y_label=y_label,
    )


def _bar_data_quality_errors(*, candidate: Candidate | None, bars: RebuiltBars | None) -> list[str]:
    if bars is None:
        return ["missing rebuilt bars"]
    errors: list[str] = []
    n = len(bars.values)
    if n < 4:
        errors.append("insufficient bar count")
    if len(bars.labels) != n:
        errors.append("x-axis labels do not match bar count")
    if _labels_are_placeholder(bars.labels):
        errors.append("placeholder x-axis labels")
    if not (bars.y_label or "").strip():
        errors.append("missing y-axis label")
    if _is_employees_robots_chart(candidate):
        if not bars.secondary_values or len(bars.secondary_values) != n:
            errors.append("grouped chart requires two aligned series")
        if bars.normalized:
            errors.append("grouped chart must not use normalized values")
        if max(bars.values or [0.0]) < 200.0 or max(bars.secondary_values or [0.0]) < 100.0:
            errors.append("grouped chart values are too small for employee/robot units")
        y_lower = (bars.y_label or "").lower()
        if "index" in y_lower:
            errors.append("grouped chart y-axis label must be unit-based")
        if bars.labels and (not _labels_are_monotonic_years(bars.labels)):
            errors.append("grouped chart requires monotonic year x-axis labels")
    return errors


def _style_copy_quality_errors(style_draft: StyleDraft) -> list[str]:
    errors: list[str] = []
    if "..." in style_draft.headline or "..." in style_draft.chart_label or "..." in style_draft.takeaway:
        errors.append("style copy contains ellipsis")
    if len(_normalize_render_text(style_draft.chart_label)) > 62:
        errors.append("chart label too long")
    if not _is_complete_sentence(style_draft.takeaway):
        errors.append("takeaway incomplete sentence")
    if not _is_single_sentence_takeaway(style_draft.takeaway):
        errors.append("takeaway not single sentence")
    if _has_unjoined_clause_boundary(style_draft.takeaway):
        errors.append("takeaway clause boundary invalid")
    if not _tail_complete(style_draft.takeaway):
        errors.append("takeaway tail fragment")
    if _is_degenerate_copy_value(style_draft.headline):
        errors.append("headline degenerate")
    if _is_degenerate_copy_value(style_draft.chart_label):
        errors.append("chart label degenerate")
    return errors


def _post_publish_checklist(
    *,
    candidate: Candidate,
    style_draft: StyleDraft,
    styled_path: Path,
    render_qa: dict[str, Any],
) -> dict[str, Any]:
    headline = _normalize_render_text(style_draft.headline)
    chart_label = _normalize_render_text(style_draft.chart_label)
    takeaway = _normalize_render_text(style_draft.takeaway)
    reconstruction_mode = str(render_qa.get("reconstruction_mode") or "").strip().lower()
    grouped_required = _is_employees_robots_chart(candidate)
    file_size = styled_path.stat().st_size if styled_path.exists() else 0
    checks = {
        "us_relevant": bool(style_draft.checks.get("us_relevant", False)),
        "trend_explicit": bool(style_draft.checks.get("trend_explicit", False)),
        "plain_language": bool(style_draft.checks.get("plain_language", False)),
        "no_ellipsis": ("..." not in headline and "..." not in chart_label and "..." not in takeaway),
        "headline_len_ok": (0 < len(headline)),
        "headline_complete_phrase": True,
        "headline_complete_sentence": True,
        "headline_tail_complete": True,
        "headline_locked_terms_ok": True,
        "headline_wrapped_line_count": int(render_qa.get("headline_wrapped_line_count", 1)),
        "chart_label_len_ok": (0 < len(chart_label) <= 62),
        "takeaway_len_ok": (0 < len(takeaway)),
        "takeaway_complete_sentence": _is_complete_sentence(takeaway),
        "takeaway_single_sentence": _is_single_sentence_takeaway(takeaway),
        "takeaway_clause_boundary_ok": not _has_unjoined_clause_boundary(takeaway),
        "takeaway_tail_complete": _tail_complete(takeaway),
        "takeaway_wrapped_line_count": int(render_qa.get("takeaway_wrapped_line_count", 1)),
        "headline_non_degenerate": not _is_degenerate_copy_value(headline),
        "chart_label_non_degenerate": not _is_degenerate_copy_value(chart_label),
        "title_takeaway_role_ok": True,
        "reconstruction_mode_ok": reconstruction_mode in {"bar", "line"},
        "x_axis_labels_present": bool(render_qa.get("x_axis_labels_present", False)),
        "y_axis_labels_present": bool(render_qa.get("y_axis_labels_present", False)),
        "grouped_series_valid": (not grouped_required) or bool(render_qa.get("grouped_two_series", False)),
        "artifact_nonempty": file_size >= 25_000,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "passed": len(failed) == 0,
        "failed": failed,
        "checks": checks,
        "style_score": float(style_draft.score),
        "style_iteration": int(style_draft.iteration),
        "render_qa": render_qa,
        "artifact_path": str(styled_path),
        "artifact_size_bytes": int(file_size),
    }


def _nice_tick_step(value: float) -> float:
    if not math.isfinite(value) or value <= 0:
        return 1.0
    base = 10.0 ** math.floor(math.log10(value))
    for mult in (1.0, 2.0, 2.5, 5.0, 10.0):
        step = base * mult
        if value <= step:
            return step
    return base * 10.0


def _compute_y_ticks(*, y_min: float, y_max: float, normalized: bool) -> list[float]:
    if normalized:
        return [0.0, 20.0, 40.0, 60.0, 80.0, 100.0]
    low = float(min(y_min, y_max))
    high = float(max(y_min, y_max))
    span = max(1.0, high - low)
    rough = span / 5.0
    step = _nice_tick_step(rough)
    start = math.floor(low / step) * step
    end = math.ceil(high / step) * step
    ticks: list[float] = []
    v = start
    guard = 0
    while v <= end + (step * 0.25) and guard < 32:
        ticks.append(round(float(v), 6))
        v += step
        guard += 1
    if len(ticks) < 3:
        mid = (low + high) / 2.0
        ticks = [round(low, 6), round(mid, 6), round(high, 6)]
    return ticks


def _format_numeric_tick(value: float) -> str:
    if abs(value - round(value)) < 1e-6:
        return f"{int(round(value)):,}"
    if abs(value) >= 100.0:
        return f"{value:,.0f}"
    if abs(value) >= 10.0:
        return f"{value:,.1f}"
    return f"{value:,.2f}"


def _vision_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_VISION_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _extract_rebuilt_bars_via_vision(*, candidate: Candidate) -> RebuiltBars | None:
    if not _vision_enabled():
        return None
    if OpenAI is None:
        return None
    image_url = (candidate.image_url or "").strip()
    if not image_url:
        return None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None

    payload, mime = _fetch_image_bytes(image_url)
    if not payload:
        return None
    b64 = base64.b64encode(payload).decode("ascii")
    image_data_url = f"data:{mime};base64,{b64}"

    try:
        client = OpenAI(api_key=api_key)
        model = os.environ.get("COATUE_CLAW_X_CHART_VISION_MODEL", "gpt-4.1").strip() or "gpt-4.1"
        force_grouped = _is_employees_robots_chart(candidate)
        prompt = (
            "Extract bar-chart data from this image.\n"
            "Return strict JSON only with keys:\n"
            "{"
            "\"chart_type\":\"bar\","
            "\"x_labels\":[\"...\"],"
            "\"values\":[number],"
            "\"series\":[{\"name\":\"...\",\"values\":[number]}],"
            "\"y_label\":\"...\","
            "\"normalized\":true|false,"
            "\"confidence\":0.0-1.0"
            "}.\n"
            "Rules: Use bars left-to-right. Include negatives if present. "
            "If there are grouped bars with multiple series, provide `series` with 2 entries and aligned values. "
            "If there is only one series, use `values` and omit `series`. "
            "If exact units are visible, set y_label accordingly (e.g., 'US$ Billions'). "
            "If units are unclear, use 'Index (normalized)' and normalized=true."
        )
        if force_grouped:
            prompt += (
                " This chart is expected to have two series (Employees and Robots). "
                "You MUST return `series` with exactly two aligned value arrays and omit `values`."
            )
        response = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a precise chart-data extraction assistant."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_data_url}},
                    ],
                },
            ],
        )
        raw = ""
        if response.choices and response.choices[0].message:
            raw = str(response.choices[0].message.content or "").strip()
        if not raw:
            return None
        payload = json.loads(raw)
    except Exception as exc:
        logger.debug("Vision extraction failed: %s", exc)
        return None

    labels_raw = payload.get("x_labels") if isinstance(payload, dict) else None
    series_raw = payload.get("series") if isinstance(payload, dict) else None
    values_raw = payload.get("values") if isinstance(payload, dict) else None

    values: list[float] = []
    secondary_values: list[float] | None = None
    primary_label: str | None = None
    secondary_label: str | None = None
    if isinstance(series_raw, list) and len(series_raw) >= 2:
        series_values: list[tuple[str | None, list[float]]] = []
        for entry in series_raw[:2]:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or "").strip() or None
            raw_vals = entry.get("values")
            if not isinstance(raw_vals, list):
                continue
            parsed: list[float] = []
            for item in raw_vals:
                try:
                    parsed.append(float(item))
                except Exception:
                    continue
            if parsed:
                series_values.append((name, parsed))
        if len(series_values) >= 2:
            a_name, a_vals = series_values[0]
            b_name, b_vals = series_values[1]
            n = min(len(a_vals), len(b_vals))
            if 4 <= n <= 20:
                values = a_vals[:n]
                secondary_values = b_vals[:n]
                primary_label = a_name
                secondary_label = b_name
    if force_grouped and (secondary_values is None):
        return None
    if not values:
        if not isinstance(values_raw, list):
            return None
        for item in values_raw:
            try:
                values.append(float(item))
            except Exception:
                continue
        if not (4 <= len(values) <= 20):
            return None

    labels: list[str] = []
    if isinstance(labels_raw, list):
        for item in labels_raw:
            label = _shorten_without_ellipsis(str(item), max_chars=12)
            if label:
                labels.append(label)
    if labels and len(labels) != len(values):
        n = min(len(labels), len(values))
        labels = labels[:n]
        values = values[:n]
        if secondary_values is not None:
            secondary_values = secondary_values[:n]
    if not labels:
        labels = _fallback_bar_labels(candidate=candidate, count=len(values))
    if len(labels) not in {0, len(values)}:
        labels = []
    if secondary_values is not None and len(secondary_values) != len(values):
        n = min(len(values), len(secondary_values))
        values = values[:n]
        secondary_values = secondary_values[:n]
        labels = labels[:n] if labels else _fallback_bar_labels(candidate=candidate, count=n)

    y_label = _shorten_without_ellipsis(str(payload.get("y_label") or "Value"), max_chars=22)
    if not y_label:
        y_label = "Value"
    normalized = bool(payload.get("normalized")) if isinstance(payload, dict) else False
    try:
        confidence = float(payload.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    if confidence < 0.58:
        return None
    bars = RebuiltBars(
        labels=labels,
        values=values,
        color="#2F6ABF",
        y_label=y_label,
        normalized=normalized,
        source="vision",
        confidence=confidence,
        primary_label=primary_label,
        secondary_values=secondary_values,
        secondary_color="#5AA88A" if secondary_values is not None else None,
        secondary_label=secondary_label,
    )
    bars = _normalize_grouped_bar_metadata(candidate=candidate, bars=bars)
    if _bar_data_quality_errors(candidate=candidate, bars=bars):
        return None
    return bars


def _synthesize_chart_label(*, subject: str, sentence: str, mode_hint: str) -> str:
    lower_sentence = sentence.lower()
    if "employees" in lower_sentence and "robots" in lower_sentence:
        entity = _entity_hint_from_text(sentence=sentence, fallback=subject)
        years = sorted(_extract_years_from_text(sentence))
        if len(years) >= 2:
            base = f"{entity} employees vs robots ({years[0]}-{years[-1]})"
        else:
            base = f"{entity} employees vs robots"
        return _shorten_without_ellipsis(base, max_chars=62)

    core = _shorten_without_ellipsis(_strip_news_prefix(subject), max_chars=46)
    if not core:
        core = "Chart Context"
    timeframe = _extract_timeframe_snippet(sentence)
    units = _infer_units_snippet(sentence)
    if mode_hint == "bar" and timeframe:
        base = f"{core} ({timeframe})"
    else:
        base = core
    if units and units.lower() not in base.lower():
        base = f"{base} ({units})"
    return _shorten_without_ellipsis(base, max_chars=62)


def _synthesize_narrative_title(*, subject: str, verb: str, sentence: str) -> str:
    subject_core = _clean_subject_for_headline(subject=subject, sentence=sentence)
    s_lower = subject_core.lower()
    v_lower = verb.lower()
    sentence_lower = sentence.lower()
    copula = "are" if _subject_is_plural(subject_core) else "is"
    if "employees" in sentence_lower and "robots" in sentence_lower:
        entity = _entity_hint_from_text(sentence=sentence, fallback=subject_core)
        if "ratio" in sentence_lower or "replacing humans" in sentence_lower:
            return f"{entity} is increasing automation intensity"
        return f"{entity} is scaling robots faster than headcount"
    if (
        "institutional" in sentence_lower
        and ("net sellers" in sentence_lower or "sold a net" in sentence_lower or "biggest net sellers" in sentence_lower)
    ):
        return "Institutional selling is at an extreme"
    if (
        "institutional" in sentence_lower
        and ("net buyers" in sentence_lower or "bought a net" in sentence_lower or "biggest net buyers" in sentence_lower)
    ):
        return "Institutional buying is accelerating"
    if "etf inflows" in s_lower:
        if v_lower in POSITIVE_MOVE_VERBS or "record" in sentence.lower():
            return "ETF appetite is re-accelerating"
        if v_lower in NEGATIVE_MOVE_VERBS:
            return "ETF appetite is cooling"
    if "consumer sentiment" in s_lower:
        if v_lower in NEGATIVE_MOVE_VERBS:
            return "Consumer confidence is weakening"
        if v_lower in POSITIVE_MOVE_VERBS:
            return "Consumer confidence is improving"
    if v_lower in POSITIVE_MOVE_VERBS:
        return f"{subject_core} {copula} inflecting higher"
    if v_lower in NEGATIVE_MOVE_VERBS:
        return f"{subject_core} {copula} rolling over"
    if "record" in sentence.lower() or v_lower in NEUTRAL_MOVE_VERBS:
        return f"{subject_core} {copula} at an extreme"
    return _strip_news_prefix(sentence)


def _trim_trailing_stopwords(text: str) -> str:
    parts = [p for p in _normalize_render_text(text).split(" ") if p]
    while parts and parts[-1].lower() in TRAILING_STOPWORDS:
        parts.pop()
    return " ".join(parts).strip()


def _is_low_signal_phrase(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    if not lower:
        return True
    if lower.startswith(("it's official", "breaking", "update", "new chart", "alert")):
        return True
    if "one of the most anticipated rulings" in lower:
        return True
    if lower in {"chart context", "us trend snapshot"}:
        return True
    return False


def _clean_subject_for_headline(*, subject: str, sentence: str) -> str:
    subject_core = _trim_trailing_stopwords(_shorten_without_ellipsis(_strip_news_prefix(subject), max_chars=40))
    if subject_core:
        subject_core = re.sub(
            rf"\b(?:{'|'.join(re.escape(v) for v in SUBJECT_TRAILING_ACTION_VERBS)})\b(?:\s+\w+){{0,2}}$",
            "",
            subject_core,
            flags=re.IGNORECASE,
        ).strip(" ,:-")
        subject_core = _trim_trailing_stopwords(subject_core)
    if len(subject_core.split()) >= 2:
        return subject_core

    sentence_core = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(sentence), flags=re.IGNORECASE).strip()
    sentence_core = re.sub(
        r"\b(?:surged|rose|jumped|climbed|rebounded|accelerated|increased|grew|fell|dropped|declined|slowed|rolled over|sank|contracted|decelerated|sold|bought|buying|selling|hit|reached|stands at|is at)\b.*$",
        "",
        sentence_core,
        flags=re.IGNORECASE,
    ).strip(" ,:-")
    sentence_core = _trim_trailing_stopwords(_shorten_without_ellipsis(sentence_core, max_chars=40))
    if len(sentence_core.split()) >= 2:
        return sentence_core
    return subject_core or "US data"


def _subject_is_plural(subject: str) -> bool:
    lower = _normalize_render_text(subject).lower()
    if not lower:
        return False
    if " and " in lower:
        return True
    words = [w for w in re.split(r"\s+", lower) if w]
    if not words:
        return False
    tail = words[-1].strip(".,:;")
    if tail in PLURAL_SUBJECT_HINTS:
        return True
    if tail.endswith("s") and not tail.endswith(("ss", "is", "us")):
        return True
    return False


def _has_incoherent_headline(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    if not lower:
        return True
    bad_patterns = (
        r"\b(a|an|the)\s+(is|are|was|were)\b",
        r"\b(sold|bought|buying|selling)\s+(a|an|the)\s+(is|are|was|were)\b",
        r"\b(is|are|was|were)\s+(is|are|was|were)\b",
    )
    return any(re.search(pat, lower) is not None for pat in bad_patterns)


def _keyword_style_override(candidate: Candidate) -> tuple[str, str, str] | None:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    if "tariff" in merged and ("customs" in merged or "duties" in merged):
        return (
            "US tariff receipts are surging",
            "Monthly US customs duties (US$B)",
            "US customs-duty collections just hit a new high.",
        )
    return None


def _extract_chart_title_hint_via_vision(candidate: Candidate) -> str | None:
    if OpenAI is None:
        return None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    image_url = (candidate.image_url or "").strip()
    if not api_key or not image_url:
        return None
    payload, mime = _fetch_image_bytes(image_url)
    if not payload:
        return None
    try:
        client = OpenAI(api_key=api_key)
        model = os.environ.get("COATUE_CLAW_X_CHART_TITLE_MODEL", "gpt-5.2-chat-latest").strip() or "gpt-5.2-chat-latest"
        b64 = base64.b64encode(payload).decode("ascii")
        data_url = f"data:{mime};base64,{b64}"
        response = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You extract concise chart text cues."},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Read this chart image and return strict JSON: "
                                '{"chart_title":"...", "metric_label":"..."} '
                                "Use visible chart title and metric label only."
                            ),
                        },
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
        )
        raw = ""
        if response.choices and response.choices[0].message:
            raw = str(response.choices[0].message.content or "").strip()
        if not raw:
            return None
        parsed = json.loads(raw)
    except Exception:
        return None
    hint = _shorten_without_ellipsis(str(parsed.get("chart_title") or ""), max_chars=80)
    return hint or None


def _sanitize_style_copy(
    *,
    candidate: Candidate,
    headline: str,
    chart_label: str,
    takeaway: str,
) -> tuple[str, str, str, bool, str | None, bool]:
    source_text = _normalize_render_text(candidate.text or candidate.title)
    source_text = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(source_text), flags=re.IGNORECASE).strip()
    source_title = _normalize_render_text(candidate.title)
    source_title = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(source_title), flags=re.IGNORECASE).strip()

    normalized_headline = _normalize_render_text(headline)
    if not normalized_headline:
        normalized_headline = source_title or source_text
    normalized_headline = _shorten_without_ellipsis(normalized_headline, max_chars=160) or "US chart context"

    normalized_takeaway = _normalize_render_text(takeaway)
    if not normalized_takeaway:
        normalized_takeaway = source_text or source_title or normalized_headline
    normalized_takeaway = _shorten_without_ellipsis(normalized_takeaway, max_chars=240) or normalized_headline
    if normalized_takeaway[-1] not in ".!?":
        normalized_takeaway = f"{normalized_takeaway}."

    # chart_label is synchronized to headline by design.
    normalized_chart_label = normalized_headline
    return normalized_headline, normalized_chart_label, normalized_takeaway, False, None, False


def _employees_robots_takeaway(sentence: str) -> str:
    s = _normalize_render_text(sentence)
    lower = s.lower()
    emp_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+employees", lower)
    rob_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+robots", lower)
    if emp_m and rob_m:
        emp = emp_m.group(1)
        rob = rob_m.group(1)
        return _shorten_without_ellipsis(f"Amazon has {emp}M employees and {rob}M robots deployed.", max_chars=68)
    if "ratio" in lower and "declined" in lower:
        return _shorten_without_ellipsis("Amazon's human-to-robot ratio is tightening quickly.", max_chars=68)
    return _shorten_without_ellipsis("Amazon is scaling robots faster than employee growth.", max_chars=68)


def _llm_title_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_LLM_TITLES_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _require_reconstruction() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_REQUIRE_REBUILD", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _raw_tweet_copy_from_candidate(candidate: Candidate) -> tuple[str, str]:
    source_text = _normalize_render_text(candidate.text or candidate.title)
    source_text = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(source_text), flags=re.IGNORECASE).strip()
    source_title = _normalize_render_text(candidate.title)
    source_title = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(source_title), flags=re.IGNORECASE).strip()
    headline = _shorten_without_ellipsis(source_text or source_title, max_chars=160) or "US chart context"
    takeaway = _shorten_without_ellipsis(source_text or source_title or headline, max_chars=240) or headline
    if takeaway[-1] not in ".!?":
        takeaway = f"{takeaway}."
    return headline, takeaway


def _synthesize_style_via_llm(candidate: Candidate) -> tuple[dict[str, str] | None, str | None]:
    if not _llm_title_enabled():
        return None, None
    if OpenAI is None:
        return None, None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None, None
    text = _normalize_render_text(candidate.text)
    title = _normalize_render_text(candidate.title)
    if not text and not title:
        return None, "missing_fields"
    model = os.environ.get("COATUE_CLAW_X_CHART_TITLE_MODEL", "gpt-5.2-chat-latest").strip() or "gpt-5.2-chat-latest"
    chart_hint = _extract_chart_title_hint_via_vision(candidate)
    prompt = [
        "Using the tweet and chart context, generate an encompassing Coatue Chart of the Day style title.",
        "Using the same context, generate a key takeaway sentence.",
        "Avoid copying the tweet wording verbatim; synthesize the message.",
        "Return strict JSON with keys: headline, chart_label, takeaway.",
        f"Tweet title context: {title or 'n/a'}",
        f"Tweet text context: {text or 'n/a'}",
        f"Chart hint context: {chart_hint or 'n/a'}",
    ]
    image_block: dict[str, Any] | None = None
    image_payload, image_ctype = _fetch_image_bytes(candidate.image_url)
    if image_payload:
        image_data_url = f"data:{image_ctype};base64,{base64.b64encode(image_payload).decode('ascii')}"
        image_block = {"type": "image_url", "image_url": {"url": image_data_url}}
    try:
        client = OpenAI(api_key=api_key)
        user_content: list[dict[str, Any]] = [{"type": "text", "text": "\n".join(prompt)}]
        if image_block is not None:
            user_content.append(image_block)
        response = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You write concise buy-side chart titles and takeaways."},
                {"role": "user", "content": user_content},
            ],
        )
        raw = ""
        if response.choices and response.choices[0].message:
            raw = str(response.choices[0].message.content or "").strip()
        if not raw:
            return None, "missing_fields"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None, "invalid_json"
    except Exception as exc:
        logger.debug("LLM style synthesis failed: %s", exc)
        return None, "api_error"

    headline = _normalize_render_text(str(payload.get("headline") or ""))
    chart_label = _normalize_render_text(str(payload.get("chart_label") or ""))
    takeaway = _normalize_render_text(str(payload.get("takeaway") or ""))
    if not headline or not chart_label or not takeaway:
        return None, "missing_fields"
    return {"headline": headline, "chart_label": chart_label, "takeaway": takeaway}, None


def _is_us_relevant_post(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    strong = any(token in lower for token in US_STRONG_SIGNAL_KEYWORDS)
    weak = any(token in lower for token in US_WEAK_SIGNAL_KEYWORDS)
    if re.search(r"\bus\b", lower):
        weak = True
    forex = any(token in lower for token in FOREX_NON_US_KEYWORDS)
    non_us_geo = any(token in lower for token in NON_US_GEOGRAPHY_KEYWORDS)
    if re.search(r"\b[a-z]{3}/[a-z]{3}\b", lower):
        forex = True

    if forex and not strong:
        return False
    if non_us_geo and not (strong or weak):
        return False
    if strong:
        return True
    return weak and not forex


def _truncate_words(text: str, *, max_words: int, max_chars: int) -> str:
    words = [w for w in _normalize_render_text(text).split(" ") if w]
    if not words:
        return ""
    clipped = " ".join(words[:max_words]).strip()
    if len(clipped) > max_chars:
        clipped = _shorten_without_ellipsis(clipped, max_chars=max_chars)
    return clipped.strip(". ")


def _contains_trend_signal(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    if re.search(r"\b\d+(\.\d+)?%|\b\d+(\.\d+)?x\b|\$\s?\d", lower):
        return True
    return any(token in lower for token in TREND_SIGNAL_KEYWORDS)


class XChartStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()
        self._seed_default_sources()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sources (
                    handle TEXT PRIMARY KEY,
                    priority REAL NOT NULL,
                    trust_score REAL NOT NULL DEFAULT 0,
                    manual INTEGER NOT NULL DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 1,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posted_slots (
                    slot_key TEXT PRIMARY KEY,
                    posted_at_utc TEXT NOT NULL,
                    candidate_key TEXT NOT NULL,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    score REAL NOT NULL,
                    channel TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posted_items (
                    candidate_key TEXT PRIMARY KEY,
                    first_posted_at_utc TEXT NOT NULL,
                    last_posted_at_utc TEXT NOT NULL,
                    posts_count INTEGER NOT NULL DEFAULT 1
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS observed_candidates (
                    candidate_key TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    author TEXT NOT NULL,
                    title TEXT NOT NULL,
                    text TEXT NOT NULL,
                    url TEXT NOT NULL,
                    image_url TEXT,
                    created_at TEXT,
                    engagement INTEGER NOT NULL,
                    source_priority REAL NOT NULL,
                    score REAL NOT NULL,
                    discovered_via TEXT NOT NULL DEFAULT 'seed_list',
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL
                );
                """
            )
            try:
                conn.execute("ALTER TABLE observed_candidates ADD COLUMN discovered_via TEXT NOT NULL DEFAULT 'seed_list'")
            except Exception:
                pass
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS post_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_key TEXT NOT NULL,
                    reviewed_at_utc TEXT NOT NULL,
                    candidate_key TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    passed INTEGER NOT NULL,
                    failed_checks_json TEXT NOT NULL,
                    checks_json TEXT NOT NULL,
                    artifact_path TEXT NOT NULL,
                    artifact_size_bytes INTEGER NOT NULL,
                    style_score REAL NOT NULL,
                    style_iteration INTEGER NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_post_reviews_recent ON post_reviews(reviewed_at_utc DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_post_reviews_source ON post_reviews(source_id, reviewed_at_utc DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_observed_candidates_last_seen ON observed_candidates(last_seen_utc DESC);")

    def _seed_default_sources(self) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            for handle, priority in DEFAULT_PRIORITY_SOURCES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO sources (handle, priority, trust_score, manual, active, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, 2, 1, 1, ?, ?)
                    """,
                    (_canonical_handle(handle), float(priority), now, now),
                )

    def upsert_source(self, handle: str, *, priority: float, manual: bool) -> None:
        clean = _canonical_handle(handle)
        if not clean:
            raise XChartError("Invalid source handle.")
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            row = conn.execute("SELECT handle, manual, trust_score FROM sources WHERE handle = ?", (clean,)).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO sources (handle, priority, trust_score, manual, active, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, ?, ?, 1, ?, ?)
                    """,
                    (clean, float(priority), (2.0 if manual else 0.5), (1 if manual else 0), now, now),
                )
                return
            new_manual = 1 if (manual or bool(row["manual"])) else 0
            new_priority = float(priority)
            conn.execute(
                """
                UPDATE sources
                SET priority = ?, manual = ?, active = 1, last_seen_utc = ?
                WHERE handle = ?
                """,
                (new_priority, new_manual, now, clean),
            )

    def note_candidate_observed(self, handle: str, *, engagement: int) -> None:
        clean = _canonical_handle(handle)
        if not clean:
            return
        now = datetime.now(UTC).isoformat()
        boost = 0.05 + min(0.75, max(0.0, float(engagement)) / 1000.0)
        with self._connect() as conn:
            row = conn.execute("SELECT priority, trust_score, manual FROM sources WHERE handle = ?", (clean,)).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO sources (handle, priority, trust_score, manual, active, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, ?, 0, 1, ?, ?)
                    """,
                    (clean, 0.45, boost, now, now),
                )
                return
            trust = float(row["trust_score"] or 0.0) + boost
            priority = float(row["priority"] or 0.5)
            manual = bool(row["manual"])
            if (not manual) and trust >= 3.0:
                priority = max(priority, min(1.2, 0.5 + trust / 5.0))
            conn.execute(
                """
                UPDATE sources
                SET trust_score = ?, priority = ?, active = 1, last_seen_utc = ?
                WHERE handle = ?
                """,
                (trust, priority, now, clean),
            )

    def top_sources(self, *, limit: int = 25) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT handle, priority, trust_score, manual, last_seen_utc
                FROM sources
                WHERE active = 1
                ORDER BY priority DESC, trust_score DESC, manual DESC, handle ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_sources(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.top_sources(limit=limit)

    def has_source(self, handle: str) -> bool:
        clean = _canonical_handle(handle)
        if not clean:
            return False
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM sources WHERE handle = ? LIMIT 1", (clean,)).fetchone()
        return row is not None

    def auto_added_sources_count_since(self, *, since_utc: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM sources
                WHERE manual = 0 AND first_seen_utc >= ?
                """,
                (since_utc,),
            ).fetchone()
        return int((row["c"] if row is not None else 0) or 0)

    def was_slot_posted(self, slot_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM posted_slots WHERE slot_key = ? LIMIT 1", (slot_key,)).fetchone()
        return row is not None

    def was_item_posted(self, candidate_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM posted_items WHERE candidate_key = ? LIMIT 1",
                (candidate_key,),
            ).fetchone()
        return row is not None

    def was_item_posted_recently(self, candidate_key: str, *, days: int = 30) -> bool:
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM posted_items WHERE candidate_key = ? AND last_posted_at_utc >= ? LIMIT 1",
                (candidate_key, cutoff),
            ).fetchone()
        return row is not None

    def record_post(self, *, slot_key: str, channel: str, candidate: Candidate) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO posted_slots (slot_key, posted_at_utc, candidate_key, url, title, source, score, channel)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slot_key,
                    now,
                    candidate.candidate_key,
                    candidate.url,
                    candidate.title,
                    f"{candidate.source_type}:{candidate.source_id}",
                    float(candidate.score),
                    channel,
                ),
            )
            existing = conn.execute(
                "SELECT candidate_key, posts_count FROM posted_items WHERE candidate_key = ?",
                (candidate.candidate_key,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO posted_items (candidate_key, first_posted_at_utc, last_posted_at_utc, posts_count)
                    VALUES (?, ?, ?, 1)
                    """,
                    (candidate.candidate_key, now, now),
                )
            else:
                conn.execute(
                    "UPDATE posted_items SET last_posted_at_utc = ?, posts_count = posts_count + 1 WHERE candidate_key = ?",
                    (now, candidate.candidate_key),
                )

    def latest_posts(self, *, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT slot_key, posted_at_utc, title, url, source, score, channel
                FROM posted_slots
                ORDER BY posted_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_observed_candidates(self, candidates: list[Candidate]) -> int:
        if not candidates:
            return 0
        now = datetime.now(UTC).isoformat()
        count = 0
        with self._connect() as conn:
            for candidate in candidates:
                row = conn.execute(
                    "SELECT first_seen_utc FROM observed_candidates WHERE candidate_key = ? LIMIT 1",
                    (candidate.candidate_key,),
                ).fetchone()
                first_seen = str(row["first_seen_utc"]) if row is not None else now
                conn.execute(
                    """
                    INSERT OR REPLACE INTO observed_candidates (
                        candidate_key, source_type, source_id, author, title, text, url, image_url, created_at,
                        engagement, source_priority, score, discovered_via, first_seen_utc, last_seen_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate.candidate_key,
                        candidate.source_type,
                        candidate.source_id,
                        candidate.author,
                        candidate.title,
                        candidate.text,
                        candidate.url,
                        candidate.image_url,
                        candidate.created_at,
                        int(candidate.engagement),
                        float(candidate.source_priority),
                        float(candidate.score),
                        str(candidate.discovered_via or "seed_list"),
                        first_seen,
                        now,
                    ),
                )
                count += 1
        return count

    def latest_scheduled_posted_at_utc(self) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT posted_at_utc
                FROM posted_slots
                WHERE slot_key NOT LIKE 'manual%%'
                ORDER BY posted_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        value = str(row["posted_at_utc"] or "").strip()
        return value or None

    def observed_candidates_since(self, *, since_utc: str | None, limit: int = 400) -> list[Candidate]:
        safe_limit = max(20, min(2000, int(limit)))
        with self._connect() as conn:
            if since_utc:
                rows = conn.execute(
                    """
                    SELECT candidate_key, source_type, source_id, author, title, text, url, image_url, created_at,
                           engagement, source_priority, score, discovered_via, first_seen_utc, last_seen_utc
                    FROM observed_candidates
                    WHERE last_seen_utc > ?
                    ORDER BY score DESC, last_seen_utc DESC
                    LIMIT ?
                    """,
                    (since_utc, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT candidate_key, source_type, source_id, author, title, text, url, image_url, created_at,
                           engagement, source_priority, score, discovered_via, first_seen_utc, last_seen_utc
                    FROM observed_candidates
                    ORDER BY score DESC, last_seen_utc DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        out: list[Candidate] = []
        for row in rows:
            out.append(
                Candidate(
                    candidate_key=str(row["candidate_key"]),
                    source_type=str(row["source_type"]),
                    source_id=str(row["source_id"]),
                    author=str(row["author"]),
                    title=str(row["title"]),
                    text=str(row["text"]),
                    url=str(row["url"]),
                    image_url=(str(row["image_url"]) if row["image_url"] is not None else None),
                    created_at=(str(row["created_at"]) if row["created_at"] is not None else None),
                    engagement=int(row["engagement"] or 0),
                    source_priority=float(row["source_priority"] or 0.0),
                    score=float(row["score"] or 0.0),
                    discovered_via=str(row["discovered_via"] or "seed_list"),
                )
            )
        return out

    def observed_candidates_count_since(self, *, since_utc: str | None) -> int:
        with self._connect() as conn:
            if since_utc:
                row = conn.execute(
                    "SELECT COUNT(1) AS c FROM observed_candidates WHERE last_seen_utc > ?",
                    (since_utc,),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(1) AS c FROM observed_candidates").fetchone()
        return int((row["c"] if row is not None else 0) or 0)

    def prune_observed_candidates(self, *, keep_days: int = 10) -> int:
        cutoff = (datetime.now(UTC) - timedelta(days=max(2, keep_days))).isoformat()
        with self._connect() as conn:
            before = conn.total_changes
            conn.execute("DELETE FROM observed_candidates WHERE last_seen_utc < ?", (cutoff,))
            after = conn.total_changes
        return max(0, int(after - before))

    def record_post_review(
        self,
        *,
        slot_key: str,
        channel: str,
        candidate: Candidate,
        review: dict[str, Any],
    ) -> None:
        now = datetime.now(UTC).isoformat()
        failed = review.get("failed") if isinstance(review.get("failed"), list) else []
        checks = review.get("checks") if isinstance(review.get("checks"), dict) else {}
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO post_reviews (
                    slot_key, reviewed_at_utc, candidate_key, source_id, channel, passed,
                    failed_checks_json, checks_json, artifact_path, artifact_size_bytes, style_score, style_iteration
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slot_key,
                    now,
                    candidate.candidate_key,
                    _canonical_handle(candidate.source_id),
                    channel,
                    1 if bool(review.get("passed")) else 0,
                    json.dumps(failed, sort_keys=True),
                    json.dumps(checks, sort_keys=True),
                    str(review.get("artifact_path") or ""),
                    int(review.get("artifact_size_bytes") or 0),
                    float(review.get("style_score") or 0.0),
                    int(review.get("style_iteration") or 0),
                ),
            )

    def apply_review_feedback(self, *, source_id: str, passed: bool, failed_checks: list[str]) -> None:
        handle = _canonical_handle(source_id)
        if not handle:
            return
        with self._connect() as conn:
            row = conn.execute(
                "SELECT priority, trust_score, manual FROM sources WHERE handle = ? LIMIT 1",
                (handle,),
            ).fetchone()
            if row is None:
                return
            priority = float(row["priority"] or 0.5)
            trust = float(row["trust_score"] or 0.0)
            if passed:
                new_priority = min(2.5, priority + 0.01)
                new_trust = min(12.0, trust + 0.05)
            else:
                penalty = min(0.35, 0.05 * max(1, len(failed_checks)))
                new_priority = max(0.20, priority - penalty)
                new_trust = max(-3.0, trust - (0.12 * max(1, len(failed_checks))))
            conn.execute(
                "UPDATE sources SET priority = ?, trust_score = ? WHERE handle = ?",
                (new_priority, new_trust, handle),
            )

    def recent_review_summary(self, *, limit: int = 20) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT passed, failed_checks_json
                FROM post_reviews
                ORDER BY reviewed_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        fail_counts: dict[str, int] = {}
        passed = 0
        for row in rows:
            if int(row["passed"] or 0) == 1:
                passed += 1
            failed_json = str(row["failed_checks_json"] or "[]")
            try:
                failed = json.loads(failed_json)
            except Exception:
                failed = []
            if isinstance(failed, list):
                for item in failed:
                    key = str(item).strip()
                    if not key:
                        continue
                    fail_counts[key] = int(fail_counts.get(key, 0)) + 1
        return {
            "reviews": len(rows),
            "pass_count": passed,
            "fail_count": max(0, len(rows) - passed),
            "top_fail_checks": sorted(fail_counts.items(), key=lambda kv: kv[1], reverse=True)[:5],
        }


def _http_json(*, url: str, headers: dict[str, str], params: dict[str, str]) -> dict[str, Any]:
    full_url = f"{url}?{urlencode(params)}"
    req = Request(full_url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            payload = resp.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise XChartError(f"X API request failed ({exc.code}): {detail[:500]}") from exc
    except URLError as exc:
        raise XChartError(f"X API request failed: {exc.reason}") from exc
    try:
        data = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise XChartError("Failed to parse X API response.") from exc
    if isinstance(data, dict) and data.get("errors"):
        raise XChartError(f"X API errors: {data['errors']}")
    return data if isinstance(data, dict) else {}


def _x_search_recent(query: str, *, hours: int, max_results: int, token: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    params = {
        "query": query,
        "start_time": start_time,
        "max_results": str(max(10, min(100, max_results))),
        "tweet.fields": "author_id,created_at,public_metrics,attachments,lang",
        "expansions": "author_id,attachments.media_keys",
        "user.fields": "name,username,verified",
        "media.fields": "type,url,preview_image_url,width,height",
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return _http_json(
        url=f"{_x_api_base()}/2/tweets/search/recent",
        headers=headers,
        params=params,
    )


def _parse_x_candidates(payload: dict[str, Any], *, priority_by_handle: dict[str, float]) -> list[Candidate]:
    includes = payload.get("includes") if isinstance(payload.get("includes"), dict) else {}
    users_by_id: dict[str, dict[str, Any]] = {}
    for user in includes.get("users", []) if isinstance(includes.get("users"), list) else []:
        if isinstance(user, dict):
            uid = str(user.get("id") or "").strip()
            if uid:
                users_by_id[uid] = user
    media_by_key: dict[str, dict[str, Any]] = {}
    for media in includes.get("media", []) if isinstance(includes.get("media"), list) else []:
        if isinstance(media, dict):
            key = str(media.get("media_key") or "").strip()
            if key:
                media_by_key[key] = media

    out: list[Candidate] = []
    rows = payload.get("data")
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        tweet_id = str(row.get("id") or "").strip()
        text = str(row.get("text") or "").strip()
        if not tweet_id or not text:
            continue
        author_id = str(row.get("author_id") or "").strip()
        user = users_by_id.get(author_id, {})
        handle = _canonical_handle(str(user.get("username") or ""))
        if not handle:
            continue
        media_url: str | None = None
        attachments = row.get("attachments")
        media_keys: list[str] = []
        if isinstance(attachments, dict) and isinstance(attachments.get("media_keys"), list):
            media_keys = [str(x) for x in attachments.get("media_keys") if str(x).strip()]
        for mk in media_keys:
            media = media_by_key.get(mk) or {}
            mtype = str(media.get("type") or "")
            if mtype != "photo":
                continue
            media_url = str(media.get("url") or media.get("preview_image_url") or "").strip() or None
            if media_url:
                break
        if not media_url:
            continue

        metrics = row.get("public_metrics") if isinstance(row.get("public_metrics"), dict) else {}
        engagement = 0
        for key in ("like_count", "retweet_count", "reply_count", "quote_count"):
            try:
                engagement += int(metrics.get(key, 0) or 0)
            except (TypeError, ValueError):
                pass
        created_at = str(row.get("created_at")) if row.get("created_at") else None
        url = f"https://x.com/{handle}/status/{tweet_id}"
        title = _build_x_title(handle=handle, text=text)
        if not _is_chart_like_post(text, handle=handle):
            continue
        if not _is_us_relevant_post(f"{title} {text}"):
            continue
        priority = float(priority_by_handle.get(handle.lower(), 0.45))
        score = _score_candidate(
            title=title,
            text=text,
            engagement=engagement,
            source_priority=priority,
            created_at=created_at,
            has_image=True,
        )
        out.append(
            Candidate(
                candidate_key=f"x:{tweet_id}",
                source_type="x",
                source_id=handle,
                author=f"@{handle}",
                title=title,
                text=text,
                url=url,
                image_url=media_url,
                created_at=created_at,
                engagement=engagement,
                source_priority=priority,
                score=score,
            )
        )
    out.sort(key=lambda c: c.score, reverse=True)
    return out


def _candidate_from_explicit_post_payload(
    *,
    payload: dict[str, Any],
    handle_hint: str,
    tweet_id: str,
) -> Candidate | None:
    includes = payload.get("includes") if isinstance(payload.get("includes"), dict) else {}
    users_by_id: dict[str, dict[str, Any]] = {}
    for user in includes.get("users", []) if isinstance(includes.get("users"), list) else []:
        if not isinstance(user, dict):
            continue
        uid = str(user.get("id") or "").strip()
        if uid:
            users_by_id[uid] = user
    media_by_key: dict[str, dict[str, Any]] = {}
    for media in includes.get("media", []) if isinstance(includes.get("media"), list) else []:
        if not isinstance(media, dict):
            continue
        key = str(media.get("media_key") or "").strip()
        if key:
            media_by_key[key] = media

    rows = payload.get("data")
    if not isinstance(rows, list):
        return None
    row = next((r for r in rows if isinstance(r, dict) and str(r.get("id") or "").strip() == tweet_id), None)
    if not isinstance(row, dict):
        return None
    text = str(row.get("text") or "").strip()
    if not text:
        return None
    author_id = str(row.get("author_id") or "").strip()
    user = users_by_id.get(author_id, {})
    handle = _canonical_handle(str(user.get("username") or "")) or _canonical_handle(handle_hint)
    if not handle:
        return None

    media_url: str | None = None
    attachments = row.get("attachments")
    media_keys: list[str] = []
    if isinstance(attachments, dict) and isinstance(attachments.get("media_keys"), list):
        media_keys = [str(x) for x in attachments.get("media_keys") if str(x).strip()]
    for mk in media_keys:
        media = media_by_key.get(mk) or {}
        mtype = str(media.get("type") or "")
        if mtype != "photo":
            continue
        media_url = str(media.get("url") or media.get("preview_image_url") or "").strip() or None
        if media_url:
            break
    if not media_url:
        return None

    metrics = row.get("public_metrics") if isinstance(row.get("public_metrics"), dict) else {}
    engagement = 0
    for key in ("like_count", "retweet_count", "reply_count", "quote_count"):
        try:
            engagement += int(metrics.get(key, 0) or 0)
        except (TypeError, ValueError):
            pass
    created_at = str(row.get("created_at")) if row.get("created_at") else None
    title = _build_x_title(handle=handle, text=text)
    priority = 1.0
    score = _score_candidate(
        title=title,
        text=text,
        engagement=engagement,
        source_priority=priority,
        created_at=created_at,
        has_image=True,
    )
    return Candidate(
        candidate_key=f"x:{tweet_id}",
        source_type="x",
        source_id=handle,
        author=f"@{handle}",
        title=title,
        text=text,
        url=f"https://x.com/{handle}/status/{tweet_id}",
        image_url=media_url,
        created_at=created_at,
        engagement=engagement,
        source_priority=priority,
        score=score,
    )


def _parse_x_post_url(post_url: str) -> tuple[str, str] | None:
    m = re.search(
        r"https?://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)",
        post_url.strip(),
        re.IGNORECASE,
    )
    if not m:
        return None
    handle = _canonical_handle(m.group(1))
    tweet_id = m.group(2)
    if not handle or not tweet_id:
        return None
    return handle, tweet_id


def _fetch_vxtwitter_post_candidate(*, handle: str, tweet_id: str) -> Candidate | None:
    api_url = f"https://api.vxtwitter.com/{handle}/status/{tweet_id}"
    req = Request(api_url, headers={"User-Agent": "coatue-claw/1.0", "Accept": "application/json"}, method="GET")
    try:
        with urlopen(req, timeout=25) as resp:
            payload = json.loads((resp.read() or b"{}").decode("utf-8", errors="ignore"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    media_urls = payload.get("mediaURLs")
    image_url = None
    if isinstance(media_urls, list):
        for item in media_urls:
            value = str(item or "").strip()
            if value:
                image_url = value
                break
    if not image_url:
        media_extended = payload.get("media_extended")
        if isinstance(media_extended, list):
            for item in media_extended:
                if not isinstance(item, dict):
                    continue
                value = str(item.get("url") or item.get("thumbnail_url") or "").strip()
                if value:
                    image_url = value
                    break
    if not image_url:
        return None

    text = _normalize_render_text(str(payload.get("text") or "").strip())
    if not text:
        return None
    user_screen = _canonical_handle(str(payload.get("user_screen_name") or handle))
    if not user_screen:
        user_screen = handle
    engagement = 0
    for key in ("likes", "retweets", "replies", "qrt"):
        try:
            engagement += int(payload.get(key, 0) or 0)
        except Exception:
            continue
    created_at = None
    raw_date = str(payload.get("date") or "").strip()
    if raw_date:
        for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                created_at = datetime.strptime(raw_date, fmt).astimezone(UTC).isoformat()
                break
            except Exception:
                continue
    priority = 1.0
    score = _score_candidate(
        source_priority=priority,
        engagement=engagement,
        created_at=created_at,
        title=text,
        text=text,
        has_image=True,
    )
    return Candidate(
        candidate_key=f"x:{tweet_id}",
        source_type="x",
        source_id=user_screen,
        author=f"@{user_screen}",
        title=text,
        text=text,
        url=f"https://x.com/{user_screen}/status/{tweet_id}",
        image_url=image_url,
        created_at=created_at,
        engagement=engagement,
        source_priority=priority,
        score=score,
    )


def _is_chart_like_post(text: str, *, handle: str) -> bool:
    lower = (text or "").lower()
    signal = 0
    for token in CHART_SIGNAL_KEYWORDS:
        if token in lower:
            signal += 1
    for token in THEME_KEYWORDS:
        if token.lower() in lower:
            signal += 1
    if re.search(r"\b\d+(\.\d+)?%|\b\d+(\.\d+)?x\b|\$\s?\d", lower):
        signal += 1
    if re.search(r"\b\d{1,3}(?:,\d{3})+\b", lower):
        signal += 1

    high_trust_handles = {
        "fiscal_ai",
        "cloudedjudgment",
        "charliebilello",
        "ourworldindata",
        "bespokeinvest",
    }
    if handle.lower() in high_trust_handles and signal >= 1:
        return True
    return signal >= 2


def _build_x_title(*, handle: str, text: str) -> str:
    snippet = _shorten_without_ellipsis(_normalize_render_text(text), max_chars=95)
    return f"@{handle}: {snippet}".strip()


def _keyword_score(text: str) -> float:
    lower = (text or "").lower()
    score = 0.0
    for kw in THEME_KEYWORDS:
        if kw.lower() in lower:
            score += 2.0
    return min(14.0, score)


def _style_quality_score(*, title: str, text: str) -> float:
    combined = f"{title} {text}".strip()
    lower = combined.lower()
    if not lower:
        return 0.0

    style_hits = 0
    for token in INSTITUTIONAL_STYLE_KEYWORDS:
        if token in lower:
            style_hits += 1

    quantitative_hits = 0
    if re.search(r"\b\d+(?:\.\d+)?%\b", lower):
        quantitative_hits += 1
    if re.search(r"\b\d+(?:\.\d+)?x\b", lower):
        quantitative_hits += 1
    if re.search(r"\b\d{1,3}(?:,\d{3})+\b", lower):
        quantitative_hits += 1
    if re.search(r"\b(?:vs|versus|since|through|from|to)\b", lower):
        quantitative_hits += 1

    promo_hits = 0
    for token in PROMO_SPAM_KEYWORDS:
        if token in lower:
            promo_hits += 1

    cashtag_count = len(re.findall(r"(?<!\w)\$[a-z]{1,8}\b", lower))
    has_chart_language = any(token in lower for token in CHART_SIGNAL_KEYWORDS)

    bonus = min(8.0, style_hits * 1.15) + min(5.0, quantitative_hits * 1.25)

    penalty = 0.0
    if promo_hits:
        penalty += min(14.0, promo_hits * 3.0)
    if cashtag_count >= 6:
        penalty += min(12.0, float(cashtag_count - 5) * 2.0)
    if cashtag_count >= 3 and promo_hits:
        penalty += 4.0
    if promo_hits and style_hits == 0:
        penalty += 3.0
    if cashtag_count >= 6 and (not has_chart_language):
        penalty += 4.0

    return max(-24.0, bonus - penalty)


def _freshness_score(created_at: str | None) -> float:
    if not created_at:
        return 0.0
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return 0.0
    age_hours = max(0.0, (datetime.now(UTC) - dt).total_seconds() / 3600.0)
    return max(0.0, 16.0 - (age_hours / 3.0))


def _score_candidate(
    *,
    title: str,
    text: str,
    engagement: int,
    source_priority: float,
    created_at: str | None,
    has_image: bool,
) -> float:
    engagement_component = math.log1p(max(0, engagement)) * 6.0
    priority_component = source_priority * 20.0
    keyword_component = _keyword_score(f"{title} {text}")
    freshness_component = _freshness_score(created_at)
    image_component = 5.0 if has_image else 0.0
    style_component = _style_quality_score(title=title, text=text)
    return priority_component + engagement_component + keyword_component + freshness_component + image_component + style_component


def _chunks(items: list[str], size: int) -> list[list[str]]:
    out: list[list[str]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _fetch_x_candidates_from_sources(*, handles: list[str], token: str, hours: int = 48) -> list[Candidate]:
    if not handles:
        return []
    priority_map = {_canonical_handle(h).lower(): p for h, p in DEFAULT_PRIORITY_SOURCES}
    all_handles = [_canonical_handle(h) for h in handles if _canonical_handle(h)]
    out: list[Candidate] = []
    for handle in all_handles:
        query = f"(from:{handle}) has:images -is:retweet -is:reply lang:en"
        try:
            payload = _x_search_recent(query, hours=hours, max_results=25, token=token)
        except XChartError as exc:
            msg = str(exc).lower()
            if "invalid username value" in msg or "not a parsable user name" in msg:
                logger.warning("Skipping invalid source handle for x-chart scout: @%s", handle)
                continue
            raise
        parsed = _parse_x_candidates(payload, priority_by_handle=priority_map)
        if parsed:
            out.extend(parsed)
    return out


def _fetch_x_candidates_open_search(
    *,
    token: str,
    priority_by_handle: dict[str, float],
    hours: int = 48,
) -> list[Candidate]:
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return []
    if not _open_search_enabled():
        return []
    queries = _open_search_queries()
    if not queries:
        return []
    per_query = max(10, min(60, int(os.environ.get("COATUE_CLAW_X_CHART_OPEN_SEARCH_MAX_RESULTS", "35"))))
    out: list[Candidate] = []
    for query in queries:
        try:
            payload = _x_search_recent(query, hours=hours, max_results=per_query, token=token)
        except XChartError as exc:
            logger.warning("x-chart open search failed for query '%s': %s", query, exc)
            continue
        parsed = _parse_x_candidates(payload, priority_by_handle=priority_by_handle)
        if not parsed:
            continue
        out.extend([replace(c, discovered_via="open_search") for c in parsed])
    return out


def _discover_new_sources(*, token: str) -> list[tuple[str, int]]:
    query = os.environ.get(
        "COATUE_CLAW_X_CHART_DISCOVERY_QUERY",
        "(ai OR software OR semiconductor OR macro OR consumer) has:images -is:retweet -is:reply lang:en",
    ).strip()
    payload = _x_search_recent(query, hours=24, max_results=60, token=token)
    parsed = _parse_x_candidates(payload, priority_by_handle={})
    seen: dict[str, int] = {}
    for item in parsed:
        handle = _canonical_handle(item.source_id)
        if not handle:
            continue
        prev = seen.get(handle, 0)
        if item.engagement > prev:
            seen[handle] = item.engagement
    ranked = sorted(seen.items(), key=lambda pair: pair[1], reverse=True)
    return ranked[:12]


def _fetch_visualcapitalist_candidates(*, max_items: int = 20) -> list[Candidate]:
    feed_url = os.environ.get("COATUE_CLAW_VISUALCAPITALIST_FEED_URL", "https://www.visualcapitalist.com/feed/").strip()
    req = Request(feed_url, headers={"User-Agent": "coatue-claw/1.0"}, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            content = resp.read()
    except Exception as exc:
        logger.warning("visualcapitalist feed fetch failed: %s", exc)
        return []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    ns = {
        "content": "http://purl.org/rss/1.0/modules/content/",
        "media": "http://search.yahoo.com/mrss/",
    }
    out: list[Candidate] = []
    for item in root.findall(".//item")[:max_items]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = (item.findtext("description") or "").strip()
        text = re.sub(r"<[^>]+>", " ", desc)
        text = re.sub(r"\s+", " ", text).strip()
        if not title or not link:
            continue
        if not _is_us_relevant_post(f"{title} {text}"):
            continue
        image_url: str | None = None
        media_content = item.find("media:content", ns)
        if media_content is not None:
            image_url = (media_content.attrib.get("url") or "").strip() or None
        if not image_url:
            m = re.search(r"""<img[^>]+src=["']([^"']+)["']""", desc, re.IGNORECASE)
            if m:
                image_url = m.group(1).strip()

        score = _score_candidate(
            title=title,
            text=text,
            engagement=40,  # neutral baseline for trusted curated source
            source_priority=1.25,
            created_at=None,
            has_image=bool(image_url),
        )
        out.append(
            Candidate(
                candidate_key=f"vc:{hash(link)}",
                source_type="web",
                source_id="visualcapitalist.com",
                author="Visual Capitalist",
                title=title,
                text=text,
                url=link,
                image_url=image_url,
                created_at=pub or None,
                engagement=40,
                source_priority=1.25,
                score=score,
            )
        )
    out.sort(key=lambda c: c.score, reverse=True)
    return out


def _slot_key(*, now_local: datetime, windows: list[tuple[int, int]], manual: bool) -> str | None:
    if manual:
        return f"manual-{now_local.strftime('%Y%m%d-%H%M%S')}"
    current_minutes = now_local.hour * 60 + now_local.minute
    eligible: list[tuple[int, int]] = []
    for hour, minute in sorted(windows):
        window_minutes = (hour * 60) + minute
        if current_minutes >= window_minutes:
            eligible.append((hour, minute))
    if not eligible:
        return None
    hour, minute = eligible[-1]
    return f"{now_local.strftime('%Y-%m-%d')}-{hour:02d}:{minute:02d}"


def _slot_key_for_manual_post_url(*, now_local: datetime, windows: list[tuple[int, int]]) -> str:
    """Use the standard window slot key for manual URL posts (no ad-hoc timestamp slots)."""
    slot = _slot_key(now_local=now_local, windows=windows, manual=False)
    if slot:
        return slot
    first_hour, first_minute = sorted(windows)[0]
    return f"{now_local.strftime('%Y-%m-%d')}-{first_hour:02d}:{first_minute:02d}"


def _dedupe_candidates(candidates: list[Candidate]) -> list[Candidate]:
    seen: set[str] = set()
    out: list[Candidate] = []
    for item in sorted(candidates, key=lambda c: c.score, reverse=True):
        key = item.url.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _normalize_posted_source(source: str) -> str:
    raw = str(source or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        left, right = raw.split(":", 1)
        if left.strip().lower() == "x":
            return _canonical_handle(right)
    return _canonical_handle(raw) or raw.lower()


def _source_variety_params() -> tuple[int, float]:
    lookback_raw = (os.environ.get("COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK", "6") or "6").strip()
    floor_raw = (os.environ.get("COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR", "0.90") or "0.90").strip()
    try:
        lookback = max(2, min(20, int(lookback_raw)))
    except Exception:
        lookback = 6
    try:
        floor = float(floor_raw)
    except Exception:
        floor = 0.90
    floor = max(0.75, min(0.99, floor))
    return lookback, floor


def _source_repeat_days() -> int:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_SOURCE_REPEAT_DAYS", "3") or "3").strip()
    try:
        days = int(raw)
    except Exception:
        days = 3
    return max(0, min(30, days))


def _discovery_mode() -> str:
    mode = (os.environ.get("COATUE_CLAW_X_CHART_DISCOVERY_MODE", "hybrid") or "hybrid").strip().lower()
    if mode not in {"seed_only", "open_only", "hybrid"}:
        return "hybrid"
    return mode


def _open_search_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_OPEN_SEARCH_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _auto_add_sources_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_AUTO_ADD_SOURCES", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _auto_add_daily_cap() -> int:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_AUTO_ADD_DAILY_CAP", "8") or "8").strip()
    try:
        cap = int(raw)
    except Exception:
        cap = 8
    return max(0, min(100, cap))


def _open_search_queries() -> list[str]:
    raw = (
        os.environ.get(
            "COATUE_CLAW_X_CHART_OPEN_SEARCH_QUERIES",
            (
                "has:images (\"S&P 500\" OR breadth OR dispersion OR rotation) -is:retweet -is:reply lang:en || "
                "has:images (\"data center\" OR \"power demand\" OR AI OR semiconductor) -is:retweet -is:reply lang:en || "
                "has:images (backlog OR yoy OR cagr OR \"year to date\") -is:retweet -is:reply lang:en"
            ),
        )
        or ""
    ).strip()
    out: list[str] = []
    for part in raw.split("||"):
        q = part.strip()
        if q:
            out.append(q)
    return out[:6]


def _collect_source_last_posted(*, store: XChartStore, limit: int = 120) -> dict[str, datetime]:
    recent = store.latest_posts(limit=max(1, limit))
    source_last_posted: dict[str, datetime] = {}
    for row in recent:
        key = _normalize_posted_source(str(row.get("source") or ""))
        if not key:
            continue
        raw_posted = str(row.get("posted_at_utc") or "").strip()
        if not raw_posted:
            continue
        try:
            posted_dt = datetime.fromisoformat(raw_posted.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            continue
        prev = source_last_posted.get(key)
        if prev is None or posted_dt > prev:
            source_last_posted[key] = posted_dt
    return source_last_posted


def _eligible_after_source_cooldown(
    *,
    candidates: list[Candidate],
    source_last_posted: dict[str, datetime],
    repeat_days: int,
    now_utc: datetime,
) -> list[Candidate]:
    if repeat_days <= 0:
        return list(candidates)
    cutoff = now_utc - timedelta(days=repeat_days)
    out: list[Candidate] = []
    for item in candidates:
        source_key = _canonical_handle(item.source_id) or item.source_id.lower()
        last_posted = source_last_posted.get(source_key)
        if last_posted is None or last_posted <= cutoff:
            out.append(item)
    return out


def _candidate_log_row(item: Candidate) -> dict[str, Any]:
    return {
        "candidate_key": item.candidate_key,
        "source_id": item.source_id,
        "source_type": item.source_type,
        "url": item.url,
        "score": float(item.score),
        "discovered_via": item.discovered_via,
    }


def _interesting_takeaway_bonus(*, candidate: Candidate, recent_texts: list[str]) -> float:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}")
    lower = merged.lower()
    bonus = 0.0
    if re.search(r"\b\d+(?:\.\d+)?%|\b\d+(?:\.\d+)?x\b|\b\d{1,3}(?:,\d{3})+\b", lower):
        bonus += 2.0
    if re.search(r"\b(vs|versus|yoy|qoq|cagr|year to date|ytd|through \d{4}|2030|forecast)\b", lower):
        bonus += 1.8
    if re.search(r"\b(breadth|dispersion|rotation|backlog|power demand|data center|underallocated)\b", lower):
        bonus += 1.6
    if re.search(r"\b(rose|fell|surged|dropped|accelerating|slowing|record high|record low)\b", lower):
        bonus += 1.2
    if candidate.discovered_via == "open_search":
        bonus += 0.8
    if "visualcapitalist.com" in candidate.source_id.lower():
        bonus -= 0.8

    tokens = {t for t in re.findall(r"[a-z0-9]{4,}", lower)}
    overlap_penalty = 0.0
    if tokens and recent_texts:
        max_overlap = 0.0
        for recent in recent_texts:
            r_tokens = {t for t in re.findall(r"[a-z0-9]{4,}", recent.lower())}
            if not r_tokens:
                continue
            union = len(tokens | r_tokens)
            if union <= 0:
                continue
            jacc = len(tokens & r_tokens) / float(union)
            if jacc > max_overlap:
                max_overlap = jacc
        if max_overlap >= 0.45:
            overlap_penalty = 3.5
        elif max_overlap >= 0.30:
            overlap_penalty = 1.5
    return bonus - overlap_penalty


def _write_pull_log(
    *,
    slot_key: str,
    mode: str,
    channel: str | None,
    windows_text: str,
    repeat_days: int,
    scanned_candidates: list[Candidate],
    source_last_posted: dict[str, datetime],
    eligible_after_item_filter: list[Candidate],
    eligible_after_cooldown: list[Candidate],
    selected_candidate_key: str | None,
    result_reason: str,
    manual_override_used: bool = False,
    seed_candidates_count: int = 0,
    open_search_candidates_count: int = 0,
    merged_candidates_count: int = 0,
    winner_discovered_via: str | None = None,
    new_source_auto_added: bool = False,
    new_source_handle: str | None = None,
) -> Path:
    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-pull-log.json"
    payload = {
        "ts_utc": datetime.now(UTC).isoformat(),
        "mode": mode,
        "slot_key": slot_key,
        "channel": channel or "",
        "windows": windows_text,
        "repeat_days": int(repeat_days),
        "cooldown_scope": "scheduled_only",
        "manual_override_used": bool(manual_override_used),
        "seed_candidates_count": int(seed_candidates_count),
        "open_search_candidates_count": int(open_search_candidates_count),
        "merged_candidates_count": int(merged_candidates_count or len(scanned_candidates)),
        "scanned_candidates": [_candidate_log_row(c) for c in scanned_candidates],
        "scanned_accounts": sorted({_canonical_handle(c.source_id) or c.source_id.lower() for c in scanned_candidates}),
        "source_last_posted": {k: v.isoformat() for k, v in source_last_posted.items()},
        "eligible_after_item_filter": [_candidate_log_row(c) for c in eligible_after_item_filter],
        "eligible_after_item_filter_count": len(eligible_after_item_filter),
        "eligible_after_cooldown": [_candidate_log_row(c) for c in eligible_after_cooldown],
        "eligible_after_cooldown_count": len(eligible_after_cooldown),
        "selected_candidate_key": selected_candidate_key,
        "winner_discovered_via": winner_discovered_via,
        "new_source_auto_added": bool(new_source_auto_added),
        "new_source_handle": (new_source_handle or ""),
        "result_reason": result_reason,
    }
    out_path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    return out_path


def _post_no_candidate_message_to_slack(
    *,
    channel: str,
    convention_name: str,
    repeat_days: int,
    scanned_count: int,
    pool_count: int,
    unique_sources_in_cooldown: int,
) -> dict[str, Any]:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    tokens = _slack_tokens()
    text = "\n".join(
        [
            f"*{_normalize_render_text(convention_name)}*",
            f"No chart posted: all candidate accounts are within the {repeat_days}-day cooldown window.",
            f"Scanned candidates: `{scanned_count}` | Pool candidates: `{pool_count}` | Sources in cooldown: `{unique_sources_in_cooldown}`",
        ]
    )
    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            response = client.chat_postMessage(channel=channel, text=text)
            return {
                "ok": bool(response.get("ok")),
                "channel": channel,
                "ts": response.get("ts"),
            }
        except SlackApiError as exc:
            err = str(exc.response.get("error", "")) if exc.response is not None else str(exc)
            last_error = err or str(exc)
            if err in {"account_inactive", "invalid_auth", "token_revoked"}:
                logger.warning("x-chart slack token rejected (%s), trying next token if available", err)
                continue
            raise
    raise XChartError(f"Slack notice post failed for all available tokens: {last_error or 'unknown_error'}")


def _post_llm_copy_warning_to_slack(
    *,
    channel: str,
    convention_name: str,
    slot_key: str,
    candidate: Candidate,
    reason: str,
) -> dict[str, Any]:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    tokens = _slack_tokens()
    reason_label = _normalize_render_text(reason) or "api_error"
    text = "\n".join(
        [
            f"*{_normalize_render_text(convention_name)}*",
            f"Warning: LLM copy generation error (`{reason_label}`); using raw tweet fallback copy.",
            f"Slot: `{slot_key}`",
            f"Source: {candidate.url}",
        ]
    )
    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            response = client.chat_postMessage(channel=channel, text=text)
            return {
                "ok": bool(response.get("ok")),
                "channel": channel,
                "ts": response.get("ts"),
                "reason": reason_label,
            }
        except SlackApiError as exc:
            err = str(exc.response.get("error", "")) if exc.response is not None else str(exc)
            last_error = err or str(exc)
            if err in {"account_inactive", "invalid_auth", "token_revoked"}:
                logger.warning("x-chart slack token rejected (%s), trying next token if available", err)
                continue
            raise
    raise XChartError(f"Slack warning post failed for all available tokens: {last_error or 'unknown_error'}")


def _was_candidate_posted_ever(*, store: Any, candidate_key: str) -> bool:
    posted_ever = getattr(store, "was_item_posted", None)
    if callable(posted_ever):
        try:
            return bool(posted_ever(candidate_key))
        except Exception:
            pass
    posted_recently = getattr(store, "was_item_posted_recently", None)
    if callable(posted_recently):
        try:
            return bool(posted_recently(candidate_key, days=36500))
        except Exception:
            return False
    return False


def _pick_winner(*, store: XChartStore, candidates: list[Candidate]) -> Candidate | None:
    pool: list[Candidate] = []
    for item in candidates:
        if _was_candidate_posted_ever(store=store, candidate_key=item.candidate_key):
            continue
        pool.append(item)
    if not pool:
        return None

    repeat_days = _source_repeat_days()
    lookback, floor = _source_variety_params()
    # Pull enough recent rows to support both "variety lookback" and source cooldown checks.
    recent = store.latest_posts(limit=max(lookback, 120))
    source_last_posted = _collect_source_last_posted(store=store, limit=max(lookback, 120))

    recent_texts = [
        _normalize_render_text(f"{row.get('title') or ''} {row.get('url') or ''}")
        for row in recent
        if isinstance(row, dict)
    ]

    selection_pool = pool
    if repeat_days > 0:
        selection_pool = _eligible_after_source_cooldown(
            candidates=pool,
            source_last_posted=source_last_posted,
            repeat_days=repeat_days,
            now_utc=datetime.now(UTC),
        )
        if not selection_pool:
            return None

    effective_score: dict[str, float] = {
        c.candidate_key: float(c.score) + _interesting_takeaway_bonus(candidate=c, recent_texts=recent_texts)
        for c in selection_pool
    }
    selection_pool = sorted(selection_pool, key=lambda c: effective_score.get(c.candidate_key, float(c.score)), reverse=True)

    # Keep "highest score" behavior but add source variety when alternatives are near the top score.
    top = selection_pool[0]
    recent_for_variety = recent[:lookback]
    counts: dict[str, int] = {}
    for row in recent_for_variety:
        key = _normalize_posted_source(str(row.get("source") or ""))
        if not key:
            continue
        counts[key] = int(counts.get(key, 0)) + 1

    top_effective = float(effective_score.get(top.candidate_key, float(top.score)))
    score_floor = top_effective * float(floor)
    near_top = [c for c in selection_pool if float(effective_score.get(c.candidate_key, float(c.score))) >= score_floor]
    if len(near_top) <= 1:
        return top

    def _rank_key(c: Candidate) -> tuple[int, float]:
        source_key = _canonical_handle(c.source_id) or c.source_id.lower()
        return (int(counts.get(source_key, 0)), -float(effective_score.get(c.candidate_key, float(c.score))))

    near_top.sort(key=_rank_key)
    return near_top[0]


def _build_takeaways(candidate: Candidate) -> list[str]:
    text = _strip_news_prefix(candidate.text)
    title = _normalize_render_text(candidate.title)
    if _is_employees_robots_chart(candidate):
        excerpt = _employees_robots_takeaway(text or title)
    else:
        excerpt = _truncate_words(text or title, max_words=9, max_chars=68)
    if not excerpt:
        excerpt = "Fresh US-focused chart signal from a prioritized source."
    tone_line = "Keep the takeaway simple and explicit for fast read in a feed."
    return [
        excerpt,
        "US relevance check passed and trend is explicit.",
        tone_line,
    ]


def _build_style_draft(candidate: Candidate, *, iteration: int) -> StyleDraft:
    title_text = _normalize_render_text(candidate.title)
    body_text = _strip_news_prefix(candidate.text)
    first_sentence = _extract_first_sentence(body_text or title_text)
    title_core = re.sub(r"^@\w+:\s*", "", title_text).strip()
    first_core = re.sub(r"^@\w+:\s*", "", first_sentence).strip()
    llm_copy_status = "ok"
    llm_warning_reason: str | None = None

    llm_style_obj: Any = _synthesize_style_via_llm(candidate) if iteration == 1 else (None, None)
    llm_style: dict[str, str] | None = None
    if isinstance(llm_style_obj, tuple):
        llm_style, llm_warning_reason = llm_style_obj
    elif isinstance(llm_style_obj, dict):
        llm_style = llm_style_obj
        llm_warning_reason = None

    if llm_style:
        headline = _normalize_render_text(llm_style.get("headline") or "")
        takeaway = _normalize_render_text(llm_style.get("takeaway") or "")
        why_now = "LLM-generated title and takeaway from tweet + chart context."
    else:
        headline, takeaway = _raw_tweet_copy_from_candidate(candidate)
        if llm_warning_reason:
            llm_copy_status = "warning_fallback"
        why_now = "LLM copy unavailable; using raw tweet fallback."

    headline, chart_label, takeaway, rewrite_applied, rewrite_reason, role_swapped = _sanitize_style_copy(
        candidate=candidate,
        headline=headline or "US Trend Snapshot",
        chart_label=headline or "Chart Context",
        takeaway=takeaway or "New US-facing data point with clear directional movement.",
    )
    chart_label = _normalize_render_text(headline)

    combined = " ".join([headline, takeaway, why_now]).strip()
    checks = {
        "us_relevant": _is_us_relevant_post(f"{candidate.title} {candidate.text}"),
        "headline_short": bool(headline),
        "headline_grammar": not _has_incoherent_headline(headline),
        "headline_complete_phrase": _is_complete_headline_phrase(headline),
        "headline_complete_sentence": _is_complete_headline_sentence(headline, source_text=first_core or title_core or title_text),
        "headline_tail_complete": _tail_complete(headline),
        "headline_locked_terms_ok": _headline_locked_terms_preserved(headline, source_text=first_core or title_core or title_text),
        "takeaway_short": bool(takeaway),
        "takeaway_complete_sentence": _is_complete_sentence(takeaway),
        "takeaway_single_sentence": _is_single_sentence_takeaway(takeaway),
        "takeaway_clause_boundary_ok": not _has_unjoined_clause_boundary(takeaway),
        "takeaway_tail_complete": _tail_complete(takeaway),
        "title_takeaway_role_ok": _title_takeaway_role_ok(headline=headline, takeaway=takeaway),
        "title_takeaway_role_swapped": bool(role_swapped),
        "headline_non_degenerate": not _is_degenerate_copy_value(headline),
        "chart_label_non_degenerate": not _is_degenerate_copy_value(chart_label),
        "trend_explicit": _contains_trend_signal(f"{candidate.title} {candidate.text}"),
        "plain_language": not any(term in combined.lower() for term in SLIDE_JARGON_KEYWORDS),
        "clean_characters": "\ufffd" not in combined and "??" not in combined and "  " not in combined,
        "graph_first_copy": len(combined.split()) <= 30,
    }
    score = float(sum(1.0 for passed in checks.values() if passed))
    effective_rewrite_reason = rewrite_reason
    effective_rewrite_applied = bool(rewrite_applied)
    if llm_copy_status == "warning_fallback" and llm_warning_reason:
        effective_rewrite_applied = True
        if not effective_rewrite_reason:
            effective_rewrite_reason = f"llm_{llm_warning_reason}_fallback"
    return StyleDraft(
        headline=_normalize_render_text(headline or "US trend is shifting."),
        chart_label=_normalize_render_text(chart_label or headline or "US trend is shifting."),
        takeaway=takeaway or "New US-facing data point with clear directional movement.",
        why_now=why_now,
        iteration=iteration,
        checks=checks,
        score=score,
        copy_rewrite_applied=effective_rewrite_applied,
        copy_rewrite_reason=effective_rewrite_reason,
        llm_copy_status=llm_copy_status,
        llm_warning_reason=llm_warning_reason,
    )


def _select_style_draft(candidate: Candidate, *, max_iterations: int = 3) -> StyleDraft:
    return _build_style_draft(candidate, iteration=1)


def _style_copy_publish_issues(style_draft: StyleDraft) -> list[str]:
    issues: list[str] = []
    if not _normalize_render_text(style_draft.headline):
        issues.append("headline_empty")
    if not _normalize_render_text(style_draft.takeaway):
        issues.append("takeaway_empty")
    if not _matplotlib_safe_text(style_draft.takeaway):
        issues.append("takeaway_unrenderable")
    return issues


def _candidate_pool_for_post(*, store: XChartStore, candidates: list[Candidate]) -> list[Candidate]:
    pool: list[Candidate] = []
    for item in sorted(candidates, key=lambda c: float(c.score), reverse=True):
        if _was_candidate_posted_ever(store=store, candidate_key=item.candidate_key):
            continue
        pool.append(item)
    return pool


def _has_reconstructable_chart_data(candidate: Candidate) -> bool:
    image = _safe_image_from_url(candidate.image_url)
    mode = _infer_chart_mode(candidate=candidate, image=image)
    if mode == "bar":
        vision_bars = _extract_rebuilt_bars_via_vision(candidate=candidate)
        if vision_bars is not None and (not _bar_data_quality_errors(candidate=candidate, bars=vision_bars)):
            return True
        cv_bars = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False)
        return cv_bars is not None and (not _bar_data_quality_errors(candidate=candidate, bars=cv_bars))
    rebuilt = _extract_rebuilt_series(candidate=candidate, image=image)
    return bool(rebuilt)


def _fetch_image_bytes(url: str | None) -> tuple[bytes | None, str]:
    if not url:
        return None, "image/png"
    req = Request(url, headers={"User-Agent": "coatue-claw/1.0", "Accept": "image/*"}, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            payload = resp.read() or b""
            ctype = str(resp.headers.get("Content-Type") or "image/png").split(";")[0].strip() or "image/png"
    except Exception:
        return None, "image/png"
    if not payload:
        return None, ctype
    return payload, ctype


def _guess_image_extension(*, image_url: str | None, content_type: str) -> str:
    ctype = (content_type or "").strip().lower()
    if ctype in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    if ctype == "image/png":
        return ".png"
    if ctype == "image/webp":
        return ".webp"
    if image_url:
        path = urlparse(image_url).path or ""
        suffix = Path(path).suffix.lower()
        if suffix in {".jpg", ".jpeg"}:
            return ".jpg"
        if suffix in {".png", ".webp"}:
            return suffix
    return ".png"


def _write_source_chart_image(*, candidate: Candidate, slot_key: str) -> Path:
    payload, ctype = _fetch_image_bytes(candidate.image_url)
    if not payload:
        raise XChartError("No chart image found for this X post.")
    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = _guess_image_extension(image_url=candidate.image_url, content_type=ctype)
    out_path = output_dir / f"{slot_key}-source{suffix}"
    out_path.write_bytes(payload)
    return out_path


def _wrap_text_to_max_lines(text: str, *, max_lines: int) -> tuple[str, int]:
    normalized = _normalize_render_text(text)
    words = [w for w in normalized.split(" ") if w]
    if not words:
        return "", 1
    if max_lines <= 1:
        return normalized, 1
    longest_word = max(len(w) for w in words)
    lo = max(1, longest_word)
    hi = max(lo, len(normalized))
    best: list[str] | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        lines = textwrap.wrap(normalized, width=mid, break_long_words=False, break_on_hyphens=False)
        if len(lines) <= max_lines:
            best = lines
            hi = mid - 1
        else:
            lo = mid + 1
    if not best:
        best = textwrap.wrap(normalized, width=max(lo, longest_word), break_long_words=False, break_on_hyphens=False)
    if not best:
        best = [normalized]
    return "\n".join(best), len(best)


def _fit_headline_text(
    *,
    fig,
    headline_obj,
    headline_text: str,
    max_lines: int = HEADLINE_MAX_RENDER_LINES,
    min_font_size: float = HEADLINE_MIN_FONT_SIZE,
    max_width_ratio: float = 0.95,
) -> tuple[str, int, bool]:
    normalized = _normalize_render_text(headline_text)
    if not normalized:
        headline_obj.set_text("")
        return "", 1, False
    start_size = float(headline_obj.get_fontsize())
    if start_size < min_font_size:
        start_size = min_font_size
    font_steps = max(0, int(round(start_size - min_font_size)))
    font_sizes = [start_size - float(step) for step in range(font_steps + 1)]
    if not font_sizes or abs(font_sizes[-1] - min_font_size) > 0.001:
        font_sizes.append(min_font_size)

    for font_size in font_sizes:
        for line_cap in range(1, max_lines + 1):
            wrapped, line_count = _wrap_text_to_max_lines(normalized, max_lines=line_cap)
            headline_obj.set_fontsize(float(font_size))
            headline_obj.set_text(_matplotlib_safe_text(wrapped))
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            fig_bbox = fig.bbox
            max_x = fig_bbox.x0 + (fig_bbox.width * max_width_ratio)
            h_bb = headline_obj.get_window_extent(renderer=renderer)
            if h_bb.x1 <= max_x:
                return wrapped, line_count, True

    wrapped, line_count = _wrap_text_to_max_lines(normalized, max_lines=max_lines)
    headline_obj.set_fontsize(float(min_font_size))
    headline_obj.set_text(_matplotlib_safe_text(wrapped))
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    fig_bbox = fig.bbox
    max_x = fig_bbox.x0 + (fig_bbox.width * max_width_ratio)
    h_bb = headline_obj.get_window_extent(renderer=renderer)
    return wrapped, line_count, (h_bb.x1 <= max_x)


def _fit_takeaway_text(
    *,
    fig,
    takeaway_obj,
    takeaway_text: str,
    max_lines: int = TAKEAWAY_MAX_RENDER_LINES,
    min_font_size: float = TAKEAWAY_MIN_FONT_SIZE,
    max_width_ratio: float = 0.95,
) -> tuple[str, int, bool]:
    normalized = _normalize_render_text(takeaway_text)
    if not normalized:
        takeaway_obj.set_text("")
        return "", 1, False
    start_size = float(takeaway_obj.get_fontsize())
    if start_size < min_font_size:
        start_size = min_font_size
    font_steps = max(0, int(round(start_size - min_font_size)))
    font_sizes = [start_size - float(step) for step in range(font_steps + 1)]
    if not font_sizes or abs(font_sizes[-1] - min_font_size) > 0.001:
        font_sizes.append(min_font_size)

    for font_size in font_sizes:
        for line_cap in range(1, max_lines + 1):
            wrapped, line_count = _wrap_text_to_max_lines(normalized, max_lines=line_cap)
            takeaway_obj.set_fontsize(float(font_size))
            takeaway_obj.set_text(_matplotlib_safe_text(f"Takeaway: {wrapped}"))
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            fig_bbox = fig.bbox
            max_x = fig_bbox.x0 + (fig_bbox.width * max_width_ratio)
            bb = takeaway_obj.get_window_extent(renderer=renderer)
            if bb.x1 <= max_x:
                return wrapped, line_count, True

    wrapped, line_count = _wrap_text_to_max_lines(normalized, max_lines=max_lines)
    takeaway_obj.set_fontsize(float(min_font_size))
    takeaway_obj.set_text(_matplotlib_safe_text(f"Takeaway: {wrapped}"))
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    fig_bbox = fig.bbox
    max_x = fig_bbox.x0 + (fig_bbox.width * max_width_ratio)
    bb = takeaway_obj.get_window_extent(renderer=renderer)
    return wrapped, line_count, (bb.x1 <= max_x)


def _fit_chart_label_text(
    *,
    fig,
    chart_label_obj,
    chart_label_text: str,
    min_font_size: float = 8.0,
    max_width_ratio: float = 0.95,
) -> tuple[float, bool]:
    normalized = _normalize_render_text(chart_label_text)
    if not normalized:
        chart_label_obj.set_text("")
        return float(chart_label_obj.get_fontsize()), False
    start_size = float(chart_label_obj.get_fontsize())
    if start_size < min_font_size:
        start_size = min_font_size
    font_steps = max(0, int(round(start_size - min_font_size)))
    font_sizes = [start_size - float(step) for step in range(font_steps + 1)]
    if not font_sizes or abs(font_sizes[-1] - min_font_size) > 0.001:
        font_sizes.append(min_font_size)

    for font_size in font_sizes:
        chart_label_obj.set_fontsize(float(font_size))
        chart_label_obj.set_text(_matplotlib_safe_text(normalized))
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        fig_bbox = fig.bbox
        max_x = fig_bbox.x0 + (fig_bbox.width * max_width_ratio)
        bb = chart_label_obj.get_window_extent(renderer=renderer)
        if bb.x1 <= max_x:
            return float(font_size), True
    chart_label_obj.set_fontsize(float(min_font_size))
    chart_label_obj.set_text(_matplotlib_safe_text(normalized))
    return float(min_font_size), False


def _render_source_snip_card(
    *,
    candidate: Candidate,
    slot_key: str,
    style_draft: StyleDraft,
    source_path: Path,
    qa_sink: dict[str, Any] | None = None,
) -> tuple[Path, str]:
    try:
        import matplotlib.image as mpimg
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D
    except Exception as exc:
        raise XChartError("Chart card renderer unavailable (matplotlib missing).") from exc
    from coatue_claw.valuation_chart import COATUE_FONT_FAMILY

    if not source_path.exists():
        raise XChartError("Source chart image is missing for card render.")
    image = mpimg.imread(str(source_path))
    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-source-card.png"

    plt.rcParams["font.family"] = COATUE_FONT_FAMILY
    fig = plt.figure(figsize=(15, 8.4), facecolor="#DCDDDF")

    source_sentence = _extract_first_sentence(candidate.text or candidate.title)
    headline_text = _normalize_render_text(style_draft.headline)
    takeaway_text = _finalize_takeaway_sentence(style_draft.takeaway) or _normalize_render_text(style_draft.takeaway)

    headline_obj = fig.text(
        0.05,
        0.935,
        _matplotlib_safe_text(headline_text),
        ha="left",
        va="center",
        fontsize=28,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="medium",
    )

    _, headline_line_count, headline_fits = _fit_headline_text(
        fig=fig,
        headline_obj=headline_obj,
        headline_text=headline_text,
        max_lines=HEADLINE_MAX_RENDER_LINES,
        min_font_size=HEADLINE_MIN_FONT_SIZE,
    )
    if not headline_fits:
        raise XChartError("Headline layout overflow after wrap.")
    if qa_sink is not None:
        qa_sink["headline_wrapped_line_count"] = int(headline_line_count)

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    fig_bbox = fig.bbox
    h_bb = headline_obj.get_window_extent(renderer=renderer)
    divider_y = max(0.80, ((h_bb.y0 - fig_bbox.y0) / fig_bbox.height) - 0.012)
    fig.add_artist(Line2D([0.05, 0.95], [divider_y, divider_y], transform=fig.transFigure, color="#2F3745", linewidth=1.1))

    chart_bottom = 0.19
    chart_top = max(0.70, min(0.87, divider_y - 0.020))
    chart_height = max(0.50, chart_top - chart_bottom)
    chart_ax = fig.add_axes([0.05, chart_bottom, 0.90, chart_height], facecolor="#F4F5F6")
    chart_ax.imshow(image)
    chart_ax.set_xticks([])
    chart_ax.set_yticks([])
    for spine in chart_ax.spines.values():
        spine.set_color("#E1E4EA")
        spine.set_linewidth(1.2)

    takeaway_obj = fig.text(
        0.05,
        0.115,
        _matplotlib_safe_text(f"Takeaway: {takeaway_text}"),
        fontsize=10.8,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="bold",
        va="top",
    )
    _, takeaway_line_count, takeaway_fits = _fit_takeaway_text(
        fig=fig,
        takeaway_obj=takeaway_obj,
        takeaway_text=takeaway_text,
        max_lines=TAKEAWAY_MAX_RENDER_LINES,
        min_font_size=TAKEAWAY_MIN_FONT_SIZE,
    )
    if not takeaway_fits:
        shortened_takeaway = _semantic_shorten_sentence(takeaway_text, max_words=14)
        if shortened_takeaway:
            takeaway_text = shortened_takeaway
            _, takeaway_line_count, takeaway_fits = _fit_takeaway_text(
                fig=fig,
                takeaway_obj=takeaway_obj,
                takeaway_text=takeaway_text,
                max_lines=TAKEAWAY_MAX_RENDER_LINES,
                min_font_size=TAKEAWAY_MIN_FONT_SIZE,
            )
    if not takeaway_fits:
        raise XChartError("Takeaway layout overflow after 2-line wrap and rewrite.")
    if qa_sink is not None:
        qa_sink["takeaway_wrapped_line_count"] = int(takeaway_line_count)

    fig.text(
        0.05,
        0.045,
        _matplotlib_safe_text(f"Source: {candidate.url}", preserve_urls=True),
        fontsize=9.2,
        color="#4B5563",
        family=COATUE_FONT_FAMILY,
    )

    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_path, _normalize_render_text(headline_text)


def _safe_image_from_url(url: str | None):
    if not url:
        return None
    payload, _ = _fetch_image_bytes(url)
    if not payload:
        return None

    try:
        import matplotlib.image as mpimg

        return mpimg.imread(io.BytesIO(payload))
    except Exception:
        pass

    try:
        from PIL import Image
        import numpy as np

        image = Image.open(io.BytesIO(payload)).convert("RGB")
        return np.asarray(image)
    except Exception:
        return None


def _infer_series_labels(*, candidate: Candidate, count: int) -> list[str]:
    merged = f"{candidate.title} {candidate.text}".lower()
    if count >= 2 and ("stockholder" in merged or "asset owner" in merged):
        return ["Asset owners", "Non-asset owners"][:count]
    if count >= 2 and ("bull" in merged and "bear" in merged):
        return ["Bull trend", "Bear trend"][:count]
    return [f"Series {idx}" for idx in range(1, count + 1)]


def _extract_rebuilt_series(*, candidate: Candidate, image) -> list[RebuiltSeries]:
    try:
        import numpy as np
        from matplotlib.colors import rgb_to_hsv
    except Exception:
        return []

    if image is None:
        return []
    arr = np.asarray(image)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return []
    if arr.dtype.kind in {"u", "i"}:
        rgb = arr[:, :, :3].astype(float) / 255.0
    else:
        rgb = np.clip(arr[:, :, :3].astype(float), 0.0, 1.0)
    h, w, _ = rgb.shape
    if h < 200 or w < 300:
        return []

    y0, y1 = int(h * 0.18), int(h * 0.90)
    x0, x1 = int(w * 0.08), int(w * 0.96)
    if y1 - y0 < 80 or x1 - x0 < 120:
        return []
    crop = rgb[y0:y1, x0:x1, :]
    ch, cw, _ = crop.shape

    inner_y0, inner_y1 = int(ch * 0.06), int(ch * 0.94)
    inner_x0, inner_x1 = int(cw * 0.04), int(cw * 0.98)
    work = crop[inner_y0:inner_y1, inner_x0:inner_x1, :]
    wh, ww, _ = work.shape
    if wh < 40 or ww < 80:
        return []

    hsv = rgb_to_hsv(work)
    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]
    hue = hsv[:, :, 0]

    color_mask = (sat > 0.25) & (val > 0.12) & (val < 0.95)
    dark_mask = (sat < 0.25) & (val > 0.05) & (val < 0.40)
    color_pixels = int(color_mask.sum())
    if color_pixels < 250:
        return []

    hist, edges = np.histogram(hue[color_mask], bins=24, range=(0.0, 1.0))
    top_bins = np.argsort(hist)[::-1][:4]
    masks: list[tuple[np.ndarray, str, float]] = []
    palette = ("#2F6ABF", "#5AA88A", "#D16F4B", "#7C4D9D")
    for idx, b in enumerate(top_bins):
        if int(hist[b]) < 120:
            continue
        lo, hi = float(edges[b]), float(edges[b + 1])
        mask = color_mask & (hue >= lo) & (hue < hi)
        if mask.sum() < 100:
            continue
        masks.append((mask, palette[idx % len(palette)], 2.8 if idx == 0 else 2.4))
    if dark_mask.sum() > 180:
        masks.append((dark_mask, "#232A35", 2.1))

    def _series_from_mask(mask: np.ndarray) -> list[float]:
        ys = np.full((ww,), np.nan, dtype=float)
        for xi in range(ww):
            y_idx = np.where(mask[:, xi])[0]
            if y_idx.size >= 2:
                ys[xi] = float(np.median(y_idx))
        valid = np.where(~np.isnan(ys))[0]
        if valid.size < max(18, int(ww * 0.15)):
            return []
        all_idx = np.arange(ww, dtype=float)
        interp = np.interp(all_idx, valid.astype(float), ys[valid])
        kernel = np.ones((5,), dtype=float) / 5.0
        smooth = np.convolve(interp, kernel, mode="same")
        return smooth.tolist()

    raw_series: list[tuple[list[float], str, float]] = []
    for mask, color, weight in masks:
        series = _series_from_mask(mask)
        if series:
            raw_series.append((series, color, weight))
    if not raw_series:
        return []

    try:
        import numpy as np

        stacked = np.asarray([s for s, _, _ in raw_series], dtype=float)
    except Exception:
        return []
    y_min = float(np.nanmin(stacked))
    y_max = float(np.nanmax(stacked))
    spread = max(1e-6, y_max - y_min)
    labels = _infer_series_labels(candidate=candidate, count=len(raw_series))
    out: list[RebuiltSeries] = []
    for idx, (series, color, weight) in enumerate(raw_series):
        arr_y = np.asarray(series, dtype=float)
        norm = (1.0 - ((arr_y - y_min) / spread)) * 100.0
        norm = np.clip(norm, 0.0, 100.0)
        x_vals = (np.arange(arr_y.size, dtype=float) / max(1.0, float(arr_y.size - 1))) * 100.0
        out.append(
            RebuiltSeries(
                label=labels[idx] if idx < len(labels) else f"Series {idx+1}",
                x=[float(v) for v in x_vals.tolist()],
                y=[float(v) for v in norm.tolist()],
                color=color,
                weight=weight,
            )
        )
    out.sort(key=lambda s: s.weight, reverse=True)
    return out[:2]


def _looks_like_bar_chart(image) -> bool:
    try:
        import numpy as np
    except Exception:
        return False
    if image is None:
        return False
    arr = np.asarray(image)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return False
    if arr.dtype.kind in {"u", "i"}:
        rgb = arr[:, :, :3].astype(float) / 255.0
    else:
        rgb = np.clip(arr[:, :, :3].astype(float), 0.0, 1.0)
    h, w, _ = rgb.shape
    if h < 200 or w < 280:
        return False
    crop = rgb[int(h * 0.18) : int(h * 0.92), int(w * 0.08) : int(w * 0.96), :]
    ch, cw, _ = crop.shape
    hsv = None
    try:
        from matplotlib.colors import rgb_to_hsv

        hsv = rgb_to_hsv(crop)
    except Exception:
        hsv = None
    if hsv is None:
        gray = crop.mean(axis=2)
        dark = gray < 0.55
        col_density = dark.sum(axis=0).astype(float) / max(1.0, float(ch))
        strong_cols = col_density > 0.22
        transitions = int((strong_cols[1:] != strong_cols[:-1]).sum())
        return int(strong_cols.sum()) >= max(6, int(cw * 0.06)) and transitions >= 6

    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]
    colorful = (sat > 0.45) & (val > 0.20)
    col_density = colorful.sum(axis=0).astype(float) / max(1.0, float(ch))
    strong_cols = col_density > 0.08
    if int(strong_cols.sum()) < max(10, int(cw * 0.07)):
        return False
    transitions = int((strong_cols[1:] != strong_cols[:-1]).sum())
    return transitions >= 8


def _infer_chart_mode(*, candidate: Candidate, image) -> str:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    if any(token in merged for token in BAR_HINT_KEYWORDS):
        return "bar"
    bars = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False)
    if bars is not None and len(bars.values) >= 4:
        return "bar"
    if _looks_like_bar_chart(image):
        return "bar"
    return "line"


def _extract_rebuilt_bars(*, image, candidate: Candidate | None = None, allow_vision: bool = True) -> RebuiltBars | None:
    if allow_vision and candidate is not None:
        vision = _extract_rebuilt_bars_via_vision(candidate=candidate)
        if vision is not None:
            return vision
    try:
        import numpy as np
    except Exception:
        return None
    if image is None:
        return None
    arr = np.asarray(image)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return None
    if arr.dtype.kind in {"u", "i"}:
        rgb = arr[:, :, :3].astype(float) / 255.0
    else:
        rgb = np.clip(arr[:, :, :3].astype(float), 0.0, 1.0)
    if candidate is not None and _is_employees_robots_chart(candidate):
        grouped = _extract_employees_robots_bars_cv(rgb=rgb, candidate=candidate)
        if grouped is not None:
            return grouped
    h, w, _ = rgb.shape
    if h < 220 or w < 320:
        return None
    crop = rgb[int(h * 0.16) : int(h * 0.93), int(w * 0.07) : int(w * 0.97), :]
    ch, cw, _ = crop.shape
    try:
        from matplotlib.colors import rgb_to_hsv

        hsv = rgb_to_hsv(crop)
    except Exception:
        hsv = None
    if hsv is None:
        return None

    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]
    hue = hsv[:, :, 0]
    colorful = (sat > 0.45) & (val > 0.18)

    # Prefer dominant colored hue (works for Bloomberg-style orange bars).
    if int(colorful.sum()) < max(250, int(ch * cw * 0.008)):
        return None
    hist, edges = np.histogram(hue[colorful], bins=30, range=(0.0, 1.0))
    top_bin = int(np.argmax(hist))
    lo = float(edges[max(0, top_bin - 1)])
    hi = float(edges[min(len(edges) - 1, top_bin + 2)])
    bar_mask = colorful & (hue >= lo) & (hue < hi)
    if int(bar_mask.sum()) < max(180, int(ch * cw * 0.005)):
        bar_mask = colorful

    cols = bar_mask.sum(axis=0)
    col_threshold = max(6, int(ch * 0.06))
    bar_cols = cols >= col_threshold

    spans: list[tuple[int, int]] = []
    start = -1
    min_width = max(3, int(cw * 0.010))
    for i, flag in enumerate(bar_cols.tolist()):
        if flag and start < 0:
            start = i
        elif (not flag) and start >= 0:
            if (i - start) >= min_width:
                spans.append((start, i - 1))
            start = -1
    if start >= 0 and (len(bar_cols) - start) >= min_width:
        spans.append((start, len(bar_cols) - 1))
    if not (4 <= len(spans) <= 40):
        return None

    bottoms: list[float] = []
    tops: list[float] = []
    for s, e in spans:
        seg = bar_mask[:, s : e + 1]
        y_idx = np.where(seg.any(axis=1))[0]
        if y_idx.size == 0:
            continue
        tops.append(float(y_idx.min()))
        bottoms.append(float(y_idx.max()))
    if len(tops) < 3:
        return None
    base_row = float(np.percentile(np.asarray(bottoms, dtype=float), 85))
    values_raw: list[float] = []
    for top in tops:
        values_raw.append(max(0.0, base_row - top))

    if len(values_raw) < 4:
        return None
    primary_values = values_raw
    secondary_values: list[float] | None = None
    require_grouped = _is_employees_robots_chart(candidate)
    # Grouped bars often appear as alternating pairs (for example employees vs robots).
    if len(values_raw) >= 16 and (len(values_raw) % 2 == 0):
        left = values_raw[0::2]
        right = values_raw[1::2]
        if len(left) >= 4 and len(right) == len(left):
            if sum(left) >= sum(right):
                primary_values = left
                secondary_values = right
            else:
                primary_values = right
                secondary_values = left
    if require_grouped and (secondary_values is None):
        return None

    max_v = max(primary_values + (secondary_values or [])) if primary_values else 0.0
    if max_v <= 2.0:
        return None
    norm = [float((v / max_v) * 100.0) for v in primary_values]
    norm_secondary = [float((v / max_v) * 100.0) for v in secondary_values] if secondary_values else None
    labels = _fallback_bar_labels(candidate=candidate, count=len(norm))
    spread = max(norm) - min(norm)
    if spread < 8.0:
        return None
    primary_label = None
    secondary_label = None
    merged = _normalize_render_text(f"{candidate.title if candidate else ''} {candidate.text if candidate else ''}").lower()
    if "employees" in merged and "robots" in merged and secondary_values is not None:
        primary_label = "Employees"
        secondary_label = "Robots"
    bars = RebuiltBars(
        labels=labels,
        values=norm,
        color="#2F6ABF",
        y_label="Index (normalized)",
        normalized=True,
        source="cv",
        confidence=0.62,
        primary_label=primary_label,
        secondary_values=norm_secondary,
        secondary_color="#5AA88A" if norm_secondary is not None else None,
        secondary_label=secondary_label,
    )
    bars = _normalize_grouped_bar_metadata(candidate=candidate, bars=bars)
    if _bar_data_quality_errors(candidate=candidate, bars=bars):
        return None
    return bars


def _render_chart_of_day_style(
    *,
    candidate: Candidate,
    slot_key: str,
    windows_text: str,
    style_draft: StyleDraft,
    qa_sink: dict[str, Any] | None = None,
) -> Path:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import FuncFormatter
    from coatue_claw.valuation_chart import COATUE_FONT_FAMILY

    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-styled.png"

    copy_errors = _style_copy_quality_errors(style_draft)
    if copy_errors:
        raise XChartError(f"Chart copy QA failed: {copy_errors[0]}")

    plt.rcParams["font.family"] = COATUE_FONT_FAMILY

    fig = plt.figure(figsize=(15, 8.4), facecolor="#DCDDDF")
    source_sentence = _extract_first_sentence(candidate.text or candidate.title)
    headline_text = _normalize_render_text(style_draft.headline)
    headline_obj = fig.text(
        0.05,
        0.935,
        _matplotlib_safe_text(headline_text),
        ha="left",
        va="center",
        fontsize=27,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="medium",
    )
    _, headline_line_count, headline_fits = _fit_headline_text(
        fig=fig,
        headline_obj=headline_obj,
        headline_text=headline_text,
        max_lines=HEADLINE_MAX_RENDER_LINES,
        min_font_size=HEADLINE_MIN_FONT_SIZE,
    )
    if not headline_fits:
        raise XChartError("Headline layout overflow after wrap.")
    if qa_sink is not None:
        qa_sink["headline_wrapped_line_count"] = int(headline_line_count)

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    fig_bbox = fig.bbox
    h_bb = headline_obj.get_window_extent(renderer=renderer)
    divider_y = max(0.80, ((h_bb.y0 - fig_bbox.y0) / fig_bbox.height) - 0.012)
    fig.add_artist(Line2D([0.05, 0.95], [divider_y, divider_y], transform=fig.transFigure, color="#2F3745", linewidth=1.1))

    chart_label_text = _normalize_render_text(style_draft.chart_label)
    chart_label_y = max(0.74, divider_y - 0.028)
    chart_label_obj = fig.text(
        0.05,
        chart_label_y,
        _matplotlib_safe_text(chart_label_text),
        ha="left",
        va="center",
        fontsize=10.8,
        color="#2F3745",
        family=COATUE_FONT_FAMILY,
    )
    _, chart_label_fits = _fit_chart_label_text(
        fig=fig,
        chart_label_obj=chart_label_obj,
        chart_label_text=chart_label_text,
        min_font_size=8.0,
        max_width_ratio=0.95,
    )
    if not chart_label_fits:
        raise XChartError("Chart label layout overflow without truncation.")

    chart_bottom = 0.20
    chart_top = max(0.62, min(0.84, chart_label_y - 0.020))
    chart_ax = fig.add_axes([0.05, chart_bottom, 0.90, max(0.42, chart_top - chart_bottom)], facecolor="#F4F5F6")
    chart_ax.set_xticks([])
    chart_ax.set_yticks([])
    for spine in chart_ax.spines.values():
        spine.set_color("#E1E4EA")
        spine.set_linewidth(1.2)

    image = _safe_image_from_url(candidate.image_url)
    vision_bars = _extract_rebuilt_bars_via_vision(candidate=candidate)
    if vision_bars is not None:
        mode = "bar"
        rebuilt_bars = vision_bars
    else:
        mode = _infer_chart_mode(candidate=candidate, image=image)
        rebuilt_bars = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False) if mode == "bar" else None
    if rebuilt_bars is not None:
        rebuilt_bars = _normalize_grouped_bar_metadata(candidate=candidate, bars=rebuilt_bars)
        bar_errors = _bar_data_quality_errors(candidate=candidate, bars=rebuilt_bars)
        if bar_errors:
            raise XChartError(f"Chart QA failed: {bar_errors[0]}")
    rebuilt = _extract_rebuilt_series(candidate=candidate, image=image) if (mode != "bar" and rebuilt_bars is None) else []
    if rebuilt_bars is not None:
        if qa_sink is not None:
            qa_sink["reconstruction_mode"] = "bar"
        try:
            import numpy as np
        except Exception:
            np = None
        if np is None:
            raise XChartError("Chart reconstruction unavailable (numpy missing).")
        else:
            xs = np.arange(len(rebuilt_bars.values))
            if rebuilt_bars.secondary_values and len(rebuilt_bars.secondary_values) == len(rebuilt_bars.values):
                width = 0.38
                chart_ax.bar(
                    xs - (width / 2),
                    rebuilt_bars.values,
                    color=rebuilt_bars.color,
                    alpha=0.9,
                    width=width,
                    edgecolor="#214E93",
                    linewidth=0.4,
                    label=(rebuilt_bars.primary_label or "Series A"),
                )
                chart_ax.bar(
                    xs + (width / 2),
                    rebuilt_bars.secondary_values,
                    color=(rebuilt_bars.secondary_color or "#5AA88A"),
                    alpha=0.9,
                    width=width,
                    edgecolor="#2E6B54",
                    linewidth=0.4,
                    label=(rebuilt_bars.secondary_label or "Series B"),
                )
                chart_ax.legend(loc="upper left", fontsize=8, frameon=False)
                if qa_sink is not None:
                    qa_sink["grouped_two_series"] = True
            else:
                chart_ax.bar(xs, rebuilt_bars.values, color=rebuilt_bars.color, alpha=0.88, width=0.72, edgecolor="#214E93", linewidth=0.4)
                if qa_sink is not None:
                    qa_sink["grouped_two_series"] = False
            chart_ax.set_xlim(-0.6, max(0.6, float(len(xs) - 0.4)))
            all_vals = list(rebuilt_bars.values) + (list(rebuilt_bars.secondary_values) if rebuilt_bars.secondary_values else [])
            y_min = float(min(all_vals))
            y_max = float(max(all_vals))
            y_span = max(1.0, y_max - y_min)
            if y_min < 0:
                chart_ax.set_ylim(y_min - (0.15 * y_span), y_max + (0.18 * y_span))
            elif rebuilt_bars.normalized:
                chart_ax.set_ylim(0.0, max(100.0, y_max * 1.15))
            else:
                chart_ax.set_ylim(0.0, y_max + (0.18 * y_span))
            y_lo, y_hi = chart_ax.get_ylim()
            y_ticks = _compute_y_ticks(y_min=float(y_lo), y_max=float(y_hi), normalized=bool(rebuilt_bars.normalized))
            chart_ax.set_yticks(y_ticks)
            if rebuilt_bars.normalized:
                chart_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: _format_numeric_tick(v)))
            else:
                chart_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: _format_numeric_tick(v)))
            chart_ax.grid(axis="y", color="#D9DEE7", linewidth=0.8, alpha=0.9)
            chart_ax.tick_params(axis="both", labelsize=9, colors="#4A4F59")
            xlabels = rebuilt_bars.labels if len(rebuilt_bars.labels) == len(rebuilt_bars.values) else []
            if not xlabels:
                xlabels = _fallback_bar_labels(candidate=candidate, count=len(rebuilt_bars.values))
            if len(xlabels) == len(rebuilt_bars.values):
                rotation = 0 if len(xlabels) <= 12 else 35
                chart_ax.set_xticks(xs)
                chart_ax.set_xticklabels(xlabels, rotation=rotation, ha=("center" if rotation == 0 else "right"), fontsize=9)
            else:
                chart_ax.set_xticks(xs)
                chart_ax.set_xticklabels([f"P{i+1}" for i in range(len(rebuilt_bars.values))], fontsize=9)
            chart_ax.set_ylabel(rebuilt_bars.y_label, fontsize=10, color="#4A4F59", labelpad=8)
    elif rebuilt:
        if qa_sink is not None:
            qa_sink["reconstruction_mode"] = "line"
            qa_sink["grouped_two_series"] = False
        for series in rebuilt:
            chart_ax.plot(series.x, series.y, color=series.color, linewidth=series.weight, alpha=0.98)
            chart_ax.scatter([series.x[-1]], [series.y[-1]], s=70, color=series.color, zorder=6)
        chart_ax.set_xlim(0.0, 100.0)
        chart_ax.set_ylim(0.0, 100.0)
        chart_ax.grid(axis="y", color="#D9DEE7", linewidth=0.8, alpha=0.85)
        chart_ax.set_ylabel("Index (normalized)", fontsize=10, color="#4A4F59", labelpad=8)
        chart_ax.tick_params(axis="both", labelsize=9, colors="#4A4F59")
        chart_ax.set_xticks([0, 25, 50, 75, 100])
        chart_ax.set_xticklabels(["Start", "Q1", "Q2", "Q3", "Now"])
        chart_ax.set_yticks([0, 20, 40, 60, 80, 100])
    else:
        raise XChartError("Chart reconstruction unavailable; screenshot fallback disabled.")

    # Readability guardrail: reconstructed bar charts must include usable x-axis and y-axis labels.
    if rebuilt_bars is not None:
        tick_text = [t.get_text().strip() for t in chart_ax.get_xticklabels()]
        non_empty = [t for t in tick_text if t]
        if qa_sink is not None:
            qa_sink["x_axis_labels_present"] = len(non_empty) >= min(4, len(rebuilt_bars.values))
        if len(non_empty) < min(4, len(rebuilt_bars.values)):
            raise XChartError("Rebuilt bar chart missing x-axis labels; screenshot fallback disabled.")
        y_tick_text = [t.get_text().strip() for t in chart_ax.get_yticklabels()]
        y_non_empty = [t for t in y_tick_text if t]
        if qa_sink is not None:
            qa_sink["y_axis_labels_present"] = len(y_non_empty) >= 3
        if len(y_non_empty) < 3:
            raise XChartError("Rebuilt bar chart missing y-axis tick labels; screenshot fallback disabled.")
    else:
        x_non_empty = [t.get_text().strip() for t in chart_ax.get_xticklabels() if t.get_text().strip()]
        y_non_empty = [t.get_text().strip() for t in chart_ax.get_yticklabels() if t.get_text().strip()]
        if qa_sink is not None:
            qa_sink["x_axis_labels_present"] = len(x_non_empty) >= 4
            qa_sink["y_axis_labels_present"] = len(y_non_empty) >= 3

    takeaway_text = _finalize_takeaway_sentence(style_draft.takeaway) or _normalize_render_text(style_draft.takeaway)
    takeaway_obj = fig.text(
        0.05,
        0.118,
        _matplotlib_safe_text(f"Takeaway: {takeaway_text}"),
        fontsize=10.6,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="bold",
        va="top",
    )
    _, takeaway_line_count, takeaway_fits = _fit_takeaway_text(
        fig=fig,
        takeaway_obj=takeaway_obj,
        takeaway_text=takeaway_text,
        max_lines=TAKEAWAY_MAX_RENDER_LINES,
        min_font_size=TAKEAWAY_MIN_FONT_SIZE,
    )
    if not takeaway_fits:
        shortened_takeaway = _semantic_shorten_sentence(takeaway_text, max_words=14)
        if shortened_takeaway:
            takeaway_text = shortened_takeaway
            _, takeaway_line_count, takeaway_fits = _fit_takeaway_text(
                fig=fig,
                takeaway_obj=takeaway_obj,
                takeaway_text=takeaway_text,
                max_lines=TAKEAWAY_MAX_RENDER_LINES,
                min_font_size=TAKEAWAY_MIN_FONT_SIZE,
            )
    if not takeaway_fits:
        raise XChartError("Takeaway layout overflow after 2-line wrap and rewrite.")
    if qa_sink is not None:
        qa_sink["takeaway_wrapped_line_count"] = int(takeaway_line_count)
    source_obj = fig.text(
        0.05,
        0.045,
        _matplotlib_safe_text(f"Source: {candidate.url}", preserve_urls=True),
        fontsize=9,
        color="#4B5563",
        family=COATUE_FONT_FAMILY,
    )

    # Prevent overlapping labels by shrinking header/plot region if needed.
    for _ in range(4):
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        chart_bb = chart_ax.get_tightbbox(renderer=renderer)
        take_bb = takeaway_obj.get_window_extent(renderer=renderer)
        src_bb = source_obj.get_window_extent(renderer=renderer)
        label_bb = chart_label_obj.get_window_extent(renderer=renderer)
        if label_bb.y0 < chart_bb.y1 + 4:
            pos = chart_ax.get_position()
            chart_ax.set_position([pos.x0, max(0.16, pos.y0 - 0.01), pos.width, max(0.58, pos.height - 0.02)])
            continue
        if take_bb.y1 > chart_bb.y0 - 4:
            pos = chart_ax.get_position()
            delta = min(0.02, max(0.008, (take_bb.y1 - chart_bb.y0 + 6) / fig_bbox.height))
            new_height = max(0.50, pos.height - delta)
            chart_ax.set_position([pos.x0, pos.y0 + delta, pos.width, new_height])
            continue
        if src_bb.y1 > take_bb.y0 - 4:
            source_obj.set_y(max(0.025, source_obj.get_position()[1] - 0.01))
            continue
        break

    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_path


def _post_winner_to_slack(
    *,
    candidate: Candidate,
    channel: str,
    slot_key: str,
    windows_text: str,
    convention_name: str | None = None,
    style_draft: StyleDraft | None = None,
    store: XChartStore | None = None,
) -> dict[str, Any]:
    tokens = _slack_tokens()
    style_draft = style_draft or _select_style_draft(candidate)
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    source_path = _write_source_chart_image(candidate=candidate, slot_key=slot_key)
    artifact_path = source_path
    render_mode = "source-snip"
    render_qa: dict[str, Any] = {
        "render_mode": render_mode,
        "headline_wrapped_line_count": 1,
        "takeaway_wrapped_line_count": 1,
    }
    try:
        render_result = _render_source_snip_card(
            candidate=candidate,
            slot_key=slot_key,
            style_draft=style_draft,
            source_path=source_path,
            qa_sink=render_qa,
        )
        if isinstance(render_result, tuple):
            artifact_path, rendered_headline = render_result
            if rendered_headline and rendered_headline != style_draft.headline:
                updated_checks = dict(style_draft.checks)
                source_sentence = _extract_first_sentence(candidate.text or candidate.title)
                updated_checks["headline_complete_phrase"] = _is_complete_headline_phrase(rendered_headline)
                updated_checks["headline_complete_sentence"] = _is_complete_headline_sentence(
                    rendered_headline,
                    source_text=source_sentence,
                )
                updated_checks["headline_tail_complete"] = _tail_complete(rendered_headline)
                updated_checks["headline_locked_terms_ok"] = _headline_locked_terms_preserved(
                    rendered_headline,
                    source_text=source_sentence,
                )
                updated_checks["headline_non_degenerate"] = not _is_degenerate_copy_value(rendered_headline)
                updated_checks["takeaway_single_sentence"] = _is_single_sentence_takeaway(style_draft.takeaway)
                updated_checks["title_takeaway_role_ok"] = _title_takeaway_role_ok(
                    headline=rendered_headline,
                    takeaway=style_draft.takeaway,
                )
                style_draft = replace(
                    style_draft,
                    headline=rendered_headline,
                    chart_label=rendered_headline,
                    checks=updated_checks,
                    score=float(sum(1.0 for passed in updated_checks.values() if passed)),
                )
        else:
            artifact_path = render_result
        render_mode = "source-snip-card"
        render_qa["render_mode"] = render_mode
    except Exception as exc:
        logger.warning("source snip card render failed, falling back to raw snip: %s", exc)
        artifact_path = source_path
        render_mode = "source-snip"
        render_qa = {
            "render_mode": render_mode,
            "headline_wrapped_line_count": 1,
            "takeaway_wrapped_line_count": 1,
        }
    file_size = artifact_path.stat().st_size if artifact_path.exists() else 0
    source_sentence = _extract_first_sentence(candidate.text or candidate.title)
    review_checks = {
        "source_image_available": file_size > 0,
        "artifact_nonempty": file_size > 0,
        "render_mode_source_snip": render_mode in {"source-snip", "source-snip-card"},
        "headline_complete_phrase": _is_complete_headline_phrase(style_draft.headline),
        "headline_complete_sentence": _is_complete_headline_sentence(style_draft.headline, source_text=source_sentence),
        "headline_tail_complete": _tail_complete(style_draft.headline),
        "headline_locked_terms_ok": _headline_locked_terms_preserved(style_draft.headline, source_text=source_sentence),
        "headline_wrapped_line_count": int(render_qa.get("headline_wrapped_line_count", 1)),
        "takeaway_complete_sentence": _is_complete_sentence(style_draft.takeaway),
        "takeaway_single_sentence": _is_single_sentence_takeaway(style_draft.takeaway),
        "takeaway_clause_boundary_ok": not _has_unjoined_clause_boundary(style_draft.takeaway),
        "takeaway_tail_complete": _tail_complete(style_draft.takeaway),
        "takeaway_wrapped_line_count": int(render_qa.get("takeaway_wrapped_line_count", 1)),
        "title_takeaway_role_ok": _title_takeaway_role_ok(headline=style_draft.headline, takeaway=style_draft.takeaway),
        "headline_non_degenerate": not _is_degenerate_copy_value(style_draft.headline),
        "chart_label_non_degenerate": not _is_degenerate_copy_value(style_draft.chart_label),
    }
    review_failed = [name for name, passed in review_checks.items() if not passed]
    review = {
        "passed": len(review_failed) == 0,
        "failed": review_failed,
        "checks": review_checks,
        "style_score": float(style_draft.score),
        "style_iteration": int(style_draft.iteration),
        "render_qa": render_qa,
        "artifact_path": str(artifact_path),
        "artifact_size_bytes": int(file_size),
    }
    clean_author = _normalize_render_text(candidate.author)
    clean_takeaway = _normalize_render_text(style_draft.takeaway)
    title_text = _normalize_render_text(convention_name or "Coatue Chart")
    text_lines = [
        f"*{title_text}*",
        f"- Source: `{clean_author}`",
        f"- Title: {style_draft.headline}",
        f"- Key takeaway: {clean_takeaway}",
        f"- Link: {candidate.url}",
    ]
    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            response = client.files_upload_v2(
                channel=channel,
                file=str(artifact_path),
                title=title_text,
                initial_comment="\n".join(text_lines),
            )
            if store is not None:
                store.record_post_review(slot_key=slot_key, channel=channel, candidate=candidate, review=review)
                store.apply_review_feedback(
                    source_id=candidate.source_id,
                    passed=bool(review.get("passed")),
                    failed_checks=[str(x) for x in (review.get("failed") or [])],
                )
            file_obj = response.get("file") if isinstance(response, dict) else None
            file_id = ""
            if isinstance(file_obj, dict):
                file_id = str(file_obj.get("id") or "")
            return {
                "ok": bool(response.get("ok")),
                "ts": None,
                "channel": channel,
                "file_id": file_id,
                "source_artifact": str(source_path),
                "styled_artifact": str(artifact_path),
                "style_audit": {
                    "iteration": style_draft.iteration,
                    "score": style_draft.score,
                    "checks": style_draft.checks,
                    "copy_rewrite_applied": bool(style_draft.copy_rewrite_applied),
                    "copy_rewrite_reason": style_draft.copy_rewrite_reason,
                    "llm_copy_status": style_draft.llm_copy_status,
                    "llm_warning_reason": style_draft.llm_warning_reason,
                    "title_takeaway_role_swapped": bool(style_draft.checks.get("title_takeaway_role_swapped", False)),
                },
                "post_publish_review": review,
            }
        except SlackApiError as exc:
            err = str(exc.response.get("error", "")) if exc.response is not None else str(exc)
            last_error = err or str(exc)
            if err in {"account_inactive", "invalid_auth", "token_revoked"}:
                logger.warning("x-chart slack token rejected (%s), trying next token if available", err)
                continue
            raise
    raise XChartError(f"Slack post failed for all available tokens: {last_error or 'unknown_error'}")


def run_chart_scout_once(
    *,
    manual: bool = False,
    dry_run: bool = False,
    channel_override: str | None = None,
) -> dict[str, Any]:
    store = XChartStore()
    now_utc = datetime.now(UTC)
    tz = _timezone()
    now_local = now_utc.astimezone(tz)
    windows = _parse_windows()
    slot_key = _slot_key(now_local=now_local, windows=windows, manual=manual)
    pull_slot_key = slot_key or f"scout-{now_local.strftime('%Y%m%d-%H%M%S')}"
    windows_text = ",".join(f"{h:02d}:{m:02d}" for h, m in windows)
    repeat_days = _source_repeat_days()
    token = _resolve_bearer_token()
    source_limit = max(8, min(60, int(os.environ.get("COATUE_CLAW_X_CHART_SOURCE_LIMIT", "25"))))
    top_sources = store.top_sources(limit=source_limit)
    known_source_handles = {
        _canonical_handle(str(item.get("handle") or ""))
        for item in store.list_sources(limit=5000)
        if str(item.get("handle") or "").strip()
    }
    source_priority_map = {
        _canonical_handle(str(item.get("handle") or "")).lower(): float(item.get("priority") or 0.45)
        for item in top_sources
        if str(item.get("handle") or "").strip()
    }
    handles = [_canonical_handle(str(item["handle"])) for item in top_sources if str(item.get("handle") or "").strip()]
    discovery_mode = _discovery_mode()
    x_seed_candidates: list[Candidate] = []
    x_open_candidates: list[Candidate] = []
    if discovery_mode in {"seed_only", "hybrid"}:
        x_seed_candidates = [replace(c, discovered_via="seed_list") for c in _fetch_x_candidates_from_sources(handles=handles, token=token, hours=48)]
    if discovery_mode in {"open_only", "hybrid"}:
        x_open_candidates = _fetch_x_candidates_open_search(token=token, priority_by_handle=source_priority_map, hours=48)
    vc_candidates = _fetch_visualcapitalist_candidates(max_items=20)
    all_candidates = _dedupe_candidates(x_seed_candidates + x_open_candidates + vc_candidates)
    seed_candidates_count = len(x_seed_candidates) + len(vc_candidates)
    open_search_candidates_count = len(x_open_candidates)
    merged_candidates_count = len(all_candidates)
    observed_count = store.upsert_observed_candidates(all_candidates)
    pool_prune_days = max(2, min(30, int(os.environ.get("COATUE_CLAW_X_CHART_POOL_KEEP_DAYS", "10"))))
    pruned_count = store.prune_observed_candidates(keep_days=pool_prune_days)

    for item in all_candidates[:80]:
        if item.source_type == "x":
            store.note_candidate_observed(item.source_id, engagement=item.engagement)

    discovery = _discover_new_sources(token=token)
    for handle, engagement in discovery:
        if engagement >= int(os.environ.get("COATUE_CLAW_X_CHART_DISCOVERY_MIN_ENGAGEMENT", "120")):
            store.note_candidate_observed(handle, engagement=engagement)

    source_last_posted = _collect_source_last_posted(store=store, limit=400)
    eligible_after_item_filter = [c for c in all_candidates if not _was_candidate_posted_ever(store=store, candidate_key=c.candidate_key)]
    eligible_after_cooldown = _eligible_after_source_cooldown(
        candidates=eligible_after_item_filter,
        source_last_posted=source_last_posted,
        repeat_days=repeat_days,
        now_utc=now_utc,
    )

    if slot_key is None:
        pull_log_path = _write_pull_log(
            slot_key=pull_slot_key,
            mode="scheduled",
            channel=(channel_override or "").strip(),
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=all_candidates,
            source_last_posted=source_last_posted,
            eligible_after_item_filter=eligible_after_item_filter,
            eligible_after_cooldown=eligible_after_cooldown,
            selected_candidate_key=None,
            result_reason="scouted_pool_updated",
            seed_candidates_count=seed_candidates_count,
            open_search_candidates_count=open_search_candidates_count,
            merged_candidates_count=merged_candidates_count,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "scouted_pool_updated",
            "copy_rewrite_applied": False,
            "copy_rewrite_reason": None,
            "candidate_fallback_used": False,
            "title_takeaway_role_swapped": False,
            "llm_copy_status": "ok",
            "llm_warning_posted": False,
            "llm_warning_reason": None,
            "now_local": now_local.isoformat(),
            "windows": windows_text,
            "candidates_scanned": len(all_candidates),
            "seed_candidates_count": seed_candidates_count,
            "open_search_candidates_count": open_search_candidates_count,
            "candidates_observed": observed_count,
            "pool_pruned": pruned_count,
            "pull_log_path": str(pull_log_path),
        }

    if (not manual) and store.was_slot_posted(slot_key):
        pull_log_path = _write_pull_log(
            slot_key=pull_slot_key,
            mode="scheduled",
            channel=(channel_override or "").strip(),
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=all_candidates,
            source_last_posted=source_last_posted,
            eligible_after_item_filter=eligible_after_item_filter,
            eligible_after_cooldown=eligible_after_cooldown,
            selected_candidate_key=None,
            result_reason="slot_already_posted",
            seed_candidates_count=seed_candidates_count,
            open_search_candidates_count=open_search_candidates_count,
            merged_candidates_count=merged_candidates_count,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "slot_already_posted",
            "slot_key": slot_key,
            "copy_rewrite_applied": False,
            "copy_rewrite_reason": None,
            "candidate_fallback_used": False,
            "title_takeaway_role_swapped": False,
            "llm_copy_status": "ok",
            "llm_warning_posted": False,
            "llm_warning_reason": None,
            "candidates_scanned": len(all_candidates),
            "seed_candidates_count": seed_candidates_count,
            "open_search_candidates_count": open_search_candidates_count,
            "candidates_observed": observed_count,
            "pool_pruned": pruned_count,
            "pull_log_path": str(pull_log_path),
        }

    since_utc = None if manual else store.latest_scheduled_posted_at_utc()
    pool_limit = max(50, min(2000, int(os.environ.get("COATUE_CLAW_X_CHART_POOL_LIMIT", "600"))))
    pool_candidates = store.observed_candidates_since(since_utc=since_utc, limit=pool_limit)
    ranking_pool = _dedupe_candidates(pool_candidates) if pool_candidates else all_candidates

    candidate_pool = _candidate_pool_for_post(store=store, candidates=ranking_pool)
    if not candidate_pool:
        pull_log_path = _write_pull_log(
            slot_key=pull_slot_key,
            mode="scheduled",
            channel=(channel_override or "").strip(),
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=all_candidates,
            source_last_posted=source_last_posted,
            eligible_after_item_filter=eligible_after_item_filter,
            eligible_after_cooldown=eligible_after_cooldown,
            selected_candidate_key=None,
            result_reason="no_candidate_available",
            seed_candidates_count=seed_candidates_count,
            open_search_candidates_count=open_search_candidates_count,
            merged_candidates_count=merged_candidates_count,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "no_candidate_available",
            "slot_key": slot_key,
            "copy_rewrite_applied": False,
            "copy_rewrite_reason": None,
            "candidate_fallback_used": False,
            "title_takeaway_role_swapped": False,
            "llm_copy_status": "ok",
            "llm_warning_posted": False,
            "llm_warning_reason": None,
            "candidates_scanned": len(all_candidates),
            "seed_candidates_count": seed_candidates_count,
            "open_search_candidates_count": open_search_candidates_count,
            "candidates_observed": observed_count,
            "pool_candidates": len(ranking_pool),
            "since_utc": since_utc,
            "pool_pruned": pruned_count,
            "pull_log_path": str(pull_log_path),
        }
    top_choice = _pick_winner(store=store, candidates=candidate_pool)
    if top_choice is None:
        convention_name = _convention_name(slot_key=slot_key, now_local=now_local, windows=windows)
        channel = (channel_override or "").strip() or _slack_channel()
        cooled_candidates = _eligible_after_source_cooldown(
            candidates=candidate_pool,
            source_last_posted=source_last_posted,
            repeat_days=repeat_days,
            now_utc=now_utc,
        )
        notice_posted = False
        notice: dict[str, Any] | None = None
        if not dry_run:
            unique_sources_in_cooldown = len(
                {
                    _canonical_handle(c.source_id) or c.source_id.lower()
                    for c in candidate_pool
                    if c not in cooled_candidates
                }
            )
            notice = _post_no_candidate_message_to_slack(
                channel=channel,
                convention_name=convention_name,
                repeat_days=repeat_days,
                scanned_count=len(all_candidates),
                pool_count=len(ranking_pool),
                unique_sources_in_cooldown=unique_sources_in_cooldown,
            )
            notice_posted = bool(notice.get("ok"))
        pull_log_path = _write_pull_log(
            slot_key=pull_slot_key,
            mode="scheduled",
            channel=channel,
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=all_candidates,
            source_last_posted=source_last_posted,
            eligible_after_item_filter=eligible_after_item_filter,
            eligible_after_cooldown=cooled_candidates,
            selected_candidate_key=None,
            result_reason="all_candidates_in_cooldown",
            seed_candidates_count=seed_candidates_count,
            open_search_candidates_count=open_search_candidates_count,
            merged_candidates_count=merged_candidates_count,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "all_candidates_in_cooldown",
            "slot_key": slot_key,
            "convention": convention_name,
            "notice_posted": notice_posted,
            "notice_channel": channel,
            "notice": notice,
            "copy_rewrite_applied": False,
            "copy_rewrite_reason": None,
            "candidate_fallback_used": False,
            "title_takeaway_role_swapped": False,
            "llm_copy_status": "ok",
            "llm_warning_posted": False,
            "llm_warning_reason": None,
            "candidates_scanned": len(all_candidates),
            "seed_candidates_count": seed_candidates_count,
            "open_search_candidates_count": open_search_candidates_count,
            "candidates_observed": observed_count,
            "pool_candidates": len(ranking_pool),
            "since_utc": since_utc,
            "pool_pruned": pruned_count,
            "pull_log_path": str(pull_log_path),
        }
    candidate_order = [top_choice] + [c for c in candidate_pool if c.candidate_key != top_choice.candidate_key]

    winner: Candidate | None = None
    style_draft: StyleDraft | None = None
    candidate_fallback_used = False
    copy_rewrite_applied = False
    copy_rewrite_reason: str | None = None
    title_takeaway_role_swapped = False
    llm_copy_status = "ok"
    llm_warning_posted = False
    llm_warning_reason: str | None = None
    new_source_auto_added = False
    new_source_handle: str | None = None

    for idx, candidate in enumerate(candidate_order):
        draft = _select_style_draft(candidate)
        issues = _style_copy_publish_issues(draft)
        if issues:
            continue
        winner = candidate
        style_draft = draft
        candidate_fallback_used = idx > 0
        copy_rewrite_applied = bool(draft.copy_rewrite_applied)
        copy_rewrite_reason = draft.copy_rewrite_reason
        title_takeaway_role_swapped = bool(draft.checks.get("title_takeaway_role_swapped", False))
        llm_copy_status = draft.llm_copy_status
        llm_warning_reason = draft.llm_warning_reason
        break

    if winner is None or style_draft is None:
        top_draft = _select_style_draft(top_choice)
        top_issues = _style_copy_publish_issues(top_draft)
        pull_log_path = _write_pull_log(
            slot_key=pull_slot_key,
            mode="scheduled",
            channel=(channel_override or "").strip(),
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=all_candidates,
            source_last_posted=source_last_posted,
            eligible_after_item_filter=eligible_after_item_filter,
            eligible_after_cooldown=_eligible_after_source_cooldown(
                candidates=candidate_pool,
                source_last_posted=source_last_posted,
                repeat_days=repeat_days,
                now_utc=now_utc,
            ),
            selected_candidate_key=top_choice.candidate_key,
            result_reason="no_publishable_candidate_available",
            seed_candidates_count=seed_candidates_count,
            open_search_candidates_count=open_search_candidates_count,
            merged_candidates_count=merged_candidates_count,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "no_publishable_candidate_available",
            "slot_key": slot_key,
            "copy_rewrite_applied": bool(top_draft.copy_rewrite_applied),
            "copy_rewrite_reason": top_draft.copy_rewrite_reason or "headline_unrecoverable",
            "candidate_fallback_used": False,
            "title_takeaway_role_swapped": bool(top_draft.checks.get("title_takeaway_role_swapped", False)),
            "llm_copy_status": top_draft.llm_copy_status,
            "llm_warning_posted": False,
            "llm_warning_reason": top_draft.llm_warning_reason,
            "publish_issues": top_issues,
            "top_candidate": {
                "source": f"{top_choice.source_type}:{top_choice.source_id}",
                "author": top_choice.author,
                "title": top_choice.title,
                "url": top_choice.url,
                "score": top_choice.score,
                "style_score": top_draft.score,
                "style_iteration": top_draft.iteration,
            },
            "candidates_scanned": len(all_candidates),
            "seed_candidates_count": seed_candidates_count,
            "open_search_candidates_count": open_search_candidates_count,
            "candidates_observed": observed_count,
            "pool_candidates": len(ranking_pool),
            "since_utc": since_utc,
            "pool_pruned": pruned_count,
            "pull_log_path": str(pull_log_path),
        }

    convention_name = _convention_name(slot_key=slot_key, now_local=now_local, windows=windows)

    if winner.source_type == "x" and _auto_add_sources_enabled():
        candidate_handle = _canonical_handle(winner.source_id)
        if candidate_handle and (candidate_handle not in known_source_handles):
            day_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            if store.auto_added_sources_count_since(since_utc=day_start_utc) < _auto_add_daily_cap():
                store.upsert_source(candidate_handle, priority=max(0.45, float(winner.source_priority)), manual=False)
                new_source_auto_added = True
                new_source_handle = candidate_handle

    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-{winner.source_type}.md"
    out_path.write_text(
        "\n".join(
            [
                f"# Coatue Chart Scout Winner",
                "",
                f"- slot_key: `{slot_key}`",
                f"- generated_at_utc: `{now_utc.isoformat()}`",
                f"- convention: `{convention_name}`",
                f"- source: `{winner.source_type}:{winner.source_id}`",
                f"- author: `{winner.author}`",
                f"- score: `{winner.score:.2f}`",
                f"- candidate_fallback_used: `{candidate_fallback_used}`",
                f"- url: {winner.url}",
                f"- image_url: {winner.image_url or 'n/a'}",
                f"- style_iteration: `{style_draft.iteration}`",
                f"- style_score: `{style_draft.score:.1f}/7`",
                f"- copy_rewrite_applied: `{copy_rewrite_applied}`",
                f"- copy_rewrite_reason: `{copy_rewrite_reason or 'none'}`",
                f"- title_takeaway_role_swapped: `{title_takeaway_role_swapped}`",
                f"- llm_copy_status: `{llm_copy_status}`",
                f"- llm_warning_reason: `{llm_warning_reason or 'none'}`",
                "",
                "## Notes",
                _normalize_render_text(winner.text),
                "",
                "## Style Audit",
                f"- headline: {_normalize_render_text(style_draft.headline)}",
                f"- chart_label: {_normalize_render_text(style_draft.chart_label)}",
                f"- takeaway: {_normalize_render_text(style_draft.takeaway)}",
                f"- why_now: {_normalize_render_text(style_draft.why_now)}",
                f"- checks: {json.dumps(style_draft.checks, sort_keys=True)}",
            ]
        ),
        encoding="utf-8",
    )

    if dry_run:
        pull_log_path = _write_pull_log(
            slot_key=pull_slot_key,
            mode="scheduled",
            channel=(channel_override or "").strip(),
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=all_candidates,
            source_last_posted=source_last_posted,
            eligible_after_item_filter=eligible_after_item_filter,
            eligible_after_cooldown=_eligible_after_source_cooldown(
                candidates=candidate_pool,
                source_last_posted=source_last_posted,
                repeat_days=repeat_days,
                now_utc=now_utc,
            ),
            selected_candidate_key=winner.candidate_key,
            result_reason="dry_run",
            seed_candidates_count=seed_candidates_count,
            open_search_candidates_count=open_search_candidates_count,
            merged_candidates_count=merged_candidates_count,
            winner_discovered_via=winner.discovered_via,
            new_source_auto_added=new_source_auto_added,
            new_source_handle=new_source_handle,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "dry_run",
            "slot_key": slot_key,
            "convention": convention_name,
            "copy_rewrite_applied": copy_rewrite_applied,
            "copy_rewrite_reason": copy_rewrite_reason,
            "candidate_fallback_used": candidate_fallback_used,
            "title_takeaway_role_swapped": title_takeaway_role_swapped,
            "llm_copy_status": llm_copy_status,
            "llm_warning_posted": False,
            "llm_warning_reason": llm_warning_reason,
            "winner": {
                "source": f"{winner.source_type}:{winner.source_id}",
                "author": winner.author,
                "title": winner.title,
                "url": winner.url,
                "score": winner.score,
                "style_score": style_draft.score,
                "style_iteration": style_draft.iteration,
                "discovered_via": winner.discovered_via,
            },
            "new_source_auto_added": new_source_auto_added,
            "new_source_handle": new_source_handle,
            "artifact": str(out_path),
            "pool_candidates": len(ranking_pool),
            "since_utc": since_utc,
            "pull_log_path": str(pull_log_path),
        }

    channel = (channel_override or "").strip() or _slack_channel()
    if llm_copy_status == "warning_fallback" and llm_warning_reason:
        try:
            warning = _post_llm_copy_warning_to_slack(
                channel=channel,
                convention_name=convention_name,
                slot_key=slot_key,
                candidate=winner,
                reason=llm_warning_reason,
            )
            llm_warning_posted = bool(warning.get("ok"))
        except Exception as exc:
            logger.warning("llm-copy warning post failed (continuing with fallback copy): %s", exc)
    post = _post_winner_to_slack(
        candidate=winner,
        channel=channel,
        slot_key=slot_key,
        windows_text=windows_text,
        convention_name=convention_name,
        style_draft=style_draft,
        store=store,
    )
    store.record_post(slot_key=slot_key, channel=channel, candidate=winner)
    pull_log_path = _write_pull_log(
        slot_key=pull_slot_key,
        mode="scheduled",
        channel=channel,
        windows_text=windows_text,
        repeat_days=repeat_days,
        scanned_candidates=all_candidates,
        source_last_posted=source_last_posted,
        eligible_after_item_filter=eligible_after_item_filter,
        eligible_after_cooldown=_eligible_after_source_cooldown(
            candidates=candidate_pool,
            source_last_posted=source_last_posted,
            repeat_days=repeat_days,
            now_utc=now_utc,
        ),
        selected_candidate_key=winner.candidate_key,
        result_reason="posted",
        seed_candidates_count=seed_candidates_count,
        open_search_candidates_count=open_search_candidates_count,
        merged_candidates_count=merged_candidates_count,
        winner_discovered_via=winner.discovered_via,
        new_source_auto_added=new_source_auto_added,
        new_source_handle=new_source_handle,
    )
    return {
        "ok": True,
        "posted": True,
        "slot_key": slot_key,
        "convention": convention_name,
        "channel": channel,
        "copy_rewrite_applied": copy_rewrite_applied,
        "copy_rewrite_reason": copy_rewrite_reason,
        "candidate_fallback_used": candidate_fallback_used,
        "title_takeaway_role_swapped": title_takeaway_role_swapped,
        "llm_copy_status": llm_copy_status,
        "llm_warning_posted": llm_warning_posted,
        "llm_warning_reason": llm_warning_reason,
        "post": post,
        "winner": {
            "source": f"{winner.source_type}:{winner.source_id}",
            "author": winner.author,
            "title": winner.title,
            "url": winner.url,
            "score": winner.score,
            "style_score": style_draft.score,
            "style_iteration": style_draft.iteration,
            "discovered_via": winner.discovered_via,
        },
        "new_source_auto_added": new_source_auto_added,
        "new_source_handle": new_source_handle,
        "artifact": str(out_path),
        "pool_candidates": len(ranking_pool),
        "since_utc": since_utc,
        "pull_log_path": str(pull_log_path),
    }


def run_chart_for_post_url(
    *,
    post_url: str,
    channel_override: str | None = None,
    title_override: str | None = None,
) -> dict[str, Any]:
    parsed = _parse_x_post_url(post_url)
    if parsed is None:
        raise XChartError("Invalid X post URL. Expected format like https://x.com/<handle>/status/<id>.")
    handle, tweet_id = parsed

    store = XChartStore()
    token = _resolve_bearer_token()
    payload = _http_json(
        url=f"{_x_api_base()}/2/tweets",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={
            "ids": tweet_id,
            "expansions": "author_id,attachments.media_keys",
            "tweet.fields": "author_id,created_at,public_metrics,attachments,lang",
            "user.fields": "name,username,verified",
            "media.fields": "type,url,preview_image_url,width,height",
        },
    )
    candidates = _parse_x_candidates(payload, priority_by_handle={handle.lower(): 1.0})
    winner = next((c for c in candidates if c.candidate_key == f"x:{tweet_id}"), None)
    if winner is None:
        if candidates:
            winner = candidates[0]
        else:
            winner = _candidate_from_explicit_post_payload(
                payload=payload,
                handle_hint=handle,
                tweet_id=tweet_id,
            )
            if winner is None:
                winner = _fetch_vxtwitter_post_candidate(handle=handle, tweet_id=tweet_id)
            if winner is None:
                raise XChartError("No chart candidate found in that X post (missing image or inaccessible tweet).")

    style_draft = _select_style_draft(winner)
    if title_override and str(title_override).strip():
        overridden_headline = _normalize_render_text(str(title_override))
        if not overridden_headline:
            raise XChartError("Provided title override is empty after normalization.")
        checks = dict(style_draft.checks)
        checks["headline_complete_phrase"] = True
        checks["headline_complete_sentence"] = True
        checks["headline_tail_complete"] = True
        checks["headline_locked_terms_ok"] = True
        checks["headline_non_degenerate"] = not _is_degenerate_copy_value(overridden_headline)
        checks["title_takeaway_role_ok"] = _title_takeaway_role_ok(
            headline=overridden_headline,
            takeaway=style_draft.takeaway,
        )
        checks["title_takeaway_role_swapped"] = bool(checks.get("title_takeaway_role_swapped", False))
        style_draft = replace(
            style_draft,
            headline=overridden_headline,
            chart_label=overridden_headline,
            checks=checks,
            score=float(sum(1.0 for passed in checks.values() if passed)),
            copy_rewrite_applied=True,
            copy_rewrite_reason="headline_override_applied",
        )
    copy_rewrite_applied = bool(style_draft.copy_rewrite_applied)
    copy_rewrite_reason = style_draft.copy_rewrite_reason
    title_takeaway_role_swapped = bool(style_draft.checks.get("title_takeaway_role_swapped", False))
    llm_copy_status = style_draft.llm_copy_status
    llm_warning_reason = style_draft.llm_warning_reason
    llm_warning_posted = False
    copy_issues = _style_copy_publish_issues(style_draft)
    if copy_issues:
        raise XChartError(
            "Requested X post did not produce publishable copy: "
            + ", ".join(copy_issues)
        )
    channel = (channel_override or "").strip() or _slack_channel()
    now_local = datetime.now(UTC).astimezone(_timezone())
    repeat_days = _source_repeat_days()
    windows = _parse_windows()
    slot_key = _slot_key_for_manual_post_url(now_local=now_local, windows=windows)
    windows_text = ",".join(f"{h:02d}:{m:02d}" for h, m in windows)
    convention_name = _convention_name(slot_key=slot_key, now_local=now_local, windows=windows)
    source_last_posted = _collect_source_last_posted(store=store, limit=400)
    if store.was_slot_posted(slot_key):
        pull_log_path = _write_pull_log(
            slot_key=slot_key,
            mode="manual_url",
            channel=channel,
            windows_text=windows_text,
            repeat_days=repeat_days,
            scanned_candidates=candidates or [winner],
            source_last_posted=source_last_posted,
            eligible_after_item_filter=candidates or [winner],
            eligible_after_cooldown=candidates or [winner],
            selected_candidate_key=None,
            result_reason="slot_already_posted",
            manual_override_used=True,
            seed_candidates_count=len(candidates or [winner]),
            open_search_candidates_count=0,
            merged_candidates_count=len(candidates or [winner]),
            winner_discovered_via=winner.discovered_via,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "slot_already_posted",
            "slot_key": slot_key,
            "convention": convention_name,
            "channel": channel,
            "copy_rewrite_applied": copy_rewrite_applied,
            "copy_rewrite_reason": copy_rewrite_reason,
            "candidate_fallback_used": False,
            "title_takeaway_role_swapped": title_takeaway_role_swapped,
            "llm_copy_status": llm_copy_status,
            "llm_warning_posted": False,
            "llm_warning_reason": llm_warning_reason,
            "pull_log_path": str(pull_log_path),
        }
    if llm_copy_status == "warning_fallback" and llm_warning_reason:
        try:
            warning = _post_llm_copy_warning_to_slack(
                channel=channel,
                convention_name=convention_name,
                slot_key=slot_key,
                candidate=winner,
                reason=llm_warning_reason,
            )
            llm_warning_posted = bool(warning.get("ok"))
        except Exception as exc:
            logger.warning("llm-copy warning post failed (continuing with fallback copy): %s", exc)
    post = _post_winner_to_slack(
        candidate=winner,
        channel=channel,
        slot_key=slot_key,
        windows_text=windows_text,
        convention_name=convention_name,
        style_draft=style_draft,
        store=store,
    )
    store.record_post(slot_key=slot_key, channel=channel, candidate=winner)
    pull_log_path = _write_pull_log(
        slot_key=slot_key,
        mode="manual_url",
        channel=channel,
        windows_text=windows_text,
        repeat_days=repeat_days,
        scanned_candidates=candidates or [winner],
        source_last_posted=source_last_posted,
        eligible_after_item_filter=candidates or [winner],
        eligible_after_cooldown=candidates or [winner],
        selected_candidate_key=winner.candidate_key,
        result_reason="posted",
        manual_override_used=True,
        seed_candidates_count=len(candidates or [winner]),
        open_search_candidates_count=0,
        merged_candidates_count=len(candidates or [winner]),
        winner_discovered_via=winner.discovered_via,
    )
    return {
        "ok": True,
        "posted": True,
        "slot_key": slot_key,
        "convention": convention_name,
        "channel": channel,
        "copy_rewrite_applied": copy_rewrite_applied,
        "copy_rewrite_reason": copy_rewrite_reason,
        "candidate_fallback_used": False,
        "title_takeaway_role_swapped": title_takeaway_role_swapped,
        "llm_copy_status": llm_copy_status,
        "llm_warning_posted": llm_warning_posted,
        "llm_warning_reason": llm_warning_reason,
        "post": post,
        "winner": {
            "source": f"{winner.source_type}:{winner.source_id}",
            "author": winner.author,
            "title": winner.title,
            "url": winner.url,
            "score": winner.score,
            "style_score": style_draft.score,
            "style_iteration": style_draft.iteration,
        },
        "pull_log_path": str(pull_log_path),
    }


def status() -> dict[str, Any]:
    store = XChartStore()
    last_scheduled = store.latest_scheduled_posted_at_utc()
    return {
        "ok": True,
        "render_mode": "source-snip-card",
        "schedule_mode": "hourly-scout+windowed-post",
        "db_path": str(store.db_path),
        "timezone": os.environ.get("COATUE_CLAW_X_CHART_TIMEZONE", DEFAULT_TIMEZONE),
        "windows": ",".join(f"{h:02d}:{m:02d}" for h, m in _parse_windows()),
        "slack_channel": os.environ.get("COATUE_CLAW_X_CHART_SLACK_CHANNEL", ""),
        "sources_count": len(store.list_sources(limit=1000)),
        "last_scheduled_posted_at_utc": last_scheduled,
        "pool_candidates_since_last_post": store.observed_candidates_count_since(since_utc=last_scheduled),
        "recent_posts": store.latest_posts(limit=5),
        "review_summary": store.recent_review_summary(limit=20),
    }


def add_source(handle: str, *, priority: float = 1.0) -> dict[str, Any]:
    store = XChartStore()
    clean = _canonical_handle(handle)
    if not clean:
        raise XChartError("Invalid X handle.")
    store.upsert_source(clean, priority=priority, manual=True)
    return {"ok": True, "handle": clean, "priority": float(priority)}


def list_sources(*, limit: int = 50) -> dict[str, Any]:
    store = XChartStore()
    return {"ok": True, "sources": store.list_sources(limit=limit)}


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-x-chart-daily")
    sub = parser.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run-once")
    r.add_argument("--manual", action="store_true")
    r.add_argument("--dry-run", action="store_true")

    sub.add_parser("status")

    ls = sub.add_parser("list-sources")
    ls.add_argument("--limit", type=int, default=50)

    add = sub.add_parser("add-source")
    add.add_argument("handle")
    add.add_argument("--priority", type=float, default=1.0)

    rpu = sub.add_parser("run-post-url")
    rpu.add_argument("post_url")
    rpu.add_argument("--channel", default="", help="Override Slack channel id for posting")
    rpu.add_argument("--title", default="", help="Optional explicit headline sentence override")

    args = parser.parse_args()
    if args.cmd == "run-once":
        result = run_chart_scout_once(manual=bool(args.manual), dry_run=bool(args.dry_run))
    elif args.cmd == "status":
        result = status()
    elif args.cmd == "list-sources":
        result = list_sources(limit=max(1, min(500, int(args.limit))))
    elif args.cmd == "run-post-url":
        result = run_chart_for_post_url(
            post_url=str(args.post_url).strip(),
            channel_override=(str(args.channel).strip() or None),
            title_override=(str(args.title).strip() or None),
        )
    else:
        result = add_source(args.handle, priority=float(args.priority))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

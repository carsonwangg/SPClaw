from __future__ import annotations

import argparse
from dataclasses import dataclass
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
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


load_dotenv("/opt/coatue-claw/.env.prod")

logger = logging.getLogger(__name__)

DEFAULT_WINDOWS = "09:00,12:00,18:00"
DEFAULT_TIMEZONE = "America/Los_Angeles"


DEFAULT_PRIORITY_SOURCES: list[tuple[str, float]] = [
    ("fiscal_AI", 1.6),
    ("cloudedjudgment", 1.5),
    ("KobeissiLetter", 1.3),
    ("charliebilello", 1.25),
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


@dataclass(frozen=True)
class StyleDraft:
    headline: str
    takeaway: str
    why_now: str
    iteration: int
    checks: dict[str, bool]
    score: float


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
        out = [(9, 0), (12, 0), (18, 0)]
    out.sort()
    return out


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


def _extract_first_sentence(text: str) -> str:
    normalized = _normalize_render_text(text)
    if not normalized:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", normalized)
    if not parts:
        return normalized
    first = parts[0].strip()
    return first or normalized


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
    full = " ".join(words).strip()
    clipped = " ".join(words[:max_words]).strip()
    if len(clipped) > max_chars:
        clipped = clipped[:max_chars].rstrip()
    truncated = len(words) > max_words or len(full) > len(clipped)
    if truncated:
        if len(clipped) > max(0, max_chars - 3):
            clipped = clipped[: max(0, max_chars - 3)].rstrip()
        return clipped.rstrip(".,;:") + "..."
    return clipped


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

    def was_slot_posted(self, slot_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM posted_slots WHERE slot_key = ? LIMIT 1", (slot_key,)).fetchone()
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
    snippet = _normalize_render_text(text)
    if len(snippet) > 95:
        snippet = snippet[:94].rstrip() + "..."
    return f"@{handle}: {snippet}".strip()


def _keyword_score(text: str) -> float:
    lower = (text or "").lower()
    score = 0.0
    for kw in THEME_KEYWORDS:
        if kw.lower() in lower:
            score += 2.0
    return min(14.0, score)


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
    return priority_component + engagement_component + keyword_component + freshness_component + image_component


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
    for hour, minute in windows:
        if now_local.hour == hour and abs(now_local.minute - minute) <= 20:
            return f"{now_local.strftime('%Y-%m-%d')}-{hour:02d}:{minute:02d}"
    return None


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


def _pick_winner(*, store: XChartStore, candidates: list[Candidate]) -> Candidate | None:
    for item in candidates:
        if store.was_item_posted_recently(item.candidate_key, days=30):
            continue
        return item
    return None


def _build_takeaways(candidate: Candidate) -> list[str]:
    text = _normalize_render_text(candidate.text)
    title = _normalize_render_text(candidate.title)
    excerpt = _truncate_words(text or title, max_words=13, max_chars=96)
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
    body_text = _normalize_render_text(candidate.text)
    first_sentence = _extract_first_sentence(body_text or title_text)
    title_core = re.sub(r"^@\w+:\s*", "", title_text).strip()
    first_core = re.sub(r"^@\w+:\s*", "", first_sentence).strip()

    if iteration == 1:
        headline = _truncate_words(first_core or title_core or title_text, max_words=8, max_chars=58)
        takeaway = _truncate_words(body_text or title_core or title_text, max_words=13, max_chars=96)
        why_now = "Clear US trend; chart carries the story."
    elif iteration == 2:
        headline = _truncate_words(title_core or first_core or title_text, max_words=8, max_chars=54)
        takeaway = _truncate_words(first_core or body_text, max_words=11, max_chars=84)
        why_now = "Fast read in a feed."
    else:
        anchor = _truncate_words(first_core or title_core or title_text, max_words=7, max_chars=50)
        headline = anchor or "US Trend Snapshot"
        takeaway = _truncate_words(body_text or title_core or title_text, max_words=9, max_chars=74)
        why_now = "Simple trend read."

    combined = " ".join([headline, takeaway, why_now]).strip()
    checks = {
        "us_relevant": _is_us_relevant_post(f"{candidate.title} {candidate.text}"),
        "headline_short": bool(headline) and len(headline) <= 72,
        "takeaway_short": bool(takeaway) and len(takeaway) <= 96,
        "trend_explicit": _contains_trend_signal(f"{candidate.title} {candidate.text}"),
        "plain_language": not any(term in combined.lower() for term in SLIDE_JARGON_KEYWORDS),
        "clean_characters": "\ufffd" not in combined and "??" not in combined and "  " not in combined,
        "graph_first_copy": len(combined.split()) <= 30,
    }
    score = float(sum(1.0 for passed in checks.values() if passed))
    return StyleDraft(
        headline=headline or "US Trend Snapshot",
        takeaway=takeaway or "New US-facing data point with clear directional movement.",
        why_now=why_now,
        iteration=iteration,
        checks=checks,
        score=score,
    )


def _select_style_draft(candidate: Candidate, *, max_iterations: int = 3) -> StyleDraft:
    best = _build_style_draft(candidate, iteration=1)
    target_score = 6.0
    if best.score >= target_score and best.checks.get("us_relevant", False):
        return best
    for iteration in range(2, max_iterations + 1):
        draft = _build_style_draft(candidate, iteration=iteration)
        if draft.score > best.score:
            best = draft
        if draft.score >= target_score and draft.checks.get("us_relevant", False):
            return draft
    return best


def _safe_image_from_url(url: str | None):
    if not url:
        return None
    req = Request(url, headers={"User-Agent": "coatue-claw/1.0"}, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            payload = resp.read()
    except Exception:
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


def _render_chart_of_day_style(
    *,
    candidate: Candidate,
    slot_key: str,
    windows_text: str,
    style_draft: StyleDraft,
) -> Path:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.patches import Rectangle
    from coatue_claw.valuation_chart import COATUE_FONT_FAMILY

    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-styled.png"

    generated_local = datetime.now(_timezone())
    generated_line = generated_local.strftime("Generated %b %-d, %Y at %-I:%M %p %Z")
    plt.rcParams["font.family"] = COATUE_FONT_FAMILY

    fig = plt.figure(figsize=(15, 8.4), facecolor="#DCDDDF")
    fig.text(
        0.05,
        0.935,
        _normalize_render_text(style_draft.headline),
        ha="left",
        va="center",
        fontsize=27,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="medium",
    )
    fig.text(0.05, 0.902, generated_line, ha="left", va="center", fontsize=9.8, color="#4A4F59")
    fig.add_artist(Line2D([0.05, 0.95], [0.886, 0.886], transform=fig.transFigure, color="#2F3745", linewidth=1.1))

    chart_ax = fig.add_axes([0.05, 0.14, 0.90, 0.72], facecolor="#F4F5F6")
    chart_ax.set_xticks([])
    chart_ax.set_yticks([])
    for spine in chart_ax.spines.values():
        spine.set_color("#E1E4EA")
        spine.set_linewidth(1.2)

    image = _safe_image_from_url(candidate.image_url)
    if image is not None:
        chart_ax.imshow(image)
        chart_ax.set_aspect("auto")
    else:
        chart_ax.text(0.5, 0.5, "Chart image unavailable", ha="center", va="center", fontsize=16, color="#6B7280", transform=chart_ax.transAxes)
        chart_ax.add_patch(Rectangle((0.05, 0.1), 0.9, 0.8, fill=False, linewidth=1.2, linestyle=(0, (4, 3)), edgecolor="#9CA3AF", transform=chart_ax.transAxes))

    chart_ax.text(
        0.012,
        0.985,
        _normalize_render_text(candidate.author),
        ha="left",
        va="top",
        fontsize=10.5,
        color="#2A7FBE",
        family=COATUE_FONT_FAMILY,
        weight="bold",
        transform=chart_ax.transAxes,
        bbox={"boxstyle": "round,pad=0.18", "facecolor": "#EEF4FB", "edgecolor": "#D0E1F3"},
    )

    fig.text(0.05, 0.082, f"Takeaway: {_normalize_render_text(style_draft.takeaway)}", fontsize=11, color="#1F2430", family=COATUE_FONT_FAMILY, weight="bold")
    fig.text(0.05, 0.045, f"Source: {candidate.url}", fontsize=9, color="#4B5563", family=COATUE_FONT_FAMILY)
    fig.text(0.95, 0.045, f"Score {candidate.score:.1f}", fontsize=9, color="#4B5563", ha="right", family=COATUE_FONT_FAMILY)

    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_path


def _post_winner_to_slack(
    *,
    candidate: Candidate,
    channel: str,
    slot_key: str,
    windows_text: str,
    style_draft: StyleDraft | None = None,
) -> dict[str, Any]:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    tokens = _slack_tokens()
    style_draft = style_draft or _select_style_draft(candidate)
    takeaways = _build_takeaways(candidate)
    styled_path = _render_chart_of_day_style(
        candidate=candidate,
        slot_key=slot_key,
        windows_text=windows_text,
        style_draft=style_draft,
    )
    clean_author = _normalize_render_text(candidate.author)
    clean_takeaway = _normalize_render_text(takeaways[0])
    style_pass = style_draft.score >= 6.0 and style_draft.checks.get("us_relevant", False)
    text_lines = [
        "*Coatue Chart of the Day*",
        f"- Slot: `{slot_key}` ({windows_text})",
        f"- Source: `{clean_author}`",
        f"- Score: `{candidate.score:.1f}`",
        f"- Style fit: `{'pass' if style_pass else 'iterate'} {int(style_draft.score)}/7`",
        f"- Trend: {style_draft.headline}",
        f"- Takeaway: {clean_takeaway}",
        f"- Link: {candidate.url}",
    ]
    blocks: list[dict[str, Any]] = [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(text_lines)}}]
    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            response = client.chat_postMessage(
                channel=channel,
                text="\n".join(text_lines),
                blocks=blocks,
                unfurl_links=False,
                unfurl_media=False,
            )
            ts = str(response.get("ts") or "")
            if ts:
                try:
                    client.files_upload_v2(
                        channel=channel,
                        thread_ts=ts,
                        file=str(styled_path),
                        title="Coatue Chart of the Day",
                    )
                except Exception:
                    logger.exception("Failed to upload styled chart artifact to Slack thread")
            return {
                "ok": bool(response.get("ok")),
                "ts": response.get("ts"),
                "channel": response.get("channel"),
                "styled_artifact": str(styled_path),
                "style_audit": {
                    "iteration": style_draft.iteration,
                    "score": style_draft.score,
                    "checks": style_draft.checks,
                },
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
    windows_text = ",".join(f"{h:02d}:{m:02d}" for h, m in windows)
    if slot_key is None:
        return {
            "ok": True,
            "posted": False,
            "reason": "outside_scheduled_window",
            "now_local": now_local.isoformat(),
            "windows": windows_text,
        }
    if (not manual) and store.was_slot_posted(slot_key):
        return {"ok": True, "posted": False, "reason": "slot_already_posted", "slot_key": slot_key}

    token = _resolve_bearer_token()
    source_limit = max(8, min(60, int(os.environ.get("COATUE_CLAW_X_CHART_SOURCE_LIMIT", "25"))))
    top_sources = store.top_sources(limit=source_limit)
    handles = [_canonical_handle(str(item["handle"])) for item in top_sources if str(item.get("handle") or "").strip()]
    x_candidates = _fetch_x_candidates_from_sources(handles=handles, token=token, hours=48)
    vc_candidates = _fetch_visualcapitalist_candidates(max_items=20)
    all_candidates = _dedupe_candidates(x_candidates + vc_candidates)

    for item in all_candidates[:80]:
        if item.source_type == "x":
            store.note_candidate_observed(item.source_id, engagement=item.engagement)

    discovery = _discover_new_sources(token=token)
    for handle, engagement in discovery:
        if engagement >= int(os.environ.get("COATUE_CLAW_X_CHART_DISCOVERY_MIN_ENGAGEMENT", "120")):
            store.note_candidate_observed(handle, engagement=engagement)

    winner = _pick_winner(store=store, candidates=all_candidates)
    if winner is None:
        return {
            "ok": True,
            "posted": False,
            "reason": "no_candidate_available",
            "slot_key": slot_key,
            "candidates_scanned": len(all_candidates),
        }
    style_draft = _select_style_draft(winner)

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
                f"- source: `{winner.source_type}:{winner.source_id}`",
                f"- author: `{winner.author}`",
                f"- score: `{winner.score:.2f}`",
                f"- url: {winner.url}",
                f"- image_url: {winner.image_url or 'n/a'}",
                f"- style_iteration: `{style_draft.iteration}`",
                f"- style_score: `{style_draft.score:.1f}/7`",
                "",
                "## Notes",
                _normalize_render_text(winner.text),
                "",
                "## Style Audit",
                f"- headline: {_normalize_render_text(style_draft.headline)}",
                f"- takeaway: {_normalize_render_text(style_draft.takeaway)}",
                f"- why_now: {_normalize_render_text(style_draft.why_now)}",
                f"- checks: {json.dumps(style_draft.checks, sort_keys=True)}",
            ]
        ),
        encoding="utf-8",
    )

    if dry_run:
        return {
            "ok": True,
            "posted": False,
            "reason": "dry_run",
            "slot_key": slot_key,
            "winner": {
                "source": f"{winner.source_type}:{winner.source_id}",
                "author": winner.author,
                "title": winner.title,
                "url": winner.url,
                "score": winner.score,
                "style_score": style_draft.score,
                "style_iteration": style_draft.iteration,
            },
            "artifact": str(out_path),
        }

    channel = (channel_override or "").strip() or _slack_channel()
    post = _post_winner_to_slack(
        candidate=winner,
        channel=channel,
        slot_key=slot_key,
        windows_text=windows_text,
        style_draft=style_draft,
    )
    store.record_post(slot_key=slot_key, channel=channel, candidate=winner)
    return {
        "ok": True,
        "posted": True,
        "slot_key": slot_key,
        "channel": channel,
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
        "artifact": str(out_path),
    }


def status() -> dict[str, Any]:
    store = XChartStore()
    return {
        "ok": True,
        "db_path": str(store.db_path),
        "timezone": os.environ.get("COATUE_CLAW_X_CHART_TIMEZONE", DEFAULT_TIMEZONE),
        "windows": ",".join(f"{h:02d}:{m:02d}" for h, m in _parse_windows()),
        "slack_channel": os.environ.get("COATUE_CLAW_X_CHART_SLACK_CHANNEL", ""),
        "sources_count": len(store.list_sources(limit=1000)),
        "recent_posts": store.latest_posts(limit=5),
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

    args = parser.parse_args()
    if args.cmd == "run-once":
        result = run_chart_scout_once(manual=bool(args.manual), dry_run=bool(args.dry_run))
    elif args.cmd == "status":
        result = status()
    elif args.cmd == "list-sources":
        result = list_sources(limit=max(1, min(500, int(args.limit))))
    else:
        result = add_source(args.handle, priority=float(args.priority))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

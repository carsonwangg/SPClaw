from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]

try:
    from slack_sdk import WebClient
except Exception:  # pragma: no cover - optional dependency
    WebClient = None  # type: ignore[assignment]


load_dotenv("/opt/spclaw/.env.prod")

logger = logging.getLogger(__name__)

DEFAULT_CHANNEL = "general"
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_MODEL = "gpt-5.2-chat-latest"
DEFAULT_TOP_N = 5
DEFAULT_MAX_RESULTS_PER_QUERY = 25
DEFAULT_MAX_QUERIES = 8

DEFAULT_SOURCES = [
    "openai",
    "AnthropicAI",
    "GoogleDeepMind",
    "GoogleAI",
    "github",
    "vercel",
    "CloudflareDev",
    "Docker",
    "kubernetesio",
    "figma",
    "cursor_ai",
    "Replit",
    "huggingface",
    "Firebase",
    "supabase",
    "stripe",
]

DEFAULT_KEYWORDS = [
    '"developer tools" launch',
    '"API" launch developer',
    '"open source" release',
    '"AI coding" launch',
    '"new model" developers',
    '"framework" release',
]


class DevBuzzError(RuntimeError):
    pass


@dataclass(frozen=True)
class XPost:
    tweet_id: str
    text: str
    created_at: str | None
    author_username: str | None
    author_name: str | None
    metrics: dict[str, int]
    expanded_urls: tuple[str, ...] = ()

    @property
    def url(self) -> str:
        if self.author_username:
            return f"https://x.com/{self.author_username}/status/{self.tweet_id}"
        return f"https://x.com/i/web/status/{self.tweet_id}"

    @property
    def engagement(self) -> int:
        return sum(int(self.metrics.get(k, 0)) for k in ("like_count", "retweet_count", "reply_count", "quote_count"))


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _data_root() -> Path:
    return Path(os.environ.get("SPCLAW_DATA_ROOT", "/opt/spclaw-data")).expanduser().resolve()


def _db_path() -> Path:
    return Path(os.environ.get("SPCLAW_DEV_BUZZ_DB_PATH", str(_data_root() / "db/dev_buzz.sqlite"))).expanduser().resolve()


def _artifact_dir() -> Path:
    return Path(
        os.environ.get("SPCLAW_DEV_BUZZ_ARTIFACT_DIR", str(_data_root() / "artifacts/dev-buzz"))
    ).expanduser().resolve()


def _timezone() -> ZoneInfo:
    name = (os.environ.get("SPCLAW_DEV_BUZZ_TIMEZONE", DEFAULT_TIMEZONE) or DEFAULT_TIMEZONE).strip()
    try:
        return ZoneInfo(name)
    except Exception as exc:
        raise DevBuzzError(f"Invalid timezone: {name}") from exc


def _slack_channel() -> str:
    return (os.environ.get("SPCLAW_DEV_BUZZ_SLACK_CHANNEL", DEFAULT_CHANNEL) or DEFAULT_CHANNEL).strip()


def _model() -> str:
    return (os.environ.get("SPCLAW_DEV_BUZZ_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip()


def _max_results_per_query() -> int:
    raw = (os.environ.get("SPCLAW_DEV_BUZZ_MAX_RESULTS_PER_QUERY", str(DEFAULT_MAX_RESULTS_PER_QUERY)) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_MAX_RESULTS_PER_QUERY
    return max(10, min(100, value))


def _max_queries() -> int:
    raw = (os.environ.get("SPCLAW_DEV_BUZZ_MAX_QUERIES", str(DEFAULT_MAX_QUERIES)) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_MAX_QUERIES
    return max(1, min(24, value))


def _top_n() -> int:
    raw = (os.environ.get("SPCLAW_DEV_BUZZ_PUBLISH_TOP_N", str(DEFAULT_TOP_N)) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_TOP_N
    return max(1, min(10, value))


def _x_api_base() -> str:
    return (os.environ.get("SPCLAW_X_API_BASE", "https://api.x.com").strip() or "https://api.x.com").rstrip("/")


def _resolve_bearer_token() -> str:
    for key in ("SPCLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "SPCLAW_TWITTER_BEARER_TOKEN"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    raise DevBuzzError("X bearer token missing. Set SPCLAW_X_BEARER_TOKEN in .env.prod.")


def _slack_tokens() -> list[str]:
    tokens: list[str] = []
    env_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if env_token:
        tokens.append(env_token)
    config_path = Path.home() / ".openclaw/openclaw.json"
    if config_path.exists():
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            candidate = str(payload.get("channels", {}).get("slack", {}).get("botToken", "")).strip()
        except Exception:
            candidate = ""
        if candidate:
            tokens.append(candidate)
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    if not out:
        raise DevBuzzError("Slack bot token missing (SLACK_BOT_TOKEN or ~/.openclaw/openclaw.json).")
    return out


def _clean_handle(handle: str) -> str:
    clean = str(handle or "").strip().lstrip("@")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,30}", clean):
        raise DevBuzzError(f"Invalid X handle: {handle}")
    return clean


def _normalize_keyword(keyword: str) -> str:
    clean = re.sub(r"\s+", " ", str(keyword or "")).strip()
    if len(clean) < 2 or len(clean) > 160:
        raise DevBuzzError("Keyword must be 2-160 characters.")
    return clean


def _default_fetch_json(*, url: str, headers: dict[str, str], params: dict[str, str]) -> dict[str, Any]:
    full_url = f"{url}?{urlencode(params)}"
    request = Request(full_url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise DevBuzzError(f"X API request failed ({exc.code}): {detail[:500]}") from exc
    except URLError as exc:
        raise DevBuzzError(f"X API request failed: {exc.reason}") from exc
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise DevBuzzError("Failed to parse X API response JSON.") from exc
    if isinstance(parsed, dict) and parsed.get("errors"):
        raise DevBuzzError(f"X API returned errors: {parsed['errors']}")
    return parsed if isinstance(parsed, dict) else {}


def _parse_posts(payload: dict[str, Any]) -> list[XPost]:
    users_by_id: dict[str, dict[str, Any]] = {}
    includes = payload.get("includes")
    if isinstance(includes, dict) and isinstance(includes.get("users"), list):
        for user in includes["users"]:
            if isinstance(user, dict) and user.get("id"):
                users_by_id[str(user["id"])] = user

    out: list[XPost] = []
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
        metrics: dict[str, int] = {}
        raw_metrics = row.get("public_metrics")
        if isinstance(raw_metrics, dict):
            for key in ("like_count", "retweet_count", "reply_count", "quote_count"):
                try:
                    metrics[key] = int(raw_metrics.get(key, 0) or 0)
                except Exception:
                    metrics[key] = 0
        urls: list[str] = []
        entities = row.get("entities")
        if isinstance(entities, dict) and isinstance(entities.get("urls"), list):
            for item in entities["urls"]:
                if not isinstance(item, dict):
                    continue
                expanded = str(item.get("expanded_url") or item.get("url") or "").strip()
                if expanded:
                    urls.append(expanded)
        user = users_by_id.get(str(row.get("author_id") or "").strip(), {})
        out.append(
            XPost(
                tweet_id=tweet_id,
                text=text,
                created_at=(str(row.get("created_at")) if row.get("created_at") else None),
                author_username=(str(user.get("username")) if user.get("username") else None),
                author_name=(str(user.get("name")) if user.get("name") else None),
                metrics=metrics,
                expanded_urls=tuple(urls),
            )
        )
    return out


def _canonical_url(urls: tuple[str, ...]) -> str | None:
    for url in urls:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if not host or host in {"x.com", "twitter.com", "t.co"}:
            continue
        path = re.sub(r"/+$", "", parsed.path or "")
        return f"{host}{path}".lower()
    return None


def _canonical_key(post: XPost) -> str:
    url_key = _canonical_url(post.expanded_urls)
    if url_key:
        return f"url:{url_key}"
    text = re.sub(r"https?://\S+", " ", post.text.lower())
    text = re.sub(r"[@#][a-z0-9_]+", " ", text)
    text = re.sub(r"\b(launch|launched|announce|announcing|released|shipping|ship|new|today|now)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 120:
        text = text[:120].rsplit(" ", 1)[0]
    return f"text:{text or post.tweet_id}"


def _item_id(canonical_key: str) -> str:
    return "dbz_" + hashlib.sha1(canonical_key.encode("utf-8")).hexdigest()[:12]


def _excerpt(text: str, limit: int = 220) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def _week_key(now_local: datetime) -> str:
    year, week, _ = now_local.isocalendar()
    return f"{year}-W{week:02d}"


class DevBuzzStore:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or _db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_items (
                    item_id TEXT PRIMARY KEY,
                    canonical_key TEXT UNIQUE NOT NULL,
                    headline TEXT,
                    category TEXT,
                    why_matters TEXT,
                    rationale TEXT,
                    confidence TEXT,
                    reject_reason TEXT,
                    status TEXT NOT NULL DEFAULT 'candidate',
                    editorial_rank INTEGER,
                    pinned INTEGER NOT NULL DEFAULT 0,
                    dropped INTEGER NOT NULL DEFAULT 0,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL,
                    top_x_url TEXT,
                    top_text TEXT,
                    top_author TEXT,
                    total_engagement INTEGER NOT NULL DEFAULT 0,
                    observation_count INTEGER NOT NULL DEFAULT 0,
                    unique_authors INTEGER NOT NULL DEFAULT 0,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_observations (
                    tweet_id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    query TEXT NOT NULL,
                    text TEXT NOT NULL,
                    author_username TEXT,
                    author_name TEXT,
                    created_at TEXT,
                    url TEXT NOT NULL,
                    expanded_urls_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    engagement INTEGER NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES dev_buzz_items(item_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_sources (
                    handle TEXT PRIMARY KEY,
                    active INTEGER NOT NULL DEFAULT 1,
                    manual INTEGER NOT NULL DEFAULT 0,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_keywords (
                    keyword TEXT PRIMARY KEY,
                    active INTEGER NOT NULL DEFAULT 1,
                    manual INTEGER NOT NULL DEFAULT 0,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_shortlist_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    created_at_utc TEXT NOT NULL,
                    model TEXT,
                    status TEXT NOT NULL,
                    fallback_reason TEXT,
                    raw_json TEXT NOT NULL,
                    item_count INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_runs (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    started_at_utc TEXT NOT NULL,
                    completed_at_utc TEXT,
                    artifact_path TEXT,
                    posted_channel TEXT,
                    message_ts TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_buzz_published_weeks (
                    week_key TEXT PRIMARY KEY,
                    posted_at_utc TEXT NOT NULL,
                    artifact_path TEXT,
                    channel TEXT,
                    message_ts TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dev_buzz_items_rank ON dev_buzz_items(dropped, pinned, editorial_rank)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dev_buzz_obs_item ON dev_buzz_observations(item_id)")
        self.ensure_defaults()

    def ensure_defaults(self) -> None:
        now = _utc_now().isoformat()
        with self._connect() as conn:
            for handle in DEFAULT_SOURCES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO dev_buzz_sources(handle, active, manual, updated_at_utc)
                    VALUES (?, 1, 0, ?)
                    """,
                    (_clean_handle(handle), now),
                )
            for keyword in DEFAULT_KEYWORDS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO dev_buzz_keywords(keyword, active, manual, updated_at_utc)
                    VALUES (?, 1, 0, ?)
                    """,
                    (_normalize_keyword(keyword), now),
                )

    def active_sources(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT handle FROM dev_buzz_sources WHERE active = 1 ORDER BY manual DESC, handle ASC").fetchall()
        return [str(row["handle"]) for row in rows]

    def active_keywords(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT keyword FROM dev_buzz_keywords WHERE active = 1 ORDER BY manual DESC, keyword ASC").fetchall()
        return [str(row["keyword"]) for row in rows]

    def set_source(self, handle: str, *, active: bool, manual: bool = True) -> dict[str, Any]:
        clean = _clean_handle(handle)
        now = _utc_now().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO dev_buzz_sources(handle, active, manual, updated_at_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(handle) DO UPDATE SET active = excluded.active, manual = max(dev_buzz_sources.manual, excluded.manual), updated_at_utc = excluded.updated_at_utc
                """,
                (clean, 1 if active else 0, 1 if manual else 0, now),
            )
        return {"ok": True, "handle": clean, "active": bool(active)}

    def set_keyword(self, keyword: str, *, active: bool, manual: bool = True) -> dict[str, Any]:
        clean = _normalize_keyword(keyword)
        now = _utc_now().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO dev_buzz_keywords(keyword, active, manual, updated_at_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(keyword) DO UPDATE SET active = excluded.active, manual = max(dev_buzz_keywords.manual, excluded.manual), updated_at_utc = excluded.updated_at_utc
                """,
                (clean, 1 if active else 0, 1 if manual else 0, now),
            )
        return {"ok": True, "keyword": clean, "active": bool(active)}

    def upsert_posts(self, posts: list[XPost], *, query: str, observed_at_utc: str) -> int:
        count = 0
        with self._connect() as conn:
            for post in posts:
                canonical_key = _canonical_key(post)
                item_id = _item_id(canonical_key)
                author = post.author_username or post.author_name or ""
                existing = conn.execute("SELECT item_id FROM dev_buzz_items WHERE item_id = ?", (item_id,)).fetchone()
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO dev_buzz_items(
                            item_id, canonical_key, status, first_seen_utc, last_seen_utc, top_x_url, top_text,
                            top_author, total_engagement, observation_count, unique_authors, updated_at_utc
                        ) VALUES (?, ?, 'candidate', ?, ?, ?, ?, ?, ?, 0, 0, ?)
                        """,
                        (
                            item_id,
                            canonical_key,
                            observed_at_utc,
                            observed_at_utc,
                            post.url,
                            post.text,
                            author,
                            int(post.engagement),
                            observed_at_utc,
                        ),
                    )
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO dev_buzz_observations(
                        tweet_id, item_id, observed_at_utc, query, text, author_username, author_name,
                        created_at, url, expanded_urls_json, metrics_json, engagement
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        post.tweet_id,
                        item_id,
                        observed_at_utc,
                        query,
                        post.text,
                        post.author_username,
                        post.author_name,
                        post.created_at,
                        post.url,
                        json.dumps(list(post.expanded_urls), sort_keys=True),
                        json.dumps(post.metrics, sort_keys=True),
                        int(post.engagement),
                    ),
                )
                if cur.rowcount > 0:
                    count += 1
                stats = conn.execute(
                    """
                    SELECT COUNT(1) AS n, COALESCE(SUM(engagement), 0) AS engagement,
                           COUNT(DISTINCT COALESCE(author_username, author_name, tweet_id)) AS authors
                    FROM dev_buzz_observations
                    WHERE item_id = ?
                    """,
                    (item_id,),
                ).fetchone()
                top = conn.execute(
                    """
                    SELECT text, url, COALESCE(author_username, author_name, '') AS author, engagement
                    FROM dev_buzz_observations
                    WHERE item_id = ?
                    ORDER BY engagement DESC, observed_at_utc DESC
                    LIMIT 1
                    """,
                    (item_id,),
                ).fetchone()
                conn.execute(
                    """
                    UPDATE dev_buzz_items
                    SET last_seen_utc = ?, top_x_url = ?, top_text = ?, top_author = ?,
                        total_engagement = ?, observation_count = ?, unique_authors = ?, updated_at_utc = ?
                    WHERE item_id = ?
                    """,
                    (
                        observed_at_utc,
                        str(top["url"]),
                        str(top["text"]),
                        str(top["author"]),
                        int(stats["engagement"] or 0),
                        int(stats["n"] or 0),
                        int(stats["authors"] or 0),
                        observed_at_utc,
                        item_id,
                    ),
                )
        return count

    def candidates_for_editor(self, *, limit: int = 60) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT item_id, headline, category, why_matters, rationale, confidence, reject_reason,
                       status, editorial_rank, pinned, dropped, first_seen_utc, last_seen_utc,
                       top_x_url, top_text, top_author, total_engagement, observation_count, unique_authors
                FROM dev_buzz_items
                WHERE dropped = 0
                ORDER BY pinned DESC, editorial_rank IS NULL ASC, editorial_rank ASC,
                         total_engagement DESC, unique_authors DESC, last_seen_utc DESC
                LIMIT ?
                """,
                (max(10, min(200, limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def shortlist(self, *, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT item_id, headline, category, why_matters, rationale, confidence, reject_reason,
                       status, editorial_rank, pinned, dropped, first_seen_utc, last_seen_utc,
                       top_x_url, top_text, top_author, total_engagement, observation_count, unique_authors
                FROM dev_buzz_items
                WHERE dropped = 0 AND (pinned = 1 OR editorial_rank IS NOT NULL OR status = 'shortlisted')
                ORDER BY pinned DESC, editorial_rank IS NULL ASC, editorial_rank ASC,
                         total_engagement DESC, last_seen_utc DESC
                LIMIT ?
                """,
                (max(1, min(100, limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def item(self, item_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT item_id, headline, category, why_matters, rationale, confidence, reject_reason,
                       status, editorial_rank, pinned, dropped, first_seen_utc, last_seen_utc,
                       top_x_url, top_text, top_author, total_engagement, observation_count, unique_authors
                FROM dev_buzz_items
                WHERE item_id = ?
                """,
                (item_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def pin_item(self, item_id: str, *, pinned: bool = True) -> dict[str, Any]:
        now = _utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE dev_buzz_items SET pinned = ?, dropped = 0, status = 'shortlisted', updated_at_utc = ? WHERE item_id = ?",
                (1 if pinned else 0, now, item_id),
            )
        if cur.rowcount < 1:
            raise DevBuzzError(f"Unknown DevBuzz item: {item_id}")
        return {"ok": True, "item_id": item_id, "pinned": bool(pinned)}

    def drop_item(self, item_id: str) -> dict[str, Any]:
        now = _utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE dev_buzz_items SET dropped = 1, pinned = 0, status = 'dropped', updated_at_utc = ? WHERE item_id = ?",
                (now, item_id),
            )
        if cur.rowcount < 1:
            raise DevBuzzError(f"Unknown DevBuzz item: {item_id}")
        return {"ok": True, "item_id": item_id, "dropped": True}

    def apply_editorial(self, *, payload: dict[str, Any], model: str, fallback_reason: str | None) -> str:
        now = _utc_now().isoformat()
        snapshot_id = "dbzs_" + hashlib.sha1(f"{now}:{json.dumps(payload, sort_keys=True)}".encode("utf-8")).hexdigest()[:12]
        shortlist = payload.get("shortlist")
        rows = shortlist if isinstance(shortlist, list) else []
        with self._connect() as conn:
            conn.execute(
                "UPDATE dev_buzz_items SET editorial_rank = NULL WHERE pinned = 0 AND dropped = 0"
            )
            for idx, item in enumerate(rows, start=1):
                if not isinstance(item, dict):
                    continue
                item_id = str(item.get("item_id") or "").strip()
                if not item_id:
                    continue
                friday_worthy = bool(item.get("friday_worthy", True))
                status = "shortlisted" if friday_worthy else "rejected"
                rank_raw = item.get("rank", idx)
                try:
                    rank = int(rank_raw)
                except Exception:
                    rank = idx
                conn.execute(
                    """
                    UPDATE dev_buzz_items
                    SET headline = ?, category = ?, why_matters = ?, rationale = ?, confidence = ?,
                        reject_reason = ?, status = ?, editorial_rank = ?, updated_at_utc = ?
                    WHERE item_id = ? AND dropped = 0
                    """,
                    (
                        str(item.get("headline") or "").strip()[:240],
                        str(item.get("category") or "").strip()[:80],
                        str(item.get("why_matters") or "").strip()[:1000],
                        str(item.get("rationale") or "").strip()[:1000],
                        str(item.get("confidence") or "medium").strip().lower()[:20],
                        str(item.get("reject_reason") or "").strip()[:500],
                        status,
                        rank if friday_worthy else None,
                        now,
                        item_id,
                    ),
                )
            conn.execute(
                """
                INSERT INTO dev_buzz_shortlist_snapshots(snapshot_id, created_at_utc, model, status, fallback_reason, raw_json, item_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    now,
                    model,
                    "fallback" if fallback_reason else "llm",
                    fallback_reason,
                    json.dumps(payload, sort_keys=True),
                    len(rows),
                ),
            )
        return snapshot_id

    def record_run(
        self,
        *,
        run_id: str,
        run_type: str,
        status: str,
        started_at_utc: str,
        reason: str | None = None,
        artifact_path: str | None = None,
        posted_channel: str | None = None,
        message_ts: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO dev_buzz_runs(
                    run_id, run_type, status, reason, started_at_utc, completed_at_utc,
                    artifact_path, posted_channel, message_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, run_type, status, reason, started_at_utc, _utc_now().isoformat(), artifact_path, posted_channel, message_ts),
            )

    def was_week_published(self, week_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM dev_buzz_published_weeks WHERE week_key = ? LIMIT 1", (week_key,)).fetchone()
        return row is not None

    def record_published_week(self, *, week_key: str, artifact_path: str, channel: str, message_ts: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO dev_buzz_published_weeks(week_key, posted_at_utc, artifact_path, channel, message_ts)
                VALUES (?, ?, ?, ?, ?)
                """,
                (week_key, _utc_now().isoformat(), artifact_path, channel, message_ts),
            )

    def status(self) -> dict[str, Any]:
        with self._connect() as conn:
            item_count = conn.execute("SELECT COUNT(1) AS n FROM dev_buzz_items").fetchone()["n"]
            shortlist_count = conn.execute(
                "SELECT COUNT(1) AS n FROM dev_buzz_items WHERE dropped = 0 AND (pinned = 1 OR editorial_rank IS NOT NULL)"
            ).fetchone()["n"]
            obs_count = conn.execute("SELECT COUNT(1) AS n FROM dev_buzz_observations").fetchone()["n"]
            runs = conn.execute(
                "SELECT run_type, status, reason, completed_at_utc, artifact_path FROM dev_buzz_runs ORDER BY completed_at_utc DESC LIMIT 5"
            ).fetchall()
        return {
            "ok": True,
            "db_path": str(self.db_path),
            "artifact_dir": str(_artifact_dir()),
            "timezone": os.environ.get("SPCLAW_DEV_BUZZ_TIMEZONE", DEFAULT_TIMEZONE),
            "daily_collect_time": os.environ.get("SPCLAW_DEV_BUZZ_COLLECT_TIME", "12:00"),
            "weekly_publish_time": os.environ.get("SPCLAW_DEV_BUZZ_PUBLISH_TIME", "16:00"),
            "slack_channel": _slack_channel(),
            "model": _model(),
            "top_n": _top_n(),
            "active_sources": len(self.active_sources()),
            "active_keywords": len(self.active_keywords()),
            "items": int(item_count),
            "observations": int(obs_count),
            "shortlist_items": int(shortlist_count),
            "recent_runs": [dict(row) for row in runs],
        }


def _build_queries(sources: list[str], keywords: list[str]) -> list[str]:
    queries: list[str] = []
    release_terms = "(launch OR launched OR release OR released OR announce OR announced OR shipped OR available OR preview)"
    for i in range(0, len(sources), 6):
        chunk = sources[i : i + 6]
        if not chunk:
            continue
        source_part = " OR ".join(f"from:{handle}" for handle in chunk)
        queries.append(f"({source_part}) {release_terms} -is:retweet -is:reply lang:en")
    for keyword in keywords:
        queries.append(f"{keyword} -is:retweet -is:reply lang:en")
    return queries[: _max_queries()]


def _editor_prompt(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    compact = []
    for item in candidates:
        compact.append(
            {
                "item_id": str(item.get("item_id")),
                "current_headline": str(item.get("headline") or ""),
                "text": _excerpt(str(item.get("top_text") or ""), 260),
                "author": str(item.get("top_author") or ""),
                "x_url": str(item.get("top_x_url") or ""),
                "engagement": int(item.get("total_engagement") or 0),
                "observations": int(item.get("observation_count") or 0),
                "unique_authors": int(item.get("unique_authors") or 0),
                "pinned": bool(item.get("pinned")),
            }
        )
    system = (
        "You are OpenClaw's DevBuzzAgent editor. Rank tech launches and major tech news by what engineers "
        "and developers are actually talking about on X. Use judgment, not a formula. Favor concrete launches, "
        "platform shifts, technical breakthroughs, major product moves, and ecosystem changes. Include broader "
        "tech news only when it is clearly driving engineering discussion. Reject pure hype, stale recycled news, "
        "generic takes, and items without a concrete event. Return strict JSON only."
    )
    user = (
        "Update the living shortlist for Friday's Slack digest. Pick up to 12 candidates. "
        "Pinned items should remain eligible unless obviously invalid. JSON shape: "
        '{"shortlist":[{"item_id":"...","rank":1,"headline":"...","category":"...",'
        '"why_matters":"...","rationale":"...","confidence":"high|medium|low",'
        '"friday_worthy":true,"reject_reason":""}]}\n\n'
        f"Candidates:\n{json.dumps(compact, indent=2, sort_keys=True)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = (text or "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            parsed = json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


def _fallback_editor(candidates: list[dict[str, Any]], *, reason: str) -> dict[str, Any]:
    ranked = sorted(
        candidates,
        key=lambda item: (
            -int(bool(item.get("pinned"))),
            -int(item.get("unique_authors") or 0),
            -int(item.get("total_engagement") or 0),
            str(item.get("last_seen_utc") or ""),
        ),
    )
    shortlist: list[dict[str, Any]] = []
    for idx, item in enumerate(ranked[:12], start=1):
        text = str(item.get("top_text") or "")
        shortlist.append(
            {
                "item_id": str(item.get("item_id")),
                "rank": idx,
                "headline": _excerpt(text, 90),
                "category": "Tech news",
                "why_matters": "Engineers are discussing this on X; review source context before relying on it.",
                "rationale": f"Fallback ranking because LLM editor was unavailable or malformed: {reason}.",
                "confidence": "low",
                "friday_worthy": True,
                "reject_reason": "",
            }
        )
    return {"shortlist": shortlist}


def _run_editor(
    candidates: list[dict[str, Any]],
    *,
    llm_editor: Callable[[list[dict[str, str]]], dict[str, Any] | None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    if not candidates:
        return {"shortlist": []}, None
    messages = _editor_prompt(candidates)
    if llm_editor is not None:
        parsed = llm_editor(messages)
        if parsed and isinstance(parsed.get("shortlist"), list):
            return parsed, None
        return _fallback_editor(candidates, reason="test_editor_returned_invalid_payload"), "test_editor_returned_invalid_payload"

    if OpenAI is None:
        return _fallback_editor(candidates, reason="openai_package_missing"), "openai_package_missing"
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _fallback_editor(candidates, reason="openai_api_key_missing"), "openai_api_key_missing"

    try:
        response = OpenAI(api_key=api_key).chat.completions.create(
            model=_model(),
            messages=messages,
            temperature=0.2,
        )
        text = response.choices[0].message.content if response.choices else ""
    except Exception as exc:
        logger.exception("DevBuzz LLM editor failed")
        return _fallback_editor(candidates, reason=f"llm_error:{type(exc).__name__}"), f"llm_error:{type(exc).__name__}"
    parsed = _extract_json_object(text or "")
    if parsed is None or not isinstance(parsed.get("shortlist"), list):
        return _fallback_editor(candidates, reason="llm_json_parse_failed"), "llm_json_parse_failed"
    return parsed, None


def collect(
    *,
    manual: bool = False,
    now_utc: datetime | None = None,
    fetch_json: Callable[..., dict[str, Any]] | None = None,
    llm_editor: Callable[[list[dict[str, str]]], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    started = (now_utc or _utc_now()).replace(microsecond=0)
    store = DevBuzzStore()
    run_id = "dbzc_" + started.strftime("%Y%m%d%H%M%S")
    token = _resolve_bearer_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    fetch = fetch_json or _default_fetch_json
    queries = _build_queries(store.active_sources(), store.active_keywords())
    observed_at = started.isoformat()
    inserted = 0
    errors: list[str] = []
    start_time = (started - timedelta(hours=30)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for query in queries:
        params = {
            "query": query,
            "max_results": str(_max_results_per_query()),
            "start_time": start_time,
            "tweet.fields": "author_id,created_at,public_metrics,lang,entities",
            "expansions": "author_id",
            "user.fields": "name,username,verified",
        }
        try:
            payload = fetch(url=f"{_x_api_base()}/2/tweets/search/recent", headers=headers, params=params)
            inserted += store.upsert_posts(_parse_posts(payload), query=query, observed_at_utc=observed_at)
        except Exception as exc:
            logger.exception("DevBuzz query failed")
            errors.append(f"{query[:80]}: {type(exc).__name__}: {exc}")

    candidates = store.candidates_for_editor(limit=60)
    editorial, fallback_reason = _run_editor(candidates, llm_editor=llm_editor)
    snapshot_id = store.apply_editorial(payload=editorial, model=_model(), fallback_reason=fallback_reason)
    status_value = "ok" if not errors else "partial"
    reason = "; ".join(errors[:3]) if errors else fallback_reason
    store.record_run(run_id=run_id, run_type="collect_manual" if manual else "collect", status=status_value, reason=reason, started_at_utc=observed_at)
    return {
        "ok": not errors,
        "run_id": run_id,
        "queries": len(queries),
        "observations_inserted": inserted,
        "candidates_for_editor": len(candidates),
        "snapshot_id": snapshot_id,
        "editor_mode": "fallback" if fallback_reason else "llm",
        "fallback_reason": fallback_reason,
        "errors": errors,
    }


def _compose_slack_message(items: list[dict[str, Any]], *, now_local: datetime) -> str:
    lines = [f"*Dev Buzz: Buzziest Tech Releases + News This Week* ({now_local.strftime('%b %-d')})", ""]
    if not items:
        lines.append("No Friday-worthy items made the shortlist this week.")
        return "\n".join(lines)
    for idx, item in enumerate(items[: _top_n()], start=1):
        headline = str(item.get("headline") or "").strip() or _excerpt(str(item.get("top_text") or ""), 90)
        why = str(item.get("why_matters") or "").strip() or "Engineers are discussing this on X."
        url = str(item.get("top_x_url") or "").strip()
        lines.append(f"{idx}. *{headline}*")
        lines.append(f"Why devs care: {why}")
        lines.append(
            "Buzz signal: "
            f"{int(item.get('observation_count') or 0)} posts from {int(item.get('unique_authors') or 0)} authors, "
            f"{int(item.get('total_engagement') or 0)} engagement"
        )
        if url:
            lines.append(f"X discussion: {url}")
        lines.append("")
    return "\n".join(lines).strip()


def _write_artifact(*, items: list[dict[str, Any]], message: str, now_local: datetime) -> Path:
    out_dir = _artifact_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"dev-buzz-{now_local.strftime('%Y-%m-%d')}.md"
    lines = [
        f"# Dev Buzz Weekly Digest - {now_local.strftime('%Y-%m-%d')}",
        "",
        f"Generated UTC: `{_utc_now().isoformat()}`",
        "Source: `X Recent Search API v2`",
        "Evidence mode: `X-only`",
        "",
        "## Slack Message",
        "",
        message,
        "",
        "## Editorial Rationale",
    ]
    for idx, item in enumerate(items[: _top_n()], start=1):
        lines.append("")
        lines.append(f"### {idx}. {str(item.get('headline') or item.get('item_id'))}")
        lines.append(f"- item_id: `{item.get('item_id')}`")
        lines.append(f"- category: `{item.get('category') or ''}`")
        lines.append(f"- confidence: `{item.get('confidence') or ''}`")
        lines.append(f"- rationale: {item.get('rationale') or ''}")
        lines.append(f"- X source: {item.get('top_x_url') or ''}")
        lines.append(f"- top text: {_excerpt(str(item.get('top_text') or ''), 400)}")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return path


def _post_to_slack(*, channel_ref: str, text: str) -> tuple[str | None, str | None]:
    if WebClient is None:
        raise DevBuzzError("slack_sdk is unavailable.")
    last_error: Exception | None = None
    for token in _slack_tokens():
        client = WebClient(token=token)
        try:
            resp = client.chat_postMessage(channel=channel_ref, text=text)
            channel = str(resp.get("channel") or channel_ref)
            ts = str(resp.get("ts") or "")
            return channel, ts or None
        except Exception as exc:
            last_error = exc
            logger.exception("DevBuzz Slack post failed")
    raise DevBuzzError(f"Slack post failed: {last_error}")


def publish(
    *,
    dry_run: bool = False,
    force: bool = False,
    now_utc: datetime | None = None,
    channel_override: str | None = None,
) -> dict[str, Any]:
    started = (now_utc or _utc_now()).replace(microsecond=0)
    now_local = started.astimezone(_timezone())
    store = DevBuzzStore()
    run_id = "dbzp_" + started.strftime("%Y%m%d%H%M%S")
    week = _week_key(now_local)
    if not force and store.was_week_published(week):
        store.record_run(run_id=run_id, run_type="publish", status="skipped", reason="week_already_published", started_at_utc=started.isoformat())
        return {"ok": True, "status": "skipped", "reason": "week_already_published", "week_key": week}
    items = store.shortlist(limit=max(_top_n(), 12))
    message = _compose_slack_message(items, now_local=now_local)
    artifact = _write_artifact(items=items, message=message, now_local=now_local)
    channel_ref = (channel_override or _slack_channel()).strip() or DEFAULT_CHANNEL
    posted_channel: str | None = None
    message_ts: str | None = None
    status_value = "dry_run" if dry_run else "posted"
    reason = None
    if not dry_run:
        posted_channel, message_ts = _post_to_slack(channel_ref=channel_ref, text=message)
        store.record_published_week(week_key=week, artifact_path=str(artifact), channel=posted_channel or channel_ref, message_ts=message_ts)
    store.record_run(
        run_id=run_id,
        run_type="publish",
        status=status_value,
        reason=reason,
        started_at_utc=started.isoformat(),
        artifact_path=str(artifact),
        posted_channel=posted_channel or channel_ref,
        message_ts=message_ts,
    )
    return {
        "ok": True,
        "status": status_value,
        "week_key": week,
        "items": len(items[: _top_n()]),
        "artifact_path": str(artifact),
        "channel": posted_channel or channel_ref,
        "message_ts": message_ts,
        "preview": message if dry_run else "",
    }


def status() -> dict[str, Any]:
    return DevBuzzStore().status()


def shortlist(*, limit: int = 10) -> dict[str, Any]:
    return {"ok": True, "items": DevBuzzStore().shortlist(limit=limit)}


def explain(item_id: str) -> dict[str, Any]:
    item = DevBuzzStore().item(item_id)
    if item is None:
        raise DevBuzzError(f"Unknown DevBuzz item: {item_id}")
    return {"ok": True, "item": item}


def add_source(handle: str) -> dict[str, Any]:
    return DevBuzzStore().set_source(handle, active=True)


def remove_source(handle: str) -> dict[str, Any]:
    return DevBuzzStore().set_source(handle, active=False)


def add_keyword(keyword: str) -> dict[str, Any]:
    return DevBuzzStore().set_keyword(keyword, active=True)


def remove_keyword(keyword: str) -> dict[str, Any]:
    return DevBuzzStore().set_keyword(keyword, active=False)


def pin(item_id: str) -> dict[str, Any]:
    return DevBuzzStore().pin_item(item_id)


def drop(item_id: str) -> dict[str, Any]:
    return DevBuzzStore().drop_item(item_id)


def format_shortlist(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Dev Buzz shortlist is empty."
    lines = ["Dev Buzz shortlist:"]
    for item in items:
        rank = item.get("editorial_rank") or "-"
        pin_mark = " pinned" if item.get("pinned") else ""
        headline = str(item.get("headline") or "").strip() or _excerpt(str(item.get("top_text") or ""), 90)
        lines.append(f"- `{item.get('item_id')}` rank `{rank}`{pin_mark}: {headline}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser("spclaw-dev-buzz")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    collect_cmd = sub.add_parser("collect")
    collect_cmd.add_argument("--manual", action="store_true")
    publish_cmd = sub.add_parser("publish")
    publish_cmd.add_argument("--dry-run", action="store_true")
    publish_cmd.add_argument("--force", action="store_true")
    publish_cmd.add_argument("--channel", default="")
    shortlist_cmd = sub.add_parser("shortlist")
    shortlist_cmd.add_argument("--limit", type=int, default=10)
    add_src = sub.add_parser("add-source")
    add_src.add_argument("handle")
    rm_src = sub.add_parser("remove-source")
    rm_src.add_argument("handle")
    add_kw = sub.add_parser("add-keyword")
    add_kw.add_argument("keyword")
    rm_kw = sub.add_parser("remove-keyword")
    rm_kw.add_argument("keyword")
    pin_cmd = sub.add_parser("pin")
    pin_cmd.add_argument("item_id")
    drop_cmd = sub.add_parser("drop")
    drop_cmd.add_argument("item_id")
    exp = sub.add_parser("explain")
    exp.add_argument("item_id")
    args = parser.parse_args()

    if args.cmd == "status":
        payload = status()
    elif args.cmd == "collect":
        payload = collect(manual=bool(args.manual))
    elif args.cmd == "publish":
        payload = publish(dry_run=bool(args.dry_run), force=bool(args.force), channel_override=(str(args.channel).strip() or None))
    elif args.cmd == "shortlist":
        payload = shortlist(limit=int(args.limit))
    elif args.cmd == "add-source":
        payload = add_source(args.handle)
    elif args.cmd == "remove-source":
        payload = remove_source(args.handle)
    elif args.cmd == "add-keyword":
        payload = add_keyword(args.keyword)
    elif args.cmd == "remove-keyword":
        payload = remove_keyword(args.keyword)
    elif args.cmd == "pin":
        payload = pin(args.item_id)
    elif args.cmd == "drop":
        payload = drop(args.item_id)
    else:
        payload = explain(args.item_id)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from html import unescape
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import yfinance as yf

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]

try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
except Exception:  # pragma: no cover - optional dependency
    WebClient = None  # type: ignore[assignment]
    SlackApiError = Exception  # type: ignore[assignment]


load_dotenv("/opt/coatue-claw/.env.prod")

logger = logging.getLogger(__name__)

DEFAULT_TZ = "America/Los_Angeles"
DEFAULT_TIMES = "07:00,14:15"
DEFAULT_TOP_N = 3
DEFAULT_TOP_K = 40
DEFAULT_CHANNEL = "general"
DEFAULT_DATA_ROOT = "/opt/coatue-claw-data"
DEFAULT_SEED_PATH = "/opt/coatue-claw/config/md_tmt_seed_universe.csv"
DEFAULT_MODEL = "gpt-5.2-chat-latest"
US_MARKET_TZ = "America/New_York"
FALLBACK_CAUSE_LINE = "Likely positioning/flow; no single confirmed catalyst."


class MarketDailyError(RuntimeError):
    pass


@dataclass(frozen=True)
class QuoteSnapshot:
    ticker: str
    market_cap: float | None
    last_price: float | None
    previous_close: float | None
    pct_move: float | None
    as_of_utc: str


@dataclass(frozen=True)
class CatalystEvidence:
    ticker: str
    x_text: str | None
    x_url: str | None
    x_engagement: int
    news_title: str | None
    news_url: str | None
    web_title: str | None = None
    web_url: str | None = None
    confidence: float = 0.0
    chosen_source: str | None = None
    driver_keywords: tuple[str, ...] = ()
    top_evidence: tuple[str, ...] = ()
    rejected_reasons: tuple[str, ...] = ()
    since_utc: str | None = None
    confirmed_cluster: str | None = None
    confirmed_cause_phrase: str | None = None
    corroborated_sources: int = 0
    corroborated_domains: int = 0
    web_backend: str | None = None
    selected_cluster: str | None = None
    cluster_debug: tuple[str, ...] = ()
    cause_source_type: str | None = None
    cause_source_url: str | None = None
    cause_mode: str | None = None
    cause_render_mode: str | None = None
    cause_raw_phrase: str | None = None
    cause_final_phrase: str | None = None
    cause_anchor_url: str | None = None
    cause_anchor_text: str | None = None
    cause_support_urls: tuple[str, ...] = ()
    consensus_event_family: str | None = None
    consensus_winner_url: str | None = None
    attribution_stripped: bool = False
    generation_format: str | None = None
    generation_policy: str | None = None
    quality_rejections: tuple[str, ...] = ()
    synth_generation_mode: str | None = None
    synth_model_used: str | None = None
    synth_candidates_considered: tuple[str, ...] = ()
    synth_candidates_used: tuple[str, ...] = ()
    synth_chosen_urls: tuple[str, ...] = ()
    time_integrity_mode: str | None = None
    publish_time_rejections: tuple[str, ...] = ()
    candidate_publish_times: tuple[str, ...] = ()
    historical_callback_rejections: tuple[str, ...] = ()


@dataclass(frozen=True)
class EarningsPreviewItem:
    ticker: str
    company: str
    earnings_date_et: str
    expected_session: str


@dataclass(frozen=True)
class EarningsRecapRow:
    ticker: str
    company: str
    earnings_date_et: str
    inferred_session: str
    market_cap: float | None
    last_price: float | None
    regular_close: float | None
    since_close_pct: float | None
    eps_estimate: float | None = None
    reported_eps: float | None = None
    surprise_pct: float | None = None
    evidence: tuple[str, ...] = ()
    source_links: tuple[str, ...] = ()
    bullets: tuple[str, ...] = ()
    recap_anchor_url: str | None = None
    recap_support_urls: tuple[str, ...] = ()
    recap_generation_mode: str | None = None
    recap_quality_rejections: tuple[str, ...] = ()


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", DEFAULT_DATA_ROOT)).expanduser().resolve()


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_MD_DB_PATH",
            str(_data_root() / "db/market_daily.sqlite"),
        )
    ).expanduser().resolve()


def _artifact_dir() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_MD_ARTIFACT_DIR",
            str(_data_root() / "artifacts/market-daily"),
        )
    ).expanduser().resolve()


def _seed_path() -> Path:
    return Path(os.environ.get("COATUE_CLAW_MD_CANDIDATE_SEED_PATH", DEFAULT_SEED_PATH)).expanduser().resolve()


def _timezone() -> ZoneInfo:
    name = (os.environ.get("COATUE_CLAW_MD_TZ", DEFAULT_TZ) or "").strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except Exception as exc:  # pragma: no cover
        raise MarketDailyError(f"Invalid timezone: {name}") from exc


def _parse_times(raw: str | None = None) -> list[tuple[int, int]]:
    value = (raw or os.environ.get("COATUE_CLAW_MD_TIMES", DEFAULT_TIMES) or DEFAULT_TIMES).strip()
    out: list[tuple[int, int]] = []
    for item in value.split(","):
        item = item.strip()
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", item)
        if not m:
            continue
        hh = int(m.group(1))
        mm = int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            out.append((hh, mm))
    if len(out) < 2:
        out = [(7, 0), (14, 15)]
    out = sorted(out)
    return out[:2]


def _earnings_recap_time() -> tuple[int, int]:
    raw = (os.environ.get("COATUE_CLAW_MD_EARNINGS_RECAP_TIME", "19:00") or "19:00").strip()
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if not m:
        return (19, 0)
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return (19, 0)
    return (hh, mm)


def _md_model() -> str:
    return (os.environ.get("COATUE_CLAW_MD_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip() or DEFAULT_MODEL


def _reason_quality_mode() -> str:
    raw = (os.environ.get("COATUE_CLAW_MD_REASON_QUALITY_MODE", "hybrid") or "hybrid").strip().lower()
    if raw == "deterministic":
        return "deterministic"
    return "hybrid"


def _reason_polish_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_REASON_POLISH_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _reason_polish_model() -> str:
    fallback = _md_model()
    return (os.environ.get("COATUE_CLAW_MD_REASON_POLISH_MODEL", fallback) or fallback).strip() or fallback


def _reason_polish_max_chars() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_REASON_POLISH_MAX_CHARS", "90") or "90").strip()
    try:
        val = int(raw)
    except Exception:
        val = 90
    return max(70, min(130, val))


def _reason_output_mode() -> str:
    raw = (os.environ.get("COATUE_CLAW_MD_REASON_OUTPUT_MODE", "free_sentence") or "free_sentence").strip().lower()
    if raw in {"wrapper", "legacy_wrapper"}:
        return "wrapper"
    return "free_sentence"


def _synth_support_count() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_SYNTH_SUPPORT_COUNT", "2") or "2").strip()
    try:
        val = int(raw)
    except Exception:
        val = 2
    return max(0, min(4, val))


def _md_post_as_is() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_POST_AS_IS", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _relevance_mode() -> str:
    raw = (os.environ.get("COATUE_CLAW_MD_RELEVANCE_MODE", "llm_first") or "llm_first").strip().lower()
    if raw in {"deterministic", "code", "heuristic"}:
        return "deterministic"
    return "llm_first"


def _recap_support_count() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_RECAP_SUPPORT_COUNT", "") or "").strip()
    if not raw:
        return _synth_support_count()
    try:
        val = int(raw)
    except Exception:
        return _synth_support_count()
    return max(0, min(4, val))


def _recap_post_as_is() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_RECAP_POST_AS_IS", "") or "").strip().lower()
    if not raw:
        return _md_post_as_is()
    return raw in {"1", "true", "yes", "on"}


def _catalyst_mode() -> str:
    raw = (os.environ.get("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis") or "simple_synthesis").strip().lower()
    if raw in {"legacy", "legacy_heuristic", "heuristic"}:
        return "legacy_heuristic"
    return "simple_synthesis"


def _synth_max_results() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_SYNTH_MAX_RESULTS", "5") or "5").strip()
    try:
        val = int(raw)
    except Exception:
        val = 5
    return max(1, min(10, val))


def _synth_source_mode() -> str:
    raw = (os.environ.get("COATUE_CLAW_MD_SYNTH_SOURCE_MODE", "google_plus_yahoo") or "google_plus_yahoo").strip().lower()
    if raw in {"google_only", "google"}:
        return "google_only"
    if raw in {"yahoo_only", "yahoo"}:
        return "yahoo_only"
    return "google_plus_yahoo"


def _synth_domain_gate() -> str:
    raw = (os.environ.get("COATUE_CLAW_MD_SYNTH_DOMAIN_GATE", "soft") or "soft").strip().lower()
    if raw in {"quality_only", "strict"}:
        return "quality_only"
    if raw in {"off", "any", "none"}:
        return "off"
    return "soft"


def _synth_force_best_guess() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_SYNTH_FORCE_BEST_GUESS", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _require_in_window_dates() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_REQUIRE_IN_WINDOW_DATES", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _allow_undated_fallback() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_ALLOW_UNDATED_FALLBACK", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _reject_historical_callback() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_REJECT_HISTORICAL_CALLBACK", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _publish_time_enrich_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_PUBLISH_TIME_ENRICH_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _publish_time_enrich_timeout_ms() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_PUBLISH_TIME_ENRICH_TIMEOUT_MS", "1200") or "1200").strip()
    try:
        val = int(raw)
    except Exception:
        val = 1200
    return max(200, min(10000, val))


def _top_n() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_TOP_N", str(DEFAULT_TOP_N)) or str(DEFAULT_TOP_N)).strip()
    try:
        val = int(raw)
    except Exception:
        val = DEFAULT_TOP_N
    return max(1, min(10, val))


def _top_k() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_TMT_TOP_K", str(DEFAULT_TOP_K)) or str(DEFAULT_TOP_K)).strip()
    try:
        val = int(raw)
    except Exception:
        val = DEFAULT_TOP_K
    return max(10, min(200, val))


def _channel_default() -> str:
    return (os.environ.get("COATUE_CLAW_MD_SLACK_CHANNEL", DEFAULT_CHANNEL) or DEFAULT_CHANNEL).strip()


def _x_max_results() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_X_MAX_RESULTS", "50") or "50").strip()
    try:
        val = int(raw)
    except Exception:
        val = 50
    return max(10, min(100, val))


def _max_lookback_hours() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_MAX_LOOKBACK_HOURS", "96") or "96").strip()
    try:
        val = int(raw)
    except Exception:
        val = 96
    return max(8, min(240, val))


def _web_search_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_WEB_SEARCH_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _web_search_backend() -> str:
    return (os.environ.get("COATUE_CLAW_MD_WEB_SEARCH_BACKEND", "google_serp") or "google_serp").strip().lower()


def _google_serp_api_key() -> str | None:
    for key in ("COATUE_CLAW_MD_GOOGLE_SERP_API_KEY", "SERPAPI_API_KEY"):
        value = (os.environ.get(key, "") or "").strip()
        if value:
            return value
    return None


def _google_serp_endpoint() -> str:
    return (os.environ.get("COATUE_CLAW_MD_GOOGLE_SERP_ENDPOINT", "https://serpapi.com/search.json") or "https://serpapi.com/search.json").strip()


def _web_max_results() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_WEB_MAX_RESULTS", "20") or "20").strip()
    try:
        val = int(raw)
    except Exception:
        val = 20
    return max(1, min(40, val))


def _min_evidence_confidence() -> float:
    raw = (os.environ.get("COATUE_CLAW_MD_MIN_EVIDENCE_CONFIDENCE", "0.55") or "0.55").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.55
    return max(0.1, min(0.95, val))


def _min_cause_sources() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_MIN_CAUSE_SOURCES", "2") or "2").strip()
    try:
        val = int(raw)
    except Exception:
        val = 2
    return max(1, min(5, val))


def _min_cause_domains() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_MIN_CAUSE_DOMAINS", "2") or "2").strip()
    try:
        val = int(raw)
    except Exception:
        val = 2
    return max(1, min(5, val))


def _enable_cause_cluster_reuse() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_ENABLE_CAUSE_CLUSTER_REUSE", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _generic_headline_blocklist_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_GENERIC_HEADLINE_BLOCKLIST_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _reason_mode() -> str:
    mode = (os.environ.get("COATUE_CLAW_MD_REASON_MODE", "best_effort") or "best_effort").strip().lower()
    return mode if mode in {"best_effort"} else "best_effort"


def _decisive_primary_reason_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _decisive_primary_reason_min_score() -> float:
    raw = (os.environ.get("COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE", "0.60") or "0.60").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.60
    return max(0.3, min(0.95, val))


def _decisive_primary_reason_min_margin() -> float:
    raw = (os.environ.get("COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN", "0.03") or "0.03").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.03
    return max(0.0, min(0.5, val))


def _x_api_base() -> str:
    return (os.environ.get("COATUE_CLAW_X_API_BASE", "https://api.x.com").strip() or "https://api.x.com").rstrip("/")


def _x_bearer_token() -> str | None:
    for key in ("COATUE_CLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "COATUE_CLAW_TWITTER_BEARER_TOKEN"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return None


def _slack_tokens() -> list[str]:
    tokens: list[str] = []
    env_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if env_token:
        tokens.append(env_token)
    config_path = Path.home() / ".openclaw/openclaw.json"
    if config_path.exists():
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            cfg_token = str((payload.get("channels", {}).get("slack", {}).get("botToken", ""))).strip()
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
        raise MarketDailyError("Slack bot token missing (env SLACK_BOT_TOKEN or ~/.openclaw/openclaw.json).")
    return unique


class MarketDailyStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS md_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date_local TEXT NOT NULL,
                    slot_name TEXT NOT NULL,
                    triggered_manual INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    reason TEXT,
                    channel_ref TEXT,
                    channel_id TEXT,
                    message_ts TEXT,
                    artifact_path TEXT,
                    posted_at_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE(run_date_local, slot_name)
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS md_universe_snapshots (
                    run_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    market_cap REAL,
                    pct_move REAL,
                    source_bucket TEXT NOT NULL,
                    PRIMARY KEY(run_id, ticker)
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS md_coatue_holdings (
                    holding_key TEXT PRIMARY KEY,
                    filing_date TEXT,
                    accession_no TEXT,
                    issuer TEXT,
                    cusip TEXT,
                    ticker TEXT,
                    shares REAL,
                    value_usd REAL,
                    source_url TEXT,
                    resolver TEXT,
                    confidence REAL,
                    updated_at_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS md_cusip_ticker_cache (
                    cusip TEXT PRIMARY KEY,
                    ticker TEXT,
                    resolver TEXT,
                    confidence REAL,
                    updated_at_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS md_overrides (
                    ticker TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    updated_by TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_md_runs_recent
                ON md_runs(created_at_utc DESC);
                """
            )

    def slot_already_recorded(self, *, run_date_local: str, slot_name: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM md_runs WHERE run_date_local = ? AND slot_name = ? LIMIT 1",
                (run_date_local, slot_name),
            ).fetchone()
        return row is not None

    def record_run(
        self,
        *,
        run_date_local: str,
        slot_name: str,
        triggered_manual: bool,
        status: str,
        reason: str | None,
        channel_ref: str | None,
        channel_id: str | None,
        message_ts: str | None,
        artifact_path: str | None,
        posted_at_utc: str | None,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR REPLACE INTO md_runs (
                    run_id,
                    run_date_local,
                    slot_name,
                    triggered_manual,
                    status,
                    reason,
                    channel_ref,
                    channel_id,
                    message_ts,
                    artifact_path,
                    posted_at_utc,
                    created_at_utc
                ) VALUES (
                    COALESCE((SELECT run_id FROM md_runs WHERE run_date_local = ? AND slot_name = ?), NULL),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    run_date_local,
                    slot_name,
                    run_date_local,
                    slot_name,
                    1 if triggered_manual else 0,
                    status,
                    reason,
                    channel_ref,
                    channel_id,
                    message_ts,
                    artifact_path,
                    posted_at_utc,
                    _utc_now_iso(),
                ),
            )
            run_id = int(cur.lastrowid)
            if run_id <= 0:
                row = conn.execute(
                    "SELECT run_id FROM md_runs WHERE run_date_local = ? AND slot_name = ? LIMIT 1",
                    (run_date_local, slot_name),
                ).fetchone()
                run_id = int(row["run_id"]) if row else 0
        return run_id

    def save_universe_snapshot(
        self,
        *,
        run_id: int,
        snapshots: list[QuoteSnapshot],
        source_map: dict[str, str],
    ) -> None:
        if run_id <= 0:
            return
        with self._connect() as conn:
            for item in snapshots:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO md_universe_snapshots (
                        run_id, ticker, market_cap, pct_move, source_bucket
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        item.ticker,
                        item.market_cap,
                        item.pct_move,
                        source_map.get(item.ticker, "top40"),
                    ),
                )

    def latest_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, run_date_local, slot_name, triggered_manual, status, reason,
                       channel_ref, channel_id, message_ts, artifact_path, posted_at_utc, created_at_utc
                FROM md_runs
                ORDER BY created_at_utc DESC
                LIMIT ?
                """,
                (max(1, min(200, int(limit))),),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_runs_for_slot(self, *, slot_name: str, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, run_date_local, slot_name, triggered_manual, status, reason,
                       channel_ref, channel_id, message_ts, artifact_path, posted_at_utc, created_at_utc
                FROM md_runs
                WHERE slot_name = ?
                ORDER BY created_at_utc DESC
                LIMIT ?
                """,
                (slot_name, max(1, min(200, int(limit)))),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_override(self, *, ticker: str, action: str, updated_by: str | None) -> None:
        if action not in {"include", "exclude"}:
            raise MarketDailyError("Override action must be include|exclude")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO md_overrides (ticker, action, updated_at_utc, updated_by)
                VALUES (?, ?, ?, ?)
                """,
                (ticker.upper(), action, _utc_now_iso(), updated_by),
            )

    def list_overrides(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ticker, action, updated_at_utc, updated_by
                FROM md_overrides
                ORDER BY ticker ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def read_override_sets(self) -> tuple[set[str], set[str]]:
        include: set[str] = set()
        exclude: set[str] = set()
        for row in self.list_overrides():
            ticker = str(row.get("ticker") or "").upper().strip()
            action = str(row.get("action") or "").lower().strip()
            if not ticker:
                continue
            if action == "include":
                include.add(ticker)
            elif action == "exclude":
                exclude.add(ticker)
        return include, exclude

    def coatue_tickers(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT ticker
                FROM md_coatue_holdings
                WHERE ticker IS NOT NULL AND ticker != ''
                ORDER BY ticker ASC
                """
            ).fetchall()
        return [str(row["ticker"]).upper() for row in rows]

    def coatue_holdings_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM md_coatue_holdings"
            ).fetchone()
        return int(row["n"] or 0) if row else 0

    def upsert_cusip_cache(self, *, cusip: str, ticker: str | None, resolver: str, confidence: float) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO md_cusip_ticker_cache (cusip, ticker, resolver, confidence, updated_at_utc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (cusip, ticker, resolver, float(confidence), _utc_now_iso()),
            )

    def lookup_cusip_cache(self, cusip: str) -> tuple[str | None, str | None, float] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT ticker, resolver, confidence FROM md_cusip_ticker_cache WHERE cusip = ? LIMIT 1",
                (cusip,),
            ).fetchone()
        if row is None:
            return None
        return (
            str(row["ticker"]).upper() if row["ticker"] else None,
            str(row["resolver"]) if row["resolver"] else None,
            float(row["confidence"] or 0.0),
        )

    def replace_holdings(self, *, rows: list[dict[str, Any]]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM md_coatue_holdings")
            for item in rows:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO md_coatue_holdings (
                        holding_key, filing_date, accession_no, issuer, cusip, ticker,
                        shares, value_usd, source_url, resolver, confidence, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.get("holding_key"),
                        item.get("filing_date"),
                        item.get("accession_no"),
                        item.get("issuer"),
                        item.get("cusip"),
                        item.get("ticker"),
                        item.get("shares"),
                        item.get("value_usd"),
                        item.get("source_url"),
                        item.get("resolver"),
                        item.get("confidence"),
                        _utc_now_iso(),
                    ),
                )

    def holdings_last_updated_utc(self) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(updated_at_utc) AS ts FROM md_coatue_holdings").fetchone()
        if row is None or not row["ts"]:
            return None
        return str(row["ts"])


def _normalize_ticker(raw: str) -> str | None:
    ticker = (raw or "").upper().lstrip("$").strip()
    ticker = re.sub(r"[^A-Z.\-]", "", ticker)
    core = ticker.replace(".", "").replace("-", "")
    if not core or len(core) > 6 or not core.isalpha():
        return None
    return ticker


def _load_seed_tickers(path: Path | None = None) -> list[str]:
    file_path = (path or _seed_path()).expanduser().resolve()
    if not file_path.exists():
        raise MarketDailyError(f"Seed universe file not found: {file_path}")

    lines = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines()]
    out: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if (not line) or line.startswith("#"):
            continue
        if "," in line:
            cell = line.split(",", 1)[0]
        else:
            cell = line
        if cell.lower() in {"ticker", "symbol"}:
            continue
        t = _normalize_ticker(cell)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    if not out:
        raise MarketDailyError(f"Seed universe is empty: {file_path}")
    return out


def _fetch_quote_snapshots(tickers: list[str]) -> list[QuoteSnapshot]:
    out: list[QuoteSnapshot] = []
    now_iso = _utc_now_iso()
    for ticker in tickers:
        t = _normalize_ticker(ticker)
        if not t:
            continue
        try:
            fast = dict(yf.Ticker(t).fast_info)
        except Exception:
            continue

        market_cap = _safe_float(fast.get("marketCap"))
        last_price = _safe_float(fast.get("lastPrice"))
        previous_close = _safe_float(fast.get("regularMarketPreviousClose")) or _safe_float(fast.get("previousClose"))
        pct_move = None
        if last_price is not None and previous_close and previous_close > 0:
            pct_move = (last_price - previous_close) / previous_close

        out.append(
            QuoteSnapshot(
                ticker=t,
                market_cap=market_cap,
                last_price=last_price,
                previous_close=previous_close,
                pct_move=pct_move,
                as_of_utc=now_iso,
            )
        )
    return out


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _build_top_k_universe(*, seed_snapshots: list[QuoteSnapshot], top_k: int) -> list[QuoteSnapshot]:
    ranked = sorted(
        seed_snapshots,
        key=lambda x: (
            -float(x.market_cap or 0.0),
            x.ticker,
        ),
    )
    return ranked[: max(1, top_k)]


def _merge_universe(
    *,
    top_seed: list[QuoteSnapshot],
    coatue_tickers: list[str],
    include_overrides: set[str],
    exclude_overrides: set[str],
) -> tuple[list[str], dict[str, str]]:
    ordered: list[str] = []
    source_bucket: dict[str, str] = {}

    def _push(ticker: str, bucket: str) -> None:
        t = _normalize_ticker(ticker)
        if not t:
            return
        if t in exclude_overrides:
            return
        if t not in source_bucket:
            ordered.append(t)
            source_bucket[t] = bucket
        elif source_bucket[t] != "override_include" and bucket == "override_include":
            source_bucket[t] = bucket

    for item in top_seed:
        _push(item.ticker, "top40")
    for ticker in coatue_tickers:
        _push(ticker, "coatue_overlay")
    for ticker in sorted(include_overrides):
        _push(ticker, "override_include")
    return ordered, source_bucket


def _select_top_movers(*, snapshots: list[QuoteSnapshot], top_n: int) -> list[QuoteSnapshot]:
    valid = [s for s in snapshots if s.pct_move is not None]
    ranked = sorted(
        valid,
        key=lambda s: (
            -abs(float(s.pct_move or 0.0)),
            -float(s.market_cap or 0.0),
            s.ticker,
        ),
    )
    return ranked[: max(1, top_n)]


def _slot_name(*, now_local: datetime, times: list[tuple[int, int]], manual: bool) -> str | None:
    if len(times) < 2:
        times = [(7, 0), (14, 15)]
    first, second = times[0], times[1]

    if manual:
        first_dt = now_local.replace(hour=first[0], minute=first[1], second=0, microsecond=0)
        second_dt = now_local.replace(hour=second[0], minute=second[1], second=0, microsecond=0)
        if abs((now_local - first_dt).total_seconds()) <= abs((now_local - second_dt).total_seconds()):
            return "open"
        return "close"

    now_min = now_local.hour * 60 + now_local.minute
    for name, (hh, mm) in (("open", first), ("close", second)):
        target = hh * 60 + mm
        if abs(now_min - target) <= 20:
            return name
    return None


def _is_weekday(now_local: datetime) -> bool:
    return now_local.weekday() < 5


def _is_market_closed_now(now_local: datetime) -> bool:
    if not _is_weekday(now_local):
        return True
    # Probe SPY intraday bars; holidays generally return no data for current local date.
    try:
        bars = yf.Ticker("SPY").history(period="1d", interval="5m", auto_adjust=False, prepost=False)
    except Exception:
        # On provider error, do not hard-close the gate; better to post than silently skip.
        return False
    if bars is None or bars.empty:
        return True
    latest = bars.index.max()
    if latest is None:
        return True
    try:
        latest_ts = latest.to_pydatetime()  # type: ignore[attr-defined]
    except Exception:
        latest_ts = latest
    if isinstance(latest_ts, datetime):
        if latest_ts.tzinfo is None:
            latest_local = latest_ts.replace(tzinfo=UTC).astimezone(_timezone())
        else:
            latest_local = latest_ts.astimezone(_timezone())
    else:
        return False
    return latest_local.date() != now_local.date()


def _http_json(
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
    method: str = "GET",
    body: bytes | None = None,
) -> Any:
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"
    req = Request(full_url, headers=headers, method=method, data=body)
    try:
        with urlopen(req, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise MarketDailyError(f"HTTP error {exc.code} for {url}: {detail[:300]}") from exc
    except URLError as exc:
        raise MarketDailyError(f"Network error for {url}: {exc.reason}") from exc
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise MarketDailyError(f"Invalid JSON payload from {url}") from exc
    return parsed


def _sec_headers() -> dict[str, str]:
    ua = (os.environ.get("COATUE_CLAW_SEC_USER_AGENT", "CoatueClaw/1.0 (ops@coatueclaw.local)") or "").strip()
    return {"User-Agent": ua, "Accept": "application/json,text/plain,*/*"}


def _latest_13f_filing(cik_raw: str) -> dict[str, str] | None:
    digits = re.sub(r"\D", "", cik_raw or "")
    if not digits:
        return None
    cik10 = digits.zfill(10)
    payload = _http_json(
        f"https://data.sec.gov/submissions/CIK{cik10}.json",
        headers=_sec_headers(),
    )
    recent = (payload.get("filings") or {}).get("recent") if isinstance(payload.get("filings"), dict) else None
    if not isinstance(recent, dict):
        return None

    forms = recent.get("form") or []
    accs = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    docs = recent.get("primaryDocument") or []
    n = min(len(forms), len(accs), len(dates), len(docs))
    for i in range(n):
        form = str(forms[i] or "").upper().strip()
        if not form.startswith("13F-HR"):
            continue
        accession = str(accs[i] or "").strip()
        filing_date = str(dates[i] or "").strip()
        primary_doc = str(docs[i] or "").strip()
        if not accession:
            continue
        accession_nodash = accession.replace("-", "")
        cik_int = str(int(cik10))
        filing_base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}"
        return {
            "cik": cik10,
            "accession_no": accession,
            "filing_date": filing_date,
            "primary_doc": primary_doc,
            "filing_base": filing_base,
        }
    return None


def _resolve_info_table_url(filing_base: str) -> str | None:
    index_url = f"{filing_base}/index.json"
    payload = _http_json(index_url, headers=_sec_headers())
    directory = payload.get("directory") if isinstance(payload, dict) else None
    items = (directory or {}).get("item") if isinstance(directory, dict) else None
    if not isinstance(items, list):
        return None

    names = [str(item.get("name") or "") for item in items if isinstance(item, dict)]
    candidates = [n for n in names if n.lower().endswith(".xml")]
    preferred = [
        n
        for n in candidates
        if ("infotable" in n.lower()) or ("informationtable" in n.lower()) or ("form13f" in n.lower())
    ]
    pick = preferred[0] if preferred else (candidates[0] if candidates else None)
    if not pick:
        return None
    return f"{filing_base}/{pick}"


def _fetch_text(url: str, *, headers: dict[str, str]) -> str:
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=30) as response:
            payload = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise MarketDailyError(f"HTTP error {exc.code} for {url}: {detail[:300]}") from exc
    except URLError as exc:
        raise MarketDailyError(f"Network error for {url}: {exc.reason}") from exc
    return payload.decode("utf-8", errors="ignore")


def _normalize_cusip(cusip: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (cusip or "").upper())


def _parse_13f_info_table(xml_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as exc:
        raise MarketDailyError(f"Failed to parse 13F info table XML: {exc}") from exc

    for node in root.findall(".//{*}infoTable"):
        issuer = _normalize_whitespace(node.findtext("{*}nameOfIssuer") or "")
        cusip = _normalize_cusip(node.findtext("{*}cusip") or "")
        value_k = _safe_float(node.findtext("{*}value"))
        shares = _safe_float(node.findtext("{*}shrsOrPrnAmt/{*}sshPrnamt"))
        if not issuer and not cusip:
            continue
        rows.append(
            {
                "issuer": issuer,
                "cusip": cusip,
                "value_usd": (float(value_k) * 1000.0) if value_k is not None else None,
                "shares": shares,
            }
        )
    return rows


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _resolve_ticker_via_openfigi(cusip: str) -> tuple[str | None, str, float]:
    api_key = os.environ.get("COATUE_CLAW_MD_OPENFIGI_API_KEY", "").strip()
    if not api_key:
        return (None, "openfigi_missing_key", 0.0)

    url = "https://api.openfigi.com/v3/mapping"
    headers = {
        "Content-Type": "application/json",
        "X-OPENFIGI-APIKEY": api_key,
    }
    body = json.dumps([{"idType": "ID_CUSIP", "idValue": cusip}]).encode("utf-8")
    try:
        payload = _http_json(url, headers=headers, method="POST", body=body)
    except Exception:
        return (None, "openfigi_error", 0.0)

    rows: list[Any]
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = [payload]
    else:
        rows = []
    if not rows:
        return (None, "openfigi_no_match", 0.0)
    first = rows[0]
    if not isinstance(first, dict):
        return (None, "openfigi_no_match", 0.0)
    data = first.get("data")
    if not isinstance(data, list) or not data:
        return (None, "openfigi_no_match", 0.0)
    best = data[0]
    if not isinstance(best, dict):
        return (None, "openfigi_no_match", 0.0)
    ticker = _normalize_ticker(str(best.get("ticker") or ""))
    if ticker:
        return (ticker, "openfigi", 0.95)
    return (None, "openfigi_no_ticker", 0.0)


def _resolve_ticker_via_name(issuer: str) -> tuple[str | None, str, float]:
    if not issuer:
        return (None, "name_missing", 0.0)
    try:
        search = yf.Search(issuer, max_results=10)
        quotes = getattr(search, "quotes", None) or []
    except Exception:
        return (None, "name_search_error", 0.0)

    for item in quotes:
        if not isinstance(item, dict):
            continue
        quote_type = str(item.get("quoteType") or "").upper()
        if quote_type and quote_type != "EQUITY":
            continue
        symbol = _normalize_ticker(str(item.get("symbol") or ""))
        if not symbol:
            continue
        return (symbol, "name_search", 0.65)
    return (None, "name_no_match", 0.0)


def refresh_coatue_holdings(*, store: MarketDailyStore | None = None) -> dict[str, Any]:
    store = store or MarketDailyStore()
    cik = (os.environ.get("COATUE_CLAW_MD_COATUE_CIK", "") or "").strip()
    if not cik:
        return {
            "ok": True,
            "updated": False,
            "reason": "cik_missing",
            "rows": 0,
        }

    filing = _latest_13f_filing(cik)
    if filing is None:
        return {
            "ok": True,
            "updated": False,
            "reason": "no_13f_filing",
            "rows": 0,
        }

    info_table_url = _resolve_info_table_url(filing["filing_base"])
    if not info_table_url:
        return {
            "ok": True,
            "updated": False,
            "reason": "info_table_missing",
            "rows": 0,
            "filing": filing,
        }

    xml_text = _fetch_text(info_table_url, headers=_sec_headers())
    parsed_rows = _parse_13f_info_table(xml_text)

    merged_rows: list[dict[str, Any]] = []
    resolved = 0
    for row in parsed_rows:
        issuer = str(row.get("issuer") or "")
        cusip = _normalize_cusip(str(row.get("cusip") or ""))
        ticker: str | None = None
        resolver = "unresolved"
        confidence = 0.0

        if cusip:
            cached = store.lookup_cusip_cache(cusip)
            if cached is not None:
                ticker, resolver, confidence = cached
        if ticker is None and cusip:
            ticker, resolver, confidence = _resolve_ticker_via_openfigi(cusip)
            if ticker:
                store.upsert_cusip_cache(cusip=cusip, ticker=ticker, resolver=resolver, confidence=confidence)
        if ticker is None:
            ticker, resolver, confidence = _resolve_ticker_via_name(issuer)
            if ticker and cusip:
                store.upsert_cusip_cache(cusip=cusip, ticker=ticker, resolver=resolver, confidence=confidence)

        if ticker:
            resolved += 1

        holding_key = f"{filing['accession_no']}:{cusip or issuer}".strip(":")
        merged_rows.append(
            {
                "holding_key": holding_key,
                "filing_date": filing.get("filing_date"),
                "accession_no": filing.get("accession_no"),
                "issuer": issuer,
                "cusip": cusip,
                "ticker": ticker,
                "shares": row.get("shares"),
                "value_usd": row.get("value_usd"),
                "source_url": info_table_url,
                "resolver": resolver,
                "confidence": confidence,
            }
        )

    store.replace_holdings(rows=merged_rows)
    return {
        "ok": True,
        "updated": True,
        "rows": len(merged_rows),
        "resolved_rows": resolved,
        "filing": filing,
        "info_table_url": info_table_url,
    }


_COMPANY_ALIAS_OVERRIDES: dict[str, list[str]] = {
    "NET": ["Cloudflare", "Cloudflare Inc", "Cloudflare, Inc."],
    "CRWD": ["CrowdStrike", "CrowdStrike Holdings"],
    "OKTA": ["Okta"],
    "PANW": ["Palo Alto Networks"],
    "ORCL": ["Oracle"],
    "BKNG": ["Booking Holdings", "Booking.com", "online travel agency", "OTA"],
    "APP": ["AppLovin", "AppLovin Corporation", "AppLovin Corp"],
}

_DRIVER_KEYWORDS: dict[str, tuple[str, ...]] = {
    "anthropic_claude_cyber": (
        "anthropic",
        "claude code security",
        "claude code",
        "security tool",
        "cybersecurity stocks fell",
        "cybersecurity stocks drop",
    ),
    "anthropic_claude": ("anthropic", "claude", "code security"),
    "cybersecurity_competition": ("cybersecurity", "security tool", "threat detection", "vulnerability"),
    "ota_ai_disruption": (
        "ai threat",
        "ai panic",
        "agent disruption",
        "ota disruption",
        "online travel agency",
        "booking holdings",
        "booking.com",
    ),
    "travel_demand_outlook": (
        "travel demand slowdown",
        "forward outlook pressure",
        "forward outlook",
        "demand slowdown",
        "ota industry",
    ),
    "regulatory_probe": (
        "sec probe",
        "sec investigation",
        "federal probe",
        "regulatory probe",
        "under investigation",
        "ongoing probe",
        "probe report",
        "short report",
        "short seller report",
        "legal overhang",
    ),
    "earnings_guidance": ("earnings", "guidance", "forecast", "outlook"),
    "macro_rates": ("rate cut", "rates", "treasury", "yield"),
    "deal_contract": ("deal", "contract", "partnership"),
    "product_launch": ("launch", "rollout", "release"),
    "analyst_move": ("upgrade", "downgrade", "price target"),
}

_CLUSTER_EVENT_PHRASES: dict[str, str] = {
    "anthropic_claude_cyber": "Anthropic launched Claude Code Security, pressuring cybersecurity stocks.",
    "anthropic_claude": "Anthropic launched new Claude security capabilities.",
    "cybersecurity_competition": "new security tooling intensified competitive pressure.",
    "ota_ai_disruption": "AI-agent disruption fears pressured online travel stocks.",
    "travel_demand_outlook": "forward outlook concerns pressured travel demand expectations.",
    "regulatory_probe": "reports of an active SEC probe.",
    "earnings_guidance": "earnings and guidance reset expectations.",
    "macro_rates": "rates and macro signals shifted risk appetite.",
    "product_launch": "a product launch reset expectations.",
    "analyst_move": "analyst rating changes moved expectations.",
}

_CLUSTER_PRIORITY_BONUS: dict[str, float] = {
    "anthropic_claude_cyber": 0.35,
    "anthropic_claude": 0.2,
    "ota_ai_disruption": 0.22,
    "regulatory_probe": 0.25,
}

_CLUSTER_REUSE_ALLOWLIST: set[str] = {"anthropic_claude_cyber", "anthropic_claude"}

_DOMAIN_WEIGHTS: dict[str, float] = {
    "finance.yahoo.com": 0.95,
    "investing.com": 0.9,
    "stocktwits.com": 0.88,
    "marketwatch.com": 0.88,
    "barrons.com": 0.9,
    "bloomberg.com": 0.92,
    "reuters.com": 0.9,
    "wsj.com": 0.86,
    "seekingalpha.com": 0.82,
    "coincentral.com": 0.75,
    "tikr.com": 0.72,
    "fool.com": 0.7,
    "benzinga.com": 0.74,
}

_QUALITY_CAUSE_DOMAINS: set[str] = {
    "finance.yahoo.com",
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "barrons.com",
    "marketwatch.com",
    "investing.com",
    "stocktwits.com",
}

_GENERIC_WRAPPER_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bwhy\b.*\bstock\b.*\bdown today\b", flags=re.IGNORECASE),
    re.compile(r"\bnews today\b", flags=re.IGNORECASE),
    re.compile(r"\bstock is down today\b", flags=re.IGNORECASE),
    re.compile(r"\bstock down today\b", flags=re.IGNORECASE),
    re.compile(r"\bwhy\b.*\bshares?\b.*\btoday\b", flags=re.IGNORECASE),
    re.compile(r"\bshares?\b.*\btrading lower today\b", flags=re.IGNORECASE),
    re.compile(r"\bshares?\b.*\bfalling today\b", flags=re.IGNORECASE),
)

_BASKET_MEMBERS: dict[str, set[str]] = {
    "cybersecurity": {"NET", "CRWD", "PANW", "OKTA", "ZS", "FTNT", "S"},
}


@dataclass(frozen=True)
class _EvidenceCandidate:
    source_type: str
    text: str
    url: str | None
    published_at_utc: datetime | None
    score: float
    engagement: int = 0
    driver_keywords: tuple[str, ...] = ()
    reject_reason: str | None = None
    canonical_url: str | None = None
    domain: str | None = None
    backend: str | None = None
    context_text: str | None = None
    published_confidence: str = "none"
    published_source: str | None = None


def _company_aliases(ticker: str) -> list[str]:
    t = ticker.upper().strip()
    aliases = list(_COMPANY_ALIAS_OVERRIDES.get(t, []))
    if not aliases:
        try:
            info = yf.Ticker(t).info or {}
        except Exception:
            info = {}
        for key in ("shortName", "longName"):
            val = _normalize_whitespace(str(info.get(key) or ""))
            if val:
                aliases.append(val)
    uniq: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        cleaned = _normalize_whitespace(alias).strip(".,")
        if not cleaned:
            continue
        lo = cleaned.lower()
        if lo in seen:
            continue
        seen.add(lo)
        uniq.append(cleaned)
    return uniq[:5]


def _session_window_since_utc(*, slot_name: str, now_utc: datetime | None = None) -> datetime:
    return _session_anchor_start_utc(slot_name=slot_name, now_utc=now_utc or datetime.now(UTC))


def _session_anchor_start_utc(*, slot_name: str, now_utc: datetime) -> datetime:
    et = ZoneInfo(US_MARKET_TZ)
    now_et = now_utc.astimezone(et)
    try:
        bars = yf.Ticker("SPY").history(period="14d", interval="1d", auto_adjust=False, prepost=False)
    except Exception:
        bars = None

    market_dates: list[datetime.date] = []
    if bars is not None and not bars.empty:
        for idx in bars.index:
            try:
                dt = idx.to_pydatetime()  # type: ignore[attr-defined]
            except Exception:
                dt = idx
            if isinstance(dt, datetime):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                market_dates.append(dt.astimezone(et).date())
    market_dates = sorted(set(market_dates))

    today = now_et.date()
    if market_dates:
        if today in market_dates:
            current_date = today
        else:
            older = [d for d in market_dates if d < today]
            current_date = older[-1] if older else market_dates[-1]
    else:
        current_date = today
        while current_date.weekday() >= 5:
            current_date = current_date - timedelta(days=1)

    previous_date = current_date - timedelta(days=1)
    while previous_date.weekday() >= 5:
        previous_date = previous_date - timedelta(days=1)
    if market_dates:
        older = [d for d in market_dates if d < current_date]
        if older:
            previous_date = older[-1]

    if slot_name == "open":
        start_et = datetime(previous_date.year, previous_date.month, previous_date.day, 16, 0, 0, tzinfo=et)
    else:
        start_et = datetime(current_date.year, current_date.month, current_date.day, 9, 30, 0, tzinfo=et)

    cap = timedelta(hours=_max_lookback_hours())
    if now_utc - start_et.astimezone(UTC) > cap:
        return now_utc - cap
    return start_et.astimezone(UTC)


def _parse_datetime_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    raw = _normalize_whitespace(str(value))
    if not raw:
        return None
    if raw.isdigit():
        try:
            return datetime.fromtimestamp(int(raw), tz=UTC)
        except Exception:
            return None

    norm = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(norm)
    except Exception:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


_MONTH_NAME_TO_NUM: dict[str, int] = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_PUBLISH_TIME_CACHE: dict[str, tuple[datetime | None, str]] = {}


def _parse_relative_time_utc(raw: str, *, now_utc: datetime) -> datetime | None:
    text = _normalize_whitespace(raw).lower()
    m = re.search(r"\b(\d+)\s*(minute|minutes|min|mins|hour|hours|day|days|week|weeks)\s+ago\b", text)
    if not m:
        return None
    qty = int(m.group(1))
    unit = m.group(2)
    if unit.startswith("min"):
        return now_utc - timedelta(minutes=qty)
    if unit.startswith("hour"):
        return now_utc - timedelta(hours=qty)
    if unit.startswith("day"):
        return now_utc - timedelta(days=qty)
    if unit.startswith("week"):
        return now_utc - timedelta(days=(7 * qty))
    return None


def _parse_month_day_date_utc(raw: str, *, now_utc: datetime) -> datetime | None:
    text = _normalize_whitespace(raw)
    m = re.search(r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,\s*(\d{4}))?\b", text)
    if not m:
        return None
    month_raw = m.group(1).strip().lower()
    month = _MONTH_NAME_TO_NUM.get(month_raw)
    day = int(m.group(2))
    if month is None:
        return None
    year = int(m.group(3)) if m.group(3) else now_utc.year
    try:
        dt = datetime(year, month, day, tzinfo=UTC)
    except Exception:
        return None
    if dt > (now_utc + timedelta(days=2)):
        try:
            dt = dt.replace(year=(dt.year - 1))
        except Exception:
            return None
    return dt


def _extract_explicit_dates_from_text(text: str, *, now_utc: datetime) -> list[datetime]:
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return []
    out: list[datetime] = []
    for m in re.finditer(r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,\s*(\d{4}))?\b", cleaned):
        parsed = _parse_month_day_date_utc(m.group(0), now_utc=now_utc)
        if parsed is not None:
            out.append(parsed)
    uniq: list[datetime] = []
    seen: set[str] = set()
    for item in out:
        key = item.date().isoformat()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(item)
    return uniq


def _is_historical_callback(*, text: str, since_utc: datetime, now_utc: datetime) -> bool:
    if not _reject_historical_callback():
        return False
    if not text:
        return False
    dates = _extract_explicit_dates_from_text(text, now_utc=now_utc)
    if not dates:
        return False
    cutoff = (since_utc - timedelta(days=1)).date()
    return any(item.date() < cutoff for item in dates)


def _parse_candidate_published_at_from_serp_row(row: dict[str, Any], *, now_utc: datetime) -> tuple[datetime | None, str, str]:
    checks: list[tuple[Any, str]] = [
        (row.get("published_at"), "serp_published_at"),
        (row.get("date"), "serp_date"),
        (row.get("time"), "serp_time"),
    ]
    for value, source in checks:
        if value is None:
            continue
        raw = _normalize_whitespace(str(value))
        if not raw:
            continue
        parsed = _parse_datetime_utc(raw)
        if parsed is None:
            parsed = _parse_relative_time_utc(raw, now_utc=now_utc)
        if parsed is None:
            parsed = _parse_month_day_date_utc(raw, now_utc=now_utc)
        if parsed is not None:
            confidence = "high" if source == "serp_published_at" else "medium"
            return parsed.astimezone(UTC), confidence, source
    return None, "none", "none"


def _fetch_text_with_timeout(url: str, *, headers: dict[str, str], timeout_sec: float) -> str:
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout_sec) as response:
            payload = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise MarketDailyError(f"HTTP error {exc.code} for {url}: {detail[:300]}") from exc
    except URLError as exc:
        raise MarketDailyError(f"Network error for {url}: {exc.reason}") from exc
    return payload.decode("utf-8", errors="ignore")


def _parse_published_at_from_article_html(url: str | None) -> tuple[datetime | None, str]:
    canonical = _canonicalize_url(url) or (url or "")
    if not canonical:
        return None, "none"
    cached = _PUBLISH_TIME_CACHE.get(canonical)
    if cached is not None:
        return cached
    if not _publish_time_enrich_enabled():
        _PUBLISH_TIME_CACHE[canonical] = (None, "none")
        return None, "none"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
    timeout_sec = _publish_time_enrich_timeout_ms() / 1000.0
    try:
        html = _fetch_text_with_timeout(canonical, headers=headers, timeout_sec=timeout_sec)
    except Exception:
        _PUBLISH_TIME_CACHE[canonical] = (None, "none")
        return None, "none"
    patterns: list[tuple[re.Pattern[str], str]] = [
        (re.compile(r'property=["\']article:published_time["\'][^>]*content=["\']([^"\']+)["\']', flags=re.IGNORECASE), "article_meta"),
        (re.compile(r'property=["\']og:published_time["\'][^>]*content=["\']([^"\']+)["\']', flags=re.IGNORECASE), "og_meta"),
        (re.compile(r'"datePublished"\s*:\s*"([^"]+)"', flags=re.IGNORECASE), "jsonld"),
    ]
    for pattern, source in patterns:
        m = pattern.search(html)
        if not m:
            continue
        parsed = _parse_datetime_utc(m.group(1))
        if parsed is None:
            continue
        out = (parsed.astimezone(UTC), source)
        _PUBLISH_TIME_CACHE[canonical] = out
        return out
    _PUBLISH_TIME_CACHE[canonical] = (None, "none")
    return None, "none"


def _is_in_session_window(*, published_at_utc: datetime | None, since_utc: datetime, now_utc: datetime) -> bool:
    if published_at_utc is None:
        return False
    return since_utc <= published_at_utc <= now_utc


def _enforce_time_integrity(
    *,
    candidates: list[_EvidenceCandidate],
    since_utc: datetime,
    pct_move: float | None = None,
    enrich_limit: int | None = None,
    now_utc: datetime | None = None,
) -> tuple[list[_EvidenceCandidate], list[str]]:
    now = now_utc or datetime.now(UTC)
    limit = enrich_limit if isinstance(enrich_limit, int) and enrich_limit >= 0 else max(6, (_synth_max_results() * 2))
    out: list[_EvidenceCandidate] = []
    notes: list[str] = []
    for idx, item in enumerate(candidates):
        context = _normalize_whitespace(item.context_text or item.text)
        if _is_historical_callback(text=context, since_utc=since_utc, now_utc=now):
            notes.append(f"historical_callback_reject:{item.source_type}:{item.url or 'no-url'}")
            continue

        published = item.published_at_utc
        confidence = item.published_confidence
        source = item.published_source
        if published is None and idx < limit:
            enriched_dt, enriched_source = _parse_published_at_from_article_html(item.url)
            if enriched_dt is not None:
                published = enriched_dt
                confidence = "high" if enriched_source in {"article_meta", "og_meta"} else "medium"
                source = enriched_source
            elif _require_in_window_dates() and (not _allow_undated_fallback()):
                notes.append(f"publish_time_reject:undated_unverified:{item.source_type}:{item.url or 'no-url'}")
                continue

        if _require_in_window_dates() and (published is not None) and (not _is_in_session_window(published_at_utc=published, since_utc=since_utc, now_utc=now)):
            notes.append(f"publish_time_reject:out_of_window:{item.source_type}:{item.url or 'no-url'}")
            continue

        if _require_in_window_dates() and (published is None) and (not _allow_undated_fallback()):
            notes.append(f"publish_time_reject:publish_time_parse_failed:{item.source_type}:{item.url or 'no-url'}")
            continue

        out.append(
            replace(
                item,
                published_at_utc=published,
                published_confidence=confidence or "none",
                published_source=source,
            )
        )
    ranked = sorted(
        out,
        key=lambda c: (
            -_effective_candidate_score(candidate=c, pct_move=pct_move),
            _source_rank(c.source_type),
        ),
    )
    return ranked, notes


def _domain_weight(url: str | None) -> float:
    if not url:
        return 0.5
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    if not host:
        return 0.5
    for domain, weight in _DOMAIN_WEIGHTS.items():
        if host == domain or host.endswith("." + domain):
            return weight
    return 0.6


def _domain_from_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_quality_domain(url_or_domain: str | None) -> bool:
    domain = _domain_from_url(url_or_domain)
    if not domain:
        domain = (url_or_domain or "").strip().lower()
    if not domain:
        return False
    return any(domain == d or domain.endswith("." + d) for d in _QUALITY_CAUSE_DOMAINS)


def _title_fingerprint(text: str) -> str:
    normalized = _normalize_whitespace(text).lower()
    normalized = re.sub(r"https?://\S+", " ", normalized)
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    tokens = [tok for tok in normalized.split() if tok not in {"the", "a", "an", "and", "for", "to", "of", "is", "are"}]
    return " ".join(tokens[:18])


def _canonicalize_url(url: str | None) -> str | None:
    raw = _ddg_resolve_url(url or "")
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    if not parsed.scheme:
        return raw
    filtered_q = []
    for k, vals in parse_qs(parsed.query, keep_blank_values=False).items():
        key = k.lower()
        if key.startswith("utm_") or key in {"guccounter", "guce_referrer", "guce_referrer_sig"}:
            continue
        for v in vals:
            filtered_q.append((k, v))
    filtered_query = urlencode(filtered_q)
    cleaned = parsed._replace(query=filtered_query, fragment="")
    return urlunparse(cleaned)


def _is_quote_directory_title(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return False
    if re.search(r"\bnews,\s*quote\s*(?:&|and)\s*history\b", lower):
        return True
    if re.search(r"\bstock price,\s*news,\s*quote\s*(?:&|and)\s*history\b", lower):
        return True
    if "latest stock news & headlines" in lower or "latest stock news and headlines" in lower:
        return True
    if re.search(r"\bfind the latest\b.*\bstock quote,\s*history,\s*news\b", lower):
        return True
    if re.search(r"\bstock quote,\s*history,\s*news\b", lower):
        return True
    if ("stock price" in lower) and ("quote" in lower) and ("history" in lower):
        return True
    return False


def _is_quote_directory_url(url: str | None) -> bool:
    raw = _canonicalize_url(url) or _normalize_whitespace(url or "")
    if not raw:
        return False
    try:
        parsed = urlparse(raw)
    except Exception:
        return False
    domain = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if (domain == "finance.yahoo.com" or domain.endswith(".finance.yahoo.com")) and path.startswith("/quote/"):
        return True
    if (domain == "cnbc.com" or domain.endswith(".cnbc.com")) and path.startswith("/quotes/"):
        return True
    return False


def _is_quote_directory_wrapper(*, text: str, url: str | None) -> bool:
    return _is_quote_directory_title(text) or _is_quote_directory_url(url)


def _is_generic_headline_wrapper(*, text: str, ticker: str, aliases: list[str]) -> bool:
    if not _generic_headline_blocklist_enabled():
        return False
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return True
    if _is_quote_directory_title(cleaned):
        return True
    lower = cleaned.lower()
    specific_event_terms = (
        "after",
        "amid",
        "due to",
        "because",
        "following",
        "anthropic",
        "claude",
        "security",
        "guidance",
        "outlook",
        "downgrade",
        "upgrade",
        "lawsuit",
        "ai threat",
        "ai panic",
        "ota disruption",
    )
    for pattern in _GENERIC_WRAPPER_PATTERNS:
        if pattern.search(lower):
            if any(term in lower for term in specific_event_terms) and len(_extract_driver_keywords(cleaned)) > 0:
                return False
            return True
    tokenized = re.sub(r"[^a-z0-9$ ]+", " ", lower)
    words = [w for w in tokenized.split() if w]
    ticker_mentions = ticker.lower() in words or f"${ticker.lower()}" in words or any(a.lower() in lower for a in aliases)
    if ticker_mentions and len(words) <= 6:
        if not any(term in lower for term in ("after", "amid", "due to", "because", "launch", "earnings", "guidance", "deal", "downgrade", "upgrade", "lawsuit", "security")):
            return True
    return False


def _normalize_evidence_candidates(*, candidates: list[_EvidenceCandidate], ticker: str, aliases: list[str]) -> list[_EvidenceCandidate]:
    deduped: list[_EvidenceCandidate] = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    for item in candidates:
        canonical_url = _canonicalize_url(item.url)
        domain = _domain_from_url(canonical_url or item.url)
        fingerprint = _title_fingerprint(item.text)
        if canonical_url and canonical_url in seen_urls:
            continue
        if fingerprint and fingerprint in seen_titles:
            continue
        if canonical_url:
            seen_urls.add(canonical_url)
        if fingerprint:
            seen_titles.add(fingerprint)
        deduped.append(
            replace(
                item,
                url=canonical_url or item.url,
                canonical_url=canonical_url or item.url,
                domain=domain,
                reject_reason=(
                    "generic_wrapper"
                    if (
                        _is_generic_headline_wrapper(text=item.text, ticker=ticker, aliases=aliases)
                        or _is_quote_directory_wrapper(text=item.text, url=(canonical_url or item.url))
                    )
                    else item.reject_reason
                ),
            )
        )
    return deduped


def _extract_driver_keywords(text: str) -> tuple[str, ...]:
    lower = _normalize_whitespace(text).lower()
    out: list[str] = []
    for cluster, keywords in _DRIVER_KEYWORDS.items():
        if any(k in lower for k in keywords):
            out.append(cluster)
    if "anthropic_claude_cyber" in out and "cybersecurity_competition" in out:
        out = [x for x in out if x != "cybersecurity_competition"]
    return tuple(out)


def _compute_evidence_score(
    *,
    source_type: str,
    text: str,
    url: str | None,
    published_at_utc: datetime | None,
    since_utc: datetime,
    ticker: str,
    aliases: list[str],
) -> float:
    now = datetime.now(UTC)
    if published_at_utc is None:
        recency = 0.45
    else:
        age_hours = max(0.0, (now - published_at_utc).total_seconds() / 3600.0)
        window_hours = max(1.0, (now - since_utc).total_seconds() / 3600.0)
        recency = max(0.0, 1.0 - (age_hours / window_hours))
    domain = _domain_weight(url)
    lower = text.lower()
    mention = 0.0
    if f"${ticker.lower()}" in lower:
        mention += 0.45
    if re.search(rf"\b{re.escape(ticker.lower())}\b", lower):
        mention += 0.25
    if any(alias.lower() in lower for alias in aliases):
        mention += 0.3
    keyword_hits = len(_extract_driver_keywords(text))
    keyword_score = min(0.35, keyword_hits * 0.12)
    lower = text.lower()
    causal_bonus = 0.0
    if any(term in lower for term in (" after ", " amid ", " due to ", " because ", " following ", " as ")):
        causal_bonus += 0.09
    if any(
        term in lower
        for term in (
            "anthropic",
            "claude code security",
            "guidance",
            "outlook",
            "lawsuit",
            "downgrade",
            "upgrade",
            "earnings",
            "ai threat",
            "ai panic",
            "ota disruption",
            "travel demand slowdown",
            "sec probe",
            "sec investigation",
            "regulatory probe",
        )
    ):
        causal_bonus += 0.06
    generic_penalty = -0.18 if _is_generic_headline_wrapper(text=text, ticker=ticker, aliases=aliases) else 0.0
    ta_penalty = -0.16 if _is_technical_analysis_style(text) else 0.0
    price_action_penalty = -0.22 if _is_price_action_only_text(text) else 0.0
    roundup_penalty = -0.08 if _is_multi_ticker_roundup(text=text, ticker=ticker, aliases=aliases) else 0.0
    source_bonus = {"yahoo_news": 0.18, "x": 0.12, "web": 0.1}.get(source_type, 0.0)
    score = (
        (0.35 * recency)
        + (0.3 * domain)
        + mention
        + keyword_score
        + source_bonus
        + causal_bonus
        + generic_penalty
        + ta_penalty
        + price_action_penalty
        + roundup_penalty
    )
    return max(0.0, min(1.0, score))


def _has_causal_marker(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    return any(term in lower for term in (" after ", " amid ", " due to ", " because ", " following ", " as "))


def _has_strict_causal_marker(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    return any(term in lower for term in (" after ", " amid ", " due to ", " because ", " following "))


def _has_catalyst_vocabulary(text: str) -> bool:
    upper = _normalize_whitespace(text).upper()
    vocab = (
        "EARNINGS",
        "GUIDANCE",
        "FORECAST",
        "OUTLOOK",
        "UPGRADE",
        "DOWNGRADE",
        "PROBE",
        "INVESTIGATION",
        "CONTRACT",
        "PARTNERSHIP",
        "LAUNCH",
        "DEMAND",
        "SUPPLY",
        "MARGIN",
        "REVENUE",
        "SEC",
        "REGULATORY",
        "MISS",
        "BEAT",
    )
    return any(term in upper for term in vocab)


def _is_low_signal_x_post(text: str) -> bool:
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return True
    lower = cleaned.lower()

    promo_terms = (
        "discord",
        "chatroom",
        "link below",
        "join ",
        "free",
        "telegram",
        "whatsapp",
        "vip group",
        "dm me",
    )
    if any(term in lower for term in promo_terms):
        return True

    cashtags = re.findall(r"\$[A-Za-z]{1,6}\b", cleaned)
    if len(cashtags) > 6 and (not _has_causal_marker(cleaned)):
        return True

    if re.search(r"\$[A-Za-z]{1,6}\b", cleaned) and (not _has_catalyst_vocabulary(cleaned)):
        return True

    return False


def _x_query_for_ticker(*, ticker: str, aliases: list[str]) -> str:
    alias_terms = " OR ".join([f"\"{a}\"" for a in aliases[:3]])
    symbol_terms = f"${ticker} OR \"{ticker}\""
    identity = f"({symbol_terms})" if not alias_terms else f"({symbol_terms} OR {alias_terms})"
    driver_terms = (
        "stock OR shares OR earnings OR guidance OR revenue OR margin OR outlook OR downgrade OR upgrade "
        "OR cybersecurity OR security OR anthropic OR claude OR partnership OR contract OR launch OR forecast"
    )
    return f"{identity} ({driver_terms}) -is:retweet -is:reply lang:en"


def _fetch_x_evidence_candidates(*, ticker: str, aliases: list[str], since_utc: datetime) -> list[_EvidenceCandidate]:
    token = _x_bearer_token()
    if not token:
        return []

    start = since_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    params = {
        "query": _x_query_for_ticker(ticker=ticker, aliases=aliases),
        "max_results": str(_x_max_results()),
        "start_time": start,
        "tweet.fields": "author_id,created_at,public_metrics",
        "expansions": "author_id",
        "user.fields": "username,name",
    }
    try:
        payload = _http_json(
            f"{_x_api_base()}/2/tweets/search/recent",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            params=params,
        )
    except Exception:
        return []

    users: dict[str, str] = {}
    includes = payload.get("includes")
    if isinstance(includes, dict):
        for user in includes.get("users") or []:
            if isinstance(user, dict):
                uid = str(user.get("id") or "").strip()
                uname = str(user.get("username") or "").strip()
                if uid and uname:
                    users[uid] = uname

    out: list[_EvidenceCandidate] = []
    for row in payload.get("data") or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_whitespace(str(row.get("text") or ""))
        tweet_id = str(row.get("id") or "").strip()
        author_id = str(row.get("author_id") or "").strip()
        if not text or not tweet_id:
            continue
        if not _is_relevant_ticker_post(text=text, ticker=ticker, aliases=aliases):
            continue
        if _is_low_signal_x_post(text):
            continue
        metrics = row.get("public_metrics") if isinstance(row.get("public_metrics"), dict) else {}
        engagement = int(metrics.get("like_count", 0)) + int(metrics.get("retweet_count", 0)) + int(metrics.get("reply_count", 0)) + int(
            metrics.get("quote_count", 0)
        )
        author = users.get(author_id)
        url = f"https://x.com/{author}/status/{tweet_id}" if author else f"https://x.com/i/web/status/{tweet_id}"
        published = _parse_datetime_utc(row.get("created_at"))
        score = _compute_evidence_score(
            source_type="x",
            text=text,
            url=url,
            published_at_utc=published,
            since_utc=since_utc,
            ticker=ticker,
            aliases=aliases,
        )
        score = max(0.0, min(1.0, score + min(0.1, engagement / 2500.0)))
        out.append(
            _EvidenceCandidate(
                source_type="x",
                text=text,
                url=url,
                published_at_utc=published,
                score=score,
                engagement=engagement,
                driver_keywords=_extract_driver_keywords(text),
            )
        )
    return sorted(out, key=lambda x: (-x.score, -x.engagement))


def _fetch_x_evidence(*, ticker: str, hours: int) -> tuple[str | None, str | None, int]:
    since = datetime.now(UTC) - timedelta(hours=max(1, hours))
    aliases = _company_aliases(ticker)
    candidates = _fetch_x_evidence_candidates(ticker=ticker, aliases=aliases, since_utc=since)
    if not candidates:
        return (None, None, 0)
    best = candidates[0]
    return (best.text, best.url, best.engagement)


def _is_relevant_ticker_post(*, text: str, ticker: str, aliases: list[str] | None = None) -> bool:
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return False
    upper = cleaned.upper()
    t = ticker.upper().strip()
    if not t:
        return False

    has_alias = any(alias.lower() in cleaned.lower() for alias in (aliases or []))
    has_cashtag = bool(re.search(rf"\${re.escape(t)}\b", upper))
    has_symbol = bool(re.search(rf"\b{re.escape(t)}\b", upper))
    if len(t) <= 3 and not (has_cashtag or has_alias):
        return False
    if not has_cashtag and not has_symbol and not has_alias:
        return False

    finance_keywords = (
        "STOCK",
        "SHARES",
        "EARNINGS",
        "GUIDANCE",
        "REVENUE",
        "MARGIN",
        "PRICE TARGET",
        "UPGRADE",
        "DOWNGRADE",
        "BUY",
        "SELL",
        "OUTLOOK",
        "CLOUD",
        "CAPEX",
        "DEMAND",
        "ESTIMATE",
        "CYBERSECURITY",
        "SECURITY",
        "ANTHROPIC",
        "CLAUDE",
    )
    if not any(word in upper for word in finance_keywords):
        return False

    noise_keywords = (
        "RUN RATE",
        "CRICKET",
        "MATCH",
        "GOAL",
        "SCORE",
        "FINAL",
    )
    if any(word in upper for word in noise_keywords):
        return False
    return True


def _is_relevant_ticker_headline(*, text: str, ticker: str, aliases: list[str] | None = None) -> bool:
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return False
    t = ticker.upper().strip()
    upper = cleaned.upper()
    has_cashtag = bool(re.search(rf"\${re.escape(t)}\b", upper))
    has_symbol = bool(re.search(rf"\b{re.escape(t)}\b", upper))
    has_alias = any(alias.lower() in cleaned.lower() for alias in (aliases or []))
    return has_cashtag or has_symbol or has_alias


def _yahoo_item_title_url_published(item: dict[str, Any]) -> tuple[str, str, datetime | None, str]:
    content = item.get("content") if isinstance(item.get("content"), dict) else {}
    legacy_ts = item.get("providerPublishTime")
    legacy_title = item.get("title")
    legacy_link = item.get("link")

    pub_date_raw = content.get("pubDate") if isinstance(content, dict) else None
    title_raw = content.get("title") if isinstance(content, dict) else None
    summary_raw = content.get("summary") if isinstance(content, dict) else None
    desc_raw = content.get("description") if isinstance(content, dict) else None
    click_raw = content.get("clickThroughUrl") if isinstance(content, dict) else None
    canonical_raw = content.get("canonicalUrl") if isinstance(content, dict) else None

    title = _normalize_whitespace(str(title_raw or legacy_title or ""))
    summary = _normalize_whitespace(str(summary_raw or desc_raw or ""))
    url = ""
    if isinstance(click_raw, dict):
        url = str(click_raw.get("url") or "").strip()
    if (not url) and isinstance(canonical_raw, dict):
        url = str(canonical_raw.get("url") or "").strip()
    if not url:
        url = str(legacy_link or "").strip()
    if url.startswith("https://r.search.yahoo.com/") and "RU=" in url:
        try:
            ru = parse_qs(urlparse(url).query).get("RU", [""])[0]
            if ru:
                url = unquote(ru)
        except Exception:
            pass
    published = _parse_datetime_utc(pub_date_raw) or _parse_datetime_utc(legacy_ts)
    context = _normalize_whitespace(f"{title}. {summary}") if summary else title
    return (title, url, published, context)


def _fetch_yahoo_news_candidates(*, ticker: str, aliases: list[str], since_utc: datetime) -> list[_EvidenceCandidate]:
    try:
        news = yf.Ticker(ticker).news or []
    except Exception:
        return []

    out: list[_EvidenceCandidate] = []
    for item in news:
        if not isinstance(item, dict):
            continue
        title, link, published, context = _yahoo_item_title_url_published(item)
        if not title or not link:
            continue
        if not _is_relevant_ticker_headline(text=title, ticker=ticker, aliases=aliases):
            continue
        if published and published < since_utc:
            continue
        score = _compute_evidence_score(
            source_type="yahoo_news",
            text=title,
            url=link,
            published_at_utc=published,
            since_utc=since_utc,
            ticker=ticker,
            aliases=aliases,
        )
        out.append(
            _EvidenceCandidate(
                source_type="yahoo_news",
                text=title,
                url=link,
                published_at_utc=published,
                score=score,
                driver_keywords=_extract_driver_keywords(title),
                context_text=context,
                published_confidence=("high" if published is not None else "none"),
                published_source=("yahoo_feed" if published is not None else None),
            )
        )
    return sorted(out, key=lambda x: (-x.score, -(x.published_at_utc.timestamp() if x.published_at_utc else 0.0)))


def _fetch_yahoo_news(*, ticker: str, hours: int | None = None, since_utc: datetime | None = None) -> tuple[str | None, str | None]:
    aliases = _company_aliases(ticker)
    if since_utc is None:
        cutoff_hours = max(1, int(hours or 24))
        since_utc = datetime.now(UTC) - timedelta(hours=cutoff_hours)
    candidates = _fetch_yahoo_news_candidates(ticker=ticker, aliases=aliases, since_utc=since_utc)
    if not candidates:
        return (None, None)
    best = candidates[0]
    return (best.text, best.url)


def _ddg_resolve_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    decoded = unescape(raw_url)
    if decoded.startswith("//"):
        decoded = "https:" + decoded
    if decoded.startswith("/l/?") or decoded.startswith("https://duckduckgo.com/l/?") or decoded.startswith("http://duckduckgo.com/l/?"):
        try:
            query = parse_qs(urlparse(decoded).query)
            uddg = query.get("uddg", [""])[0]
            if uddg:
                decoded = unquote(uddg)
        except Exception:
            pass
    return decoded


def _web_queries_for_ticker(*, ticker: str, aliases: list[str], pct_move: float | None) -> list[str]:
    primary = aliases[0] if aliases else ticker
    down_or_up = "down" if (pct_move or 0.0) < 0 else "up"
    queries = [
        f"why is {ticker} stock {down_or_up}",
        f"{ticker} {primary} stock {down_or_up} reason today",
        f"{ticker} {primary} selloff cause",
        f"{ticker} {primary} stock move today why",
    ]
    if ticker.upper() == "BKNG":
        queries.extend(
            [
                "why is BKNG stock down",
                "BKNG stock down reason today",
                "Booking Holdings selloff cause",
                "BKNG AI threat travel OTA",
            ]
        )
    if ticker.upper() == "APP":
        queries.extend(
            [
                "why is APP stock down sec probe",
                "AppLovin SEC probe report",
                "AppLovin short seller report SEC investigation",
                "AppLovin regulatory probe stock move",
            ]
        )
    uniq: list[str] = []
    seen: set[str] = set()
    for q in queries:
        key = _normalize_whitespace(q).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(q)
    return uniq


def _google_serp_candidate_text(*, title: str, snippet: str) -> str:
    t = _normalize_whitespace(title)
    s = _normalize_whitespace(snippet)
    if t and s:
        return f"{t}. {s}"
    return t or s


def _fetch_web_evidence_google_serp(*, ticker: str, aliases: list[str], since_utc: datetime, pct_move: float | None = None) -> list[_EvidenceCandidate]:
    if not _web_search_enabled():
        return []
    api_key = _google_serp_api_key()
    if not api_key:
        return []
    endpoint = _google_serp_endpoint()
    out: list[_EvidenceCandidate] = []
    seen_urls: set[str] = set()
    max_results = _web_max_results()
    headers = {"Accept": "application/json", "User-Agent": "CoatueClaw/1.0"}

    now_utc = datetime.now(UTC)
    for query in _web_queries_for_ticker(ticker=ticker, aliases=aliases, pct_move=pct_move):
        if len(out) >= max_results:
            break
        params = {
            "engine": "google",
            "q": query,
            "hl": "en",
            "gl": "us",
            "num": str(min(20, max_results)),
            "api_key": api_key,
        }
        try:
            payload = _http_json(endpoint, headers=headers, params=params)
        except Exception:
            continue

        answer_box = payload.get("answer_box") if isinstance(payload, dict) else None
        if isinstance(answer_box, dict):
            a_title = str(answer_box.get("title") or "")
            a_snippet = str(answer_box.get("snippet") or answer_box.get("answer") or "")
            if isinstance(answer_box.get("snippet_highlighted_words"), list):
                words = " ".join(str(x) for x in answer_box.get("snippet_highlighted_words") if str(x).strip())
                if words:
                    a_snippet = f"{a_snippet} {words}".strip()
            a_url = str(answer_box.get("link") or answer_box.get("source") or "").strip()
            text = _google_serp_candidate_text(title=a_title, snippet=a_snippet)
            if text:
                parsed_dt, parsed_conf, parsed_source = _parse_candidate_published_at_from_serp_row(answer_box, now_utc=now_utc)
                if a_url:
                    a_url = _canonicalize_url(a_url) or a_url
                if (not a_url) or (a_url not in seen_urls):
                    if _is_relevant_ticker_headline(text=text, ticker=ticker, aliases=aliases):
                        score = _compute_evidence_score(
                            source_type="web",
                            text=text,
                            url=a_url,
                            published_at_utc=None,
                            since_utc=since_utc,
                            ticker=ticker,
                            aliases=aliases,
                        )
                        out.append(
                            _EvidenceCandidate(
                                source_type="web",
                                text=text,
                                url=a_url,
                                published_at_utc=parsed_dt,
                                score=max(0.0, min(1.0, score + 0.1)),
                                driver_keywords=_extract_driver_keywords(text),
                                canonical_url=a_url,
                                domain=_domain_from_url(a_url),
                                backend="google_serp",
                                context_text=text,
                                published_confidence=parsed_conf,
                                published_source=parsed_source,
                            )
                        )
                        if a_url:
                            seen_urls.add(a_url)
                        if len(out) >= max_results:
                            break

        if len(out) >= max_results:
            break

        for key in ("organic_results", "news_results", "top_stories"):
            rows = payload.get(key) if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = _normalize_whitespace(str(row.get("title") or ""))
                snippet = _normalize_whitespace(str(row.get("snippet") or ""))
                if isinstance(row.get("snippet_highlighted_words"), list):
                    words = " ".join(str(x) for x in row.get("snippet_highlighted_words") if str(x).strip())
                    if words:
                        snippet = f"{snippet} {words}".strip()
                article_url = str(row.get("link") or row.get("url") or "").strip()
                if not article_url:
                    continue
                article_url = _canonicalize_url(article_url) or article_url
                if (not article_url) or (article_url in seen_urls):
                    continue
                text = _google_serp_candidate_text(title=title, snippet=snippet)
                if not text:
                    continue
                if not _is_relevant_ticker_headline(text=text, ticker=ticker, aliases=aliases):
                    continue
                parsed_dt, parsed_conf, parsed_source = _parse_candidate_published_at_from_serp_row(row, now_utc=now_utc)
                seen_urls.add(article_url)
                score = _compute_evidence_score(
                    source_type="web",
                    text=text,
                    url=article_url,
                    published_at_utc=parsed_dt,
                    since_utc=since_utc,
                    ticker=ticker,
                    aliases=aliases,
                )
                if snippet:
                    score = max(0.0, min(1.0, score + 0.06))
                out.append(
                    _EvidenceCandidate(
                        source_type="web",
                        text=text,
                        url=article_url,
                        published_at_utc=parsed_dt,
                        score=score,
                        driver_keywords=_extract_driver_keywords(text),
                        canonical_url=article_url,
                        domain=_domain_from_url(article_url),
                        backend="google_serp",
                        context_text=text,
                        published_confidence=parsed_conf,
                        published_source=parsed_source,
                    )
                )
                if len(out) >= max_results:
                    break
            if len(out) >= max_results:
                break
    return sorted(out, key=lambda x: -x.score)


def _fetch_web_evidence_ddg(*, ticker: str, aliases: list[str], since_utc: datetime, pct_move: float | None = None) -> list[_EvidenceCandidate]:
    if not _web_search_enabled():
        return []
    queries = _web_queries_for_ticker(ticker=ticker, aliases=aliases, pct_move=pct_move)
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
    seen_urls: set[str] = set()
    out: list[_EvidenceCandidate] = []
    max_results = _web_max_results()

    for query in queries:
        if len(out) >= max_results:
            break
        ddg_url = "https://duckduckgo.com/html/?" + urlencode({"q": query})
        try:
            html = _fetch_text(ddg_url, headers=headers)
        except Exception:
            continue

        link_iter = re.finditer(
            r'<a[^>]*class="[^"]*result__a[^"]*"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for match in link_iter:
            article_url = _ddg_resolve_url(match.group(1))
            if not article_url or article_url in seen_urls:
                continue
            seen_urls.add(article_url)
            title = _normalize_whitespace(re.sub(r"<[^>]+>", " ", unescape(match.group(2))))
            if not title:
                continue
            lower_title = title.lower()
            if ticker.lower() not in lower_title and not any(a.lower() in lower_title for a in aliases):
                continue
            score = _compute_evidence_score(
                source_type="web",
                text=title,
                url=article_url,
                published_at_utc=None,
                since_utc=since_utc,
                ticker=ticker,
                aliases=aliases,
            )
            out.append(
                _EvidenceCandidate(
                    source_type="web",
                    text=title,
                    url=article_url,
                    published_at_utc=None,
                    score=score,
                    driver_keywords=_extract_driver_keywords(title),
                    canonical_url=article_url,
                    domain=_domain_from_url(article_url),
                    backend="ddg_html",
                    context_text=title,
                )
            )
            if len(out) >= max_results:
                break
    return sorted(out, key=lambda x: -x.score)


def _fetch_web_evidence(
    *,
    ticker: str,
    aliases: list[str],
    since_utc: datetime,
    pct_move: float | None = None,
) -> tuple[list[_EvidenceCandidate], str | None, list[str]]:
    if not _web_search_enabled():
        return ([], None, ["web:disabled"])
    backend_pref = _web_search_backend()
    notes: list[str] = []
    backend_order: list[str]
    if backend_pref in {"google_serp", "google"}:
        backend_order = ["google_serp", "ddg_html"]
    elif backend_pref in {"ddg_html", "ddg"}:
        backend_order = ["ddg_html"]
    else:
        backend_order = ["google_serp", "ddg_html"]

    for backend in backend_order:
        if backend == "google_serp":
            if not _google_serp_api_key():
                notes.append("web:google_serp_api_key_missing")
                continue
            rows = _normalize_evidence_candidates(
                candidates=_fetch_web_evidence_google_serp(ticker=ticker, aliases=aliases, since_utc=since_utc, pct_move=pct_move),
                ticker=ticker,
                aliases=aliases,
            )
            if rows:
                return (rows, "google_serp", notes)
            notes.append("web:google_serp_no_signal")
            continue

        rows = _normalize_evidence_candidates(
            candidates=_fetch_web_evidence_ddg(ticker=ticker, aliases=aliases, since_utc=since_utc, pct_move=pct_move),
            ticker=ticker,
            aliases=aliases,
        )
        if rows:
            return (rows, "ddg_html", notes)
        notes.append("web:ddg_html_no_signal")
    return ([], None, notes)


def _source_rank(source_type: str) -> int:
    return {"yahoo_news": 0, "web": 1}.get(source_type, 9)


def _md_allowed_evidence_sources() -> set[str]:
    return {"yahoo_news", "web"}


def _directional_bonus(*, text: str, pct_move: float | None) -> float:
    if pct_move is None:
        return 0.0
    lower = text.lower()
    up_terms = ("surge", "rises", "rose", "gains", "jumps", "up ", "beats", "upgrades", "partnership", "announces", "announce")
    down_terms = (
        "drops",
        "drop",
        "fell",
        "falls",
        "slides",
        "selloff",
        "sold off",
        "down ",
        "misses",
        "downgrade",
        "weighed",
        "pressured",
        "under pressure",
        "probe",
        "investigation",
        "regulatory",
        "sec",
    )
    if pct_move < 0:
        if any(term in lower for term in down_terms):
            return 0.14
        if any(term in lower for term in up_terms):
            return -0.08
    if pct_move > 0:
        if any(term in lower for term in up_terms):
            return 0.14
        if any(term in lower for term in down_terms):
            return -0.08
    return 0.0


def _basket_name_for_ticker(ticker: str) -> str | None:
    t = _normalize_ticker(ticker)
    if not t:
        return None
    for basket, members in _BASKET_MEMBERS.items():
        if t in members:
            return basket
    return None


def _effective_candidate_score(*, candidate: _EvidenceCandidate, pct_move: float | None) -> float:
    value = candidate.score + _directional_bonus(text=candidate.text, pct_move=pct_move)
    if candidate.reject_reason == "generic_wrapper" and _generic_headline_blocklist_enabled():
        value -= 0.45
    return max(0.0, min(1.0, value))


def _pick_best_by_source(candidates: list[_EvidenceCandidate], source_type: str, *, pct_move: float | None) -> _EvidenceCandidate | None:
    rows = [c for c in candidates if c.source_type == source_type]
    if not rows:
        return None
    return sorted(rows, key=lambda c: (-_effective_candidate_score(candidate=c, pct_move=pct_move), _source_rank(c.source_type)))[0]


def _pick_direct_cause_candidate(*, candidates: list[_EvidenceCandidate], pct_move: float | None) -> _EvidenceCandidate | None:
    eligible: list[_EvidenceCandidate] = []
    for item in candidates:
        if item.source_type not in {"yahoo_news", "web"}:
            continue
        if _is_quote_directory_wrapper(text=item.text, url=item.url):
            continue
        if item.reject_reason == "generic_wrapper":
            continue
        if not _is_quality_domain(item.domain or item.url):
            continue
        if _contains_disallowed_reason_phrasing(item.text):
            continue
        if not (_looks_like_specific_catalyst(item.text) or _has_causal_marker(item.text)):
            continue
        if _effective_candidate_score(candidate=item, pct_move=pct_move) < _min_evidence_confidence():
            continue
        eligible.append(item)
    if not eligible:
        return None
    return sorted(
        eligible,
        key=lambda c: (
            -_effective_candidate_score(candidate=c, pct_move=pct_move),
            _source_rank(c.source_type),
        ),
    )[0]


def _is_quality_url(url: str | None) -> bool:
    if not url:
        return False
    return _is_quality_domain(url)


def _build_links_for_mover(*, ev: CatalystEvidence, cat_line: str) -> list[str]:
    fallback = (cat_line or "").strip() == FALLBACK_CAUSE_LINE
    links: list[str] = []
    seen_domains: set[str] = set()

    def _push(url: str | None, label: str, *, quality_only: bool) -> None:
        if not url:
            return
        if quality_only and (not _is_quality_url(url)):
            return
        domain = _domain_from_url(url)
        if domain and domain in seen_domains:
            return
        links.append(f"<{url}|[{label}]>")
        if domain:
            seen_domains.add(domain)

    if ev.cause_mode == "simple_synthesis" and ev.cause_anchor_url and not fallback:
        ordered_urls: list[str] = [ev.cause_anchor_url] + list(ev.cause_support_urls)
        for url in ordered_urls:
            label = "News" if url == ev.news_url else ("Web" if url == ev.web_url else ("News" if "/news/" in (url or "") else "Web"))
            _push(url, label, quality_only=False)
        if links:
            return links

    if fallback:
        _push(ev.news_url, "News", quality_only=True)
        _push(ev.web_url, "Web", quality_only=True)
        return links

    _push(ev.news_url, "News", quality_only=False)
    _push(ev.web_url, "Web", quality_only=False)
    return links


def _driver_cluster_scores(candidates: list[_EvidenceCandidate]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for item in candidates:
        for key in item.driver_keywords:
            totals[key] = totals.get(key, 0.0) + item.score
    return totals


def _cluster_members(candidates: list[_EvidenceCandidate], cluster: str) -> list[_EvidenceCandidate]:
    return [c for c in candidates if cluster in c.driver_keywords]


def _cluster_independent_sources(candidates: list[_EvidenceCandidate]) -> int:
    source_types: set[str] = set()
    domains: set[str] = set()
    for item in candidates:
        source_types.add(item.source_type)
        if item.domain:
            domains.add(item.domain)
    if domains:
        return max(len(source_types), len(domains))
    return len(source_types)


def _cluster_domain_count(candidates: list[_EvidenceCandidate]) -> int:
    return len({item.domain for item in candidates if item.domain})


def _cluster_has_quality_domain(candidates: list[_EvidenceCandidate]) -> bool:
    return any(_is_quality_domain(item.domain or item.url) for item in candidates)


def _cluster_is_corroborated(candidates: list[_EvidenceCandidate]) -> bool:
    return (
        _cluster_independent_sources(candidates) >= _min_cause_sources()
        and _cluster_domain_count(candidates) >= _min_cause_domains()
        and _cluster_has_quality_domain(candidates)
    )


def _cluster_event_phrase(cluster: str, *, candidate: _EvidenceCandidate | None) -> str | None:
    fixed = _CLUSTER_EVENT_PHRASES.get(cluster)
    if fixed:
        return fixed
    if candidate is None:
        return None
    if _is_quote_directory_wrapper(text=candidate.text, url=candidate.url):
        return None
    if candidate.reject_reason == "generic_wrapper":
        return None
    cleaned = _strip_non_md_artifacts(candidate.text)
    cleaned = re.sub(r"(?i)^why\b.+?\b(today|now)\b", "", cleaned).strip()
    if not cleaned:
        return None
    cleaned = _shorten(cleaned.rstrip(".") + ".", 95).rstrip(".") + "."
    return cleaned


def _can_use_decisive_primary_reason(
    *,
    cluster_candidate: _EvidenceCandidate | None,
    top_cluster_score: float,
    second_cluster_score: float,
    pct_move: float | None,
) -> bool:
    if not _decisive_primary_reason_enabled():
        return False
    if cluster_candidate is None:
        return False
    if cluster_candidate.reject_reason == "generic_wrapper":
        return False
    if _contains_disallowed_reason_phrasing(cluster_candidate.text):
        return False
    if not _is_quality_domain(cluster_candidate.domain or cluster_candidate.url):
        return False
    eff = _effective_candidate_score(candidate=cluster_candidate, pct_move=pct_move)
    if eff < _decisive_primary_reason_min_score():
        return False
    margin = top_cluster_score - second_cluster_score
    if margin < _decisive_primary_reason_min_margin():
        strong_terms = (
            "lawsuit",
            "sues",
            "sued",
            "launch",
            "launched",
            "earnings",
            "guidance",
            "downgrade",
            "upgrade",
            "investigation",
            "probe",
            "regulatory",
            "sec",
            "acquisition",
            "contract",
            "partnership",
            "financing",
        )
        text = (cluster_candidate.text or "").lower()
        if not (eff >= 0.9 and any(term in text for term in strong_terms)):
            return False
    return True


def _build_reason_line_from_phrase(*, pct_move: float | None, phrase: str | None) -> str:
    if not phrase:
        return FALLBACK_CAUSE_LINE
    cleaned_phrase = _shorten(_strip_non_md_artifacts(phrase).strip(), 90).rstrip(".")
    if not cleaned_phrase:
        return FALLBACK_CAUSE_LINE
    if _is_quote_directory_title(cleaned_phrase):
        return FALLBACK_CAUSE_LINE
    if pct_move is not None and pct_move < 0:
        line = f"Shares fell after {cleaned_phrase}."
    elif pct_move is not None and pct_move > 0:
        line = f"Shares rose after {cleaned_phrase}."
    else:
        line = f"Shares moved after {cleaned_phrase}."
    line = _shorten(line, 110).rstrip(" .") + "."
    if _contains_disallowed_reason_phrasing(line):
        return FALLBACK_CAUSE_LINE
    return line


def _contains_disallowed_reason_phrasing(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    if _is_quote_directory_title(lower):
        return True
    if ("stock is down today" in lower) or ("news today" in lower):
        return True
    return any(pattern.search(lower) for pattern in _GENERIC_WRAPPER_PATTERNS)


def _has_action_verb(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    action_terms = (
        "rose",
        "rises",
        "gained",
        "jumped",
        "surged",
        "rallied",
        "fell",
        "falls",
        "dropped",
        "slides",
        "slid",
        "sank",
        "tumbled",
        "pressured",
        "beat",
        "missed",
        "raised",
        "cut",
        "launched",
        "announced",
        "inked",
        "signed",
        "upgraded",
        "downgraded",
        "report",
        "reports",
        "weighs",
    )
    return any(re.search(rf"\b{re.escape(term)}\b", lower) for term in action_terms)


def _has_event_vocab(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    event_terms = (
        "guidance",
        "earnings",
        "forecast",
        "outlook",
        "downgrade",
        "upgrade",
        "probe",
        "investigation",
        "contract",
        "deal",
        "partnership",
        "launch",
        "regulatory",
        "sec",
        "threat",
        "disruption",
        "overhang",
        "sentiment",
    )
    return any(term in lower for term in event_terms)


def _event_family(text: str) -> str:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return "other"

    if any(term in lower for term in ("sec", "investigation", "regulatory", "probe", "lawsuit", "antitrust", "doj", "ftc")):
        return "regulatory"
    if any(term in lower for term in ("upgrade", "downgrade", "price target", "analyst", "rating")):
        return "analyst_move"
    if any(term in lower for term in ("guidance", "outlook", "forecast", "raised guidance", "cut guidance")):
        return "guidance"
    if any(term in lower for term in ("earnings", "eps", "revenue", "quarter", "q1", "q2", "q3", "q4", "beat", "miss")):
        return "earnings"
    if any(term in lower for term in ("price increase", "raise prices", "raised prices", "price hike", "pricing", "cut prices", "price cut")):
        return "pricing"
    if any(
        term in lower
        for term in (
            "partnership",
            "deal",
            "agreement",
            "contract",
            "collaboration",
            "joint venture",
            "tie-up",
            "tie up",
            "signed",
            "inked",
            "investment round",
            "funding round",
        )
    ):
        return "deal_partnership"
    return "other"


def _sentence_family(text: str) -> str:
    return _event_family(text)


def _strip_publisher_attribution(sentence: str) -> tuple[str, bool]:
    cleaned = _normalize_whitespace(str(sentence or ""))
    if not cleaned:
        return "", False

    original = _normalize_generated_sentence(cleaned, max_chars=220)
    patterns = (
        r"(?i)^\s*(?:according to|per)\s+(?:a\s+)?(?:reuters|bloomberg|the wall street journal|wsj|cnbc|marketwatch|yahoo finance)\s*[,:-]?\s*",
        r"(?i)^\s*(?:a\s+)?(?:reuters|bloomberg|the wall street journal|wsj|cnbc|marketwatch|yahoo finance)\s+report(?:ed)?\s+(?:that\s+)?",
        r"(?i)\b(?:a|an)\s+(?:reuters|bloomberg|the wall street journal|wsj|cnbc|marketwatch|yahoo finance)\s+report\s+(?:said|says|noted)\s+(?:that\s+)?",
        r"(?i)\b(?:reuters|bloomberg|the wall street journal|wsj|cnbc|marketwatch|yahoo finance)\s+(?:reported|reports|said|says|noted|notes)\s+(?:that\s+)?",
    )
    for pattern in patterns:
        cleaned = re.sub(pattern, "", cleaned)
    cleaned = re.sub(r"(?i)^\s*that\s+", "", cleaned).strip(" ,.-")
    normalized = _normalize_generated_sentence(cleaned, max_chars=220) if cleaned else ""
    return normalized, (normalized != original)


def _strip_publisher_suffix(text: str) -> str:
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return ""
    cleaned = re.sub(
        r"\s*(?:[-|]\s*)?(?:yahoo finance|reuters|bloomberg|cnbc|marketwatch|barrons?)\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return _normalize_whitespace(cleaned)


def _extract_causal_clause(text: str) -> str | None:
    cleaned = _strip_publisher_suffix(_strip_non_md_artifacts(text))
    if not cleaned:
        return None
    chunks = [_normalize_whitespace(x) for x in re.split(r"[.;:]", cleaned) if _normalize_whitespace(x)]
    clauses = chunks or [cleaned]
    for clause in clauses:
        lower = f" {clause.lower()} "
        if any(marker in lower for marker in (" after ", " amid ", " as ", " following ", " due to ", " because ", " on ")):
            if _has_action_verb(clause) or ("shares" in lower) or ("stock" in lower):
                return _shorten(clause.strip(), _reason_polish_max_chars()).rstrip(" .")
    for clause in clauses:
        if _has_event_vocab(clause) and _has_action_verb(clause):
            return _shorten(clause.strip(), _reason_polish_max_chars()).rstrip(" .")
    for clause in clauses:
        if _has_event_vocab(clause):
            return _shorten(clause.strip(), _reason_polish_max_chars()).rstrip(" .")
    if _has_event_vocab(cleaned) and _has_action_verb(cleaned):
        return _shorten(cleaned, _reason_polish_max_chars()).rstrip(" .")
    if _has_event_vocab(cleaned):
        return _shorten(cleaned, _reason_polish_max_chars()).rstrip(" .")
    return None


def _reason_phrase_quality_rejections(phrase: str) -> list[str]:
    cleaned = _normalize_whitespace(phrase)
    if not cleaned:
        return ["empty"]

    out: list[str] = []
    lower = cleaned.lower()
    words = re.findall(r"[A-Za-z0-9']+", cleaned)
    max_chars = _reason_polish_max_chars()

    if _is_quote_directory_title(cleaned):
        out.append("quote_directory_title")
    if re.search(r"\b(?:menu|watchlist|overview|historical data)\b", lower):
        out.append("metadata_phrase")
    if re.search(r"\b(?:nasdaq|nyse)\s*[:)]", lower):
        out.append("ticker_metadata")
    if len(words) < 4:
        out.append("too_short")
    if len(cleaned) > max_chars:
        out.append("too_long")
    if re.search(r"\b(?:and|or|but|with|to|for|on|as|after|amid|because|following|due)\s*$", lower):
        out.append("dangling_ending")
    if (not _has_action_verb(cleaned)) and (not _has_event_vocab(cleaned)):
        out.append("no_action_verb")
    if not (_has_causal_marker(cleaned) or _has_event_vocab(cleaned)):
        out.append("no_causal_signal")

    uniq: list[str] = []
    seen: set[str] = set()
    for reason in out:
        if reason in seen:
            continue
        seen.add(reason)
        uniq.append(reason)
    return uniq


def _is_reason_phrase_acceptable(phrase: str) -> bool:
    return len(_reason_phrase_quality_rejections(phrase)) == 0


def _lexical_overlap_ratio(*, raw_phrase: str, candidate: str) -> float:
    stop = {"the", "and", "for", "with", "after", "amid", "because", "following", "shares", "stock"}
    raw_tokens = {t for t in re.findall(r"[a-z0-9]{3,}", raw_phrase.lower()) if t not in stop}
    cand_tokens = {t for t in re.findall(r"[a-z0-9]{3,}", candidate.lower()) if t not in stop}
    if not raw_tokens or not cand_tokens:
        return 0.0
    return len(raw_tokens & cand_tokens) / float(len(raw_tokens))


def _has_entity_drift(*, raw_phrase: str, candidate: str) -> bool:
    raw_tickers = {x.upper() for x in re.findall(r"\b[A-Z]{2,8}\b", raw_phrase)}
    cand_tickers = {x.upper() for x in re.findall(r"\b[A-Z]{2,8}\b", candidate)}
    if cand_tickers - raw_tickers:
        return True
    raw_numbers = set(re.findall(r"\d+(?:\.\d+)?%?", raw_phrase))
    cand_numbers = set(re.findall(r"\d+(?:\.\d+)?%?", candidate))
    if cand_numbers - raw_numbers:
        return True
    raw_entities = {x.lower() for x in re.findall(r"\b[A-Z][a-z]{2,}\b", raw_phrase)}
    cand_entities = {x.lower() for x in re.findall(r"\b[A-Z][a-z]{2,}\b", candidate)}
    ignore = {"shares", "stock"}
    if (cand_entities - ignore) - (raw_entities - ignore):
        return True
    return False


def _polish_reason_phrase_llm(*, raw_phrase: str, evidence_text: str, ticker: str) -> str | None:
    if _reason_quality_mode() != "hybrid":
        return None
    if not _reason_polish_enabled():
        return None
    client = _openai_client()
    if client is None:
        return None

    max_chars = _reason_polish_max_chars()
    prompt = (
        "Rewrite the source phrase into one concise, grammatical causal phrase for a stock move.\n"
        "Rules:\n"
        f"- Max {max_chars} characters.\n"
        "- Preserve meaning exactly; do not add new facts, entities, or numbers.\n"
        "- Do not include publisher names, menus, quote-page language, or links.\n"
        "- Return exactly one plain phrase (no bullets, no markdown).\n\n"
        f"Ticker: {ticker}\n"
        f"Source phrase: {raw_phrase}\n"
        f"Evidence context: {evidence_text}\n"
    )
    try:
        response = client.chat.completions.create(
            model=_reason_polish_model(),
            messages=[
                {"role": "system", "content": "You rewrite market catalyst phrases with strict factual faithfulness."},
                {"role": "user", "content": prompt},
            ],
        )
        text = str(response.choices[0].message.content or "").strip()  # type: ignore[index]
    except Exception:
        logger.exception("reason-phrase polish failed for %s", ticker)
        return None

    polished = _normalize_whitespace(text.lstrip("-*• ").strip()).strip(" .")
    if polished.lower().startswith("shares "):
        polished = re.sub(
            r"(?i)^shares\s+(?:rose|fell|moved)\s+(?:after|amid|as|due to|because|following|on)\s+",
            "",
            polished,
        ).strip(" .")
    polished = _shorten(polished, max_chars).rstrip(" .")
    if not polished:
        return None
    if not _is_reason_phrase_acceptable(polished):
        return None
    if _lexical_overlap_ratio(raw_phrase=raw_phrase, candidate=polished) < 0.35:
        return None
    if _has_entity_drift(raw_phrase=raw_phrase, candidate=polished):
        return None
    if not (_has_causal_marker(polished) or _has_event_vocab(polished)):
        return None
    return polished


def _render_reason_line_with_quality(
    *,
    ticker: str,
    pct_move: float | None,
    candidate_phrase: str | None,
    evidence_text: str | None,
) -> tuple[str, str | None, str | None, str, tuple[str, ...]]:
    phrase_seed = _normalize_whitespace(candidate_phrase or "")
    raw_phrase = _extract_causal_clause(phrase_seed) if phrase_seed else None
    if raw_phrase is None and evidence_text:
        raw_phrase = _extract_causal_clause(evidence_text)
    if not raw_phrase:
        return (
            FALLBACK_CAUSE_LINE,
            None,
            None,
            "fallback",
            ("no_causal_clause",),
        )

    rejections = _reason_phrase_quality_rejections(raw_phrase)
    final_phrase = raw_phrase
    render_mode = "deterministic"
    if rejections:
        if _reason_quality_mode() == "hybrid":
            polished = _polish_reason_phrase_llm(
                raw_phrase=raw_phrase,
                evidence_text=_normalize_whitespace(evidence_text or phrase_seed or raw_phrase),
                ticker=ticker,
            )
            if polished:
                final_phrase = polished
                render_mode = "llm_polish"
            else:
                return (
                    FALLBACK_CAUSE_LINE,
                    raw_phrase,
                    None,
                    "fallback",
                    tuple(list(rejections) + ["llm_polish_unusable"]),
                )
        else:
            return (
                FALLBACK_CAUSE_LINE,
                raw_phrase,
                None,
                "fallback",
                tuple(rejections),
            )

    line = _build_reason_line_from_phrase(pct_move=pct_move, phrase=final_phrase)
    if line == FALLBACK_CAUSE_LINE:
        rejection_list = list(rejections)
        rejection_list.append("line_builder_rejected")
        return (
            FALLBACK_CAUSE_LINE,
            raw_phrase,
            final_phrase,
            "fallback",
            tuple(rejection_list),
        )
    return line, raw_phrase, final_phrase, render_mode, tuple(rejections)


def _collect_evidence_for_ticker(
    *,
    ticker: str,
    aliases: list[str],
    since_utc: datetime,
    pct_move: float | None = None,
) -> tuple[list[_EvidenceCandidate], list[str], str | None]:
    rejected: list[str] = []
    yahoo_candidates = _fetch_yahoo_news_candidates(ticker=ticker, aliases=aliases, since_utc=since_utc)
    if not yahoo_candidates:
        rejected.append("yahoo:no_recent_relevant_headlines")

    combined = _normalize_evidence_candidates(candidates=yahoo_candidates, ticker=ticker, aliases=aliases)
    baseline_best = max((c.score for c in combined), default=0.0)
    source_diversity = len({c.source_type for c in combined})
    directional_hits = any(_directional_bonus(text=c.text, pct_move=pct_move) > 0 for c in combined)
    needs_web = (baseline_best < _min_evidence_confidence()) or (source_diversity < 2) or (not directional_hits)
    web_backend: str | None = None
    if needs_web:
        web_candidates, web_backend, web_notes = _fetch_web_evidence(
            ticker=ticker,
            aliases=aliases,
            since_utc=since_utc,
            pct_move=pct_move,
        )
        rejected.extend(web_notes)
        if web_candidates:
            combined = _normalize_evidence_candidates(candidates=(combined + web_candidates), ticker=ticker, aliases=aliases)
            rejected.append(f"web:backend={web_backend}")
        else:
            rejected.append("web:no_relevant_results")
    allowed_sources = _md_allowed_evidence_sources()
    filtered = [c for c in combined if c.source_type in allowed_sources]
    filtered = sorted(
        filtered,
        key=lambda c: (
            -_effective_candidate_score(candidate=c, pct_move=pct_move),
            _source_rank(c.source_type),
        ),
    )
    filtered, time_notes = _enforce_time_integrity(candidates=filtered, since_utc=since_utc, pct_move=pct_move)
    rejected.extend(time_notes)
    if combined and (not filtered):
        rejected.append("all_sources:filtered_to_empty")
    if not filtered:
        rejected.append("all_sources:empty")
    return (
        sorted(filtered, key=lambda c: (-_effective_candidate_score(candidate=c, pct_move=pct_move), _source_rank(c.source_type))),
        rejected,
        web_backend,
    )


def _candidate_debug_entry(*, item: _EvidenceCandidate, pct_move: float | None) -> str:
    return _shorten(
        f"{item.source_type}({_effective_candidate_score(candidate=item, pct_move=pct_move):.2f}): {item.text} [{item.url or 'no-url'}]",
        180,
    )


def _candidate_publish_debug(item: _EvidenceCandidate) -> str:
    ts = item.published_at_utc.isoformat() if item.published_at_utc is not None else "none"
    conf = item.published_confidence or "none"
    src = item.published_source or "none"
    return _shorten(f"{item.source_type}: {ts} ({conf}:{src}) [{item.url or 'no-url'}]", 220)


def _split_time_integrity_notes(notes: list[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    publish: list[str] = []
    historical: list[str] = []
    for note in notes:
        if note.startswith("publish_time_reject:"):
            publish.append(note)
        elif note.startswith("historical_callback_reject:"):
            historical.append(note)
    return tuple(publish), tuple(historical)


def _is_technical_analysis_style(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return False
    ta_terms = (
        "price forecast",
        "breakout",
        "support",
        "resistance",
        "rsi",
        "macd",
        "chart pattern",
        "bulls defend support",
        "technical analysis",
        "moving average",
        "candlestick",
    )
    return any(term in lower for term in ta_terms)


def _is_price_action_only_text(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return False
    price_terms = (
        "intraday high",
        "intraday low",
        "midday trading",
        "buying momentum",
        "selling pressure",
        "traded higher",
        "traded lower",
        "surging in midday",
        "gained in the session",
        "fell in the session",
    )
    if not any(term in lower for term in price_terms):
        return False
    # If we also have a clear event catalyst, do not classify as price-action-only.
    if _has_strict_causal_marker(lower) or _has_event_vocab(lower):
        return False
    if any(term in lower for term in ("upgrade", "downgrade", "price target", "guidance", "earnings", "deal")):
        return False
    return True


def _is_multi_ticker_roundup(*, text: str, ticker: str, aliases: list[str]) -> bool:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return False
    roundup_terms = (
        "top analyst calls",
        "upgraded:",
        "downgraded:",
        "stocks to watch",
        "best stocks",
        "market movers today",
    )
    has_roundup = any(term in lower for term in roundup_terms)
    if has_roundup:
        return True
    tokens = re.findall(r"\b[A-Z]{2,5}\b", text)
    if len({t for t in tokens if t != ticker.upper()}) >= 2:
        return True
    company_mentions = sum(1 for alias in aliases if alias and alias.lower() in lower)
    if company_mentions == 0:
        comma_chunks = [x.strip() for x in lower.split(",") if x.strip()]
        if len(comma_chunks) >= 3:
            return True
    return False


def _has_explainer_today_pattern(text: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return False
    patterns = (
        "why is",
        "why did",
        "stock is up today",
        "stock is down today",
        "shares are up today",
        "shares are down today",
        "trading higher today",
        "trading lower today",
    )
    return any(p in lower for p in patterns)


def _is_deterministic_causal_candidate(item: _EvidenceCandidate, *, pct_move: float | None) -> bool:
    text = _normalize_whitespace(item.text)
    if not text:
        return False
    if item.reject_reason == "generic_wrapper":
        return False
    if _is_quote_directory_wrapper(text=text, url=item.url):
        return False
    if _contains_disallowed_reason_phrasing(text):
        return False
    if _is_technical_analysis_style(text):
        return False
    if _is_price_action_only_text(text):
        return False
    if _effective_candidate_score(candidate=item, pct_move=pct_move) < _min_evidence_confidence():
        return False
    if _has_causal_marker(text):
        return True
    if _has_explainer_today_pattern(text):
        return True
    return False


def _apply_synth_domain_gate(*, candidates: list[_EvidenceCandidate], max_results: int) -> list[_EvidenceCandidate]:
    gate = _synth_domain_gate()
    if gate == "off":
        return candidates[:max_results]
    quality = [c for c in candidates if _is_quality_domain(c.domain or c.url)]
    non_quality = [c for c in candidates if not _is_quality_domain(c.domain or c.url)]
    if gate == "quality_only":
        return quality[:max_results]
    if len(quality) >= 2:
        return quality[:max_results]
    out = quality + non_quality
    return out[:max_results]


def _collect_synthesis_candidates(
    *,
    ticker: str,
    aliases: list[str],
    since_utc: datetime,
    pct_move: float | None = None,
) -> tuple[list[_EvidenceCandidate], list[_EvidenceCandidate], list[str], str | None]:
    notes: list[str] = []
    source_mode = _synth_source_mode()
    merged: list[_EvidenceCandidate] = []
    web_backend: str | None = None

    if source_mode in {"google_plus_yahoo", "yahoo_only"}:
        yahoo = _fetch_yahoo_news_candidates(ticker=ticker, aliases=aliases, since_utc=since_utc)
        if not yahoo:
            notes.append("yahoo:no_recent_relevant_headlines")
        merged.extend(yahoo)

    if source_mode in {"google_plus_yahoo", "google_only"}:
        if not _google_serp_api_key():
            notes.append("web:google_serp_required_missing")
        else:
            web_rows, backend, web_notes = _fetch_web_evidence(
                ticker=ticker,
                aliases=aliases,
                since_utc=since_utc,
                pct_move=pct_move,
            )
            web_backend = backend
            notes.extend(web_notes)
            if web_rows:
                notes.append(f"web:backend={backend}")
            else:
                notes.append("web:no_relevant_results")
            merged.extend(web_rows)

    normalized = _normalize_evidence_candidates(candidates=merged, ticker=ticker, aliases=aliases)
    allowed = _md_allowed_evidence_sources()
    normalized = [c for c in normalized if c.source_type in allowed]
    normalized = sorted(
        normalized,
        key=lambda c: (
            -_effective_candidate_score(candidate=c, pct_move=pct_move),
            _source_rank(c.source_type),
        ),
    )
    normalized, time_notes = _enforce_time_integrity(candidates=normalized, since_utc=since_utc, pct_move=pct_move)
    notes.extend(time_notes)
    ranked = normalized
    usable = [
        c
        for c in ranked
        if c.reject_reason != "generic_wrapper" and (not _is_quote_directory_wrapper(text=c.text, url=c.url))
    ]
    if ranked and (not usable):
        notes.append("all_candidates:rejected_as_wrappers")
    gated = _apply_synth_domain_gate(candidates=usable, max_results=_synth_max_results())
    return ranked, gated, notes, web_backend


def _is_explainer_headline_for_ticker(*, text: str, ticker: str) -> bool:
    lower = _normalize_whitespace(text).lower()
    if not lower:
        return False
    t = ticker.lower()
    patterns = (
        f"why is {t}",
        f"why {t}",
        f"{t} stock soaring today",
        f"{t} stock up today",
        f"{t} stock down today",
    )
    return any(p in lower for p in patterns) or _has_explainer_today_pattern(lower)


def _pick_anchor_candidate(*, candidates: list[_EvidenceCandidate], ticker: str, pct_move: float | None) -> _EvidenceCandidate | None:
    if not candidates:
        return None

    def _rank(item: _EvidenceCandidate) -> tuple[float, float]:
        score = _effective_candidate_score(candidate=item, pct_move=pct_move)
        headline = _normalize_whitespace(item.text)
        context = _normalize_whitespace(f"{headline}. {item.context_text}") if item.context_text else headline
        if _is_price_action_only_text(context):
            score -= 0.28
        if _is_explainer_headline_for_ticker(text=headline, ticker=ticker) or _is_explainer_headline_for_ticker(text=context, ticker=ticker):
            score += 0.24
        if any(term in context.lower() for term in ("upgrade", "downgrade", "price target", "rating")):
            score += 0.14
        if _has_strict_causal_marker(context):
            score += 0.12
        if _has_event_vocab(context):
            score += 0.05
        ts = item.published_at_utc.timestamp() if item.published_at_utc else 0.0
        return (score, ts)

    return sorted(candidates, key=lambda item: _rank(item), reverse=True)[0]


def _candidate_event_family(item: _EvidenceCandidate) -> str:
    headline = _normalize_whitespace(item.text)
    context = _normalize_whitespace(f"{headline}. {item.context_text}") if item.context_text else headline
    family = _event_family(context)
    if family == "other":
        family = _event_family(headline)
    return family


def _pick_consensus_winner(
    *,
    candidates: list[_EvidenceCandidate],
    ticker: str,
    pct_move: float | None,
    top_k: int,
) -> tuple[_EvidenceCandidate | None, str | None]:
    if not candidates:
        return None, None

    window = candidates[: max(1, top_k)]
    buckets: dict[str, dict[str, float]] = {}
    for item in window:
        family = _candidate_event_family(item)
        if family == "other":
            continue
        score = _effective_candidate_score(candidate=item, pct_move=pct_move)
        ts = item.published_at_utc.timestamp() if item.published_at_utc else 0.0
        slot = buckets.setdefault(family, {"count": 0.0, "score_sum": 0.0, "newest_ts": 0.0})
        slot["count"] += 1.0
        slot["score_sum"] += score
        slot["newest_ts"] = max(slot["newest_ts"], ts)

    if not buckets:
        return None, None

    winner_family, winner_stats = sorted(
        buckets.items(),
        key=lambda kv: (-kv[1]["count"], -kv[1]["score_sum"], -kv[1]["newest_ts"], kv[0]),
    )[0]
    if int(winner_stats["count"]) < 2:
        return None, None

    members = [item for item in window if _candidate_event_family(item) == winner_family]
    if not members:
        return None, None
    winner = sorted(
        members,
        key=lambda item: (
            -_effective_candidate_score(candidate=item, pct_move=pct_move),
            -(item.published_at_utc.timestamp() if item.published_at_utc else 0.0),
        ),
    )[0]
    return winner, winner_family


def _pick_support_candidates(
    *,
    candidates: list[_EvidenceCandidate],
    anchor: _EvidenceCandidate | None,
    max_support: int,
    pct_move: float | None,
) -> list[_EvidenceCandidate]:
    if not candidates or anchor is None or max_support <= 0:
        return []
    anchor_key = anchor.canonical_url or anchor.url
    ranked = sorted(
        candidates,
        key=lambda c: (
            -_effective_candidate_score(candidate=c, pct_move=pct_move),
            -(c.published_at_utc.timestamp() if c.published_at_utc else 0.0),
        ),
    )
    out: list[_EvidenceCandidate] = []
    seen: set[str] = set()
    if anchor_key:
        seen.add(anchor_key)
    for item in ranked:
        key = item.canonical_url or item.url or f"{item.source_type}:{item.text}"
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= max_support:
            break
    return out


def _filter_support_candidates_by_family(
    *,
    supports: list[_EvidenceCandidate],
    family: str | None,
) -> list[_EvidenceCandidate]:
    if not supports:
        return []
    if (not family) or family == "other":
        return supports
    return [item for item in supports if _candidate_event_family(item) == family]


def _sentence_from_anchor_candidate(*, ticker: str, anchor: _EvidenceCandidate | None, pct_move: float | None) -> str | None:
    if anchor is None:
        return None
    text = _normalize_whitespace(anchor.context_text or anchor.text)
    if not text:
        return None
    clauses = [_normalize_whitespace(x) for x in re.split(r"(?<=[.!?])\s+", text) if _normalize_whitespace(x)]
    if not clauses:
        return None
    chosen = clauses[0]
    for clause in clauses:
        if _has_strict_causal_marker(clause):
            chosen = clause
            break
    chosen = _normalize_whitespace(chosen).strip(" .")
    if not chosen:
        return None
    # If anchor is a "why is <ticker>" headline, prefer causal detail from the remaining context.
    if _is_explainer_headline_for_ticker(text=chosen, ticker=ticker):
        for clause in clauses[1:]:
            if _has_causal_marker(clause) or _has_event_vocab(clause):
                chosen = clause.strip(" .")
                break
    chosen = re.sub(r"(?i)^(?:podcast|video|watch|live updates?)\s*:\s*", "", chosen).strip(" .")
    if not chosen:
        return None
    return _shorten(chosen, 220).rstrip(" .") + "."


def _normalize_generated_sentence(raw: str, *, max_chars: int = 220) -> str:
    sentence = _normalize_whitespace(str(raw or ""))
    if not sentence:
        return ""
    if len(sentence) > max_chars:
        sentence = _shorten(sentence, max_chars)
    return sentence.rstrip(" .") + "."


def _sanitize_synth_phrase(raw: str) -> str:
    phrase = _normalize_whitespace(str(raw or "")).strip().lstrip("-*• ").strip(" \"'`")
    phrase = re.sub(r"(?i)^(?:financialcontent|marketbeat|benzinga|yahoo finance|reuters|bloomberg)\s*[-:]\s*", "", phrase)
    phrase = re.sub(r"https?://\S+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\[[^\]]+\]\([^)]+\)", "", phrase)
    phrase = phrase.strip(" .")
    phrase = re.sub(
        r"(?i)^shares\s+(?:rose|fell|moved)\s+(?:after|amid|as|following|due to|because|on)\s+",
        "",
        phrase,
    ).strip(" .")
    return _shorten(phrase, 90).strip(" .")


def _synth_phrase_invalid_reason(*, phrase: str, evidence_text: str) -> str | None:
    if not phrase:
        return "empty_phrase"
    lower = phrase.lower()
    if "http://" in lower or "https://" in lower or "www." in lower:
        return "contains_link"
    if _is_quote_directory_title(phrase) or _contains_disallowed_reason_phrasing(phrase):
        return "wrapper_phrase"
    if _has_entity_drift(raw_phrase=evidence_text, candidate=phrase):
        return "entity_drift"
    if _lexical_overlap_ratio(raw_phrase=evidence_text, candidate=phrase) < 0.12:
        return "low_overlap"
    return None


def _best_guess_phrase_from_candidates(candidates: list[_EvidenceCandidate], *, pct_move: float | None = None) -> str | None:
    for item in candidates:
        if not _is_deterministic_causal_candidate(item, pct_move=pct_move):
            continue
        phrase = _extract_causal_clause(item.text)
        if phrase:
            return _sanitize_synth_phrase(phrase)
        fallback = _sanitize_synth_phrase(_strip_non_md_artifacts(item.text))
        if fallback:
            return fallback
    return None


def _synthesize_catalyst_phrase_simple(
    *,
    client: Any | None,
    ticker: str,
    pct_move: float | None,
    candidates: list[_EvidenceCandidate],
) -> tuple[str | None, str | None]:
    if not candidates:
        return None, "no_candidates"
    if client is None:
        return None, "llm_unavailable"

    evidence_lines: list[str] = []
    for idx, item in enumerate(candidates[: _synth_max_results()], start=1):
        evidence_lines.append(
            f"[S{idx}] src={item.source_type} score={_effective_candidate_score(candidate=item, pct_move=pct_move):.2f} title={item.text} url={item.url or 'n/a'}"
        )
    evidence_text = "\n".join(evidence_lines)
    move = _format_pct(pct_move)
    prompt = (
        "Write exactly one causal phrase for the stock move using only the provided evidence.\n"
        "Rules:\n"
        "- One phrase only, plain text.\n"
        "- Max 90 characters.\n"
        "- No links, no markdown, no publisher names, no menu/quote-directory language.\n"
        "- Do not add facts/entities/numbers not present in evidence.\n"
        "- Phrase should fit after the word 'after'.\n\n"
        f"Ticker: {ticker}\n"
        f"Move: {move}\n"
        "Evidence:\n"
        f"{evidence_text}\n"
    )
    try:
        response = client.chat.completions.create(
            model=_reason_polish_model(),
            messages=[
                {"role": "system", "content": "You write concise, factual catalyst phrases grounded in source evidence."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = str(response.choices[0].message.content or "").strip()  # type: ignore[index]
    except Exception:
        logger.exception("simple catalyst synthesis failed for %s", ticker)
        return None, "llm_exception"

    phrase = _sanitize_synth_phrase(raw)
    invalid = _synth_phrase_invalid_reason(phrase=phrase, evidence_text=" ".join(c.text for c in candidates))
    if invalid is not None:
        return None, f"llm_invalid_output:{invalid}"
    return phrase, None


def _render_simple_reason_line(*, pct_move: float | None, phrase: str | None) -> str:
    if _reason_output_mode() == "free_sentence":
        sentence = _normalize_generated_sentence(phrase or "", max_chars=220)
        if not sentence:
            return FALLBACK_CAUSE_LINE
        return sentence
    cleaned = _sanitize_synth_phrase(phrase or "")
    if not cleaned:
        return FALLBACK_CAUSE_LINE
    if pct_move is not None and pct_move < 0:
        line = f"Shares fell after {cleaned}."
    elif pct_move is not None and pct_move > 0:
        line = f"Shares rose after {cleaned}."
    else:
        line = f"Shares moved after {cleaned}."
    return _shorten(line, 110).rstrip(" .") + "."


def _synthesize_catalyst_sentence_simple(
    *,
    client: Any | None,
    ticker: str,
    pct_move: float | None,
    anchor: _EvidenceCandidate | None,
    supports: list[_EvidenceCandidate],
) -> tuple[str | None, str | None]:
    if anchor is None:
        return None, "no_candidates"
    if client is None:
        return None, "llm_unavailable"

    pack: list[_EvidenceCandidate] = [anchor] + supports
    evidence_lines: list[str] = []
    for idx, item in enumerate(pack, start=1):
        tag = "A1" if idx == 1 else f"S{idx}"
        context = _shorten(_normalize_whitespace(item.context_text or item.text), 360)
        ts = item.published_at_utc.isoformat() if item.published_at_utc else "unknown"
        evidence_lines.append(
            f"[{tag}] src={item.source_type} ts={ts} score={_effective_candidate_score(candidate=item, pct_move=pct_move):.2f} text={context} url={item.url or 'n/a'}"
        )
    evidence_text = "\n".join(evidence_lines)
    move = _format_pct(pct_move)
    prompt = (
        "Write exactly one complete sentence explaining this stock move.\n"
        "Use only the evidence below.\n"
        "Prioritize the consensus event from [A1] and keep support entries only when they corroborate that same event.\n"
        "Do not mention publisher names or attribution (Reuters/Bloomberg/WSJ/CNBC/MarketWatch/Yahoo Finance).\n"
        "Do not use phrases like 'according to' or 'report said'.\n"
        "No markdown and no links.\n\n"
        f"Ticker: {ticker}\n"
        f"Move: {move}\n"
        f"Evidence:\n{evidence_text}\n"
    )
    try:
        response = client.chat.completions.create(
            model=_reason_polish_model(),
            messages=[
                {"role": "system", "content": "You write concise, factual market move explanations grounded in sources."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = str(response.choices[0].message.content or "").strip()  # type: ignore[index]
    except Exception:
        logger.exception("simple catalyst sentence synthesis failed for %s", ticker)
        return None, "llm_exception"
    if not raw:
        return None, "llm_empty"
    if _md_post_as_is():
        return _normalize_generated_sentence(raw, max_chars=220), None
    cleaned = _normalize_generated_sentence(raw, max_chars=220)
    return (cleaned if cleaned else None), ("llm_empty_after_normalize" if not cleaned else None)


def _extract_json_object(text: str) -> str | None:
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    return text[start : end + 1]


def _select_anchor_support_llm(
    *,
    client: Any | None,
    ticker: str,
    pct_move: float | None,
    candidates: list[_EvidenceCandidate],
    max_support: int,
) -> tuple[_EvidenceCandidate | None, list[_EvidenceCandidate], str | None]:
    if client is None:
        return None, [], "llm_unavailable"
    if not candidates:
        return None, [], "no_candidates"

    evidence_lines: list[str] = []
    for idx, item in enumerate(candidates[:8], start=1):
        context = _shorten(_normalize_whitespace(item.context_text or item.text), 260)
        ts = item.published_at_utc.isoformat() if item.published_at_utc else "unknown"
        evidence_lines.append(
            f"[C{idx}] src={item.source_type} score={_effective_candidate_score(candidate=item, pct_move=pct_move):.2f} ts={ts} text={context} url={item.url or 'n/a'}"
        )
    evidence_block = "\n".join(evidence_lines)
    prompt = (
        "Pick the best primary catalyst source and up to support sources for this stock move.\n"
        "Prioritize source relevance to WHY the stock moved (not generic price-action description).\n"
        "Prefer concrete causal explainers (earnings/guidance/analyst upgrade-downgrade/deal/product/regulatory) over intraday momentum phrasing.\n"
        "Use only candidate IDs that exist.\n"
        "Return strict JSON only with this shape:\n"
        '{"anchor":"C1","supports":["C2","C3"]}\n'
        f"Ticker: {ticker}\n"
        f"Move: {_format_pct(pct_move)}\n"
        f"Candidates:\n{evidence_block}\n"
    )
    try:
        response = client.chat.completions.create(
            model=_reason_polish_model(),
            messages=[
                {"role": "system", "content": "You select the most causally relevant source for market-move attribution."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = str(response.choices[0].message.content or "").strip()  # type: ignore[index]
    except Exception:
        logger.exception("llm relevance selection failed for %s", ticker)
        return None, [], "llm_relevance_exception"

    obj_text = _extract_json_object(raw)
    if not obj_text:
        return None, [], "llm_relevance_invalid_json"
    try:
        payload = json.loads(obj_text)
    except Exception:
        return None, [], "llm_relevance_invalid_json"
    if not isinstance(payload, dict):
        return None, [], "llm_relevance_invalid_shape"

    index: dict[str, _EvidenceCandidate] = {f"C{i}": c for i, c in enumerate(candidates[:8], start=1)}
    anchor_key = str(payload.get("anchor") or "").strip().upper()
    anchor = index.get(anchor_key)
    if anchor is None:
        return None, [], "llm_relevance_missing_anchor"

    raw_supports = payload.get("supports")
    supports: list[_EvidenceCandidate] = []
    seen: set[str] = {anchor_key}
    if isinstance(raw_supports, list):
        for val in raw_supports:
            key = str(val or "").strip().upper()
            if key in seen:
                continue
            item = index.get(key)
            if item is None:
                continue
            seen.add(key)
            supports.append(item)
            if len(supports) >= max(0, max_support):
                break
    return anchor, supports, None


def _preferred_evidence_text(evidence: CatalystEvidence) -> str:
    if evidence.chosen_source == "web" and evidence.web_title:
        return evidence.web_title
    if evidence.chosen_source == "yahoo_news" and evidence.news_title:
        return evidence.news_title
    return evidence.news_title or evidence.web_title or ""


def _summarize_catalyst(*, ticker: str, slot_name: str, evidence: CatalystEvidence) -> str:
    if _reason_mode() != "best_effort":
        return FALLBACK_CAUSE_LINE
    if evidence.confirmed_cause_phrase and evidence.confidence >= _min_evidence_confidence():
        return _ensure_reason_like_line(
            _build_reason_line_from_phrase(pct_move=None, phrase=evidence.confirmed_cause_phrase),
            evidence=evidence,
        )
    return FALLBACK_CAUSE_LINE


def _strip_non_md_artifacts(text: str) -> str:
    cleaned = _normalize_whitespace(text)
    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", " ", cleaned)
    cleaned = re.sub(r"(^|\s)#[A-Za-z0-9_]+\b", " ", cleaned)
    cleaned = re.sub(r"(^|\s)@[A-Za-z0-9_]+\b", " ", cleaned)
    cleaned = re.sub(r"(^|\s)\$[A-Za-z]{1,6}\b", " ", cleaned)
    cleaned = re.sub(r"\bBREAKING:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,:;|")
    return cleaned


def _ensure_reason_like_line(text: str, *, evidence: CatalystEvidence) -> str:
    cleaned = _strip_non_md_artifacts(text)
    cleaned = re.split(r"[!?;]", cleaned, maxsplit=1)[0].strip()
    if not cleaned:
        return FALLBACK_CAUSE_LINE
    if _contains_disallowed_reason_phrasing(cleaned):
        return FALLBACK_CAUSE_LINE

    lower = cleaned.lower()
    if "no single confirmed catalyst" in lower:
        return FALLBACK_CAUSE_LINE
    if "no clear single catalyst" in lower:
        return FALLBACK_CAUSE_LINE

    causal_markers = (" after ", " on ", " as ", " amid ", " due to ", " because ", " following ")
    if not any(marker in f" {lower} " for marker in causal_markers):
        return FALLBACK_CAUSE_LINE

    cleaned = _shorten(cleaned, 110)
    if _contains_disallowed_reason_phrasing(cleaned):
        return FALLBACK_CAUSE_LINE
    return cleaned.rstrip(" .") + "."


def _looks_like_specific_catalyst(text: str) -> bool:
    if not text:
        return False
    upper = text.upper()
    generic_markers = (
        "UNDER-THE-RADAR",
        "COULD SIGNAL",
        "NEW TREND",
        "TOP ",
        "BEST ",
        "WATCHLIST",
        "IDEAS",
    )
    if any(marker in upper for marker in generic_markers):
        return False
    markers = (
        "EARNINGS",
        "GUIDANCE",
        "DEAL",
        "CONTRACT",
        "UPGRADE",
        "DOWNGRADE",
        "FORECAST",
        "REVENUE",
        "MARGIN",
        "ACQUISITION",
        "BUYBACK",
        "LAYOFF",
        "APPROVAL",
        "REGULATORY",
    )
    return any(marker in upper for marker in markers)


def _shorten(text: str, limit: int) -> str:
    cleaned = _normalize_whitespace(text)
    cleaned = cleaned.replace("...", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= limit:
        return cleaned
    words = cleaned.split(" ")
    out: list[str] = []
    for word in words:
        candidate = (" ".join(out + [word])).strip()
        if len(candidate) <= limit:
            out.append(word)
            continue
        break
    return (" ".join(out) if out else cleaned[:limit]).strip(" .")


def _format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    pct = value * 100.0
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _resolve_channel_id(client: Any, channel_ref: str) -> str | None:
    ref = str(channel_ref or "").strip()
    if not ref:
        return None
    if re.fullmatch(r"[CGD][A-Z0-9]{8,}", ref):
        return ref
    target = ref.lstrip("#").strip().lower()
    cursor: str | None = None
    while True:
        try:
            payload = client.conversations_list(
                types="public_channel,private_channel",
                exclude_archived=True,
                limit=500,
                cursor=cursor,
            )
        except SlackApiError as exc:
            err = str(exc.response.get("error") or "")
            if err == "missing_scope":
                return target
            return None
        channels = payload.get("channels") if isinstance(payload, dict) else None
        for item in channels if isinstance(channels, list) else []:
            name = str(item.get("name") or "").strip().lower()
            if name == target:
                cid = str(item.get("id") or "").strip()
                if cid:
                    return cid
        meta = payload.get("response_metadata") if isinstance(payload, dict) else None
        next_cursor = str((meta or {}).get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
        if not next_cursor:
            break
        cursor = next_cursor
    return None


def _build_message(
    *,
    slot_name: str,
    now_local: datetime,
    universe_count: int,
    movers: list[QuoteSnapshot],
    catalyst_rows: list[CatalystEvidence],
    catalyst_lines: list[str],
    earnings_after_close: list[EarningsPreviewItem] | None = None,
) -> str:
    slot_title = "MD — Market Open" if slot_name == "open" else "MD — Market Close+"
    as_of = now_local.strftime("%-I:%M %p PT")
    period_label = "morning" if slot_name == "open" else "afternoon"
    lines = [f"*{slot_title} | As of {as_of}*"]
    lines.append(f"3 biggest movers this {period_label}:")

    for idx, mover in enumerate(movers):
        ev = (
            catalyst_rows[idx]
            if idx < len(catalyst_rows)
            else CatalystEvidence(ticker=mover.ticker, x_text=None, x_url=None, x_engagement=0, news_title=None, news_url=None)
        )
        cat = catalyst_lines[idx] if idx < len(catalyst_lines) else FALLBACK_CAUSE_LINE
        if ev.cause_mode == "simple_synthesis" and _reason_output_mode() == "free_sentence":
            cat = _normalize_generated_sentence(cat, max_chars=220) or FALLBACK_CAUSE_LINE
        else:
            cat = _ensure_reason_like_line(cat, evidence=ev)
        links = _build_links_for_mover(ev=ev, cat_line=cat)
        link_text = f" {' '.join(links)}" if links else ""
        emoji = "📈" if (mover.pct_move or 0.0) >= 0 else "📉"
        lines.append(f"- {emoji} {mover.ticker} {_format_pct(mover.pct_move)} — {cat}{link_text}")

    if slot_name == "open" and earnings_after_close:
        lines.append("")
        lines.append("Earnings After Close Today:")
        for item in earnings_after_close:
            lines.append(f"- {item.ticker} ({item.company}) — {_session_human_label(item.expected_session)}")

    lines.append(f"Data UTC: {_utc_now_iso()} | Sources: Yahoo fast_info + Yahoo news + web search")
    return "\n".join(lines)


def _write_artifact(
    *,
    slot_name: str,
    now_local: datetime,
    universe: list[QuoteSnapshot],
    movers: list[QuoteSnapshot],
    catalyst_rows: list[CatalystEvidence],
    catalyst_lines: list[str],
    message_text: str,
    earnings_after_close: list[EarningsPreviewItem] | None = None,
) -> Path:
    out_dir = _artifact_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    path = out_dir / f"md-{slot_name}-{ts}.md"

    lines: list[str] = []
    lines.append(f"# MD - {'Market Open' if slot_name == 'open' else 'Market Close+'}")
    lines.append("")
    lines.append(f"Generated local: `{now_local.isoformat()}`")
    lines.append(f"Generated utc: `{_utc_now_iso()}`")
    lines.append("")
    lines.append("## Slack Post")
    lines.append("")
    lines.append(message_text)
    lines.append("")
    lines.append("## Top Movers")
    lines.append("")
    for idx, mover in enumerate(movers):
        ev = (
            catalyst_rows[idx]
            if idx < len(catalyst_rows)
            else CatalystEvidence(ticker=mover.ticker, x_text=None, x_url=None, x_engagement=0, news_title=None, news_url=None)
        )
        cat = catalyst_lines[idx] if idx < len(catalyst_lines) else ""
        lines.append(f"- `{mover.ticker}` pct_move `{_format_pct(mover.pct_move)}` market_cap `{mover.market_cap}`")
        lines.append(f"  - catalyst: {cat}")
        lines.append(f"  - confidence: {ev.confidence:.2f}")
        lines.append(f"  - since_utc: {ev.since_utc or 'n/a'}")
        if ev.chosen_source:
            lines.append(f"  - chosen_source: {ev.chosen_source}")
        if ev.web_backend:
            lines.append(f"  - web_backend: {ev.web_backend}")
        if ev.selected_cluster:
            lines.append(f"  - selected_cluster: {ev.selected_cluster}")
        if ev.cause_mode:
            lines.append(f"  - cause_mode: {ev.cause_mode}")
        if ev.cause_render_mode:
            lines.append(f"  - cause_render_mode: {ev.cause_render_mode}")
        if ev.cause_raw_phrase:
            lines.append(f"  - cause_raw_phrase: {ev.cause_raw_phrase}")
        if ev.cause_final_phrase:
            lines.append(f"  - cause_final_phrase: {ev.cause_final_phrase}")
        if ev.cause_anchor_url:
            lines.append(f"  - cause_anchor_url: {ev.cause_anchor_url}")
        if ev.cause_anchor_text:
            lines.append(f"  - cause_anchor_text: {ev.cause_anchor_text}")
        if ev.cause_support_urls:
            lines.append("  - cause_support_urls:")
            for entry in ev.cause_support_urls:
                lines.append(f"    - {entry}")
        if ev.generation_format:
            lines.append(f"  - generation_format: {ev.generation_format}")
        if ev.generation_policy:
            lines.append(f"  - generation_policy: {ev.generation_policy}")
        if ev.consensus_event_family:
            lines.append(f"  - consensus_event_family: {ev.consensus_event_family}")
        if ev.consensus_winner_url:
            lines.append(f"  - consensus_winner_url: {ev.consensus_winner_url}")
        if ev.attribution_stripped:
            lines.append("  - attribution_stripped: true")
        if ev.quality_rejections:
            lines.append(f"  - quality_rejections: {', '.join(ev.quality_rejections)}")
        if ev.synth_generation_mode:
            lines.append(f"  - synth_generation_mode: {ev.synth_generation_mode}")
        if ev.synth_model_used:
            lines.append(f"  - synth_model_used: {ev.synth_model_used}")
        if ev.synth_candidates_considered:
            lines.append("  - synth_candidates_considered:")
            for entry in ev.synth_candidates_considered:
                lines.append(f"    - {entry}")
        if ev.synth_candidates_used:
            lines.append("  - synth_candidates_used:")
            for entry in ev.synth_candidates_used:
                lines.append(f"    - {entry}")
        if ev.synth_chosen_urls:
            lines.append("  - synth_chosen_urls:")
            for url in ev.synth_chosen_urls:
                lines.append(f"    - {url}")
        if ev.time_integrity_mode:
            lines.append(f"  - time_integrity_mode: {ev.time_integrity_mode}")
        if ev.publish_time_rejections:
            lines.append("  - publish_time_rejections:")
            for entry in ev.publish_time_rejections:
                lines.append(f"    - {entry}")
        if ev.historical_callback_rejections:
            lines.append("  - historical_callback_rejections:")
            for entry in ev.historical_callback_rejections:
                lines.append(f"    - {entry}")
        if ev.candidate_publish_times:
            lines.append("  - candidate_publish_times:")
            for entry in ev.candidate_publish_times:
                lines.append(f"    - {entry}")
        if ev.cause_source_type:
            lines.append(f"  - cause_source_type: {ev.cause_source_type}")
        if ev.cause_source_url:
            lines.append(f"  - cause_source_url: {ev.cause_source_url}")
        if ev.driver_keywords:
            lines.append(f"  - driver_keywords: {', '.join(ev.driver_keywords)}")
        if ev.cluster_debug:
            lines.append("  - cluster_debug:")
            for entry in ev.cluster_debug:
                lines.append(f"    - {entry}")
        if ev.news_url:
            lines.append(f"  - news: {ev.news_url}")
        if ev.web_url:
            lines.append(f"  - web: {ev.web_url}")
        lines.append("  - evidence_considered:")
        if ev.top_evidence:
            for entry in ev.top_evidence:
                lines.append(f"    - {entry}")
        else:
            lines.append("    - none")
        if ev.rejected_reasons:
            lines.append(f"  - rejected: {', '.join(ev.rejected_reasons)}")

    if earnings_after_close:
        lines.append("")
        lines.append("## Earnings After Close Today")
        lines.append("")
        lines.append("Data source: `yfinance calendar + historical earnings timestamp inference`")
        lines.append("")
        for item in earnings_after_close:
            lines.append(
                f"- `{item.ticker}` `{item.company}` | earnings_date_et=`{item.earnings_date_et}` | expected_session=`{item.expected_session}`"
            )

    lines.append("")
    lines.append("## Final Universe")
    lines.append("")
    lines.append(f"Count: `{len(universe)}`")
    lines.append("")
    for item in universe:
        lines.append(f"- {item.ticker} | market_cap={item.market_cap} | pct_move={_format_pct(item.pct_move)}")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _post_to_slack(*, channel_ref: str, text: str) -> tuple[str | None, str | None]:
    if WebClient is None:
        raise MarketDailyError("slack_sdk unavailable")

    tokens = _slack_tokens()
    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            channel_id = _resolve_channel_id(client, channel_ref) or channel_ref
            resp = client.chat_postMessage(channel=channel_id, text=text)
            ts = str(resp.get("ts") or "") if isinstance(resp, dict) else ""
            return (channel_id, ts or None)
        except SlackApiError as exc:
            err = str(exc.response.get("error") or "")
            if err in {"invalid_auth", "account_inactive", "not_authed"}:
                last_error = err
                logger.warning("market_daily slack token rejected (%s), trying next token", err)
                continue
            raise MarketDailyError(f"Slack post failed: {err}") from exc
    raise MarketDailyError(f"Slack post failed after token fallback: {last_error or 'unknown'}")


def _build_catalyst_for_mover_simple(*, mover: QuoteSnapshot, slot_name: str, since_utc: datetime) -> tuple[CatalystEvidence, str]:
    aliases = _company_aliases(mover.ticker)
    considered, selected, rejected, web_backend = _collect_synthesis_candidates(
        ticker=mover.ticker,
        aliases=aliases,
        since_utc=since_utc,
        pct_move=mover.pct_move,
    )
    publish_time_rejections, historical_callback_rejections = _split_time_integrity_notes(rejected)
    candidate_publish_times = tuple(_candidate_publish_debug(item) for item in considered[:10])
    if not selected:
        rejected = list(rejected) + ["no_candidates"]
        evidence = CatalystEvidence(
            ticker=mover.ticker,
            x_text=None,
            x_url=None,
            x_engagement=0,
            news_title=None,
            news_url=None,
            web_title=None,
            web_url=None,
            confidence=0.0,
            chosen_source=None,
            top_evidence=tuple(_candidate_debug_entry(item=c, pct_move=mover.pct_move) for c in considered[:5]),
            rejected_reasons=tuple(rejected),
            since_utc=since_utc.replace(microsecond=0).isoformat(),
            web_backend=web_backend,
            cause_mode="simple_synthesis",
            cause_render_mode="fallback",
            generation_format="free_sentence",
            generation_policy=("post_as_is" if _md_post_as_is() else "normalized"),
            quality_rejections=("no_candidates",),
            synth_generation_mode="simple_synthesis",
            synth_model_used=(_reason_polish_model() if _openai_client() is not None else None),
            synth_candidates_considered=tuple(_candidate_debug_entry(item=c, pct_move=mover.pct_move) for c in considered[:5]),
            synth_candidates_used=(),
            synth_chosen_urls=(),
            time_integrity_mode="strict_in_window" if _require_in_window_dates() else "off",
            publish_time_rejections=publish_time_rejections,
            candidate_publish_times=candidate_publish_times,
            historical_callback_rejections=historical_callback_rejections,
        )
        return evidence, FALLBACK_CAUSE_LINE

    client = _openai_client()
    relevance_mode = _relevance_mode()
    llm_anchor: _EvidenceCandidate | None = None
    llm_supports: list[_EvidenceCandidate] = []
    if relevance_mode == "llm_first":
        llm_anchor, llm_supports, relevance_error = _select_anchor_support_llm(
            client=client,
            ticker=mover.ticker,
            pct_move=mover.pct_move,
            candidates=selected,
            max_support=_synth_support_count(),
        )
        if relevance_error:
            rejected.append(relevance_error)

    consensus_top_k = max(3, 1 + _synth_support_count())
    consensus_anchor, consensus_family = _pick_consensus_winner(
        candidates=selected,
        ticker=mover.ticker,
        pct_move=mover.pct_move,
        top_k=consensus_top_k,
    )
    if llm_anchor is not None:
        anchor = llm_anchor
        supports = llm_supports
        consensus_family = _candidate_event_family(anchor)
        rejected.append("anchor_selected_by_llm")
    else:
        anchor = consensus_anchor or _pick_anchor_candidate(candidates=selected, ticker=mover.ticker, pct_move=mover.pct_move) or selected[0]
        if consensus_anchor is None:
            rejected.append("consensus_not_established")
        consensus_family = consensus_family or _candidate_event_family(anchor)
        supports = _pick_support_candidates(
            candidates=selected,
            anchor=anchor,
            max_support=_synth_support_count(),
            pct_move=mover.pct_move,
        )
        supports = _filter_support_candidates_by_family(supports=supports, family=consensus_family)
    used = [anchor] + supports

    sentence, llm_error = _synthesize_catalyst_sentence_simple(
        client=client,
        ticker=mover.ticker,
        pct_move=mover.pct_move,
        anchor=anchor,
        supports=supports,
    )
    render_mode = "simple_llm"
    quality_rejections: list[str] = []
    attribution_stripped = False
    if llm_error:
        quality_rejections.append(llm_error)
        rejected.append(llm_error)

    if sentence:
        stripped_sentence, did_strip = _strip_publisher_attribution(sentence)
        if did_strip:
            rejected.append("publisher_attribution_stripped")
            attribution_stripped = True
        sentence = stripped_sentence or None

    if sentence and consensus_family and consensus_family != "other" and llm_anchor is None:
        sentence_family = _sentence_family(sentence)
        if sentence_family not in {consensus_family, "other"}:
            rejected.append(f"consensus_family_mismatch:{sentence_family}->{consensus_family}")
            quality_rejections.append("consensus_family_mismatch")
            sentence = _sentence_from_anchor_candidate(ticker=mover.ticker, anchor=anchor, pct_move=mover.pct_move)
            if sentence:
                stripped_sentence, did_strip = _strip_publisher_attribution(sentence)
                sentence = stripped_sentence or None
                attribution_stripped = attribution_stripped or did_strip
                render_mode = "simple_consensus_backup"
            else:
                quality_rejections.append("consensus_backup_failed")

    if not sentence:
        sentence = _sentence_from_anchor_candidate(ticker=mover.ticker, anchor=anchor, pct_move=mover.pct_move)
        if sentence:
            stripped_sentence, did_strip = _strip_publisher_attribution(sentence)
            sentence = stripped_sentence or None
            attribution_stripped = attribution_stripped or did_strip
            render_mode = "simple_anchor_backup"
            rejected.append("fallback_to_anchor_backup")
        else:
            quality_rejections.append("anchor_backup_failed")

    line = _render_simple_reason_line(pct_move=mover.pct_move, phrase=sentence)
    if line == FALLBACK_CAUSE_LINE:
        quality_rejections.append("line_builder_rejected")
        render_mode = "fallback"

    best_news = _pick_best_by_source(used, "yahoo_news", pct_move=mover.pct_move) or _pick_best_by_source(selected, "yahoo_news", pct_move=mover.pct_move)
    best_web = _pick_best_by_source(used, "web", pct_move=mover.pct_move) or _pick_best_by_source(selected, "web", pct_move=mover.pct_move)
    confidence = _effective_candidate_score(candidate=anchor, pct_move=mover.pct_move)
    evidence = CatalystEvidence(
        ticker=mover.ticker,
        x_text=None,
        x_url=None,
        x_engagement=0,
        news_title=best_news.text if best_news else None,
        news_url=best_news.url if best_news else None,
        web_title=best_web.text if best_web else None,
        web_url=best_web.url if best_web else None,
        confidence=confidence,
        chosen_source=anchor.source_type,
        top_evidence=tuple(_candidate_debug_entry(item=c, pct_move=mover.pct_move) for c in considered[:5]),
        rejected_reasons=tuple(rejected),
        since_utc=since_utc.replace(microsecond=0).isoformat(),
        web_backend=web_backend,
        cause_source_type=anchor.source_type,
        cause_source_url=anchor.url,
        cause_mode="simple_synthesis",
        cause_render_mode=render_mode,
        cause_raw_phrase=sentence,
        cause_final_phrase=(line if line != FALLBACK_CAUSE_LINE else None),
        cause_anchor_url=anchor.url,
        cause_anchor_text=_shorten(_normalize_whitespace(anchor.context_text or anchor.text), 220),
        cause_support_urls=tuple([c.url for c in supports if c.url]),
        consensus_event_family=consensus_family,
        consensus_winner_url=anchor.url,
        attribution_stripped=attribution_stripped,
        generation_format="free_sentence",
        generation_policy=("post_as_is" if _md_post_as_is() else "normalized"),
        confirmed_cause_phrase=(line if line != FALLBACK_CAUSE_LINE else None),
        quality_rejections=tuple(quality_rejections),
        synth_generation_mode=("simple_synthesis_llm_first" if llm_anchor is not None else "simple_synthesis"),
        synth_model_used=(_reason_polish_model() if client is not None else None),
        synth_candidates_considered=tuple(_candidate_debug_entry(item=c, pct_move=mover.pct_move) for c in considered[:5]),
        synth_candidates_used=tuple(_candidate_debug_entry(item=c, pct_move=mover.pct_move) for c in used[: 1 + _synth_support_count()]),
        synth_chosen_urls=tuple([c.url for c in used[: 1 + _synth_support_count()] if c.url]),
        time_integrity_mode="strict_in_window" if _require_in_window_dates() else "off",
        publish_time_rejections=publish_time_rejections,
        candidate_publish_times=candidate_publish_times,
        historical_callback_rejections=historical_callback_rejections,
    )
    return evidence, line


def _build_catalyst_for_mover_legacy(*, mover: QuoteSnapshot, slot_name: str, since_utc: datetime) -> tuple[CatalystEvidence, str]:
    aliases = _company_aliases(mover.ticker)
    collected = _collect_evidence_for_ticker(
        ticker=mover.ticker,
        aliases=aliases,
        since_utc=since_utc,
        pct_move=mover.pct_move,
    )
    if len(collected) == 3:
        candidates, rejected, web_backend = collected
    else:  # backwards-compatible for patched tests/mocks returning legacy shape
        candidates, rejected = collected  # type: ignore[misc]
        web_backend = None
    publish_time_rejections, historical_callback_rejections = _split_time_integrity_notes(list(rejected))
    candidates = [c for c in candidates if c.source_type in _md_allowed_evidence_sources()]
    if not candidates:
        rejected = list(rejected) + ["all_sources:empty_after_filter"]
    ranked = sorted(
        candidates,
        key=lambda c: (
            -_effective_candidate_score(candidate=c, pct_move=mover.pct_move),
            -_directional_bonus(text=c.text, pct_move=mover.pct_move),
            _source_rank(c.source_type),
        ),
    )
    chosen = ranked[0] if ranked else None

    ranked_specific = [c for c in ranked if c.reject_reason != "generic_wrapper"] or ranked
    cluster_scores: dict[str, float] = {}
    for item in ranked_specific[:12]:
        for cluster in item.driver_keywords:
            cluster_scores[cluster] = cluster_scores.get(cluster, 0.0) + _effective_candidate_score(candidate=item, pct_move=mover.pct_move)
    for cluster, bonus in _CLUSTER_PRIORITY_BONUS.items():
        if cluster in cluster_scores:
            cluster_scores[cluster] += bonus
    cluster_ranking = sorted(cluster_scores.items(), key=lambda kv: kv[1], reverse=True)
    top_cluster = cluster_ranking[0][0] if cluster_ranking else None
    top_cluster_score = cluster_ranking[0][1] if cluster_ranking else 0.0
    second_cluster_score = cluster_ranking[1][1] if len(cluster_ranking) > 1 else 0.0
    cluster_debug: list[str] = []
    for cluster, score in cluster_ranking[:5]:
        members = _cluster_members(ranked_specific, cluster)
        cluster_debug.append(
            f"{cluster}: score={score:.2f} sources={_cluster_independent_sources(members)} domains={_cluster_domain_count(members)} quality={1 if _cluster_has_quality_domain(members) else 0}"
        )

    top_cluster_rows = _cluster_members(ranked_specific, top_cluster) if top_cluster else []
    corroborated_sources = _cluster_independent_sources(top_cluster_rows) if top_cluster_rows else 0
    corroborated_domains = _cluster_domain_count(top_cluster_rows) if top_cluster_rows else 0
    top_cluster_confirmed = _cluster_is_corroborated(top_cluster_rows) if top_cluster_rows else False

    cluster_candidate = None
    if top_cluster_rows:
        cluster_candidate = sorted(
            top_cluster_rows,
            key=lambda c: (
                -_effective_candidate_score(candidate=c, pct_move=mover.pct_move),
                -_directional_bonus(text=c.text, pct_move=mover.pct_move),
                _source_rank(c.source_type),
            ),
        )[0]

    decisive_primary = _can_use_decisive_primary_reason(
        cluster_candidate=cluster_candidate,
        top_cluster_score=top_cluster_score,
        second_cluster_score=second_cluster_score,
        pct_move=mover.pct_move,
    )
    confidence = _effective_candidate_score(candidate=cluster_candidate, pct_move=mover.pct_move) if cluster_candidate else 0.0
    cause_mode = "fallback"
    cause_source_type: str | None = None
    cause_source_url: str | None = None
    cause_phrase: str | None = None
    direct_candidate: _EvidenceCandidate | None = None
    cause_render_mode = "fallback"
    cause_raw_phrase: str | None = None
    cause_final_phrase: str | None = None
    quality_rejections: tuple[str, ...] = ()

    if top_cluster_confirmed:
        confidence = max(_min_evidence_confidence(), min(1.0, confidence + 0.12))
        cause_mode = "cluster_confirmed"
        cause_source_type = cluster_candidate.source_type if cluster_candidate else None
        cause_source_url = cluster_candidate.url if cluster_candidate else None
        cause_phrase = _cluster_event_phrase(top_cluster or "", candidate=cluster_candidate)
    elif decisive_primary:
        confidence = max(_min_evidence_confidence(), min(1.0, confidence + 0.07))
        cause_mode = "decisive_primary"
        cause_source_type = cluster_candidate.source_type if cluster_candidate else None
        cause_source_url = cluster_candidate.url if cluster_candidate else None
        cause_phrase = _cluster_event_phrase(top_cluster or "", candidate=cluster_candidate)
    else:
        if not top_cluster:
            rejected.append("cluster:none_detected")
        else:
            rejected.append(f"cluster:{top_cluster}:unconfirmed sources={corroborated_sources} domains={corroborated_domains}")
            if len(cluster_ranking) > 1 and (top_cluster_score - second_cluster_score) < _decisive_primary_reason_min_margin():
                rejected.append("cluster:ambiguous_competing_signals")

        direct_candidate = _pick_direct_cause_candidate(candidates=ranked_specific, pct_move=mover.pct_move)
        if direct_candidate is not None:
            direct_phrase = _shorten(_strip_non_md_artifacts(direct_candidate.text), 95).rstrip(".")
            if direct_phrase:
                cause_mode = "direct_evidence"
                cause_source_type = direct_candidate.source_type
                cause_source_url = direct_candidate.url
                cause_phrase = direct_phrase
                confidence = max(confidence, _effective_candidate_score(candidate=direct_candidate, pct_move=mover.pct_move))
                rejected.append("cluster:using_direct_evidence_fallback")

    line = FALLBACK_CAUSE_LINE
    if cause_mode in {"cluster_confirmed", "decisive_primary", "direct_evidence"} and cause_phrase:
        evidence_seed = (
            direct_candidate.text
            if direct_candidate is not None
            else (
                cluster_candidate.text
                if cluster_candidate is not None
                else (chosen.text if chosen is not None else cause_phrase)
            )
        )
        (
            line,
            cause_raw_phrase,
            cause_final_phrase,
            cause_render_mode,
            quality_rejections,
        ) = _render_reason_line_with_quality(
            ticker=mover.ticker,
            pct_move=mover.pct_move,
            candidate_phrase=cause_phrase,
            evidence_text=evidence_seed,
        )
    else:
        quality_rejections = ("no_specific_cause_mode",)

    best_news = _pick_best_by_source(ranked, "yahoo_news", pct_move=mover.pct_move)
    best_web = _pick_best_by_source(ranked, "web", pct_move=mover.pct_move)
    top_evidence = tuple(
        _shorten(
            f"{c.source_type}({_effective_candidate_score(candidate=c, pct_move=mover.pct_move):.2f}): {c.text} [{c.url or 'no-url'}]",
            180,
        )
        for c in ranked[:5]
    )
    candidate_publish_times = tuple(_candidate_publish_debug(item) for item in ranked[:10])

    evidence = CatalystEvidence(
        ticker=mover.ticker,
        x_text=None,
        x_url=None,
        x_engagement=0,
        news_title=best_news.text if best_news else None,
        news_url=best_news.url if best_news else None,
        web_title=best_web.text if best_web else None,
        web_url=best_web.url if best_web else None,
        confidence=confidence,
        chosen_source=(cause_source_type or (cluster_candidate.source_type if cluster_candidate else (chosen.source_type if chosen else None))),
        driver_keywords=tuple([top_cluster] if top_cluster else ()),
        top_evidence=top_evidence,
        rejected_reasons=tuple(rejected),
        since_utc=since_utc.replace(microsecond=0).isoformat(),
        confirmed_cluster=(top_cluster if (top_cluster_confirmed or decisive_primary) else None),
        confirmed_cause_phrase=(
            cause_final_phrase
            if (line != FALLBACK_CAUSE_LINE and cause_mode in {"cluster_confirmed", "decisive_primary", "direct_evidence"})
            else None
        ),
        corroborated_sources=corroborated_sources,
        corroborated_domains=corroborated_domains,
        web_backend=web_backend,
        selected_cluster=top_cluster,
        cluster_debug=tuple(cluster_debug),
        cause_source_type=cause_source_type,
        cause_source_url=cause_source_url,
        cause_mode=cause_mode,
        cause_render_mode=cause_render_mode,
        cause_raw_phrase=cause_raw_phrase,
        cause_final_phrase=cause_final_phrase,
        quality_rejections=quality_rejections,
        time_integrity_mode="strict_in_window" if _require_in_window_dates() else "off",
        publish_time_rejections=publish_time_rejections,
        candidate_publish_times=candidate_publish_times,
        historical_callback_rejections=historical_callback_rejections,
    )
    return evidence, line


def _build_catalyst_for_mover(*, mover: QuoteSnapshot, slot_name: str, since_utc: datetime) -> tuple[CatalystEvidence, str]:
    if _catalyst_mode() == "legacy_heuristic":
        return _build_catalyst_for_mover_legacy(mover=mover, slot_name=slot_name, since_utc=since_utc)
    return _build_catalyst_for_mover_simple(mover=mover, slot_name=slot_name, since_utc=since_utc)


def _build_catalyst_rows(*, movers: list[QuoteSnapshot], slot_name: str) -> tuple[list[CatalystEvidence], list[str]]:
    since_utc = _session_window_since_utc(slot_name=slot_name)
    rows: list[CatalystEvidence] = []
    lines: list[str] = []
    for mover in movers:
        evidence, line = _build_catalyst_for_mover(mover=mover, slot_name=slot_name, since_utc=since_utc)
        rows.append(evidence)
        lines.append(line)

    if _enable_cause_cluster_reuse():
        cluster_to_idx: dict[str, list[int]] = {}
        cluster_phrase: dict[str, str] = {}
        for idx, row in enumerate(rows):
            if not row.confirmed_cluster or not row.confirmed_cause_phrase:
                continue
            cluster_to_idx.setdefault(row.confirmed_cluster, []).append(idx)
            cluster_phrase.setdefault(row.confirmed_cluster, row.confirmed_cause_phrase)
        for cluster, idxs in cluster_to_idx.items():
            if len(idxs) < 2:
                continue
            if cluster not in _CLUSTER_REUSE_ALLOWLIST:
                continue
            phrase = cluster_phrase.get(cluster)
            for idx in idxs:
                lines[idx] = _build_reason_line_from_phrase(
                    pct_move=movers[idx].pct_move if idx < len(movers) else None,
                    phrase=phrase,
                )

        # Basket-level carry-through: if one cyber name has confirmed Anthropic/Claude cause,
        # apply the same cause phrase to other cyber selloff names in the same run.
        anthro_idxs = [
            idx
            for idx, row in enumerate(rows)
            if row.confirmed_cluster == "anthropic_claude_cyber" and idx < len(movers) and ((movers[idx].pct_move or 0.0) < 0.0)
        ]
        if anthro_idxs:
            phrase = rows[anthro_idxs[0]].confirmed_cause_phrase or _CLUSTER_EVENT_PHRASES["anthropic_claude_cyber"]
            cyber_selloff_idxs = [
                idx
                for idx, mover in enumerate(movers)
                if _basket_name_for_ticker(mover.ticker) == "cybersecurity" and ((mover.pct_move or 0.0) < 0.0)
            ]
            if len(cyber_selloff_idxs) >= 2:
                for idx in cyber_selloff_idxs:
                    current = rows[idx]
                    if current.confirmed_cluster == "anthropic_claude_cyber":
                        continue
                    if current.confirmed_cluster not in {None, "deal_contract", "analyst_move", "product_launch"} and lines[idx] != FALLBACK_CAUSE_LINE:
                        continue
                    rows[idx] = replace(
                        current,
                        confirmed_cluster="anthropic_claude_cyber",
                        confirmed_cause_phrase=phrase,
                    )
                    lines[idx] = _build_reason_line_from_phrase(
                        pct_move=movers[idx].pct_move if idx < len(movers) else None,
                        phrase=phrase,
                    )
    return rows, lines


def debug_catalyst(*, ticker: str, slot_name: str = "open") -> dict[str, Any]:
    norm = _normalize_ticker(ticker)
    if not norm:
        raise MarketDailyError(f"Invalid ticker: {ticker}")
    slot = "close" if str(slot_name).strip().lower() == "close" else "open"
    since_utc = _session_window_since_utc(slot_name=slot)
    snaps = _fetch_quote_snapshots([norm])
    mover = snaps[0] if snaps else QuoteSnapshot(
        ticker=norm,
        market_cap=None,
        last_price=None,
        previous_close=None,
        pct_move=None,
        as_of_utc=_utc_now_iso(),
    )
    evidence, line = _build_catalyst_for_mover(mover=mover, slot_name=slot, since_utc=since_utc)
    return {
        "ok": True,
        "ticker": norm,
        "slot": slot,
        "since_utc": evidence.since_utc,
        "confidence": evidence.confidence,
        "chosen_source": evidence.chosen_source,
        "web_backend": evidence.web_backend,
        "driver_keywords": list(evidence.driver_keywords),
        "selected_cluster": evidence.selected_cluster,
        "confirmed_cluster": evidence.confirmed_cluster,
        "confirmed_cause_phrase": evidence.confirmed_cause_phrase,
        "cause_mode": evidence.cause_mode,
        "cause_render_mode": evidence.cause_render_mode,
        "cause_raw_phrase": evidence.cause_raw_phrase,
        "cause_final_phrase": evidence.cause_final_phrase,
        "cause_anchor_url": evidence.cause_anchor_url,
        "cause_anchor_text": evidence.cause_anchor_text,
        "cause_support_urls": list(evidence.cause_support_urls),
        "generation_format": evidence.generation_format,
        "generation_policy": evidence.generation_policy,
        "consensus_event_family": evidence.consensus_event_family,
        "consensus_winner_url": evidence.consensus_winner_url,
        "attribution_stripped": evidence.attribution_stripped,
        "quality_rejections": list(evidence.quality_rejections),
        "synth_generation_mode": evidence.synth_generation_mode,
        "synth_model_used": evidence.synth_model_used,
        "synth_candidates_considered": list(evidence.synth_candidates_considered),
        "synth_candidates_used": list(evidence.synth_candidates_used),
        "synth_chosen_urls": list(evidence.synth_chosen_urls),
        "time_integrity_mode": evidence.time_integrity_mode,
        "publish_time_rejections": list(evidence.publish_time_rejections),
        "historical_callback_rejections": list(evidence.historical_callback_rejections),
        "candidate_publish_times": list(evidence.candidate_publish_times),
        "cause_source_type": evidence.cause_source_type,
        "cause_source_url": evidence.cause_source_url,
        "corroborated_sources": evidence.corroborated_sources,
        "corroborated_domains": evidence.corroborated_domains,
        "line": line,
        "top_evidence": list(evidence.top_evidence),
        "cluster_debug": list(evidence.cluster_debug),
        "rejected_reasons": list(evidence.rejected_reasons),
        "links": {
            "news": evidence.news_url,
            "web": evidence.web_url,
        },
    }


def _auto_refresh_holdings_if_stale(store: MarketDailyStore) -> dict[str, Any] | None:
    last = store.holdings_last_updated_utc()
    if not last:
        return refresh_coatue_holdings(store=store)
    try:
        dt = datetime.fromisoformat(last)
    except Exception:
        return refresh_coatue_holdings(store=store)
    if (datetime.now(UTC) - dt.astimezone(UTC)) >= timedelta(days=30):
        return refresh_coatue_holdings(store=store)
    return None


def _rank_universe_snapshots(*, snapshots: list[QuoteSnapshot], tickers: list[str]) -> list[QuoteSnapshot]:
    by_ticker = {s.ticker: s for s in snapshots}
    selected = [by_ticker[t] for t in tickers if t in by_ticker]
    return sorted(selected, key=lambda s: (-float(s.market_cap or 0.0), s.ticker))


def _safe_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    try:
        to_py = getattr(value, "to_pydatetime", None)
        if callable(to_py):
            out = to_py()
            if isinstance(out, datetime):
                return out
    except Exception:
        return None
    return None


def _coerce_date_et(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=ZoneInfo(US_MARKET_TZ))
        return dt.astimezone(ZoneInfo(US_MARKET_TZ)).date()
    if isinstance(value, (list, tuple)):
        for item in value:
            found = _coerce_date_et(item)
            if found is not None:
                return found
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except Exception:
            return None
    as_dt = _safe_datetime(value)
    if as_dt is None:
        return None
    return _coerce_date_et(as_dt)


def _calendar_earnings_date(payload: Any) -> date | None:
    if not isinstance(payload, dict):
        return None
    return _coerce_date_et(payload.get("Earnings Date"))


def _ticker_earnings_history(*, ticker_obj: Any, limit: int = 8) -> list[tuple[datetime, float | None, float | None, float | None]]:
    try:
        frame = ticker_obj.get_earnings_dates(limit=max(1, int(limit)))
    except Exception:
        return []
    if frame is None:
        return []
    if bool(getattr(frame, "empty", False)):
        return []

    rows: list[tuple[datetime, float | None, float | None, float | None]] = []
    try:
        iterator = frame.iterrows()
    except Exception:
        return rows

    for idx, row in iterator:
        dt = _safe_datetime(idx)
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(US_MARKET_TZ))
        dt_utc = dt.astimezone(UTC)
        estimate = _safe_float(row.get("EPS Estimate")) if hasattr(row, "get") else None
        reported = _safe_float(row.get("Reported EPS")) if hasattr(row, "get") else None
        surprise = _safe_float(row.get("Surprise(%)")) if hasattr(row, "get") else None
        rows.append((dt_utc, estimate, reported, surprise))
    rows.sort(key=lambda x: x[0], reverse=True)
    return rows[: max(1, int(limit))]


def _infer_expected_session(*, earnings_history: list[tuple[datetime, float | None, float | None, float | None]]) -> str:
    if not earnings_history:
        return "unknown"
    et = ZoneInfo(US_MARKET_TZ)
    minutes: list[int] = []
    for dt, _, _, _ in earnings_history[:5]:
        loc = dt.astimezone(et)
        minutes.append(loc.hour * 60 + loc.minute)
    if not minutes:
        return "unknown"
    pick = sorted(minutes)[len(minutes) // 2]
    if pick >= (16 * 60):
        return "after_close"
    if pick <= (10 * 60):
        return "before_open"
    return "unknown"


def _session_human_label(session: str) -> str:
    if session == "after_close":
        return "after close"
    if session == "before_open":
        return "before open"
    return "unknown"


def _collect_after_close_earnings_preview(*, tickers: list[str], now_utc: datetime | None = None) -> list[EarningsPreviewItem]:
    now = now_utc or datetime.now(UTC)
    today_et = now.astimezone(ZoneInfo(US_MARKET_TZ)).date()
    out: list[EarningsPreviewItem] = []
    for ticker in sorted({t for t in tickers if _normalize_ticker(t)}):
        norm = _normalize_ticker(ticker)
        if not norm:
            continue
        try:
            ticker_obj = yf.Ticker(norm)
            calendar_payload = ticker_obj.calendar
        except Exception:
            continue
        cal_date = _calendar_earnings_date(calendar_payload)
        if cal_date != today_et:
            continue
        session = _infer_expected_session(earnings_history=_ticker_earnings_history(ticker_obj=ticker_obj, limit=8))
        if session != "after_close":
            continue
        aliases = _company_aliases(norm)
        company = aliases[0] if aliases else norm
        out.append(
            EarningsPreviewItem(
                ticker=norm,
                company=company,
                earnings_date_et=today_et.isoformat(),
                expected_session=session,
            )
        )
    out.sort(key=lambda x: x.ticker)
    return out


def _build_final_universe(
    *,
    store: MarketDailyStore,
    refresh_holdings: bool = True,
) -> tuple[list[QuoteSnapshot], dict[str, str], set[str], set[str], dict[str, Any] | None]:
    refresh_result: dict[str, Any] | None = None
    if refresh_holdings:
        refresh_result = _auto_refresh_holdings_if_stale(store)

    seed = _load_seed_tickers()
    seed_snaps = _fetch_quote_snapshots(seed)
    top_seed = _build_top_k_universe(seed_snapshots=seed_snaps, top_k=_top_k())

    include_overrides, exclude_overrides = store.read_override_sets()
    merged_tickers, source_bucket = _merge_universe(
        top_seed=top_seed,
        coatue_tickers=store.coatue_tickers(),
        include_overrides=include_overrides,
        exclude_overrides=exclude_overrides,
    )

    known = {s.ticker for s in seed_snaps}
    extras = [t for t in merged_tickers if t not in known]
    extra_snaps = _fetch_quote_snapshots(extras)
    all_snaps = seed_snaps + extra_snaps
    final_universe = _rank_universe_snapshots(snapshots=all_snaps, tickers=merged_tickers)
    return final_universe, source_bucket, include_overrides, exclude_overrides, refresh_result


def _close_anchor_utc(*, now_utc: datetime) -> datetime:
    et = ZoneInfo(US_MARKET_TZ)
    day = now_utc.astimezone(et).date()
    return datetime(day.year, day.month, day.day, 16, 0, 0, tzinfo=et).astimezone(UTC)


def _collect_reported_today_rows(*, universe: list[QuoteSnapshot], now_utc: datetime | None = None) -> list[EarningsRecapRow]:
    now = now_utc or datetime.now(UTC)
    today_et = now.astimezone(ZoneInfo(US_MARKET_TZ)).date()
    rows: list[EarningsRecapRow] = []
    for snap in universe:
        norm = _normalize_ticker(snap.ticker)
        if not norm:
            continue
        try:
            ticker_obj = yf.Ticker(norm)
            calendar_payload = ticker_obj.calendar
        except Exception:
            continue
        history = _ticker_earnings_history(ticker_obj=ticker_obj, limit=8)
        latest = history[0] if history else None
        latest_date_et = latest[0].astimezone(ZoneInfo(US_MARKET_TZ)).date() if latest else None
        calendar_date = _calendar_earnings_date(calendar_payload)
        reported_today = (latest_date_et == today_et) or (calendar_date == today_et)
        if not reported_today:
            continue

        eps_estimate = latest[1] if (latest and latest_date_et == today_et) else None
        reported_eps = latest[2] if (latest and latest_date_et == today_et) else None
        surprise_pct = latest[3] if (latest and latest_date_et == today_et) else None
        since_close_pct = None
        if snap.last_price is not None and snap.previous_close and snap.previous_close > 0:
            since_close_pct = (float(snap.last_price) - float(snap.previous_close)) / float(snap.previous_close)
        session = _infer_expected_session(earnings_history=history)
        aliases = _company_aliases(norm)
        company = aliases[0] if aliases else norm
        rows.append(
            EarningsRecapRow(
                ticker=norm,
                company=company,
                earnings_date_et=today_et.isoformat(),
                inferred_session=session,
                market_cap=snap.market_cap,
                last_price=snap.last_price,
                regular_close=snap.previous_close,
                since_close_pct=since_close_pct,
                eps_estimate=eps_estimate,
                reported_eps=reported_eps,
                surprise_pct=surprise_pct,
            )
        )
    rows.sort(
        key=lambda x: (
            -abs(float(x.since_close_pct or 0.0)),
            -float(x.market_cap or 0.0),
            x.ticker,
        )
    )
    return rows


def _recap_citation(*, source_count: int, preferred_idx: int = 1) -> str:
    if source_count <= 0:
        return ""
    idx = max(1, min(source_count, preferred_idx))
    return f"[S{idx}]"


def _normalize_recap_sentence(text: str, *, source_count: int, preferred_idx: int = 1, max_chars: int = 220) -> str | None:
    cleaned = _normalize_generated_sentence(text, max_chars=max_chars)
    if not cleaned:
        return None
    if _is_quote_directory_title(cleaned) or _contains_disallowed_reason_phrasing(cleaned):
        return None
    refs = re.findall(r"\[S(\d+)\]", cleaned, flags=re.IGNORECASE)
    if refs:
        def _keep_ref(match: re.Match[str]) -> str:
            try:
                idx = int(match.group(1))
            except Exception:
                return ""
            if source_count > 0 and 1 <= idx <= source_count:
                return f"[S{idx}]"
            return ""
        cleaned = re.sub(r"\[S(\d+)\]", _keep_ref, cleaned, flags=re.IGNORECASE)
        cleaned = _normalize_whitespace(cleaned)
    valid_ref_found = False
    if refs and source_count > 0:
        for raw in refs:
            try:
                idx = int(raw)
            except Exception:
                continue
            if 1 <= idx <= source_count:
                valid_ref_found = True
                break
    if source_count > 0 and not valid_ref_found:
        marker = _recap_citation(source_count=source_count, preferred_idx=preferred_idx)
        cleaned = f"{cleaned.rstrip('.')} {marker}."
    return cleaned


def _deterministic_recap_blocks(
    *,
    row: EarningsRecapRow,
    anchor: _EvidenceCandidate | None,
    supports: list[_EvidenceCandidate],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    source_count = 1 + len(supports) if anchor is not None else 0
    catalyst_sentence = _sentence_from_anchor_candidate(ticker=row.ticker, anchor=anchor, pct_move=row.since_close_pct)
    if not catalyst_sentence:
        catalyst_sentence = "Coverage remains mixed on the immediate driver behind the post-earnings move."
    key = _normalize_recap_sentence(
        f"Key catalyst: {catalyst_sentence}",
        source_count=source_count,
        preferred_idx=1,
    ) or f"Key catalyst remains mixed across early coverage. {_recap_citation(source_count=source_count, preferred_idx=1)}".strip()

    reaction = _normalize_recap_sentence(
        (
            f"Since regular close, shares traded {_format_pct(row.since_close_pct)} "
            f"({row.regular_close if row.regular_close is not None else 'n/a'} -> {row.last_price if row.last_price is not None else 'n/a'})."
        ),
        source_count=source_count,
        preferred_idx=1,
    ) or "Since regular close, shares showed a notable reaction as results and commentary were digested."

    if row.reported_eps is not None and row.eps_estimate is not None:
        surprise = f"{row.surprise_pct:.1f}%" if row.surprise_pct is not None else "n/a"
        takeaway_raw = f"EPS was {row.reported_eps:.2f} vs {row.eps_estimate:.2f} estimate (surprise {surprise})."
    else:
        takeaway_raw = (
            "Initial investor sentiment is tracking early headline read-through, with guidance and call commentary driving the next leg."
        )
    takeaway = _normalize_recap_sentence(
        takeaway_raw,
        source_count=source_count,
        preferred_idx=2 if source_count >= 2 else 1,
    ) or takeaway_raw

    bullets = tuple([key, reaction, takeaway][:4])
    return bullets, ("llm_unavailable",)


def _openai_client() -> Any | None:
    if OpenAI is None:
        return None
    key = (os.environ.get("OPENAI_API_KEY", "") or "").strip()
    if not key:
        return None
    try:
        return OpenAI(api_key=key)
    except Exception:
        return None


def _extract_bullets(text: str) -> list[str]:
    out: list[str] = []
    for raw in str(text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(("-", "*", "•")):
            line = line[1:].strip()
        if not line:
            continue
        out.append(line)
    return out


def _synthesize_earnings_recap_blocks(
    *,
    client: Any | None,
    row: EarningsRecapRow,
    anchor: _EvidenceCandidate | None,
    supports: list[_EvidenceCandidate],
) -> tuple[tuple[str, ...], str, tuple[str, ...]]:
    if anchor is None:
        bullets, rejections = _deterministic_recap_blocks(row=row, anchor=anchor, supports=supports)
        return bullets, "deterministic_backup", tuple(list(rejections) + ["no_anchor"])

    source_rows = [anchor] + supports
    source_count = len(source_rows)
    if client is None:
        bullets, rejections = _deterministic_recap_blocks(row=row, anchor=anchor, supports=supports)
        return bullets, "deterministic_backup", rejections

    source_lines: list[str] = []
    for idx, item in enumerate(source_rows, start=1):
        ts = item.published_at_utc.isoformat() if item.published_at_utc else "unknown"
        context = _shorten(_normalize_whitespace(item.context_text or item.text), 300)
        source_lines.append(f"[S{idx}] {context} ({item.url or 'n/a'}) ts={ts}")
    evidence_block = "\n".join(source_lines)
    prompt = (
        "Write exactly 3 bullets for an earnings recap using only the provided evidence.\n"
        "Bullet requirements:\n"
        "- Bullet 1: key catalyst for the move.\n"
        "- Bullet 2: reaction since regular close.\n"
        "- Bullet 3: takeaway on earnings/guidance/investor sentiment.\n"
        "- Each bullet must be one complete sentence.\n"
        "- Include citations like [S1], [S2], [S3].\n"
        "- No links and no markdown headers.\n\n"
        f"Ticker: {row.ticker}\n"
        f"Company: {row.company}\n"
        f"Since close move: {_format_pct(row.since_close_pct)}\n"
        f"Regular close: {row.regular_close}\n"
        f"Last traded: {row.last_price}\n"
        f"Earnings date ET: {row.earnings_date_et}\n"
        f"Inferred session: {row.inferred_session}\n"
        f"EPS estimate: {row.eps_estimate}\n"
        f"Reported EPS: {row.reported_eps}\n"
        f"Surprise(%): {row.surprise_pct}\n"
        "Evidence:\n"
        f"{evidence_block}\n"
    )
    try:
        response = client.chat.completions.create(
            model=_md_model(),
            messages=[
                {"role": "system", "content": "You write concise, source-grounded earnings recap bullets for traders."},
                {"role": "user", "content": prompt},
            ],
        )
        text = str(response.choices[0].message.content or "").strip()  # type: ignore[index]
    except Exception:
        logger.exception("earnings recap block synthesis failed for %s", row.ticker)
        bullets, rejections = _deterministic_recap_blocks(row=row, anchor=anchor, supports=supports)
        return bullets, "deterministic_backup", tuple(list(rejections) + ["llm_exception"])

    raw_bullets = _extract_bullets(text)
    normalized: list[str] = []
    rejections: list[str] = []
    for idx, bullet in enumerate(raw_bullets[:4], start=1):
        normalized_bullet = _normalize_recap_sentence(
            bullet,
            source_count=source_count,
            preferred_idx=min(idx, source_count),
            max_chars=240,
        )
        if normalized_bullet is None:
            rejections.append(f"invalid_bullet_{idx}")
            continue
        normalized.append(normalized_bullet)
    if len(normalized) < 2:
        bullets, det_rejections = _deterministic_recap_blocks(row=row, anchor=anchor, supports=supports)
        return bullets, "deterministic_backup", tuple(rejections + list(det_rejections) + ["llm_insufficient_bullets"])

    if len(normalized) > 4:
        normalized = normalized[:4]
    if _recap_post_as_is():
        return tuple(normalized), "llm", tuple(rejections)

    normalized = [(_normalize_generated_sentence(b, max_chars=220) or b) for b in normalized]
    return tuple(normalized), "llm", tuple(rejections)


# compatibility wrapper for older direct tests/paths; recap runtime now uses _synthesize_earnings_recap_blocks
def _synthesize_earnings_bullets(*, client: Any | None, row: EarningsRecapRow) -> tuple[str, ...]:
    anchor = None
    supports: list[_EvidenceCandidate] = []
    bullets, _, _ = _synthesize_earnings_recap_blocks(client=client, row=row, anchor=anchor, supports=supports)
    return bullets


def _hydrate_recap_row(*, row: EarningsRecapRow, since_utc: datetime, client: Any | None) -> EarningsRecapRow:
    aliases = _company_aliases(row.ticker)
    candidates: list[_EvidenceCandidate] = []
    anchor: _EvidenceCandidate | None = None
    supports: list[_EvidenceCandidate] = []
    consensus_family: str | None = None
    if _catalyst_mode() == "simple_synthesis":
        considered, selected, _, _ = _collect_synthesis_candidates(
            ticker=row.ticker,
            aliases=aliases,
            since_utc=since_utc,
            pct_move=row.since_close_pct,
        )
        candidates = selected or considered
        consensus_top_k = max(3, 1 + _synth_support_count())
        anchor, consensus_family = _pick_consensus_winner(
            candidates=candidates,
            ticker=row.ticker,
            pct_move=row.since_close_pct,
            top_k=consensus_top_k,
        )
        if anchor is None:
            anchor = _pick_anchor_candidate(candidates=candidates, ticker=row.ticker, pct_move=row.since_close_pct)
        if anchor is not None and consensus_family is None:
            consensus_family = _candidate_event_family(anchor)
        supports = _pick_support_candidates(
            candidates=candidates,
            anchor=anchor,
            max_support=_recap_support_count(),
            pct_move=row.since_close_pct,
        )
        # Recap keeps anchor + support evidence ordering for citation stability.
    else:
        collected = _collect_evidence_for_ticker(
            ticker=row.ticker,
            aliases=aliases,
            since_utc=since_utc,
            pct_move=row.since_close_pct,
        )
        if len(collected) == 3:
            candidates = collected[0]
        else:
            candidates = collected[0]  # type: ignore[index]

    evidence_lines: list[str] = []
    links: list[str] = []
    if _catalyst_mode() == "simple_synthesis" and anchor is not None:
        ordered = [anchor] + supports
    else:
        ordered = candidates[:4]
    for item in ordered[:4]:
        evidence_lines.append(_shorten(f"{item.source_type}: {item.text}", 180))
        if item.url:
            links.append(item.url)
    hydrated = replace(
        row,
        evidence=tuple(evidence_lines),
        source_links=tuple(links),
        recap_anchor_url=(anchor.url if anchor is not None else None),
        recap_support_urls=tuple([x.url for x in supports if x.url]),
    )
    bullets, generation_mode, quality_rejections = _synthesize_earnings_recap_blocks(
        client=client,
        row=hydrated,
        anchor=anchor,
        supports=supports,
    )
    cleaned_bullets = tuple([b for b in bullets if _normalize_whitespace(b)])
    if len(cleaned_bullets) < 2:
        cleaned_bullets, fallback_rejections = _deterministic_recap_blocks(row=hydrated, anchor=anchor, supports=supports)
        generation_mode = "deterministic_backup"
        quality_rejections = tuple(list(quality_rejections) + list(fallback_rejections) + ["empty_or_short_bullets"])
    if len(cleaned_bullets) > 4:
        cleaned_bullets = cleaned_bullets[:4]
    return replace(
        hydrated,
        bullets=cleaned_bullets,
        recap_generation_mode=generation_mode,
        recap_quality_rejections=quality_rejections,
    )


def _build_earnings_recap_message(*, rows: list[EarningsRecapRow], now_local: datetime) -> str:
    as_of = now_local.strftime("%-I:%M %p PT")
    lines: list[str] = [f"*MD — Earnings Recap | As of {as_of}*"]
    lines.append("Top earnings names by move since regular close:")
    for row in rows:
        lines.append(f"*{row.ticker}* {_format_pct(row.since_close_pct)} vs close ({_session_human_label(row.inferred_session)})")
        for bullet in row.bullets:
            lines.append(f"- {bullet}")
        if row.source_links:
            refs = [f"<{url}|[S{idx}]>" for idx, url in enumerate(row.source_links, start=1)]
            lines.append(f"Sources: {' '.join(refs)}")
    lines.append(f"Data UTC: {_utc_now_iso()} | Sources: Yahoo earnings calendar/history + Yahoo fast_info + Google web + Yahoo news evidence")
    return "\n".join(lines)


def _write_earnings_recap_artifact(*, now_local: datetime, rows: list[EarningsRecapRow], message_text: str) -> Path:
    out_dir = _artifact_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    path = out_dir / f"md-earnings-recap-{ts}.md"

    lines: list[str] = []
    lines.append("# MD - Earnings Recap")
    lines.append("")
    lines.append(f"Generated local: `{now_local.isoformat()}`")
    lines.append(f"Generated utc: `{_utc_now_iso()}`")
    lines.append("")
    lines.append("## Slack Post")
    lines.append("")
    lines.append(message_text)
    lines.append("")
    lines.append("## Recap Rows")
    lines.append("")
    for row in rows:
        lines.append(f"- `{row.ticker}` since_close `{_format_pct(row.since_close_pct)}` market_cap `{row.market_cap}`")
        lines.append(f"  - earnings_date_et: `{row.earnings_date_et}`")
        lines.append(f"  - inferred_session: `{row.inferred_session}`")
        lines.append(f"  - eps_estimate: `{row.eps_estimate}`")
        lines.append(f"  - reported_eps: `{row.reported_eps}`")
        lines.append(f"  - surprise_pct: `{row.surprise_pct}`")
        if row.recap_anchor_url:
            lines.append(f"  - recap_anchor_url: `{row.recap_anchor_url}`")
        if row.recap_support_urls:
            lines.append("  - recap_support_urls:")
            for url in row.recap_support_urls:
                lines.append(f"    - `{url}`")
        if row.recap_generation_mode:
            lines.append(f"  - recap_generation_mode: `{row.recap_generation_mode}`")
        if row.recap_quality_rejections:
            lines.append(f"  - recap_quality_rejections: `{', '.join(row.recap_quality_rejections)}`")
        lines.append("  - bullets:")
        for bullet in row.bullets:
            lines.append(f"    - {bullet}")
        lines.append("  - evidence:")
        if row.evidence:
            for idx, entry in enumerate(row.evidence, start=1):
                lines.append(f"    - [S{idx}] {entry}")
        else:
            lines.append("    - none")
        if row.source_links:
            lines.append("  - source_links:")
            for idx, url in enumerate(row.source_links, start=1):
                lines.append(f"    - [S{idx}] {url}")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def run_once(
    *,
    manual: bool = False,
    force: bool = False,
    dry_run: bool = False,
    channel_override: str | None = None,
) -> dict[str, Any]:
    store = MarketDailyStore()
    now_local = datetime.now(UTC).astimezone(_timezone())
    times = _parse_times()
    slot = _slot_name(now_local=now_local, times=times, manual=manual)
    if slot is None:
        return {
            "ok": True,
            "posted": False,
            "reason": "outside_scheduled_window",
            "now_local": now_local.isoformat(),
            "times": ",".join(f"{h:02d}:{m:02d}" for h, m in times),
        }

    date_key = now_local.strftime("%Y-%m-%d")
    if (not force) and store.slot_already_recorded(run_date_local=date_key, slot_name=slot):
        return {
            "ok": True,
            "posted": False,
            "reason": "slot_already_posted",
            "slot": slot,
            "date": date_key,
        }

    if (not force) and _is_market_closed_now(now_local):
        run_id = store.record_run(
            run_date_local=date_key,
            slot_name=slot,
            triggered_manual=manual,
            status="skipped_market_closed",
            reason="market_closed",
            channel_ref=(channel_override or _channel_default()),
            channel_id=None,
            message_ts=None,
            artifact_path=None,
            posted_at_utc=None,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "skipped_market_closed",
            "slot": slot,
            "run_id": run_id,
        }

    final_universe, source_bucket, include_overrides, exclude_overrides, refresh_result = _build_final_universe(store=store, refresh_holdings=True)

    movers = _select_top_movers(snapshots=final_universe, top_n=_top_n())
    if not movers:
        run_id = store.record_run(
            run_date_local=date_key,
            slot_name=slot,
            triggered_manual=manual,
            status="skipped_no_data",
            reason="no_movers",
            channel_ref=(channel_override or _channel_default()),
            channel_id=None,
            message_ts=None,
            artifact_path=None,
            posted_at_utc=None,
        )
        return {
            "ok": True,
            "posted": False,
            "reason": "no_movers",
            "slot": slot,
            "run_id": run_id,
        }

    catalyst_rows, catalyst_lines = _build_catalyst_rows(movers=movers, slot_name=slot)
    earnings_after_close = (
        _collect_after_close_earnings_preview(tickers=[x.ticker for x in final_universe], now_utc=datetime.now(UTC))
        if slot == "open"
        else []
    )

    message = _build_message(
        slot_name=slot,
        now_local=now_local,
        universe_count=len(final_universe),
        movers=movers,
        catalyst_rows=catalyst_rows,
        catalyst_lines=catalyst_lines,
        earnings_after_close=earnings_after_close,
    )
    artifact = _write_artifact(
        slot_name=slot,
        now_local=now_local,
        universe=final_universe,
        movers=movers,
        catalyst_rows=catalyst_rows,
        catalyst_lines=catalyst_lines,
        message_text=message,
        earnings_after_close=earnings_after_close,
    )

    channel_ref = (channel_override or _channel_default()).strip() or _channel_default()
    channel_id: str | None = None
    message_ts: str | None = None
    posted = False
    status = "dry_run"
    reason = None

    if dry_run:
        posted = False
    else:
        channel_id, message_ts = _post_to_slack(channel_ref=channel_ref, text=message)
        posted = True
        status = "posted"

    run_id = store.record_run(
        run_date_local=date_key,
        slot_name=slot,
        triggered_manual=manual,
        status=status,
        reason=reason,
        channel_ref=channel_ref,
        channel_id=channel_id,
        message_ts=message_ts,
        artifact_path=str(artifact),
        posted_at_utc=(_utc_now_iso() if posted and (not dry_run) else None),
    )
    store.save_universe_snapshot(run_id=run_id, snapshots=final_universe, source_map=source_bucket)

    return {
        "ok": True,
        "posted": posted,
        "slot": slot,
        "run_id": run_id,
        "status": status,
        "channel": channel_id or channel_ref,
        "message_ts": message_ts,
        "artifact_path": str(artifact),
        "refresh_result": refresh_result,
        "movers": [
            {
                "ticker": m.ticker,
                "pct_move": m.pct_move,
                "market_cap": m.market_cap,
            }
            for m in movers
        ],
        "overrides": {
            "include": sorted(include_overrides),
            "exclude": sorted(exclude_overrides),
        },
        "earnings_after_close": [
            {
                "ticker": item.ticker,
                "company": item.company,
                "earnings_date_et": item.earnings_date_et,
                "expected_session": item.expected_session,
            }
            for item in earnings_after_close
        ],
    }


def run_earnings_recap(
    *,
    manual: bool = False,
    force: bool = False,
    dry_run: bool = False,
    channel_override: str | None = None,
) -> dict[str, Any]:
    store = MarketDailyStore()
    now_utc = datetime.now(UTC)
    now_local = now_utc.astimezone(_timezone())
    recap_hh, recap_mm = _earnings_recap_time()
    target = now_local.replace(hour=recap_hh, minute=recap_mm, second=0, microsecond=0)
    within_scheduled_window = abs((now_local - target).total_seconds()) <= (20 * 60)
    if not manual:
        if not within_scheduled_window:
            return {
                "ok": True,
                "posted": False,
                "reason": "outside_scheduled_window",
                "now_local": now_local.isoformat(),
                "recap_time": f"{recap_hh:02d}:{recap_mm:02d}",
            }

    # Keep daytime manual test runs from consuming the nightly scheduled slot.
    slot = "earnings_recap"
    if manual and (not within_scheduled_window):
        slot = "earnings_recap_manual"
    date_key = now_local.strftime("%Y-%m-%d")
    if (not force) and store.slot_already_recorded(run_date_local=date_key, slot_name=slot):
        return {
            "ok": True,
            "posted": False,
            "reason": "slot_already_posted",
            "slot": slot,
            "date": date_key,
        }

    final_universe, source_bucket, include_overrides, exclude_overrides, refresh_result = _build_final_universe(store=store, refresh_holdings=True)
    candidates = _collect_reported_today_rows(universe=final_universe, now_utc=now_utc)
    if not candidates:
        run_id = store.record_run(
            run_date_local=date_key,
            slot_name=slot,
            triggered_manual=manual,
            status="skipped_no_reporters",
            reason="no_reporters",
            channel_ref=(channel_override or _channel_default()),
            channel_id=None,
            message_ts=None,
            artifact_path=None,
            posted_at_utc=None,
        )
        store.save_universe_snapshot(run_id=run_id, snapshots=final_universe, source_map=source_bucket)
        return {
            "ok": True,
            "posted": False,
            "reason": "no_reporters",
            "slot": slot,
            "run_id": run_id,
            "reporters": 0,
        }

    selected = candidates[:4]
    since_utc = _close_anchor_utc(now_utc=now_utc)
    client = _openai_client()
    hydrated = [_hydrate_recap_row(row=row, since_utc=since_utc, client=client) for row in selected]
    message = _build_earnings_recap_message(rows=hydrated, now_local=now_local)
    artifact = _write_earnings_recap_artifact(now_local=now_local, rows=hydrated, message_text=message)

    channel_ref = (channel_override or _channel_default()).strip() or _channel_default()
    channel_id: str | None = None
    message_ts: str | None = None
    posted = False
    status = "dry_run"
    if not dry_run:
        channel_id, message_ts = _post_to_slack(channel_ref=channel_ref, text=message)
        posted = True
        status = "posted"

    run_id = store.record_run(
        run_date_local=date_key,
        slot_name=slot,
        triggered_manual=manual,
        status=status,
        reason=None,
        channel_ref=channel_ref,
        channel_id=channel_id,
        message_ts=message_ts,
        artifact_path=str(artifact),
        posted_at_utc=(_utc_now_iso() if posted and (not dry_run) else None),
    )
    store.save_universe_snapshot(run_id=run_id, snapshots=final_universe, source_map=source_bucket)
    return {
        "ok": True,
        "posted": posted,
        "slot": slot,
        "run_id": run_id,
        "status": status,
        "channel": channel_id or channel_ref,
        "message_ts": message_ts,
        "artifact_path": str(artifact),
        "refresh_result": refresh_result,
        "reporters": len(hydrated),
        "movers": [
            {
                "ticker": row.ticker,
                "since_close_pct": row.since_close_pct,
                "market_cap": row.market_cap,
            }
            for row in hydrated
        ],
        "overrides": {
            "include": sorted(include_overrides),
            "exclude": sorted(exclude_overrides),
        },
    }


def status() -> dict[str, Any]:
    store = MarketDailyStore()
    recap_hh, recap_mm = _earnings_recap_time()
    return {
        "ok": True,
        "db_path": str(store.db_path),
        "timezone": os.environ.get("COATUE_CLAW_MD_TZ", DEFAULT_TZ),
        "times": ",".join(f"{h:02d}:{m:02d}" for h, m in _parse_times()),
        "earnings_recap_time": f"{recap_hh:02d}:{recap_mm:02d}",
        "channel": _channel_default(),
        "model": _md_model(),
        "catalyst_mode": _catalyst_mode(),
        "synth_max_results": _synth_max_results(),
        "synth_source_mode": _synth_source_mode(),
        "synth_domain_gate": _synth_domain_gate(),
        "synth_support_count": _synth_support_count(),
        "synth_force_best_guess": _synth_force_best_guess(),
        "relevance_mode": _relevance_mode(),
        "reason_output_mode": _reason_output_mode(),
        "post_as_is": _md_post_as_is(),
        "recap_support_count": _recap_support_count(),
        "recap_post_as_is": _recap_post_as_is(),
        "require_in_window_dates": _require_in_window_dates(),
        "allow_undated_fallback": _allow_undated_fallback(),
        "reject_historical_callback": _reject_historical_callback(),
        "publish_time_enrich_enabled": _publish_time_enrich_enabled(),
        "publish_time_enrich_timeout_ms": _publish_time_enrich_timeout_ms(),
        "reason_quality_mode": _reason_quality_mode(),
        "reason_polish_enabled": _reason_polish_enabled(),
        "reason_polish_model": _reason_polish_model(),
        "reason_polish_max_chars": _reason_polish_max_chars(),
        "top_n": _top_n(),
        "top_k": _top_k(),
        "max_lookback_hours": _max_lookback_hours(),
        "web_search_enabled": _web_search_enabled(),
        "web_search_backend": _web_search_backend(),
        "google_serp_configured": bool(_google_serp_api_key()),
        "web_max_results": _web_max_results(),
        "min_evidence_confidence": _min_evidence_confidence(),
        "decisive_primary_reason_enabled": _decisive_primary_reason_enabled(),
        "decisive_primary_reason_min_score": _decisive_primary_reason_min_score(),
        "decisive_primary_reason_min_margin": _decisive_primary_reason_min_margin(),
        "seed_path": str(_seed_path()),
        "recent_runs": store.latest_runs(limit=10),
        "recent_earnings_recap_runs": store.latest_runs_for_slot(slot_name="earnings_recap", limit=5),
        "holdings_count": store.coatue_holdings_count(),
        "overrides": store.list_overrides(),
    }


def holdings() -> dict[str, Any]:
    store = MarketDailyStore()
    tickers = store.coatue_tickers()
    return {
        "ok": True,
        "count": len(tickers),
        "tickers": tickers,
        "last_updated_utc": store.holdings_last_updated_utc(),
    }


def set_override(*, ticker: str, action: str, updated_by: str | None = None) -> dict[str, Any]:
    t = _normalize_ticker(ticker)
    if not t:
        raise MarketDailyError(f"Invalid ticker: {ticker}")
    store = MarketDailyStore()
    store.set_override(ticker=t, action=action, updated_by=updated_by)
    return {
        "ok": True,
        "ticker": t,
        "action": action,
        "overrides": store.list_overrides(),
    }


def _main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-market-daily")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run-once")
    run.add_argument("--manual", action="store_true")
    run.add_argument("--force", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--channel", default="")

    recap = sub.add_parser("run-earnings-recap")
    recap.add_argument("--manual", action="store_true")
    recap.add_argument("--force", action="store_true")
    recap.add_argument("--dry-run", action="store_true")
    recap.add_argument("--channel", default="")

    sub.add_parser("status")
    sub.add_parser("holdings")
    sub.add_parser("refresh-coatue-holdings")

    incl = sub.add_parser("include")
    incl.add_argument("ticker")

    excl = sub.add_parser("exclude")
    excl.add_argument("ticker")

    dbg = sub.add_parser("debug-catalyst")
    dbg.add_argument("ticker")
    dbg.add_argument("--slot", choices=("open", "close"), default="open")

    args = parser.parse_args()
    if args.cmd == "run-once":
        result = run_once(
            manual=bool(args.manual),
            force=bool(args.force),
            dry_run=bool(args.dry_run),
            channel_override=(str(args.channel).strip() or None),
        )
    elif args.cmd == "run-earnings-recap":
        result = run_earnings_recap(
            manual=bool(args.manual),
            force=bool(args.force),
            dry_run=bool(args.dry_run),
            channel_override=(str(args.channel).strip() or None),
        )
    elif args.cmd == "status":
        result = status()
    elif args.cmd == "holdings":
        result = holdings()
    elif args.cmd == "refresh-coatue-holdings":
        result = refresh_coatue_holdings()
    elif args.cmd == "include":
        result = set_override(ticker=args.ticker, action="include")
    elif args.cmd == "debug-catalyst":
        result = debug_catalyst(ticker=args.ticker, slot_name=args.slot)
    else:
        result = set_override(ticker=args.ticker, action="exclude")

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    _main()

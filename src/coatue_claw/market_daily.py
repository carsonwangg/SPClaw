from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
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
DEFAULT_MODEL = "gpt-5-mini"
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
    return (os.environ.get("COATUE_CLAW_MD_WEB_SEARCH_BACKEND", "ddg_html") or "ddg_html").strip().lower()


def _web_max_results() -> int:
    raw = (os.environ.get("COATUE_CLAW_MD_WEB_MAX_RESULTS", "8") or "8").strip()
    try:
        val = int(raw)
    except Exception:
        val = 8
    return max(1, min(15, val))


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
    raw = (os.environ.get("COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_SCORE", "0.64") or "0.64").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.64
    return max(0.3, min(0.95, val))


def _decisive_primary_reason_min_margin() -> float:
    raw = (os.environ.get("COATUE_CLAW_MD_DECISIVE_PRIMARY_REASON_MIN_MARGIN", "0.06") or "0.06").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.06
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
    "earnings_guidance": "earnings and guidance reset expectations.",
    "macro_rates": "rates and macro signals shifted risk appetite.",
    "product_launch": "a product launch reset expectations.",
    "analyst_move": "analyst rating changes moved expectations.",
}

_CLUSTER_PRIORITY_BONUS: dict[str, float] = {
    "anthropic_claude_cyber": 0.35,
    "anthropic_claude": 0.2,
}

_CLUSTER_REUSE_ALLOWLIST: set[str] = {"anthropic_claude_cyber", "anthropic_claude"}

_DOMAIN_WEIGHTS: dict[str, float] = {
    "finance.yahoo.com": 0.95,
    "investing.com": 0.9,
    "stocktwits.com": 0.88,
    "marketwatch.com": 0.88,
    "bloomberg.com": 0.92,
    "reuters.com": 0.9,
    "wsj.com": 0.86,
    "seekingalpha.com": 0.82,
    "coincentral.com": 0.75,
}

_QUALITY_CAUSE_DOMAINS: set[str] = {
    "finance.yahoo.com",
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
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


def _is_generic_headline_wrapper(*, text: str, ticker: str, aliases: list[str]) -> bool:
    if not _generic_headline_blocklist_enabled():
        return False
    cleaned = _normalize_whitespace(text)
    if not cleaned:
        return True
    lower = cleaned.lower()
    for pattern in _GENERIC_WRAPPER_PATTERNS:
        if pattern.search(lower):
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
                reject_reason=("generic_wrapper" if _is_generic_headline_wrapper(text=item.text, ticker=ticker, aliases=aliases) else item.reject_reason),
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
    source_bonus = {"yahoo_news": 0.18, "x": 0.12, "web": 0.1}.get(source_type, 0.0)
    score = (0.35 * recency) + (0.3 * domain) + mention + keyword_score + source_bonus
    return max(0.0, min(1.0, score))


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


def _yahoo_item_title_url_published(item: dict[str, Any]) -> tuple[str, str, datetime | None]:
    content = item.get("content") if isinstance(item.get("content"), dict) else {}
    legacy_ts = item.get("providerPublishTime")
    legacy_title = item.get("title")
    legacy_link = item.get("link")

    pub_date_raw = content.get("pubDate") if isinstance(content, dict) else None
    title_raw = content.get("title") if isinstance(content, dict) else None
    click_raw = content.get("clickThroughUrl") if isinstance(content, dict) else None
    canonical_raw = content.get("canonicalUrl") if isinstance(content, dict) else None

    title = _normalize_whitespace(str(title_raw or legacy_title or ""))
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
    return (title, url, published)


def _fetch_yahoo_news_candidates(*, ticker: str, aliases: list[str], since_utc: datetime) -> list[_EvidenceCandidate]:
    try:
        news = yf.Ticker(ticker).news or []
    except Exception:
        return []

    out: list[_EvidenceCandidate] = []
    for item in news:
        if not isinstance(item, dict):
            continue
        title, link, published = _yahoo_item_title_url_published(item)
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


def _fetch_web_evidence_ddg(*, ticker: str, aliases: list[str], since_utc: datetime, pct_move: float | None = None) -> list[_EvidenceCandidate]:
    if (not _web_search_enabled()) or _web_search_backend() != "ddg_html":
        return []
    primary = aliases[0] if aliases else ticker
    down_or_up = "fall today why" if (pct_move or 0.0) < 0 else "rise today why"
    queries = [
        f"{ticker} {primary} stock {down_or_up}",
        f"{ticker} {primary} stock move today why",
        f"{ticker} {primary} cybersecurity anthropic claude",
    ]
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
                )
            )
            if len(out) >= max_results:
                break
    return sorted(out, key=lambda x: -x.score)


def _source_rank(source_type: str) -> int:
    return {"yahoo_news": 0, "web": 1, "x": 2}.get(source_type, 9)


def _directional_bonus(*, text: str, pct_move: float | None) -> float:
    if pct_move is None:
        return 0.0
    lower = text.lower()
    up_terms = ("surge", "rises", "rose", "gains", "jumps", "up ", "beats", "upgrades", "partnership", "announces", "announce")
    down_terms = ("drops", "drop", "fell", "falls", "slides", "selloff", "sold off", "down ", "misses", "downgrade", "weighed", "pressured", "under pressure")
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
    if ("stock is down today" in lower) or ("news today" in lower):
        return True
    return any(pattern.search(lower) for pattern in _GENERIC_WRAPPER_PATTERNS)


def _collect_evidence_for_ticker(
    *,
    ticker: str,
    aliases: list[str],
    since_utc: datetime,
    pct_move: float | None = None,
) -> tuple[list[_EvidenceCandidate], list[str]]:
    rejected: list[str] = []
    x_candidates = _fetch_x_evidence_candidates(ticker=ticker, aliases=aliases, since_utc=since_utc)
    if not x_candidates:
        rejected.append("x:no_relevant_matches")
    yahoo_candidates = _fetch_yahoo_news_candidates(ticker=ticker, aliases=aliases, since_utc=since_utc)
    if not yahoo_candidates:
        rejected.append("yahoo:no_recent_relevant_headlines")

    combined = _normalize_evidence_candidates(candidates=(x_candidates + yahoo_candidates), ticker=ticker, aliases=aliases)
    x_y_best = max((c.score for c in combined), default=0.0)
    source_diversity = len({c.source_type for c in combined})
    directional_hits = any(_directional_bonus(text=c.text, pct_move=pct_move) > 0 for c in combined)
    needs_web = (x_y_best < _min_evidence_confidence()) or (source_diversity < 2) or (not directional_hits)
    if needs_web:
        web_candidates = _normalize_evidence_candidates(
            candidates=_fetch_web_evidence_ddg(ticker=ticker, aliases=aliases, since_utc=since_utc, pct_move=pct_move),
            ticker=ticker,
            aliases=aliases,
        )
        if web_candidates:
            combined = _normalize_evidence_candidates(candidates=(combined + web_candidates), ticker=ticker, aliases=aliases)
        else:
            rejected.append("web:no_relevant_results")
    if not combined:
        rejected.append("all_sources:empty")
    return (sorted(combined, key=lambda c: (-_effective_candidate_score(candidate=c, pct_move=pct_move), _source_rank(c.source_type))), rejected)


def _preferred_evidence_text(evidence: CatalystEvidence) -> str:
    if evidence.chosen_source == "web" and evidence.web_title:
        return evidence.web_title
    if evidence.chosen_source == "x" and evidence.x_text:
        return evidence.x_text
    if evidence.chosen_source == "yahoo_news" and evidence.news_title:
        return evidence.news_title
    return evidence.news_title or evidence.web_title or evidence.x_text or ""


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
        cat = _ensure_reason_like_line(cat, evidence=ev)
        links: list[str] = []
        if ev.x_url:
            links.append(f"<{ev.x_url}|[X]>")
        if ev.news_url:
            links.append(f"<{ev.news_url}|[News]>")
        if ev.web_url and len(links) < 2:
            links.append(f"<{ev.web_url}|[Web]>")
        link_text = f" {' '.join(links)}" if links else ""
        emoji = "📈" if (mover.pct_move or 0.0) >= 0 else "📉"
        lines.append(f"- {emoji} {mover.ticker} {_format_pct(mover.pct_move)} — {cat}{link_text}")

    lines.append(f"Data UTC: {_utc_now_iso()} | Sources: Yahoo fast_info + X recent search + Yahoo news + web fallback")
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
        if ev.driver_keywords:
            lines.append(f"  - driver_keywords: {', '.join(ev.driver_keywords)}")
        if ev.x_url:
            lines.append(f"  - x: {ev.x_url}")
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


def _build_catalyst_for_mover(*, mover: QuoteSnapshot, slot_name: str, since_utc: datetime) -> tuple[CatalystEvidence, str]:
    aliases = _company_aliases(mover.ticker)
    candidates, rejected = _collect_evidence_for_ticker(
        ticker=mover.ticker,
        aliases=aliases,
        since_utc=since_utc,
        pct_move=mover.pct_move,
    )
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
    if top_cluster_confirmed:
        confidence = max(_min_evidence_confidence(), min(1.0, confidence + 0.12))
    elif decisive_primary:
        confidence = max(_min_evidence_confidence(), min(1.0, confidence + 0.07))

    cause_phrase = None
    if top_cluster_confirmed or decisive_primary:
        cause_phrase = _cluster_event_phrase(top_cluster or "", candidate=cluster_candidate)

    best_x = _pick_best_by_source(ranked, "x", pct_move=mover.pct_move)
    best_news = _pick_best_by_source(ranked, "yahoo_news", pct_move=mover.pct_move)
    best_web = _pick_best_by_source(ranked, "web", pct_move=mover.pct_move)
    top_evidence = tuple(
        _shorten(
            f"{c.source_type}({_effective_candidate_score(candidate=c, pct_move=mover.pct_move):.2f}): {c.text} [{c.url or 'no-url'}]",
            180,
        )
        for c in ranked[:3]
    )

    evidence = CatalystEvidence(
        ticker=mover.ticker,
        x_text=best_x.text if best_x else None,
        x_url=best_x.url if best_x else None,
        x_engagement=best_x.engagement if best_x else 0,
        news_title=best_news.text if best_news else None,
        news_url=best_news.url if best_news else None,
        web_title=best_web.text if best_web else None,
        web_url=best_web.url if best_web else None,
        confidence=confidence,
        chosen_source=((cluster_candidate.source_type if cluster_candidate else (chosen.source_type if chosen else None))),
        driver_keywords=tuple([top_cluster] if top_cluster else ()),
        top_evidence=top_evidence,
        rejected_reasons=tuple(rejected),
        since_utc=since_utc.replace(microsecond=0).isoformat(),
        confirmed_cluster=(top_cluster if (top_cluster_confirmed or decisive_primary) else None),
        confirmed_cause_phrase=(cause_phrase if (top_cluster_confirmed or decisive_primary) else None),
        corroborated_sources=corroborated_sources,
        corroborated_domains=corroborated_domains,
    )
    if (top_cluster_confirmed or decisive_primary) and cause_phrase:
        line = _build_reason_line_from_phrase(pct_move=mover.pct_move, phrase=cause_phrase)
    else:
        line = FALLBACK_CAUSE_LINE
    return evidence, line


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
        "driver_keywords": list(evidence.driver_keywords),
        "confirmed_cluster": evidence.confirmed_cluster,
        "confirmed_cause_phrase": evidence.confirmed_cause_phrase,
        "corroborated_sources": evidence.corroborated_sources,
        "corroborated_domains": evidence.corroborated_domains,
        "line": line,
        "top_evidence": list(evidence.top_evidence),
        "rejected_reasons": list(evidence.rejected_reasons),
        "links": {
            "x": evidence.x_url,
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
    message = _build_message(
        slot_name=slot,
        now_local=now_local,
        universe_count=len(final_universe),
        movers=movers,
        catalyst_rows=catalyst_rows,
        catalyst_lines=catalyst_lines,
    )
    artifact = _write_artifact(
        slot_name=slot,
        now_local=now_local,
        universe=final_universe,
        movers=movers,
        catalyst_rows=catalyst_rows,
        catalyst_lines=catalyst_lines,
        message_text=message,
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
    }


def status() -> dict[str, Any]:
    store = MarketDailyStore()
    return {
        "ok": True,
        "db_path": str(store.db_path),
        "timezone": os.environ.get("COATUE_CLAW_MD_TZ", DEFAULT_TZ),
        "times": ",".join(f"{h:02d}:{m:02d}" for h, m in _parse_times()),
        "channel": _channel_default(),
        "top_n": _top_n(),
        "top_k": _top_k(),
        "x_max_results": _x_max_results(),
        "max_lookback_hours": _max_lookback_hours(),
        "web_search_enabled": _web_search_enabled(),
        "web_search_backend": _web_search_backend(),
        "web_max_results": _web_max_results(),
        "min_evidence_confidence": _min_evidence_confidence(),
        "seed_path": str(_seed_path()),
        "recent_runs": store.latest_runs(limit=10),
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

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
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


def _hours_for_slot(slot_name: str) -> int:
    return 18 if slot_name == "open" else 8


def _fetch_x_evidence(*, ticker: str, hours: int) -> tuple[str | None, str | None, int]:
    token = _x_bearer_token()
    if not token:
        return (None, None, 0)

    now = datetime.now(UTC)
    start = (now - timedelta(hours=max(1, hours))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    query = f"({ticker}) (earnings OR guidance OR contract OR launch OR upgrade OR downgrade OR demand OR margin OR revenue) -is:retweet -is:reply"
    params = {
        "query": query,
        "max_results": "10",
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
        return (None, None, 0)

    users: dict[str, str] = {}
    includes = payload.get("includes")
    if isinstance(includes, dict):
        for user in includes.get("users") or []:
            if isinstance(user, dict):
                uid = str(user.get("id") or "").strip()
                uname = str(user.get("username") or "").strip()
                if uid and uname:
                    users[uid] = uname

    best_text: str | None = None
    best_url: str | None = None
    best_engagement = -1
    for row in payload.get("data") or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_whitespace(str(row.get("text") or ""))
        tweet_id = str(row.get("id") or "").strip()
        author_id = str(row.get("author_id") or "").strip()
        if not text or not tweet_id:
            continue
        metrics = row.get("public_metrics") if isinstance(row.get("public_metrics"), dict) else {}
        engagement = int(metrics.get("like_count", 0)) + int(metrics.get("retweet_count", 0)) + int(metrics.get("reply_count", 0)) + int(metrics.get("quote_count", 0))
        if engagement > best_engagement:
            best_engagement = engagement
            best_text = text
            author = users.get(author_id)
            best_url = f"https://x.com/{author}/status/{tweet_id}" if author else f"https://x.com/i/web/status/{tweet_id}"
    if best_engagement < 0:
        return (None, None, 0)
    return (best_text, best_url, best_engagement)


def _fetch_yahoo_news(*, ticker: str, hours: int) -> tuple[str | None, str | None]:
    try:
        news = yf.Ticker(ticker).news or []
    except Exception:
        return (None, None)

    cutoff = datetime.now(UTC) - timedelta(hours=max(1, hours))
    best_title: str | None = None
    best_url: str | None = None
    best_ts = 0
    for item in news:
        if not isinstance(item, dict):
            continue
        ts = int(item.get("providerPublishTime") or 0)
        if ts <= 0:
            continue
        dt = datetime.fromtimestamp(ts, tz=UTC)
        if dt < cutoff:
            continue
        title = _normalize_whitespace(str(item.get("title") or ""))
        link = str(item.get("link") or "").strip()
        if title and link and ts > best_ts:
            best_ts = ts
            best_title = title
            best_url = link
    return (best_title, best_url)


def _summarize_catalyst(*, ticker: str, slot_name: str, evidence: CatalystEvidence) -> str:
    fallback = "No clear single catalyst; likely positioning/flow"
    x_text = evidence.x_text or ""
    news_title = evidence.news_title or ""
    if not x_text and not news_title:
        return fallback

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or not api_key:
        base = news_title or x_text
        return _shorten(base or fallback, 110)

    prompt = (
        "Write one plain-English catalyst line for a market mover.\n"
        "Rules: <=110 chars, coherent sentence fragment, no emoji, no hype, no ticker symbol repetition.\n"
        f"Slot: {slot_name}\n"
        f"Ticker: {ticker}\n"
        f"X evidence: {x_text}\n"
        f"News evidence: {news_title}\n"
        "Return only the catalyst line."
    )
    model = (os.environ.get("COATUE_CLAW_MD_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip()
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "You write concise institutional market summaries."},
                {"role": "user", "content": prompt},
            ],
        )
        text = ""
        if response.choices and response.choices[0].message:
            text = str(response.choices[0].message.content or "").strip()
        text = _normalize_whitespace(text)
        if text:
            return _shorten(text, 110)
    except Exception:
        pass

    return _shorten(news_title or x_text or fallback, 110)


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
    lines = [f"*{slot_title} | As of {as_of}*"]
    lines.append(f"Universe: Top 40 US TMT + Coatue overlay ({universe_count} names)")

    for idx, mover in enumerate(movers):
        ev = catalyst_rows[idx] if idx < len(catalyst_rows) else CatalystEvidence(ticker=mover.ticker, x_text=None, x_url=None, x_engagement=0, news_title=None, news_url=None)
        cat = catalyst_lines[idx] if idx < len(catalyst_lines) else "No clear single catalyst; likely positioning/flow"
        links: list[str] = []
        if ev.x_url:
            links.append(f"<{ev.x_url}|[X]>")
        if ev.news_url:
            links.append(f"<{ev.news_url}|[News]>")
        link_text = f" {' '.join(links)}" if links else ""
        lines.append(f"- {mover.ticker} {_format_pct(mover.pct_move)} — {cat}{link_text}")

    lines.append(f"Data UTC: {_utc_now_iso()} | Sources: Yahoo fast_info + X recent search + Yahoo news")
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
        ev = catalyst_rows[idx] if idx < len(catalyst_rows) else CatalystEvidence(ticker=mover.ticker, x_text=None, x_url=None, x_engagement=0, news_title=None, news_url=None)
        cat = catalyst_lines[idx] if idx < len(catalyst_lines) else ""
        lines.append(f"- `{mover.ticker}` pct_move `{_format_pct(mover.pct_move)}` market_cap `{mover.market_cap}`")
        lines.append(f"  - catalyst: {cat}")
        if ev.x_url:
            lines.append(f"  - x: {ev.x_url}")
        if ev.news_url:
            lines.append(f"  - news: {ev.news_url}")

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


def _build_catalyst_rows(*, movers: list[QuoteSnapshot], slot_name: str) -> tuple[list[CatalystEvidence], list[str]]:
    hours = _hours_for_slot(slot_name)
    rows: list[CatalystEvidence] = []
    lines: list[str] = []
    for mover in movers:
        x_text, x_url, x_eng = _fetch_x_evidence(ticker=mover.ticker, hours=hours)
        news_title, news_url = _fetch_yahoo_news(ticker=mover.ticker, hours=hours)
        evidence = CatalystEvidence(
            ticker=mover.ticker,
            x_text=x_text,
            x_url=x_url,
            x_engagement=x_eng,
            news_title=news_title,
            news_url=news_url,
        )
        rows.append(evidence)
        lines.append(_summarize_catalyst(ticker=mover.ticker, slot_name=slot_name, evidence=evidence))
    return rows, lines


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
    else:
        result = set_override(ticker=args.ticker, action="exclude")

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    _main()

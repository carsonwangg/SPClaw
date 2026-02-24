from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
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
    from slack_sdk.errors import SlackApiError
except Exception:  # pragma: no cover - optional dependency for non-Slack test envs
    WebClient = None  # type: ignore[assignment]
    SlackApiError = Exception  # type: ignore[assignment]


load_dotenv("/opt/coatue-claw/.env.prod")

DEFAULT_TZ = "America/Los_Angeles"
DEFAULT_PORTCOS: list[tuple[str, str]] = [
    ("Anduril", "anduril"),
    ("Anthropic", "anthropic"),
    ("Cursor", "cursor"),
    ("Neuralink", "neuralink"),
    ("OpenAI", "openai"),
    ("Physical Intelligence", "physical-intelligence"),
    ("Ramp", "ramp"),
    ("SpaceX", "spacex"),
    ("Stripe", "stripe"),
    ("Sunday Robotics", "sunday-robotics"),
]

PITCH_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "were",
    "with",
}

SIGNIFICANT_CHANGE_TERMS = {
    "acquisition",
    "ai",
    "backlog",
    "burn",
    "capex",
    "churn",
    "contract",
    "customer",
    "earnings",
    "funding",
    "guidance",
    "hiring",
    "launch",
    "lawsuit",
    "margin",
    "partnership",
    "pricing",
    "product",
    "regulatory",
    "revenue",
    "risk",
}

BOARD_SEAT_HEADER_RE = re.compile(r"board seat as a service\s*[—-]\s*(.+)$", re.IGNORECASE)
BOARD_SEAT_FORMAT_VERSION = "v5_target_first_confidence_sources"
MAX_LINE_WORDS = 18
THESIS_LABELS: tuple[str, ...] = ("Idea", "Why now", "What's different", "MOS/risks", "Bottom line")
CONTEXT_LABELS: tuple[str, ...] = ("Current efforts", "Domain fit/gaps")
FUNDING_LABELS: tuple[str, ...] = ("History", "Latest round/backers")
FUNDING_CACHE_TTL_DAYS_DEFAULT = 14
UNKNOWN_FUNDING_TEXT = "Funding details are currently unavailable."
WEB_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
BRAVE_SEARCH_RESULTS = 5
FUNDING_EXTRACT_MODEL = "gpt-5.2-chat-latest"
ACQ_SEARCH_RESULTS = 6
TARGET_SEARCH_RESULTS = 10
ACQ_PLACEHOLDER_TARGETS = {
    "tbd",
    "unknown",
    "none",
    "n/a",
    "startup",
    "company",
    "target",
    "no",
    "stealth",
    "stealthaisystems",
}
ACQ_INVALID_TARGET_TERMS = {"startup team", "domain-adjacent", "internal", "in-house"}
SOURCE_POLICY_DEFAULT = "target_first_3_1"
LOW_SIGNAL_MODE_DEFAULT = "candidate_with_confidence"
TARGET_CONFIDENCE_LEVELS = {"High", "Medium", "Low"}
DEFAULT_TARGET_BY_COMPANY: dict[str, str] = {
    "anduril": "Saronic",
    "anthropic": "Langfuse",
    "cursor": "Sourcegraph",
    "neuralink": "Blackrock Neurotech",
    "openai": "Browserbase",
    "physicalintelligence": "Covariant",
    "ramp": "Brex",
    "spacex": "K2 Space",
    "stripe": "Modern Treasury",
    "sundayrobotics": "Viam",
}
FUNDING_CONTEXT_TERMS = {
    "funding",
    "series ",
    "valuation",
    "backers",
    "raised",
    "round",
    "investors",
    "softbank",
    "reuters",
}
TARGET_PROXY_TERMS = {
    "browser",
    "browser automation",
    "agent",
    "agentic",
    "runtime",
    "workflow",
    "security",
    "governance",
    "compliance",
    "telemetry",
    "computer use",
    "automation",
    "enterprise",
    "control plane",
}
SOURCE_QUALITY_DOMAIN_SUFFIXES = {
    "openai.com",
    "anthropic.com",
    "browserbase.com",
    "sec.gov",
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "marketwatch.com",
    "finance.yahoo.com",
    "techcrunch.com",
    "theinformation.com",
    "axios.com",
    "investing.com",
    "stocktwits.com",
    "fool.com",
    "seekingalpha.com",
}
SOURCE_LOW_QUALITY_DOMAIN_SUFFIXES = {
    "reddit.com",
    "wikipedia.org",
    "medium.com",
    "substack.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "tiktok.com",
}
TARGET_TOKEN_STOPWORDS = {
    "inc",
    "corp",
    "corporation",
    "company",
    "holdings",
    "technologies",
    "technology",
    "systems",
    "group",
    "llc",
    "ltd",
}


@dataclass(frozen=True)
class FundingSnapshot:
    history: str
    latest_round: str
    latest_date: str
    backers: list[str]
    source_urls: list[str]
    source_type: str
    as_of_utc: str
    confidence: float = 0.0


@dataclass(frozen=True)
class SourceRef:
    name_or_publisher: str
    title: str
    url: str


@dataclass(frozen=True)
class SourceCandidate:
    ref: SourceRef
    category: str
    quality: int
    score: float
    text_blob: str


@dataclass(frozen=True)
class SourceSelection:
    refs: list[SourceRef]
    confidence: str


@dataclass(frozen=True)
class BoardSeatDraft:
    idea_line: str
    idea_confidence: str
    why_now: str
    whats_different: str
    mos_risks: str
    bottom_line: str
    context_current_efforts: str
    context_domain_fit_gaps: str
    funding_history: str
    funding_latest_round_backers: str
    source_refs: list[SourceRef] = field(default_factory=list)
    raw_model_output: str = ""
    rewrite_reasons: list[str] = field(default_factory=list)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _timezone() -> ZoneInfo:
    name = (os.environ.get("COATUE_CLAW_BOARD_SEAT_TZ", DEFAULT_TZ) or "").strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)


def _today_key() -> str:
    return datetime.now(_timezone()).strftime("%Y-%m-%d")


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_DB_PATH",
            str(_data_root() / "db/board_seat_daily.sqlite"),
        )
    )


def _funding_ttl_days() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_TTL_DAYS", str(FUNDING_CACHE_TTL_DAYS_DEFAULT)) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = FUNDING_CACHE_TTL_DAYS_DEFAULT
    return max(1, min(90, value))


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.environ.get(name, "1" if default else "0") or "").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.environ.get(name, str(default)) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def _source_policy() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_SOURCE_POLICY", SOURCE_POLICY_DEFAULT) or SOURCE_POLICY_DEFAULT).strip()


def _include_funding_links() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_INCLUDE_FUNDING_LINKS", False)


def _target_min_quality_sources() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_TARGET_MIN_QUALITY_SOURCES", 1, minimum=1, maximum=4)


def _target_min_total_sources() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_TARGET_MIN_TOTAL_SOURCES", 2, minimum=1, maximum=4)


def _low_signal_mode() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_LOW_SIGNAL_MODE", LOW_SIGNAL_MODE_DEFAULT) or LOW_SIGNAL_MODE_DEFAULT).strip()


def _manual_default_targets() -> dict[str, str]:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_DEFAULT_TARGETS", "") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in payload.items():
        company_key = _slug_company(str(key or ""))
        target = _normalize_source_text(str(value or ""), max_chars=80)
        if not company_key or not target:
            continue
        out[company_key] = target
    return out


def _manual_funding_path() -> Path | None:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH", "") or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    return path if path.exists() else None


def _slug_company(text: str) -> str:
    return _slug(text).replace("-", "")


def _brave_search_api_key() -> str:
    for key in ("COATUE_CLAW_BRAVE_API_KEY", "BRAVE_SEARCH_API_KEY"):
        value = (os.environ.get(key, "") or "").strip()
        if value:
            return value
    config_path = Path.home() / ".openclaw/openclaw.json"
    if not config_path.exists():
        return ""
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    tools = payload.get("tools") if isinstance(payload, dict) else None
    web = tools.get("web") if isinstance(tools, dict) else None
    search = web.get("search") if isinstance(web, dict) else None
    value = str((search or {}).get("apiKey") or "").strip() if isinstance(search, dict) else ""
    return value


def _load_manual_funding_seed() -> dict[str, FundingSnapshot]:
    path = _manual_funding_path()
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, FundingSnapshot] = {}
    for key, row in payload.items():
        if not isinstance(row, dict):
            continue
        company_key = _slug_company(str(key or ""))
        if not company_key:
            continue
        history = _normalize_text(str(row.get("history") or ""), max_chars=500)
        latest_round = _normalize_text(str(row.get("latest_round") or ""), max_chars=200)
        latest_date = _normalize_text(str(row.get("latest_date") or ""), max_chars=80)
        backers = [str(item).strip() for item in row.get("backers", []) if str(item).strip()] if isinstance(row.get("backers"), list) else []
        source_urls = [str(item).strip() for item in row.get("source_urls", []) if str(item).strip()] if isinstance(row.get("source_urls"), list) else []
        out[company_key] = FundingSnapshot(
            history=history,
            latest_round=latest_round,
            latest_date=latest_date,
            backers=backers[:8],
            source_urls=source_urls[:8],
            source_type="manual_seed",
            as_of_utc=_utc_now_iso(),
            confidence=float(row.get("confidence") or 0.9),
        )
    return out


def _normalize_line_text(text: str) -> str:
    cleaned = _normalize_text(str(text or ""), max_chars=420)
    cleaned = cleaned.strip().lstrip("-").lstrip("•").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" ;,")
    return cleaned


def _limit_words(text: str, *, max_words: int = MAX_LINE_WORDS) -> str:
    words = str(text or "").split()
    if len(words) <= max_words:
        return " ".join(words).strip()
    return " ".join(words[:max_words]).strip()

def _normalize_line(text: str, *, max_words: int = MAX_LINE_WORDS) -> str:
    return _limit_words(_normalize_line_text(text), max_words=max_words)


def _normalize_line_list(items: list[str], *, max_items: int, max_words: int = MAX_LINE_WORDS) -> list[str]:
    out: list[str] = []
    for item in items:
        line = _normalize_line(item, max_words=max_words)
        if not line:
            continue
        out.append(line)
        if len(out) >= max_items:
            break
    return out


def _normalize_source_url(url: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(url or "")).strip()
    if cleaned.startswith("<") and cleaned.endswith(">"):
        cleaned = cleaned[1:-1].strip()
    if "|" in cleaned:
        cleaned = cleaned.split("|", 1)[0].strip()
    cleaned = cleaned.strip("<>").strip()
    cleaned = cleaned.rstrip(".,;)")
    if not re.match(r"^https?://", cleaned, flags=re.IGNORECASE):
        return ""
    return cleaned[:320]


def _publisher_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return "Web"
    primary = host.split(".")[0]
    name = primary.replace("-", " ").replace("_", " ").strip()
    if not name:
        return "Web"
    return " ".join(piece.capitalize() for piece in name.split())


def _normalize_source_text(text: str, *, max_chars: int) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip().strip(" -:;,.")
    return cleaned[:max_chars]


def _normalize_source_ref(ref: SourceRef) -> SourceRef | None:
    url = _normalize_source_url(ref.url)
    if not url:
        return None
    name = _normalize_source_text(ref.name_or_publisher, max_chars=64) or _publisher_from_url(url)
    title = _normalize_source_text(ref.title, max_chars=180) or "Reference"
    return SourceRef(name_or_publisher=name, title=title, url=url)


def _normalize_source_refs(refs: list[SourceRef], *, max_items: int = 4) -> list[SourceRef]:
    out: list[SourceRef] = []
    seen: set[str] = set()
    for ref in refs:
        normalized = _normalize_source_ref(ref)
        if normalized is None:
            continue
        key = normalized.url.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
        if len(out) >= max_items:
            break
    return out


def _source_refs_from_urls(urls: list[str], *, title_hint: str) -> list[SourceRef]:
    out: list[SourceRef] = []
    for raw in urls:
        url = _normalize_source_url(raw)
        if not url:
            continue
        out.append(
            SourceRef(
                name_or_publisher=_publisher_from_url(url),
                title=title_hint,
                url=url,
            )
        )
    return out


def _source_domain(url: str) -> str:
    parsed = urlparse(url)
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _domain_matches(host: str, domain_suffixes: set[str]) -> bool:
    if not host:
        return False
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in domain_suffixes)


def _is_quality_source(url: str) -> bool:
    host = _source_domain(url)
    return _domain_matches(host, SOURCE_QUALITY_DOMAIN_SUFFIXES)


def _is_low_quality_source(url: str) -> bool:
    host = _source_domain(url)
    return _domain_matches(host, SOURCE_LOW_QUALITY_DOMAIN_SUFFIXES)


def _title_fingerprint(text: str) -> str:
    tokens = re.findall(r"[a-z0-9]{3,}", str(text or "").lower())
    return " ".join(tokens[:16]).strip()


def _extract_target_tokens_from_idea(idea_line: str) -> tuple[str, set[str]]:
    target = _extract_acquisition_target(idea_line)
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", target.lower())
    tokens = {
        tok
        for tok in cleaned.split()
        if len(tok) >= 3 and tok not in PITCH_STOPWORDS and tok not in TARGET_TOKEN_STOPWORDS
    }
    return target, tokens


def _matches_any_token(text: str, tokens: set[str]) -> bool:
    if not text or not tokens:
        return False
    return any(re.search(rf"\b{re.escape(token)}\b", text) for token in tokens)


def _classify_source_ref(
    *,
    company: str,
    target: str,
    target_tokens: set[str],
    text_blob: str,
) -> str:
    blob = str(text_blob or "").lower()
    company_tokens = set(_tokenize(company))
    target_blob = str(target or "").strip().lower()
    has_target_phrase = bool(target_blob and target_blob in blob)
    has_target_token = _matches_any_token(blob, target_tokens)
    has_proxy = any(term in blob for term in TARGET_PROXY_TERMS)
    has_funding = any(term in blob for term in FUNDING_CONTEXT_TERMS)
    has_parent = _matches_any_token(blob, company_tokens)
    if has_funding:
        return "funding_context"
    if has_target_phrase or has_target_token:
        return "target_direct"
    if has_parent:
        return "parent_context"
    if has_proxy:
        return "target_proxy"
    return "target_proxy"


def _target_search_rows(*, target: str, company: str, snippets: list[str]) -> list[dict[str, str]]:
    api_key = _brave_search_api_key()
    if not api_key:
        return []
    hints = " ".join(snippets[:2]).strip()
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
        "User-Agent": "CoatueClaw/1.0",
    }
    queries = [
        f"{target} company product enterprise",
        f"{target} browser automation security runtime",
        f"{company} acquire {target}",
        f"{target} traction customers funding",
    ]
    if hints:
        queries.append(f"{target} {hints}")

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for query in queries:
        try:
            payload = _http_json(
                url=WEB_SEARCH_ENDPOINT,
                headers=headers,
                params={"q": query, "count": str(TARGET_SEARCH_RESULTS), "country": "us", "search_lang": "en"},
            )
        except Exception:
            continue
        web = payload.get("web") if isinstance(payload, dict) else None
        results = web.get("results") if isinstance(web, dict) else None
        if not isinstance(results, list):
            continue
        for item in results:
            if not isinstance(item, dict):
                continue
            url = _normalize_source_url(str(item.get("url") or ""))
            if not url:
                continue
            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            title = _normalize_source_text(str(item.get("title") or ""), max_chars=180)
            snippet = _normalize_text(str(item.get("description") or ""), max_chars=420)
            rows.append(
                {
                    "publisher": _publisher_from_url(url),
                    "title": title or _normalize_source_text(snippet, max_chars=180) or "Reference",
                    "snippet": snippet,
                    "url": url,
                }
            )
            if len(rows) >= TARGET_SEARCH_RESULTS:
                return rows
    return rows


def _source_selection_confidence(candidates: list[SourceCandidate]) -> str:
    target_candidates = [item for item in candidates if item.category in {"target_direct", "target_proxy"}]
    quality_target = [item for item in target_candidates if item.quality > 0]
    quality_domains = {_source_domain(item.ref.url) for item in quality_target}
    direct_quality = [item for item in quality_target if item.category == "target_direct"]
    if len(target_candidates) >= 3 and len(quality_domains) >= 2:
        return "High"
    if len(direct_quality) >= _target_min_quality_sources() and len(target_candidates) >= _target_min_total_sources():
        return "Medium"
    return "Low"


def _normalize_confidence_label(value: str, *, fallback: str = "Low") -> str:
    normalized = str(value or "").strip().capitalize()
    if normalized in TARGET_CONFIDENCE_LEVELS:
        return normalized
    return fallback


def _fallback_source_refs(company: str, *, target: str = "") -> list[SourceRef]:
    target_query = target or f"{company} strategic acquisition target"
    return [
        SourceRef(
            name_or_publisher="Google Search",
            title=f"{target_query} enterprise fit",
            url=f"https://www.google.com/search?{urlencode({'q': f'{target_query} company product customers'})}",
        ),
        SourceRef(
            name_or_publisher="Google Search",
            title=f"{target_query} security runtime",
            url=f"https://www.google.com/search?{urlencode({'q': f'{target_query} security automation runtime'})}",
        ),
    ]


def _message_source_refs(*, company: str, draft: BoardSeatDraft) -> list[SourceRef]:
    refs = _normalize_source_refs(draft.source_refs, max_items=4)
    if refs:
        return refs
    return _fallback_source_refs(company, target=_extract_acquisition_target(draft.idea_line))


def _format_source_ref_for_slack(ref: SourceRef) -> str:
    return f"*{ref.name_or_publisher} — {ref.title}:* <{ref.url}>"

def _slack_tokens() -> list[str]:
    tokens: list[str] = []
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if token:
        tokens.append(token)
    config_path = Path.home() / ".openclaw/openclaw.json"
    if config_path.exists():
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            candidate = str(payload.get("channels", {}).get("slack", {}).get("botToken", "")).strip()
            if candidate:
                tokens.append(candidate)
        except Exception:
            pass
    unique: list[str] = []
    seen: set[str] = set()
    for item in tokens:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    if not unique:
        raise RuntimeError("Slack bot token missing (SLACK_BOT_TOKEN or ~/.openclaw/openclaw.json).")
    return unique


def _slug(text: str) -> str:
    out = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower())
    return out.strip("-")


def _parse_portcos(raw: str | None = None) -> list[tuple[str, str]]:
    value = (raw if raw is not None else os.environ.get("COATUE_CLAW_BOARD_SEAT_PORTCOS", "")).strip()
    if not value:
        return list(DEFAULT_PORTCOS)
    out: list[tuple[str, str]] = []
    for part in value.split(","):
        item = part.strip()
        if not item:
            continue
        if ":" in item:
            company, channel_ref = item.split(":", 1)
            clean_company = company.strip()
            clean_channel = channel_ref.strip().lstrip("#")
            if clean_company and clean_channel:
                out.append((clean_company, clean_channel))
            continue
        company = item.strip()
        if company:
            out.append((company, _slug(company)))
    return out or list(DEFAULT_PORTCOS)


class BoardSeatStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()
        self._seed_pitches_from_runs()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_runs (
                    run_date_local TEXT NOT NULL,
                    company TEXT NOT NULL,
                    channel_ref TEXT NOT NULL,
                    channel_id TEXT,
                    posted_at_utc TEXT NOT NULL,
                    message_ts TEXT,
                    summary TEXT NOT NULL,
                    PRIMARY KEY (run_date_local, company)
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_runs_recent ON board_seat_runs(posted_at_utc DESC);"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_pitches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    channel_ref TEXT NOT NULL,
                    channel_id TEXT,
                    source TEXT NOT NULL,
                    posted_at_utc TEXT NOT NULL,
                    message_ts TEXT,
                    run_date_local TEXT,
                    message_text TEXT NOT NULL,
                    investment_text TEXT NOT NULL,
                    investment_hash TEXT NOT NULL,
                    investment_signature TEXT NOT NULL,
                    context_signature TEXT NOT NULL,
                    context_snippets_json TEXT NOT NULL DEFAULT '[]',
                    significant_change INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_pitches_company_recent ON board_seat_pitches(company, posted_at_utc DESC);"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_board_seat_pitches_message_ts ON board_seat_pitches(message_ts) WHERE message_ts IS NOT NULL;"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_funding_cache (
                    company TEXT PRIMARY KEY,
                    history TEXT NOT NULL,
                    latest_round TEXT NOT NULL,
                    latest_date TEXT NOT NULL,
                    backers_json TEXT NOT NULL,
                    source_urls_json TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    as_of_utc TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0.0
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_funding_cache_asof ON board_seat_funding_cache(as_of_utc DESC);"
            )

    def _seed_pitches_from_runs(self) -> None:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    run_date_local,
                    company,
                    channel_ref,
                    channel_id,
                    posted_at_utc,
                    message_ts,
                    summary
                FROM board_seat_runs
                ORDER BY posted_at_utc ASC
                """
            ).fetchall()
        for row in rows:
            message_text = str(row["summary"] or "").strip()
            if not message_text:
                continue
            investment_text = _extract_investment_text(message_text)
            core_investment_text = _core_investment_text(message_text)
            investment_signature = _token_signature(core_investment_text)
            context_snippets = [core_investment_text] if core_investment_text else []
            self.record_pitch(
                company=str(row["company"] or ""),
                channel_ref=str(row["channel_ref"] or ""),
                channel_id=(str(row["channel_id"] or "").strip() or None),
                source="legacy_run_seed",
                message_ts=(str(row["message_ts"] or "").strip() or None),
                run_date_local=(str(row["run_date_local"] or "").strip() or None),
                posted_at_utc=str(row["posted_at_utc"] or _utc_now_iso()),
                message_text=message_text,
                investment_text=investment_text,
                investment_hash=_stable_hash(investment_signature or core_investment_text or investment_text),
                investment_signature=investment_signature,
                context_signature=_context_signature_from_snippets(context_snippets),
                context_snippets=context_snippets,
                significant_change=False,
            )

    def already_posted(self, *, run_date_local: str, company: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM board_seat_runs
                WHERE run_date_local = ? AND company = ?
                LIMIT 1
                """,
                (run_date_local, company),
            ).fetchone()
        return row is not None

    def record_post(
        self,
        *,
        run_date_local: str,
        company: str,
        channel_ref: str,
        channel_id: str | None,
        message_ts: str | None,
        summary: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO board_seat_runs (
                    run_date_local, company, channel_ref, channel_id, posted_at_utc, message_ts, summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_date_local,
                    company,
                    channel_ref,
                    channel_id,
                    _utc_now_iso(),
                    message_ts,
                    summary,
                ),
            )

    def recent_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_date_local, company, channel_ref, channel_id, posted_at_utc, message_ts, summary
                FROM board_seat_runs
                ORDER BY posted_at_utc DESC
                LIMIT ?
                """,
                (max(1, min(200, int(limit))),),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_pitch(
        self,
        *,
        company: str,
        channel_ref: str,
        channel_id: str | None,
        source: str,
        message_ts: str | None,
        run_date_local: str | None,
        posted_at_utc: str,
        message_text: str,
        investment_text: str,
        investment_hash: str,
        investment_signature: str,
        context_signature: str,
        context_snippets: list[str],
        significant_change: bool,
    ) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO board_seat_pitches (
                    company,
                    channel_ref,
                    channel_id,
                    source,
                    posted_at_utc,
                    message_ts,
                    run_date_local,
                    message_text,
                    investment_text,
                    investment_hash,
                    investment_signature,
                    context_signature,
                    context_snippets_json,
                    significant_change
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company,
                    channel_ref,
                    channel_id,
                    source,
                    posted_at_utc,
                    message_ts,
                    run_date_local,
                    message_text,
                    investment_text,
                    investment_hash,
                    investment_signature,
                    context_signature,
                    json.dumps(context_snippets, ensure_ascii=False),
                    1 if significant_change else 0,
                ),
            )
        return bool(cur.rowcount)

    def recent_pitches(self, *, company: str, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    company,
                    channel_ref,
                    channel_id,
                    source,
                    posted_at_utc,
                    message_ts,
                    run_date_local,
                    message_text,
                    investment_text,
                    investment_hash,
                    investment_signature,
                    context_signature,
                    context_snippets_json,
                    significant_change
                FROM board_seat_pitches
                WHERE company = ?
                ORDER BY posted_at_utc DESC
                LIMIT ?
                """,
                (company, max(1, min(500, int(limit)))),
            ).fetchall()
        return [dict(row) for row in rows]

    def pitch_count(self, *, company: str | None = None) -> int:
        with self._connect() as conn:
            if company:
                row = conn.execute(
                    "SELECT COUNT(1) AS n FROM board_seat_pitches WHERE company = ?",
                    (company,),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(1) AS n FROM board_seat_pitches").fetchone()
        return int(row["n"]) if row is not None else 0

    def get_funding_snapshot(self, *, company: str) -> FundingSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT company, history, latest_round, latest_date, backers_json, source_urls_json, source_type, as_of_utc, confidence
                FROM board_seat_funding_cache
                WHERE company = ?
                LIMIT 1
                """,
                (company,),
            ).fetchone()
        if row is None:
            return None
        try:
            backers = json.loads(str(row["backers_json"] or "[]"))
            if not isinstance(backers, list):
                backers = []
        except Exception:
            backers = []
        try:
            source_urls = json.loads(str(row["source_urls_json"] or "[]"))
            if not isinstance(source_urls, list):
                source_urls = []
        except Exception:
            source_urls = []
        return FundingSnapshot(
            history=str(row["history"] or ""),
            latest_round=str(row["latest_round"] or ""),
            latest_date=str(row["latest_date"] or ""),
            backers=[str(item).strip() for item in backers if str(item).strip()],
            source_urls=[str(item).strip() for item in source_urls if str(item).strip()],
            source_type=str(row["source_type"] or "unknown").strip() or "unknown",
            as_of_utc=str(row["as_of_utc"] or _utc_now_iso()),
            confidence=float(row["confidence"] if row["confidence"] is not None else 0.0),
        )

    def upsert_funding_snapshot(self, *, company: str, snapshot: FundingSnapshot) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_funding_cache (
                    company, history, latest_round, latest_date, backers_json, source_urls_json, source_type, as_of_utc, confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company) DO UPDATE SET
                    history = excluded.history,
                    latest_round = excluded.latest_round,
                    latest_date = excluded.latest_date,
                    backers_json = excluded.backers_json,
                    source_urls_json = excluded.source_urls_json,
                    source_type = excluded.source_type,
                    as_of_utc = excluded.as_of_utc,
                    confidence = excluded.confidence
                """,
                (
                    company,
                    snapshot.history,
                    snapshot.latest_round,
                    snapshot.latest_date,
                    json.dumps(snapshot.backers, ensure_ascii=False),
                    json.dumps(snapshot.source_urls, ensure_ascii=False),
                    snapshot.source_type,
                    snapshot.as_of_utc,
                    float(snapshot.confidence),
                ),
            )

    def funding_cache_age_days(self, *, company: str) -> float | None:
        snapshot = self.get_funding_snapshot(company=company)
        if snapshot is None:
            return None
        try:
            as_of = datetime.fromisoformat(snapshot.as_of_utc.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            return None
        delta = datetime.now(UTC) - as_of
        return max(0.0, round(delta.total_seconds() / 86400.0, 2))


def _normalize_text(text: str, *, max_chars: int = 240) -> str:
    cleaned = re.sub(r"https?://\S+", "", text or "")
    cleaned = re.sub(r"<@[^>]+>", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_chars]


def _tokenize(text: str) -> list[str]:
    words = re.findall(r"[a-z0-9]{3,}", (text or "").lower())
    return [w for w in words if w not in PITCH_STOPWORDS]


def _token_signature(text: str, *, max_tokens: int = 64) -> str:
    tokens = _tokenize(_normalize_text(text, max_chars=4000))
    seen: set[str] = set()
    out: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= max_tokens:
            break
    return " ".join(out).strip()


def _stable_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _extract_investment_sections(message: str) -> dict[str, list[str]]:
    def _label_key(text: str) -> str:
        key = str(text or "").lower().replace("’", "'")
        key = re.sub(r"\s+", " ", key).strip()
        return key

    label_to_section = {
        _label_key("Idea"): "thesis",
        _label_key("Why now"): "thesis",
        _label_key("What's different"): "thesis",
        _label_key("MOS/risks"): "thesis",
        _label_key("Bottom line"): "thesis",
        _label_key("Current efforts"): "context",
        _label_key("Domain fit/gaps"): "context",
        _label_key("History"): "funding",
        _label_key("Latest round/backers"): "funding",
    }

    lines = [line.strip() for line in str(message or "").splitlines() if line.strip()]
    sections: dict[str, list[str]] = {"thesis": [], "context": [], "funding": []}
    active: str | None = None
    for line in lines:
        lower = line.lower().strip("* ")
        if lower == "thesis":
            active = "thesis"
            continue
        if lower.endswith(" context"):
            active = "context"
            continue
        if lower == "funding snapshot":
            active = "funding"
            continue
        plain = re.sub(r"^\s*[-•]\s*", "", line).replace("*", "").strip()
        if ":" in plain:
            label, value = plain.split(":", 1)
            section = label_to_section.get(_label_key(label))
            if section:
                cleaned = _normalize_line(value)
                if cleaned:
                    sections[section].append(cleaned)
                continue
        if lower.startswith("- signal:"):
            sections["thesis"].append(line.split(":", 1)[1].strip())
            active = None
            continue
        if lower.startswith("- board lens:") or lower.startswith("- watchlist:"):
            sections["context"].append(line.split(":", 1)[1].strip())
            active = None
            continue
        if lower.startswith("- team ask:"):
            active = None
            continue
        if active and line.startswith("- "):
            sections[active].append(line[2:].strip())
    for key, max_items in (("thesis", 5), ("context", 2), ("funding", 2)):
        sections[key] = _normalize_line_list(sections[key], max_items=max_items)
    return sections


def _extract_investment_text(message: str) -> str:
    sections = _extract_investment_sections(message)
    combined: list[str] = []
    combined.extend(sections.get("thesis", []))
    combined.extend(sections.get("context", []))
    combined.extend(sections.get("funding", []))
    if combined:
        return _normalize_text(" | ".join(combined), max_chars=1200)

    lines = [line for line in str(message or "").splitlines() if line.strip()]
    fallback = [line.strip() for line in lines if "board seat as a service" not in line.lower()]
    return _normalize_text(" | ".join(fallback[:4]), max_chars=1200)


def _core_investment_text(message: str) -> str:
    sections = _extract_investment_sections(message)
    core = [*sections.get("thesis", []), *sections.get("context", [])]
    if core:
        return _normalize_text(" | ".join(core), max_chars=1200)
    return _extract_investment_text(message)


def _render_board_seat_message(*, company: str, draft: BoardSeatDraft) -> str:
    source_refs = _message_source_refs(company=company, draft=draft)
    lines = [
        f"*Board Seat as a Service — {company}*",
        "",
        "*Thesis*",
        f"*Idea:* {draft.idea_line}",
        f"*Idea confidence:* {draft.idea_confidence}",
        f"*Why now:* {draft.why_now}",
        f"*What's different:* {draft.whats_different}",
        f"*MOS/risks:* {draft.mos_risks}",
        f"*Bottom line:* {draft.bottom_line}",
        "",
        f"*{company} context*",
        f"*Current efforts:* {draft.context_current_efforts}",
        f"*Domain fit/gaps:* {draft.context_domain_fit_gaps}",
        "",
        "*Funding snapshot*",
        f"*History:* {draft.funding_history}",
        f"*Latest round/backers:* {draft.funding_latest_round_backers}",
        "",
        "*Sources*",
        *[_format_source_ref_for_slack(ref) for ref in source_refs],
    ]
    return "\n".join(lines)


def _acquisition_verb(text: str) -> str | None:
    lower = str(text or "").lower()
    if "acquihire" in lower:
        return "Acquihire"
    if "acquire" in lower or "acquisition" in lower:
        return "Acquire"
    return None


def _extract_acquisition_target(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if not cleaned:
        return ""
    patterns = [
        r"\b(?:acquihire|acquire)\s+([A-Z][A-Za-z0-9&.'\- ]{1,60})",
        r"\b(?:acquihires?|acquired)\s+([A-Z][A-Za-z0-9&.'\- ]{1,60})",
    ]
    for pattern in patterns:
        m = re.search(pattern, cleaned, flags=re.IGNORECASE)
        if not m:
            continue
        candidate = m.group(1).strip(" .,:;-")
        candidate = re.split(r"\b(to|for|as|with|while|amid)\b", candidate, maxsplit=1, flags=re.IGNORECASE)[0].strip(" .,:;-")
        if candidate:
            return candidate
    return ""


def _is_valid_acquisition_idea_line(text: str) -> bool:
    line = _normalize_line(text)
    if not line:
        return False
    if _acquisition_verb(line) is None:
        return False
    target = _extract_acquisition_target(line)
    if not target:
        return False
    target_key = re.sub(r"[^a-z0-9]+", "", target.lower())
    if target_key in ACQ_PLACEHOLDER_TARGETS:
        return False
    lowered_target = target.lower()
    if any(term in lowered_target for term in ACQ_INVALID_TARGET_TERMS):
        return False
    return True


def _target_candidates_from_seed(*, company: str, seed_text: str) -> list[str]:
    cleaned = re.sub(r"[“”\"'`]", "", str(seed_text or ""))
    matches = re.findall(r"\b[A-Z][A-Za-z0-9&.\-]{1,30}(?:\s+[A-Z][A-Za-z0-9&.\-]{1,30}){0,2}\b", cleaned)
    company_key = _slug(company)
    blocked = {
        "reuters",
        "techcrunch",
        "bloomberg",
        "microsoft",
        "softbank",
        "wall street journal",
        "wsj",
        "series",
        "funding",
        "deal",
        "news",
        "ai",
        "build",
        "create",
        "develop",
        "launch",
        "acquire",
        "acquihire",
        "stealth",
    }
    out: list[str] = []
    seen: set[str] = set()
    for item in matches:
        candidate = re.sub(r"\s+", " ", item).strip(" .,:;-")
        if not candidate:
            continue
        candidate = re.sub(r"^(?:Acquire|Acquihire)\s+", "", candidate, flags=re.IGNORECASE).strip()
        if not candidate:
            continue
        if len(candidate) < 3:
            continue
        key = _slug(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        single_token = re.sub(r"[^a-z0-9]+", "", candidate.lower())
        if single_token in TARGET_TOKEN_STOPWORDS:
            continue
        if key in ACQ_PLACEHOLDER_TARGETS:
            continue
        if key in blocked:
            continue
        if company_key and key == company_key:
            continue
        if "stealth" in candidate.lower():
            continue
        if any(term in candidate.lower() for term in ACQ_INVALID_TARGET_TERMS):
            continue
        out.append(candidate)
    return out


def _is_valid_target_name(*, company: str, target: str) -> bool:
    candidate = _normalize_text(str(target or ""), max_chars=100).strip()
    if not candidate:
        return False
    key = re.sub(r"[^a-z0-9]+", "", candidate.lower())
    if not key or key in ACQ_PLACEHOLDER_TARGETS:
        return False
    if any(term in candidate.lower() for term in ACQ_INVALID_TARGET_TERMS):
        return False
    if _slug_company(company) == _slug_company(candidate):
        return False
    return True


def _default_target_for_company(company: str) -> str:
    company_key = _slug_company(company)
    manual = _manual_default_targets()
    if company_key in manual and _is_valid_target_name(company=company, target=manual[company_key]):
        return manual[company_key]
    candidate = DEFAULT_TARGET_BY_COMPANY.get(company_key, "")
    if _is_valid_target_name(company=company, target=candidate):
        return candidate
    return "Scale AI"


def _best_effort_target(*, company: str, seed_text: str) -> str:
    candidates = _target_candidates_from_seed(company=company, seed_text=seed_text)
    for candidate in candidates:
        if _is_valid_target_name(company=company, target=candidate):
            return candidate
    return _default_target_for_company(company)


def _best_effort_idea_line(*, company: str, seed_text: str) -> str:
    extracted = _extract_acquisition_target(seed_text)
    target = extracted if _is_valid_target_name(company=company, target=extracted) else _best_effort_target(company=company, seed_text=seed_text)
    line = f"Acquire {target} to accelerate {company} execution in a strategic wedge."
    return _normalize_line(line)


def _source_ref_from_row(row: dict[str, str]) -> SourceRef | None:
    url = _normalize_source_url(str(row.get("url") or ""))
    if not url:
        return None
    title = _normalize_source_text(str(row.get("title") or ""), max_chars=180) or "Reference"
    publisher = _normalize_source_text(str(row.get("publisher") or ""), max_chars=64) or _publisher_from_url(url)
    return SourceRef(name_or_publisher=publisher, title=title, url=url)


def _build_source_refs(
    *,
    company: str,
    draft: BoardSeatDraft,
    funding: FundingSnapshot,
    acquisition_rows: list[dict[str, str]],
) -> SourceSelection:
    target, target_tokens = _extract_target_tokens_from_idea(draft.idea_line)
    target_rows = _target_search_rows(
        target=target or company,
        company=company,
        snippets=[draft.why_now, draft.whats_different, draft.context_domain_fit_gaps],
    )
    include_funding = _include_funding_links()
    policy = _source_policy()

    normalized_candidates: list[SourceCandidate] = []
    seen_url: set[str] = set()
    seen_title: set[str] = set()

    def _append_candidate(*, ref: SourceRef, text_blob: str, origin: str) -> None:
        normalized = _normalize_source_ref(ref)
        if normalized is None:
            return
        url_key = normalized.url.lower()
        title_key = _title_fingerprint(normalized.title)
        dedupe_key = f"{_source_domain(normalized.url)}::{title_key}"
        if url_key in seen_url or dedupe_key in seen_title:
            return
        seen_url.add(url_key)
        seen_title.add(dedupe_key)
        category = _classify_source_ref(
            company=company,
            target=target,
            target_tokens=target_tokens,
            text_blob=text_blob,
        )
        quality = 1 if _is_quality_source(normalized.url) else (-1 if _is_low_quality_source(normalized.url) else 0)
        score = {
            "target_direct": 3.0,
            "target_proxy": 2.0,
            "parent_context": 1.0,
            "funding_context": 0.2,
        }.get(category, 1.0)
        score += {"target_search": 0.4, "acquisition_search": 0.25, "draft": 0.1, "funding": -0.25}.get(origin, 0.0)
        score += 0.4 if quality > 0 else (-0.2 if quality < 0 else 0.0)
        normalized_candidates.append(
            SourceCandidate(
                ref=normalized,
                category=category,
                quality=quality,
                score=score,
                text_blob=text_blob,
            )
        )

    for ref in draft.source_refs:
        _append_candidate(ref=ref, text_blob=f"{ref.title} {ref.url}", origin="draft")
    for row in target_rows:
        ref = _source_ref_from_row(row)
        if ref is not None:
            _append_candidate(
                ref=ref,
                text_blob=" ".join(
                    [str(row.get("title") or ""), str(row.get("snippet") or ""), str(row.get("url") or "")]
                ),
                origin="target_search",
            )
    for row in acquisition_rows:
        ref = _source_ref_from_row(row)
        if ref is not None:
            _append_candidate(
                ref=ref,
                text_blob=" ".join(
                    [str(row.get("title") or ""), str(row.get("snippet") or ""), str(row.get("url") or "")]
                ),
                origin="acquisition_search",
            )
    if include_funding:
        for ref in _source_refs_from_urls(funding.source_urls, title_hint=f"{company} funding reference"):
            _append_candidate(ref=ref, text_blob=f"{ref.title} {ref.url}", origin="funding")

    if not include_funding:
        normalized_candidates = [item for item in normalized_candidates if item.category != "funding_context"]

    normalized_candidates.sort(key=lambda item: item.score, reverse=True)
    selected: list[SourceCandidate] = []
    selected_urls: set[str] = set()
    parent_count = 0

    if policy == "target_first_3_1":
        for candidate in normalized_candidates:
            if len(selected) >= 3:
                break
            if candidate.category not in {"target_direct", "target_proxy"}:
                continue
            key = candidate.ref.url.lower()
            if key in selected_urls:
                continue
            selected.append(candidate)
            selected_urls.add(key)

        for candidate in normalized_candidates:
            if len(selected) >= 4:
                break
            key = candidate.ref.url.lower()
            if key in selected_urls:
                continue
            if candidate.category == "parent_context" and parent_count < 1:
                selected.append(candidate)
                selected_urls.add(key)
                parent_count += 1
                continue
            if candidate.category in {"target_direct", "target_proxy"}:
                selected.append(candidate)
                selected_urls.add(key)
    else:
        for candidate in normalized_candidates:
            if len(selected) >= 4:
                break
            key = candidate.ref.url.lower()
            if key in selected_urls:
                continue
            selected.append(candidate)
            selected_urls.add(key)

    if not selected:
        fallback = _fallback_source_refs(company, target=target)
        return SourceSelection(refs=_normalize_source_refs(fallback, max_items=4), confidence="Low")

    refs = [item.ref for item in selected[:4]]
    confidence = _source_selection_confidence(selected[:4])
    if confidence == "Low" and _low_signal_mode() != "candidate_with_confidence":
        confidence = "Medium"
    return SourceSelection(refs=refs, confidence=confidence)


def _validate_draft(draft: BoardSeatDraft) -> list[str]:
    errors: list[str] = []
    checks = {
        "idea_line": draft.idea_line,
        "idea_confidence": draft.idea_confidence,
        "why_now": draft.why_now,
        "whats_different": draft.whats_different,
        "mos_risks": draft.mos_risks,
        "bottom_line": draft.bottom_line,
        "context_current_efforts": draft.context_current_efforts,
        "context_domain_fit_gaps": draft.context_domain_fit_gaps,
        "funding_history": draft.funding_history,
        "funding_latest_round_backers": draft.funding_latest_round_backers,
    }
    for key, value in checks.items():
        text = str(value or "").strip()
        if not text:
            errors.append(f"missing_{key}")
            continue
        if len(text.split()) > MAX_LINE_WORDS:
            errors.append(f"{key}_too_long")
    if not _is_valid_acquisition_idea_line(draft.idea_line):
        errors.append("idea_line_invalid")
    if _normalize_confidence_label(draft.idea_confidence, fallback="") not in TARGET_CONFIDENCE_LEVELS:
        errors.append("idea_confidence_invalid")
    if not _normalize_source_refs(draft.source_refs, max_items=4):
        errors.append("missing_source_refs")
    return errors


def _sanitize_draft(
    *,
    company: str,
    draft: BoardSeatDraft,
    funding: FundingSnapshot,
    acquisition_rows: list[dict[str, str]] | None = None,
) -> BoardSeatDraft:
    funding_history, funding_latest_round_backers = _funding_lines_from_snapshot(funding)
    acq_rows = acquisition_rows or []
    idea_line = _normalize_line(draft.idea_line)
    if not _is_valid_acquisition_idea_line(idea_line):
        seed_text = " ".join(
            [
                str(draft.idea_line or ""),
                str(draft.why_now or ""),
                str(draft.whats_different or ""),
                " ".join(str(row.get("title") or "") for row in acq_rows[:2]),
            ]
        )
        idea_line = _best_effort_idea_line(company=company, seed_text=seed_text)
    source_selection = _build_source_refs(company=company, draft=draft, funding=funding, acquisition_rows=acq_rows)
    idea_confidence = source_selection.confidence
    return BoardSeatDraft(
        idea_line=idea_line,
        idea_confidence=idea_confidence,
        why_now=_normalize_line(draft.why_now)
        or f"{company} has a near-term window to drive measurable strategic leverage.",
        whats_different=_normalize_line(draft.whats_different)
        or "Differentiation is strongest where product velocity and mission alignment beat alternatives.",
        mos_risks=_normalize_line(draft.mos_risks)
        or "Upside is meaningful, but execution, procurement timing, and integration risk remain.",
        bottom_line=_normalize_line(draft.bottom_line)
        or f"Prioritize one high-conviction move for {company} with clear 12-month milestones.",
        context_current_efforts=_normalize_line(draft.context_current_efforts)
        or f"{company} is already investing in adjacent capabilities and customer programs in this domain.",
        context_domain_fit_gaps=_normalize_line(draft.context_domain_fit_gaps)
        or "Fit is strongest where roadmap, partnerships, and deployment constraints are explicitly addressed.",
        funding_history=_normalize_line(draft.funding_history) or funding_history,
        funding_latest_round_backers=_normalize_line(draft.funding_latest_round_backers) or funding_latest_round_backers,
        source_refs=source_selection.refs,
        raw_model_output=draft.raw_model_output,
        rewrite_reasons=draft.rewrite_reasons,
    )


def _signal_signature_from_investment(investment_text: str) -> str:
    primary = str(investment_text or "").split("|", 1)[0].strip()
    return _token_signature(primary, max_tokens=40)


def _signal_text_from_investment(investment_text: str) -> str:
    primary = str(investment_text or "").split("|", 1)[0].strip().lower()
    primary = re.sub(r"[^a-z0-9\s]+", " ", primary)
    return re.sub(r"\s+", " ", primary).strip()


def _context_signature_from_snippets(snippets: list[str]) -> str:
    joined = " ".join(_normalize_text(item, max_chars=320) for item in snippets[:12] if item)
    return _token_signature(joined, max_tokens=96)


def _jaccard_similarity(sig_a: str, sig_b: str) -> float:
    a = {item for item in (sig_a or "").split(" ") if item}
    b = {item for item in (sig_b or "").split(" ") if item}
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return float(len(a & b)) / float(len(a | b))


def _parse_context_snippets(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [_normalize_text(str(item), max_chars=320) for item in raw if str(item).strip()]
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [_normalize_text(str(item), max_chars=320) for item in parsed if str(item).strip()]
    return []


def _has_significant_change(*, previous_pitch: dict[str, Any] | None, current_snippets: list[str]) -> bool:
    if previous_pitch is None:
        return True

    current_sig = _context_signature_from_snippets(current_snippets)
    previous_sig = str(previous_pitch.get("context_signature") or "").strip()
    if not current_sig:
        return False
    if not previous_sig:
        return True

    similarity = _jaccard_similarity(previous_sig, current_sig)
    novelty = 1.0 - similarity

    previous_tokens = {item for item in previous_sig.split(" ") if item}
    current_tokens = {item for item in current_sig.split(" ") if item}
    new_event_terms = (current_tokens - previous_tokens) & SIGNIFICANT_CHANGE_TERMS

    prev_snippets = _parse_context_snippets(previous_pitch.get("context_snippets_json"))
    prev_numbers = set(re.findall(r"\b\d+(?:\.\d+)?%?\b", " ".join(prev_snippets)))
    current_numbers = set(re.findall(r"\b\d+(?:\.\d+)?%?\b", " ".join(current_snippets)))
    numeric_delta = bool(current_numbers) and current_numbers != prev_numbers

    return novelty >= 0.35 or bool(new_event_terms) or numeric_delta


def _detect_repeat_investment(
    *,
    investment_hash: str,
    investment_signature: str,
    signal_signature: str,
    signal_text: str,
    recent_pitches: list[dict[str, Any]],
) -> tuple[bool, dict[str, Any] | None, float]:
    for pitch in recent_pitches:
        previous_hash = str(pitch.get("investment_hash") or "").strip()
        previous_sig = str(pitch.get("investment_signature") or "").strip()
        previous_signal_sig = _signal_signature_from_investment(str(pitch.get("investment_text") or ""))
        previous_signal_text = _signal_text_from_investment(str(pitch.get("investment_text") or ""))
        if previous_hash and previous_hash == investment_hash:
            return True, pitch, 1.0
        if ("no high signal" in previous_signal_text) and ("no high signal" in signal_text):
            return True, pitch, 1.0
        signal_similarity = _jaccard_similarity(previous_signal_sig, signal_signature)
        if signal_similarity >= 0.60:
            return True, pitch, signal_similarity
        similarity = _jaccard_similarity(previous_sig, investment_signature)
        if similarity >= 0.82:
            return True, pitch, similarity
    return False, None, 0.0


def _build_novel_fallback_draft(
    *,
    company: str,
    snippets: list[str],
    recent_pitches: list[dict[str, Any]],
    funding: FundingSnapshot,
    acquisition_rows: list[dict[str, str]] | None = None,
) -> BoardSeatDraft:
    previous_signatures = [str(item.get("investment_signature") or "").strip() for item in recent_pitches]
    chosen = ""
    for snippet in snippets:
        sig = _token_signature(snippet, max_tokens=40)
        if not sig:
            continue
        if all(_jaccard_similarity(sig, prev) < 0.6 for prev in previous_signatures if prev):
            chosen = _normalize_line(snippet)
            break
    if not chosen:
        chosen = _normalize_line(snippets[0]) if snippets else f"No high-signal updates surfaced for {company} in the last 24 hours."

    context_line = (
        _normalize_line(snippets[1])
        if len(snippets) > 1
        else f"Prioritize net-new ideas for {company} unless underlying data changed materially."
    )
    funding_history, funding_latest_round_backers = _funding_lines_from_snapshot(funding)
    return _sanitize_draft(
        company=company,
        funding=funding,
        acquisition_rows=acquisition_rows or [],
        draft=BoardSeatDraft(
            idea_line=_best_effort_idea_line(company=company, seed_text=chosen),
            idea_confidence="Low",
            why_now=chosen,
            whats_different="Use net-new evidence versus previously pitched ideas.",
            mos_risks="Main risk is repeating stale theses without materially new information.",
            bottom_line=f"Advance only one differentiated idea for {company} this week.",
            context_current_efforts=context_line,
            context_domain_fit_gaps="Map recommendation to current roadmap, partnerships, and deployment gaps.",
            funding_history=funding_history,
            funding_latest_round_backers=funding_latest_round_backers,
            source_refs=[],
            raw_model_output="",
            rewrite_reasons=["novel_fallback"],
        ),
    )


def _empty_funding_snapshot(*, source_type: str = "unknown") -> FundingSnapshot:
    return FundingSnapshot(
        history="",
        latest_round="",
        latest_date="",
        backers=[],
        source_urls=[],
        source_type=source_type,
        as_of_utc=_utc_now_iso(),
        confidence=0.0,
    )


def _is_funding_snapshot_unknown(snapshot: FundingSnapshot) -> bool:
    if snapshot.source_type == "unknown":
        return True
    if snapshot.history.strip():
        return False
    if snapshot.latest_round.strip():
        return False
    if snapshot.latest_date.strip():
        return False
    return not snapshot.backers


def _funding_lines_from_snapshot(snapshot: FundingSnapshot) -> tuple[str, str]:
    if _is_funding_snapshot_unknown(snapshot):
        unknown = _normalize_line(UNKNOWN_FUNDING_TEXT)
        return (unknown, unknown)

    history = _normalize_line(snapshot.history)
    latest = _normalize_line(snapshot.latest_round, max_words=8)
    latest_date = _normalize_line(snapshot.latest_date, max_words=6)
    backers = _normalize_line(", ".join(snapshot.backers[:4]), max_words=10)

    history_line = history or "Funding history not confirmed from current sources."
    latest_parts: list[str] = []
    if latest:
        latest_parts.append(f"{latest}")
    if latest_date:
        latest_parts.append(f"({latest_date})")
    if backers:
        latest_parts.append(f"backers: {backers}")
    latest_line = _normalize_line(" ".join(latest_parts)) if latest_parts else "Funding details are currently unavailable."
    return (_normalize_line(history_line), _normalize_line(latest_line))


def _funding_snapshot_fresh(*, snapshot: FundingSnapshot | None, ttl_days: int) -> bool:
    if snapshot is None:
        return False
    try:
        as_of = datetime.fromisoformat(snapshot.as_of_utc.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return False
    age_days = (datetime.now(UTC) - as_of).total_seconds() / 86400.0
    return age_days <= float(ttl_days)


def _http_json(*, url: str, headers: dict[str, str], params: dict[str, str]) -> Any:
    request_url = url + "?" + urlencode(params)
    request = Request(request_url, headers=headers, method="GET")
    with urlopen(request, timeout=20) as response:  # nosec B310
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def _brave_search_rows(company: str) -> list[dict[str, str]]:
    api_key = _brave_search_api_key()
    if not api_key:
        return []
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
        "User-Agent": "CoatueClaw/1.0",
    }
    queries = [
        f"{company} funding history latest round backers",
        f"{company} raised series funding investors",
    ]
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for query in queries:
        try:
            payload = _http_json(
                url=WEB_SEARCH_ENDPOINT,
                headers=headers,
                params={"q": query, "count": str(BRAVE_SEARCH_RESULTS), "country": "us", "search_lang": "en"},
            )
        except Exception:
            continue
        web = payload.get("web") if isinstance(payload, dict) else None
        results = web.get("results") if isinstance(web, dict) else None
        if not isinstance(results, list):
            continue
        for item in results:
            if not isinstance(item, dict):
                continue
            url = _normalize_text(str(item.get("url") or ""), max_chars=320)
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append(
                {
                    "title": _normalize_text(str(item.get("title") or ""), max_chars=240),
                    "snippet": _normalize_text(str(item.get("description") or ""), max_chars=420),
                    "url": url,
                }
            )
            if len(rows) >= BRAVE_SEARCH_RESULTS:
                return rows
    return rows


def _acquisition_search_rows(*, company: str, snippets: list[str]) -> list[dict[str, str]]:
    api_key = _brave_search_api_key()
    if not api_key:
        return []
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
        "User-Agent": "CoatueClaw/1.0",
    }
    hints = " ".join(snippets[:3]).strip()
    queries = [
        f"{company} acquisition acquihire startup",
        f"{company} acquihire team",
        f"{company} M&A target {hints}" if hints else f"{company} M&A target startup",
    ]
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for query in queries:
        try:
            payload = _http_json(
                url=WEB_SEARCH_ENDPOINT,
                headers=headers,
                params={"q": query, "count": str(ACQ_SEARCH_RESULTS), "country": "us", "search_lang": "en"},
            )
        except Exception:
            continue
        web = payload.get("web") if isinstance(payload, dict) else None
        results = web.get("results") if isinstance(web, dict) else None
        if not isinstance(results, list):
            continue
        for item in results:
            if not isinstance(item, dict):
                continue
            url = _normalize_source_url(str(item.get("url") or ""))
            if not url:
                continue
            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            title = _normalize_source_text(str(item.get("title") or ""), max_chars=180)
            snippet = _normalize_text(str(item.get("description") or ""), max_chars=420)
            rows.append(
                {
                    "publisher": _publisher_from_url(url),
                    "title": title or _normalize_source_text(snippet, max_chars=180) or "Reference",
                    "snippet": snippet,
                    "url": url,
                }
            )
            if len(rows) >= ACQ_SEARCH_RESULTS:
                return rows
    return rows


def _extract_funding_with_llm(*, company: str, rows: list[dict[str, str]]) -> FundingSnapshot | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key) or (not rows):
        return None
    model = (os.environ.get("COATUE_CLAW_BOARD_SEAT_MODEL", FUNDING_EXTRACT_MODEL) or FUNDING_EXTRACT_MODEL).strip()
    client = OpenAI(api_key=api_key)
    evidence = "\n".join(
        f"- title: {item.get('title', '')}\n  snippet: {item.get('snippet', '')}\n  url: {item.get('url', '')}"
        for item in rows[:BRAVE_SEARCH_RESULTS]
    )
    prompt = (
        f"Extract funding facts for {company} from the evidence below.\n"
        "Return strict JSON with keys: history, latest_round, latest_date, backers (array), confidence.\n"
        "Use empty strings when unknown. Keep history <= 20 words.\n"
        "Do not hallucinate; rely only on evidence.\n"
        f"Evidence:\n{evidence}"
    )
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.0,
            messages=[
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
    except Exception:
        return None
    text = ""
    if response and response.choices:
        text = str(response.choices[0].message.content or "").strip()
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = json.loads(text[start : end + 1])
    except Exception:
        return None
    history = _normalize_text(str(payload.get("history") or ""), max_chars=320)
    latest_round = _normalize_text(str(payload.get("latest_round") or ""), max_chars=120)
    latest_date = _normalize_text(str(payload.get("latest_date") or ""), max_chars=80)
    backers_raw = payload.get("backers")
    backers = [str(item).strip() for item in backers_raw if str(item).strip()] if isinstance(backers_raw, list) else []
    confidence = float(payload.get("confidence") or 0.0)
    return FundingSnapshot(
        history=history,
        latest_round=latest_round,
        latest_date=latest_date,
        backers=backers[:8],
        source_urls=[item.get("url", "") for item in rows if item.get("url")][:8],
        source_type="web_refresh",
        as_of_utc=_utc_now_iso(),
        confidence=max(0.0, min(1.0, confidence)),
    )


def _extract_funding_with_regex(*, rows: list[dict[str, str]]) -> FundingSnapshot | None:
    if not rows:
        return None
    text = " ".join(f"{item.get('title', '')}. {item.get('snippet', '')}" for item in rows[:BRAVE_SEARCH_RESULTS]).strip()
    if not text:
        return None
    history_match = re.search(r"([A-Z][^.;]{0,140}\b(raised|funding|valuation|series)\b[^.;]{0,140})", text, flags=re.IGNORECASE)
    round_match = re.search(r"\b(series\s+[A-Z][\+\-]?|seed|pre-seed|growth|ipo|debt)\b", text, flags=re.IGNORECASE)
    date_match = re.search(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b|\b20\d{2}\b", text)
    backers: list[str] = []
    backers_match = re.search(r"(?:led by|backed by|investors include)\s+([A-Z][^.;]{3,120})", text, flags=re.IGNORECASE)
    if backers_match:
        candidate = backers_match.group(1)
        for piece in re.split(r",| and ", candidate):
            token = piece.strip(" .")
            if token and len(token.split()) <= 4:
                backers.append(token)
    snapshot = FundingSnapshot(
        history=_normalize_text(history_match.group(1), max_chars=220) if history_match else "",
        latest_round=_normalize_text(round_match.group(1), max_chars=50) if round_match else "",
        latest_date=_normalize_text(date_match.group(0), max_chars=30) if date_match else "",
        backers=backers[:6],
        source_urls=[item.get("url", "") for item in rows if item.get("url")][:8],
        source_type="web_refresh",
        as_of_utc=_utc_now_iso(),
        confidence=0.45,
    )
    if not snapshot.history and not snapshot.latest_round and not snapshot.latest_date and not snapshot.backers:
        return None
    return snapshot


def _refresh_funding_snapshot_from_web(*, company: str) -> FundingSnapshot | None:
    rows = _brave_search_rows(company)
    if not rows:
        return None
    llm_snapshot = _extract_funding_with_llm(company=company, rows=rows)
    if llm_snapshot and (llm_snapshot.history or llm_snapshot.latest_round or llm_snapshot.latest_date or llm_snapshot.backers):
        return llm_snapshot
    return _extract_funding_with_regex(rows=rows)


def _resolve_funding_snapshot(*, store: BoardSeatStore, company: str) -> FundingSnapshot:
    manual = _load_manual_funding_seed()
    key = _slug_company(company)
    if key in manual:
        return manual[key]

    cached = store.get_funding_snapshot(company=company)
    if _funding_snapshot_fresh(snapshot=cached, ttl_days=_funding_ttl_days()) and cached is not None:
        return FundingSnapshot(
            history=cached.history,
            latest_round=cached.latest_round,
            latest_date=cached.latest_date,
            backers=cached.backers,
            source_urls=cached.source_urls,
            source_type="cache",
            as_of_utc=cached.as_of_utc,
            confidence=cached.confidence,
        )

    refreshed = _refresh_funding_snapshot_from_web(company=company)
    if refreshed is not None:
        store.upsert_funding_snapshot(company=company, snapshot=refreshed)
        return refreshed

    if cached is not None:
        return FundingSnapshot(
            history=cached.history,
            latest_round=cached.latest_round,
            latest_date=cached.latest_date,
            backers=cached.backers,
            source_urls=cached.source_urls,
            source_type="cache",
            as_of_utc=cached.as_of_utc,
            confidence=cached.confidence,
        )

    return _empty_funding_snapshot(source_type="unknown")


def _iso_from_slack_ts(ts: str | None) -> str:
    raw = str(ts or "").strip()
    if not raw:
        return _utc_now_iso()
    try:
        seconds = float(raw)
    except Exception:
        return _utc_now_iso()
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat()


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
                # Fallback: post directly by channel name when list scope is unavailable.
                return target
            return None
        channels = payload.get("channels")
        for item in channels if isinstance(channels, list) else []:
            name = str(item.get("name") or "").strip().lower()
            if name == target:
                cid = str(item.get("id") or "").strip()
                if cid:
                    return cid
        meta = payload.get("response_metadata")
        next_cursor = str((meta or {}).get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
        if not next_cursor:
            break
        cursor = next_cursor
    return None


def _fetch_recent_context(client: Any, *, channel_id: str, company: str) -> list[str]:
    lookback = max(2, min(72, int(os.environ.get("COATUE_CLAW_BOARD_SEAT_LOOKBACK_HOURS", "24"))))
    oldest = (datetime.now(UTC) - timedelta(hours=lookback)).timestamp()
    max_messages = max(20, min(400, int(os.environ.get("COATUE_CLAW_BOARD_SEAT_MAX_MESSAGES", "160"))))
    cursor: str | None = None
    snippets: list[str] = []
    while len(snippets) < max_messages:
        try:
            payload = client.conversations_history(
                channel=channel_id,
                oldest=str(oldest),
                inclusive=False,
                limit=min(200, max_messages - len(snippets)),
                cursor=cursor,
            )
        except SlackApiError as exc:
            err = str(exc.response.get("error") or "")
            if err == "missing_scope":
                return []
            return []
        messages = payload.get("messages")
        for item in messages if isinstance(messages, list) else []:
            if not isinstance(item, dict):
                continue
            if item.get("subtype") or item.get("bot_id"):
                continue
            text = _normalize_text(str(item.get("text") or ""), max_chars=240)
            if not text:
                continue
            lower = text.lower()
            if company.lower() in lower or re.search(r"\b(revenue|margin|growth|customer|product|launch|contract|guidance|capex)\b", lower):
                snippets.append(text)
                if len(snippets) >= max_messages:
                    break
        meta = payload.get("response_metadata")
        next_cursor = str((meta or {}).get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
        if not next_cursor:
            break
        cursor = next_cursor
    return snippets[:12]


def _fetch_channel_history(client: Any, *, channel_id: str, max_messages: int) -> list[dict[str, Any]]:
    cursor: str | None = None
    out: list[dict[str, Any]] = []
    remaining = max(20, min(5000, int(max_messages)))
    while len(out) < remaining:
        try:
            payload = client.conversations_history(
                channel=channel_id,
                inclusive=False,
                limit=min(200, remaining - len(out)),
                cursor=cursor,
            )
        except SlackApiError:
            return out
        messages = payload.get("messages")
        for item in messages if isinstance(messages, list) else []:
            if isinstance(item, dict):
                out.append(item)
                if len(out) >= remaining:
                    break
        meta = payload.get("response_metadata")
        next_cursor = str((meta or {}).get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
        if not next_cursor:
            break
        cursor = next_cursor
    return out


def _message_looks_like_board_seat_pitch(*, company: str, text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    first_line = ""
    for line in raw.splitlines():
        if line.strip():
            first_line = line.strip()
            break
    if not first_line:
        return False
    match = BOARD_SEAT_HEADER_RE.search(first_line.strip("* "))
    if not match:
        return False
    title_company = _slug(match.group(1))
    return (not title_company) or (title_company == _slug(company))


def _local_date_for_utc_iso(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(_timezone())
    except Exception:
        return _today_key()
    return dt.strftime("%Y-%m-%d")


def _backfill_channel_pitches(
    *,
    store: BoardSeatStore,
    client: Any,
    company: str,
    channel_ref: str,
    channel_id: str,
    max_messages: int | None = None,
) -> dict[str, Any]:
    limit = (
        max_messages
        if max_messages is not None
        else max(100, min(5000, int(os.environ.get("COATUE_CLAW_BOARD_SEAT_BACKFILL_MESSAGES", "2000"))))
    )
    history = _fetch_channel_history(client, channel_id=channel_id, max_messages=limit)
    if not history:
        return {"scanned": 0, "matched": 0, "inserted": 0}

    matched = 0
    inserted = 0
    for item in reversed(history):
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "")
        if not _message_looks_like_board_seat_pitch(company=company, text=text):
            continue
        matched += 1
        message_ts = str(item.get("ts") or "").strip() or None
        posted_at_utc = _iso_from_slack_ts(message_ts)
        investment_text = _extract_investment_text(text)
        core_investment_text = _core_investment_text(text)
        investment_signature = _token_signature(core_investment_text)
        context_snippets = [core_investment_text] if core_investment_text else []
        context_signature = _context_signature_from_snippets(context_snippets)
        did_insert = store.record_pitch(
            company=company,
            channel_ref=channel_ref,
            channel_id=channel_id,
            source="slack_history_backfill",
            message_ts=message_ts,
            run_date_local=_local_date_for_utc_iso(posted_at_utc),
            posted_at_utc=posted_at_utc,
            message_text=text,
            investment_text=investment_text,
            investment_hash=_stable_hash(investment_signature or core_investment_text or investment_text),
            investment_signature=investment_signature,
            context_signature=context_signature,
            context_snippets=context_snippets,
            significant_change=False,
        )
        if did_insert:
            inserted += 1
    return {"scanned": len(history), "matched": matched, "inserted": inserted}


def _fallback_draft(
    *,
    company: str,
    snippets: list[str],
    funding: FundingSnapshot,
    acquisition_rows: list[dict[str, str]] | None = None,
) -> BoardSeatDraft:
    why_now = _normalize_line(snippets[0]) if snippets else f"No high-signal updates surfaced for {company} in the last 24 hours."
    whats_different = _normalize_line(snippets[1]) if len(snippets) > 1 else "Differentiate on speed to deployment, measured outcomes, and implementation feasibility."
    mos_risks = _normalize_line(snippets[2]) if len(snippets) > 2 else "Key risks are execution bandwidth, procurement timing, and integration complexity."
    funding_history, funding_latest_round_backers = _funding_lines_from_snapshot(funding)
    draft = BoardSeatDraft(
        idea_line=_best_effort_idea_line(company=company, seed_text=why_now),
        idea_confidence="Low",
        why_now=why_now,
        whats_different=whats_different,
        mos_risks=mos_risks,
        bottom_line=f"Prioritize one high-conviction move for {company} with clear 12-month milestones.",
        context_current_efforts=f"Tie this to {company}'s current products, customer momentum, and execution priorities.",
        context_domain_fit_gaps="Spell out where existing capabilities fit, and which gaps need partners or build plans.",
        funding_history=funding_history,
        funding_latest_round_backers=funding_latest_round_backers,
        source_refs=[],
        raw_model_output="",
        rewrite_reasons=["fallback"],
    )
    return _sanitize_draft(company=company, draft=draft, funding=funding, acquisition_rows=acquisition_rows or [])


def _parse_llm_draft_payload(payload: Any) -> BoardSeatDraft | None:
    if not isinstance(payload, dict):
        return None
    required = (
        "idea_line",
        "idea_confidence",
        "why_now",
        "whats_different",
        "mos_risks",
        "bottom_line",
        "context_current_efforts",
        "context_domain_fit_gaps",
        "funding_history",
        "funding_latest_round_backers",
    )
    if any(not isinstance(payload.get(key), str) for key in required):
        return None
    source_refs_raw = payload.get("source_refs")
    source_refs: list[SourceRef] = []
    if isinstance(source_refs_raw, list):
        for item in source_refs_raw:
            if not isinstance(item, dict):
                continue
            source_refs.append(
                SourceRef(
                    name_or_publisher=str(item.get("name_or_publisher") or ""),
                    title=str(item.get("title") or ""),
                    url=str(item.get("url") or ""),
                )
            )
    return BoardSeatDraft(
        idea_line=str(payload.get("idea_line") or ""),
        idea_confidence=str(payload.get("idea_confidence") or ""),
        why_now=str(payload.get("why_now") or ""),
        whats_different=str(payload.get("whats_different") or ""),
        mos_risks=str(payload.get("mos_risks") or ""),
        bottom_line=str(payload.get("bottom_line") or ""),
        context_current_efforts=str(payload.get("context_current_efforts") or ""),
        context_domain_fit_gaps=str(payload.get("context_domain_fit_gaps") or ""),
        funding_history=str(payload.get("funding_history") or ""),
        funding_latest_round_backers=str(payload.get("funding_latest_round_backers") or ""),
        source_refs=source_refs,
        raw_model_output=json.dumps(payload, ensure_ascii=False),
        rewrite_reasons=[],
    )


def _llm_draft(
    *,
    company: str,
    snippets: list[str],
    funding: FundingSnapshot,
    prior_investments: list[str] | None = None,
) -> BoardSeatDraft | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key):
        return None
    model = (os.environ.get("COATUE_CLAW_BOARD_SEAT_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest").strip()
    client = OpenAI(api_key=api_key)
    joined = "\n".join(f"- {line}" for line in snippets[:10]) if snippets else "- no fresh channel snippets"
    funding_json = json.dumps(
        {
            "history": funding.history,
            "latest_round": funding.latest_round,
            "latest_date": funding.latest_date,
            "backers": funding.backers,
            "source_type": funding.source_type,
        },
        ensure_ascii=False,
    )
    prompt = (
        f"Generate a structured board-seat brief for {company}.\n"
        "Return strict JSON only with keys: "
        "idea_line, idea_confidence, why_now, whats_different, mos_risks, bottom_line, "
        "context_current_efforts, context_domain_fit_gaps, "
        "funding_history, funding_latest_round_backers, source_refs.\n"
        "Constraints:\n"
        f"- each value must be a single line <= {MAX_LINE_WORDS} words\n"
        "- idea_line must start with Acquire or Acquihire and name a concrete target.\n"
        "- idea_confidence must be exactly one of High, Medium, Low.\n"
        "- do not propose internal build as primary recommendation.\n"
        "- short, high skim value, decision-useful.\n"
        "- style must be concise labeled-line content, not bullets.\n"
        "- do not use legacy labels (Signal/Board lens/Watchlist/Team ask).\n"
        "- source_refs is an array of objects with name_or_publisher, title, url.\n"
        "- source_refs must prioritize target-company evidence, not parent-company funding links.\n"
        "- avoid Reuters/SoftBank-style parent funding links in source_refs by default.\n"
        "Recent channel context:\n"
        f"{joined}\n"
        f"Funding snapshot input:\n{funding_json}\n"
    )
    if prior_investments:
        prior = "\n".join(f"- {item}" for item in prior_investments[:5] if item.strip())
        prompt += (
            "Avoid repeating prior theses unless context has materially changed:\n"
            f"{prior}\n"
        )
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "You write concise, board-ready bullets. Return JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
        text = ""
        if response and response.choices:
            text = str(response.choices[0].message.content or "").strip()
        if not text:
            return None
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        payload = json.loads(text[start : end + 1])
        draft = _parse_llm_draft_payload(payload)
        if draft is None:
            return None
        return BoardSeatDraft(
            idea_line=draft.idea_line,
            idea_confidence=draft.idea_confidence,
            why_now=draft.why_now,
            whats_different=draft.whats_different,
            mos_risks=draft.mos_risks,
            bottom_line=draft.bottom_line,
            context_current_efforts=draft.context_current_efforts,
            context_domain_fit_gaps=draft.context_domain_fit_gaps,
            funding_history=draft.funding_history,
            funding_latest_round_backers=draft.funding_latest_round_backers,
            source_refs=draft.source_refs,
            raw_model_output=text,
            rewrite_reasons=[],
        )
    except Exception:
        return None


def _build_draft(
    *,
    company: str,
    snippets: list[str],
    funding: FundingSnapshot,
    recent_pitches: list[dict[str, Any]] | None = None,
) -> BoardSeatDraft:
    acquisition_rows = _acquisition_search_rows(company=company, snippets=snippets)
    prior_investments = [str(item.get("investment_text") or "").strip() for item in (recent_pitches or [])]
    llm = _llm_draft(company=company, snippets=snippets, funding=funding, prior_investments=prior_investments)
    draft = llm if llm is not None else _fallback_draft(company=company, snippets=snippets, funding=funding, acquisition_rows=acquisition_rows)
    draft = _sanitize_draft(company=company, draft=draft, funding=funding, acquisition_rows=acquisition_rows)
    if _validate_draft(draft):
        return _fallback_draft(company=company, snippets=snippets, funding=funding, acquisition_rows=acquisition_rows)
    return draft


def run_once(*, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    store = BoardSeatStore()
    run_date = _today_key()
    portcos = _parse_portcos()
    result: dict[str, Any] = {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "run_date_local": run_date,
        "timezone": str(_timezone()),
        "portcos": [{"company": c, "channel_ref": ch} for c, ch in portcos],
        "sent": [],
        "skipped": [],
        "history_backfill": [],
    }

    if WebClient is None and not dry_run:
        raise RuntimeError("slack_sdk is not installed in this environment.")

    clients = [WebClient(token=item) for item in _slack_tokens()] if WebClient is not None else []
    for company, channel_ref in portcos:
        if (not force) and store.already_posted(run_date_local=run_date, company=company):
            result["skipped"].append({"company": company, "channel_ref": channel_ref, "reason": "already_posted_today"})
            continue

        if dry_run and not clients:
            funding = _resolve_funding_snapshot(store=store, company=company)
            draft = _build_draft(
                company=company,
                snippets=[],
                funding=funding,
                recent_pitches=store.recent_pitches(company=company, limit=12),
            )
            message = _render_board_seat_message(company=company, draft=draft)
            result["sent"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "preview": message,
                    "format_version": BOARD_SEAT_FORMAT_VERSION,
                    "funding_source_type": funding.source_type,
                    "funding_as_of_utc": funding.as_of_utc,
                    "funding_unknown": _is_funding_snapshot_unknown(funding),
                }
            )
            continue

        posted = False
        last_error = "unknown"
        for client in clients:
            try:
                channel_id = _resolve_channel_id(client, channel_ref)
                if not channel_id:
                    last_error = "channel_not_found"
                    continue
                if str(os.environ.get("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "1")).strip().lower() not in {"0", "false", "off", "no"}:
                    backfill_stats = _backfill_channel_pitches(
                        store=store,
                        client=client,
                        company=company,
                        channel_ref=channel_ref,
                        channel_id=channel_id,
                    )
                    result["history_backfill"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            **backfill_stats,
                        }
                    )
                snippets = _fetch_recent_context(client, channel_id=channel_id, company=company)
                recent_pitches = store.recent_pitches(company=company, limit=12)
                funding = _resolve_funding_snapshot(store=store, company=company)
                draft = _build_draft(company=company, snippets=snippets, funding=funding, recent_pitches=recent_pitches)
                message = _render_board_seat_message(company=company, draft=draft)
                investment_text = _extract_investment_text(message)
                core_investment_text = _core_investment_text(message)
                investment_signature = _token_signature(core_investment_text)
                signal_signature = _signal_signature_from_investment(investment_text)
                signal_text = _signal_text_from_investment(investment_text)
                investment_hash = _stable_hash(investment_signature or core_investment_text or investment_text)
                context_signature = _context_signature_from_snippets(snippets)
                repeated, matched_pitch, similarity = _detect_repeat_investment(
                    investment_hash=investment_hash,
                    investment_signature=investment_signature,
                    signal_signature=signal_signature,
                    signal_text=signal_text,
                    recent_pitches=recent_pitches,
                )
                previous_pitch = recent_pitches[0] if recent_pitches else None
                significant_change = _has_significant_change(previous_pitch=previous_pitch, current_snippets=snippets)
                if repeated and not significant_change:
                    acquisition_rows = _acquisition_search_rows(company=company, snippets=snippets)
                    draft = _build_novel_fallback_draft(
                        company=company,
                        snippets=snippets,
                        recent_pitches=recent_pitches,
                        funding=funding,
                        acquisition_rows=acquisition_rows,
                    )
                    message = _render_board_seat_message(company=company, draft=draft)
                    investment_text = _extract_investment_text(message)
                    core_investment_text = _core_investment_text(message)
                    investment_signature = _token_signature(core_investment_text)
                    signal_signature = _signal_signature_from_investment(investment_text)
                    signal_text = _signal_text_from_investment(investment_text)
                    investment_hash = _stable_hash(investment_signature or core_investment_text or investment_text)
                    repeated, matched_pitch, similarity = _detect_repeat_investment(
                        investment_hash=investment_hash,
                        investment_signature=investment_signature,
                        signal_signature=signal_signature,
                        signal_text=signal_text,
                        recent_pitches=recent_pitches,
                    )
                if repeated and not significant_change:
                    result["skipped"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "reason": "repeat_investment_without_significant_change",
                            "similarity": round(float(similarity), 3),
                            "matched_posted_at_utc": str((matched_pitch or {}).get("posted_at_utc") or ""),
                        }
                    )
                    posted = True
                    break
                if dry_run:
                    result["sent"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "preview": message,
                            "significant_change": bool(significant_change),
                            "format_version": BOARD_SEAT_FORMAT_VERSION,
                            "funding_source_type": funding.source_type,
                            "funding_as_of_utc": funding.as_of_utc,
                            "funding_unknown": _is_funding_snapshot_unknown(funding),
                        }
                    )
                    posted = True
                    break
                post = client.chat_postMessage(channel=channel_id, text=message)
                ts = str(post.get("ts") or "")
                store.record_post(
                    run_date_local=run_date,
                    company=company,
                    channel_ref=channel_ref,
                    channel_id=channel_id,
                    message_ts=(ts or None),
                    summary=message,
                )
                store.record_pitch(
                    company=company,
                    channel_ref=channel_ref,
                    channel_id=channel_id,
                    source="live_post",
                    message_ts=(ts or None),
                    run_date_local=run_date,
                    posted_at_utc=_iso_from_slack_ts(ts),
                    message_text=message,
                    investment_text=investment_text,
                    investment_hash=investment_hash,
                    investment_signature=investment_signature,
                    context_signature=context_signature,
                    context_snippets=snippets,
                    significant_change=bool(significant_change),
                )
                result["sent"].append(
                    {
                        "company": company,
                        "channel_ref": channel_ref,
                        "channel_id": channel_id,
                        "ts": ts,
                        "significant_change": bool(significant_change),
                        "format_version": BOARD_SEAT_FORMAT_VERSION,
                        "funding_source_type": funding.source_type,
                        "funding_as_of_utc": funding.as_of_utc,
                        "funding_unknown": _is_funding_snapshot_unknown(funding),
                    }
                )
                posted = True
                break
            except SlackApiError as exc:
                response = getattr(exc, "response", None)
                err = str(response.get("error") or "") if isinstance(response, dict) else ""
                last_error = err or "slack_api_error"
                if err in {"account_inactive", "invalid_auth", "token_revoked", "not_authed"}:
                    continue
                break
            except Exception:
                last_error = "unexpected_error"
                break

        if not posted:
            result["skipped"].append({"company": company, "channel_ref": channel_ref, "reason": last_error})

    return result


def status() -> dict[str, Any]:
    store = BoardSeatStore()
    portcos = [{"company": c, "channel_ref": ch} for c, ch in _parse_portcos()]
    manual_seed = _load_manual_funding_seed()
    funding_age_days: dict[str, float | None] = {}
    funding_source_by_company: dict[str, str] = {}
    for item in portcos:
        company = item["company"]
        manual_snapshot = manual_seed.get(_slug_company(company))
        if manual_snapshot is not None:
            funding_age_days[company] = 0.0
            funding_source_by_company[company] = "manual_seed"
            continue
        funding_age_days[company] = store.funding_cache_age_days(company=company)
        snapshot = store.get_funding_snapshot(company=company)
        funding_source_by_company[company] = str(snapshot.source_type).strip() if snapshot is not None else "unknown"
    return {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "timezone": str(_timezone()),
        "run_date_local": _today_key(),
        "portcos": portcos,
        "recent_runs": store.recent_runs(limit=20),
        "pitch_counts": {
            "total": store.pitch_count(),
            "by_company": {
                item["company"]: store.pitch_count(company=item["company"])
                for item in portcos
            },
        },
        "funding_cache_age_days_by_company": funding_age_days,
        "funding_data_source_by_company": funding_source_by_company,
    }


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-board-seat-daily")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run-once")
    run.add_argument("--force", action="store_true")
    run.add_argument("--dry-run", action="store_true")

    sub.add_parser("status")

    args = parser.parse_args()
    if args.command == "run-once":
        payload = run_once(force=bool(args.force), dry_run=bool(args.dry_run))
    else:
        payload = status()
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

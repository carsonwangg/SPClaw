from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Iterator
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
except Exception:  # pragma: no cover - optional dependency
    WebClient = None  # type: ignore[assignment]

    class SlackApiError(Exception):
        pass


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

BOARD_SEAT_FORMAT_VERSION = "v1_noon_natural_synth"
RESET_REASON = "feature_reset_in_progress"
LOW_CONF_WARNING = "Funding data is low-confidence; verify before action."
MEMORY_FALLBACK_WARNING = (
    "⚠️ Fallback mode used: this pitch was rewritten from model memory "
    "(no live web retrieval in final pass). Verify key claims before action."
)

CONCEPT_BLOCKLIST = {
    "ai",
    "ai-first",
    "llm",
    "llms",
    "roi",
    "platform",
    "workflow",
    "automation",
    "infrastructure",
    "productivity",
    "startup",
    "company",
    "target",
    "business",
    "solution",
    "tool",
    "software",
    "hardware",
}

ARTIFACT_TERMS = {
    "read more",
    "cookie",
    "all rights reserved",
    "book a demo",
    "see pricing",
    "subscribe",
    "skip to content",
    "click here",
    "sign up",
    "try for free",
}

MAJOR_EVENT_TERMS = {
    "acquire",
    "acquisition",
    "raised",
    "funding",
    "series",
    "valuation",
    "contract",
    "partnership",
    "launch",
    "deal",
    "revenue",
}


@dataclass(frozen=True)
class SeedTargetResult:
    inserted: bool
    company: str
    target: str
    target_key: str
    posted_at_utc: str


@dataclass(frozen=True)
class DiscoveryChannel:
    company: str
    channel_ref: str
    channel_id: str | None


@dataclass(frozen=True)
class EvidenceRow:
    title: str
    snippet: str
    url: str
    canonical_url: str
    publisher: str
    domain: str
    published_at_utc: str | None
    backend: str
    quality: float


@dataclass(frozen=True)
class CandidateScore:
    target: str
    target_key: str
    score: float
    confidence: str
    evidence_count: int
    distinct_domains: int
    row_indexes: tuple[int, ...]


@dataclass(frozen=True)
class FundingSnapshot:
    target: str
    target_key: str
    total_raised: str
    latest_round: str
    latest_round_date: str
    backers: tuple[str, ...]
    evidence_count: int
    distinct_domains: int
    conflict_flags: tuple[str, ...]
    verification_status: str
    source_rows: tuple[EvidenceRow, ...]


@dataclass(frozen=True)
class DraftResult:
    text: str
    generation_mode: str
    quality_fail_codes: tuple[str, ...]
    memory_rewrite_used: bool


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _timezone() -> ZoneInfo:
    name = (os.environ.get("COATUE_CLAW_BOARD_SEAT_TZ", DEFAULT_TZ) or "").strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)


def _today_key() -> str:
    return datetime.now(_timezone()).strftime("%Y-%m-%d")


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_DB_PATH",
            str(_data_root() / "db/board_seat_daily.sqlite"),
        )
    )


def _fallback_db_path() -> Path:
    return Path.home() / ".coatue-claw-data" / "db" / "board_seat_daily.sqlite"


def _artifact_dir() -> Path:
    path = _data_root() / "artifacts" / "board-seat"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _reset_mode_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_RESET_MODE", False)


def _board_seat_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_ENABLED", True)


def _weekdays_only() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_WEEKDAYS_ONLY", True)


def _board_seat_time() -> str:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_TIME", "12:00") or "12:00").strip()
    if re.fullmatch(r"\d{1,2}:\d{2}", raw):
        hh, mm = raw.split(":", 1)
        h = int(hh)
        m = int(mm)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return f"{h:02d}:{m:02d}"
    return "12:00"


def _target_lock_days() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS", "14") or "14").strip()
    try:
        val = int(raw)
    except Exception:
        val = 14
    return max(1, min(90, val))


def _require_high_conf_new_target() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", True)


def _repitch_significance_min() -> float:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_REPITCH_SIGNIFICANCE_MIN", "0.85") or "0.85").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.85
    return max(0.5, min(1.0, val))


def _search_order() -> list[str]:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_SEARCH_ORDER", "brave,serp") or "brave,serp").strip().lower()
    out: list[str] = []
    for token in raw.split(","):
        item = token.strip()
        if item in {"brave", "serp"} and item not in out:
            out.append(item)
    return out or ["brave", "serp"]


def _funding_min_domains() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_MIN_DOMAINS", "2") or "2").strip()
    try:
        val = int(raw)
    except Exception:
        val = 2
    return max(1, min(5, val))


def _funding_low_conf_threshold() -> float:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_LOW_CONF_THRESHOLD", "0.55") or "0.55").strip()
    try:
        val = float(raw)
    except Exception:
        val = 0.55
    return max(0.05, min(0.95, val))


def _funding_cache_ttl_hours() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_CACHE_TTL_HOURS", "168") or "168").strip()
    try:
        val = int(raw)
    except Exception:
        val = 168
    return max(1, min(24 * 90, val))


def _max_web_rewrites() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_MAX_WEB_REWRITES", "2") or "2").strip()
    try:
        val = int(raw)
    except Exception:
        val = 2
    return max(0, min(6, val))


def _memory_rewrite_on_fail() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_MEMORY_REWRITE_ON_FAIL", True)


def _memory_rewrite_max_retries() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_MEMORY_REWRITE_MAX_RETRIES", "1") or "1").strip()
    try:
        val = int(raw)
    except Exception:
        val = 1
    return max(0, min(3, val))


def _memory_rewrite_thread_warning() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_MEMORY_REWRITE_THREAD_WARNING", True)


def _no_quotes() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_NO_QUOTES", True)


def _sources_in_thread() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_SOURCES_IN_THREAD", True)


def _channel_discovery_mode() -> str:
    mode = (os.environ.get("COATUE_CLAW_BOARD_SEAT_CHANNEL_DISCOVERY", "company_match") or "company_match").strip().lower()
    return mode if mode in {"company_match", "static"} else "company_match"


def _channel_types() -> str:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_CHANNEL_TYPES", "public_channel,private_channel") or "public_channel,private_channel").strip()
    return raw or "public_channel,private_channel"


def _openai_model() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest").strip()


def _openai_api_key() -> str | None:
    for key in (
        "COATUE_CLAW_OPENAI_API_KEY",
        "OPENAI_API_KEY",
    ):
        value = (os.environ.get(key, "") or "").strip()
        if value:
            return value
    return None


def _openai_client() -> Any | None:
    if OpenAI is None:
        return None
    key = _openai_api_key()
    if not key:
        return None
    try:
        return OpenAI(api_key=key)
    except Exception:
        return None


def _slug_company(company: str) -> str:
    return re.sub(r"[^a-z0-9]", "", company.lower())


def _target_key(target: str) -> str:
    return re.sub(r"[^a-z0-9]", "", target.lower())


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_company_name(name: str) -> str:
    return _normalize_whitespace(name).strip("#")


def _parse_iso(value: str | None) -> datetime | None:
    raw = _normalize_whitespace(value or "")
    if not raw:
        return None
    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _domain_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    return str(parsed.netloc or "").lower().replace("www.", "").strip()


def _canonicalize_url(url: str) -> str:
    raw = _normalize_whitespace(url)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower().replace("www.", "")
    path = parsed.path or "/"
    return f"{scheme}://{netloc}{path}".rstrip("/")


def _parse_portcos(raw: str | None = None) -> list[tuple[str, str]]:
    data = raw if raw is not None else os.environ.get("COATUE_CLAW_BOARD_SEAT_PORTCOS", "")
    if not data:
        return list(DEFAULT_PORTCOS)
    parsed: list[tuple[str, str]] = []
    for chunk in data.split(","):
        item = chunk.strip()
        if not item:
            continue
        if ":" in item:
            company, channel = item.split(":", 1)
            company = _normalize_company_name(company)
            channel = channel.strip().lstrip("#")
            if company and channel:
                parsed.append((company, channel))
            continue
        channel = item.lstrip("#")
        parsed.append((item, channel))
    return parsed or list(DEFAULT_PORTCOS)


def _is_weekday_local(now_local: datetime) -> bool:
    return now_local.weekday() < 5


def _within_schedule_window(now_local: datetime, *, force: bool) -> bool:
    if force:
        return True
    if _weekdays_only() and not _is_weekday_local(now_local):
        return False
    hh, mm = _board_seat_time().split(":")
    target_minutes = int(hh) * 60 + int(mm)
    now_minutes = now_local.hour * 60 + now_local.minute
    return abs(now_minutes - target_minutes) <= 180


def _http_json(url: str, *, headers: dict[str, str] | None = None, params: dict[str, str] | None = None, timeout: int = 8) -> dict[str, Any]:
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"
    req = Request(full_url, headers=headers or {}, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8", errors="replace")
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("json_object_expected")
    return data


def _brave_api_key() -> str | None:
    for key in ("COATUE_CLAW_BRAVE_API_KEY", "BRAVE_SEARCH_API_KEY"):
        val = (os.environ.get(key, "") or "").strip()
        if val:
            return val
    return None


def _serp_api_key() -> str | None:
    for key in ("COATUE_CLAW_BOARD_SEAT_GOOGLE_SERP_API_KEY", "SERPAPI_API_KEY"):
        val = (os.environ.get(key, "") or "").strip()
        if val:
            return val
    return None


def _search_count() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_SEARCH_MAX_RESULTS", "12") or "12").strip()
    try:
        val = int(raw)
    except Exception:
        val = 12
    return max(3, min(30, val))


def _brave_search_rows(query: str) -> list[EvidenceRow]:
    key = _brave_api_key()
    if not key:
        return []
    endpoint = (os.environ.get("COATUE_CLAW_BOARD_SEAT_BRAVE_ENDPOINT", "https://api.search.brave.com/res/v1/web/search") or "").strip()
    if not endpoint:
        return []
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": key,
        "User-Agent": "CoatueClaw/1.0",
    }
    try:
        payload = _http_json(endpoint, headers=headers, params={"q": query, "count": str(_search_count())}, timeout=8)
    except Exception:
        return []
    web = payload.get("web") if isinstance(payload, dict) else None
    rows = web.get("results") if isinstance(web, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[EvidenceRow] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = _normalize_whitespace(str(row.get("title") or ""))
        snippet = _normalize_whitespace(str(row.get("description") or ""))
        url = _normalize_whitespace(str(row.get("url") or ""))
        if not url:
            continue
        canonical = _canonicalize_url(url)
        domain = _domain_from_url(canonical)
        publisher = domain or "Unknown"
        quality = _evidence_quality(title=title, snippet=snippet, url=canonical)
        out.append(
            EvidenceRow(
                title=title,
                snippet=_clean_snippet(snippet),
                url=url,
                canonical_url=canonical,
                publisher=publisher,
                domain=domain,
                published_at_utc=None,
                backend="brave",
                quality=quality,
            )
        )
    return out


def _google_serp_rows(query: str) -> list[EvidenceRow]:
    key = _serp_api_key()
    if not key:
        return []
    endpoint = (os.environ.get("COATUE_CLAW_BOARD_SEAT_GOOGLE_SERP_ENDPOINT", "https://serpapi.com/search.json") or "").strip()
    try:
        payload = _http_json(
            endpoint,
            headers={"Accept": "application/json", "User-Agent": "CoatueClaw/1.0"},
            params={
                "engine": "google",
                "q": query,
                "hl": "en",
                "gl": "us",
                "num": str(min(20, _search_count())),
                "api_key": key,
            },
            timeout=10,
        )
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for key_name in ("organic_results", "news_results", "top_stories"):
        block = payload.get(key_name)
        if isinstance(block, list):
            rows.extend([x for x in block if isinstance(x, dict)])

    out: list[EvidenceRow] = []
    for row in rows:
        title = _normalize_whitespace(str(row.get("title") or ""))
        snippet = _normalize_whitespace(str(row.get("snippet") or ""))
        url = _normalize_whitespace(str(row.get("link") or row.get("url") or ""))
        if not url:
            continue
        canonical = _canonicalize_url(url)
        domain = _domain_from_url(canonical)
        publisher = domain or "Unknown"
        quality = _evidence_quality(title=title, snippet=snippet, url=canonical)
        out.append(
            EvidenceRow(
                title=title,
                snippet=_clean_snippet(snippet),
                url=url,
                canonical_url=canonical,
                publisher=publisher,
                domain=domain,
                published_at_utc=None,
                backend="serp",
                quality=quality,
            )
        )
    return out


def _evidence_quality(*, title: str, snippet: str, url: str) -> float:
    text = f"{title} {snippet}".lower()
    score = 0.25
    if any(term in text for term in ("acquire", "acquisition", "buy", "takeover", "acqui-hire", "merger")):
        score += 0.25
    if any(term in text for term in ("raised", "funding", "series", "valuation", "investors", "led by")):
        score += 0.18
    if len(snippet) > 40:
        score += 0.12
    domain = _domain_from_url(url)
    if domain and all(x not in domain for x in ("wikipedia", "reddit", "facebook", "instagram", "x.com", "twitter.com")):
        score += 0.15
    if any(term in text for term in ARTIFACT_TERMS):
        score -= 0.35
    return max(0.0, min(1.0, score))


def _clean_snippet(snippet: str) -> str:
    text = _normalize_whitespace(snippet)
    if not text:
        return ""
    lowered = text.lower()
    if any(term in lowered for term in ARTIFACT_TERMS):
        return ""
    return text


def _dedupe_rows(rows: Iterable[EvidenceRow]) -> list[EvidenceRow]:
    out: list[EvidenceRow] = []
    seen: set[str] = set()
    for row in rows:
        key = row.canonical_url or row.url
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        if row.quality < 0.22:
            continue
        out.append(row)
    return out


def _search_queries_for_company(company: str) -> list[str]:
    return [
        f"{company} acquisition target startup",
        f"{company} acquihire opportunity",
        f"{company} strategic acquisition candidate",
        f"{company} buy startup AI infrastructure",
    ]


def _search_queries_for_funding(target: str) -> list[str]:
    return [
        f"{target} total funding raised",
        f"{target} latest funding round investors",
        f"{target} series round backers",
    ]


def _collect_web_rows(queries: list[str]) -> tuple[list[EvidenceRow], list[str]]:
    notes: list[str] = []
    out: list[EvidenceRow] = []
    order = _search_order()
    for q in queries:
        q_rows: list[EvidenceRow] = []
        for backend in order:
            rows = _brave_search_rows(q) if backend == "brave" else _google_serp_rows(q)
            if rows:
                q_rows.extend(rows)
                notes.append(f"search:{backend}:ok")
                if backend == order[0]:
                    break
            else:
                notes.append(f"search:{backend}:no_signal")
        out.extend(q_rows)
    return _dedupe_rows(out), notes


def _candidate_stopwords() -> set[str]:
    return {
        "The",
        "This",
        "That",
        "These",
        "Those",
        "Today",
        "Series",
        "Funding",
        "Round",
        "Investors",
        "Company",
        "Startup",
        "Portfolio",
        "Board",
        "Seat",
    }


def _extract_title_candidates(title: str) -> list[str]:
    cleaned = _normalize_whitespace(title)
    if not cleaned:
        return []
    parts = re.split(r"\s*[\-–—|:]\s*", cleaned)
    out: list[str] = []
    for part in parts[:2]:
        match = re.findall(r"\b[A-Z][A-Za-z0-9&.\-]{1,}(?:\s+[A-Z][A-Za-z0-9&.\-]{1,}){0,2}\b", part)
        for m in match:
            val = _normalize_whitespace(m)
            if val and val.split(" ")[0] not in _candidate_stopwords():
                out.append(val)
    return out


def _is_valid_target_name(*, target: str, company: str) -> tuple[bool, str]:
    t = _normalize_whitespace(target)
    if not t:
        return False, "empty"
    if len(t) < 3 or len(t) > 42:
        return False, "length"
    tk = _target_key(t)
    if not tk:
        return False, "empty"
    if tk in CONCEPT_BLOCKLIST:
        return False, "conceptual"
    if _target_key(company) == tk or tk in {_target_key(company) + "s", _target_key(company).rstrip("s")}:
        return False, "self_company"
    if re.search(r"\b(inc|llc|ltd|corp|company|startup)\b", t.lower()) and len(t.split()) > 3:
        return False, "noisy_suffix"
    if len(re.findall(r"[A-Za-z]", t)) < 2:
        return False, "not_name_like"
    return True, "ok"


def _extract_candidates(company: str, rows: list[EvidenceRow]) -> list[CandidateScore]:
    by_key: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(rows):
        text = f"{row.title} {row.snippet}".lower()
        if not any(term in text for term in ("acquire", "acquisition", "acqui", "buy", "takeover", "merger", "funding", "startup")):
            continue
        candidates = _extract_title_candidates(row.title)
        if not candidates:
            candidates = _extract_title_candidates(row.snippet)
        for candidate in candidates:
            valid, _ = _is_valid_target_name(target=candidate, company=company)
            if not valid:
                continue
            key = _target_key(candidate)
            state = by_key.setdefault(
                key,
                {
                    "target": candidate,
                    "score": 0.0,
                    "rows": set(),
                    "domains": set(),
                },
            )
            state["rows"].add(idx)
            state["domains"].add(row.domain)
            row_score = row.quality
            if any(term in text for term in ("acquisition", "acquire", "buy")):
                row_score += 0.10
            if any(term in text for term in ("funding", "raised", "series", "investors")):
                row_score += 0.06
            state["score"] += max(0.0, min(0.4, row_score))

    out: list[CandidateScore] = []
    for key, state in by_key.items():
        row_indexes = sorted(int(x) for x in state["rows"])
        row_count = len(row_indexes)
        domain_count = len(state["domains"])
        score = min(1.0, (state["score"] / max(1.0, row_count * 0.33)) * 0.55 + min(0.2, row_count * 0.05) + min(0.25, domain_count * 0.1))
        if score >= 0.7 and domain_count >= 2:
            conf = "high"
        elif score >= 0.5:
            conf = "medium"
        else:
            conf = "low"
        out.append(
            CandidateScore(
                target=state["target"],
                target_key=key,
                score=round(score, 4),
                confidence=conf,
                evidence_count=row_count,
                distinct_domains=domain_count,
                row_indexes=tuple(row_indexes),
            )
        )
    out.sort(key=lambda x: (-x.score, -x.distinct_domains, -x.evidence_count, x.target.lower()))
    return out


def _significance_score_for_events(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    domains = {str(r.get("domain") or "") for r in rows if str(r.get("domain") or "")}
    major_hits = 0
    for row in rows:
        text = _normalize_whitespace(str(row.get("title") or "") + " " + str(row.get("snippet") or "")).lower()
        if any(term in text for term in MAJOR_EVENT_TERMS):
            major_hits += 1
    score = min(1.0, len(rows) * 0.07 + len(domains) * 0.18 + major_hits * 0.22)
    return round(score, 4)


def _already_acquired_signal(*, company: str, target: str, rows: list[EvidenceRow]) -> bool:
    company_key = _target_key(company)
    target_key = _target_key(target)
    if not company_key or not target_key:
        return False
    for row in rows:
        text = _normalize_whitespace(f"{row.title} {row.snippet}")
        text_key = _target_key(text)
        if company_key not in text_key or target_key not in text_key:
            continue
        lowered = text.lower()
        if re.search(r"\b(acquires|acquired|buys|bought|purchased|purchase of|acquisition of)\b", lowered):
            return True
    return False


def _money_to_usd(text: str) -> int | None:
    raw = _normalize_whitespace(text).lower().replace(",", "")
    m = re.search(r"\$?\s*(\d+(?:\.\d+)?)\s*(k|m|b|t|thousand|million|billion|trillion)?", raw)
    if not m:
        return None
    has_dollar = "$" in raw
    val = float(m.group(1))
    suffix = (m.group(2) or "").lower()
    if not has_dollar and not suffix:
        return None
    mult = 1.0
    if suffix in {"k", "thousand"}:
        mult = 1e3
    elif suffix in {"m", "million"}:
        mult = 1e6
    elif suffix in {"b", "billion"}:
        mult = 1e9
    elif suffix in {"t", "trillion"}:
        mult = 1e12
    amount = int(val * mult)
    if amount < 250_000:
        return None
    if amount > 250_000_000_000:
        return None
    return amount


def _format_usd_short(amount: int | None) -> str:
    if amount is None or amount <= 0:
        return "unknown"
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.0f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.0f}K"
    return f"${amount}"


def _extract_backers(text: str) -> list[str]:
    lowered = text.lower()
    backers: list[str] = []
    m = re.search(r"(?:led by|investors include|backed by)\s+([A-Za-z0-9&.,'\- ]{3,220})", text)
    if m:
        body = m.group(1)
        body = re.split(r"\bwith participation from\b|\bparticipation from\b|\baccording to\b", body, maxsplit=1, flags=re.IGNORECASE)[0]
        body = re.split(r"[.;:]", body, maxsplit=1)[0]
        for token in re.split(r",| and ", body):
            cleaned = _normalize_whitespace(token).strip(" .")
            if not cleaned:
                continue
            if cleaned.lower().startswith(("with ", "including ", "participation ")):
                continue
            if not re.search(r"[A-Za-z]", cleaned):
                continue
            if len(cleaned) <= 38 and not re.search(r"\b(including|participation|from|which|where|founded)\b", cleaned.lower()):
                backers.append(cleaned)
    if "andreessen" in lowered:
        backers.append("Andreessen Horowitz")
    if "sequoia" in lowered:
        backers.append("Sequoia")
    if "insight partners" in lowered:
        backers.append("Insight Partners")
    unique: list[str] = []
    seen: set[str] = set()
    for backer in backers:
        key = _target_key(backer)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(backer)
    return unique[:6]


def _parse_round(text: str) -> str:
    m = re.search(r"\b(series\s+[a-z]|seed|pre-seed|growth|venture|debt)\b", text, flags=re.IGNORECASE)
    if not m:
        return "unknown"
    return m.group(1).title()


def _funding_from_rows(target: str, rows: list[EvidenceRow]) -> FundingSnapshot:
    amount_values: list[int] = []
    round_values: list[str] = []
    backers: list[str] = []

    for row in rows:
        text = f"{row.title}. {row.snippet}".strip()
        for match in re.finditer(r"\$\s*\d+(?:\.\d+)?\s*(?:[KMBT]|thousand|million|billion|trillion)?", text, flags=re.IGNORECASE):
            amt = match.group(0)
            parsed = _money_to_usd(amt)
            if parsed is not None:
                span_start = max(0, match.start() - 48)
                span_end = min(len(text), match.end() + 48)
                context = text[span_start:span_end].lower()
                has_funding_context = any(term in context for term in ("raised", "funding", "round", "investor", "led by", "backed"))
                valuation_only = ("valuation" in context or "valued at" in context) and not has_funding_context
                if valuation_only or not has_funding_context:
                    continue
                amount_values.append(parsed)
        round_val = _parse_round(text)
        if round_val != "unknown":
            round_values.append(round_val)
        backers.extend(_extract_backers(text))

    total_raised = "unknown"
    if amount_values:
        amount_values = sorted(x for x in amount_values if x > 0)
        total_raised = _format_usd_short(max(amount_values))

    latest_round = round_values[0] if round_values else "unknown"
    latest_round_date = "unknown"
    distinct_domains = len({r.domain for r in rows if r.domain})
    evidence_count = len(rows)

    conflicts: list[str] = []
    if len(amount_values) >= 2:
        lo = min(amount_values)
        hi = max(amount_values)
        if lo > 0 and (hi / lo) >= 1.8:
            conflicts.append("amount_mismatch")
    if len(set(round_values)) >= 3:
        conflicts.append("round_mismatch")

    verification_score = 0.0
    verification_score += min(0.5, evidence_count * 0.08)
    verification_score += min(0.3, distinct_domains * 0.12)
    verification_score -= min(0.4, len(conflicts) * 0.2)
    verification_score = max(0.0, min(1.0, verification_score))

    if verification_score >= 0.7 and distinct_domains >= _funding_min_domains() and not conflicts:
        status = "verified"
    elif verification_score >= _funding_low_conf_threshold():
        status = "partial"
    else:
        status = "weak"

    unique_backers: list[str] = []
    seen: set[str] = set()
    for b in backers:
        key = _target_key(b)
        if not key or key in seen:
            continue
        seen.add(key)
        unique_backers.append(b)

    return FundingSnapshot(
        target=target,
        target_key=_target_key(target),
        total_raised=total_raised,
        latest_round=latest_round,
        latest_round_date=latest_round_date,
        backers=tuple(unique_backers[:6]),
        evidence_count=evidence_count,
        distinct_domains=distinct_domains,
        conflict_flags=tuple(conflicts),
        verification_status=status,
        source_rows=tuple(rows[:10]),
    )


def _serialize_evidence_rows(rows: Iterable[EvidenceRow]) -> list[dict[str, Any]]:
    return [
        {
            "title": r.title,
            "snippet": r.snippet,
            "url": r.url,
            "canonical_url": r.canonical_url,
            "publisher": r.publisher,
            "domain": r.domain,
            "published_at_utc": r.published_at_utc,
            "backend": r.backend,
            "quality": r.quality,
        }
        for r in rows
    ]


def _deserialize_evidence_rows(payload: Any) -> list[EvidenceRow]:
    if not isinstance(payload, list):
        return []
    out: list[EvidenceRow] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        out.append(
            EvidenceRow(
                title=_normalize_whitespace(str(row.get("title") or "")),
                snippet=_normalize_whitespace(str(row.get("snippet") or "")),
                url=_normalize_whitespace(str(row.get("url") or "")),
                canonical_url=_normalize_whitespace(str(row.get("canonical_url") or "")),
                publisher=_normalize_whitespace(str(row.get("publisher") or "")),
                domain=_normalize_whitespace(str(row.get("domain") or "")),
                published_at_utc=_normalize_whitespace(str(row.get("published_at_utc") or "")) or None,
                backend=_normalize_whitespace(str(row.get("backend") or "")) or "unknown",
                quality=float(row.get("quality") or 0.0),
            )
        )
    return out


def _chat_completion(*, prompt: str, system: str, temperature: float = 0.2, max_tokens: int = 700) -> str | None:
    client = _openai_client()
    if client is None:
        return None
    try:
        res = client.chat.completions.create(
            model=_openai_model(),
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
        )
    except Exception:
        return None
    try:
        text = str(res.choices[0].message.content or "").strip()
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        return text
    except Exception:
        return None


def _render_funding_lines(snapshot: FundingSnapshot) -> list[str]:
    if snapshot.backers:
        shown = list(snapshot.backers[:4])
        suffix = f" (+{len(snapshot.backers) - 4} more)" if len(snapshot.backers) > 4 else ""
        backers = ", ".join(shown) + suffix
    else:
        backers = "not clearly disclosed"
    lines = [
        f"- Total raised: {snapshot.total_raised}",
        f"- Most recent round: {snapshot.latest_round} ({snapshot.latest_round_date})",
        f"- Backers: {backers}",
    ]
    if snapshot.verification_status == "weak":
        lines.append(f"- {LOW_CONF_WARNING}")
    return lines


def _deterministic_draft(*, company: str, target: str, funding: FundingSnapshot, repitch_note: str | None = None) -> str:
    thesis = f"Acquire/acquihire {target} to accelerate {company}'s product and go-to-market leverage in a strategic wedge."
    if repitch_note:
        thesis = f"{thesis} {repitch_note}"
    lines = [
        f"*Board Seat as a Service — {company}*",
        "*Thesis*",
        f"- {thesis}",
        "*What the target does*",
        f"- {target} builds core technology and workflows that can be integrated quickly into {company}'s existing stack.",
        "*Why it’s a fit for portfolio company*",
        f"- Improves speed-to-market, increases product defensibility, and opens cross-sell paths in {company}'s current customer base.",
        "*Risks*",
        "- Integration complexity and cultural mismatch could delay value capture.",
        "- Pricing/valuation discipline is critical if the process turns competitive.",
        "*Funding history and backers*",
    ]
    lines.extend(_render_funding_lines(funding))
    return "\n".join(lines)


def _web_synth_prompt(company: str, target: str, claims: list[str], funding: FundingSnapshot, repitch_note: str | None) -> str:
    payload = {
        "company": company,
        "target": target,
        "repitch_note": repitch_note,
        "claims": claims[:8],
        "funding": {
            "total_raised": funding.total_raised,
            "latest_round": funding.latest_round,
            "latest_round_date": funding.latest_round_date,
            "backers": list(funding.backers),
            "verification_status": funding.verification_status,
            "warning": LOW_CONF_WARNING if funding.verification_status == "weak" else "",
        },
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def _web_synth_system() -> str:
    return (
        "You write concise board-style acquisition pitches. "
        "Output only markdown with exactly these section headers: "
        "*Thesis*, *What the target does*, *Why it’s a fit for portfolio company*, *Risks*, *Funding history and backers*. "
        "Keep bullets short and natural. Do not quote sources. Do not include sources section."
    )


def _quality_gate(text: str, *, source_rows: list[EvidenceRow]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    raw = str(text or "")
    if not raw.strip():
        return False, ["empty"]

    required = [
        "*thesis*",
        "*what the target does*",
        "*why it’s a fit for portfolio company*",
        "*risks*",
        "*funding history and backers*",
    ]
    lower = raw.lower()
    for needle in required:
        if needle not in lower:
            reasons.append("missing_section")
            break

    if _no_quotes() and re.search(r"\"[^\"]{8,}\"", raw):
        reasons.append("contains_quote")

    if any(term in lower for term in ARTIFACT_TERMS):
        reasons.append("artifact_term")

    for line in raw.splitlines():
        stripped = _normalize_whitespace(line)
        if not stripped or stripped.startswith("*"):
            continue
        if stripped.startswith("-") and len(stripped.split()) < 3:
            reasons.append("fragment_line")
            break

    # lexical overlap against source snippets
    source_texts = [
        _normalize_whitespace(f"{r.title}. {r.snippet}").lower()
        for r in source_rows
        if _normalize_whitespace(r.title) or _normalize_whitespace(r.snippet)
    ]
    draft_norm = _normalize_whitespace(raw).lower()
    if source_texts and draft_norm:
        for src in source_texts[:12]:
            if len(src) < 40:
                continue
            common = _token_overlap_ratio(src, draft_norm)
            if common > 0.68:
                reasons.append("high_lexical_overlap")
                break

    return len(reasons) == 0, sorted(set(reasons))


def _token_overlap_ratio(a: str, b: str) -> float:
    ta = [t for t in re.findall(r"[a-z0-9]+", a.lower()) if len(t) > 2]
    tb = [t for t in re.findall(r"[a-z0-9]+", b.lower()) if len(t) > 2]
    if not ta or not tb:
        return 0.0
    sa = set(ta)
    sb = set(tb)
    denom = max(1, min(len(sa), len(sb)))
    return len(sa & sb) / denom


def _claims_from_rows(rows: list[EvidenceRow], *, limit: int = 8) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        claim = _normalize_whitespace(f"{row.title}. {row.snippet}").strip(" .")
        if not claim:
            continue
        key = hashlib.sha1(claim.lower().encode("utf-8")).hexdigest()[:16]
        if key in seen:
            continue
        seen.add(key)
        out.append(claim)
        if len(out) >= limit:
            break
    return out


def _build_draft(
    *,
    company: str,
    target: str,
    evidence_rows: list[EvidenceRow],
    funding: FundingSnapshot,
    repitch_note: str | None,
) -> DraftResult:
    claims = _claims_from_rows(evidence_rows, limit=8)
    quality_fail_codes: list[str] = []

    # web-grounded loop
    draft = ""
    for attempt in range(_max_web_rewrites() + 1):
        if attempt == 0:
            prompt = _web_synth_prompt(company, target, claims, funding, repitch_note)
            generated = _chat_completion(prompt=prompt, system=_web_synth_system(), temperature=0.2, max_tokens=700)
            draft = generated or _deterministic_draft(company=company, target=target, funding=funding, repitch_note=repitch_note)
        else:
            rewrite_prompt = json.dumps(
                {
                    "company": company,
                    "target": target,
                    "feedback": quality_fail_codes,
                    "draft": draft,
                    "constraints": [
                        "natural concise writing",
                        "no source quotes",
                        "no snippet artifacts",
                        "exact five required sections",
                    ],
                },
                indent=2,
            )
            generated = _chat_completion(
                prompt=rewrite_prompt,
                system="Rewrite the draft to satisfy all constraints. Output markdown only.",
                temperature=0.15,
                max_tokens=650,
            )
            if generated:
                draft = generated
        ok, reasons = _quality_gate(draft, source_rows=evidence_rows)
        if ok:
            return DraftResult(
                text=draft,
                generation_mode="web_synth",
                quality_fail_codes=tuple(),
                memory_rewrite_used=False,
            )
        quality_fail_codes = reasons

    if not _memory_rewrite_on_fail():
        return DraftResult(
            text=draft,
            generation_mode="web_synth",
            quality_fail_codes=tuple(quality_fail_codes),
            memory_rewrite_used=False,
        )

    # memory-only rewrite fallback; no web access here
    fallback = ""
    for _ in range(_memory_rewrite_max_retries() + 1):
        prompt = json.dumps(
            {
                "company": company,
                "target": target,
                "template": [
                    "Thesis",
                    "What the target does",
                    "Why it’s a fit for portfolio company",
                    "Risks",
                    "Funding history and backers",
                ],
                "style": "very concise, natural, no quotes",
            },
            indent=2,
        )
        generated = _chat_completion(
            prompt=prompt,
            system="Write a concise strategic acquisition pitch from model memory only. Markdown only.",
            temperature=0.2,
            max_tokens=550,
        )
        fallback = generated or _deterministic_draft(company=company, target=target, funding=funding, repitch_note=repitch_note)
        ok, _ = _quality_gate(fallback, source_rows=[])
        if ok:
            return DraftResult(
                text=fallback,
                generation_mode="memory_rewrite",
                quality_fail_codes=tuple(quality_fail_codes),
                memory_rewrite_used=True,
            )

    return DraftResult(
        text=_deterministic_draft(company=company, target=target, funding=funding, repitch_note=repitch_note),
        generation_mode="memory_rewrite",
        quality_fail_codes=tuple(quality_fail_codes),
        memory_rewrite_used=True,
    )


def _slack_tokens() -> list[str]:
    tokens: list[str] = []
    env_token = (os.environ.get("SLACK_BOT_TOKEN", "") or "").strip()
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
    return unique


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
                types=_channel_types(),
                exclude_archived=True,
                limit=500,
                cursor=cursor,
            )
        except Exception:
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


def _post_to_slack(*, channel_ref: str, text: str, thread_ts: str | None = None) -> tuple[str | None, str | None, str | None]:
    if WebClient is None:
        return None, None, "slack_sdk_unavailable"
    tokens = _slack_tokens()
    if not tokens:
        return None, None, "slack_token_missing"

    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            channel_id = _resolve_channel_id(client, channel_ref) or channel_ref
            kwargs: dict[str, Any] = {"channel": channel_id, "text": text}
            if thread_ts:
                kwargs["thread_ts"] = thread_ts
            resp = client.chat_postMessage(**kwargs)
            ts = str(resp.get("ts") or "") if isinstance(resp, dict) else ""
            return channel_id, ts or None, None
        except SlackApiError as exc:  # pragma: no cover - runtime integration
            err = str(getattr(exc, "response", {}).get("error") or "") if hasattr(exc, "response") else str(exc)
            if err in {"invalid_auth", "account_inactive", "not_authed"}:
                last_error = err
                continue
            return None, None, f"slack_post_failed:{err or 'unknown'}"
        except Exception as exc:  # pragma: no cover - runtime integration
            return None, None, f"slack_post_failed:{str(exc)[:180]}"
    return None, None, f"slack_post_failed:{last_error or 'unknown'}"


def _source_line(row: EvidenceRow) -> str:
    title = row.title or "Untitled"
    pub = row.publisher or row.domain or "Unknown"
    url = row.url or row.canonical_url
    return f"- {pub} — {title}: {url}"


def _render_sources_thread(rows: list[EvidenceRow], *, limit: int = 6) -> str:
    lines = ["Sources"]
    seen: set[str] = set()
    count = 0
    for row in rows:
        key = row.canonical_url or row.url
        if not key or key in seen:
            continue
        seen.add(key)
        lines.append(_source_line(row))
        count += 1
        if count >= limit:
            break
    if count == 0:
        lines.append("- No high-quality source URLs captured.")
    return "\n".join(lines)


def _company_map() -> dict[str, str]:
    names = {company for company, _ in DEFAULT_PORTCOS}
    names.update(company for company, _ in _parse_portcos())
    out: dict[str, str] = {}
    for name in sorted(names):
        out[_slug_company(name)] = name
    return out


def _discover_channels_from_slack() -> tuple[list[DiscoveryChannel], list[str]]:
    notes: list[str] = []
    if _channel_discovery_mode() != "company_match":
        return [DiscoveryChannel(company=c, channel_ref=ch, channel_id=None) for c, ch in _parse_portcos()], ["discovery:static"]
    if WebClient is None:
        return [], ["discovery:slack_sdk_unavailable"]
    tokens = _slack_tokens()
    if not tokens:
        return [], ["discovery:slack_token_missing"]

    company_map = _company_map()
    discovered: list[DiscoveryChannel] = []
    seen_channel_ids: set[str] = set()

    for token in tokens:
        client = WebClient(token=token)
        cursor: str | None = None
        while True:
            try:
                payload = client.conversations_list(
                    types=_channel_types(),
                    exclude_archived=True,
                    limit=500,
                    cursor=cursor,
                )
            except Exception as exc:  # pragma: no cover - runtime integration
                notes.append(f"discovery:error:{str(exc)[:80]}")
                break
            rows = payload.get("channels") if isinstance(payload, dict) else None
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    channel_name = str(row.get("name") or "").strip()
                    channel_id = str(row.get("id") or "").strip()
                    if not channel_name or not channel_id or channel_id in seen_channel_ids:
                        continue
                    slug = _slug_company(channel_name)
                    company = company_map.get(slug)
                    if not company:
                        continue
                    seen_channel_ids.add(channel_id)
                    discovered.append(DiscoveryChannel(company=company, channel_ref=channel_name, channel_id=channel_id))
            meta = payload.get("response_metadata") if isinstance(payload, dict) else None
            next_cursor = str((meta or {}).get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
            if not next_cursor:
                break
            cursor = next_cursor
        if discovered:
            break

    if not discovered:
        notes.append("discovery:no_company_match")
    return discovered, notes


class BoardSeatStore:
    def __init__(self, path: Path | None = None) -> None:
        chosen_path = path or _db_path()
        try:
            chosen_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            chosen_path = _fallback_db_path()
            chosen_path.parent.mkdir(parents=True, exist_ok=True)
        self.path = chosen_path
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_target_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    channel_ref TEXT,
                    source TEXT,
                    posted_at_utc TEXT,
                    run_date_local TEXT,
                    message_ts TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_board_seat_target_memory_company
                ON board_seat_target_memory(company)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_board_seat_target_memory_target_key
                ON board_seat_target_memory(target_key)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date_local TEXT NOT NULL,
                    company TEXT NOT NULL,
                    channel_ref TEXT,
                    channel_id TEXT,
                    status TEXT NOT NULL,
                    reason TEXT,
                    gate_reason TEXT,
                    target TEXT,
                    target_key TEXT,
                    target_confidence TEXT,
                    funding_confidence TEXT,
                    generation_mode TEXT,
                    quality_fail_codes TEXT,
                    memory_rewrite_used INTEGER NOT NULL DEFAULT 0,
                    message_ts TEXT,
                    sources_thread_ts TEXT,
                    warning_message_ts TEXT,
                    posted_at_utc TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_board_seat_runs_recent
                ON board_seat_runs(created_at_utc DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date_local TEXT NOT NULL,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    score REAL,
                    confidence TEXT,
                    evidence_count INTEGER,
                    distinct_domains INTEGER,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_target_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    title TEXT,
                    snippet TEXT,
                    source_url TEXT,
                    canonical_url TEXT,
                    publisher TEXT,
                    domain TEXT,
                    significance REAL,
                    occurred_at_utc TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_board_seat_target_events_recent
                ON board_seat_target_events(target_key, created_at_utc DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_funding_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    source_rows_json TEXT NOT NULL,
                    evidence_count INTEGER NOT NULL DEFAULT 0,
                    distinct_domains INTEGER NOT NULL DEFAULT 0,
                    conflict_flags TEXT,
                    verification_status TEXT,
                    updated_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_channel_discovery (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date_local TEXT NOT NULL,
                    company TEXT NOT NULL,
                    channel_ref TEXT NOT NULL,
                    channel_id TEXT,
                    discovered_at_utc TEXT NOT NULL
                )
                """
            )
            self._migrate_legacy_schema(conn)

    def _table_columns(self, conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        out: set[str] = set()
        for row in rows:
            try:
                out.add(str(row["name"]))
            except Exception:
                if len(row) >= 2:
                    out.add(str(row[1]))
        return out

    def _ensure_columns(
        self,
        conn: sqlite3.Connection,
        *,
        table_name: str,
        column_specs: list[tuple[str, str]],
    ) -> None:
        existing = self._table_columns(conn, table_name)
        for column_name, spec in column_specs:
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {spec}")
            existing.add(column_name)

    def _migrate_legacy_schema(self, conn: sqlite3.Connection) -> None:
        # Idempotent, additive-only migrations for legacy DB files.
        self._ensure_columns(
            conn,
            table_name="board_seat_runs",
            column_specs=[
                ("channel_id", "channel_id TEXT"),
                ("gate_reason", "gate_reason TEXT"),
                ("target_confidence", "target_confidence TEXT"),
                ("funding_confidence", "funding_confidence TEXT"),
                ("generation_mode", "generation_mode TEXT"),
                ("quality_fail_codes", "quality_fail_codes TEXT"),
                ("memory_rewrite_used", "memory_rewrite_used INTEGER NOT NULL DEFAULT 0"),
                ("sources_thread_ts", "sources_thread_ts TEXT"),
                ("warning_message_ts", "warning_message_ts TEXT"),
            ],
        )
        self._ensure_columns(
            conn,
            table_name="board_seat_target_events",
            column_specs=[
                ("source_url", "source_url TEXT"),
                ("canonical_url", "canonical_url TEXT"),
                ("publisher", "publisher TEXT"),
                ("domain", "domain TEXT"),
                ("significance", "significance REAL"),
                ("occurred_at_utc", "occurred_at_utc TEXT"),
            ],
        )
        self._ensure_columns(
            conn,
            table_name="board_seat_funding_cache",
            column_specs=[
                ("source_rows_json", "source_rows_json TEXT NOT NULL DEFAULT '[]'"),
                ("evidence_count", "evidence_count INTEGER NOT NULL DEFAULT 0"),
                ("distinct_domains", "distinct_domains INTEGER NOT NULL DEFAULT 0"),
                ("conflict_flags", "conflict_flags TEXT"),
                ("verification_status", "verification_status TEXT"),
            ],
        )

    def record_run(self, payload: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_runs (
                    run_date_local, company, channel_ref, channel_id, status, reason, gate_reason,
                    target, target_key, target_confidence, funding_confidence, generation_mode,
                    quality_fail_codes, memory_rewrite_used, message_ts, sources_thread_ts,
                    warning_message_ts, posted_at_utc, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.get("run_date_local") or _today_key()),
                    str(payload.get("company") or ""),
                    str(payload.get("channel_ref") or ""),
                    str(payload.get("channel_id") or ""),
                    str(payload.get("status") or ""),
                    str(payload.get("reason") or ""),
                    str(payload.get("gate_reason") or ""),
                    str(payload.get("target") or ""),
                    str(payload.get("target_key") or ""),
                    str(payload.get("target_confidence") or ""),
                    str(payload.get("funding_confidence") or ""),
                    str(payload.get("generation_mode") or ""),
                    json.dumps(payload.get("quality_fail_codes") or []),
                    1 if bool(payload.get("memory_rewrite_used")) else 0,
                    str(payload.get("message_ts") or ""),
                    str(payload.get("sources_thread_ts") or ""),
                    str(payload.get("warning_message_ts") or ""),
                    str(payload.get("posted_at_utc") or ""),
                    _utc_now_iso(),
                ),
            )

    def record_candidates(self, *, company: str, candidates: list[CandidateScore], run_date_local: str) -> None:
        if not candidates:
            return
        with self._connect() as conn:
            for c in candidates:
                conn.execute(
                    """
                    INSERT INTO board_seat_candidates (
                        run_date_local, company, target, target_key, score, confidence,
                        evidence_count, distinct_domains, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_date_local,
                        company,
                        c.target,
                        c.target_key,
                        float(c.score),
                        c.confidence,
                        int(c.evidence_count),
                        int(c.distinct_domains),
                        _utc_now_iso(),
                    ),
                )

    def record_channel_discovery(self, *, run_date_local: str, channels: list[DiscoveryChannel]) -> None:
        if not channels:
            return
        with self._connect() as conn:
            for channel in channels:
                conn.execute(
                    """
                    INSERT INTO board_seat_channel_discovery (
                        run_date_local, company, channel_ref, channel_id, discovered_at_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        run_date_local,
                        channel.company,
                        channel.channel_ref,
                        channel.channel_id or "",
                        _utc_now_iso(),
                    ),
                )

    def record_event(self, *, company: str, target: str, row: EvidenceRow, significance: float) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_target_events (
                    company, target, target_key, title, snippet, source_url, canonical_url,
                    publisher, domain, significance, occurred_at_utc, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company,
                    target,
                    _target_key(target),
                    row.title,
                    row.snippet,
                    row.url,
                    row.canonical_url,
                    row.publisher,
                    row.domain,
                    float(significance),
                    row.published_at_utc or "",
                    _utc_now_iso(),
                ),
            )

    def recent_events(self, *, company: str, target_key: str, since_utc: datetime | None = None, limit: int = 200) -> list[dict[str, Any]]:
        query = """
            SELECT company, target, target_key, title, snippet, source_url, canonical_url,
                   publisher, domain, significance, occurred_at_utc, created_at_utc
            FROM board_seat_target_events
            WHERE lower(company)=? AND target_key=?
        """
        params: list[Any] = [company.lower(), target_key]
        if since_utc is not None:
            query += " AND created_at_utc >= ?"
            params.append(since_utc.replace(microsecond=0).isoformat())
        query += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(5000, limit)))
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def upsert_funding_cache(self, snapshot: FundingSnapshot) -> None:
        payload_json = json.dumps(
            {
                "target": snapshot.target,
                "target_key": snapshot.target_key,
                "total_raised": snapshot.total_raised,
                "latest_round": snapshot.latest_round,
                "latest_round_date": snapshot.latest_round_date,
                "backers": list(snapshot.backers),
                "evidence_count": snapshot.evidence_count,
                "distinct_domains": snapshot.distinct_domains,
                "conflict_flags": list(snapshot.conflict_flags),
                "verification_status": snapshot.verification_status,
            },
            sort_keys=True,
        )
        source_json = json.dumps(_serialize_evidence_rows(snapshot.source_rows), sort_keys=True)
        now_iso = _utc_now_iso()
        exp_iso = (_utc_now() + timedelta(hours=_funding_cache_ttl_hours())).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_funding_cache (
                    target, target_key, payload_json, source_rows_json, evidence_count,
                    distinct_domains, conflict_flags, verification_status, updated_at_utc, expires_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_key) DO UPDATE SET
                    target=excluded.target,
                    payload_json=excluded.payload_json,
                    source_rows_json=excluded.source_rows_json,
                    evidence_count=excluded.evidence_count,
                    distinct_domains=excluded.distinct_domains,
                    conflict_flags=excluded.conflict_flags,
                    verification_status=excluded.verification_status,
                    updated_at_utc=excluded.updated_at_utc,
                    expires_at_utc=excluded.expires_at_utc
                """,
                (
                    snapshot.target,
                    snapshot.target_key,
                    payload_json,
                    source_json,
                    snapshot.evidence_count,
                    snapshot.distinct_domains,
                    json.dumps(list(snapshot.conflict_flags)),
                    snapshot.verification_status,
                    now_iso,
                    exp_iso,
                ),
            )

    def get_funding_cache(self, *, target_key: str) -> FundingSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json, source_rows_json, expires_at_utc
                FROM board_seat_funding_cache
                WHERE target_key=?
                LIMIT 1
                """,
                (target_key,),
            ).fetchone()
        if row is None:
            return None
        expires = _parse_iso(str(row["expires_at_utc"]))
        if expires is not None and expires < _utc_now():
            return None
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
            source_rows = _deserialize_evidence_rows(json.loads(str(row["source_rows_json"] or "[]")))
            return FundingSnapshot(
                target=str(payload.get("target") or ""),
                target_key=str(payload.get("target_key") or target_key),
                total_raised=str(payload.get("total_raised") or "unknown"),
                latest_round=str(payload.get("latest_round") or "unknown"),
                latest_round_date=str(payload.get("latest_round_date") or "unknown"),
                backers=tuple(str(x) for x in (payload.get("backers") or []) if str(x).strip()),
                evidence_count=int(payload.get("evidence_count") or 0),
                distinct_domains=int(payload.get("distinct_domains") or 0),
                conflict_flags=tuple(str(x) for x in (payload.get("conflict_flags") or []) if str(x).strip()),
                verification_status=str(payload.get("verification_status") or "weak"),
                source_rows=tuple(source_rows),
            )
        except Exception:
            return None

    def funding_cache_rows(self, *, limit: int = 500) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT target, target_key, evidence_count, distinct_domains,
                       conflict_flags, verification_status, updated_at_utc, expires_at_utc
                FROM board_seat_funding_cache
                ORDER BY updated_at_utc DESC
                LIMIT ?
                """,
                (max(1, min(5000, limit)),),
            ).fetchall()
        return [dict(r) for r in rows]

    def record_target(
        self,
        *,
        company: str,
        target: str,
        channel_ref: str,
        channel_id: str | None,
        source: str,
        posted_at_utc: str,
        run_date_local: str,
        message_ts: str | None,
    ) -> bool:
        company_text = str(company or "").strip()
        target_text = str(target or "").strip()
        if not company_text or not target_text:
            return False
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_target_memory (
                    company,
                    target,
                    target_key,
                    channel_ref,
                    source,
                    posted_at_utc,
                    run_date_local,
                    message_ts,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company_text,
                    target_text,
                    _target_key(target_text),
                    str(channel_ref or channel_id or "").strip(),
                    str(source or "manual").strip(),
                    str(posted_at_utc or "").strip(),
                    str(run_date_local or _today_key()).strip(),
                    str(message_ts or "").strip(),
                    _utc_now_iso(),
                ),
            )
        return True

    def target_ledger_rows(self, *, company: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        query = """
            SELECT company, target, target_key, channel_ref, source, posted_at_utc, run_date_local, message_ts, created_at_utc
            FROM board_seat_target_memory
        """
        params: list[Any] = []
        if company:
            query += " WHERE lower(company) = ?"
            params.append(str(company).strip().lower())
        query += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(5000, int(limit))))
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def target_memory_count(self, *, company: str | None = None) -> int:
        with self._connect() as conn:
            if company:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM board_seat_target_memory WHERE lower(company)=?",
                    (str(company).strip().lower(),),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM board_seat_target_memory").fetchone()
        return int(row["c"]) if row is not None else 0

    def latest_target_post(self, *, company: str, target_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT company, target, target_key, posted_at_utc, run_date_local, source
                FROM board_seat_target_memory
                WHERE lower(company)=? AND target_key=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (company.lower(), target_key),
            ).fetchone()
        return dict(row) if row is not None else None

    def latest_runs(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run_date_local, company, channel_ref, channel_id, status, reason, gate_reason,
                       target, target_key, target_confidence, funding_confidence, generation_mode,
                       quality_fail_codes, memory_rewrite_used, message_ts, sources_thread_ts,
                       warning_message_ts, posted_at_utc, created_at_utc
                FROM board_seat_runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, min(5000, limit)),),
            ).fetchall()
        return [dict(r) for r in rows]


def _repitch_note(*, last_post: dict[str, Any] | None, events: list[dict[str, Any]]) -> str | None:
    if not last_post:
        return None
    posted = _parse_iso(str(last_post.get("posted_at_utc") or ""))
    posted_txt = posted.date().isoformat() if posted else "prior run"
    top_titles = []
    for row in events[:2]:
        title = _normalize_whitespace(str(row.get("title") or ""))
        if title:
            top_titles.append(title)
    if top_titles:
        return f"Previously pitched on {posted_txt}. New evidence: {'; '.join(top_titles)}."
    return f"Previously pitched on {posted_txt}. New evidence now materially strengthens the case."


def _should_block_recent(*, last_post: dict[str, Any] | None, now_utc: datetime) -> bool:
    if not last_post:
        return False
    last_dt = _parse_iso(str(last_post.get("posted_at_utc") or ""))
    if last_dt is None:
        return False
    return (now_utc - last_dt) < timedelta(days=_target_lock_days())


def _compute_repitch_eligibility(*, store: BoardSeatStore, company: str, target: CandidateScore, now_utc: datetime) -> tuple[bool, float, list[dict[str, Any]]]:
    last = store.latest_target_post(company=company, target_key=target.target_key)
    if not last:
        return True, 1.0, []
    last_dt = _parse_iso(str(last.get("posted_at_utc") or ""))
    if last_dt is None:
        return True, 1.0, []
    if (now_utc - last_dt) < timedelta(days=_target_lock_days()):
        return False, 0.0, []
    events = store.recent_events(company=company, target_key=target.target_key, since_utc=last_dt, limit=200)
    score = _significance_score_for_events(events)
    return score >= _repitch_significance_min(), score, events


def _pick_target(
    *,
    company: str,
    rows: list[EvidenceRow],
    store: BoardSeatStore,
    now_utc: datetime,
    run_date_local: str,
) -> tuple[CandidateScore | None, str | None, float, list[CandidateScore], list[dict[str, Any]]]:
    candidates = _extract_candidates(company, rows)
    store.record_candidates(company=company, candidates=candidates, run_date_local=run_date_local)

    for candidate in candidates[:6]:
        # always keep events flowing for promising targets
        for idx in candidate.row_indexes[:3]:
            if 0 <= idx < len(rows):
                store.record_event(company=company, target=candidate.target, row=rows[idx], significance=candidate.score)

    if not candidates:
        return None, "invalid_target", 0.0, candidates, []

    blocked_acquired = False
    for c in candidates:
        candidate_rows = [rows[i] for i in c.row_indexes if 0 <= i < len(rows)]
        if _already_acquired_signal(company=company, target=c.target, rows=candidate_rows):
            blocked_acquired = True
            continue
        valid, reason = _is_valid_target_name(target=c.target, company=company)
        if not valid:
            continue
        if _require_high_conf_new_target() and c.confidence != "high":
            continue
        recent = store.latest_target_post(company=company, target_key=c.target_key)
        if recent is None:
            return c, None, 1.0, candidates, []
        if _should_block_recent(last_post=recent, now_utc=now_utc):
            continue
        eligible, sig_score, events = _compute_repitch_eligibility(store=store, company=company, target=c, now_utc=now_utc)
        if eligible:
            return c, None, sig_score, candidates, events

    # choose best reason for gate
    top = candidates[0]
    if blocked_acquired:
        return None, "target_already_acquired", 0.0, candidates, []
    valid, reason = _is_valid_target_name(target=top.target, company=company)
    if not valid:
        return None, reason, 0.0, candidates, []
    if _require_high_conf_new_target() and top.confidence != "high":
        return None, "target_confidence_not_high", 0.0, candidates, []
    latest = store.latest_target_post(company=company, target_key=top.target_key)
    if latest and _should_block_recent(last_post=latest, now_utc=now_utc):
        return None, "target_not_new", 0.0, candidates, []
    return None, "repitch_not_significant", 0.0, candidates, []


def _funding_snapshot_for_target(*, store: BoardSeatStore, target: str, force_refresh: bool = False) -> FundingSnapshot:
    key = _target_key(target)
    if not force_refresh:
        cached = store.get_funding_cache(target_key=key)
        if cached is not None:
            return cached

    rows, _ = _collect_web_rows(_search_queries_for_funding(target))
    snapshot = _funding_from_rows(target, rows)
    store.upsert_funding_cache(snapshot)
    return snapshot


def _process_company(
    *,
    store: BoardSeatStore,
    channel: DiscoveryChannel,
    dry_run: bool,
    run_date_local: str,
    now_utc: datetime,
) -> tuple[str, dict[str, Any]]:
    company = channel.company
    result_base = {
        "company": company,
        "channel_ref": channel.channel_ref,
        "channel_id": channel.channel_id,
        "run_date_local": run_date_local,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
    }

    rows, search_notes = _collect_web_rows(_search_queries_for_company(company))
    if not rows:
        payload = {
            **result_base,
            "status": "skipped",
            "reason": "no_high_confidence_new_target",
            "gate_reason": "invalid_target",
            "search_notes": search_notes,
            "delivery_mode_applied": "skip",
        }
        store.record_run(payload)
        return "skipped", payload

    chosen, gate_reason, repitch_score, candidates, repitch_events = _pick_target(
        company=company,
        rows=rows,
        store=store,
        now_utc=now_utc,
        run_date_local=run_date_local,
    )
    if chosen is None:
        payload = {
            **result_base,
            "status": "skipped",
            "reason": "no_high_confidence_new_target",
            "gate_reason": gate_reason or "target_confidence_not_high",
            "search_notes": search_notes,
            "candidates_considered": len(candidates),
            "delivery_mode_applied": "skip",
        }
        store.record_run(payload)
        return "skipped", payload

    chosen_rows = [rows[i] for i in chosen.row_indexes if 0 <= i < len(rows)]
    funding = _funding_snapshot_for_target(store=store, target=chosen.target)
    last_post = store.latest_target_post(company=company, target_key=chosen.target_key)
    repitch_note = _repitch_note(last_post=last_post, events=repitch_events) if last_post else None

    draft = _build_draft(
        company=company,
        target=chosen.target,
        evidence_rows=chosen_rows,
        funding=funding,
        repitch_note=repitch_note,
    )

    posted_at_utc = _utc_now_iso()
    message_ts: str | None = None
    sources_thread_ts: str | None = None
    warning_ts: str | None = None
    post_error: str | None = None
    effective_channel_ref = channel.channel_id or channel.channel_ref
    posted_channel_id = channel.channel_id

    if dry_run:
        text = draft.text
    else:
        channel_id, ts, err = _post_to_slack(channel_ref=effective_channel_ref, text=draft.text)
        if err:
            post_error = err
        else:
            message_ts = ts
            posted_channel_id = channel_id or posted_channel_id
            effective_channel_ref = posted_channel_id or channel.channel_ref
            if _sources_in_thread():
                _, src_ts, _ = _post_to_slack(
                    channel_ref=effective_channel_ref,
                    text=_render_sources_thread(chosen_rows + list(funding.source_rows)),
                    thread_ts=message_ts,
                )
                sources_thread_ts = src_ts
            if draft.memory_rewrite_used and _memory_rewrite_thread_warning():
                _, warn_ts, _ = _post_to_slack(
                    channel_ref=effective_channel_ref,
                    text=MEMORY_FALLBACK_WARNING,
                    thread_ts=message_ts,
                )
                warning_ts = warn_ts
        text = draft.text

    if post_error:
        payload = {
            **result_base,
            "status": "skipped",
            "reason": post_error,
            "gate_reason": "delivery_failed",
            "target": chosen.target,
            "target_key": chosen.target_key,
            "target_confidence": chosen.confidence,
            "funding_confidence": funding.verification_status,
            "generation_mode": draft.generation_mode,
            "quality_fail_codes": list(draft.quality_fail_codes),
            "memory_rewrite_used": draft.memory_rewrite_used,
            "delivery_mode_applied": "skip",
        }
        store.record_run(payload)
        return "skipped", payload

    store.record_target(
        company=company,
        target=chosen.target,
        channel_ref=channel.channel_ref,
        channel_id=posted_channel_id,
        source="board_seat_v1",
        posted_at_utc=posted_at_utc,
        run_date_local=run_date_local,
        message_ts=message_ts,
    )

    payload = {
        **result_base,
        "status": "sent",
        "channel_id": posted_channel_id,
        "target": chosen.target,
        "target_key": chosen.target_key,
        "target_confidence": chosen.confidence,
        "target_confidence_score": chosen.score,
        "funding_confidence": funding.verification_status,
        "generation_mode": draft.generation_mode,
        "quality_fail_codes": list(draft.quality_fail_codes),
        "memory_rewrite_used": draft.memory_rewrite_used,
        "warning_thread_posted": bool(warning_ts),
        "reason": "",
        "gate_reason": "",
        "message_ts": message_ts,
        "sources_thread_ts": sources_thread_ts,
        "warning_message_ts": warning_ts,
        "posted_at_utc": posted_at_utc,
        "search_notes": search_notes,
        "repitch_significance_score": repitch_score,
        "preview": text if dry_run else "",
        "delivery_mode_applied": "dry_run_preview" if dry_run else "post",
    }
    store.record_run(payload)
    return "sent", payload


def run_once(*, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    now_local = datetime.now(_timezone())
    now_utc = _utc_now()
    run_date_local = _today_key()
    result: dict[str, Any] = {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "run_date_local": run_date_local,
        "timezone": str(_timezone()),
        "board_seat_enabled": _board_seat_enabled(),
        "reset_mode": _reset_mode_enabled(),
        "sent": [],
        "skipped": [],
        "search_order": _search_order(),
        "require_high_conf_new_target": _require_high_conf_new_target(),
        "target_lock_days": _target_lock_days(),
        "repitch_significance_min": _repitch_significance_min(),
        "schedule_time": _board_seat_time(),
        "weekdays_only": _weekdays_only(),
    }

    if _reset_mode_enabled():
        for company, channel_ref in _parse_portcos():
            result["skipped"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "reason": RESET_REASON,
                    "detail": "Board Seat reset mode is enabled.",
                    "delivery_mode_applied": "skip",
                }
            )
        return result

    if not _board_seat_enabled():
        for company, channel_ref in _parse_portcos():
            result["skipped"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "reason": "board_seat_disabled",
                    "detail": "Enable COATUE_CLAW_BOARD_SEAT_ENABLED=1.",
                    "delivery_mode_applied": "skip",
                }
            )
        return result

    if not _within_schedule_window(now_local, force=force):
        for company, channel_ref in _parse_portcos():
            result["skipped"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "reason": "outside_scheduled_window",
                    "detail": f"Configured for weekdays { _board_seat_time() } {str(_timezone())}",
                    "delivery_mode_applied": "skip",
                }
            )
        return result

    store = BoardSeatStore()
    discovered, discovery_notes = _discover_channels_from_slack()
    if not discovered:
        discovered = [DiscoveryChannel(company=c, channel_ref=ch, channel_id=None) for c, ch in _parse_portcos()]
        discovery_notes.append("discovery:fallback_static")

    store.record_channel_discovery(run_date_local=run_date_local, channels=discovered)
    result["discovered_channels"] = [
        {"company": item.company, "channel_ref": item.channel_ref, "channel_id": item.channel_id}
        for item in discovered
    ]
    result["discovery_notes"] = discovery_notes

    for channel in discovered:
        try:
            status, payload = _process_company(
                store=store,
                channel=channel,
                dry_run=dry_run,
                run_date_local=run_date_local,
                now_utc=now_utc,
            )
        except Exception as exc:
            status = "skipped"
            payload = {
                "company": channel.company,
                "channel_ref": channel.channel_ref,
                "channel_id": channel.channel_id,
                "status": "skipped",
                "reason": f"run_error:{str(exc)[:160]}",
                "gate_reason": "internal_error",
                "delivery_mode_applied": "skip",
            }
            store.record_run({
                **payload,
                "run_date_local": run_date_local,
                "quality_fail_codes": [],
                "memory_rewrite_used": False,
            })
        result["sent" if status == "sent" else "skipped"].append(payload)

    return result


def status() -> dict[str, Any]:
    store = BoardSeatStore()
    portcos = _parse_portcos()
    recent = store.latest_runs(limit=500)
    sent = [r for r in recent if str(r.get("status") or "") == "sent"]
    skipped = [r for r in recent if str(r.get("status") or "") != "sent"]
    fallback_count = sum(1 for r in recent if int(r.get("memory_rewrite_used") or 0) == 1)

    skip_reason_counts: dict[str, int] = {}
    for row in skipped:
        reason = str(row.get("reason") or "unknown")
        skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1

    funding_rows = store.funding_cache_rows(limit=2000)
    funding_mix: dict[str, int] = {}
    oldest_cache_age_hours: float | None = None
    now = _utc_now()
    for row in funding_rows:
        status_key = str(row.get("verification_status") or "unknown")
        funding_mix[status_key] = funding_mix.get(status_key, 0) + 1
        updated = _parse_iso(str(row.get("updated_at_utc") or ""))
        if updated is None:
            continue
        age_hours = max(0.0, (now - updated).total_seconds() / 3600.0)
        if oldest_cache_age_hours is None or age_hours > oldest_cache_age_hours:
            oldest_cache_age_hours = age_hours

    return {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "run_date_local": _today_key(),
        "timezone": str(_timezone()),
        "status": "active_v1" if _board_seat_enabled() and not _reset_mode_enabled() else "disabled_or_reset",
        "reset_mode": _reset_mode_enabled(),
        "board_seat_enabled": _board_seat_enabled(),
        "schedule_time": _board_seat_time(),
        "weekdays_only": _weekdays_only(),
        "channel_discovery_mode": _channel_discovery_mode(),
        "channel_types": _channel_types(),
        "search_order": _search_order(),
        "require_high_conf_new_target": _require_high_conf_new_target(),
        "target_lock_days": _target_lock_days(),
        "repitch_significance_min": _repitch_significance_min(),
        "funding_mode": "web_only",
        "memory_rewrite_on_fail": _memory_rewrite_on_fail(),
        "recent_runs_count": len(recent),
        "recent_sent_count": len(sent),
        "recent_skipped_count": len(skipped),
        "fallback_memory_rewrite_count": fallback_count,
        "skip_reason_counts": skip_reason_counts,
        "funding_confidence_distribution": funding_mix,
        "oldest_funding_cache_age_hours": round(oldest_cache_age_hours, 2) if oldest_cache_age_hours is not None else None,
        "portcos": [{"company": company, "channel_ref": channel_ref} for company, channel_ref in portcos],
        "target_memory_counts": {
            "total": store.target_memory_count(),
            "by_company": {company: store.target_memory_count(company=company) for company, _ in portcos},
        },
    }


def _funding_entities(all_portcos: bool, company: str) -> list[str]:
    if all_portcos:
        return [item[0] for item in _parse_portcos()]
    clean_company = str(company or "").strip()
    return [clean_company] if clean_company else []


def _refresh_funding_payload(*, entities: list[str], include_recent_targets: bool, report: bool) -> dict[str, Any]:
    store = BoardSeatStore()
    refreshed: list[dict[str, Any]] = []
    for company in entities:
        targets: list[str] = []
        if include_recent_targets:
            seen: set[str] = set()
            for row in store.target_ledger_rows(company=company, limit=50):
                target = _normalize_whitespace(str(row.get("target") or ""))
                if not target:
                    continue
                key = _target_key(target)
                if key in seen:
                    continue
                seen.add(key)
                targets.append(target)
                if len(targets) >= 8:
                    break
        for target in targets:
            snap = _funding_snapshot_for_target(store=store, target=target, force_refresh=True)
            refreshed.append(
                {
                    "company": company,
                    "target": target,
                    "verification_status": snap.verification_status,
                    "evidence_count": snap.evidence_count,
                    "distinct_domains": snap.distinct_domains,
                    "conflict_flags": list(snap.conflict_flags),
                }
            )

    report_path: str | None = None
    if report:
        report_path = str(_write_funding_quality_report(store=store))

    return {
        "ok": True,
        "status": "ok",
        "action": "funding-quality-report" if report else "refresh-funding",
        "entities": entities,
        "include_recent_targets": include_recent_targets,
        "refreshed": refreshed,
        "count": len(refreshed),
        "report_path": report_path,
    }


def _write_funding_quality_report(*, store: BoardSeatStore) -> Path:
    day = _today_key()
    path = _artifact_dir() / f"funding-quality-report-{day}.md"
    rows = store.funding_cache_rows(limit=5000)
    lines = [
        f"# Funding Quality Report ({day})",
        "",
        f"Rows: `{len(rows)}`",
        "",
        "| Target | Verification | Evidence | Domains | Updated UTC |",
        "|---|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row.get('target')} | {row.get('verification_status')} | {row.get('evidence_count')} | {row.get('distinct_domains')} | {row.get('updated_at_utc')} |"
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _seed_target(*, company: str, target: str, channel_ref: str) -> SeedTargetResult:
    store = BoardSeatStore()
    now_iso = _utc_now_iso()
    clean_company = str(company or "").strip()
    clean_target = str(target or "").strip()
    inserted = store.record_target(
        company=clean_company,
        target=clean_target,
        channel_ref=str(channel_ref or "manual").strip() or "manual",
        channel_id=None,
        source="manual_seed",
        posted_at_utc=now_iso,
        run_date_local=_today_key(),
        message_ts=None,
    )
    return SeedTargetResult(
        inserted=inserted,
        company=clean_company,
        target=clean_target,
        target_key=_target_key(clean_target),
        posted_at_utc=now_iso,
    )


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-board-seat-daily")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run-once")
    run.add_argument("--force", action="store_true")
    run.add_argument("--dry-run", action="store_true")

    sub.add_parser("status")

    seed = sub.add_parser("seed-target")
    seed.add_argument("--company", required=True)
    seed.add_argument("--target", required=True)
    seed.add_argument("--channel-ref", default="manual")

    export = sub.add_parser("export-ledger")
    export.add_argument("--company", default="")

    memory = sub.add_parser("target-memory")
    memory.add_argument("--company", default="")
    memory.add_argument("--limit", type=int, default=200)

    refresh = sub.add_parser("refresh-funding")
    refresh.add_argument("--all-portcos", action="store_true")
    refresh.add_argument("--company", default="")
    refresh.add_argument("--include-recent-targets", action="store_true")

    report = sub.add_parser("funding-quality-report")
    report.add_argument("--all-portcos", action="store_true")
    report.add_argument("--company", default="")
    report.add_argument("--include-recent-targets", action="store_true")

    args = parser.parse_args()

    if args.command == "run-once":
        payload = run_once(force=bool(args.force), dry_run=bool(args.dry_run))
    elif args.command == "status":
        payload = status()
    elif args.command == "seed-target":
        seeded = _seed_target(company=args.company, target=args.target, channel_ref=args.channel_ref)
        payload = {
            "ok": True,
            "inserted": seeded.inserted,
            "company": seeded.company,
            "target": seeded.target,
            "target_key": seeded.target_key,
            "posted_at_utc": seeded.posted_at_utc,
        }
    elif args.command == "export-ledger":
        store = BoardSeatStore()
        rows = store.target_ledger_rows(company=(args.company or None), limit=5000)
        payload = {
            "ok": True,
            "status": "active_v1",
            "rows": rows,
            "count": len(rows),
            "company_filter": args.company or "",
        }
    elif args.command == "target-memory":
        store = BoardSeatStore()
        rows = store.target_ledger_rows(company=(args.company or None), limit=max(1, min(5000, int(args.limit))))
        payload = {
            "ok": True,
            "target_lock_days": _target_lock_days(),
            "status": "active_v1",
            "company_filter": args.company or "",
            "count": len(rows),
            "rows": rows,
        }
    elif args.command == "refresh-funding":
        entities = _funding_entities(bool(args.all_portcos), str(args.company or ""))
        payload = _refresh_funding_payload(entities=entities, include_recent_targets=bool(args.include_recent_targets), report=False)
    elif args.command == "funding-quality-report":
        entities = _funding_entities(bool(args.all_portcos), str(args.company or ""))
        payload = _refresh_funding_payload(entities=entities, include_recent_targets=bool(args.include_recent_targets), report=True)
    else:
        payload = {"ok": False, "error": "unknown_command"}

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

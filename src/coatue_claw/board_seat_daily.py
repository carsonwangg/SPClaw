from __future__ import annotations

import argparse
import csv
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
import html
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse
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
BOARD_SEAT_FORMAT_VERSION = "v6_richtext_target_does_monthly_theme"
MAX_LINE_WORDS = 18
MAX_LINE_WORDS_DEFAULT = 0
TARGET_LOCK_DAYS_DEFAULT = 14
HARD_NO_REPITCH_DAYS = 14
THESIS_LABELS: tuple[str, ...] = ("Idea", "Target does", "Why now", "What's different", "MOS/risks", "Bottom line")
CONTEXT_LABELS: tuple[str, ...] = ("Current efforts", "Domain fit/gaps")
FUNDING_LABELS: tuple[str, ...] = ("History", "Latest round/backers")
FUNDING_CACHE_TTL_DAYS_DEFAULT = 14
UNKNOWN_FUNDING_TEXT = "Target funding data is limited; verify via Crunchbase/PitchBook before action."
LOW_CONFIDENCE_FUNDING_WARNING_TEXT = "Funding data is low-confidence; verify before action."
REPITCH_DISCOURAGE_TEXT = "Spencer preference: avoid repeated ideas unless evidence is exceptionally material."
WEB_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
BRAVE_SEARCH_RESULTS = 5
FUNDING_EXTRACT_MODEL = "gpt-5.2-chat-latest"
FUNDING_WEB_TOP_ROWS_DEFAULT = 8
FUNDING_MIN_DOMAINS_DEFAULT = 2
FUNDING_LOW_CONF_THRESHOLD_DEFAULT = 0.55
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
    "board",
    "boardseat",
    "aifirst",
}
ACQ_INVALID_TARGET_TERMS = {"startup team", "domain-adjacent", "internal", "in-house", "ai-first", "ai first"}
SOURCE_POLICY_DEFAULT = "tiered_trusted_first"
LOW_SIGNAL_MODE_DEFAULT = "candidate_with_confidence"
TARGET_CONFIDENCE_LEVELS = {"High", "Medium", "Low"}
REQUIRED_NEW_TARGET_CONFIDENCE = "High"
WRITING_MODE_DEFAULT = "llm_passthrough"
QUALITY_GATE_ENABLED_DEFAULT = True
REWRITE_MAX_RETRIES_DEFAULT = 4
SOURCE_GATE_MODE_DEFAULT = "soft_block"
QUALITY_FAIL_POLICY_DEFAULT = "skip"
DELIVERY_MODE_DEFAULT = "diagnostic_fallback"
FACT_CARD_MODE_DEFAULT = "always"
QUOTE_OVERLAP_MAX_DEFAULT = 0.22
DIAGNOSTIC_MAX_REASONS_DEFAULT = 4
DIAGNOSTIC_INCLUDE_URLS_DEFAULT = True
WHY_NOW_RECENCY_DAYS_DEFAULT = 45
CRITIC_MIN_FIELD_SCORE_DEFAULT = 0.70
CRITIC_MIN_OVERALL_SCORE_DEFAULT = 0.78
EVIDENCE_FETCH_ENABLED_DEFAULT = True
EVIDENCE_FETCH_TIMEOUT_MS_DEFAULT = 2500
EVIDENCE_MAX_URLS_DEFAULT = 12
CONFIDENCE_MODEL_DEFAULT = "broad_weighted_v1"
CONFIDENCE_HIGH_MIN_DEFAULT = 2.40
CONFIDENCE_MEDIUM_MIN_DEFAULT = 1.35
DEFAULT_THEME_LOOKBACK_DAYS = 30
GOOGLE_SERP_ENDPOINT_DEFAULT = "https://serpapi.com/search.json"
GENERIC_LINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bno high[- ]signal updates surfaced\b", re.IGNORECASE),
    re.compile(r"\btie this to .* current products\b", re.IGNORECASE),
    re.compile(r"\bdifferentiate on speed to deployment\b", re.IGNORECASE),
    re.compile(r"\bprioritize one high-conviction move\b", re.IGNORECASE),
    re.compile(r"\bkey risks are execution bandwidth\b", re.IGNORECASE),
)
GENERIC_SOURCE_WRAPPER_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(stock price|quote(?:\s*&\s*|\s+and\s+)history|news,\s*quote)\b", re.IGNORECASE),
    re.compile(r"\b(latest stock news(?:\s*&\s*headlines)?)\b", re.IGNORECASE),
    re.compile(r"\b(roundup|top\s+\d+|best\s+\d+|watchlist)\b", re.IGNORECASE),
)
MONTHLY_TREND_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(last|past|over)\s+(month|30 days|4 weeks)\b", re.IGNORECASE),
    re.compile(r"\bmonth\b", re.IGNORECASE),
    re.compile(r"\bquarter\b", re.IGNORECASE),
    re.compile(r"\btrend\b", re.IGNORECASE),
    re.compile(r"\baccelerat|decelerat|inflect|shift\b", re.IGNORECASE),
)
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
TARGET_ROTATION_BY_COMPANY: dict[str, tuple[str, ...]] = {
    "anduril": ("Saronic", "Shield AI", "Skydio", "AeroVironment", "Epirus"),
    "anthropic": ("Langfuse", "Weights & Biases", "Scale AI"),
    "cursor": ("Sourcegraph", "Codeium", "PostHog"),
    "neuralink": ("Blackrock Neurotech", "Paradromics", "Synchron"),
    "openai": ("Browserbase", "Langfuse", "Unstructured", "Vercel"),
    "physicalintelligence": ("Covariant", "Skild AI", "Viam"),
    "ramp": ("Brex", "Mercury", "Modern Treasury"),
    "spacex": ("K2 Space", "Impulse Space", "Terran Orbital"),
    "stripe": ("Modern Treasury", "Adyen", "Plaid"),
    "sundayrobotics": ("Viam", "Realtime Robotics", "Skild AI"),
}
TARGET_COMPANY_ALIAS_DEFAULTS: dict[str, str] = {
    "next.js": "Vercel",
    "nextjs": "Vercel",
}
NON_COMPANY_TARGET_SUFFIX_TOKENS = {
    "sdk",
    "api",
    "framework",
    "library",
    "cli",
    "plugin",
    "toolkit",
    "module",
    "runtime",
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
SOURCE_TIER1_TRUSTED_DOMAIN_SUFFIXES = {
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "axios.com",
    "theinformation.com",
    "techcrunch.com",
    "finance.yahoo.com",
    "openai.com",
    "anthropic.com",
    "sec.gov",
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
SOURCE_TIER3_DOMAIN_SUFFIXES = {
    "x.com",
    "twitter.com",
    "reddit.com",
    "wikipedia.org",
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
    "over",
    "past",
    "last",
    "month",
    "quarter",
    "trend",
    "market",
    "this",
    "that",
    "these",
    "those",
    "there",
    "here",
    "d2c",
    "b2b",
    "b2c",
    "plg",
    "director",
    "the",
    "a",
    "an",
    "roi",
}
CONCEPTUAL_TARGET_TERMS = {
    "ai",
    "llm",
    "llms",
    "model",
    "models",
    "roi",
    "automation",
    "infrastructure",
    "platform",
    "platforms",
    "agent",
    "agents",
    "workflow",
    "workflows",
    "security",
    "governance",
}
LEGACY_BOARD_SEAT_PATTERNS = (
    "1. idea title",
    "2. why now",
    "3. target(s) / sector",
    "4. strategic fit for",
    "5. value creation",
)
FUNDING_SIGNAL_TERMS = {
    "funding",
    "raised",
    "raises",
    "raising",
    "round",
    "series",
    "backed",
    "backers",
    "investor",
    "investors",
    "valuation",
    "seed",
    "pre-seed",
    "growth",
}
FUNDING_ROUND_RE = re.compile(r"\b(series\s+[a-z][\+\-]?|seed|pre-seed|growth|debt|ipo)\b", re.IGNORECASE)
FUNDING_AMOUNT_RE = re.compile(r"\$?\d+(?:\.\d+)?\s?(?:m|b|million|billion)\b", re.IGNORECASE)
FUNDING_YEAR_RE = re.compile(r"\b(20\d{2})\b")
TARGET_EVENT_MAX_TARGETS_PER_COMPANY_DEFAULT = 4
TARGET_EVENT_MAX_ROWS_PER_TARGET_DEFAULT = 8
WHY_NOW_CATALYST_TERMS = {
    "launch",
    "launched",
    "partnership",
    "partnered",
    "pricing",
    "price",
    "adoption",
    "demand",
    "hiring",
    "revenue",
    "guidance",
    "policy",
    "contract",
    "expansion",
    "rollout",
    "integration",
}
DIFFERENTIATION_TERMS = {
    "differentiated",
    "differentiator",
    "faster",
    "faster than",
    "lower cost",
    "higher accuracy",
    "enterprise",
    "exclusive",
    "moat",
    "advantage",
}
RISK_SIGNAL_TERMS = {
    "risk",
    "integration",
    "execution",
    "regulatory",
    "security",
    "cost",
    "margin",
    "churn",
    "retention",
    "overlap",
    "concentration",
    "reliability",
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
    evidence_count: int = 0
    distinct_domains: int = 0
    conflict_flags: list[str] = field(default_factory=list)
    verification_status: str = "weak"


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
    tier: str = "tier_2"
    page_type: str = "news_report"
    published_hint: str = ""


@dataclass(frozen=True)
class SourceSelection:
    refs: list[SourceRef]
    confidence: str
    target_description: str = ""


@dataclass(frozen=True)
class EvidenceItem:
    id: str
    url: str
    publisher: str
    title: str
    snippet: str
    published_hint: str
    tier: str
    page_type: str
    quality_score: float


@dataclass(frozen=True)
class FactCard:
    field: str
    claim: str
    source_url: str
    source_title: str
    published_hint: str
    confidence: float
    tier: str
    page_type: str


@dataclass(frozen=True)
class BoardSeatDraft:
    idea_line: str
    target_does: str
    why_now: str
    whats_different: str
    mos_risks: str
    bottom_line: str
    context_current_efforts: str
    context_domain_fit_gaps: str
    funding_history: str
    funding_latest_round_backers: str
    funding_warning: str = ""
    repitch_note: str = ""
    repitch_new_evidence: str = ""
    source_refs: list[SourceRef] = field(default_factory=list)
    raw_model_output: str = ""
    rewrite_reasons: list[str] = field(default_factory=list)
    target_original: str = ""
    target_resolution_reason: str = "as_extracted"
    writing_artifact_cleanups: list[str] = field(default_factory=list)
    writing_field_dedup_fixes: list[str] = field(default_factory=list)
    quality_gate_passed: bool = True
    quality_score: float = 1.0
    quality_reasons: list[str] = field(default_factory=list)
    rewrite_attempts: int = 0
    quality_fail_stage: str = ""
    quality_field_scores: dict[str, float] = field(default_factory=dict)
    quality_failed_fields: list[str] = field(default_factory=list)
    quality_failure_codes: list[str] = field(default_factory=list)
    quality_required_evidence: dict[str, bool] = field(default_factory=dict)
    evidence_tier_mix: dict[str, int] = field(default_factory=dict)
    fact_cards_count_by_field: dict[str, int] = field(default_factory=dict)
    quote_overlap_by_field: dict[str, float] = field(default_factory=dict)
    why_now_recency_passed: bool = False


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


def _target_lock_days() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS",
        TARGET_LOCK_DAYS_DEFAULT,
        minimum=HARD_NO_REPITCH_DAYS,
        maximum=365,
    )


def _allow_repeat_targets() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_ALLOW_REPEAT_TARGETS", False)


def _target_event_max_targets_per_company() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_EVENT_TRACK_TARGETS_PER_COMPANY",
        TARGET_EVENT_MAX_TARGETS_PER_COMPANY_DEFAULT,
        minimum=1,
        maximum=12,
    )


def _target_event_max_rows_per_target() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_EVENT_TRACK_ROWS_PER_TARGET",
        TARGET_EVENT_MAX_ROWS_PER_TARGET_DEFAULT,
        minimum=3,
        maximum=20,
    )


def _ledger_dir() -> Path:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_LEDGER_DIR", "") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (_data_root() / "artifacts/board-seat").resolve()


def _ledger_mirror_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_ENABLED", True)


def _ledger_mirror_path() -> Path:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_PATH", "") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    primary = Path("/Users/spclaw/Documents/SPClaw Database/Companies/Board-Seat")
    fallback = Path("/Users/spclaw/Documents/Google Drive Local/Companies/Board-Seat")
    if primary.exists():
        return primary.resolve()
    if fallback.exists():
        return fallback.resolve()
    return primary.resolve()


def _funding_ttl_days() -> int:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_TTL_DAYS", str(FUNDING_CACHE_TTL_DAYS_DEFAULT)) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = FUNDING_CACHE_TTL_DAYS_DEFAULT
    return max(1, min(90, value))


def _funding_web_top_rows() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_FUNDING_WEB_TOP_ROWS",
        FUNDING_WEB_TOP_ROWS_DEFAULT,
        minimum=3,
        maximum=20,
    )


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


def _max_line_words() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS", MAX_LINE_WORDS_DEFAULT, minimum=0, maximum=120)


def _writing_mode() -> str:
    return (
        os.environ.get("COATUE_CLAW_BOARD_SEAT_WRITING_MODE", WRITING_MODE_DEFAULT) or WRITING_MODE_DEFAULT
    ).strip().lower()


def _quality_gate_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_QUALITY_GATE_ENABLED", QUALITY_GATE_ENABLED_DEFAULT)


def _rewrite_max_retries() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_REWRITE_MAX_RETRIES", REWRITE_MAX_RETRIES_DEFAULT, minimum=0, maximum=8)


def _source_gate_mode() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_SOURCE_GATE_MODE", SOURCE_GATE_MODE_DEFAULT) or SOURCE_GATE_MODE_DEFAULT).strip().lower()


def _quality_fail_policy() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_QUALITY_FAIL_POLICY", QUALITY_FAIL_POLICY_DEFAULT) or QUALITY_FAIL_POLICY_DEFAULT).strip().lower()


def _delivery_mode() -> str:
    return (
        os.environ.get("COATUE_CLAW_BOARD_SEAT_DELIVERY_MODE", DELIVERY_MODE_DEFAULT)
        or DELIVERY_MODE_DEFAULT
    ).strip().lower()


def _fact_card_mode() -> str:
    return (
        os.environ.get("COATUE_CLAW_BOARD_SEAT_FACT_CARD_MODE", FACT_CARD_MODE_DEFAULT)
        or FACT_CARD_MODE_DEFAULT
    ).strip().lower()


def _quote_overlap_max() -> float:
    raw = (
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_QUOTE_OVERLAP_MAX",
            str(QUOTE_OVERLAP_MAX_DEFAULT),
        )
        or str(QUOTE_OVERLAP_MAX_DEFAULT)
    ).strip()
    try:
        value = float(raw)
    except Exception:
        value = QUOTE_OVERLAP_MAX_DEFAULT
    return max(0.05, min(0.95, value))


def _diagnostic_max_reasons() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_DIAGNOSTIC_MAX_REASONS",
        DIAGNOSTIC_MAX_REASONS_DEFAULT,
        minimum=1,
        maximum=10,
    )


def _diagnostic_include_urls() -> bool:
    return _env_flag(
        "COATUE_CLAW_BOARD_SEAT_DIAGNOSTIC_INCLUDE_URLS",
        DIAGNOSTIC_INCLUDE_URLS_DEFAULT,
    )


def _why_now_recency_days() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_WHY_NOW_RECENCY_DAYS",
        WHY_NOW_RECENCY_DAYS_DEFAULT,
        minimum=7,
        maximum=365,
    )


def _critic_min_field_score() -> float:
    raw = (
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_CRITIC_MIN_FIELD_SCORE",
            str(CRITIC_MIN_FIELD_SCORE_DEFAULT),
        )
        or str(CRITIC_MIN_FIELD_SCORE_DEFAULT)
    ).strip()
    try:
        value = float(raw)
    except Exception:
        value = CRITIC_MIN_FIELD_SCORE_DEFAULT
    return max(0.0, min(1.0, value))


def _critic_min_overall_score() -> float:
    raw = (
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_CRITIC_MIN_OVERALL_SCORE",
            str(CRITIC_MIN_OVERALL_SCORE_DEFAULT),
        )
        or str(CRITIC_MIN_OVERALL_SCORE_DEFAULT)
    ).strip()
    try:
        value = float(raw)
    except Exception:
        value = CRITIC_MIN_OVERALL_SCORE_DEFAULT
    return max(0.0, min(1.0, value))


def _evidence_fetch_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_EVIDENCE_FETCH_ENABLED", EVIDENCE_FETCH_ENABLED_DEFAULT)


def _evidence_fetch_timeout_ms() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_EVIDENCE_FETCH_TIMEOUT_MS",
        EVIDENCE_FETCH_TIMEOUT_MS_DEFAULT,
        minimum=200,
        maximum=10000,
    )


def _evidence_max_urls() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_EVIDENCE_MAX_URLS",
        EVIDENCE_MAX_URLS_DEFAULT,
        minimum=4,
        maximum=40,
    )


def _review_model() -> str:
    return (
        os.environ.get("COATUE_CLAW_BOARD_SEAT_REVIEW_MODEL", (os.environ.get("COATUE_CLAW_BOARD_SEAT_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest"))
        or "gpt-5.2-chat-latest"
    ).strip()


def _strip_obvious_artifacts() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_STRIP_OBVIOUS_ARTIFACTS", True)


def _include_funding_links() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_INCLUDE_FUNDING_LINKS", False)


def _target_min_quality_sources() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_TARGET_MIN_QUALITY_SOURCES", 1, minimum=1, maximum=4)


def _target_min_total_sources() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_TARGET_MIN_TOTAL_SOURCES", 2, minimum=1, maximum=4)


def _low_signal_mode() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_LOW_SIGNAL_MODE", LOW_SIGNAL_MODE_DEFAULT) or LOW_SIGNAL_MODE_DEFAULT).strip()


def _target_confidence_model() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MODEL", CONFIDENCE_MODEL_DEFAULT) or CONFIDENCE_MODEL_DEFAULT).strip().lower()


def _target_confidence_high_min() -> float:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_CONFIDENCE_HIGH_MIN", str(CONFIDENCE_HIGH_MIN_DEFAULT)) or str(CONFIDENCE_HIGH_MIN_DEFAULT)).strip()
    try:
        value = float(raw)
    except Exception:
        value = CONFIDENCE_HIGH_MIN_DEFAULT
    return max(0.1, min(10.0, value))


def _target_confidence_medium_min() -> float:
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MEDIUM_MIN", str(CONFIDENCE_MEDIUM_MIN_DEFAULT)) or str(CONFIDENCE_MEDIUM_MIN_DEFAULT)).strip()
    try:
        value = float(raw)
    except Exception:
        value = CONFIDENCE_MEDIUM_MIN_DEFAULT
    return max(0.0, min(_target_confidence_high_min(), value))


def _allow_medium_new_target() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_ALLOW_MEDIUM_NEW_TARGET", True)


def _allowed_new_target_confidences() -> set[str]:
    return {"High", "Medium"} if _allow_medium_new_target() else {"High"}


def _require_company_target() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_REQUIRE_COMPANY_TARGET", True)


def _target_company_aliases() -> dict[str, str]:
    merged: dict[str, str] = {}
    for raw_key, raw_value in TARGET_COMPANY_ALIAS_DEFAULTS.items():
        key = _target_key(raw_key)
        value = _normalize_source_text(raw_value, max_chars=100)
        if key and value:
            merged[key] = value
    raw = (os.environ.get("COATUE_CLAW_BOARD_SEAT_TARGET_COMPANY_ALIAS_JSON", "") or "").strip()
    if not raw:
        return merged
    try:
        payload = json.loads(raw)
    except Exception:
        return merged
    if not isinstance(payload, dict):
        return merged
    for raw_key, raw_value in payload.items():
        key = _target_key(str(raw_key or ""))
        value = _normalize_source_text(str(raw_value or ""), max_chars=100)
        if not key or not value:
            continue
        merged[key] = value
    return merged


def _theme_lookback_days() -> int:
    return _env_int("COATUE_CLAW_BOARD_SEAT_THEME_LOOKBACK_DAYS", DEFAULT_THEME_LOOKBACK_DAYS, minimum=7, maximum=120)


def _header_style() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_HEADER_STYLE", "richtext") or "richtext").strip().lower()


def _specificity_mode() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_SPECIFICITY_MODE", "moderate") or "moderate").strip().lower()


def _funding_scope() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_FUNDING_SCOPE", "target") or "target").strip().lower()


def _funding_min_domains() -> int:
    return _env_int(
        "COATUE_CLAW_BOARD_SEAT_FUNDING_MIN_DOMAINS",
        FUNDING_MIN_DOMAINS_DEFAULT,
        minimum=1,
        maximum=5,
    )


def _funding_low_conf_threshold() -> float:
    raw = (
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_FUNDING_LOW_CONF_THRESHOLD",
            str(FUNDING_LOW_CONF_THRESHOLD_DEFAULT),
        )
        or str(FUNDING_LOW_CONF_THRESHOLD_DEFAULT)
    ).strip()
    try:
        value = float(raw)
    except Exception:
        value = FUNDING_LOW_CONF_THRESHOLD_DEFAULT
    return max(0.1, min(0.95, value))


def _funding_warning_mode() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_FUNDING_WARNING_MODE", True)


def _require_high_conf_new_target() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", True)


def _crunchbase_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_CRUNCHBASE_ENABLED", True)


def _crunchbase_api_key() -> str:
    for key in ("COATUE_CLAW_CRUNCHBASE_API_KEY", "CRUNCHBASE_API_KEY"):
        value = (os.environ.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _board_seat_google_serp_key() -> str:
    for key in ("COATUE_CLAW_BOARD_SEAT_GOOGLE_SERP_API_KEY", "SERPAPI_API_KEY"):
        value = (os.environ.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _board_seat_google_serp_endpoint() -> str:
    return (os.environ.get("COATUE_CLAW_BOARD_SEAT_GOOGLE_SERP_ENDPOINT", GOOGLE_SERP_ENDPOINT_DEFAULT) or GOOGLE_SERP_ENDPOINT_DEFAULT).strip()


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


def _target_key(text: str) -> str:
    return _slug_company(str(text or ""))


def _canonical_target_key(text: str) -> str:
    key = _target_key(text)
    if len(key) > 4 and key.endswith("s"):
        return key[:-1]
    return key


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
            evidence_count=int(row.get("evidence_count") or max(1, len(source_urls[:8]))),
            distinct_domains=int(row.get("distinct_domains") or max(1, len({_domain_from_url(item) for item in source_urls if _domain_from_url(item)}))),
            conflict_flags=[str(item).strip() for item in row.get("conflict_flags", []) if str(item).strip()]
            if isinstance(row.get("conflict_flags"), list)
            else [],
            verification_status=str(row.get("verification_status") or "verified").strip().lower() or "verified",
        )
    return out


def _normalize_line_text(text: str) -> str:
    cleaned = _normalize_text(str(text or ""), max_chars=420)
    cleaned = cleaned.strip().lstrip("-").lstrip("•").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" ;,")
    return cleaned


_DANGLING_TAIL_TOKENS = {
    "and",
    "or",
    "but",
    "with",
    "without",
    "to",
    "for",
    "of",
    "in",
    "on",
    "at",
    "by",
    "from",
    "into",
    "onto",
    "over",
    "under",
    "than",
    "that",
    "which",
    "who",
    "whose",
    "when",
    "while",
    "where",
    "because",
    "since",
    "if",
    "then",
    "as",
}


_WRITING_ARTIFACT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("html_tag", re.compile(r"<[^>]+>")),
    ("menu_boilerplate", re.compile(r"\b(get the full list|read more|learn more|click here)\b", re.IGNORECASE)),
    ("cta_boilerplate", re.compile(r"\b(book a demo|see pricing|sign in|sign up|request demo|start for free|try for free)\b", re.IGNORECASE)),
    ("ellipsis", re.compile(r"\.{3,}|…")),
    ("trail_boilerplate", re.compile(r"\b(?:view more|show more)\b", re.IGNORECASE)),
)

LOW_SIGNAL_COPY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(book a demo|see pricing|sign in|sign up|request demo|start for free|try for free)\b", re.IGNORECASE),
    re.compile(r"\b(menu|pricing page|product tour|docs navigation)\b", re.IGNORECASE),
    re.compile(r"(?:\b(book a demo|see pricing)\b[\s•|·]*){2,}", re.IGNORECASE),
)


def _trim_line_tail(text: str) -> str:
    cleaned = str(text or "").strip(" ,;:-")
    if not cleaned:
        return ""
    words = cleaned.split()
    while words:
        token = re.sub(r"[^a-z0-9]+", "", words[-1].lower())
        if token and token in _DANGLING_TAIL_TOKENS:
            words.pop()
            continue
        break
    return " ".join(words).strip(" ,;:-")


def _trim_incomplete_sentence_tail(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    if re.search(r"[.!?]\s*$", cleaned):
        return cleaned
    matches = list(re.finditer(r"[.!?](?=\s+\S)", cleaned))
    if not matches:
        return cleaned
    last = matches[-1]
    tail = cleaned[last.end() :].strip()
    if 0 < len(tail.split()) <= 4:
        return cleaned[: last.end()].strip()
    return cleaned


def _limit_words(text: str, *, max_words: int = MAX_LINE_WORDS) -> str:
    if max_words <= 0:
        return _trim_line_tail(str(text or "").strip())
    words = str(text or "").split()
    if len(words) <= max_words:
        candidate = " ".join(words)
    else:
        candidate = " ".join(words[:max_words]).strip()
    sentence_ends = [m.end() for m in re.finditer(r"[.!?](?=\s|$)", candidate)]
    if sentence_ends:
        cutoff = sentence_ends[-1]
        sentence_only = candidate[:cutoff].strip()
        if len(sentence_only.split()) >= max(6, max_words // 3):
            candidate = sentence_only
    candidate = _trim_incomplete_sentence_tail(candidate)
    return _trim_line_tail(candidate)


def _normalize_line(text: str, *, max_words: int | None = None) -> str:
    resolved_max_words = _max_line_words() if max_words is None else max_words
    return _limit_words(_normalize_line_text(text), max_words=resolved_max_words)


def _strip_obvious_writing_artifacts(text: str) -> tuple[str, list[str]]:
    value = str(text or "")
    cleanups: list[str] = []
    if not value.strip():
        return "", cleanups
    decoded = html.unescape(value)
    if decoded != value:
        cleanups.append("html_unescape")
    value = decoded
    for label, pattern in _WRITING_ARTIFACT_PATTERNS:
        updated = pattern.sub(" ", value)
        if updated != value:
            cleanups.append(label)
        value = updated
    value = re.sub(r"\s+", " ", value).strip(" ,;:-")
    return value, cleanups


def _is_low_signal_copy(text: str) -> bool:
    normalized = _normalize_line_text(text)
    if not normalized:
        return True
    return any(pattern.search(normalized) for pattern in LOW_SIGNAL_COPY_PATTERNS)


def _token_jaccard_similarity(text_a: str, text_b: str) -> float:
    tokens_a = set(_tokenize(_normalize_text(text_a, max_chars=2000)))
    tokens_b = set(_tokenize(_normalize_text(text_b, max_chars=2000)))
    if not tokens_a or not tokens_b:
        return 0.0
    union = tokens_a | tokens_b
    if not union:
        return 0.0
    return len(tokens_a & tokens_b) / float(len(union))


def _line_information_density_score(text: str) -> float:
    tokens = _tokenize(_normalize_text(text, max_chars=1000))
    if not tokens:
        return 0.0
    unique = len(set(tokens))
    return unique / float(len(tokens))


def _quote_overlap_score(field_text: str, source_text: str) -> float:
    field_tokens = set(_tokenize(_normalize_text(field_text, max_chars=2000)))
    source_tokens = set(_tokenize(_normalize_text(source_text, max_chars=2000)))
    if not field_tokens or not source_tokens:
        return 0.0
    return len(field_tokens & source_tokens) / float(len(field_tokens))


def _quote_overlap_by_field(*, draft: BoardSeatDraft, evidence_pack: dict[str, Any] | None) -> dict[str, float]:
    source_texts: list[str] = []
    seen: set[str] = set()
    if isinstance(evidence_pack, dict):
        for bucket in (
            "all_evidence",
            "target_does_evidence",
            "why_now_evidence",
            "whats_different_evidence",
            "mos_risks_evidence",
        ):
            items = evidence_pack.get(bucket)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                parts = [
                    str(item.get("title") or ""),
                    str(item.get("snippet") or ""),
                ]
                blob = _normalize_line_text(" ".join(parts))
                key = blob.lower()
                if (not blob) or (key in seen):
                    continue
                seen.add(key)
                source_texts.append(blob)
        fact_cards = evidence_pack.get("fact_cards")
        if isinstance(fact_cards, dict):
            for cards in fact_cards.values():
                if not isinstance(cards, list):
                    continue
                for card in cards:
                    if not isinstance(card, dict):
                        continue
                    blob = _normalize_line_text(
                        f"{str(card.get('claim') or '')} {str(card.get('source_title') or '')}"
                    )
                    key = blob.lower()
                    if (not blob) or (key in seen):
                        continue
                    seen.add(key)
                    source_texts.append(blob)
    fields = {
        "target_does": draft.target_does,
        "why_now": draft.why_now,
        "whats_different": draft.whats_different,
        "mos_risks": draft.mos_risks,
    }
    out: dict[str, float] = {}
    for field_name, field_text in fields.items():
        if not source_texts:
            out[field_name] = 0.0
            continue
        max_score = 0.0
        for source_text in source_texts:
            max_score = max(max_score, _quote_overlap_score(field_text, source_text))
        out[field_name] = round(max_score, 4)
    return out


def _quality_failure_codes_from_reasons(reasons: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in reasons:
        text = str(raw or "").strip()
        if not text:
            continue
        text = text.replace("draft_validator:", "")
        text = text.replace("critic:", "")
        code = text.split(":", 1)[0].strip().lower()
        if not code:
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _quality_fields_payload(draft: BoardSeatDraft) -> dict[str, Any]:
    failure_codes = [str(item) for item in list(draft.quality_failure_codes or []) if str(item).strip()]
    if not failure_codes:
        failure_codes = _quality_failure_codes_from_reasons([str(item) for item in list(draft.quality_reasons or [])])
    fact_cards_count_by_field = {str(key): int(value) for key, value in dict(draft.fact_cards_count_by_field or {}).items()}
    quote_overlap_by_field = {str(key): round(float(value), 4) for key, value in dict(draft.quote_overlap_by_field or {}).items()}
    for field_name in _fact_card_fields():
        fact_cards_count_by_field.setdefault(field_name, 0)
        quote_overlap_by_field.setdefault(field_name, 0.0)
    return {
        "quality_gate_passed": bool(draft.quality_gate_passed),
        "quality_score": round(float(draft.quality_score), 4),
        "quality_reasons": [str(item) for item in list(draft.quality_reasons or [])],
        "rewrite_attempts": int(draft.rewrite_attempts),
        "quality_fail_stage": str(draft.quality_fail_stage or ""),
        "quality_field_scores": {str(key): round(float(value), 4) for key, value in dict(draft.quality_field_scores or {}).items()},
        "quality_failed_fields": [str(item) for item in list(draft.quality_failed_fields or [])],
        "quality_failure_codes": failure_codes,
        "quality_required_evidence": {str(key): bool(value) for key, value in dict(draft.quality_required_evidence or {}).items()},
        "evidence_tier_mix": {str(key): int(value) for key, value in dict(draft.evidence_tier_mix or {}).items()},
        "fact_cards_count_by_field": fact_cards_count_by_field,
        "quote_overlap_by_field": quote_overlap_by_field,
        "why_now_recency_passed": bool(draft.why_now_recency_passed),
    }


def _quality_fields_default(*, passed: bool = False, stage: str = "not_run") -> dict[str, Any]:
    return {
        "delivery_mode_applied": "normal" if passed else "skip",
        "quality_blocked": not bool(passed),
        "quality_gate_passed": bool(passed),
        "quality_score": 0.0,
        "quality_reasons": [],
        "rewrite_attempts": 0,
        "quality_fail_stage": stage,
        "quality_field_scores": {},
        "quality_failed_fields": [],
        "quality_failure_codes": [],
        "quality_required_evidence": {},
        "evidence_tier_mix": {"tier_1": 0, "tier_2": 0, "tier_3": 0},
        "fact_cards_count_by_field": {},
        "quote_overlap_by_field": {},
        "why_now_recency_passed": False,
    }


def _quality_metrics_path() -> Path:
    return (_ledger_dir() / "quality-last-run.json").resolve()


def _quality_metrics_history_path() -> Path:
    return (_ledger_dir() / "quality-history.json").resolve()


def _persist_quality_run_metrics(result: dict[str, Any]) -> None:
    sent = list(result.get("sent") or [])
    skipped = list(result.get("skipped") or [])
    rows = [*sent, *skipped]
    rewrite_values = [
        int(row.get("rewrite_attempts") or 0)
        for row in rows
        if isinstance(row, dict)
    ]
    fails = [
        row
        for row in skipped
        if isinstance(row, dict) and str(row.get("reason") or "") == "quality_gate_failed"
    ]
    diagnostic_fallback_count = sum(
        1
        for row in sent
        if isinstance(row, dict) and str(row.get("delivery_mode_applied") or "") == "diagnostic_fallback"
    )
    quote_overlap_violations = 0
    fact_card_coverage_values: list[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        overlap = row.get("quote_overlap_by_field")
        if isinstance(overlap, dict):
            if any(float(value or 0.0) > _quote_overlap_max() for value in overlap.values()):
                quote_overlap_violations += 1
        cards = row.get("fact_cards_count_by_field")
        if isinstance(cards, dict) and cards:
            values = [int(value or 0) for value in cards.values()]
            if values:
                covered = sum(1 for value in values if value > 0)
                fact_card_coverage_values.append(covered / float(len(values)))
    reason_counts: dict[str, int] = {}
    failure_code_counts: dict[str, int] = {}
    for row in fails:
        for reason in list(row.get("quality_reasons") or []):
            key = str(reason).strip()
            if not key:
                continue
            reason_counts[key] = reason_counts.get(key, 0) + 1
        raw_codes = row.get("quality_failure_codes")
        codes = [str(item).strip() for item in raw_codes if str(item).strip()] if isinstance(raw_codes, list) else []
        if not codes:
            codes = _quality_failure_codes_from_reasons([str(item) for item in list(row.get("quality_reasons") or [])])
        for code in codes:
            failure_code_counts[code] = failure_code_counts.get(code, 0) + 1
    failed_field_counts: dict[str, int] = {}
    for row in fails:
        for field_name in list(row.get("quality_failed_fields") or []):
            key = str(field_name).strip()
            if not key:
                continue
            failed_field_counts[key] = failed_field_counts.get(key, 0) + 1
    top_reasons = sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    recorded_at = _utc_now_iso()

    history_path = _quality_metrics_history_path()
    history: list[dict[str, Any]] = []
    if history_path.exists():
        try:
            loaded = json.loads(history_path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                history = [item for item in loaded if isinstance(item, dict)]
        except Exception:
            history = []
    history.append(
        {
            "recorded_at_utc": recorded_at,
            "quality_pass_count": sum(1 for row in rows if isinstance(row, dict) and bool(row.get("quality_gate_passed"))),
            "quality_fail_count": len(fails),
            "rewrite_attempts": rewrite_values,
            "failed_fields": dict(failed_field_counts),
            "quality_failure_codes": dict(failure_code_counts),
            "diagnostic_fallback_count": diagnostic_fallback_count,
            "quote_overlap_violations": quote_overlap_violations,
            "fact_card_coverage_values": fact_card_coverage_values,
        }
    )
    cutoff = datetime.now(UTC) - timedelta(days=7)
    trimmed_history: list[dict[str, Any]] = []
    for item in history:
        raw_dt = str(item.get("recorded_at_utc") or "")
        try:
            dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            continue
        if dt >= cutoff:
            trimmed_history.append(item)

    total_pass = 0
    total_fail = 0
    rewrite_all: list[int] = []
    failed_fields_7d: dict[str, int] = {}
    failure_codes_7d: dict[str, int] = {}
    diagnostic_fallback_count_7d = 0
    quote_overlap_violations_7d = 0
    fact_card_coverage_all: list[float] = []
    for item in trimmed_history:
        total_pass += int(item.get("quality_pass_count") or 0)
        total_fail += int(item.get("quality_fail_count") or 0)
        for value in list(item.get("rewrite_attempts") or []):
            try:
                rewrite_all.append(int(value))
            except Exception:
                continue
        field_map = item.get("failed_fields")
        if isinstance(field_map, dict):
            for field_name, count in field_map.items():
                key = str(field_name).strip()
                if not key:
                    continue
                failed_fields_7d[key] = failed_fields_7d.get(key, 0) + int(count or 0)
        codes_map = item.get("quality_failure_codes")
        if isinstance(codes_map, dict):
            for code, count in codes_map.items():
                key = str(code).strip()
                if not key:
                    continue
                failure_codes_7d[key] = failure_codes_7d.get(key, 0) + int(count or 0)
        diagnostic_fallback_count_7d += int(item.get("diagnostic_fallback_count") or 0)
        quote_overlap_violations_7d += int(item.get("quote_overlap_violations") or 0)
        for value in list(item.get("fact_card_coverage_values") or []):
            try:
                fact_card_coverage_all.append(float(value))
            except Exception:
                continue
    total_checks = total_pass + total_fail
    quality_pass_rate_7d = round(total_pass / float(total_checks), 4) if total_checks else 0.0
    avg_rewrite_attempts_7d = round(sum(rewrite_all) / float(len(rewrite_all)), 3) if rewrite_all else 0.0
    top_failed_fields_7d = [
        {"field": key, "count": value}
        for key, value in sorted(failed_fields_7d.items(), key=lambda item: item[1], reverse=True)[:5]
    ]
    top_quality_failure_codes_7d = [
        {"code": key, "count": value}
        for key, value in sorted(failure_codes_7d.items(), key=lambda item: item[1], reverse=True)[:6]
    ]
    fact_card_coverage_7d = (
        round(sum(fact_card_coverage_all) / float(len(fact_card_coverage_all)), 4)
        if fact_card_coverage_all
        else 0.0
    )

    payload = {
        "run_date_local": str(result.get("run_date_local") or ""),
        "recorded_at_utc": recorded_at,
        "quality_fail_count": len(fails),
        "diagnostic_fallback_count_7d": diagnostic_fallback_count_7d,
        "top_quality_failure_codes_7d": top_quality_failure_codes_7d,
        "quote_overlap_violations_7d": quote_overlap_violations_7d,
        "fact_card_coverage_7d": fact_card_coverage_7d,
        "avg_rewrite_attempts": round(sum(rewrite_values) / float(len(rewrite_values)), 3) if rewrite_values else 0.0,
        "top_failure_reasons": [{"reason": key, "count": value} for key, value in top_reasons],
        "quality_pass_rate_7d": quality_pass_rate_7d,
        "top_failed_fields_7d": top_failed_fields_7d,
        "avg_rewrite_attempts_7d": avg_rewrite_attempts_7d,
    }
    path = _quality_metrics_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    history_path.write_text(json.dumps(trimmed_history, indent=2), encoding="utf-8")

def _normalize_line_list(items: list[str], *, max_items: int, max_words: int | None = None) -> list[str]:
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
    return _canonicalize_url(cleaned)[:320]


def _canonicalize_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        return ""
    host = str(parsed.netloc or "").strip().lower()
    if not host:
        return ""
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    ignored_params = {"ref", "source", "fbclid", "gclid", "mc_cid", "mc_eid"}
    kept_params = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_l = str(key or "").strip().lower()
        if (not key_l) or key_l.startswith("utm_") or key_l in ignored_params:
            continue
        kept_params.append((key_l, value))
    query = urlencode(sorted(kept_params))
    return urlunparse((parsed.scheme.lower(), host, path or "/", "", query, ""))


def _url_dedupe_key(url: str) -> str:
    canonical = _canonicalize_url(url)
    if not canonical:
        return ""
    parsed = urlparse(canonical)
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return urlunparse((parsed.scheme.lower(), host, parsed.path, "", parsed.query, ""))


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


def _is_search_results_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    host = _source_domain(url)
    path = str(parsed.path or "").strip().lower()
    query = parse_qs(str(parsed.query or ""))
    if host in {"google.com", "bing.com", "duckduckgo.com", "search.yahoo.com"}:
        return ("q" in query) and (path in {"", "/", "/search"})
    return False


def _domain_from_url(url: str) -> str:
    return _source_domain(url)


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


def _funding_row_signal_score(row: dict[str, str]) -> float:
    text = f"{row.get('title', '')} {row.get('snippet', '')}".lower()
    score = 0.0
    if any(term in text for term in FUNDING_SIGNAL_TERMS):
        score += 1.0
    if FUNDING_ROUND_RE.search(text):
        score += 1.3
    if FUNDING_AMOUNT_RE.search(text):
        score += 1.2
    if re.search(r"\b(led by|backed by|investors include|participated)\b", text):
        score += 0.8
    url = str(row.get("url") or "")
    if _is_quality_source(url):
        score += 0.6
    if _is_low_quality_source(url):
        score -= 0.5
    return score


def _extract_published_date_hint(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", value)
    if m:
        return m.group(1)
    m = re.search(r"\b(20\d{2}-\d{2})\b", value)
    if m:
        return m.group(1)
    m = re.search(
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(20\d{2})\b",
        value,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1)
    m = FUNDING_YEAR_RE.search(value)
    if m:
        return m.group(1)
    return ""


def _funding_evidence_conflicts(rows: list[dict[str, str]]) -> list[str]:
    rounds: set[str] = set()
    amounts: set[str] = set()
    years: set[str] = set()
    for row in rows:
        text = f"{row.get('title', '')} {row.get('snippet', '')}".lower()
        round_match = FUNDING_ROUND_RE.search(text)
        if round_match:
            rounds.add(round_match.group(1).strip().lower())
        amount_match = FUNDING_AMOUNT_RE.search(text)
        if amount_match:
            amounts.add(amount_match.group(0).strip().lower())
        for match in FUNDING_YEAR_RE.finditer(text):
            years.add(match.group(1))
    flags: list[str] = []
    if len(rounds) > 1:
        flags.append("major_round_mismatch")
    if len(amounts) > 1:
        flags.append("major_amount_mismatch")
    if len(years) > 2:
        flags.append("minor_date_variance")
    return flags


def _prepare_funding_evidence_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for row in rows:
        raw_url = str(row.get("url") or "")
        url = _normalize_source_url(raw_url)
        if not url:
            continue
        key = _url_dedupe_key(url) or url.lower()
        if key in seen_urls:
            continue
        seen_urls.add(key)
        title = _normalize_source_text(str(row.get("title") or ""), max_chars=180)
        snippet = _normalize_text(str(row.get("snippet") or ""), max_chars=420)
        publisher = _normalize_source_text(str(row.get("publisher") or ""), max_chars=64) or _publisher_from_url(url)
        published_hint = _extract_published_date_hint(
            " ".join(
                str(row.get(item) or "")
                for item in ("published", "date", "published_at", "age", "page_age", "snippet", "title")
            )
        )
        candidate = {
            "publisher": publisher,
            "title": title or _normalize_source_text(snippet, max_chars=180) or "Reference",
            "snippet": snippet,
            "url": url,
            "published_hint": published_hint,
        }
        signal_score = _funding_row_signal_score(candidate)
        if signal_score < 1.2:
            continue
        candidate["signal_score"] = f"{signal_score:.3f}"
        normalized.append(candidate)

    normalized.sort(
        key=lambda row: (
            float(row.get("signal_score") or "0"),
            row.get("published_hint") or "",
            row.get("title") or "",
        ),
        reverse=True,
    )
    return normalized[: _funding_web_top_rows()]


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


def _extract_published_hint(text: str) -> str:
    raw = _normalize_text(text, max_chars=300)
    if not raw:
        return ""
    iso_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", raw)
    if iso_match:
        return iso_match.group(1)
    us_match = re.search(
        r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
        r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+20\d{2}\b",
        raw,
        flags=re.IGNORECASE,
    )
    if us_match:
        return us_match.group(0)
    year_match = re.search(r"\b(20\d{2})\b", raw)
    if year_match:
        return year_match.group(1)
    return ""


def _parse_published_hint(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            return dt.replace(tzinfo=UTC)
        except Exception:
            continue
    return None


def _is_recent_published_hint(value: str, *, days: int) -> bool:
    dt = _parse_published_hint(value)
    if dt is None:
        return False
    return dt >= (datetime.now(UTC) - timedelta(days=max(1, days)))


def _detect_page_type(*, title: str, snippet: str, url: str) -> str:
    blob = _normalize_line_text(f"{title} {snippet} {url}").lower()
    domain = _source_domain(url)
    if _is_search_results_url(url) or _is_low_signal_copy(blob):
        return "wrapper"
    if any(domain.endswith(suffix) for suffix in SOURCE_TIER3_DOMAIN_SUFFIXES):
        return "social"
    if re.search(r"\b(newsroom|press release|announces?|announcement)\b", blob):
        return "press_release"
    if re.search(r"\b(docs?|documentation|developer|api reference|handbook)\b", blob):
        return "product_docs"
    if re.search(r"\b(pricing|book a demo|request demo|plans?|sign in|sign up)\b", blob):
        return "pricing_nav"
    if re.search(r"\b(news|reported|reports|coverage|interview)\b", blob):
        return "news_report"
    if re.search(r"\b(company profile|about us|founded|headquartered)\b", blob):
        return "company_profile"
    if domain in {"github.com", "docs.github.com", "linkedin.com"}:
        return "product_docs"
    return "news_report"


def _source_tier_from_row(*, company: str, target: str, row: dict[str, str], page_type: str) -> str:
    url = _normalize_source_url(str(row.get("url") or ""))
    domain = _source_domain(url)
    title = _normalize_line_text(str(row.get("title") or ""))
    snippet = _normalize_line_text(str(row.get("snippet") or ""))
    blob = f"{title} {snippet}".lower()
    company_key = _target_key(company).replace(" ", "")
    target_key = _target_key(target).replace(" ", "")
    is_target_official = bool(target_key and target_key in domain.replace(".", ""))
    is_company_official = bool(company_key and company_key in domain.replace(".", ""))
    if page_type in {"wrapper", "social", "pricing_nav"}:
        return "tier_3"
    if domain and any(domain.endswith(suffix) for suffix in SOURCE_TIER3_DOMAIN_SUFFIXES):
        return "tier_3"
    if _is_low_quality_source(url):
        return "tier_3"
    if is_target_official and page_type in {"press_release", "news_report", "product_docs", "company_profile"}:
        return "tier_1"
    if is_company_official and page_type in {"press_release", "news_report", "product_docs"}:
        return "tier_1"
    if domain and any(domain.endswith(suffix) for suffix in SOURCE_TIER1_TRUSTED_DOMAIN_SUFFIXES):
        return "tier_1"
    if _is_quality_source(url) and page_type in {"news_report", "press_release"}:
        return "tier_1"
    if page_type in {"product_docs", "company_profile"}:
        return "tier_2"
    if "linkedin.com/company" in blob:
        return "tier_2"
    return "tier_2"


def _evidence_quality_score(*, tier: str, page_type: str, row: dict[str, str]) -> float:
    score = 0.45
    score += {"tier_1": 0.35, "tier_2": 0.15, "tier_3": -0.25}.get(tier, 0.0)
    score += {"news_report": 0.15, "press_release": 0.12, "product_docs": 0.05, "company_profile": 0.02, "wrapper": -0.2, "social": -0.15, "pricing_nav": -0.18}.get(page_type, 0.0)
    snippet = _normalize_line_text(str(row.get("snippet") or ""))
    title = _normalize_line_text(str(row.get("title") or ""))
    if _is_generic_source_wrapper(f"{title} {snippet}"):
        score -= 0.2
    if _is_low_signal_copy(f"{title} {snippet}"):
        score -= 0.3
    if len(_tokenize(f"{title} {snippet}")) >= 8:
        score += 0.05
    return round(max(0.0, min(1.0, score)), 4)


def _fallback_published_hint_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or ""
    match = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})/", path)
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def _fetch_url_metadata(url: str) -> dict[str, str]:
    if (not _evidence_fetch_enabled()) or (not url):
        return {}
    timeout = max(0.2, float(_evidence_fetch_timeout_ms()) / 1000.0)
    req = Request(url, headers={"User-Agent": "CoatueClaw/1.0"})  # nosec B310
    try:
        with urlopen(req, timeout=timeout) as response:  # nosec B310
            body = response.read(20000).decode("utf-8", errors="ignore")
    except Exception:
        return {}
    title = ""
    title_match = re.search(r"<title[^>]*>(.*?)</title>", body, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        title = _normalize_source_text(re.sub(r"\s+", " ", title_match.group(1)), max_chars=180)
    published_hint = ""
    for pattern in (
        r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']pubdate["\'][^>]+content=["\']([^"\']+)["\']',
        r'<time[^>]+datetime=["\']([^"\']+)["\']',
    ):
        found = re.search(pattern, body, flags=re.IGNORECASE)
        if found:
            published_hint = _extract_published_hint(found.group(1))
            if published_hint:
                break
    return {
        "title": title,
        "published_hint": published_hint,
    }


def _row_to_evidence_item(
    *,
    company: str,
    target: str,
    row: dict[str, str],
    index: int,
    allow_fetch: bool = False,
) -> EvidenceItem | None:
    url = _normalize_source_url(str(row.get("url") or ""))
    if not url:
        return None
    title = _normalize_source_text(str(row.get("title") or ""), max_chars=180)
    snippet = _normalize_text(str(row.get("snippet") or ""), max_chars=420)
    publisher = _normalize_source_text(str(row.get("publisher") or ""), max_chars=64) or _publisher_from_url(url)
    published_hint = _extract_published_hint(
        " ".join(
            [
                str(row.get("published_hint") or ""),
                str(row.get("published") or ""),
                str(row.get("date") or ""),
                title,
                snippet,
                _fallback_published_hint_from_url(url),
            ]
        )
    )
    if allow_fetch and (not title or not published_hint) and _evidence_fetch_enabled():
        enriched = _fetch_url_metadata(url)
        if not title:
            title = _normalize_source_text(str(enriched.get("title") or ""), max_chars=180) or title
        if not published_hint:
            published_hint = _extract_published_hint(str(enriched.get("published_hint") or ""))
    page_type = _detect_page_type(title=title, snippet=snippet, url=url)
    tier = _source_tier_from_row(company=company, target=target, row=row, page_type=page_type)
    quality_score = _evidence_quality_score(tier=tier, page_type=page_type, row={"title": title, "snippet": snippet, "url": url})
    return EvidenceItem(
        id=f"E{index}",
        url=url,
        publisher=publisher,
        title=title or _normalize_source_text(snippet, max_chars=180) or "Reference",
        snippet=snippet,
        published_hint=published_hint,
        tier=tier,
        page_type=page_type,
        quality_score=quality_score,
    )


def _evidence_item_to_dict(item: EvidenceItem) -> dict[str, Any]:
    return {
        "id": item.id,
        "url": item.url,
        "publisher": item.publisher,
        "title": item.title,
        "snippet": item.snippet,
        "published_hint": item.published_hint,
        "tier": item.tier,
        "page_type": item.page_type,
        "quality_score": item.quality_score,
    }


def _evidence_matches_terms(item: EvidenceItem, terms: set[str]) -> bool:
    blob = _normalize_line_text(f"{item.title} {item.snippet}").lower()
    return any(term in blob for term in terms)


def _build_section_evidence(
    *,
    company: str,
    target: str,
    items: list[EvidenceItem],
) -> dict[str, Any]:
    policy = _source_policy().lower()
    non_tier3 = [item for item in items if item.tier != "tier_3" and item.page_type not in {"wrapper", "social"}]
    non_tier3 = non_tier3[: max(4, _evidence_max_urls())]
    tier_mix = {"tier_1": 0, "tier_2": 0, "tier_3": 0}
    for item in items:
        tier_mix[item.tier] = tier_mix.get(item.tier, 0) + 1

    target_does = [
        item
        for item in non_tier3
        if item.page_type in {"product_docs", "company_profile", "press_release", "news_report"}
    ]
    target_does = sorted(target_does, key=lambda item: item.quality_score, reverse=True)[:4]
    target_does_has_non_tier2 = any(item.tier != "tier_2" for item in target_does)
    target_does_required = bool(target_does) and (
        target_does_has_non_tier2 or not all(item.tier == "tier_2" for item in target_does)
    )

    why_now = [item for item in non_tier3 if item.tier == "tier_1" and item.page_type in {"news_report", "press_release", "company_profile"}]
    why_now = sorted(why_now, key=lambda item: item.quality_score, reverse=True)[:6]
    why_now_dated = [item for item in why_now if _parse_published_hint(item.published_hint) is not None]
    why_now_domains = {_source_domain(item.url) for item in why_now_dated if _source_domain(item.url)}
    why_now_recent = any(_is_recent_published_hint(item.published_hint, days=_why_now_recency_days()) for item in why_now_dated)
    why_now_required = len(why_now_dated) >= 2 and len(why_now_domains) >= 2 and why_now_recent

    whats_different = [item for item in non_tier3 if _evidence_matches_terms(item, DIFFERENTIATION_TERMS)]
    if not whats_different:
        whats_different = [item for item in non_tier3 if item.page_type in {"product_docs", "press_release"}]
    whats_different = sorted(whats_different, key=lambda item: item.quality_score, reverse=True)[:4]
    whats_different_required = bool(whats_different)

    mos_risks = [item for item in non_tier3 if _evidence_matches_terms(item, RISK_SIGNAL_TERMS)]
    if not mos_risks:
        mos_risks = [item for item in non_tier3 if item.page_type == "news_report"]
    mos_risks = sorted(mos_risks, key=lambda item: item.quality_score, reverse=True)[:4]
    mos_risks_required = bool(mos_risks)

    if policy == "tiered_trusted_first":
        target_does = sorted(target_does, key=lambda item: (0 if item.tier == "tier_1" else 1, -item.quality_score))[:4]
        whats_different = sorted(whats_different, key=lambda item: (0 if item.tier == "tier_1" else 1, -item.quality_score))[:4]
        mos_risks = sorted(mos_risks, key=lambda item: (0 if item.tier == "tier_1" else 1, -item.quality_score))[:4]

    return {
        "target_does_evidence": [_evidence_item_to_dict(item) for item in target_does],
        "why_now_evidence": [_evidence_item_to_dict(item) for item in why_now],
        "whats_different_evidence": [_evidence_item_to_dict(item) for item in whats_different],
        "mos_risks_evidence": [_evidence_item_to_dict(item) for item in mos_risks],
        "quality_required_evidence": {
            "target_does": target_does_required,
            "why_now": why_now_required,
            "whats_different": whats_different_required,
            "mos_risks": mos_risks_required,
        },
        "evidence_tier_mix": tier_mix,
        "why_now_recency_passed": bool(why_now_required),
    }


def _fact_card_fields() -> tuple[str, ...]:
    return ("target_does", "why_now", "whats_different", "mos_risks")


def _fact_card_claim_from_item(*, item: dict[str, Any]) -> str:
    raw = _normalize_text(
        str(item.get("snippet") or "") or str(item.get("title") or ""),
        max_chars=360,
    )
    if not raw:
        return ""
    stripped, _cleanups = _strip_obvious_writing_artifacts(raw)
    line = _normalize_line(_trim_incomplete_sentence_tail(stripped), max_words=40)
    if not line:
        return ""
    if _is_low_signal_copy(line) or _is_generic_source_wrapper(line):
        return ""
    if len(_tokenize(line)) < 6:
        return ""
    return line


def _fact_card_from_item(*, field: str, item: dict[str, Any]) -> FactCard | None:
    claim = _fact_card_claim_from_item(item=item)
    if not claim:
        return None
    url = _normalize_source_url(str(item.get("url") or ""))
    if not url:
        return None
    title = _normalize_source_text(str(item.get("title") or ""), max_chars=180) or "Reference"
    published_hint = _extract_published_hint(str(item.get("published_hint") or ""))
    tier = _normalize_source_text(str(item.get("tier") or ""), max_chars=16).lower() or "tier_2"
    page_type = _normalize_source_text(str(item.get("page_type") or ""), max_chars=32).lower() or "news_report"
    try:
        confidence = float(item.get("quality_score") or 0.5)
    except Exception:
        confidence = 0.5
    return FactCard(
        field=field,
        claim=claim,
        source_url=url,
        source_title=title,
        published_hint=published_hint,
        confidence=max(0.0, min(1.0, confidence)),
        tier=tier,
        page_type=page_type,
    )


def _fact_card_to_dict(card: FactCard) -> dict[str, Any]:
    return {
        "field": card.field,
        "claim": card.claim,
        "source_url": card.source_url,
        "source_title": card.source_title,
        "published_hint": card.published_hint,
        "confidence": round(float(card.confidence), 4),
        "tier": card.tier,
        "page_type": card.page_type,
    }


def _build_fact_cards(section_evidence: dict[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    if _fact_card_mode() == "off":
        empty = {field: [] for field in _fact_card_fields()}
        return empty, {field: 0 for field in _fact_card_fields()}
    bucket_by_field = {
        "target_does": "target_does_evidence",
        "why_now": "why_now_evidence",
        "whats_different": "whats_different_evidence",
        "mos_risks": "mos_risks_evidence",
    }
    out: dict[str, list[dict[str, Any]]] = {field: [] for field in _fact_card_fields()}
    counts: dict[str, int] = {field: 0 for field in _fact_card_fields()}
    for field_name, bucket in bucket_by_field.items():
        raw_items = list(section_evidence.get(bucket) or [])
        cards: list[FactCard] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            card = _fact_card_from_item(field=field_name, item=raw)
            if card is None:
                continue
            if any(_token_jaccard_similarity(card.claim, prev.claim) >= 0.86 for prev in cards):
                continue
            cards.append(card)
            if len(cards) >= 4:
                break
        out[field_name] = [_fact_card_to_dict(item) for item in cards]
        counts[field_name] = len(cards)
    return out, counts


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
    demoted_rows: list[dict[str, str]] = []
    seen: set[str] = set()
    gate_mode = _source_gate_mode()
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
            published_hint = _extract_published_hint(
                " ".join(
                    [
                        str(item.get("age") or ""),
                        str(item.get("published") or ""),
                        str(item.get("date") or ""),
                        title,
                        snippet,
                        _fallback_published_hint_from_url(url),
                    ]
                )
            )
            row = {
                "publisher": _publisher_from_url(url),
                "title": title or _normalize_source_text(snippet, max_chars=180) or "Reference",
                "snippet": snippet,
                "url": url,
                "published_hint": published_hint,
            }
            blob = f"{row['title']} {row['snippet']}"
            if gate_mode == "soft_block" and _is_low_signal_copy(blob):
                demoted_rows.append(row)
            else:
                rows.append(row)
            if len(rows) >= TARGET_SEARCH_RESULTS:
                return rows
    if gate_mode == "soft_block":
        for row in demoted_rows:
            if len(rows) >= TARGET_SEARCH_RESULTS:
                break
            rows.append(row)
    return rows


def _is_generic_source_wrapper(text: str) -> bool:
    normalized = _normalize_line_text(text)
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in GENERIC_SOURCE_WRAPPER_PATTERNS)


def _legacy_source_selection_confidence(candidates: list[SourceCandidate]) -> str:
    target_candidates = [item for item in candidates if item.category in {"target_direct", "target_proxy"}]
    quality_target = [item for item in target_candidates if item.quality > 0]
    quality_domains = {_source_domain(item.ref.url) for item in quality_target}
    direct_quality = [item for item in quality_target if item.category == "target_direct"]
    if len(target_candidates) >= 3 and len(quality_domains) >= 2:
        return "High"
    if len(direct_quality) >= _target_min_quality_sources() and len(target_candidates) >= _target_min_total_sources():
        return "Medium"
    return "Low"


def _target_confidence_score(candidates: list[SourceCandidate]) -> dict[str, Any]:
    considered = list(candidates[:4])
    if not considered:
        return {"confidence": "Low", "score": 0.0, "reasons": ["no_candidates"]}

    category_weights = {
        "target_direct": 1.0,
        "target_proxy": 0.7,
        "parent_context": 0.2,
        "funding_context": -0.4,
    }
    score = 0.0
    reasons: list[str] = []
    distinct_domains: set[str] = set()
    for idx, candidate in enumerate(considered, start=1):
        candidate_score = 0.0
        cat_weight = category_weights.get(candidate.category, 0.0)
        candidate_score += cat_weight
        quality_weight = 0.6 if candidate.quality > 0 else (-0.6 if candidate.quality < 0 else 0.0)
        candidate_score += quality_weight
        specificity_weight = 0.4 if candidate.category == "target_direct" else 0.0
        candidate_score += specificity_weight
        wrapper_weight = -0.3 if _is_generic_source_wrapper(candidate.text_blob) else 0.0
        candidate_score += wrapper_weight
        score += candidate_score
        domain = _source_domain(candidate.ref.url)
        if domain:
            distinct_domains.add(domain)
        reasons.append(
            "candidate_{idx}:{category}:cat={cat:+.2f},quality={quality:+.2f},specificity={specificity:+.2f},wrapper={wrapper:+.2f},total={total:+.2f}".format(
                idx=idx,
                category=candidate.category,
                cat=cat_weight,
                quality=quality_weight,
                specificity=specificity_weight,
                wrapper=wrapper_weight,
                total=candidate_score,
            )
        )

    diversity_bonus = min(0.6, max(0.0, 0.2 * float(max(0, len(distinct_domains) - 1))))
    score += diversity_bonus
    reasons.append(f"diversity_bonus={diversity_bonus:+.2f} domains={len(distinct_domains)}")
    high_min = _target_confidence_high_min()
    medium_min = _target_confidence_medium_min()
    if score >= high_min:
        confidence = "High"
    elif score >= medium_min:
        confidence = "Medium"
    else:
        confidence = "Low"
    reasons.append(f"thresholds high>={high_min:.2f} medium>={medium_min:.2f} score={score:.2f}")
    return {
        "confidence": confidence,
        "score": round(score, 4),
        "reasons": reasons,
    }


def _source_selection_confidence(candidates: list[SourceCandidate]) -> str:
    model = _target_confidence_model()
    if model != "broad_weighted_v1":
        return _legacy_source_selection_confidence(candidates)
    scored = _target_confidence_score(candidates)
    confidence = str(scored.get("confidence") or "Low")
    return confidence if confidence in TARGET_CONFIDENCE_LEVELS else "Low"


def _target_confidence_from_draft_sources(*, company: str, draft: BoardSeatDraft) -> dict[str, Any]:
    refs = _normalize_source_refs(draft.source_refs, max_items=4)
    if not refs:
        return {"confidence": "Low", "score": 0.0, "reasons": ["no_source_refs"], "model": _target_confidence_model()}
    target, target_tokens = _extract_target_tokens_from_idea(draft.idea_line)
    candidates: list[SourceCandidate] = []
    for ref in refs:
        blob = " ".join([ref.name_or_publisher, ref.title, ref.url])
        candidates.append(
            SourceCandidate(
                ref=ref,
                category=_classify_source_ref(
                    company=company,
                    target=target,
                    target_tokens=target_tokens,
                    text_blob=blob,
                ),
                quality=1 if _is_quality_source(ref.url) else (-1 if _is_low_quality_source(ref.url) else 0),
                score=0.0,
                text_blob=blob,
            )
        )
    model = _target_confidence_model()
    if model != "broad_weighted_v1":
        confidence = _legacy_source_selection_confidence(candidates)
        baseline_score = {"High": _target_confidence_high_min(), "Medium": _target_confidence_medium_min(), "Low": 0.0}
        return {
            "confidence": confidence,
            "score": round(float(baseline_score.get(confidence, 0.0)), 4),
            "reasons": ["legacy_model"],
            "model": model,
        }
    scored = _target_confidence_score(candidates)
    return {
        "confidence": scored.get("confidence", "Low"),
        "score": round(float(scored.get("score") or 0.0), 4),
        "reasons": list(scored.get("reasons") or []),
        "model": model,
    }


def _high_conf_new_target_gate(
    *,
    store: BoardSeatStore,
    company: str,
    draft: BoardSeatDraft,
) -> dict[str, Any]:
    target = _extract_acquisition_target(draft.idea_line)
    target_key = _target_key(target)
    target_original = _normalize_source_text(draft.target_original, max_chars=100) or target
    target_resolution_reason = _normalize_text(draft.target_resolution_reason, max_chars=64) or "as_extracted"
    writing_mode = _writing_mode()
    writing_artifact_cleanups = list(draft.writing_artifact_cleanups or [])
    writing_field_dedup_fixes = list(draft.writing_field_dedup_fixes or [])
    quality_gate_passed = bool(draft.quality_gate_passed)
    quality_score = round(float(draft.quality_score), 4)
    quality_reasons = [str(item) for item in list(draft.quality_reasons or [])]
    rewrite_attempts = int(draft.rewrite_attempts)
    quality_fail_stage = str(draft.quality_fail_stage or "")
    quality_field_scores = {str(key): round(float(value), 4) for key, value in dict(draft.quality_field_scores or {}).items()}
    quality_failed_fields = [str(item) for item in list(draft.quality_failed_fields or [])]
    quality_failure_codes = [str(item) for item in list(draft.quality_failure_codes or [])]
    quality_required_evidence = {str(key): bool(value) for key, value in dict(draft.quality_required_evidence or {}).items()}
    evidence_tier_mix = {str(key): int(value) for key, value in dict(draft.evidence_tier_mix or {}).items()}
    fact_cards_count_by_field = {str(key): int(value) for key, value in dict(draft.fact_cards_count_by_field or {}).items()}
    quote_overlap_by_field = {str(key): round(float(value), 4) for key, value in dict(draft.quote_overlap_by_field or {}).items()}
    for field_name in _fact_card_fields():
        fact_cards_count_by_field.setdefault(field_name, 0)
        quote_overlap_by_field.setdefault(field_name, 0.0)
    why_now_recency_passed = bool(draft.why_now_recency_passed)
    confidence_payload = _target_confidence_from_draft_sources(company=company, draft=draft)
    confidence = str(confidence_payload.get("confidence") or "Low")
    confidence_score = round(float(confidence_payload.get("score") or 0.0), 4)
    confidence_reasons = [str(item) for item in list(confidence_payload.get("reasons") or [])]
    target_validation_reason = _target_validation_reason(company=company, target=target)
    latest = store.latest_target_pitch(company=company, target_key=target_key) if target_key else None
    is_new_target = bool(target_key) and latest is None
    if not _require_high_conf_new_target():
        return {
            "allow": True,
            "reason": "gate_disabled",
            "target": target,
            "target_original": target_original,
            "target_resolution_reason": target_resolution_reason,
            "writing_mode": writing_mode,
            "writing_artifact_cleanups": writing_artifact_cleanups,
            "writing_field_dedup_fixes": writing_field_dedup_fixes,
            "quality_gate_passed": quality_gate_passed,
            "quality_score": quality_score,
            "quality_reasons": quality_reasons,
            "rewrite_attempts": rewrite_attempts,
            "quality_fail_stage": quality_fail_stage,
            "quality_field_scores": quality_field_scores,
            "quality_failed_fields": quality_failed_fields,
            "quality_failure_codes": quality_failure_codes,
            "quality_required_evidence": quality_required_evidence,
            "evidence_tier_mix": evidence_tier_mix,
            "fact_cards_count_by_field": fact_cards_count_by_field,
            "quote_overlap_by_field": quote_overlap_by_field,
            "why_now_recency_passed": why_now_recency_passed,
            "target_key": target_key,
            "target_confidence": confidence,
            "target_confidence_score": confidence_score,
            "target_confidence_reasons": confidence_reasons,
            "target_validation_reason": target_validation_reason,
            "is_new_target": is_new_target,
            "matched_posted_at_utc": str((latest or {}).get("posted_at_utc") or ""),
        }
    if (not target_key) or (target_validation_reason != "ok"):
        return {
            "allow": False,
            "reason": "invalid_target",
            "target": target,
            "target_original": target_original,
            "target_resolution_reason": target_resolution_reason,
            "writing_mode": writing_mode,
            "writing_artifact_cleanups": writing_artifact_cleanups,
            "writing_field_dedup_fixes": writing_field_dedup_fixes,
            "quality_gate_passed": quality_gate_passed,
            "quality_score": quality_score,
            "quality_reasons": quality_reasons,
            "rewrite_attempts": rewrite_attempts,
            "quality_fail_stage": quality_fail_stage,
            "quality_field_scores": quality_field_scores,
            "quality_failed_fields": quality_failed_fields,
            "quality_failure_codes": quality_failure_codes,
            "quality_required_evidence": quality_required_evidence,
            "evidence_tier_mix": evidence_tier_mix,
            "fact_cards_count_by_field": fact_cards_count_by_field,
            "quote_overlap_by_field": quote_overlap_by_field,
            "why_now_recency_passed": why_now_recency_passed,
            "target_key": target_key,
            "target_confidence": confidence,
            "target_confidence_score": confidence_score,
            "target_confidence_reasons": confidence_reasons,
            "target_validation_reason": target_validation_reason,
            "is_new_target": is_new_target,
            "matched_posted_at_utc": "",
        }
    allowed_confidence = _allowed_new_target_confidences()
    allow = is_new_target and confidence in allowed_confidence
    reason = ""
    if not is_new_target:
        reason = "target_not_new"
    elif confidence not in allowed_confidence:
        reason = "target_confidence_not_high"
    return {
        "allow": allow,
        "reason": reason if not allow else "ok",
        "target": target,
        "target_original": target_original,
        "target_resolution_reason": target_resolution_reason,
        "writing_mode": writing_mode,
        "writing_artifact_cleanups": writing_artifact_cleanups,
        "writing_field_dedup_fixes": writing_field_dedup_fixes,
        "quality_gate_passed": quality_gate_passed,
        "quality_score": quality_score,
        "quality_reasons": quality_reasons,
        "rewrite_attempts": rewrite_attempts,
        "quality_fail_stage": quality_fail_stage,
        "quality_field_scores": quality_field_scores,
        "quality_failed_fields": quality_failed_fields,
        "quality_failure_codes": quality_failure_codes,
        "quality_required_evidence": quality_required_evidence,
        "evidence_tier_mix": evidence_tier_mix,
        "fact_cards_count_by_field": fact_cards_count_by_field,
        "quote_overlap_by_field": quote_overlap_by_field,
        "why_now_recency_passed": why_now_recency_passed,
        "target_key": target_key,
        "target_confidence": confidence,
        "target_confidence_score": confidence_score,
        "target_confidence_reasons": confidence_reasons,
        "target_validation_reason": target_validation_reason,
        "is_new_target": is_new_target,
        "matched_posted_at_utc": str((latest or {}).get("posted_at_utc") or ""),
    }


def _is_generic_line(text: str) -> bool:
    normalized = _normalize_line_text(text)
    if not normalized:
        return True
    return any(pattern.search(normalized) for pattern in GENERIC_LINE_PATTERNS)


def _line_has_concrete_anchor(text: str) -> bool:
    line = _normalize_line_text(text)
    if not line:
        return False
    if re.search(r"\b\d+(?:\.\d+)?(?:%|x|m|b|k)?\b", line):
        return True
    if re.search(r"\b(202[0-9]|q[1-4]|month|quarter|week|year|days)\b", line, flags=re.IGNORECASE):
        return True
    if re.search(r"\b[A-Z][A-Za-z0-9&.'-]{2,}\b", line):
        return True
    return False


def _is_monthly_theme_line(text: str) -> bool:
    line = _normalize_line_text(text)
    if not line:
        return False
    if "24 hour" in line.lower() or "last 24 hours" in line.lower():
        return False
    return any(pattern.search(line) for pattern in MONTHLY_TREND_PATTERNS)


def _evidence_items_from_bucket(payload: dict[str, Any] | None, key: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items = payload.get(key)
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        url = _normalize_source_url(str(raw.get("url") or ""))
        if not url:
            continue
        out.append(
            {
                "url": url,
                "title": _normalize_source_text(str(raw.get("title") or ""), max_chars=180),
                "snippet": _normalize_text(str(raw.get("snippet") or ""), max_chars=420),
                "published_hint": _extract_published_hint(str(raw.get("published_hint") or "")),
                "tier": _normalize_source_text(str(raw.get("tier") or ""), max_chars=16).lower(),
                "page_type": _normalize_source_text(str(raw.get("page_type") or ""), max_chars=32).lower(),
            }
        )
    return out


def _why_now_semantic_recency_assessment(*, company: str, draft: BoardSeatDraft, evidence_pack: dict[str, Any] | None) -> dict[str, Any]:
    line = _normalize_line_text(draft.why_now)
    if not line:
        return {"passed": False, "recency_passed": False, "reasons": ["why_now_missing"]}
    lower = line.lower()
    if "24 hour" in lower or "last 24 hours" in lower:
        return {"passed": False, "recency_passed": False, "reasons": ["why_now_24h_disallowed"]}

    # Backward-compatible fallback for direct callers that do not pass evidence context.
    if not isinstance(evidence_pack, dict):
        heuristic = _is_monthly_theme_line(line)
        return {
            "passed": heuristic,
            "recency_passed": heuristic,
            "reasons": ([] if heuristic else ["why_now_not_monthly_theme"]),
        }

    items = _evidence_items_from_bucket(evidence_pack, "why_now_evidence")
    if not items:
        heuristic = _is_monthly_theme_line(line)
        return {
            "passed": heuristic,
            "recency_passed": heuristic,
            "reasons": ([] if heuristic else ["why_now_not_monthly_theme"]),
        }
    dated: list[dict[str, Any]] = []
    in_window = False
    domains: set[str] = set()
    for item in items:
        hint = str(item.get("published_hint") or "")
        dt = _parse_published_hint(hint)
        if dt is None:
            continue
        dated.append(item)
        domain = _source_domain(str(item.get("url") or ""))
        if domain:
            domains.add(domain)
        if _is_recent_published_hint(hint, days=_why_now_recency_days()):
            in_window = True
    reasons: list[str] = []
    if len(dated) < 2:
        reasons.append("why_now_insufficient_dated_sources")
    if len(domains) < 2:
        reasons.append("why_now_insufficient_source_diversity")
    if not in_window:
        reasons.append("why_now_no_in_window_source")
    if not any(term in lower for term in WHY_NOW_CATALYST_TERMS):
        reasons.append("why_now_missing_catalyst_semantics")

    evidence_token_set: set[str] = set()
    for item in items[:8]:
        evidence_token_set.update(_tokenize(f"{item.get('title', '')} {item.get('snippet', '')}"))
    line_tokens = set(_tokenize(line))
    overlap = line_tokens & evidence_token_set
    company_tokens = set(_tokenize(company))
    if len(overlap) < 1 and not (line_tokens & company_tokens):
        reasons.append("why_now_weak_evidence_alignment")

    passed = not reasons
    return {
        "passed": passed,
        "recency_passed": in_window and len(dated) >= 2 and len(domains) >= 2,
        "reasons": reasons,
    }


def _target_description_from_rows(*, target: str, rows: list[dict[str, str]]) -> str:
    for row in rows:
        snippet = _normalize_line_text(str(row.get("snippet") or ""))
        if not snippet:
            continue
        page_type = _detect_page_type(
            title=str(row.get("title") or ""),
            snippet=snippet,
            url=str(row.get("url") or ""),
        )
        if page_type in {"wrapper", "social", "pricing_nav"}:
            continue
        if _source_gate_mode() == "soft_block" and _is_low_signal_copy(f"{row.get('title', '')} {snippet}"):
            continue
        if len(snippet.split()) < 6:
            continue
        cleaned = re.split(r"[.;]\s*", snippet, maxsplit=1)[0].strip()
        if cleaned:
            return _normalize_line(cleaned)
    return _normalize_line(f"{target} builds enterprise software and infrastructure used in production workflows.")


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

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

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
                    significant_change INTEGER NOT NULL DEFAULT 0,
                    is_repitch INTEGER NOT NULL DEFAULT 0,
                    repitch_of_pitch_id INTEGER,
                    repitch_prev_posted_at_utc TEXT,
                    repitch_similarity REAL NOT NULL DEFAULT 0.0,
                    repitch_new_evidence_json TEXT NOT NULL DEFAULT '[]'
                );
                """
            )
            pitch_cols = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(board_seat_pitches)").fetchall()
            }
            if "is_repitch" not in pitch_cols:
                conn.execute("ALTER TABLE board_seat_pitches ADD COLUMN is_repitch INTEGER NOT NULL DEFAULT 0;")
            if "repitch_of_pitch_id" not in pitch_cols:
                conn.execute("ALTER TABLE board_seat_pitches ADD COLUMN repitch_of_pitch_id INTEGER;")
            if "repitch_prev_posted_at_utc" not in pitch_cols:
                conn.execute("ALTER TABLE board_seat_pitches ADD COLUMN repitch_prev_posted_at_utc TEXT;")
            if "repitch_similarity" not in pitch_cols:
                conn.execute("ALTER TABLE board_seat_pitches ADD COLUMN repitch_similarity REAL NOT NULL DEFAULT 0.0;")
            if "repitch_new_evidence_json" not in pitch_cols:
                conn.execute(
                    "ALTER TABLE board_seat_pitches ADD COLUMN repitch_new_evidence_json TEXT NOT NULL DEFAULT '[]';"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_pitches_company_recent ON board_seat_pitches(company, posted_at_utc DESC);"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_board_seat_pitches_message_ts ON board_seat_pitches(message_ts) WHERE message_ts IS NOT NULL;"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_pitches_repitch ON board_seat_pitches(company, is_repitch, posted_at_utc DESC);"
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
                    confidence REAL NOT NULL DEFAULT 0.0,
                    evidence_count INTEGER NOT NULL DEFAULT 0,
                    distinct_domains INTEGER NOT NULL DEFAULT 0,
                    conflict_flags_json TEXT NOT NULL DEFAULT '[]',
                    verification_status TEXT NOT NULL DEFAULT 'weak'
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_funding_cache_asof ON board_seat_funding_cache(as_of_utc DESC);"
            )
            existing_cols = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(board_seat_funding_cache)").fetchall()
            }
            if "evidence_count" not in existing_cols:
                conn.execute("ALTER TABLE board_seat_funding_cache ADD COLUMN evidence_count INTEGER NOT NULL DEFAULT 0;")
            if "distinct_domains" not in existing_cols:
                conn.execute("ALTER TABLE board_seat_funding_cache ADD COLUMN distinct_domains INTEGER NOT NULL DEFAULT 0;")
            if "conflict_flags_json" not in existing_cols:
                conn.execute(
                    "ALTER TABLE board_seat_funding_cache ADD COLUMN conflict_flags_json TEXT NOT NULL DEFAULT '[]';"
                )
            if "verification_status" not in existing_cols:
                conn.execute(
                    "ALTER TABLE board_seat_funding_cache ADD COLUMN verification_status TEXT NOT NULL DEFAULT 'weak';"
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_target_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    channel_ref TEXT NOT NULL,
                    channel_id TEXT,
                    source TEXT NOT NULL,
                    posted_at_utc TEXT NOT NULL,
                    run_date_local TEXT,
                    message_ts TEXT
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_target_memory_recent ON board_seat_target_memory(company, target_key, posted_at_utc DESC);"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_board_seat_target_memory_message_ts ON board_seat_target_memory(message_ts) WHERE message_ts IS NOT NULL;"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_target_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    event_at_utc TEXT NOT NULL,
                    publisher TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    snippet TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    evidence_quality REAL NOT NULL DEFAULT 0.0,
                    impact_score REAL NOT NULL DEFAULT 0.0,
                    source_type TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_board_seat_target_events_unique ON board_seat_target_events(target_key, url, event_at_utc);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_target_events_recent ON board_seat_target_events(target_key, event_at_utc DESC);"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_repitch_assessments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    prior_pitch_posted_at_utc TEXT NOT NULL,
                    window_start_utc TEXT NOT NULL,
                    window_end_utc TEXT NOT NULL,
                    top_events_json TEXT NOT NULL,
                    aggregate_score REAL NOT NULL DEFAULT 0.0,
                    max_event_score REAL NOT NULL DEFAULT 0.0,
                    distinct_domains INTEGER NOT NULL DEFAULT 0,
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    strictness_version TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_board_seat_repitch_assessments_recent ON board_seat_repitch_assessments(target_key, created_at_utc DESC);"
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
            target = _extract_target_from_message_text(company=str(row["company"] or ""), text=message_text)
            if target:
                self.record_target(
                    company=str(row["company"] or ""),
                    target=target,
                    channel_ref=str(row["channel_ref"] or ""),
                    channel_id=(str(row["channel_id"] or "").strip() or None),
                    source="legacy_run_seed",
                    posted_at_utc=str(row["posted_at_utc"] or _utc_now_iso()),
                    run_date_local=(str(row["run_date_local"] or "").strip() or None),
                    message_ts=(str(row["message_ts"] or "").strip() or None),
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
        is_repitch: bool = False,
        repitch_of_pitch_id: int | None = None,
        repitch_prev_posted_at_utc: str | None = None,
        repitch_similarity: float = 0.0,
        repitch_new_evidence: list[str] | None = None,
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
                    significant_change,
                    is_repitch,
                    repitch_of_pitch_id,
                    repitch_prev_posted_at_utc,
                    repitch_similarity,
                    repitch_new_evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    1 if is_repitch else 0,
                    repitch_of_pitch_id,
                    repitch_prev_posted_at_utc,
                    float(repitch_similarity),
                    json.dumps(repitch_new_evidence or [], ensure_ascii=False),
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
                    significant_change,
                    is_repitch,
                    repitch_of_pitch_id,
                    repitch_prev_posted_at_utc,
                    repitch_similarity,
                    repitch_new_evidence_json
                FROM board_seat_pitches
                WHERE company = ?
                ORDER BY posted_at_utc DESC
                LIMIT ?
                """,
                (company, max(1, min(500, int(limit)))),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_target_pitch(self, *, company: str, target_key: str) -> dict[str, Any] | None:
        if not company or not target_key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT company, target, target_key, channel_ref, channel_id, source, posted_at_utc, run_date_local, message_ts
                FROM board_seat_target_memory
                WHERE company = ? AND target_key = ?
                ORDER BY posted_at_utc DESC
                LIMIT 1
                """,
                (company, target_key),
            ).fetchone()
        return dict(row) if row is not None else None

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

    def record_target(
        self,
        *,
        company: str,
        target: str,
        channel_ref: str,
        channel_id: str | None,
        source: str,
        posted_at_utc: str,
        run_date_local: str | None,
        message_ts: str | None,
    ) -> bool:
        clean_target = _normalize_text(str(target or ""), max_chars=120).strip()
        if not clean_target:
            return False
        key = _target_key(clean_target)
        if not key:
            return False
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO board_seat_target_memory (
                    company, target, target_key, channel_ref, channel_id, source, posted_at_utc, run_date_local, message_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company,
                    clean_target,
                    key,
                    channel_ref,
                    channel_id,
                    source,
                    posted_at_utc,
                    run_date_local,
                    message_ts,
                ),
            )
        return bool(cur.rowcount)

    def recent_target_hit(self, *, company: str, target_key: str, lookback_days: int) -> dict[str, Any] | None:
        if not company or not target_key:
            return None
        cutoff = (datetime.now(UTC) - timedelta(days=max(1, int(lookback_days)))).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT company, target, target_key, channel_ref, channel_id, source, posted_at_utc, run_date_local, message_ts
                FROM board_seat_target_memory
                WHERE company = ? AND target_key = ? AND posted_at_utc >= ?
                ORDER BY posted_at_utc DESC
                LIMIT 1
                """,
                (company, target_key, cutoff),
            ).fetchone()
        return dict(row) if row is not None else None

    def target_memory_count(self, *, company: str | None = None) -> int:
        with self._connect() as conn:
            if company:
                row = conn.execute(
                    "SELECT COUNT(1) AS n FROM board_seat_target_memory WHERE company = ?",
                    (company,),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(1) AS n FROM board_seat_target_memory").fetchone()
        return int(row["n"]) if row is not None else 0

    def target_ledger_rows(self, *, company: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        cap = max(1, min(5000, int(limit)))
        where = ""
        params: tuple[Any, ...] = ()
        if company:
            where = "WHERE company = ?"
            params = (company,)
        query = f"""
            WITH ranked AS (
                SELECT
                    company,
                    target,
                    target_key,
                    channel_ref,
                    channel_id,
                    source,
                    message_ts,
                    posted_at_utc,
                    COUNT(1) OVER (PARTITION BY company, target_key) AS pitch_count,
                    MIN(posted_at_utc) OVER (PARTITION BY company, target_key) AS first_seen_utc,
                    MAX(posted_at_utc) OVER (PARTITION BY company, target_key) AS last_seen_utc,
                    ROW_NUMBER() OVER (PARTITION BY company, target_key ORDER BY posted_at_utc DESC, id DESC) AS rn
                FROM board_seat_target_memory
                {where}
            )
            SELECT
                company,
                target,
                target_key,
                first_seen_utc,
                last_seen_utc,
                pitch_count,
                channel_ref AS last_channel_ref,
                channel_id AS last_channel_id,
                source AS last_source,
                message_ts AS last_message_ts
            FROM ranked
            WHERE rn = 1
            ORDER BY company ASC, last_seen_utc DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(query, (*params, cap)).fetchall()
        return [dict(row) for row in rows]

    def record_target_event(
        self,
        *,
        company: str,
        target: str,
        event_at_utc: str,
        publisher: str,
        title: str,
        url: str,
        snippet: str,
        event_type: str,
        evidence_quality: float,
        impact_score: float,
        source_type: str,
    ) -> bool:
        target_clean = _normalize_source_text(target, max_chars=120)
        target_key = _target_key(target_clean)
        url_clean = _normalize_source_url(url)
        if (not target_key) or (not url_clean):
            return False
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO board_seat_target_events (
                    company, target, target_key, event_at_utc, publisher, title, url, snippet,
                    event_type, evidence_quality, impact_score, source_type, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company,
                    target_clean,
                    target_key,
                    event_at_utc,
                    _normalize_source_text(publisher, max_chars=80) or "Web",
                    _normalize_source_text(title, max_chars=240) or "Reference",
                    url_clean,
                    _normalize_text(snippet, max_chars=420),
                    _normalize_source_text(event_type, max_chars=48) or "other",
                    float(max(0.0, min(1.0, evidence_quality))),
                    float(max(0.0, min(1.0, impact_score))),
                    _normalize_source_text(source_type, max_chars=24) or "web",
                    _utc_now_iso(),
                ),
            )
        return bool(cur.rowcount)

    def recent_target_events(
        self,
        *,
        company: str,
        target_key: str,
        since_utc: str,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        cap = max(1, min(1000, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, company, target, target_key, event_at_utc, publisher, title, url, snippet,
                    event_type, evidence_quality, impact_score, source_type, created_at_utc
                FROM board_seat_target_events
                WHERE company = ? AND target_key = ? AND event_at_utc >= ?
                ORDER BY impact_score DESC, event_at_utc DESC
                LIMIT ?
                """,
                (company, target_key, since_utc, cap),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_repitch_assessment(
        self,
        *,
        company: str,
        target: str,
        target_key: str,
        prior_pitch_posted_at_utc: str,
        window_start_utc: str,
        window_end_utc: str,
        top_events: list[dict[str, Any]],
        aggregate_score: float,
        max_event_score: float,
        distinct_domains: int,
        decision: str,
        reason: str,
        strictness_version: str,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO board_seat_repitch_assessments (
                    company, target, target_key, prior_pitch_posted_at_utc, window_start_utc, window_end_utc,
                    top_events_json, aggregate_score, max_event_score, distinct_domains, decision, reason,
                    strictness_version, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company,
                    _normalize_source_text(target, max_chars=120),
                    target_key,
                    prior_pitch_posted_at_utc,
                    window_start_utc,
                    window_end_utc,
                    json.dumps(top_events, ensure_ascii=False),
                    float(aggregate_score),
                    float(max_event_score),
                    int(distinct_domains),
                    _normalize_source_text(decision, max_chars=16) or "reject",
                    _normalize_text(reason, max_chars=320),
                    _normalize_source_text(strictness_version, max_chars=48) or "strict_v1",
                    _utc_now_iso(),
                ),
            )
            rowid = int(cur.lastrowid)
        return rowid

    def get_funding_snapshot(self, *, company: str) -> FundingSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    company,
                    history,
                    latest_round,
                    latest_date,
                    backers_json,
                    source_urls_json,
                    source_type,
                    as_of_utc,
                    confidence,
                    evidence_count,
                    distinct_domains,
                    conflict_flags_json,
                    verification_status
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
        try:
            conflict_flags = json.loads(str(row["conflict_flags_json"] or "[]"))
            if not isinstance(conflict_flags, list):
                conflict_flags = []
        except Exception:
            conflict_flags = []
        return FundingSnapshot(
            history=str(row["history"] or ""),
            latest_round=str(row["latest_round"] or ""),
            latest_date=str(row["latest_date"] or ""),
            backers=[str(item).strip() for item in backers if str(item).strip()],
            source_urls=[str(item).strip() for item in source_urls if str(item).strip()],
            source_type=str(row["source_type"] or "unknown").strip() or "unknown",
            as_of_utc=str(row["as_of_utc"] or _utc_now_iso()),
            confidence=float(row["confidence"] if row["confidence"] is not None else 0.0),
            evidence_count=int(row["evidence_count"] if row["evidence_count"] is not None else 0),
            distinct_domains=int(row["distinct_domains"] if row["distinct_domains"] is not None else 0),
            conflict_flags=[str(item).strip() for item in conflict_flags if str(item).strip()],
            verification_status=str(row["verification_status"] or "weak").strip().lower() or "weak",
        )

    def upsert_funding_snapshot(self, *, company: str, snapshot: FundingSnapshot) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_funding_cache (
                    company, history, latest_round, latest_date, backers_json, source_urls_json, source_type, as_of_utc, confidence,
                    evidence_count, distinct_domains, conflict_flags_json, verification_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company) DO UPDATE SET
                    history = excluded.history,
                    latest_round = excluded.latest_round,
                    latest_date = excluded.latest_date,
                    backers_json = excluded.backers_json,
                    source_urls_json = excluded.source_urls_json,
                    source_type = excluded.source_type,
                    as_of_utc = excluded.as_of_utc,
                    confidence = excluded.confidence,
                    evidence_count = excluded.evidence_count,
                    distinct_domains = excluded.distinct_domains,
                    conflict_flags_json = excluded.conflict_flags_json,
                    verification_status = excluded.verification_status
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
                    int(snapshot.evidence_count),
                    int(snapshot.distinct_domains),
                    json.dumps(snapshot.conflict_flags, ensure_ascii=False),
                    str(snapshot.verification_status or "weak").strip().lower() or "weak",
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
        _label_key("Target does"): "thesis",
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
        f"*Target does:* {draft.target_does}",
        f"*Why now:* {draft.why_now}",
        f"*What's different:* {draft.whats_different}",
        f"*MOS/risks:* {draft.mos_risks}",
        f"*Bottom line:* {draft.bottom_line}",
        *([f"*Repitch note:* {draft.repitch_note}"] if draft.repitch_note else []),
        *([f"*New evidence:* {draft.repitch_new_evidence}"] if draft.repitch_new_evidence else []),
        "",
        f"*{company} context*",
        f"*Current efforts:* {draft.context_current_efforts}",
        f"*Domain fit/gaps:* {draft.context_domain_fit_gaps}",
        "",
        "*Funding snapshot*",
        f"*History:* {draft.funding_history}",
        f"*Latest round/backers:* {draft.funding_latest_round_backers}",
        *([f"*Warning:* {draft.funding_warning}"] if draft.funding_warning else []),
        "",
        "*Sources*",
        *[_format_source_ref_for_slack(ref) for ref in source_refs],
    ]
    return "\n".join(lines)


def _quality_reason_human(reason_code: str) -> str:
    code = str(reason_code or "").strip().lower()
    mapping = {
        "artifact_contamination": "one or more sections contains menu/CTA artifact text",
        "near_duplicate": "multiple thesis fields repeat the same point",
        "missing_required_evidence": "required evidence coverage is missing for at least one field",
        "why_now_not_monthly_theme": "why-now is not anchored to a recent catalyst or trend",
        "why_now_recency_missing": "why-now lacks enough in-window dated evidence",
        "why_now_no_in_window_source": "why-now evidence is stale for the configured recency window",
        "why_now_weak_evidence_alignment": "why-now text is weakly aligned with available evidence",
        "weak_evidence_alignment": "one or more sections is weakly aligned to supporting evidence",
        "quote_overlap_high": "one or more sections is too close to source wording",
        "critic_field_below_threshold": "critic score is below threshold for at least one field",
        "critic_overall_below_threshold": "critic overall score is below threshold",
        "missing_line": "a required field is blank",
        "specificity_too_generic": "draft language is too generic to be decision-useful",
        "missing_source_refs": "source references are missing from the draft",
    }
    return mapping.get(code, code.replace("_", " "))


def _quality_delivery_mode_for_draft(draft: BoardSeatDraft) -> str:
    if bool(draft.quality_gate_passed):
        return "normal"
    mode = _delivery_mode()
    if mode == "post":
        return "normal"
    if mode == "skip":
        return "skip"
    return "diagnostic_fallback"


def _diagnostic_debug_urls(*, draft: BoardSeatDraft, evidence_pack: dict[str, Any] | None, max_urls: int = 2) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for ref in list(draft.source_refs or []):
        url = _normalize_source_url(ref.url)
        key = url.lower()
        if (not url) or (key in seen):
            continue
        seen.add(key)
        urls.append(url)
        if len(urls) >= max_urls:
            return urls
    if isinstance(evidence_pack, dict):
        all_evidence = evidence_pack.get("all_evidence")
        if isinstance(all_evidence, list):
            for item in all_evidence:
                if not isinstance(item, dict):
                    continue
                url = _normalize_source_url(str(item.get("url") or ""))
                key = url.lower()
                if (not url) or (key in seen):
                    continue
                seen.add(key)
                urls.append(url)
                if len(urls) >= max_urls:
                    break
    return urls[:max_urls]


def _render_quality_diagnostic_message(
    *,
    company: str,
    draft: BoardSeatDraft,
    evidence_pack: dict[str, Any] | None = None,
) -> str:
    failed_fields = [str(item) for item in list(draft.quality_failed_fields or []) if str(item).strip()]
    if not failed_fields:
        failed_fields = [field for field, score in dict(draft.quality_field_scores or {}).items() if float(score or 0.0) < _critic_min_field_score()]
    failure_codes = [str(item) for item in list(draft.quality_failure_codes or []) if str(item).strip()]
    if not failure_codes:
        failure_codes = _quality_failure_codes_from_reasons([str(item) for item in list(draft.quality_reasons or [])])
    max_reasons = _diagnostic_max_reasons()
    reason_humans = [_quality_reason_human(code) for code in failure_codes[:max_reasons]]
    missing_evidence: list[str] = []
    for field_name in _fact_card_fields():
        required = bool(dict(draft.quality_required_evidence or {}).get(field_name, False))
        cards = int(dict(draft.fact_cards_count_by_field or {}).get(field_name, 0))
        if (not required) or cards <= 0:
            missing_evidence.append(field_name)
    lines = [
        f"*Board Seat as a Service — {company}*",
        "*Status:* Quality block (no reliable thesis draft)",
        f"*Failed fields:* {', '.join(failed_fields) if failed_fields else 'unknown'}",
        f"*Why blocked:* {'; '.join(reason_humans) if reason_humans else 'quality checks failed'}",
        f"*Missing evidence:* {', '.join(missing_evidence) if missing_evidence else 'none'}",
    ]
    if _diagnostic_include_urls():
        urls = _diagnostic_debug_urls(draft=draft, evidence_pack=evidence_pack, max_urls=2)
        if urls:
            lines.append("*Debug links:* " + " | ".join(f"<{url}>" for url in urls))
    return "\n".join(lines)


def _delivery_payload_for_draft(
    *,
    company: str,
    draft: BoardSeatDraft,
    evidence_pack: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mode = _quality_delivery_mode_for_draft(draft)
    quality_blocked = not bool(draft.quality_gate_passed)
    if quality_blocked and mode == "diagnostic_fallback":
        return {
            "delivery_mode_applied": "diagnostic_fallback",
            "quality_blocked": True,
            "message": _render_quality_diagnostic_message(
                company=company,
                draft=draft,
                evidence_pack=evidence_pack,
            ),
            "pitch_source": "quality_diagnostic_post",
            "is_diagnostic": True,
        }
    return {
        "delivery_mode_applied": "normal",
        "quality_blocked": quality_blocked,
        "message": _render_board_seat_message(company=company, draft=draft),
        "pitch_source": "live_post",
        "is_diagnostic": False,
    }


def _rich_text_header_block(header: str) -> list[dict[str, Any]]:
    return [
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [{"type": "text", "text": header, "style": {"bold": True, "underline": True}}],
                }
            ],
        },
    ]


def _rich_text_labeled_line_block(label: str, value: str) -> dict[str, Any]:
    return {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [
                    {"type": "text", "text": f"{label}: ", "style": {"bold": True}},
                    {"type": "text", "text": value},
                ],
            }
        ],
    }


def _render_board_seat_blocks(*, company: str, draft: BoardSeatDraft) -> list[dict[str, Any]]:
    refs = _message_source_refs(company=company, draft=draft)
    blocks: list[dict[str, Any]] = []
    blocks.extend(_rich_text_header_block(f"Board Seat as a Service — {company}"))
    blocks.extend(_rich_text_header_block("Thesis"))
    blocks.append(_rich_text_labeled_line_block("Idea", draft.idea_line))
    blocks.append(_rich_text_labeled_line_block("Target does", draft.target_does))
    blocks.append(_rich_text_labeled_line_block("Why now", draft.why_now))
    blocks.append(_rich_text_labeled_line_block("What's different", draft.whats_different))
    blocks.append(_rich_text_labeled_line_block("MOS/risks", draft.mos_risks))
    blocks.append(_rich_text_labeled_line_block("Bottom line", draft.bottom_line))
    if draft.repitch_note:
        blocks.append(_rich_text_labeled_line_block("Repitch note", draft.repitch_note))
    if draft.repitch_new_evidence:
        blocks.append(_rich_text_labeled_line_block("New evidence", draft.repitch_new_evidence))
    blocks.extend(_rich_text_header_block(f"{company} context"))
    blocks.append(_rich_text_labeled_line_block("Current efforts", draft.context_current_efforts))
    blocks.append(_rich_text_labeled_line_block("Domain fit/gaps", draft.context_domain_fit_gaps))
    blocks.extend(_rich_text_header_block("Funding snapshot"))
    blocks.append(_rich_text_labeled_line_block("History", draft.funding_history))
    blocks.append(_rich_text_labeled_line_block("Latest round/backers", draft.funding_latest_round_backers))
    if draft.funding_warning:
        blocks.append(_rich_text_labeled_line_block("Warning", draft.funding_warning))
    blocks.extend(_rich_text_header_block("Sources"))
    for ref in refs:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _format_source_ref_for_slack(ref),
                },
            }
        )
    return blocks


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


def _is_conceptual_target_name(target: str) -> bool:
    tokens = [item for item in re.findall(r"[a-z0-9]+", str(target or "").lower()) if item]
    if not tokens:
        return True
    significant_tokens = [
        tok
        for tok in tokens
        if tok not in {"inc", "corp", "corporation", "company", "llc", "ltd", "co", "group", "systems", "technologies"}
    ]
    if not significant_tokens:
        return True
    if len(significant_tokens) == 1 and significant_tokens[0] in CONCEPTUAL_TARGET_TERMS:
        return True
    if all(tok in CONCEPTUAL_TARGET_TERMS for tok in significant_tokens):
        return True
    return False


def _is_non_company_target_shape(target: str) -> bool:
    candidate = _normalize_text(str(target or ""), max_chars=100).strip()
    if not candidate:
        return True
    lowered = candidate.lower()
    tokens = [item for item in re.findall(r"[a-z0-9]+", lowered) if item]
    if lowered.startswith("@"):
        return True
    if "/" in candidate:
        return True
    if "." in candidate and any(tok in {"js", "ts", "py", "go", "rb"} for tok in tokens):
        return True
    if any(tok in {"ai", "llm", "model", "models"} for tok in tokens) and any(
        tok in {"focused", "first", "native", "driven"} for tok in tokens
    ):
        return True
    if tokens and tokens[-1] in NON_COMPANY_TARGET_SUFFIX_TOKENS:
        return True
    return False


def _target_validation_reason(*, company: str, target: str) -> str:
    candidate = _normalize_text(str(target or ""), max_chars=100).strip()
    if not candidate:
        return "empty_target"
    key = re.sub(r"[^a-z0-9]+", "", candidate.lower())
    single_token = re.sub(r"[^a-z0-9]+", "", candidate.lower())
    if single_token in TARGET_TOKEN_STOPWORDS:
        return "target_stopword"
    if not key:
        return "empty_target"
    if key in ACQ_PLACEHOLDER_TARGETS:
        return "target_placeholder"
    if any(term in candidate.lower() for term in ACQ_INVALID_TARGET_TERMS):
        return "target_invalid_term"
    if _is_conceptual_target_name(candidate):
        return "target_conceptual"
    if _require_company_target() and _is_non_company_target_shape(candidate):
        return "target_not_company_shape"
    if company and _canonical_target_key(company) == _canonical_target_key(candidate):
        return "target_same_as_company"
    return "ok"


def _is_valid_acquisition_idea_line(text: str, *, company: str = "") -> bool:
    line = _normalize_line(text)
    if not line:
        return False
    if _acquisition_verb(line) is None:
        return False
    target = _extract_acquisition_target(line)
    if not target:
        return False
    return _target_validation_reason(company=company, target=target) == "ok"


def _target_candidates_from_seed(*, company: str, seed_text: str) -> list[str]:
    cleaned = re.sub(r"[“”\"'`]", "", str(seed_text or ""))
    matches = re.findall(r"\b[A-Z][A-Za-z0-9&.\-]{1,30}(?:\s+[A-Z][A-Za-z0-9&.\-]{1,30}){0,2}\b", cleaned)
    company_key = _canonical_target_key(company)
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
        if company_key and _canonical_target_key(candidate) == company_key:
            continue
        if "stealth" in candidate.lower():
            continue
        if any(term in candidate.lower() for term in ACQ_INVALID_TARGET_TERMS):
            continue
        if _is_conceptual_target_name(candidate):
            continue
        if _require_company_target() and _is_non_company_target_shape(candidate):
            continue
        out.append(candidate)
    return out


def _is_valid_target_name(*, company: str, target: str) -> bool:
    return _target_validation_reason(company=company, target=target) == "ok"


def _target_fallback_reason(*, company: str, target: str) -> str:
    company_key = _slug_company(company)
    target_key = _target_key(target)
    if not target_key:
        return "fallback_default"
    manual = _manual_default_targets()
    manual_target = manual.get(company_key, "")
    if _target_key(manual_target) == target_key:
        return "fallback_default"
    rotation_keys = {_target_key(item) for item in TARGET_ROTATION_BY_COMPANY.get(company_key, ())}
    if target_key in rotation_keys:
        return "fallback_rotation"
    default_target = DEFAULT_TARGET_BY_COMPANY.get(company_key, "")
    if _target_key(default_target) == target_key:
        return "fallback_default"
    return "fallback_default"


def _default_target_for_company(company: str, *, blocked_keys: set[str] | None = None) -> str:
    company_key = _slug_company(company)
    blocked = {item for item in (blocked_keys or set()) if item}
    manual = _manual_default_targets()
    ordered: list[str] = []
    if company_key in manual:
        ordered.append(manual[company_key])
    for item in TARGET_ROTATION_BY_COMPANY.get(company_key, ()):
        ordered.append(item)
    fallback = DEFAULT_TARGET_BY_COMPANY.get(company_key, "")
    if fallback:
        ordered.append(fallback)
    ordered.append("Scale AI")
    seen: set[str] = set()
    for candidate in ordered:
        key = _target_key(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        if key in blocked:
            continue
        if _is_valid_target_name(company=company, target=candidate):
            return candidate
    for candidate in ordered:
        if _is_valid_target_name(company=company, target=candidate):
            return candidate
    return "Scale AI"


def _best_effort_target(*, company: str, seed_text: str, blocked_keys: set[str] | None = None) -> str:
    blocked = {item for item in (blocked_keys or set()) if item}
    candidates = _target_candidates_from_seed(company=company, seed_text=seed_text)
    for candidate in candidates:
        if _target_key(candidate) in blocked:
            continue
        if _is_valid_target_name(company=company, target=candidate):
            return candidate
    return _default_target_for_company(company, blocked_keys=blocked)


def _resolve_target_to_company(
    *,
    company: str,
    extracted_target: str,
    blocked_keys: set[str] | None = None,
) -> tuple[str, str]:
    blocked = {item for item in (blocked_keys or set()) if item}
    candidate = _normalize_source_text(extracted_target, max_chars=100)
    candidate_key = _target_key(candidate)
    if candidate and candidate_key and (candidate_key not in blocked):
        if _target_validation_reason(company=company, target=candidate) == "ok":
            return candidate, "as_extracted"
    aliases = _target_company_aliases()
    alias_target = aliases.get(candidate_key, "") if candidate_key else ""
    alias_target = _normalize_source_text(alias_target, max_chars=100)
    alias_key = _target_key(alias_target)
    if alias_target and alias_key and (alias_key not in blocked):
        if _target_validation_reason(company=company, target=alias_target) == "ok":
            return alias_target, "alias_mapped"
    fallback = _best_effort_target(company=company, seed_text=candidate or company, blocked_keys=blocked)
    fallback_key = _target_key(fallback)
    if fallback and fallback_key and (fallback_key not in blocked):
        if _target_validation_reason(company=company, target=fallback) == "ok":
            return fallback, _target_fallback_reason(company=company, target=fallback)
    return "", "invalid_after_resolution"


def _rewrite_idea_line_target(*, idea_line: str, target: str, replacement: str, company: str) -> str:
    line = _normalize_line(idea_line)
    if not line:
        return line
    verb = _acquisition_verb(line) or "Acquire"
    current = _normalize_source_text(target, max_chars=100)
    updated = _normalize_source_text(replacement, max_chars=100)
    if current and updated:
        pattern = re.compile(rf"\b(?:acquihire|acquire)\s+{re.escape(current)}\b", flags=re.IGNORECASE)
        rewritten = pattern.sub(f"{verb} {updated}", line, count=1)
        if rewritten != line:
            return _normalize_line(rewritten)
    if updated:
        return _normalize_line(f"{verb} {updated} to accelerate {company} execution in a strategic wedge.")
    return line


def _best_effort_idea_line(*, company: str, seed_text: str, blocked_keys: set[str] | None = None) -> str:
    blocked = {item for item in (blocked_keys or set()) if item}
    extracted = _extract_acquisition_target(seed_text)
    target, _resolution_reason = _resolve_target_to_company(
        company=company,
        extracted_target=extracted,
        blocked_keys=blocked,
    )
    if not target:
        target = _best_effort_target(company=company, seed_text=seed_text, blocked_keys=blocked)
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
        low_signal = _is_low_signal_copy(f"{normalized.title} {text_blob}")
        gate_mode = _source_gate_mode()
        if gate_mode == "hard_block" and low_signal:
            return
        page_type = _detect_page_type(title=normalized.title, snippet=text_blob, url=normalized.url)
        tier = _source_tier_from_row(
            company=company,
            target=target,
            row={"url": normalized.url, "title": normalized.title, "snippet": text_blob},
            page_type=page_type,
        )
        if _source_policy().lower() == "tiered_trusted_first" and tier == "tier_3":
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
        score += {"tier_1": 0.8, "tier_2": 0.2, "tier_3": -1.0}.get(tier, 0.0)
        score += {"news_report": 0.2, "press_release": 0.18, "product_docs": 0.08, "company_profile": 0.04, "pricing_nav": -0.4, "wrapper": -0.5, "social": -0.6}.get(page_type, 0.0)
        if gate_mode == "soft_block" and low_signal:
            score -= 1.2
        normalized_candidates.append(
            SourceCandidate(
                ref=normalized,
                category=category,
                quality=quality,
                score=score,
                text_blob=text_blob,
                tier=tier,
                page_type=page_type,
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

    if policy == "tiered_trusted_first":
        tier_rank = {"tier_1": 0, "tier_2": 1, "tier_3": 2}
        page_rank = {"news_report": 0, "press_release": 1, "product_docs": 2, "company_profile": 3, "pricing_nav": 4, "wrapper": 5, "social": 6}
        normalized_candidates.sort(
            key=lambda item: (
                tier_rank.get(item.tier, 9),
                page_rank.get(item.page_type, 9),
                -item.score,
            )
        )
        for candidate in normalized_candidates:
            if len(selected) >= 4:
                break
            key = candidate.ref.url.lower()
            if key in selected_urls:
                continue
            if candidate.tier == "tier_3":
                continue
            selected.append(candidate)
            selected_urls.add(key)
    elif policy == "target_first_3_1":
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

    target_description = _target_description_from_rows(target=target or company, rows=target_rows)
    if not selected:
        fallback = _fallback_source_refs(company, target=target)
        return SourceSelection(refs=_normalize_source_refs(fallback, max_items=4), confidence="Low", target_description=target_description)

    refs = [item.ref for item in selected[:4]]
    confidence = _source_selection_confidence(selected[:4])
    if confidence == "Low" and _low_signal_mode() != "candidate_with_confidence":
        confidence = "Medium"
    return SourceSelection(refs=refs, confidence=confidence, target_description=target_description)


def _validate_draft(
    draft: BoardSeatDraft,
    *,
    company: str = "",
    evidence_pack: dict[str, Any] | None = None,
) -> list[str]:
    errors: list[str] = []
    line_cap = _max_line_words()
    checks = {
        "idea_line": draft.idea_line,
        "target_does": draft.target_does,
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
        if line_cap > 0 and len(text.split()) > line_cap:
            errors.append(f"{key}_too_long")
    if not _is_valid_acquisition_idea_line(draft.idea_line):
        errors.append("idea_line_invalid")
    why_now_gate = _why_now_semantic_recency_assessment(
        company=company or "Company",
        draft=draft,
        evidence_pack=evidence_pack,
    )
    if not bool(why_now_gate.get("passed")):
        reasons = [str(item) for item in list(why_now_gate.get("reasons") or [])]
        if "why_now_24h_disallowed" in reasons:
            errors.append("why_now_24h_disallowed")
        if any(item in {"why_now_not_monthly_theme", "why_now_missing_recent_evidence", "why_now_no_in_window_source", "why_now_insufficient_dated_sources", "why_now_insufficient_source_diversity", "why_now_missing_catalyst_semantics", "why_now_weak_evidence_alignment"} for item in reasons):
            errors.append("why_now_not_monthly_theme")
    if (isinstance(evidence_pack, dict)) and (not bool(why_now_gate.get("recency_passed"))):
        errors.append("why_now_recency_missing")
    if _specificity_mode() == "moderate":
        specificity_fields = [
            draft.why_now,
            draft.whats_different,
            draft.mos_risks,
            draft.bottom_line,
            draft.context_current_efforts,
            draft.context_domain_fit_gaps,
        ]
        generic_count = sum(1 for item in specificity_fields if _is_generic_line(item) or not _line_has_concrete_anchor(item))
        if generic_count > 1:
            errors.append("specificity_too_generic")
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
    funding_warning = _funding_warning_line(funding)
    acq_rows = acquisition_rows or []
    seed_text = " ".join(
        [
            str(draft.idea_line or ""),
            str(draft.why_now or ""),
            str(draft.whats_different or ""),
            " ".join(str(row.get("title") or "") for row in acq_rows[:2]),
        ]
    )
    idea_line = _normalize_line(draft.idea_line)
    target_original = _normalize_source_text(draft.target_original, max_chars=100) or _extract_acquisition_target(
        str(draft.idea_line or "")
    )
    target_resolution_reason = "as_extracted"
    resolved_target = target_original
    if _require_company_target():
        resolved_target, target_resolution_reason = _resolve_target_to_company(
            company=company,
            extracted_target=target_original,
            blocked_keys=set(),
        )
    elif resolved_target and _target_validation_reason(company=company, target=resolved_target) != "ok":
        resolved_target = ""
        target_resolution_reason = "invalid_after_resolution"
    if resolved_target:
        if _is_valid_acquisition_idea_line(idea_line, company=company):
            idea_line = _rewrite_idea_line_target(
                idea_line=idea_line,
                target=target_original,
                replacement=resolved_target,
                company=company,
            )
        else:
            idea_line = _normalize_line(f"Acquire {resolved_target} to accelerate {company} execution in a strategic wedge.")
    else:
        target_resolution_reason = "invalid_after_resolution"
    if not _is_valid_acquisition_idea_line(idea_line, company=company):
        fallback_target = _best_effort_target(company=company, seed_text=seed_text, blocked_keys=set())
        idea_line = _normalize_line(f"Acquire {fallback_target} to accelerate {company} execution in a strategic wedge.")
        if _target_validation_reason(company=company, target=fallback_target) == "ok":
            target_resolution_reason = _target_fallback_reason(company=company, target=fallback_target)
        else:
            target_resolution_reason = "invalid_after_resolution"
    source_draft = BoardSeatDraft(
        idea_line=idea_line,
        target_does=draft.target_does,
        why_now=draft.why_now,
        whats_different=draft.whats_different,
        mos_risks=draft.mos_risks,
        bottom_line=draft.bottom_line,
        context_current_efforts=draft.context_current_efforts,
        context_domain_fit_gaps=draft.context_domain_fit_gaps,
        funding_history=draft.funding_history,
        funding_latest_round_backers=draft.funding_latest_round_backers,
        funding_warning=draft.funding_warning,
        repitch_note=draft.repitch_note,
        repitch_new_evidence=draft.repitch_new_evidence,
        source_refs=draft.source_refs,
        raw_model_output=draft.raw_model_output,
        rewrite_reasons=draft.rewrite_reasons,
        target_original=target_original,
        target_resolution_reason=target_resolution_reason,
    )
    source_selection = _build_source_refs(company=company, draft=source_draft, funding=funding, acquisition_rows=acq_rows)
    target = _extract_acquisition_target(idea_line) or _default_target_for_company(company, blocked_keys=set())
    default_why_now = f"Over the past month, buyer demand and deployment urgency in {company}'s category have shifted toward measurable ROI."
    default_whats_different = f"{target} adds a differentiated wedge through product depth, integration velocity, and enterprise execution."
    default_mos = f"Main risks are integration complexity, customer overlap, and execution slippage during platform consolidation."
    default_bottom = f"Execute one target-led move with 12-month milestones tied to adoption, margin, and retention."
    default_context_efforts = f"{company}'s current roadmap and customer footprint create a clear insertion point for {target}."
    default_context_fit = f"Best fit is where {target}'s capabilities close current roadmap gaps faster than internal build."
    writing_artifact_cleanups: list[str] = list(draft.writing_artifact_cleanups or [])

    def _clean(label: str, value: str) -> str:
        normalized = _normalize_line(value)
        if not _strip_obvious_artifacts():
            return normalized
        stripped, cleanups = _strip_obvious_writing_artifacts(normalized)
        writing_artifact_cleanups.extend(f"{label}:{item}" for item in cleanups)
        return _normalize_line(stripped)

    target_does = _clean("target_does", draft.target_does) or _clean("target_description", source_selection.target_description) or _normalize_line(
        f"{target} builds enterprise software and automation infrastructure for production workflows."
    )
    why_now = _clean("why_now", draft.why_now) or _normalize_line(default_why_now)
    whats_different = _clean("whats_different", draft.whats_different) or _normalize_line(default_whats_different)
    mos_risks = _clean("mos_risks", draft.mos_risks) or _normalize_line(default_mos)
    bottom_line = _clean("bottom_line", draft.bottom_line) or _normalize_line(default_bottom)
    context_current_efforts = _clean("context_current_efforts", draft.context_current_efforts) or _normalize_line(default_context_efforts)
    context_domain_fit_gaps = _clean("context_domain_fit_gaps", draft.context_domain_fit_gaps) or _normalize_line(default_context_fit)

    thesis_values = {
        "target_does": target_does,
        "why_now": why_now,
        "whats_different": whats_different,
        "mos_risks": mos_risks,
    }
    thesis_fallbacks = {
        "target_does": _normalize_line(f"{target} delivers a concrete product wedge for enterprise buyers."),
        "why_now": _normalize_line(default_why_now),
        "whats_different": _normalize_line(default_whats_different),
        "mos_risks": _normalize_line(default_mos),
    }
    writing_field_dedup_fixes: list[str] = list(draft.writing_field_dedup_fixes or [])
    seen_thesis: dict[str, str] = {}
    for field_name in ("target_does", "why_now", "whats_different", "mos_risks"):
        value = str(thesis_values.get(field_name) or "").strip()
        if not value:
            continue
        key = _normalize_line_text(value).lower()
        if not key:
            continue
        if key in seen_thesis:
            thesis_values[field_name] = thesis_fallbacks[field_name]
            writing_field_dedup_fixes.append(field_name)
            continue
        seen_thesis[key] = field_name

    target_does = thesis_values["target_does"]
    why_now = thesis_values["why_now"]
    whats_different = thesis_values["whats_different"]
    mos_risks = thesis_values["mos_risks"]
    return BoardSeatDraft(
        idea_line=idea_line,
        target_does=target_does,
        why_now=why_now,
        whats_different=whats_different,
        mos_risks=mos_risks,
        bottom_line=bottom_line,
        context_current_efforts=context_current_efforts,
        context_domain_fit_gaps=context_domain_fit_gaps,
        funding_history=_normalize_line(draft.funding_history) or funding_history,
        funding_latest_round_backers=_normalize_line(draft.funding_latest_round_backers) or funding_latest_round_backers,
        funding_warning=_clean("funding_warning", draft.funding_warning) or funding_warning,
        repitch_note=_normalize_line(draft.repitch_note, max_words=32),
        repitch_new_evidence=_normalize_line(draft.repitch_new_evidence, max_words=32),
        source_refs=source_selection.refs,
        raw_model_output=draft.raw_model_output,
        rewrite_reasons=draft.rewrite_reasons,
        target_original=target_original,
        target_resolution_reason=target_resolution_reason,
        writing_artifact_cleanups=sorted(set(writing_artifact_cleanups)),
        writing_field_dedup_fixes=writing_field_dedup_fixes,
        quality_gate_passed=bool(draft.quality_gate_passed),
        quality_score=float(draft.quality_score),
        quality_reasons=list(draft.quality_reasons or []),
        rewrite_attempts=int(draft.rewrite_attempts),
        quality_fail_stage=str(draft.quality_fail_stage or ""),
        quality_field_scores=dict(draft.quality_field_scores or {}),
        quality_failed_fields=list(draft.quality_failed_fields or []),
        quality_failure_codes=list(draft.quality_failure_codes or []),
        quality_required_evidence=dict(draft.quality_required_evidence or {}),
        evidence_tier_mix=dict(draft.evidence_tier_mix or {}),
        fact_cards_count_by_field=dict(draft.fact_cards_count_by_field or {}),
        quote_overlap_by_field=dict(draft.quote_overlap_by_field or {}),
        why_now_recency_passed=bool(draft.why_now_recency_passed),
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
        chosen = _normalize_line(snippets[0]) if snippets else f"Over the past month, execution priorities in {company}'s market shifted toward measurable ROI and deployment reliability."

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
            target_does="",
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
        evidence_count=0,
        distinct_domains=0,
        conflict_flags=[],
        verification_status="weak",
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
    latest = _normalize_line(snapshot.latest_round, max_words=10)
    latest_date = _normalize_line(snapshot.latest_date, max_words=6)
    backers = _normalize_line(", ".join(snapshot.backers[:3]), max_words=10)

    history_line = history or "Funding history not confirmed from current sources."
    latest_parts: list[str] = []
    if latest:
        latest_parts.append(f"{latest}")
    if latest_date:
        latest_parts.append(f"({latest_date})")
    if backers:
        latest_parts.append(f"backers: {backers}")
    latest_line = _normalize_line(" ".join(latest_parts)) if latest_parts else _normalize_line(UNKNOWN_FUNDING_TEXT)
    return (_normalize_line(history_line), _normalize_line(latest_line))


def _funding_has_major_conflict(conflict_flags: list[str]) -> bool:
    lowered = [str(item or "").strip().lower() for item in conflict_flags if str(item or "").strip()]
    if not lowered:
        return False
    return any(item.startswith("major_") for item in lowered)


def _funding_verification_status(snapshot: FundingSnapshot) -> str:
    status = str(snapshot.verification_status or "").strip().lower()
    if status in {"verified", "partial", "weak"}:
        return status
    if snapshot.distinct_domains >= _funding_min_domains() and (not _funding_has_major_conflict(snapshot.conflict_flags)):
        return "verified"
    if snapshot.evidence_count >= 1 and snapshot.distinct_domains >= 1:
        return "partial"
    return "weak"


def _funding_confidence_band(snapshot: FundingSnapshot) -> str:
    if _is_funding_snapshot_unknown(snapshot):
        return "low"
    status = _funding_verification_status(snapshot)
    if snapshot.confidence < _funding_low_conf_threshold():
        return "low"
    if status == "verified":
        return "high"
    if status == "partial":
        return "medium"
    return "low"


def _funding_warning_line(snapshot: FundingSnapshot) -> str:
    if (not _funding_warning_mode()) or (_funding_confidence_band(snapshot) != "low"):
        return ""
    return _normalize_line(LOW_CONFIDENCE_FUNDING_WARNING_TEXT)


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


def _http_json_post(*, url: str, headers: dict[str, str], payload: dict[str, Any]) -> Any:
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, headers=headers, method="POST")
    with urlopen(request, timeout=20) as response:  # nosec B310
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def _format_amount_short(value: float | int | str | None) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except Exception:
        return ""
    if numeric <= 0:
        return ""
    if numeric >= 1_000_000_000:
        return f"${numeric/1_000_000_000:.1f}B"
    if numeric >= 1_000_000:
        return f"${numeric/1_000_000:.0f}M"
    return f"${numeric:,.0f}"


def _target_funding_from_crunchbase(target_name: str) -> FundingSnapshot | None:
    if (not _crunchbase_enabled()) or (not target_name.strip()):
        return None
    api_key = _crunchbase_api_key()
    if not api_key:
        return None
    base = (os.environ.get("COATUE_CLAW_CRUNCHBASE_API_BASE", "https://api.crunchbase.com/api/v4") or "https://api.crunchbase.com/api/v4").strip().rstrip("/")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-cb-user-key": api_key,
        "User-Agent": "CoatueClaw/1.0",
    }
    search_payload = {
        "field_ids": [
            "identifier",
            "name",
            "short_description",
            "num_funding_rounds",
            "last_funding_type",
            "last_funding_at",
            "last_funding_total",
            "funding_total",
        ],
        "query": [{"type": "predicate", "field_id": "name", "operator_id": "contains", "values": [target_name]}],
        "limit": 5,
    }
    try:
        result = _http_json_post(url=f"{base}/searches/organizations", headers=headers, payload=search_payload)
    except Exception:
        return None
    entities = result.get("entities") if isinstance(result, dict) else None
    if not isinstance(entities, list) or not entities:
        return None
    best = entities[0] if isinstance(entities[0], dict) else {}
    properties = best.get("properties") if isinstance(best, dict) else None
    props = properties if isinstance(properties, dict) else {}
    identifier = best.get("identifier") if isinstance(best, dict) else None
    permalink = str((identifier or {}).get("permalink") or "").strip() if isinstance(identifier, dict) else ""
    org_name = str((identifier or {}).get("value") or target_name).strip() if isinstance(identifier, dict) else target_name
    short_desc = _normalize_text(str(props.get("short_description") or ""), max_chars=220)
    latest_round = _normalize_text(str(props.get("last_funding_type") or ""), max_chars=80)
    latest_date = _normalize_text(str(props.get("last_funding_at") or ""), max_chars=40)
    latest_amount = _format_amount_short(props.get("last_funding_total"))
    history_amount = _format_amount_short(props.get("funding_total"))
    history_parts = [f"{org_name} has raised {history_amount} to date." if history_amount else ""]
    if short_desc:
        history_parts.append(short_desc)
    history = _normalize_line(" ".join([item for item in history_parts if item]))
    latest_parts = [latest_round]
    if latest_amount:
        latest_parts.append(latest_amount)
    if latest_date:
        latest_parts.append(f"({latest_date})")
    latest = _normalize_line(" ".join([item for item in latest_parts if item]))
    source_urls: list[str] = []
    if permalink:
        source_urls.append(f"https://www.crunchbase.com/organization/{permalink}")
    domain_count = len({_domain_from_url(item) for item in source_urls if _domain_from_url(item)})
    snapshot = FundingSnapshot(
        history=history,
        latest_round=latest,
        latest_date=latest_date,
        backers=[],
        source_urls=source_urls,
        source_type="crunchbase_api",
        as_of_utc=_utc_now_iso(),
        confidence=0.72 if latest else 0.58,
        evidence_count=max(1, len(source_urls)),
        distinct_domains=max(1, domain_count),
        conflict_flags=[],
        verification_status="partial",
    )
    if not snapshot.history and not snapshot.latest_round:
        return None
    return snapshot


def _merge_funding_snapshots(primary: FundingSnapshot | None, secondary: FundingSnapshot | None) -> FundingSnapshot | None:
    if primary is None:
        return secondary
    if secondary is None:
        return primary
    history = primary.history or secondary.history
    latest_round = primary.latest_round or secondary.latest_round
    latest_date = primary.latest_date or secondary.latest_date
    backers = primary.backers or secondary.backers
    urls: list[str] = []
    seen: set[str] = set()
    for item in [*primary.source_urls, *secondary.source_urls]:
        url = _normalize_source_url(item)
        if not url:
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    merged_conflicts: list[str] = []
    seen_flags: set[str] = set()
    for item in [*primary.conflict_flags, *secondary.conflict_flags]:
        flag = str(item or "").strip().lower()
        if (not flag) or (flag in seen_flags):
            continue
        seen_flags.add(flag)
        merged_conflicts.append(flag)
    evidence_count = max(int(primary.evidence_count), int(secondary.evidence_count))
    distinct_domains = max(int(primary.distinct_domains), int(secondary.distinct_domains))
    verification = "weak"
    for candidate in (primary.verification_status, secondary.verification_status):
        key = str(candidate or "").strip().lower()
        if key == "verified":
            verification = "verified"
            break
        if key == "partial":
            verification = "partial"
    return FundingSnapshot(
        history=history,
        latest_round=latest_round,
        latest_date=latest_date,
        backers=backers,
        source_urls=urls[:8],
        source_type=primary.source_type if primary.history or primary.latest_round else secondary.source_type,
        as_of_utc=_utc_now_iso(),
        confidence=max(float(primary.confidence), float(secondary.confidence)),
        evidence_count=evidence_count,
        distinct_domains=distinct_domains,
        conflict_flags=merged_conflicts,
        verification_status=verification,
    )


def _brave_query_rows(query: str, *, count: int) -> list[dict[str, str]]:
    api_key = _brave_search_api_key()
    if not api_key:
        return []
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
        "User-Agent": "CoatueClaw/1.0",
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    try:
        payload = _http_json(
            url=WEB_SEARCH_ENDPOINT,
            headers=headers,
            params={"q": query, "count": str(count), "country": "us", "search_lang": "en"},
        )
    except Exception:
        return []
    web = payload.get("web") if isinstance(payload, dict) else None
    results = web.get("results") if isinstance(web, dict) else None
    if not isinstance(results, list):
        return []
    for item in results:
        if not isinstance(item, dict):
            continue
        url = _normalize_source_url(str(item.get("url") or ""))
        if not url or url in seen:
            continue
        seen.add(url)
        rows.append(
            {
                "publisher": _publisher_from_url(url),
                "title": _normalize_text(str(item.get("title") or ""), max_chars=240),
                "snippet": _normalize_text(str(item.get("description") or ""), max_chars=420),
                "url": url,
            }
        )
        if len(rows) >= count:
            return rows
    return rows


def _brave_search_rows(company: str) -> list[dict[str, str]]:
    queries = [
        f"{company} funding history latest round backers",
        f"{company} raised series funding investors",
    ]
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for query in queries:
        for row in _brave_query_rows(query, count=BRAVE_SEARCH_RESULTS):
            key = _url_dedupe_key(str(row.get("url") or "")) or str(row.get("url") or "").lower()
            if not key or key in seen:
                continue
            seen.add(key)
            rows.append(row)
            if len(rows) >= BRAVE_SEARCH_RESULTS:
                return rows
    return rows


def _funding_web_rows(entity_name: str) -> list[dict[str, str]]:
    rows = _brave_search_rows(entity_name)
    google_queries = [
        f"{entity_name} funding history latest round investors",
        f"{entity_name} raised series funding",
    ]
    seen = {str(item.get("url") or "").strip().lower() for item in rows if str(item.get("url") or "").strip()}
    for query in google_queries:
        for row in _google_serp_rows(query, max_results=8):
            url = str(row.get("url") or "").strip().lower()
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append(row)
            if len(rows) >= 12:
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
            published_hint = _extract_published_hint(
                " ".join(
                    [
                        str(item.get("age") or ""),
                        str(item.get("published") or ""),
                        str(item.get("date") or ""),
                        title,
                        snippet,
                        _fallback_published_hint_from_url(url),
                    ]
                )
            )
            rows.append(
                {
                    "publisher": _publisher_from_url(url),
                    "title": title or _normalize_source_text(snippet, max_chars=180) or "Reference",
                    "snippet": snippet,
                    "url": url,
                    "published_hint": published_hint,
                }
            )
            if len(rows) >= ACQ_SEARCH_RESULTS:
                return rows
    return rows


def _google_serp_rows(query: str, *, max_results: int = 8) -> list[dict[str, str]]:
    api_key = _board_seat_google_serp_key()
    if not api_key:
        return []
    try:
        payload = _http_json(
            url=_board_seat_google_serp_endpoint(),
            headers={"Accept": "application/json", "User-Agent": "CoatueClaw/1.0"},
            params={"engine": "google", "api_key": api_key, "q": query, "num": str(max_results), "hl": "en", "gl": "us"},
        )
    except Exception:
        return []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for section in ("organic_results",):
        entries = payload.get(section) if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            url = _normalize_source_url(str(item.get("link") or ""))
            if not url or url.lower() in seen:
                continue
            seen.add(url.lower())
            title = _normalize_source_text(str(item.get("title") or ""), max_chars=180)
            snippet = _normalize_text(str(item.get("snippet") or ""), max_chars=420)
            published_hint = _extract_published_hint(
                " ".join(
                    [
                        str(item.get("date") or ""),
                        str(item.get("snippet_highlighted_words") or ""),
                        title,
                        snippet,
                        _fallback_published_hint_from_url(url),
                    ]
                )
            )
            rows.append(
                {
                    "publisher": _publisher_from_url(url),
                    "title": title or _normalize_source_text(snippet, max_chars=180) or "Reference",
                    "snippet": snippet,
                    "url": url,
                    "published_hint": published_hint,
                }
            )
    return rows


def _funding_rows_metrics(rows: list[dict[str, str]]) -> tuple[int, int, list[str]]:
    evidence_count = len(rows)
    domains = {_domain_from_url(str(item.get("url") or "")) for item in rows}
    domains = {item for item in domains if item}
    return evidence_count, len(domains), _funding_evidence_conflicts(rows)


def _build_funding_snapshot(
    *,
    history: str,
    latest_round: str,
    latest_date: str,
    backers: list[str],
    source_urls: list[str],
    source_type: str,
    confidence: float,
    evidence_rows: list[dict[str, str]],
) -> FundingSnapshot:
    evidence_count, distinct_domains, conflict_flags = _funding_rows_metrics(evidence_rows)
    status = "weak"
    if (distinct_domains >= _funding_min_domains()) and (not _funding_has_major_conflict(conflict_flags)):
        status = "verified"
    elif evidence_count >= 1 and distinct_domains >= 1:
        status = "partial"
    return FundingSnapshot(
        history=history,
        latest_round=latest_round,
        latest_date=latest_date,
        backers=backers,
        source_urls=source_urls,
        source_type=source_type,
        as_of_utc=_utc_now_iso(),
        confidence=max(0.0, min(1.0, confidence)),
        evidence_count=evidence_count,
        distinct_domains=distinct_domains,
        conflict_flags=conflict_flags,
        verification_status=status,
    )


def _iso_to_utc(value: str | None) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return datetime.now(UTC)


def _event_type_and_score(*, title: str, snippet: str) -> tuple[str, float]:
    text = f"{title} {snippet}".lower()
    if re.search(r"\b(acquire|acquired|acquisition|merger|m&a)\b", text):
        return ("mna", 0.99)
    if re.search(r"\b(bankrupt|chapter 11|fraud|probe|investigation|sanction)\b", text):
        return ("critical_risk", 0.98)
    if re.search(r"\b(filed for ipo|files for ipo|ipo)\b", text):
        return ("ipo", 0.97)
    if re.search(r"\b(awarded|wins?)\b.{0,60}\b(contract|program)\b", text):
        return ("major_contract", 0.95)
    if re.search(r"\b(raises?|raised)\b", text) and FUNDING_AMOUNT_RE.search(text):
        return ("funding_step_change", 0.9)
    if re.search(r"\b(launches?|launched|release[ds])\b", text):
        return ("product_launch", 0.72)
    if re.search(r"\b(partner(ship|ed)?|integration)\b", text):
        return ("partnership", 0.68)
    return ("other", 0.45)


def _event_evidence_quality(url: str) -> float:
    if _is_quality_source(url):
        return 1.0
    if _is_low_quality_source(url):
        return 0.25
    return 0.6


def _target_event_rows(*, company: str, target: str, max_rows: int) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    queries = [
        f"{target} funding round valuation investors",
        f"{target} acquisition merger strategic alternatives",
        f"{target} major contract customer announcement",
        f"{target} product launch regulatory update",
        f"{target} {company} partnership",
    ]
    for query in queries:
        for item in _brave_query_rows(query, count=6):
            url = _normalize_source_url(str(item.get("url") or ""))
            key = _url_dedupe_key(url) or url.lower()
            if (not url) or (key in seen):
                continue
            seen.add(key)
            rows.append(
                {
                    "publisher": _normalize_source_text(str(item.get("publisher") or ""), max_chars=64) or _publisher_from_url(url),
                    "title": _normalize_source_text(str(item.get("title") or ""), max_chars=200),
                    "snippet": _normalize_text(str(item.get("snippet") or ""), max_chars=420),
                    "url": url,
                    "source_type": "brave",
                    "event_at_utc": _utc_now_iso(),
                }
            )
            if len(rows) >= max_rows:
                return rows
        for item in _google_serp_rows(query, max_results=6):
            url = _normalize_source_url(str(item.get("url") or ""))
            key = _url_dedupe_key(url) or url.lower()
            if (not url) or (key in seen):
                continue
            seen.add(key)
            rows.append(
                {
                    "publisher": _normalize_source_text(str(item.get("publisher") or ""), max_chars=64) or _publisher_from_url(url),
                    "title": _normalize_source_text(str(item.get("title") or ""), max_chars=200),
                    "snippet": _normalize_text(str(item.get("snippet") or ""), max_chars=420),
                    "url": url,
                    "source_type": "google_serp",
                    "event_at_utc": _utc_now_iso(),
                }
            )
            if len(rows) >= max_rows:
                return rows
    return rows


def _track_promising_target_events(
    *,
    store: BoardSeatStore,
    company: str,
    candidate_targets: list[str],
) -> dict[str, Any]:
    inserted = 0
    scanned = 0
    tracked_targets: list[str] = []
    unique_targets: list[str] = []
    seen_target_keys: set[str] = set()
    for target in candidate_targets:
        normalized = _normalize_source_text(target, max_chars=120)
        key = _target_key(normalized)
        if (not key) or (key in seen_target_keys):
            continue
        seen_target_keys.add(key)
        unique_targets.append(normalized)
        if len(unique_targets) >= _target_event_max_targets_per_company():
            break
    for target in unique_targets:
        tracked_targets.append(target)
        rows = _target_event_rows(company=company, target=target, max_rows=_target_event_max_rows_per_target())
        for row in rows:
            scanned += 1
            title = str(row.get("title") or "")
            snippet = str(row.get("snippet") or "")
            event_type, base_score = _event_type_and_score(title=title, snippet=snippet)
            quality = _event_evidence_quality(str(row.get("url") or ""))
            impact_score = max(0.0, min(1.0, base_score + (0.06 * (quality - 0.5))))
            if impact_score < 0.67:
                continue
            did_insert = store.record_target_event(
                company=company,
                target=target,
                event_at_utc=str(row.get("event_at_utc") or _utc_now_iso()),
                publisher=str(row.get("publisher") or ""),
                title=title,
                url=str(row.get("url") or ""),
                snippet=snippet,
                event_type=event_type,
                evidence_quality=quality,
                impact_score=impact_score,
                source_type=str(row.get("source_type") or "web"),
            )
            if did_insert:
                inserted += 1
    return {
        "company": company,
        "tracked_targets": tracked_targets,
        "scanned": scanned,
        "inserted": inserted,
    }


def _assess_repitch_significance(
    *,
    store: BoardSeatStore,
    company: str,
    target: str,
    prior_pitch_posted_at_utc: str,
) -> dict[str, Any]:
    prior_ts = _iso_to_utc(prior_pitch_posted_at_utc)
    window_end = datetime.now(UTC)
    window_start = max(prior_ts, window_end - timedelta(days=HARD_NO_REPITCH_DAYS))
    target_key = _target_key(target)
    events = store.recent_target_events(
        company=company,
        target_key=target_key,
        since_utc=window_start.isoformat(),
        limit=200,
    )
    ranked = sorted(
        events,
        key=lambda row: (float(row.get("impact_score") or 0.0), float(row.get("evidence_quality") or 0.0), str(row.get("event_at_utc") or "")),
        reverse=True,
    )
    top = ranked[:5]
    aggregate_score = sum(float(item.get("impact_score") or 0.0) for item in top[:3])
    max_event_score = max((float(item.get("impact_score") or 0.0) for item in top), default=0.0)
    strong_events = [
        item
        for item in top
        if (float(item.get("impact_score") or 0.0) >= 0.93) and (float(item.get("evidence_quality") or 0.0) >= 0.8)
    ]
    strong_domains = {
        _domain_from_url(str(item.get("url") or ""))
        for item in strong_events
        if _domain_from_url(str(item.get("url") or ""))
    }
    allow = bool(
        (len(strong_events) >= 2)
        and (len(strong_domains) >= 2)
        and (max_event_score >= 0.97)
        and (aggregate_score >= 2.75)
    )
    reason = (
        "allow_repitch_exceptional_signal"
        if allow
        else "reject_repitch_not_exceptional_enough"
    )
    compact_events: list[dict[str, Any]] = []
    for item in top:
        compact_events.append(
            {
                "event_at_utc": str(item.get("event_at_utc") or ""),
                "publisher": str(item.get("publisher") or ""),
                "title": str(item.get("title") or ""),
                "url": str(item.get("url") or ""),
                "event_type": str(item.get("event_type") or "other"),
                "impact_score": round(float(item.get("impact_score") or 0.0), 3),
                "evidence_quality": round(float(item.get("evidence_quality") or 0.0), 3),
            }
        )
    assessment_id = store.record_repitch_assessment(
        company=company,
        target=target,
        target_key=target_key,
        prior_pitch_posted_at_utc=prior_pitch_posted_at_utc,
        window_start_utc=window_start.isoformat(),
        window_end_utc=window_end.isoformat(),
        top_events=compact_events,
        aggregate_score=aggregate_score,
        max_event_score=max_event_score,
        distinct_domains=len(strong_domains),
        decision=("allow" if allow else "reject"),
        reason=reason,
        strictness_version="critical_v1",
    )
    return {
        "assessment_id": assessment_id,
        "allow": allow,
        "reason": reason,
        "top_events": compact_events,
        "aggregate_score": round(aggregate_score, 3),
        "max_event_score": round(max_event_score, 3),
        "distinct_domains": len(strong_domains),
    }


def _repitch_disclosure_from_assessment(
    *,
    prior_pitch_posted_at_utc: str,
    top_events: list[dict[str, Any]],
) -> tuple[str, str]:
    prior_day = _iso_to_utc(prior_pitch_posted_at_utc).strftime("%Y-%m-%d")
    note = f"This target was pitched on {prior_day}; resurfacing only because new evidence is exceptionally material."
    evidence_items: list[str] = []
    for item in top_events[:2]:
        publisher = _normalize_source_text(str(item.get("publisher") or ""), max_chars=32) or "Source"
        title = _normalize_source_text(str(item.get("title") or ""), max_chars=120)
        if not title:
            continue
        evidence_items.append(f"{publisher}: {title}")
    if not evidence_items:
        evidence = "No qualifying post-pitch evidence met strict resurfacing criteria."
    else:
        evidence = "; ".join(evidence_items)
    return (_normalize_line(note, max_words=32), _normalize_line(evidence, max_words=32))


def _fetch_thematic_context(*, company: str, target: str, snippets: list[str]) -> list[str]:
    query_seed = " ".join(snippets[:2]).strip()
    queries = [
        f"{company} industry trend last month",
        f"{company} enterprise demand trend past month",
        f"{target} product adoption trend" if target else f"{company} strategic partner trend",
    ]
    if query_seed:
        queries.append(f"{company} {query_seed}")
    out: list[str] = []
    seen: set[str] = set()
    for query in queries:
        brave_rows = _target_search_rows(target=target or company, company=company, snippets=[query])
        google_rows = _google_serp_rows(query, max_results=6)
        for row in [*brave_rows, *google_rows]:
            snippet = _normalize_line_text(str(row.get("snippet") or ""))
            if not snippet:
                continue
            key = snippet.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(_normalize_line(snippet))
            if len(out) >= 8:
                return out
    return out


def _llm_evidence_pack(
    *,
    company: str,
    target: str,
    target_rows: list[dict[str, str]],
    acquisition_rows: list[dict[str, str]],
    funding: FundingSnapshot,
) -> dict[str, Any]:
    merged_rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for row in [*list(target_rows or []), *list(acquisition_rows or [])]:
        if not isinstance(row, dict):
            continue
        url = _normalize_source_url(str(row.get("url") or ""))
        if not url:
            continue
        key = url.lower()
        if key in seen_urls:
            continue
        seen_urls.add(key)
        merged_rows.append(row)
        if len(merged_rows) >= _evidence_max_urls():
            break

    evidence_items: list[EvidenceItem] = []
    fetch_budget = _evidence_max_urls() if _evidence_fetch_enabled() else 0
    fetched = 0
    for index, row in enumerate(merged_rows, start=1):
        row_url = _normalize_source_url(str(row.get("url") or ""))
        row_domain = _source_domain(row_url)
        allow_fetch = bool(
            fetched < fetch_budget
            and row_url
            and row_domain
            and (not _is_search_results_url(row_url))
            and any(row_domain.endswith(suffix) for suffix in SOURCE_TIER1_TRUSTED_DOMAIN_SUFFIXES)
        )
        item = _row_to_evidence_item(
            company=company,
            target=target,
            row=row,
            index=index,
            allow_fetch=allow_fetch,
        )
        if item is None:
            continue
        if allow_fetch:
            fetched += 1
        evidence_items.append(item)

    section_evidence = _build_section_evidence(company=company, target=target, items=evidence_items)
    fact_cards, fact_cards_count_by_field = _build_fact_cards(section_evidence)
    target_bundle = list(section_evidence.get("target_does_evidence") or [])[:6]
    acq_bundle = [
        _evidence_item_to_dict(item)
        for item in evidence_items
        if item.page_type in {"news_report", "press_release", "company_profile"} and item.tier != "tier_3"
    ][:6]

    return {
        "company": company,
        "target": target,
        "source_policy": _source_policy(),
        "fact_card_mode": _fact_card_mode(),
        "fact_cards": fact_cards,
        "fact_cards_count_by_field": fact_cards_count_by_field,
        "target_evidence": target_bundle,
        "acquisition_evidence": acq_bundle,
        "all_evidence": [_evidence_item_to_dict(item) for item in evidence_items],
        "target_does_evidence": list(section_evidence.get("target_does_evidence") or []),
        "why_now_evidence": list(section_evidence.get("why_now_evidence") or []),
        "whats_different_evidence": list(section_evidence.get("whats_different_evidence") or []),
        "mos_risks_evidence": list(section_evidence.get("mos_risks_evidence") or []),
        "quality_required_evidence": dict(section_evidence.get("quality_required_evidence") or {}),
        "evidence_tier_mix": dict(section_evidence.get("evidence_tier_mix") or {}),
        "why_now_recency_passed": bool(section_evidence.get("why_now_recency_passed")),
        "funding_summary": {
            "history": _normalize_text(funding.history, max_chars=220),
            "latest_round": _normalize_text(funding.latest_round, max_chars=120),
            "latest_date": _normalize_text(funding.latest_date, max_chars=80),
            "backers": [_normalize_source_text(str(item), max_chars=60) for item in funding.backers[:6]],
            "source_type": _normalize_source_text(funding.source_type, max_chars=32),
        },
    }


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
    source_urls = [str(item.get("url") or "").strip() for item in rows if str(item.get("url") or "").strip()]
    return _build_funding_snapshot(
        history=history,
        latest_round=latest_round,
        latest_date=latest_date,
        backers=backers[:8],
        source_urls=source_urls[:8],
        source_type="web_refresh",
        confidence=confidence,
        evidence_rows=rows,
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
    source_urls = [str(item.get("url") or "").strip() for item in rows if str(item.get("url") or "").strip()]
    snapshot = _build_funding_snapshot(
        history=_normalize_text(history_match.group(1), max_chars=220) if history_match else "",
        latest_round=_normalize_text(round_match.group(1), max_chars=50) if round_match else "",
        latest_date=_normalize_text(date_match.group(0), max_chars=30) if date_match else "",
        backers=backers[:6],
        source_urls=source_urls[:8],
        source_type="web_refresh",
        confidence=0.45,
        evidence_rows=rows,
    )
    if not snapshot.history and not snapshot.latest_round and not snapshot.latest_date and not snapshot.backers:
        return None
    return snapshot


def _refresh_funding_snapshot_from_web(*, company: str) -> FundingSnapshot | None:
    raw_rows = _funding_web_rows(company)
    rows = _prepare_funding_evidence_rows(raw_rows)
    if not rows:
        return None
    llm_snapshot = _extract_funding_with_llm(company=company, rows=rows)
    if llm_snapshot and (llm_snapshot.history or llm_snapshot.latest_round or llm_snapshot.latest_date or llm_snapshot.backers):
        return llm_snapshot
    return _extract_funding_with_regex(rows=rows)


def _resolve_funding_snapshot(*, store: BoardSeatStore, company: str, force_refresh: bool = False) -> FundingSnapshot:
    manual = _load_manual_funding_seed()
    key = _slug_company(company)
    if key in manual:
        return manual[key]

    cached = store.get_funding_snapshot(company=company)
    if (not force_refresh) and _funding_snapshot_fresh(snapshot=cached, ttl_days=_funding_ttl_days()) and cached is not None:
        return FundingSnapshot(
            history=cached.history,
            latest_round=cached.latest_round,
            latest_date=cached.latest_date,
            backers=cached.backers,
            source_urls=cached.source_urls,
            source_type="cache",
            as_of_utc=cached.as_of_utc,
            confidence=cached.confidence,
            evidence_count=cached.evidence_count,
            distinct_domains=cached.distinct_domains,
            conflict_flags=cached.conflict_flags,
            verification_status=cached.verification_status,
        )

    refreshed_primary = _target_funding_from_crunchbase(company)
    refreshed_web = _refresh_funding_snapshot_from_web(company=company)
    refreshed = _merge_funding_snapshots(refreshed_primary, refreshed_web)
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
            evidence_count=cached.evidence_count,
            distinct_domains=cached.distinct_domains,
            conflict_flags=cached.conflict_flags,
            verification_status=cached.verification_status,
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
    lookback_default = str(_theme_lookback_days() * 24)
    lookback = max(24, min(24 * 120, int(os.environ.get("COATUE_CLAW_BOARD_SEAT_LOOKBACK_HOURS", lookback_default))))
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
    if match:
        title_company = _slug(match.group(1))
        return (not title_company) or (title_company == _slug(company))
    lower = raw.lower()
    if any(pattern in lower for pattern in LEGACY_BOARD_SEAT_PATTERNS):
        if company.lower() in lower or "board seat as a service" in lower:
            return True
    if "board seat as a service" in lower and ("why now" in lower or "target" in lower):
        return True
    return False


def _extract_target_from_message_text(*, company: str, text: str) -> str:
    raw = str(text or "")
    target = _extract_acquisition_target(raw)
    if _is_valid_target_name(company=company, target=target):
        return target
    for line in raw.splitlines():
        plain = line.strip().strip("*")
        if ":" not in plain:
            continue
        label, value = plain.split(":", 1)
        lkey = label.strip().lower()
        if lkey in {"primary", "target", "idea"}:
            candidate = _normalize_text(value, max_chars=100).strip(" .,:;-")
            if _is_valid_target_name(company=company, target=candidate):
                return candidate
    seed = " ".join(raw.splitlines()[:8])
    return _best_effort_target(company=company, seed_text=seed)


def _validate_rendered_message_format(*, company: str, message: str) -> list[str]:
    raw = str(message or "").strip()
    if not raw:
        return ["empty_message"]
    errors: list[str] = []
    first_line = ""
    for line in raw.splitlines():
        if line.strip():
            first_line = line.strip()
            break
    if first_line != f"*Board Seat as a Service — {company}*":
        errors.append("header_mismatch")
    if re.search(r"(?m)^\s*\d+\.\s+", raw):
        errors.append("numbered_heading_disallowed")
    required_tokens = [
        "*Thesis*",
        "*Idea:*",
        "*Target does:*",
        "*Why now:*",
        "*What's different:*",
        "*MOS/risks:*",
        "*Bottom line:*",
        f"*{company} context*",
        "*Current efforts:*",
        "*Domain fit/gaps:*",
        "*Funding snapshot*",
        "*History:*",
        "*Latest round/backers:*",
        "*Sources*",
    ]
    for token in required_tokens:
        if token not in raw:
            errors.append(f"missing_{token.strip('*').lower().replace(' ', '_')}")
    return errors


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
    target_inserted = 0
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
        target = _extract_target_from_message_text(company=company, text=text)
        if target:
            did_target_insert = store.record_target(
                company=company,
                target=target,
                channel_ref=channel_ref,
                channel_id=channel_id,
                source="slack_history_backfill",
                posted_at_utc=posted_at_utc,
                run_date_local=_local_date_for_utc_iso(posted_at_utc),
                message_ts=message_ts,
            )
            if did_target_insert:
                target_inserted += 1
    return {"scanned": len(history), "matched": matched, "inserted": inserted, "target_inserted": target_inserted}


def _fallback_draft(
    *,
    company: str,
    snippets: list[str],
    funding: FundingSnapshot,
    acquisition_rows: list[dict[str, str]] | None = None,
) -> BoardSeatDraft:
    why_now = _normalize_line(snippets[0]) if snippets else f"Over the past month, buying criteria in {company}'s end market shifted toward faster ROI realization."
    whats_different = _normalize_line(snippets[1]) if len(snippets) > 1 else "The target compresses deployment timelines and unlocks differentiated customer outcomes."
    mos_risks = _normalize_line(snippets[2]) if len(snippets) > 2 else "Primary risks are integration complexity, execution sequencing, and procurement delays."
    funding_history, funding_latest_round_backers = _funding_lines_from_snapshot(funding)
    draft = BoardSeatDraft(
        idea_line=_best_effort_idea_line(company=company, seed_text=why_now),
        target_does="",
        why_now=why_now,
        whats_different=whats_different,
        mos_risks=mos_risks,
        bottom_line=f"Execute one target-led move with 12-month milestones tied to revenue velocity and margin quality.",
        context_current_efforts=f"{company} has active customer programs and product pathways where this target can be integrated now.",
        context_domain_fit_gaps="Focus on the highest-friction capability gap where acquisition beats internal build speed.",
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
        "target_does",
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
        target_does=str(payload.get("target_does") or ""),
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


def _assess_draft_quality(
    *,
    company: str,
    draft: BoardSeatDraft,
    evidence_pack: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    thesis_fields = {
        "target_does": _normalize_line_text(str(draft.target_does or "")),
        "why_now": _normalize_line_text(str(draft.why_now or "")),
        "whats_different": _normalize_line_text(str(draft.whats_different or "")),
        "mos_risks": _normalize_line_text(str(draft.mos_risks or "")),
    }
    field_scores: dict[str, float] = {field: 1.0 for field in thesis_fields}
    failed_fields: set[str] = set()
    quality_required_evidence: dict[str, bool] = {
        "target_does": False,
        "why_now": False,
        "whats_different": False,
        "mos_risks": False,
    }
    evidence_tier_mix: dict[str, int] = {"tier_1": 0, "tier_2": 0, "tier_3": 0}
    fact_cards_count_by_field: dict[str, int] = {
        "target_does": 0,
        "why_now": 0,
        "whats_different": 0,
        "mos_risks": 0,
    }
    quote_overlap_by_field: dict[str, float] = {
        "target_does": 0.0,
        "why_now": 0.0,
        "whats_different": 0.0,
        "mos_risks": 0.0,
    }
    why_now_recency_passed = False

    def _penalize(field: str, delta: float, reason: str) -> None:
        field_scores[field] = max(0.0, field_scores.get(field, 1.0) - delta)
        reasons.append(reason)
        if field_scores[field] < _critic_min_field_score():
            failed_fields.add(field)

    for label, line in thesis_fields.items():
        if not line:
            _penalize(label, 0.8, f"missing_line:{label}")
            continue
        if _is_low_signal_copy(line):
            _penalize(label, 0.55, f"artifact_contamination:{label}")
        if re.search(r"<[^>]+>", line):
            _penalize(label, 0.4, f"html_artifact:{label}")
        density = _line_information_density_score(line)
        if len(_tokenize(line)) >= 8 and density < 0.50:
            _penalize(label, 0.2, f"low_information_density:{label}")

    pairs = [("target_does", "why_now"), ("target_does", "whats_different"), ("why_now", "whats_different"), ("whats_different", "mos_risks")]
    for left, right in pairs:
        left_text = thesis_fields.get(left, "")
        right_text = thesis_fields.get(right, "")
        if not left_text or not right_text:
            continue
        sim = _token_jaccard_similarity(left_text, right_text)
        if sim >= 0.82:
            _penalize(left, 0.25, f"near_duplicate:{left}:{right}")
            _penalize(right, 0.25, f"near_duplicate:{left}:{right}")

    def _evidence_blob(items: list[dict[str, Any]]) -> set[str]:
        combined: list[str] = []
        for item in items:
            combined.append(str(item.get("title") or ""))
            combined.append(str(item.get("snippet") or ""))
        return set(_tokenize(_normalize_text(" ".join(combined), max_chars=8000)))

    if isinstance(evidence_pack, dict):
        required_payload = evidence_pack.get("quality_required_evidence")
        if isinstance(required_payload, dict):
            for key in quality_required_evidence:
                quality_required_evidence[key] = bool(required_payload.get(key))
        tier_mix_payload = evidence_pack.get("evidence_tier_mix")
        if isinstance(tier_mix_payload, dict):
            for key in evidence_tier_mix:
                try:
                    evidence_tier_mix[key] = int(tier_mix_payload.get(key) or 0)
                except Exception:
                    evidence_tier_mix[key] = 0
        cards_payload = evidence_pack.get("fact_cards_count_by_field")
        if isinstance(cards_payload, dict):
            for key in fact_cards_count_by_field:
                try:
                    fact_cards_count_by_field[key] = int(cards_payload.get(key) or 0)
                except Exception:
                    fact_cards_count_by_field[key] = 0
        why_now_recency_passed = bool(evidence_pack.get("why_now_recency_passed"))

    strict_evidence_context = sum(int(value) for value in evidence_tier_mix.values()) >= 2
    if strict_evidence_context:
        for field_name, required in quality_required_evidence.items():
            if required:
                continue
            _penalize(field_name, 0.45, f"missing_required_evidence:{field_name}")
        if not why_now_recency_passed:
            _penalize("why_now", 0.35, "why_now_recency_missing")
    else:
        reasons.append("no_evidence_context")

    field_to_bucket = {
        "target_does": "target_does_evidence",
        "why_now": "why_now_evidence",
        "whats_different": "whats_different_evidence",
        "mos_risks": "mos_risks_evidence",
    }
    company_tokens = set(_tokenize(company))
    target_tokens = set(_tokenize(_extract_acquisition_target(draft.idea_line)))
    for field_name, bucket in field_to_bucket.items():
        items = _evidence_items_from_bucket(evidence_pack if isinstance(evidence_pack, dict) else {}, bucket)
        if not items:
            continue
        evidence_tokens = _evidence_blob(items)
        line_tokens = set(_tokenize(thesis_fields.get(field_name, "")))
        if len(line_tokens) < 3:
            continue
        overlap = line_tokens & evidence_tokens
        if len(overlap) >= 2:
            continue
        if (line_tokens & company_tokens) or (line_tokens & target_tokens):
            continue
        _penalize(field_name, 0.2, f"weak_evidence_alignment:{field_name}")

    quote_overlap_by_field = _quote_overlap_by_field(draft=draft, evidence_pack=evidence_pack)
    for field_name, overlap in quote_overlap_by_field.items():
        if overlap > _quote_overlap_max():
            _penalize(field_name, 0.45, f"quote_overlap_high:{field_name}")

    overall = round(sum(field_scores.values()) / float(len(field_scores)), 4) if field_scores else 0.0
    if overall < _critic_min_overall_score():
        reasons.append("critic_overall_below_threshold")
    for field_name, score in field_scores.items():
        if score < _critic_min_field_score():
            failed_fields.add(field_name)
            reasons.append(f"critic_field_below_threshold:{field_name}")

    hard_fail = any(reason.startswith("artifact_contamination") for reason in reasons)
    passed = (overall >= _critic_min_overall_score()) and (not hard_fail) and (not failed_fields)
    return {
        "passed": passed,
        "score": overall,
        "reasons": reasons,
        "reason_codes": _quality_failure_codes_from_reasons(reasons),
        "field_scores": {key: round(float(value), 4) for key, value in field_scores.items()},
        "failed_fields": sorted(failed_fields),
        "quality_required_evidence": quality_required_evidence,
        "evidence_tier_mix": evidence_tier_mix,
        "fact_cards_count_by_field": fact_cards_count_by_field,
        "quote_overlap_by_field": quote_overlap_by_field,
        "why_now_recency_passed": why_now_recency_passed,
    }


def _llm_critic_assess(
    *,
    company: str,
    draft: BoardSeatDraft,
    evidence_pack: dict[str, Any] | None,
) -> dict[str, Any] | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key):
        return None
    model = _review_model()
    client = OpenAI(api_key=api_key)
    draft_payload = {
        "idea_line": draft.idea_line,
        "target_does": draft.target_does,
        "why_now": draft.why_now,
        "whats_different": draft.whats_different,
        "mos_risks": draft.mos_risks,
        "bottom_line": draft.bottom_line,
        "context_current_efforts": draft.context_current_efforts,
        "context_domain_fit_gaps": draft.context_domain_fit_gaps,
    }
    prompt = (
        f"Assess this board-seat draft for {company}.\n"
        "Return strict JSON only with keys: overall_score, field_scores, failed_fields, reasons, reason_codes, synthesized_not_copied.\n"
        "field_scores keys must be: target_does, why_now, whats_different, mos_risks.\n"
        "Scoring rubric (0-1): relevance, non-duplication, factual alignment, specificity, writing clarity.\n"
        "Hard-fail if there is menu/CTA contamination, semantic duplication, obvious evidence mismatch, or quote-like copy from source text.\n"
        "reason_codes should be short machine-friendly tokens (snake_case).\n"
        "synthesized_not_copied must be true only if the draft is clearly synthesized rather than copied snippets.\n"
        f"Draft:\n{json.dumps(draft_payload, ensure_ascii=False)}\n"
        f"Evidence:\n{json.dumps(evidence_pack or {}, ensure_ascii=False)}\n"
    )
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.0,
            messages=[
                {"role": "system", "content": "You are a strict board-writing critic. Return JSON only."},
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
    try:
        overall_score = max(0.0, min(1.0, float(payload.get("overall_score") or 0.0)))
    except Exception:
        overall_score = 0.0
    field_scores_raw = payload.get("field_scores")
    field_scores: dict[str, float] = {}
    if isinstance(field_scores_raw, dict):
        for field_name in ("target_does", "why_now", "whats_different", "mos_risks"):
            try:
                field_scores[field_name] = max(0.0, min(1.0, float(field_scores_raw.get(field_name) or 0.0)))
            except Exception:
                field_scores[field_name] = 0.0
    failed_fields_raw = payload.get("failed_fields")
    failed_fields = [str(item) for item in failed_fields_raw if str(item)] if isinstance(failed_fields_raw, list) else []
    reasons_raw = payload.get("reasons")
    reasons = [str(item) for item in reasons_raw if str(item)] if isinstance(reasons_raw, list) else []
    reason_codes_raw = payload.get("reason_codes")
    reason_codes = [str(item).strip() for item in reason_codes_raw if str(item).strip()] if isinstance(reason_codes_raw, list) else []
    synthesized_not_copied = bool(payload.get("synthesized_not_copied", True))
    passed = (
        overall_score >= _critic_min_overall_score()
        and all(field_scores.get(field, 0.0) >= _critic_min_field_score() for field in ("target_does", "why_now", "whats_different", "mos_risks"))
        and not failed_fields
        and synthesized_not_copied
    )
    return {
        "passed": passed,
        "score": round(overall_score, 4),
        "field_scores": field_scores,
        "failed_fields": failed_fields,
        "reasons": reasons,
        "reason_codes": reason_codes,
    }


def _merge_quality_assessments(
    *,
    deterministic: dict[str, Any],
    critic: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(critic, dict):
        return deterministic
    field_scores = dict(deterministic.get("field_scores") or {})
    critic_field_scores = critic.get("field_scores")
    if isinstance(critic_field_scores, dict):
        for key in ("target_does", "why_now", "whats_different", "mos_risks"):
            if key in field_scores:
                field_scores[key] = round(min(float(field_scores.get(key) or 0.0), float(critic_field_scores.get(key) or 0.0)), 4)
            else:
                field_scores[key] = round(float(critic_field_scores.get(key) or 0.0), 4)
    failed_fields = set(str(item) for item in list(deterministic.get("failed_fields") or []))
    failed_fields.update(str(item) for item in list(critic.get("failed_fields") or []))
    for key, value in field_scores.items():
        if float(value) < _critic_min_field_score():
            failed_fields.add(key)
    reasons = [str(item) for item in list(deterministic.get("reasons") or [])]
    reasons.extend(f"critic:{item}" for item in list(critic.get("reasons") or []))
    reason_codes = [str(item) for item in list(deterministic.get("reason_codes") or [])]
    reason_codes.extend(str(item) for item in list(critic.get("reason_codes") or []))
    overall = round(min(float(deterministic.get("score") or 0.0), float(critic.get("score") or 0.0)), 4)
    passed = bool(deterministic.get("passed")) and bool(critic.get("passed")) and not failed_fields and overall >= _critic_min_overall_score()
    return {
        "passed": passed,
        "score": overall,
        "reasons": reasons,
        "reason_codes": _quality_failure_codes_from_reasons(reason_codes if reason_codes else reasons),
        "field_scores": field_scores,
        "failed_fields": sorted(failed_fields),
        "quality_required_evidence": dict(deterministic.get("quality_required_evidence") or {}),
        "evidence_tier_mix": dict(deterministic.get("evidence_tier_mix") or {}),
        "fact_cards_count_by_field": dict(deterministic.get("fact_cards_count_by_field") or {}),
        "quote_overlap_by_field": dict(deterministic.get("quote_overlap_by_field") or {}),
        "why_now_recency_passed": bool(deterministic.get("why_now_recency_passed")),
    }


def _llm_revise_draft(
    *,
    company: str,
    draft: BoardSeatDraft,
    evidence_pack: dict[str, Any] | None,
    quality_reasons: list[str],
    failed_fields: list[str] | None = None,
) -> BoardSeatDraft | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key):
        return None
    model = _review_model()
    client = OpenAI(api_key=api_key)
    evidence_json = json.dumps(evidence_pack or {}, ensure_ascii=False)
    draft_json = json.dumps(
        {
            "idea_line": draft.idea_line,
            "target_does": draft.target_does,
            "why_now": draft.why_now,
            "whats_different": draft.whats_different,
            "mos_risks": draft.mos_risks,
            "bottom_line": draft.bottom_line,
            "context_current_efforts": draft.context_current_efforts,
            "context_domain_fit_gaps": draft.context_domain_fit_gaps,
            "funding_history": draft.funding_history,
            "funding_latest_round_backers": draft.funding_latest_round_backers,
            "source_refs": [
                {"name_or_publisher": ref.name_or_publisher, "title": ref.title, "url": ref.url}
                for ref in list(draft.source_refs or [])[:6]
            ],
        },
        ensure_ascii=False,
    )
    prompt = (
        f"Revise this board-seat draft for {company}.\n"
        "Return strict JSON only with keys: "
        "idea_line, target_does, why_now, whats_different, mos_risks, bottom_line, "
        "context_current_efforts, context_domain_fit_gaps, funding_history, funding_latest_round_backers, source_refs.\n"
        "Rules:\n"
        "- Keep board-seat style and preserve strategic intent.\n"
        "- Fix only the failing fields where needed.\n"
        "- Remove low-signal CTA/menu artifacts (book a demo, see pricing, sign in, etc).\n"
        "- Avoid near-duplicate lines across thesis fields.\n"
        "- Keep content grounded in evidence but natural (inference allowed).\n"
        "- Synthesize from fact cards; do not copy source wording verbatim.\n"
        "- No HTML tags, UI crumbs, or copied nav snippets.\n"
        f"Failed fields (revise these first): {json.dumps(list(failed_fields or []), ensure_ascii=False)}\n"
        f"Current draft:\n{draft_json}\n"
        f"Quality failures:\n{json.dumps(list(quality_reasons or []), ensure_ascii=False)}\n"
        f"Evidence:\n{evidence_json}\n"
    )
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "You are a quality editor for board-ready strategic writing. Return JSON only."},
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
    parsed = _parse_llm_draft_payload(payload)
    if parsed is None:
        return None
    return replace(
        parsed,
        rewrite_reasons=[*list(draft.rewrite_reasons or []), "quality_rewrite"],
        raw_model_output=text,
    )


def _quality_gate_draft(
    *,
    company: str,
    draft: BoardSeatDraft,
    funding: FundingSnapshot,
    acquisition_rows: list[dict[str, str]],
    evidence_pack: dict[str, Any] | None,
) -> BoardSeatDraft:
    if not _quality_gate_enabled():
        return replace(
            draft,
            quality_gate_passed=True,
            quality_score=1.0,
            quality_reasons=[],
            rewrite_attempts=0,
            quality_fail_stage="",
            quality_field_scores={},
            quality_failed_fields=[],
            quality_failure_codes=[],
            quality_required_evidence={},
            evidence_tier_mix={},
            fact_cards_count_by_field={},
            quote_overlap_by_field={},
            why_now_recency_passed=False,
        )

    current = draft
    attempts = 0
    max_retries = _rewrite_max_retries()
    while True:
        current = _sanitize_draft(company=company, draft=current, funding=funding, acquisition_rows=acquisition_rows)
        validation_errors = _validate_draft(current, company=company, evidence_pack=evidence_pack)
        deterministic = _assess_draft_quality(company=company, draft=current, evidence_pack=evidence_pack)
        critic = _llm_critic_assess(company=company, draft=current, evidence_pack=evidence_pack)
        quality = _merge_quality_assessments(deterministic=deterministic, critic=critic)
        reasons = [*[f"draft_validator:{item}" for item in validation_errors], *list(quality.get("reasons") or [])]
        stage = "draft_validator" if validation_errors else "reviewer"
        if any(str(item).startswith("artifact_contamination") for item in list(quality.get("reasons") or [])):
            stage = "source_filter"
        if (not validation_errors) and bool(quality.get("passed")):
            return replace(
                current,
                quality_gate_passed=True,
                quality_score=float(quality.get("score") or 0.0),
                quality_reasons=list(quality.get("reasons") or []),
                rewrite_attempts=attempts,
                quality_fail_stage="",
                quality_field_scores=dict(quality.get("field_scores") or {}),
                quality_failed_fields=list(quality.get("failed_fields") or []),
                quality_failure_codes=list(quality.get("reason_codes") or []),
                quality_required_evidence=dict(quality.get("quality_required_evidence") or {}),
                evidence_tier_mix=dict(quality.get("evidence_tier_mix") or {}),
                fact_cards_count_by_field=dict(quality.get("fact_cards_count_by_field") or {}),
                quote_overlap_by_field=dict(quality.get("quote_overlap_by_field") or {}),
                why_now_recency_passed=bool(quality.get("why_now_recency_passed")),
            )
        if attempts >= max_retries:
            return replace(
                current,
                quality_gate_passed=False,
                quality_score=float(quality.get("score") or 0.0),
                quality_reasons=reasons,
                rewrite_attempts=attempts,
                quality_fail_stage=stage,
                quality_field_scores=dict(quality.get("field_scores") or {}),
                quality_failed_fields=list(quality.get("failed_fields") or []),
                quality_failure_codes=list(quality.get("reason_codes") or []),
                quality_required_evidence=dict(quality.get("quality_required_evidence") or {}),
                evidence_tier_mix=dict(quality.get("evidence_tier_mix") or {}),
                fact_cards_count_by_field=dict(quality.get("fact_cards_count_by_field") or {}),
                quote_overlap_by_field=dict(quality.get("quote_overlap_by_field") or {}),
                why_now_recency_passed=bool(quality.get("why_now_recency_passed")),
            )
        revised = _llm_revise_draft(
            company=company,
            draft=current,
            evidence_pack=evidence_pack,
            quality_reasons=reasons,
            failed_fields=list(quality.get("failed_fields") or []),
        )
        attempts += 1
        if revised is None:
            revised = replace(current, rewrite_reasons=[*list(current.rewrite_reasons or []), "quality_rewrite_missing"])
        current = revised


def _writer_evidence_payload(evidence_pack: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(evidence_pack, dict):
        return {}
    fact_cards = evidence_pack.get("fact_cards")
    if isinstance(fact_cards, dict) and fact_cards:
        return {
            "fact_cards": fact_cards,
            "fact_cards_count_by_field": dict(evidence_pack.get("fact_cards_count_by_field") or {}),
            "quality_required_evidence": dict(evidence_pack.get("quality_required_evidence") or {}),
            "why_now_recency_passed": bool(evidence_pack.get("why_now_recency_passed")),
            "funding_summary": dict(evidence_pack.get("funding_summary") or {}),
        }
    return {
        "target_does_evidence": list(evidence_pack.get("target_does_evidence") or []),
        "why_now_evidence": list(evidence_pack.get("why_now_evidence") or []),
        "whats_different_evidence": list(evidence_pack.get("whats_different_evidence") or []),
        "mos_risks_evidence": list(evidence_pack.get("mos_risks_evidence") or []),
        "quality_required_evidence": dict(evidence_pack.get("quality_required_evidence") or {}),
        "funding_summary": dict(evidence_pack.get("funding_summary") or {}),
    }


def _llm_draft(
    *,
    company: str,
    snippets: list[str],
    funding: FundingSnapshot,
    evidence_pack: dict[str, Any] | None = None,
    prior_investments: list[str] | None = None,
) -> BoardSeatDraft | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key):
        return None
    model = (os.environ.get("COATUE_CLAW_BOARD_SEAT_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest").strip()
    client = OpenAI(api_key=api_key)
    joined = "\n".join(f"- {line}" for line in snippets[:10]) if snippets else "- no fresh channel snippets"
    evidence_json = json.dumps(
        _writer_evidence_payload(
            evidence_pack
            if evidence_pack is not None
            else _llm_evidence_pack(company=company, target="", target_rows=[], acquisition_rows=[], funding=funding)
        ),
        ensure_ascii=False,
    )
    prompt = (
        f"Generate a structured board-seat brief for {company}.\n"
        "Return strict JSON only with keys: "
        "idea_line, target_does, why_now, whats_different, mos_risks, bottom_line, "
        "context_current_efforts, context_domain_fit_gaps, "
        "funding_history, funding_latest_round_backers, source_refs.\n"
        "Constraints:\n"
        "- each value should be one to two concise sentences.\n"
        "- idea_line must start with Acquire or Acquihire and name a concrete target.\n"
        "- target_does must explain exactly what the target sells and who buys it, using target_does_evidence.\n"
        "- why_now must state a recent catalyst/trend backed by why_now_evidence.\n"
        "- whats_different must state a specific differentiator versus alternatives using whats_different_evidence.\n"
        "- mos_risks must state concrete integration/commercial/execution risks using mos_risks_evidence.\n"
        "- do not propose internal build as primary recommendation.\n"
        "- short, high skim value, decision-useful.\n"
        "- style must be concise labeled-line content, not bullets.\n"
        "- do not use legacy labels (Signal/Board lens/Watchlist/Team ask).\n"
        "- source_refs is an array of objects with name_or_publisher, title, url.\n"
        "- source_refs must prioritize tier_1 evidence and exclude social/wrapper sources.\n"
        "- do not include HTML tags, menu text, or boilerplate snippets.\n"
        "- do not copy source text verbatim; synthesize from fact cards and evidence into original prose.\n"
        "- fields must not repeat each other.\n"
        "- if a field lacks evidence, prefer a cautious but specific fallback instead of copying another field.\n"
        f"Evidence pack (target/acquisition/funding):\n{evidence_json}\n"
        "- keep lines concrete; at most one generic fallback line across thesis/context lines.\n"
        "Recent channel context:\n"
        f"{joined}\n"
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
            target_does=draft.target_does,
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
    store: BoardSeatStore,
    recent_pitches: list[dict[str, Any]] | None = None,
) -> BoardSeatDraft:
    scope = _funding_scope()
    seed_target = _best_effort_target(company=company, seed_text=" ".join(snippets[:3]))
    thematic_rows = _fetch_thematic_context(company=company, target=seed_target, snippets=snippets)
    combined_snippets = [*snippets, *thematic_rows]
    combined_snippets = [_normalize_line(item) for item in combined_snippets if _normalize_line(item)]
    merged_snippets: list[str] = []
    seen: set[str] = set()
    for item in combined_snippets:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        merged_snippets.append(item)
        if len(merged_snippets) >= 12:
            break
    funding_entity = seed_target if scope == "target" else company
    funding = _resolve_funding_snapshot(store=store, company=funding_entity)
    target_rows = _target_search_rows(target=seed_target or company, company=company, snippets=merged_snippets[:4])
    acquisition_rows = _acquisition_search_rows(company=company, snippets=merged_snippets)
    evidence_pack = _llm_evidence_pack(
        company=company,
        target=seed_target or company,
        target_rows=target_rows,
        acquisition_rows=acquisition_rows,
        funding=funding,
    )
    prior_investments = [str(item.get("investment_text") or "").strip() for item in (recent_pitches or [])]
    llm = _llm_draft(
        company=company,
        snippets=merged_snippets,
        funding=funding,
        evidence_pack=evidence_pack,
        prior_investments=prior_investments,
    )
    draft = llm if llm is not None else _fallback_draft(company=company, snippets=merged_snippets, funding=funding, acquisition_rows=acquisition_rows)
    draft = _sanitize_draft(company=company, draft=draft, funding=funding, acquisition_rows=acquisition_rows)
    if scope == "target":
        final_target = _extract_acquisition_target(draft.idea_line)
        if _target_key(final_target) and _target_key(final_target) != _target_key(seed_target):
            target_funding = _resolve_funding_snapshot(store=store, company=final_target)
            draft = _sanitize_draft(company=company, draft=draft, funding=target_funding, acquisition_rows=acquisition_rows)
    gated = _quality_gate_draft(
        company=company,
        draft=draft,
        funding=funding,
        acquisition_rows=acquisition_rows,
        evidence_pack=evidence_pack,
    )
    if gated.quality_gate_passed:
        return gated
    fallback = _fallback_draft(company=company, snippets=merged_snippets, funding=funding, acquisition_rows=acquisition_rows)
    fallback_gated = _quality_gate_draft(
        company=company,
        draft=fallback,
        funding=funding,
        acquisition_rows=acquisition_rows,
        evidence_pack=evidence_pack,
    )
    if fallback_gated.quality_gate_passed:
        return fallback_gated
    if float(fallback_gated.quality_score) > float(gated.quality_score):
        return fallback_gated
    return gated


def _funding_entity_for_draft(*, company: str, draft: BoardSeatDraft) -> str:
    if _funding_scope() != "target":
        return company
    target = _extract_acquisition_target(draft.idea_line)
    return target or company


def _promising_targets_for_company(*, store: BoardSeatStore, company: str, draft: BoardSeatDraft) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def _push(value: str) -> None:
        target = _normalize_source_text(value, max_chars=120)
        key = _target_key(target)
        if (not key) or (key in seen):
            return
        seen.add(key)
        out.append(target)

    _push(_extract_acquisition_target(draft.idea_line))
    company_key = _slug_company(company)
    for target in TARGET_ROTATION_BY_COMPANY.get(company_key, ()):
        _push(target)
    for row in store.target_ledger_rows(company=company, limit=200):
        _push(str(row.get("target") or ""))
    return out[: _target_event_max_targets_per_company()]


def _write_target_ledger(store: BoardSeatStore) -> dict[str, str]:
    rows = store.target_ledger_rows(limit=5000)
    out_dir = _ledger_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "board-seat-target-ledger.csv"
    json_path = out_dir / "board-seat-target-ledger.json"
    fields = [
        "company",
        "target",
        "target_key",
        "first_seen_utc",
        "last_seen_utc",
        "pitch_count",
        "last_channel_ref",
        "last_channel_id",
        "last_source",
        "last_message_ts",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2, ensure_ascii=False, sort_keys=True)

    mirror_csv_path = ""
    mirror_json_path = ""
    if _ledger_mirror_enabled():
        mirror_dir = _ledger_mirror_path()
        mirror_dir.mkdir(parents=True, exist_ok=True)
        mirror_csv = mirror_dir / csv_path.name
        mirror_json = mirror_dir / json_path.name
        mirror_csv.write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")
        mirror_json.write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")
        mirror_csv_path = str(mirror_csv)
        mirror_json_path = str(mirror_json)
    return {
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "mirror_csv_path": mirror_csv_path,
        "mirror_json_path": mirror_json_path,
    }


def _funding_refresh_entities(
    *,
    store: BoardSeatStore,
    all_portcos: bool,
    company: str = "",
    include_recent_targets: bool = True,
) -> list[str]:
    entities: list[str] = []
    seen: set[str] = set()
    explicit_company = _normalize_source_text(company, max_chars=120)
    if explicit_company:
        entities.append(explicit_company)
        seen.add(_target_key(explicit_company))
    elif all_portcos:
        for name, _channel_ref in _parse_portcos():
            key = _target_key(name)
            if key in seen:
                continue
            seen.add(key)
            entities.append(name)
    if include_recent_targets:
        for row in store.target_ledger_rows(limit=5000):
            target = _normalize_source_text(str(row.get("target") or ""), max_chars=120)
            if not target:
                continue
            key = _target_key(target)
            if key in seen:
                continue
            seen.add(key)
            entities.append(target)
    return entities


def _refresh_funding_entities(
    *,
    store: BoardSeatStore,
    entities: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in entities:
        snapshot = _resolve_funding_snapshot(store=store, company=entity, force_refresh=True)
        rows.append(
            {
                "entity": entity,
                "source_type": snapshot.source_type,
                "verification_status": _funding_verification_status(snapshot),
                "confidence_band": _funding_confidence_band(snapshot),
                "confidence": round(float(snapshot.confidence), 4),
                "evidence_count": int(snapshot.evidence_count),
                "distinct_domains": int(snapshot.distinct_domains),
                "conflict_flags": [str(flag) for flag in snapshot.conflict_flags],
                "as_of_utc": snapshot.as_of_utc,
                "unknown": _is_funding_snapshot_unknown(snapshot),
            }
        )
    return rows


def _build_funding_quality_report_markdown(rows: list[dict[str, Any]]) -> str:
    total = len(rows)
    verified = sum(1 for row in rows if str(row.get("verification_status") or "") == "verified")
    low = sum(1 for row in rows if str(row.get("confidence_band") or "") == "low")
    generated_at = _utc_now_iso()
    lines = [
        "# Board Seat Funding Quality Report",
        "",
        f"- Generated at (UTC): `{generated_at}`",
        f"- Total entities: `{total}`",
        f"- Verified: `{verified}` ({round((100.0 * verified / total), 1) if total else 0.0}%)",
        f"- Low confidence: `{low}` ({round((100.0 * low / total), 1) if total else 0.0}%)",
        "",
        "| Entity | Source | Verification | Band | Confidence | Evidence | Domains | Conflicts | As Of (UTC) |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        conflicts = ",".join(str(item) for item in row.get("conflict_flags") or []) or "-"
        lines.append(
            "| {entity} | {source} | {status} | {band} | {confidence} | {evidence} | {domains} | {conflicts} | {as_of} |".format(
                entity=str(row.get("entity") or ""),
                source=str(row.get("source_type") or ""),
                status=str(row.get("verification_status") or ""),
                band=str(row.get("confidence_band") or ""),
                confidence=str(row.get("confidence") or ""),
                evidence=str(row.get("evidence_count") or 0),
                domains=str(row.get("distinct_domains") or 0),
                conflicts=conflicts,
                as_of=str(row.get("as_of_utc") or ""),
            )
        )
    return "\n".join(lines).strip() + "\n"


def _write_funding_quality_report(rows: list[dict[str, Any]]) -> str:
    out_dir = _ledger_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"funding-quality-report-{_today_key()}.md"
    report_path.write_text(_build_funding_quality_report_markdown(rows), encoding="utf-8")
    return str(report_path)


def run_once(*, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    store = BoardSeatStore()
    run_date = _today_key()
    target_lock_days = _target_lock_days()
    portcos = _parse_portcos()
    result: dict[str, Any] = {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "run_date_local": run_date,
        "timezone": str(_timezone()),
        "target_lock_days": target_lock_days,
        "portcos": [{"company": c, "channel_ref": ch} for c, ch in portcos],
        "sent": [],
        "skipped": [],
        "history_backfill": [],
        "event_tracking": [],
        "repitch_assessments": [],
        "ledger": {},
    }

    if WebClient is None and not dry_run:
        raise RuntimeError("slack_sdk is not installed in this environment.")

    clients = [WebClient(token=item) for item in _slack_tokens()] if WebClient is not None else []
    for company, channel_ref in portcos:
        if (not force) and store.already_posted(run_date_local=run_date, company=company):
            result["skipped"].append({"company": company, "channel_ref": channel_ref, "reason": "already_posted_today"})
            continue

        if dry_run and not clients:
            recent_pitches = store.recent_pitches(company=company, limit=12)
            draft = _build_draft(
                company=company,
                snippets=[],
                store=store,
                recent_pitches=recent_pitches,
            )
            initial_target = _extract_acquisition_target(draft.idea_line)
            initial_target_key = _target_key(initial_target)
            hard_locked_hit = (
                store.recent_target_hit(company=company, target_key=initial_target_key, lookback_days=HARD_NO_REPITCH_DAYS)
                if initial_target_key
                else None
            )
            locked_hit = hard_locked_hit
            if (locked_hit is None) and initial_target_key and (target_lock_days > HARD_NO_REPITCH_DAYS) and (not _allow_repeat_targets()):
                locked_hit = store.recent_target_hit(company=company, target_key=initial_target_key, lookback_days=target_lock_days)
            if locked_hit is not None:
                seed = " ".join([draft.idea_line, draft.why_now, draft.whats_different])
                replacement = _best_effort_target(
                    company=company,
                    seed_text=seed,
                    blocked_keys={initial_target_key},
                )
                replacement_key = _target_key(replacement)
                if replacement_key == initial_target_key:
                    result["skipped"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "reason": "repeat_target_within_lock_window",
                            "target": initial_target,
                            "target_key": initial_target_key,
                            "matched_posted_at_utc": str(locked_hit.get("posted_at_utc") or ""),
                        }
                    )
                    continue
                draft = BoardSeatDraft(
                    idea_line=_normalize_line(f"Acquire {replacement} to accelerate {company} execution in a strategic wedge."),
                    target_does=draft.target_does,
                    why_now=draft.why_now,
                    whats_different=draft.whats_different,
                    mos_risks=draft.mos_risks,
                    bottom_line=draft.bottom_line,
                    context_current_efforts=draft.context_current_efforts,
                    context_domain_fit_gaps=draft.context_domain_fit_gaps,
                    funding_history=draft.funding_history,
                    funding_latest_round_backers=draft.funding_latest_round_backers,
                    source_refs=draft.source_refs,
                    raw_model_output=draft.raw_model_output,
                    rewrite_reasons=[*draft.rewrite_reasons, "target_lock_retarget"],
                    target_original=draft.target_original,
                    target_resolution_reason=draft.target_resolution_reason,
                    writing_artifact_cleanups=draft.writing_artifact_cleanups,
                    writing_field_dedup_fixes=draft.writing_field_dedup_fixes,
                    quality_gate_passed=draft.quality_gate_passed,
                    quality_score=draft.quality_score,
                    quality_reasons=draft.quality_reasons,
                    rewrite_attempts=draft.rewrite_attempts,
                    quality_fail_stage=draft.quality_fail_stage,
                    quality_field_scores=draft.quality_field_scores,
                    quality_failed_fields=draft.quality_failed_fields,
                    quality_failure_codes=draft.quality_failure_codes,
                    quality_required_evidence=draft.quality_required_evidence,
                    evidence_tier_mix=draft.evidence_tier_mix,
                    fact_cards_count_by_field=draft.fact_cards_count_by_field,
                    quote_overlap_by_field=draft.quote_overlap_by_field,
                    why_now_recency_passed=draft.why_now_recency_passed,
                )
            result["event_tracking"].append(
                _track_promising_target_events(
                    store=store,
                    company=company,
                    candidate_targets=_promising_targets_for_company(store=store, company=company, draft=draft),
                )
            )
            funding = _resolve_funding_snapshot(store=store, company=_funding_entity_for_draft(company=company, draft=draft))
            draft = _sanitize_draft(company=company, draft=draft, funding=funding, acquisition_rows=[])
            delivery_mode = _quality_delivery_mode_for_draft(draft)
            if (not bool(draft.quality_gate_passed)) and (delivery_mode == "skip"):
                result["skipped"].append(
                    {
                        "company": company,
                        "channel_ref": channel_ref,
                        "reason": "quality_gate_failed",
                        "delivery_mode_applied": "skip",
                        "quality_blocked": True,
                        **_quality_fields_payload(draft),
                    }
                )
                continue
            if (not bool(draft.quality_gate_passed)) and (delivery_mode == "diagnostic_fallback"):
                diagnostic_message = _render_quality_diagnostic_message(company=company, draft=draft)
                result["sent"].append(
                    {
                        "company": company,
                        "channel_ref": channel_ref,
                        "preview": diagnostic_message,
                        "format_version": BOARD_SEAT_FORMAT_VERSION,
                        "funding_source_type": funding.source_type,
                        "funding_as_of_utc": funding.as_of_utc,
                        "funding_unknown": _is_funding_snapshot_unknown(funding),
                        "funding_verification_status": _funding_verification_status(funding),
                        "funding_confidence_band": _funding_confidence_band(funding),
                        "funding_warning": _funding_warning_line(funding),
                        "target": "",
                        "target_confidence": "",
                        "target_confidence_score": 0.0,
                        "target_confidence_reasons": [],
                        "target_validation_reason": "quality_gate_failed",
                        "target_original": draft.target_original,
                        "target_resolution_reason": draft.target_resolution_reason,
                        "writing_mode": _writing_mode(),
                        "writing_artifact_cleanups": list(draft.writing_artifact_cleanups or []),
                        "writing_field_dedup_fixes": list(draft.writing_field_dedup_fixes or []),
                        "delivery_mode_applied": "diagnostic_fallback",
                        "quality_blocked": True,
                        **_quality_fields_payload(draft),
                    }
                )
                continue
            target_gate = _high_conf_new_target_gate(store=store, company=company, draft=draft)
            if not bool(target_gate.get("allow")):
                result["skipped"].append(
                    {
                        "company": company,
                        "channel_ref": channel_ref,
                        "reason": "no_high_confidence_new_target",
                        "target": target_gate.get("target"),
                        "target_key": target_gate.get("target_key"),
                        "target_confidence": target_gate.get("target_confidence"),
                        "target_confidence_score": target_gate.get("target_confidence_score"),
                        "target_confidence_reasons": target_gate.get("target_confidence_reasons"),
                        "target_validation_reason": target_gate.get("target_validation_reason"),
                        "target_original": target_gate.get("target_original"),
                        "target_resolution_reason": target_gate.get("target_resolution_reason"),
                        "writing_mode": target_gate.get("writing_mode"),
                        "writing_artifact_cleanups": target_gate.get("writing_artifact_cleanups"),
                        "writing_field_dedup_fixes": target_gate.get("writing_field_dedup_fixes"),
                        "quality_gate_passed": target_gate.get("quality_gate_passed"),
                        "quality_score": target_gate.get("quality_score"),
                        "quality_reasons": target_gate.get("quality_reasons"),
                        "rewrite_attempts": target_gate.get("rewrite_attempts"),
                        "quality_fail_stage": target_gate.get("quality_fail_stage"),
                        "quality_field_scores": target_gate.get("quality_field_scores"),
                        "quality_failed_fields": target_gate.get("quality_failed_fields"),
                        "quality_failure_codes": target_gate.get("quality_failure_codes"),
                        "quality_required_evidence": target_gate.get("quality_required_evidence"),
                        "evidence_tier_mix": target_gate.get("evidence_tier_mix"),
                        "fact_cards_count_by_field": target_gate.get("fact_cards_count_by_field"),
                        "quote_overlap_by_field": target_gate.get("quote_overlap_by_field"),
                        "delivery_mode_applied": "skip",
                        "quality_blocked": bool(not target_gate.get("quality_gate_passed", True)),
                        "why_now_recency_passed": target_gate.get("why_now_recency_passed"),
                        "is_new_target": bool(target_gate.get("is_new_target")),
                        "gate_reason": target_gate.get("reason"),
                        "matched_posted_at_utc": target_gate.get("matched_posted_at_utc"),
                    }
                )
                continue
            message = _render_board_seat_message(company=company, draft=draft)
            format_errors = _validate_rendered_message_format(company=company, message=message)
            if format_errors:
                fallback = _fallback_draft(company=company, snippets=[], funding=funding, acquisition_rows=[])
                message = _render_board_seat_message(company=company, draft=fallback)
                format_errors = _validate_rendered_message_format(company=company, message=message)
            if format_errors:
                result["skipped"].append(
                    {
                        "company": company,
                        "channel_ref": channel_ref,
                        "reason": "invalid_format_contract",
                        "format_errors": format_errors,
                    }
                )
                continue
            result["sent"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "preview": message,
                    "format_version": BOARD_SEAT_FORMAT_VERSION,
                    "funding_source_type": funding.source_type,
                    "funding_as_of_utc": funding.as_of_utc,
                    "funding_unknown": _is_funding_snapshot_unknown(funding),
                    "funding_verification_status": _funding_verification_status(funding),
                    "funding_confidence_band": _funding_confidence_band(funding),
                    "funding_warning": _funding_warning_line(funding),
                    "target": _extract_acquisition_target(draft.idea_line),
                    "target_confidence": target_gate.get("target_confidence"),
                    "target_confidence_score": target_gate.get("target_confidence_score"),
                    "target_confidence_reasons": target_gate.get("target_confidence_reasons"),
                    "target_validation_reason": target_gate.get("target_validation_reason"),
                    "target_original": target_gate.get("target_original"),
                    "target_resolution_reason": target_gate.get("target_resolution_reason"),
                    "writing_mode": target_gate.get("writing_mode"),
                    "writing_artifact_cleanups": target_gate.get("writing_artifact_cleanups"),
                    "writing_field_dedup_fixes": target_gate.get("writing_field_dedup_fixes"),
                    "delivery_mode_applied": "normal",
                    "quality_blocked": bool(not target_gate.get("quality_gate_passed", True)),
                    "quality_gate_passed": target_gate.get("quality_gate_passed"),
                    "quality_score": target_gate.get("quality_score"),
                    "quality_reasons": target_gate.get("quality_reasons"),
                    "rewrite_attempts": target_gate.get("rewrite_attempts"),
                    "quality_fail_stage": target_gate.get("quality_fail_stage"),
                    "quality_field_scores": target_gate.get("quality_field_scores"),
                    "quality_failed_fields": target_gate.get("quality_failed_fields"),
                    "quality_failure_codes": target_gate.get("quality_failure_codes"),
                    "quality_required_evidence": target_gate.get("quality_required_evidence"),
                    "evidence_tier_mix": target_gate.get("evidence_tier_mix"),
                    "fact_cards_count_by_field": target_gate.get("fact_cards_count_by_field"),
                    "quote_overlap_by_field": target_gate.get("quote_overlap_by_field"),
                    "why_now_recency_passed": target_gate.get("why_now_recency_passed"),
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
                draft = _build_draft(company=company, snippets=snippets, store=store, recent_pitches=recent_pitches)
                initial_target = _extract_acquisition_target(draft.idea_line)
                initial_target_key = _target_key(initial_target)
                locked_hit = (
                    store.recent_target_hit(
                        company=company,
                        target_key=initial_target_key,
                        lookback_days=HARD_NO_REPITCH_DAYS,
                    )
                    if initial_target_key
                    else None
                )
                if (locked_hit is None) and initial_target_key and (target_lock_days > HARD_NO_REPITCH_DAYS) and (not _allow_repeat_targets()):
                    locked_hit = store.recent_target_hit(
                        company=company,
                        target_key=initial_target_key,
                        lookback_days=target_lock_days,
                    )
                if locked_hit is not None:
                    seed = " ".join(
                        [
                            draft.idea_line,
                            draft.why_now,
                            draft.whats_different,
                            " ".join(snippets[:3]),
                        ]
                    )
                    replacement = _best_effort_target(
                        company=company,
                        seed_text=seed,
                        blocked_keys={initial_target_key},
                    )
                    replacement_key = _target_key(replacement)
                    if replacement_key == initial_target_key:
                        result["skipped"].append(
                            {
                                "company": company,
                                "channel_ref": channel_ref,
                                "channel_id": channel_id,
                                "reason": "repeat_target_within_lock_window",
                                "target": initial_target,
                                "target_key": initial_target_key,
                                "matched_posted_at_utc": str(locked_hit.get("posted_at_utc") or ""),
                            }
                        )
                        posted = True
                        break
                    draft = BoardSeatDraft(
                        idea_line=_normalize_line(f"Acquire {replacement} to accelerate {company} execution in a strategic wedge."),
                        target_does=draft.target_does,
                        why_now=draft.why_now,
                        whats_different=draft.whats_different,
                        mos_risks=draft.mos_risks,
                        bottom_line=draft.bottom_line,
                        context_current_efforts=draft.context_current_efforts,
                        context_domain_fit_gaps=draft.context_domain_fit_gaps,
                        funding_history=draft.funding_history,
                        funding_latest_round_backers=draft.funding_latest_round_backers,
                        source_refs=draft.source_refs,
                        raw_model_output=draft.raw_model_output,
                        rewrite_reasons=[*draft.rewrite_reasons, "target_lock_retarget"],
                        target_original=draft.target_original,
                        target_resolution_reason=draft.target_resolution_reason,
                        writing_artifact_cleanups=draft.writing_artifact_cleanups,
                        writing_field_dedup_fixes=draft.writing_field_dedup_fixes,
                        quality_gate_passed=draft.quality_gate_passed,
                        quality_score=draft.quality_score,
                        quality_reasons=draft.quality_reasons,
                        rewrite_attempts=draft.rewrite_attempts,
                        quality_fail_stage=draft.quality_fail_stage,
                        quality_field_scores=draft.quality_field_scores,
                        quality_failed_fields=draft.quality_failed_fields,
                        quality_failure_codes=draft.quality_failure_codes,
                        quality_required_evidence=draft.quality_required_evidence,
                        evidence_tier_mix=draft.evidence_tier_mix,
                        fact_cards_count_by_field=draft.fact_cards_count_by_field,
                        quote_overlap_by_field=draft.quote_overlap_by_field,
                        why_now_recency_passed=draft.why_now_recency_passed,
                    )
                    replacement_funding = _resolve_funding_snapshot(
                        store=store,
                        company=_funding_entity_for_draft(company=company, draft=draft),
                    )
                    draft = _sanitize_draft(
                        company=company,
                        draft=draft,
                        funding=replacement_funding,
                        acquisition_rows=[],
                    )
                result["event_tracking"].append(
                    _track_promising_target_events(
                        store=store,
                        company=company,
                        candidate_targets=_promising_targets_for_company(store=store, company=company, draft=draft),
                    )
                )
                funding = _resolve_funding_snapshot(store=store, company=_funding_entity_for_draft(company=company, draft=draft))
                draft = _sanitize_draft(company=company, draft=draft, funding=funding, acquisition_rows=[])
                delivery_mode = _quality_delivery_mode_for_draft(draft)
                if (not bool(draft.quality_gate_passed)) and (delivery_mode == "skip"):
                    result["skipped"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "reason": "quality_gate_failed",
                            "delivery_mode_applied": "skip",
                            "quality_blocked": True,
                            **_quality_fields_payload(draft),
                        }
                    )
                    posted = True
                    break
                if (not bool(draft.quality_gate_passed)) and (delivery_mode == "diagnostic_fallback"):
                    diagnostic_message = _render_quality_diagnostic_message(company=company, draft=draft)
                    if dry_run:
                        result["sent"].append(
                            {
                                "company": company,
                                "channel_ref": channel_ref,
                                "channel_id": channel_id,
                                "preview": diagnostic_message,
                                "target": "",
                                "significant_change": False,
                                "format_version": BOARD_SEAT_FORMAT_VERSION,
                                "funding_source_type": funding.source_type,
                                "funding_as_of_utc": funding.as_of_utc,
                                "funding_unknown": _is_funding_snapshot_unknown(funding),
                                "funding_verification_status": _funding_verification_status(funding),
                                "funding_confidence_band": _funding_confidence_band(funding),
                                "funding_warning": _funding_warning_line(funding),
                                "is_repitch": False,
                                "repitch_of_pitch_id": None,
                                "repitch_prev_posted_at_utc": None,
                                "repitch_similarity": 0.0,
                                "repitch_new_evidence": [],
                                "target_confidence": "",
                                "target_confidence_score": 0.0,
                                "target_confidence_reasons": [],
                                "target_validation_reason": "quality_gate_failed",
                                "target_original": draft.target_original,
                                "target_resolution_reason": draft.target_resolution_reason,
                                "writing_mode": _writing_mode(),
                                "writing_artifact_cleanups": list(draft.writing_artifact_cleanups or []),
                                "writing_field_dedup_fixes": list(draft.writing_field_dedup_fixes or []),
                                "delivery_mode_applied": "diagnostic_fallback",
                                "quality_blocked": True,
                                **_quality_fields_payload(draft),
                            }
                        )
                        posted = True
                        break
                    post = client.chat_postMessage(channel=channel_id, text=diagnostic_message)
                    ts = str(post.get("ts") or "")
                    store.record_post(
                        run_date_local=run_date,
                        company=company,
                        channel_ref=channel_ref,
                        channel_id=channel_id,
                        message_ts=(ts or None),
                        summary=diagnostic_message,
                    )
                    diagnostic_signature = _token_signature(
                        _normalize_text("quality diagnostic block", max_chars=120),
                        max_tokens=20,
                    )
                    store.record_pitch(
                        company=company,
                        channel_ref=channel_ref,
                        channel_id=channel_id,
                        source="quality_diagnostic_post",
                        message_ts=(ts or None),
                        run_date_local=run_date,
                        posted_at_utc=_iso_from_slack_ts(ts),
                        message_text=diagnostic_message,
                        investment_text="quality diagnostic block",
                        investment_hash=_stable_hash(f"{company}:{run_date}:quality_diagnostic"),
                        investment_signature=diagnostic_signature,
                        context_signature="quality_diagnostic",
                        context_snippets=[],
                        significant_change=False,
                    )
                    result["sent"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "ts": ts,
                            "target": "",
                            "significant_change": False,
                            "format_version": BOARD_SEAT_FORMAT_VERSION,
                            "funding_source_type": funding.source_type,
                            "funding_as_of_utc": funding.as_of_utc,
                            "funding_unknown": _is_funding_snapshot_unknown(funding),
                            "funding_verification_status": _funding_verification_status(funding),
                            "funding_confidence_band": _funding_confidence_band(funding),
                            "funding_warning": _funding_warning_line(funding),
                            "is_repitch": False,
                            "repitch_of_pitch_id": None,
                            "repitch_prev_posted_at_utc": None,
                            "repitch_similarity": 0.0,
                            "repitch_new_evidence": [],
                            "target_confidence": "",
                            "target_confidence_score": 0.0,
                            "target_confidence_reasons": [],
                            "target_validation_reason": "quality_gate_failed",
                            "target_original": draft.target_original,
                            "target_resolution_reason": draft.target_resolution_reason,
                            "writing_mode": _writing_mode(),
                            "writing_artifact_cleanups": list(draft.writing_artifact_cleanups or []),
                            "writing_field_dedup_fixes": list(draft.writing_field_dedup_fixes or []),
                            "delivery_mode_applied": "diagnostic_fallback",
                            "quality_blocked": True,
                            **_quality_fields_payload(draft),
                        }
                    )
                    posted = True
                    break
                target_gate = _high_conf_new_target_gate(store=store, company=company, draft=draft)
                if not bool(target_gate.get("allow")):
                    result["skipped"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "reason": "no_high_confidence_new_target",
                            "target": target_gate.get("target"),
                            "target_key": target_gate.get("target_key"),
                            "target_confidence": target_gate.get("target_confidence"),
                            "target_confidence_score": target_gate.get("target_confidence_score"),
                            "target_confidence_reasons": target_gate.get("target_confidence_reasons"),
                            "target_validation_reason": target_gate.get("target_validation_reason"),
                            "target_original": target_gate.get("target_original"),
                            "target_resolution_reason": target_gate.get("target_resolution_reason"),
                            "writing_mode": target_gate.get("writing_mode"),
                            "writing_artifact_cleanups": target_gate.get("writing_artifact_cleanups"),
                            "writing_field_dedup_fixes": target_gate.get("writing_field_dedup_fixes"),
                            "quality_gate_passed": target_gate.get("quality_gate_passed"),
                            "quality_score": target_gate.get("quality_score"),
                            "quality_reasons": target_gate.get("quality_reasons"),
                            "rewrite_attempts": target_gate.get("rewrite_attempts"),
                            "quality_fail_stage": target_gate.get("quality_fail_stage"),
                            "quality_field_scores": target_gate.get("quality_field_scores"),
                            "quality_failed_fields": target_gate.get("quality_failed_fields"),
                            "quality_failure_codes": target_gate.get("quality_failure_codes"),
                            "quality_required_evidence": target_gate.get("quality_required_evidence"),
                            "evidence_tier_mix": target_gate.get("evidence_tier_mix"),
                            "fact_cards_count_by_field": target_gate.get("fact_cards_count_by_field"),
                            "quote_overlap_by_field": target_gate.get("quote_overlap_by_field"),
                            "delivery_mode_applied": "skip",
                            "quality_blocked": bool(not target_gate.get("quality_gate_passed", True)),
                            "why_now_recency_passed": target_gate.get("why_now_recency_passed"),
                            "is_new_target": bool(target_gate.get("is_new_target")),
                            "gate_reason": target_gate.get("reason"),
                            "matched_posted_at_utc": target_gate.get("matched_posted_at_utc"),
                        }
                    )
                    posted = True
                    break
                message = _render_board_seat_message(company=company, draft=draft)
                format_errors = _validate_rendered_message_format(company=company, message=message)
                if format_errors:
                    fallback = _fallback_draft(company=company, snippets=snippets, funding=funding, acquisition_rows=[])
                    message = _render_board_seat_message(company=company, draft=fallback)
                    format_errors = _validate_rendered_message_format(company=company, message=message)
                if format_errors:
                    result["skipped"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "reason": "invalid_format_contract",
                            "format_errors": format_errors,
                        }
                    )
                    posted = True
                    break
                investment_text = _extract_investment_text(message)
                core_investment_text = _core_investment_text(message)
                investment_signature = _token_signature(core_investment_text)
                signal_signature = _signal_signature_from_investment(investment_text)
                signal_text = _signal_text_from_investment(investment_text)
                investment_hash = _stable_hash(investment_signature or core_investment_text or investment_text)
                context_signature = _context_signature_from_snippets(snippets)
                is_repitch = False
                repitch_of_pitch_id: int | None = None
                repitch_prev_posted_at_utc: str | None = None
                repitch_similarity = 0.0
                repitch_new_evidence: list[str] = []
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
                if repeated and significant_change:
                    current_target = _extract_target_from_message_text(company=company, text=message)
                    latest_target_pitch = store.latest_target_pitch(
                        company=company,
                        target_key=_target_key(current_target),
                    )
                    prior_pitch_posted_at_utc = str(
                        (matched_pitch or {}).get("posted_at_utc")
                        or (latest_target_pitch or {}).get("posted_at_utc")
                        or ""
                    )
                    if not prior_pitch_posted_at_utc:
                        result["skipped"].append(
                            {
                                "company": company,
                                "channel_ref": channel_ref,
                                "channel_id": channel_id,
                                "reason": "repitch_missing_prior_anchor",
                            }
                        )
                        posted = True
                        break
                    repitch_assessment = _assess_repitch_significance(
                        store=store,
                        company=company,
                        target=current_target,
                        prior_pitch_posted_at_utc=prior_pitch_posted_at_utc,
                    )
                    result["repitch_assessments"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "target": current_target,
                            **repitch_assessment,
                        }
                    )
                    if not bool(repitch_assessment.get("allow")):
                        result["skipped"].append(
                            {
                                "company": company,
                                "channel_ref": channel_ref,
                                "channel_id": channel_id,
                                "reason": "repitch_not_significant_enough",
                                "target": current_target,
                                "assessment_id": repitch_assessment.get("assessment_id"),
                                "assessment_reason": repitch_assessment.get("reason"),
                                "aggregate_score": repitch_assessment.get("aggregate_score"),
                                "max_event_score": repitch_assessment.get("max_event_score"),
                                "distinct_domains": repitch_assessment.get("distinct_domains"),
                                "note": REPITCH_DISCOURAGE_TEXT,
                            }
                        )
                        posted = True
                        break
                    repitch_note, repitch_evidence = _repitch_disclosure_from_assessment(
                        prior_pitch_posted_at_utc=prior_pitch_posted_at_utc,
                        top_events=list(repitch_assessment.get("top_events") or []),
                    )
                    draft = BoardSeatDraft(
                        idea_line=draft.idea_line,
                        target_does=draft.target_does,
                        why_now=draft.why_now,
                        whats_different=draft.whats_different,
                        mos_risks=draft.mos_risks,
                        bottom_line=draft.bottom_line,
                        context_current_efforts=draft.context_current_efforts,
                        context_domain_fit_gaps=draft.context_domain_fit_gaps,
                        funding_history=draft.funding_history,
                        funding_latest_round_backers=draft.funding_latest_round_backers,
                        funding_warning=draft.funding_warning,
                        repitch_note=repitch_note,
                        repitch_new_evidence=repitch_evidence,
                        source_refs=draft.source_refs,
                        raw_model_output=draft.raw_model_output,
                        rewrite_reasons=[*draft.rewrite_reasons, "repitch_disclosure"],
                        target_original=draft.target_original,
                        target_resolution_reason=draft.target_resolution_reason,
                        writing_artifact_cleanups=draft.writing_artifact_cleanups,
                        writing_field_dedup_fixes=draft.writing_field_dedup_fixes,
                        quality_gate_passed=draft.quality_gate_passed,
                        quality_score=draft.quality_score,
                        quality_reasons=draft.quality_reasons,
                        rewrite_attempts=draft.rewrite_attempts,
                        quality_fail_stage=draft.quality_fail_stage,
                        quality_field_scores=draft.quality_field_scores,
                        quality_failed_fields=draft.quality_failed_fields,
                        quality_failure_codes=draft.quality_failure_codes,
                        quality_required_evidence=draft.quality_required_evidence,
                        evidence_tier_mix=draft.evidence_tier_mix,
                        fact_cards_count_by_field=draft.fact_cards_count_by_field,
                        quote_overlap_by_field=draft.quote_overlap_by_field,
                        why_now_recency_passed=draft.why_now_recency_passed,
                    )
                    message = _render_board_seat_message(company=company, draft=draft)
                    investment_text = _extract_investment_text(message)
                    core_investment_text = _core_investment_text(message)
                    investment_signature = _token_signature(core_investment_text)
                    signal_signature = _signal_signature_from_investment(investment_text)
                    signal_text = _signal_text_from_investment(investment_text)
                    investment_hash = _stable_hash(investment_signature or core_investment_text or investment_text)
                    is_repitch = True
                    repitch_of_pitch_id = int((matched_pitch or {}).get("id")) if (matched_pitch or {}).get("id") is not None else None
                    repitch_prev_posted_at_utc = prior_pitch_posted_at_utc
                    repitch_similarity = float(similarity)
                    repitch_new_evidence = [
                        str(item.get("title") or "").strip()
                        for item in list(repitch_assessment.get("top_events") or [])[:2]
                        if str(item.get("title") or "").strip()
                    ]
                if dry_run:
                    current_target = _extract_target_from_message_text(company=company, text=message)
                    result["sent"].append(
                        {
                            "company": company,
                            "channel_ref": channel_ref,
                            "channel_id": channel_id,
                            "preview": message,
                            "target": current_target,
                            "significant_change": bool(significant_change),
                            "format_version": BOARD_SEAT_FORMAT_VERSION,
                            "funding_source_type": funding.source_type,
                            "funding_as_of_utc": funding.as_of_utc,
                            "funding_unknown": _is_funding_snapshot_unknown(funding),
                            "funding_verification_status": _funding_verification_status(funding),
                            "funding_confidence_band": _funding_confidence_band(funding),
                            "funding_warning": _funding_warning_line(funding),
                            "is_repitch": bool(is_repitch),
                            "repitch_of_pitch_id": repitch_of_pitch_id,
                            "repitch_prev_posted_at_utc": repitch_prev_posted_at_utc,
                            "repitch_similarity": round(float(repitch_similarity), 3),
                            "repitch_new_evidence": repitch_new_evidence,
                            "target_confidence": target_gate.get("target_confidence"),
                            "target_confidence_score": target_gate.get("target_confidence_score"),
                            "target_confidence_reasons": target_gate.get("target_confidence_reasons"),
                            "target_validation_reason": target_gate.get("target_validation_reason"),
                            "target_original": target_gate.get("target_original"),
                            "target_resolution_reason": target_gate.get("target_resolution_reason"),
                            "writing_mode": target_gate.get("writing_mode"),
                            "writing_artifact_cleanups": target_gate.get("writing_artifact_cleanups"),
                            "writing_field_dedup_fixes": target_gate.get("writing_field_dedup_fixes"),
                            "delivery_mode_applied": "normal",
                            "quality_blocked": bool(not target_gate.get("quality_gate_passed", True)),
                            "quality_gate_passed": target_gate.get("quality_gate_passed"),
                            "quality_score": target_gate.get("quality_score"),
                            "quality_reasons": target_gate.get("quality_reasons"),
                            "rewrite_attempts": target_gate.get("rewrite_attempts"),
                            "quality_fail_stage": target_gate.get("quality_fail_stage"),
                            "quality_field_scores": target_gate.get("quality_field_scores"),
                            "quality_failed_fields": target_gate.get("quality_failed_fields"),
                            "quality_failure_codes": target_gate.get("quality_failure_codes"),
                            "quality_required_evidence": target_gate.get("quality_required_evidence"),
                            "evidence_tier_mix": target_gate.get("evidence_tier_mix"),
                            "fact_cards_count_by_field": target_gate.get("fact_cards_count_by_field"),
                            "quote_overlap_by_field": target_gate.get("quote_overlap_by_field"),
                            "why_now_recency_passed": target_gate.get("why_now_recency_passed"),
                        }
                    )
                    posted = True
                    break
                blocks = _render_board_seat_blocks(company=company, draft=draft) if _header_style() == "richtext" else None
                if blocks:
                    try:
                        post = client.chat_postMessage(channel=channel_id, text=message, blocks=blocks)
                    except SlackApiError as exc:
                        response = getattr(exc, "response", None)
                        err = str(response.get("error") or "") if isinstance(response, dict) else ""
                        if err in {"invalid_blocks", "invalid_arguments"}:
                            post = client.chat_postMessage(channel=channel_id, text=message)
                        else:
                            raise
                else:
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
                    is_repitch=bool(is_repitch),
                    repitch_of_pitch_id=repitch_of_pitch_id,
                    repitch_prev_posted_at_utc=repitch_prev_posted_at_utc,
                    repitch_similarity=float(repitch_similarity),
                    repitch_new_evidence=repitch_new_evidence,
                )
                current_target = _extract_target_from_message_text(company=company, text=message)
                store.record_target(
                    company=company,
                    target=current_target,
                    channel_ref=channel_ref,
                    channel_id=channel_id,
                    source="live_post",
                    posted_at_utc=_iso_from_slack_ts(ts),
                    run_date_local=run_date,
                    message_ts=(ts or None),
                )
                result["sent"].append(
                    {
                        "company": company,
                        "channel_ref": channel_ref,
                        "channel_id": channel_id,
                        "ts": ts,
                        "target": current_target,
                        "significant_change": bool(significant_change),
                        "format_version": BOARD_SEAT_FORMAT_VERSION,
                        "funding_source_type": funding.source_type,
                        "funding_as_of_utc": funding.as_of_utc,
                        "funding_unknown": _is_funding_snapshot_unknown(funding),
                        "funding_verification_status": _funding_verification_status(funding),
                        "funding_confidence_band": _funding_confidence_band(funding),
                        "funding_warning": _funding_warning_line(funding),
                        "is_repitch": bool(is_repitch),
                        "repitch_of_pitch_id": repitch_of_pitch_id,
                        "repitch_prev_posted_at_utc": repitch_prev_posted_at_utc,
                        "repitch_similarity": round(float(repitch_similarity), 3),
                        "repitch_new_evidence": repitch_new_evidence,
                        "target_confidence": target_gate.get("target_confidence"),
                        "target_confidence_score": target_gate.get("target_confidence_score"),
                        "target_confidence_reasons": target_gate.get("target_confidence_reasons"),
                        "target_validation_reason": target_gate.get("target_validation_reason"),
                        "target_original": target_gate.get("target_original"),
                        "target_resolution_reason": target_gate.get("target_resolution_reason"),
                        "writing_mode": target_gate.get("writing_mode"),
                        "writing_artifact_cleanups": target_gate.get("writing_artifact_cleanups"),
                        "writing_field_dedup_fixes": target_gate.get("writing_field_dedup_fixes"),
                        "delivery_mode_applied": "normal",
                        "quality_blocked": bool(not target_gate.get("quality_gate_passed", True)),
                        "quality_gate_passed": target_gate.get("quality_gate_passed"),
                        "quality_score": target_gate.get("quality_score"),
                        "quality_reasons": target_gate.get("quality_reasons"),
                        "rewrite_attempts": target_gate.get("rewrite_attempts"),
                        "quality_fail_stage": target_gate.get("quality_fail_stage"),
                        "quality_field_scores": target_gate.get("quality_field_scores"),
                        "quality_failed_fields": target_gate.get("quality_failed_fields"),
                        "quality_failure_codes": target_gate.get("quality_failure_codes"),
                        "quality_required_evidence": target_gate.get("quality_required_evidence"),
                        "evidence_tier_mix": target_gate.get("evidence_tier_mix"),
                        "fact_cards_count_by_field": target_gate.get("fact_cards_count_by_field"),
                        "quote_overlap_by_field": target_gate.get("quote_overlap_by_field"),
                        "why_now_recency_passed": target_gate.get("why_now_recency_passed"),
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

    for row in list(result.get("sent") or []):
        if not isinstance(row, dict):
            continue
        defaults = _quality_fields_default(passed=True, stage="")
        for key, value in defaults.items():
            row.setdefault(key, value)
    for row in list(result.get("skipped") or []):
        if not isinstance(row, dict):
            continue
        stage = "reviewer" if str(row.get("reason") or "") == "quality_gate_failed" else "not_run"
        defaults = _quality_fields_default(passed=False, stage=stage)
        for key, value in defaults.items():
            row.setdefault(key, value)

    try:
        result["ledger"] = _write_target_ledger(store)
    except Exception as exc:
        result["ledger"] = {"error": str(exc)}
    try:
        _persist_quality_run_metrics(result)
    except Exception:
        pass

    return result


def status() -> dict[str, Any]:
    store = BoardSeatStore()
    portcos = [{"company": c, "channel_ref": ch} for c, ch in _parse_portcos()]
    manual_seed = _load_manual_funding_seed()
    funding_age_days: dict[str, float | None] = {}
    funding_source_by_company: dict[str, str] = {}
    funding_verification_by_company: dict[str, dict[str, Any]] = {}
    verified_count = 0
    low_conf_count = 0
    oldest_cache_age_days = 0.0
    total_companies = len(portcos)
    quality_metrics: dict[str, Any] = {
        "run_date_local": "",
        "recorded_at_utc": "",
        "quality_fail_count": 0,
        "avg_rewrite_attempts": 0.0,
        "top_failure_reasons": [],
        "quality_pass_rate_7d": 0.0,
        "top_failed_fields_7d": [],
        "avg_rewrite_attempts_7d": 0.0,
        "diagnostic_fallback_count_7d": 0,
        "top_quality_failure_codes_7d": [],
        "quote_overlap_violations_7d": 0,
        "fact_card_coverage_7d": 0.0,
    }
    quality_path = _quality_metrics_path()
    if quality_path.exists():
        try:
            payload = json.loads(quality_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                quality_metrics = {
                    "run_date_local": str(payload.get("run_date_local") or ""),
                    "recorded_at_utc": str(payload.get("recorded_at_utc") or ""),
                    "quality_fail_count": int(payload.get("quality_fail_count") or 0),
                    "avg_rewrite_attempts": float(payload.get("avg_rewrite_attempts") or 0.0),
                    "top_failure_reasons": list(payload.get("top_failure_reasons") or []),
                    "quality_pass_rate_7d": float(payload.get("quality_pass_rate_7d") or 0.0),
                    "top_failed_fields_7d": list(payload.get("top_failed_fields_7d") or []),
                    "avg_rewrite_attempts_7d": float(payload.get("avg_rewrite_attempts_7d") or 0.0),
                    "diagnostic_fallback_count_7d": int(payload.get("diagnostic_fallback_count_7d") or 0),
                    "top_quality_failure_codes_7d": list(payload.get("top_quality_failure_codes_7d") or []),
                    "quote_overlap_violations_7d": int(payload.get("quote_overlap_violations_7d") or 0),
                    "fact_card_coverage_7d": float(payload.get("fact_card_coverage_7d") or 0.0),
                }
        except Exception:
            pass
    for item in portcos:
        company = item["company"]
        manual_snapshot = manual_seed.get(_slug_company(company))
        snapshot: FundingSnapshot | None = manual_snapshot
        if manual_snapshot is not None:
            funding_age_days[company] = 0.0
            funding_source_by_company[company] = "manual_seed"
        else:
            funding_age_days[company] = store.funding_cache_age_days(company=company)
            snapshot = store.get_funding_snapshot(company=company)
            funding_source_by_company[company] = str(snapshot.source_type).strip() if snapshot is not None else "unknown"
        age = funding_age_days[company]
        if age is not None:
            oldest_cache_age_days = max(oldest_cache_age_days, float(age))
        if snapshot is None:
            low_conf_count += 1
            funding_verification_by_company[company] = {
                "source_type": "unknown",
                "verification_status": "weak",
                "confidence_band": "low",
                "confidence": 0.0,
                "evidence_count": 0,
                "distinct_domains": 0,
                "conflict_flags": [],
            }
            continue
        verification_status = _funding_verification_status(snapshot)
        confidence_band = _funding_confidence_band(snapshot)
        if verification_status == "verified":
            verified_count += 1
        if confidence_band == "low":
            low_conf_count += 1
        funding_verification_by_company[company] = {
            "source_type": str(snapshot.source_type or "unknown").strip() or "unknown",
            "verification_status": verification_status,
            "confidence_band": confidence_band,
            "confidence": round(float(snapshot.confidence), 4),
            "evidence_count": int(snapshot.evidence_count),
            "distinct_domains": int(snapshot.distinct_domains),
            "conflict_flags": [str(flag) for flag in snapshot.conflict_flags],
        }
    verified_pct = round((100.0 * verified_count / float(total_companies)), 1) if total_companies else 0.0
    low_conf_pct = round((100.0 * low_conf_count / float(total_companies)), 1) if total_companies else 0.0
    return {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "timezone": str(_timezone()),
        "run_date_local": _today_key(),
        "target_lock_days": _target_lock_days(),
        "hard_no_repitch_days": HARD_NO_REPITCH_DAYS,
        "require_high_conf_new_target": _require_high_conf_new_target(),
        "allow_medium_new_target": _allow_medium_new_target(),
        "target_confidence_model": _target_confidence_model(),
        "target_confidence_high_min": _target_confidence_high_min(),
        "target_confidence_medium_min": _target_confidence_medium_min(),
        "max_line_words": _max_line_words(),
        "writing_mode": _writing_mode(),
        "strip_obvious_artifacts": _strip_obvious_artifacts(),
        "quality_gate_enabled": _quality_gate_enabled(),
        "rewrite_max_retries": _rewrite_max_retries(),
        "source_gate_mode": _source_gate_mode(),
        "quality_fail_policy": _quality_fail_policy(),
        "delivery_mode": _delivery_mode(),
        "fact_card_mode": _fact_card_mode(),
        "quote_overlap_max": _quote_overlap_max(),
        "diagnostic_max_reasons": _diagnostic_max_reasons(),
        "diagnostic_include_urls": _diagnostic_include_urls(),
        "source_policy": _source_policy(),
        "why_now_recency_days": _why_now_recency_days(),
        "critic_min_field_score": _critic_min_field_score(),
        "critic_min_overall_score": _critic_min_overall_score(),
        "evidence_fetch_enabled": _evidence_fetch_enabled(),
        "evidence_fetch_timeout_ms": _evidence_fetch_timeout_ms(),
        "evidence_max_urls": _evidence_max_urls(),
        "review_model": _review_model(),
        "portcos": portcos,
        "recent_runs": store.recent_runs(limit=20),
        "pitch_counts": {
            "total": store.pitch_count(),
            "by_company": {
                item["company"]: store.pitch_count(company=item["company"])
                for item in portcos
            },
        },
        "target_memory_counts": {
            "total": store.target_memory_count(),
            "by_company": {
                item["company"]: store.target_memory_count(company=item["company"])
                for item in portcos
            },
        },
        "funding_cache_age_days_by_company": funding_age_days,
        "funding_data_source_by_company": funding_source_by_company,
        "funding_verification_by_company": funding_verification_by_company,
        "funding_quality_metrics": {
            "total_companies": total_companies,
            "verified_count": verified_count,
            "verified_pct": verified_pct,
            "low_confidence_count": low_conf_count,
            "low_confidence_pct": low_conf_pct,
            "oldest_cache_age_days": round(oldest_cache_age_days, 2),
        },
        "last_quality_run_metrics": quality_metrics,
        "quality_pass_rate_7d": quality_metrics.get("quality_pass_rate_7d", 0.0),
        "top_failed_fields_7d": quality_metrics.get("top_failed_fields_7d", []),
        "avg_rewrite_attempts_7d": quality_metrics.get("avg_rewrite_attempts_7d", 0.0),
        "diagnostic_fallback_count_7d": quality_metrics.get("diagnostic_fallback_count_7d", 0),
        "top_quality_failure_codes_7d": quality_metrics.get("top_quality_failure_codes_7d", []),
        "quote_overlap_violations_7d": quality_metrics.get("quote_overlap_violations_7d", 0),
        "fact_card_coverage_7d": quality_metrics.get("fact_card_coverage_7d", 0.0),
        "ledger_paths": {
            "artifact_dir": str(_ledger_dir()),
            "mirror_dir": str(_ledger_mirror_path()) if _ledger_mirror_enabled() else "",
        },
    }


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

    funding_report = sub.add_parser("funding-quality-report")
    funding_report.add_argument("--all-portcos", action="store_true")
    funding_report.add_argument("--company", default="")
    funding_report.add_argument("--include-recent-targets", action="store_true")

    args = parser.parse_args()
    if args.command == "run-once":
        payload = run_once(force=bool(args.force), dry_run=bool(args.dry_run))
    elif args.command == "status":
        payload = status()
    elif args.command == "seed-target":
        store = BoardSeatStore()
        now_iso = _utc_now_iso()
        target = _normalize_text(args.target, max_chars=120).strip()
        inserted = store.record_target(
            company=args.company,
            target=target,
            channel_ref=args.channel_ref,
            channel_id=None,
            source="manual_seed",
            posted_at_utc=now_iso,
            run_date_local=_today_key(),
            message_ts=None,
        )
        payload = {
            "ok": True,
            "inserted": bool(inserted),
            "company": args.company,
            "target": target,
            "target_key": _target_key(target),
            "posted_at_utc": now_iso,
        }
    elif args.command == "export-ledger":
        store = BoardSeatStore()
        ledger = _write_target_ledger(store)
        rows = store.target_ledger_rows(company=args.company or None, limit=5000)
        payload = {
            "ok": True,
            "ledger": ledger,
            "rows": rows,
            "count": len(rows),
            "company_filter": args.company or "",
        }
    elif args.command == "refresh-funding":
        store = BoardSeatStore()
        include_targets = bool(args.include_recent_targets or args.all_portcos)
        entities = _funding_refresh_entities(
            store=store,
            all_portcos=bool(args.all_portcos),
            company=str(args.company or ""),
            include_recent_targets=include_targets,
        )
        rows = _refresh_funding_entities(store=store, entities=entities)
        payload = {
            "ok": True,
            "entities_refreshed": len(rows),
            "entities": entities,
            "rows": rows,
        }
    elif args.command == "funding-quality-report":
        store = BoardSeatStore()
        include_targets = bool(args.include_recent_targets or args.all_portcos)
        entities = _funding_refresh_entities(
            store=store,
            all_portcos=bool(args.all_portcos),
            company=str(args.company or ""),
            include_recent_targets=include_targets,
        )
        rows = _refresh_funding_entities(store=store, entities=entities)
        report_path = _write_funding_quality_report(rows)
        payload = {
            "ok": True,
            "report_path": report_path,
            "entities": entities,
            "rows": rows,
            "count": len(rows),
        }
    else:
        store = BoardSeatStore()
        rows = store.target_ledger_rows(company=args.company or None, limit=max(1, min(5000, int(args.limit))))
        payload = {
            "ok": True,
            "target_lock_days": _target_lock_days(),
            "company_filter": args.company or "",
            "count": len(rows),
            "rows": rows,
        }
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

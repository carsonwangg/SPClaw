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
from urllib.parse import urlencode
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
BOARD_SEAT_FORMAT_VERSION = "v2_thesis_context_funding"
MAX_TOTAL_BULLETS = 6
MAX_THESIS_BULLETS = 2
MAX_CONTEXT_BULLETS = 2
MAX_FUNDING_BULLETS = 2
MAX_BULLET_WORDS = 20
FUNDING_CACHE_TTL_DAYS_DEFAULT = 14
UNKNOWN_FUNDING_TEXT = "Funding details are currently unavailable."
WEB_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
BRAVE_SEARCH_RESULTS = 5
FUNDING_EXTRACT_MODEL = "gpt-5.2-chat-latest"


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
class BoardSeatDraft:
    thesis_bullets: list[str]
    context_bullets: list[str]
    funding_bullets: list[str]
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


def _normalize_bullet(text: str) -> str:
    cleaned = _normalize_text(str(text or ""), max_chars=320)
    cleaned = cleaned.strip().lstrip("-").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _limit_words(text: str, *, max_words: int = MAX_BULLET_WORDS) -> str:
    words = str(text or "").split()
    if len(words) <= max_words:
        return " ".join(words).strip()
    return " ".join(words[:max_words]).strip()


def _has_digits(text: str) -> bool:
    return bool(re.search(r"\d", str(text or "")))


def _normalize_bullet_list(items: list[str], *, max_items: int) -> list[str]:
    out: list[str] = []
    for item in items:
        bullet = _limit_words(_normalize_bullet(item))
        if not bullet:
            continue
        out.append(bullet)
        if len(out) >= max_items:
            break
    return out

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
    for key, max_items in (("thesis", MAX_THESIS_BULLETS), ("context", MAX_CONTEXT_BULLETS), ("funding", MAX_FUNDING_BULLETS)):
        sections[key] = _normalize_bullet_list(sections[key], max_items=max_items)
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
    lines = [f"*Board Seat as a Service — {company}*", "*Thesis*"]
    for bullet in draft.thesis_bullets:
        lines.append(f"- {bullet}")
    lines.append(f"*{company} context*")
    for bullet in draft.context_bullets:
        lines.append(f"- {bullet}")
    lines.append("*Funding snapshot*")
    for bullet in draft.funding_bullets:
        lines.append(f"- {bullet}")
    return "\n".join(lines)


def _validate_draft(draft: BoardSeatDraft) -> list[str]:
    errors: list[str] = []
    if not draft.thesis_bullets:
        errors.append("missing_thesis")
    if not draft.context_bullets:
        errors.append("missing_context")
    if not draft.funding_bullets:
        errors.append("missing_funding")
    if len(draft.thesis_bullets) > MAX_THESIS_BULLETS:
        errors.append("too_many_thesis_bullets")
    if len(draft.context_bullets) > MAX_CONTEXT_BULLETS:
        errors.append("too_many_context_bullets")
    if len(draft.funding_bullets) > MAX_FUNDING_BULLETS:
        errors.append("too_many_funding_bullets")
    total = len(draft.thesis_bullets) + len(draft.context_bullets) + len(draft.funding_bullets)
    if total > MAX_TOTAL_BULLETS:
        errors.append("too_many_total_bullets")
    for section in (draft.thesis_bullets, draft.context_bullets, draft.funding_bullets):
        for bullet in section:
            if len(bullet.split()) > MAX_BULLET_WORDS:
                errors.append("bullet_too_long")
                break
    return errors


def _sanitize_draft(*, company: str, draft: BoardSeatDraft, funding: FundingSnapshot) -> BoardSeatDraft:
    thesis = _normalize_bullet_list(draft.thesis_bullets, max_items=MAX_THESIS_BULLETS)
    context = _normalize_bullet_list(draft.context_bullets, max_items=MAX_CONTEXT_BULLETS)
    funding_bullets = _normalize_bullet_list(draft.funding_bullets, max_items=MAX_FUNDING_BULLETS)

    if not thesis:
        thesis = [f"{company} has a board-relevant opportunity with measurable upside and execution risk in the next 12 months."]
    if not context:
        context = [f"Anchor this to {company}'s existing products, customer programs, and near-term execution priorities."]
    if not funding_bullets:
        funding_bullets = _funding_bullets_from_snapshot(funding)

    total = len(thesis) + len(context) + len(funding_bullets)
    if total > MAX_TOTAL_BULLETS:
        overflow = total - MAX_TOTAL_BULLETS
        while overflow > 0 and len(funding_bullets) > 1:
            funding_bullets.pop()
            overflow -= 1
        while overflow > 0 and len(context) > 1:
            context.pop()
            overflow -= 1
        while overflow > 0 and len(thesis) > 1:
            thesis.pop()
            overflow -= 1
    return BoardSeatDraft(
        thesis_bullets=thesis[:MAX_THESIS_BULLETS],
        context_bullets=context[:MAX_CONTEXT_BULLETS],
        funding_bullets=funding_bullets[:MAX_FUNDING_BULLETS],
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
) -> BoardSeatDraft:
    previous_signatures = [str(item.get("investment_signature") or "").strip() for item in recent_pitches]
    chosen = ""
    for snippet in snippets:
        sig = _token_signature(snippet, max_tokens=40)
        if not sig:
            continue
        if all(_jaccard_similarity(sig, prev) < 0.6 for prev in previous_signatures if prev):
            chosen = _limit_words(_normalize_text(snippet, max_chars=220))
            break
    if not chosen:
        chosen = _limit_words(_normalize_text(snippets[0], max_chars=220)) if snippets else f"No high-signal updates surfaced for {company}."

    context_line = (
        _limit_words(_normalize_text(snippets[1], max_chars=220))
        if len(snippets) > 1
        else f"Prioritize net-new ideas for {company} unless underlying data changed materially."
    )
    return _sanitize_draft(
        company=company,
        funding=funding,
        draft=BoardSeatDraft(
            thesis_bullets=[chosen],
            context_bullets=[context_line],
            funding_bullets=_funding_bullets_from_snapshot(funding),
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


def _funding_bullets_from_snapshot(snapshot: FundingSnapshot) -> list[str]:
    if _is_funding_snapshot_unknown(snapshot):
        return [UNKNOWN_FUNDING_TEXT]

    history = snapshot.history.strip()
    latest = snapshot.latest_round.strip()
    latest_date = snapshot.latest_date.strip()
    backers = ", ".join(snapshot.backers[:4]).strip()

    bullets: list[str] = []
    if history:
        bullets.append(f"History: {_limit_words(history)}")
    else:
        bullets.append("History: funding history not confirmed from current sources.")
    latest_parts: list[str] = []
    if latest:
        latest_parts.append(f"Latest round {latest}")
    if latest_date:
        latest_parts.append(f"({latest_date})")
    if backers:
        latest_parts.append(f"backers: {backers}")
    if latest_parts:
        bullets.append(_limit_words(" ".join(latest_parts)))
    else:
        bullets.append("Latest round/backers are currently unknown.")
    return _normalize_bullet_list(bullets, max_items=MAX_FUNDING_BULLETS)


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
        messages = payload.get("messages") if isinstance(payload, dict) else None
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
        meta = payload.get("response_metadata") if isinstance(payload, dict) else None
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
        messages = payload.get("messages") if isinstance(payload, dict) else None
        for item in messages if isinstance(messages, list) else []:
            if isinstance(item, dict):
                out.append(item)
                if len(out) >= remaining:
                    break
        meta = payload.get("response_metadata") if isinstance(payload, dict) else None
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


def _fallback_draft(*, company: str, snippets: list[str], funding: FundingSnapshot) -> BoardSeatDraft:
    thesis = (
        _limit_words(_normalize_text(snippets[0], max_chars=220))
        if snippets
        else f"No high-signal updates surfaced for {company} in the last 24 hours."
    )
    context = (
        _limit_words(_normalize_text(snippets[1], max_chars=220))
        if len(snippets) > 1
        else f"Tie this to {company}'s current programs, customer momentum, and execution risk over the next 1-2 quarters."
    )
    draft = BoardSeatDraft(
        thesis_bullets=[thesis],
        context_bullets=[context],
        funding_bullets=_funding_bullets_from_snapshot(funding),
        raw_model_output="",
        rewrite_reasons=["fallback"],
    )
    return _sanitize_draft(company=company, draft=draft, funding=funding)


def _parse_llm_draft_payload(payload: Any) -> BoardSeatDraft | None:
    if not isinstance(payload, dict):
        return None
    thesis = payload.get("thesis_bullets")
    context = payload.get("context_bullets")
    funding = payload.get("funding_bullets")
    if not isinstance(thesis, list) or not isinstance(context, list) or not isinstance(funding, list):
        return None
    return BoardSeatDraft(
        thesis_bullets=[str(item) for item in thesis],
        context_bullets=[str(item) for item in context],
        funding_bullets=[str(item) for item in funding],
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
        f"Generate structured board-seat bullets for {company}.\n"
        "Return strict JSON only with keys: thesis_bullets, context_bullets, funding_bullets.\n"
        "Constraints:\n"
        f"- thesis_bullets: 1-{MAX_THESIS_BULLETS} bullets\n"
        f"- context_bullets: 1-{MAX_CONTEXT_BULLETS} bullets tied to the company's actual domain work\n"
        f"- funding_bullets: 1-{MAX_FUNDING_BULLETS} bullets grounded only in provided funding snapshot\n"
        f"- each bullet <= {MAX_BULLET_WORDS} words\n"
        f"- total bullets <= {MAX_TOTAL_BULLETS}\n"
        "- short, high skim value, decision-useful.\n"
        "- do not use legacy labels (Signal/Board lens/Watchlist/Team ask).\n"
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
            thesis_bullets=draft.thesis_bullets,
            context_bullets=draft.context_bullets,
            funding_bullets=draft.funding_bullets,
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
    prior_investments = [str(item.get("investment_text") or "").strip() for item in (recent_pitches or [])]
    llm = _llm_draft(company=company, snippets=snippets, funding=funding, prior_investments=prior_investments)
    draft = llm if llm is not None else _fallback_draft(company=company, snippets=snippets, funding=funding)
    draft = _sanitize_draft(company=company, draft=draft, funding=funding)
    if _validate_draft(draft):
        return _fallback_draft(company=company, snippets=snippets, funding=funding)
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
                    draft = _build_novel_fallback_draft(
                        company=company,
                        snippets=snippets,
                        recent_pitches=recent_pitches,
                        funding=funding,
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

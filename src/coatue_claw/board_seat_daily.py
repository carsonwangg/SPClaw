from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
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
            investment_signature = _token_signature(investment_text)
            context_snippets = [investment_text] if investment_text else []
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
                investment_hash=_stable_hash(investment_signature or investment_text),
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


def _extract_investment_text(message: str) -> str:
    lines = [line.strip() for line in str(message or "").splitlines() if line.strip()]
    buckets: dict[str, str] = {}
    for line in lines:
        lower = line.lower()
        if lower.startswith("- signal:"):
            buckets["signal"] = line.split(":", 1)[1].strip() if ":" in line else ""
        elif lower.startswith("- board lens:"):
            buckets["lens"] = line.split(":", 1)[1].strip() if ":" in line else ""
        elif lower.startswith("- watchlist:"):
            buckets["watch"] = line.split(":", 1)[1].strip() if ":" in line else ""
    if buckets:
        combined = " | ".join(v for v in [buckets.get("signal"), buckets.get("lens"), buckets.get("watch")] if v)
        return _normalize_text(combined, max_chars=1200)

    fallback = [
        line
        for line in lines
        if "board seat as a service" not in line.lower()
    ]
    return _normalize_text(" | ".join(fallback[:3]), max_chars=1200)


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


def _build_novel_fallback_message(*, company: str, snippets: list[str], recent_pitches: list[dict[str, Any]]) -> str:
    previous_signatures = [str(item.get("investment_signature") or "").strip() for item in recent_pitches]
    chosen = ""
    for snippet in snippets:
        sig = _token_signature(snippet, max_tokens=40)
        if not sig:
            continue
        if all(_jaccard_similarity(sig, prev) < 0.6 for prev in previous_signatures if prev):
            chosen = _normalize_text(snippet, max_chars=180)
            break
    if not chosen:
        chosen = _normalize_text(snippets[0], max_chars=180) if snippets else f"No high-signal updates surfaced for {company}."

    watch = _normalize_text(snippets[1], max_chars=140) if len(snippets) > 1 else f"Watch pipeline health, gross margin, and delivery risk at {company} this week."
    ask = _normalize_text(snippets[2], max_chars=140) if len(snippets) > 2 else f"What is one net-new investment angle for {company} we should validate this week?"
    return "\n".join(
        [
            f"*Board Seat as a Service — {company}*",
            f"- Signal: {chosen}",
            f"- Board lens: Focus on a net-new angle versus prior recommendations unless the underlying data changed materially.",
            f"- Watchlist: {watch}",
            f"- Team ask: {ask}",
        ]
    )


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
        investment_signature = _token_signature(investment_text)
        context_snippets = [investment_text] if investment_text else []
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
            investment_hash=_stable_hash(investment_signature or investment_text),
            investment_signature=investment_signature,
            context_signature=context_signature,
            context_snippets=context_snippets,
            significant_change=False,
        )
        if did_insert:
            inserted += 1
    return {"scanned": len(history), "matched": matched, "inserted": inserted}


def _fallback_message(*, company: str, snippets: list[str]) -> str:
    signal = _normalize_text(snippets[0], max_chars=140) if snippets else f"No high-signal channel updates surfaced for {company} in the last 24h."
    watch = _normalize_text(snippets[1], max_chars=120) if len(snippets) > 1 else f"Monitor {company}'s product velocity, customer traction, and cost discipline this week."
    ask = _normalize_text(snippets[2], max_chars=120) if len(snippets) > 2 else f"Reply with the single highest-priority board question for {company} today."
    return "\n".join(
        [
            f"*Board Seat as a Service — {company}*",
            f"- Signal: {signal}",
            f"- Board lens: For {company}, focus on what changed that can move growth, margin, or risk in the next 1-2 quarters.",
            f"- Watchlist: {watch}",
            f"- Team ask: {ask}",
        ]
    )


def _llm_message(*, company: str, snippets: list[str], prior_investments: list[str] | None = None) -> str | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key):
        return None
    if not snippets:
        return None
    model = (os.environ.get("COATUE_CLAW_BOARD_SEAT_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest").strip()
    client = OpenAI(api_key=api_key)
    joined = "\n".join(f"- {line}" for line in snippets[:10])
    prompt = (
        f"Write a daily Slack post for board-seat-as-a-service in the {company} channel.\n"
        "Style: concise, operator-level, no hype, plain English.\n"
        "Output format must be exactly 5 lines:\n"
        f"1) *Board Seat as a Service — {company}*\n"
        "2) - Signal: ...\n"
        "3) - Board lens: ...\n"
        "4) - Watchlist: ...\n"
        "5) - Team ask: ...\n"
        "Keep total length under 110 words. No emojis. No numbering.\n"
        "Use this context from the channel:\n"
        f"{joined}\n"
    )
    if prior_investments:
        prior = "\n".join(f"- {item}" for item in prior_investments[:5] if item.strip())
        prompt += (
            "Do not repeat these prior investment theses unless the context clearly changed:\n"
            f"{prior}\n"
        )
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "You write tight board-level daily updates."},
                {"role": "user", "content": prompt},
            ],
        )
        text = ""
        if response and response.choices:
            text = str(response.choices[0].message.content or "").strip()
        if not text:
            return None
        lines = [line.rstrip() for line in text.splitlines() if line.strip()]
        if len(lines) < 5:
            return None
        return "\n".join(lines[:5])
    except Exception:
        return None


def _build_message(*, company: str, snippets: list[str], recent_pitches: list[dict[str, Any]] | None = None) -> str:
    prior_investments = [str(item.get("investment_text") or "").strip() for item in (recent_pitches or [])]
    llm = _llm_message(company=company, snippets=snippets, prior_investments=prior_investments)
    if llm:
        return llm
    return _fallback_message(company=company, snippets=snippets)


def run_once(*, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    store = BoardSeatStore()
    run_date = _today_key()
    portcos = _parse_portcos()
    result: dict[str, Any] = {
        "ok": True,
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
            message = _build_message(company=company, snippets=[])
            result["sent"].append({"company": company, "channel_ref": channel_ref, "preview": message})
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
                message = _build_message(company=company, snippets=snippets, recent_pitches=recent_pitches)
                investment_text = _extract_investment_text(message)
                investment_signature = _token_signature(investment_text)
                signal_signature = _signal_signature_from_investment(investment_text)
                signal_text = _signal_text_from_investment(investment_text)
                investment_hash = _stable_hash(investment_signature or investment_text)
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
                    message = _build_novel_fallback_message(company=company, snippets=snippets, recent_pitches=recent_pitches)
                    investment_text = _extract_investment_text(message)
                    investment_signature = _token_signature(investment_text)
                    signal_signature = _signal_signature_from_investment(investment_text)
                    signal_text = _signal_text_from_investment(investment_text)
                    investment_hash = _stable_hash(investment_signature or investment_text)
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
    return {
        "ok": True,
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

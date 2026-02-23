from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
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


def _normalize_text(text: str, *, max_chars: int = 240) -> str:
    cleaned = re.sub(r"https?://\S+", "", text or "")
    cleaned = re.sub(r"<@[^>]+>", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_chars]


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


def _llm_message(*, company: str, snippets: list[str]) -> str | None:
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


def _build_message(*, company: str, snippets: list[str]) -> str:
    llm = _llm_message(company=company, snippets=snippets)
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
                snippets = _fetch_recent_context(client, channel_id=channel_id, company=company)
                message = _build_message(company=company, snippets=snippets)
                if dry_run:
                    result["sent"].append(
                        {"company": company, "channel_ref": channel_ref, "channel_id": channel_id, "preview": message}
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
                result["sent"].append({"company": company, "channel_ref": channel_ref, "channel_id": channel_id, "ts": ts})
                posted = True
                break
            except SlackApiError as exc:
                err = str(exc.response.get("error") or "")
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
    return {
        "ok": True,
        "timezone": str(_timezone()),
        "run_date_local": _today_key(),
        "portcos": [{"company": c, "channel_ref": ch} for c, ch in _parse_portcos()],
        "recent_runs": store.recent_runs(limit=20),
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

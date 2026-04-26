from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
except Exception:  # pragma: no cover - optional dependency for non-Slack test envs
    WebClient = None  # type: ignore[assignment]
    SlackApiError = Exception  # type: ignore[assignment]

from coatue_claw.spencer_change_log import SpencerChange, SpencerChangeLog, requester_label

load_dotenv("/opt/SPClaw/.env.prod")

DEFAULT_DM_USER_IDS = ("U0AGD28QSQG",)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _timezone() -> ZoneInfo:
    name = (os.environ.get("COATUE_CLAW_SPENCER_CHANGE_DIGEST_TZ", "America/Los_Angeles") or "").strip()
    if not name:
        name = "America/Los_Angeles"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("America/Los_Angeles")


def _today_key() -> str:
    return datetime.now(_timezone()).strftime("%Y-%m-%d")


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


def _dm_user_ids() -> list[str]:
    raw = (os.environ.get("COATUE_CLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS", "") or "").strip()
    if raw:
        out = [item.strip() for item in raw.split(",") if item.strip()]
        if out:
            return out
    return list(DEFAULT_DM_USER_IDS)


def _open_changes(changes: list[SpencerChange]) -> list[SpencerChange]:
    return [item for item in changes if item.status != "implemented"]


def _format_digest(changes: list[SpencerChange]) -> str:
    now_local = datetime.now(_timezone()).strftime("%b %d, %Y %I:%M %p %Z")
    lines = [
        f"Tracked change requests still open (as of {now_local}):",
    ]
    if not changes:
        lines.append("- None. All tracked requests are implemented.")
        return "\n".join(lines)

    for item in changes[:25]:
        channel_ref = f"<#{item.channel}>" if item.channel else "unknown-channel"
        who = requester_label(item.user_id)
        text = item.text
        if len(text) > 140:
            text = text[:137].rstrip() + "..."
        lines.append(f"- #{item.change_id} [{item.status}] [{who}] {channel_ref}: {text}")
    if len(changes) > 25:
        lines.append(f"- ... plus {len(changes) - 25} more")
    lines.append("Run `change requests` (or `spencer changes`) in Slack for the full list.")
    return "\n".join(lines)


def _ensure_digest_table(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spencer_change_digest_runs (
                digest_date TEXT NOT NULL,
                recipient_user_id TEXT NOT NULL,
                sent_at_utc TEXT NOT NULL,
                open_count INTEGER NOT NULL,
                PRIMARY KEY (digest_date, recipient_user_id)
            )
            """
        )
        conn.commit()


def _already_sent_today(*, db_path: Path, digest_date: str, recipient_user_id: str) -> bool:
    _ensure_digest_table(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM spencer_change_digest_runs WHERE digest_date = ? AND recipient_user_id = ? LIMIT 1",
            (digest_date, recipient_user_id),
        ).fetchone()
    return bool(row)


def _mark_sent(*, db_path: Path, digest_date: str, recipient_user_id: str, open_count: int) -> None:
    _ensure_digest_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO spencer_change_digest_runs(
                digest_date, recipient_user_id, sent_at_utc, open_count
            ) VALUES (?, ?, ?, ?)
            """,
            (digest_date, recipient_user_id, _utc_now_iso(), int(open_count)),
        )
        conn.commit()


def run_once(*, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    tracker = SpencerChangeLog()
    changes = tracker.list_changes(limit=200)
    open_changes = _open_changes(changes)
    message = _format_digest(open_changes)
    recipients = _dm_user_ids()
    digest_date = _today_key()

    result: dict[str, Any] = {
        "ok": True,
        "digest_date": digest_date,
        "recipients": recipients,
        "open_count": len(open_changes),
        "sent": [],
        "skipped": [],
    }
    if dry_run:
        result["preview"] = message
        return result

    if WebClient is None:
        raise RuntimeError("slack_sdk is not installed in this environment.")
    clients = [WebClient(token=item) for item in _slack_tokens()]
    db_path = tracker.db_path
    for user_id in recipients:
        if (not force) and _already_sent_today(db_path=db_path, digest_date=digest_date, recipient_user_id=user_id):
            result["skipped"].append({"user_id": user_id, "reason": "already_sent_today"})
            continue
        sent = False
        last_error = "unknown"
        for client in clients:
            try:
                dm = client.conversations_open(users=user_id)
                channel_id = str((dm.get("channel") or {}).get("id") or "")
                if not channel_id:
                    last_error = "dm_open_failed"
                    continue
                post = client.chat_postMessage(channel=channel_id, text=message)
                _mark_sent(db_path=db_path, digest_date=digest_date, recipient_user_id=user_id, open_count=len(open_changes))
                result["sent"].append({"user_id": user_id, "channel": channel_id, "ts": post.get("ts")})
                sent = True
                break
            except SlackApiError as exc:
                err = str(exc.response.get("error") or "")
                last_error = err or "slack_api_error"
                if err == "missing_scope":
                    # Fallback path: post directly to App Home DM by user id.
                    try:
                        post = client.chat_postMessage(channel=user_id, text=message)
                        _mark_sent(
                            db_path=db_path,
                            digest_date=digest_date,
                            recipient_user_id=user_id,
                            open_count=len(open_changes),
                        )
                        result["sent"].append({"user_id": user_id, "channel": user_id, "ts": post.get("ts")})
                        sent = True
                        break
                    except Exception:
                        last_error = "missing_scope"
                        continue
                if err in {"account_inactive", "invalid_auth", "token_revoked", "not_authed"}:
                    continue
                break
            except Exception:
                last_error = "unexpected_error"
                break
        if not sent:
            result["skipped"].append({"user_id": user_id, "reason": last_error})
    return result


def status() -> dict[str, Any]:
    tracker = SpencerChangeLog()
    changes = tracker.list_changes(limit=200)
    open_changes = _open_changes(changes)
    recipients = _dm_user_ids()
    db_path = tracker.db_path
    _ensure_digest_table(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT digest_date, recipient_user_id, sent_at_utc, open_count
            FROM spencer_change_digest_runs
            ORDER BY sent_at_utc DESC
            LIMIT 20
            """
        ).fetchall()
    recent = [
        {
            "digest_date": str(row["digest_date"]),
            "recipient_user_id": str(row["recipient_user_id"]),
            "sent_at_utc": str(row["sent_at_utc"]),
            "open_count": int(row["open_count"]),
        }
        for row in rows
    ]
    return {
        "ok": True,
        "timezone": str(_timezone()),
        "recipients": recipients,
        "open_count": len(open_changes),
        "digest_date_today": _today_key(),
        "recent_runs": recent,
    }


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-spencer-change-digest")
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

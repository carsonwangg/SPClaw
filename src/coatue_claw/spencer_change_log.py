from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import re
import sqlite3


DEFAULT_TRACKED_CHANGE_USERS: dict[str, str] = {
    "U0AFJ5RS31C": "Spencer Peterson",
    "U0AFJ5T6JTY": "Spencer Peterson",
    "U0AGD28QSQG": "Carson Wang",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_SPENCER_CHANGE_DB_PATH",
            str(_data_root() / "db/spencer_changes.sqlite"),
        )
    )


def _normalize_text(text: str, *, max_chars: int = 4000) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())[:max_chars]


def _normalize_status(status: str) -> str:
    value = (status or "").strip().lower()
    if value in {"captured", "handled", "implemented", "blocked", "needs_followup"}:
        return value
    return "captured"


def _parse_change_tracker_users(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in raw.split(","):
        part = item.strip()
        if not part:
            continue
        if ":" in part:
            user_id, label = part.split(":", 1)
            uid = user_id.strip()
            name = label.strip()
            if uid:
                out[uid] = name or uid
            continue
        out[part] = part
    return out


def tracked_change_users() -> dict[str, str]:
    raw_new = os.environ.get("COATUE_CLAW_CHANGE_TRACKER_USERS", "").strip()
    if raw_new:
        parsed = _parse_change_tracker_users(raw_new)
        if parsed:
            return parsed

    raw_legacy = os.environ.get("COATUE_CLAW_SPENCER_USER_IDS", "").strip()
    if raw_legacy:
        parsed = _parse_change_tracker_users(raw_legacy)
        if parsed:
            # legacy env only had IDs; synthesize labels from defaults when known.
            return {uid: DEFAULT_TRACKED_CHANGE_USERS.get(uid, uid) for uid in parsed.keys()}

    return dict(DEFAULT_TRACKED_CHANGE_USERS)


def spencer_user_ids() -> set[str]:
    return set(tracked_change_users().keys())


def is_spencer_user(user_id: str | None) -> bool:
    if not user_id:
        return False
    return user_id in spencer_user_ids()


def requester_label(user_id: str | None) -> str:
    if not user_id:
        return "Unknown"
    return tracked_change_users().get(user_id, user_id)


def looks_like_change_request(text: str) -> bool:
    lower = _normalize_text(text).lower()
    if not lower:
        return False

    if len(lower.split()) < 4:
        return False

    patterns = (
        r"\b(can you|could you|please|going forward|make sure|i want|we want|needs to|should)\b",
        r"\b(add|change|update|fix|move|rename|remove|turn on|turn off|set up|implement|enable|disable)\b",
        r"\b(bot|slack|channel|workflow|pipeline|memory|chart|diligence|email|x |twitter|drive|folder)\b",
    )
    return all(bool(re.search(pattern, lower)) for pattern in patterns)


@dataclass(frozen=True)
class SpencerChange:
    change_id: int
    user_id: str
    channel: str | None
    thread_ts: str | None
    message_ts: str | None
    text: str
    status: str
    captured_at_utc: str
    updated_at_utc: str
    implemented_at_utc: str | None
    implementation_note: str | None


class SpencerChangeLog:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or _db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS spencer_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    channel TEXT,
                    thread_ts TEXT,
                    message_ts TEXT,
                    text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    captured_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    implemented_at_utc TEXT,
                    implementation_note TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_spencer_changes_captured
                ON spencer_changes(captured_at_utc DESC);

                CREATE INDEX IF NOT EXISTS idx_spencer_changes_status
                ON spencer_changes(status, captured_at_utc DESC);
                """
            )
            conn.commit()

    def capture_request(
        self,
        *,
        user_id: str,
        channel: str | None,
        thread_ts: str | None,
        message_ts: str | None,
        text: str,
    ) -> int:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO spencer_changes(
                    user_id, channel, thread_ts, message_ts, text, status,
                    captured_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, 'captured', ?, ?)
                """,
                (
                    user_id,
                    (channel or "").strip() or None,
                    (thread_ts or "").strip() or None,
                    (message_ts or "").strip() or None,
                    _normalize_text(text),
                    now,
                    now,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def update_status(self, change_id: int, *, status: str, note: str | None = None) -> None:
        status_value = _normalize_status(status)
        now = _utc_now_iso()
        implemented_at = now if status_value in {"handled", "implemented"} else None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE spencer_changes
                SET
                    status = ?,
                    updated_at_utc = ?,
                    implemented_at_utc = COALESCE(?, implemented_at_utc),
                    implementation_note = COALESCE(?, implementation_note)
                WHERE id = ?
                """,
                (
                    status_value,
                    now,
                    implemented_at,
                    _normalize_text(note or "", max_chars=1000) or None,
                    int(change_id),
                ),
            )
            conn.commit()

    def list_changes(self, *, limit: int = 20, status: str | None = None) -> list[SpencerChange]:
        lim = max(1, min(200, int(limit)))
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM spencer_changes
                    WHERE status = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (_normalize_status(status), lim),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM spencer_changes
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (lim,),
                ).fetchall()
        out: list[SpencerChange] = []
        for row in rows:
            out.append(
                SpencerChange(
                    change_id=int(row["id"]),
                    user_id=str(row["user_id"]),
                    channel=(str(row["channel"]) if row["channel"] else None),
                    thread_ts=(str(row["thread_ts"]) if row["thread_ts"] else None),
                    message_ts=(str(row["message_ts"]) if row["message_ts"] else None),
                    text=str(row["text"]),
                    status=str(row["status"]),
                    captured_at_utc=str(row["captured_at_utc"]),
                    updated_at_utc=str(row["updated_at_utc"]),
                    implemented_at_utc=(str(row["implemented_at_utc"]) if row["implemented_at_utc"] else None),
                    implementation_note=(str(row["implementation_note"]) if row["implementation_note"] else None),
                )
            )
        return out


def format_changes(changes: list[SpencerChange], *, title: str = "Tracked Change Requests") -> str:
    if not changes:
        return f"{title}:\n- none captured yet."
    lines = [f"{title}:"]
    for item in changes:
        channel_ref = f"<#{item.channel}>" if item.channel else "unknown-channel"
        text = _normalize_text(item.text, max_chars=150)
        note = f" | note: {item.implementation_note}" if item.implementation_note else ""
        requester = requester_label(item.user_id)
        lines.append(
            f"- `#{item.change_id}` [{item.status}] [{requester}] {channel_ref} at `{item.captured_at_utc}` | {text}{note}"
        )
    return "\n".join(lines)

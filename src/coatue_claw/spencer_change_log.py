from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import csv
import os
from pathlib import Path
import re
import sqlite3
from typing import Any


DEFAULT_TRACKED_CHANGE_USERS: dict[str, str] = {
    "U0AFJ5RS31C": "Spencer Peterson",
    "U0AFJ5T6JTY": "Spencer Peterson",
    "U0AGD28QSQG": "Carson Wang",
}

DEFAULT_REQUEST_KIND = "change_request"
REQUEST_KINDS = {DEFAULT_REQUEST_KIND, "memory_git"}
DEFAULT_TRIGGER_MODE = "manual"
TRIGGER_MODES = {DEFAULT_TRIGGER_MODE, "git_memory_prefix", "auto_behavior_request"}
LEDGER_COLUMNS: tuple[str, ...] = (
    "change_id",
    "request_kind",
    "trigger_mode",
    "captured_at_utc",
    "status_before",
    "status_after",
    "decision",
    "source_ref",
    "mapped_paths",
    "related_commit",
    "resolved_at_utc",
    "resolved_by",
    "note",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _repo_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_REPO_PATH", "/opt/coatue-claw"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_SPENCER_CHANGE_DB_PATH",
            str(_data_root() / "db/spencer_changes.sqlite"),
        )
    )


def memory_reconcile_dir() -> Path:
    return _repo_root() / "docs/memory-inbox"


def memory_reconcile_queue_path() -> Path:
    return memory_reconcile_dir() / "queue.md"


def memory_reconcile_ledger_path() -> Path:
    return memory_reconcile_dir() / "reconciliation-ledger.csv"


def _normalize_text(text: str, *, max_chars: int = 4000) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())[:max_chars]


def _normalize_status(status: str) -> str:
    value = (status or "").strip().lower()
    if value in {"captured", "handled", "implemented", "blocked", "needs_followup"}:
        return value
    return "captured"


def _normalize_request_kind(request_kind: str | None) -> str:
    value = (request_kind or "").strip().lower()
    if value in REQUEST_KINDS:
        return value
    return DEFAULT_REQUEST_KIND


def _normalize_trigger_mode(trigger_mode: str | None) -> str:
    value = (trigger_mode or "").strip().lower()
    if value in TRIGGER_MODES:
        return value
    return DEFAULT_TRIGGER_MODE


def _normalize_commit_hash(commit: str | None) -> str | None:
    value = (commit or "").strip().lower()
    if not value:
        return None
    if re.fullmatch(r"[0-9a-f]{7,40}", value):
        return value
    raise ValueError(f"Invalid git commit hash: {commit}")


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
    base = dict(DEFAULT_TRACKED_CHANGE_USERS)

    raw_new = os.environ.get("COATUE_CLAW_CHANGE_TRACKER_USERS", "").strip()
    if raw_new:
        parsed = _parse_change_tracker_users(raw_new)
        if parsed:
            base.update(parsed)
            return base

    raw_legacy = os.environ.get("COATUE_CLAW_SPENCER_USER_IDS", "").strip()
    if raw_legacy:
        parsed = _parse_change_tracker_users(raw_legacy)
        if parsed:
            for uid in parsed.keys():
                base[uid] = DEFAULT_TRACKED_CHANGE_USERS.get(uid, uid)
            return base

    return base


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
    request_kind: str = DEFAULT_REQUEST_KIND
    trigger_mode: str = DEFAULT_TRIGGER_MODE
    source_ref: str | None = None
    related_commit: str | None = None


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
                    implementation_note TEXT,
                    request_kind TEXT NOT NULL DEFAULT 'change_request',
                    trigger_mode TEXT NOT NULL DEFAULT 'manual',
                    source_ref TEXT,
                    related_commit TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_spencer_changes_captured
                ON spencer_changes(captured_at_utc DESC);

                CREATE INDEX IF NOT EXISTS idx_spencer_changes_status
                ON spencer_changes(status, captured_at_utc DESC);
                """
            )
            cols = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(spencer_changes)").fetchall()
            }
            if "request_kind" not in cols:
                conn.execute(
                    "ALTER TABLE spencer_changes ADD COLUMN request_kind TEXT NOT NULL DEFAULT 'change_request'"
                )
            if "trigger_mode" not in cols:
                conn.execute(
                    "ALTER TABLE spencer_changes ADD COLUMN trigger_mode TEXT NOT NULL DEFAULT 'manual'"
                )
            if "source_ref" not in cols:
                conn.execute("ALTER TABLE spencer_changes ADD COLUMN source_ref TEXT")
            if "related_commit" not in cols:
                conn.execute("ALTER TABLE spencer_changes ADD COLUMN related_commit TEXT")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_spencer_changes_kind_status
                ON spencer_changes(request_kind, status, captured_at_utc DESC)
                """
            )
            conn.commit()

    def _row_to_change(self, row: sqlite3.Row) -> SpencerChange:
        return SpencerChange(
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
            request_kind=_normalize_request_kind(str(row["request_kind"]) if row["request_kind"] else DEFAULT_REQUEST_KIND),
            trigger_mode=_normalize_trigger_mode(str(row["trigger_mode"]) if row["trigger_mode"] else DEFAULT_TRIGGER_MODE),
            source_ref=(str(row["source_ref"]) if row["source_ref"] else None),
            related_commit=(str(row["related_commit"]) if row["related_commit"] else None),
        )

    def get_change(self, change_id: int) -> SpencerChange | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM spencer_changes WHERE id = ? LIMIT 1",
                (int(change_id),),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_change(row)

    def capture_request(
        self,
        *,
        user_id: str,
        channel: str | None,
        thread_ts: str | None,
        message_ts: str | None,
        text: str,
        request_kind: str = DEFAULT_REQUEST_KIND,
        trigger_mode: str = DEFAULT_TRIGGER_MODE,
        source_ref: str | None = None,
    ) -> int:
        now = _utc_now_iso()
        request_kind_value = _normalize_request_kind(request_kind)
        trigger_mode_value = _normalize_trigger_mode(trigger_mode)
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO spencer_changes(
                    user_id, channel, thread_ts, message_ts, text, status,
                    captured_at_utc, updated_at_utc, request_kind, trigger_mode, source_ref
                ) VALUES (?, ?, ?, ?, ?, 'captured', ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    (channel or "").strip() or None,
                    (thread_ts or "").strip() or None,
                    (message_ts or "").strip() or None,
                    _normalize_text(text),
                    now,
                    now,
                    request_kind_value,
                    trigger_mode_value,
                    _normalize_text(source_ref or "", max_chars=1000) or None,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def update_status(
        self,
        change_id: int,
        *,
        status: str,
        note: str | None = None,
        related_commit: str | None = None,
    ) -> None:
        status_value = _normalize_status(status)
        now = _utc_now_iso()
        implemented_at = now if status_value in {"handled", "implemented"} else None
        commit_value = _normalize_commit_hash(related_commit)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE spencer_changes
                SET
                    status = ?,
                    updated_at_utc = ?,
                    implemented_at_utc = COALESCE(?, implemented_at_utc),
                    implementation_note = COALESCE(?, implementation_note),
                    related_commit = COALESCE(?, related_commit)
                WHERE id = ?
                """,
                (
                    status_value,
                    now,
                    implemented_at,
                    _normalize_text(note or "", max_chars=1000) or None,
                    commit_value,
                    int(change_id),
                ),
            )
            conn.commit()

    def list_changes(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
        request_kind: str | None = None,
        open_only: bool = False,
    ) -> list[SpencerChange]:
        lim = max(1, min(1000, int(limit)))
        where: list[str] = []
        params: list[Any] = []

        if status:
            where.append("status = ?")
            params.append(_normalize_status(status))
        if request_kind:
            where.append("request_kind = ?")
            params.append(_normalize_request_kind(request_kind))
        if open_only:
            where.append("status != 'implemented'")

        sql = "SELECT * FROM spencer_changes"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(lim)

        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_change(row) for row in rows]

    def reconcile_status(self) -> dict[str, Any]:
        with self._connect() as conn:
            total_row = conn.execute(
                "SELECT COUNT(1) AS n FROM spencer_changes WHERE request_kind = 'memory_git'"
            ).fetchone()
            open_row = conn.execute(
                "SELECT COUNT(1) AS n FROM spencer_changes WHERE request_kind = 'memory_git' AND status != 'implemented'"
            ).fetchone()
            grouped_rows = conn.execute(
                """
                SELECT status, COUNT(1) AS n
                FROM spencer_changes
                WHERE request_kind = 'memory_git'
                GROUP BY status
                ORDER BY status
                """
            ).fetchall()
            latest = conn.execute(
                "SELECT * FROM spencer_changes WHERE request_kind = 'memory_git' ORDER BY id DESC LIMIT 1"
            ).fetchone()

        latest_entry = self._row_to_change(latest) if latest is not None else None
        return {
            "ok": True,
            "request_kind": "memory_git",
            "total": int(total_row["n"] if total_row else 0),
            "open": int(open_row["n"] if open_row else 0),
            "by_status": {str(row["status"]): int(row["n"]) for row in grouped_rows},
            "latest_change_id": (latest_entry.change_id if latest_entry else None),
            "latest_status": (latest_entry.status if latest_entry else None),
            "queue_path": str(memory_reconcile_queue_path()),
            "ledger_path": str(memory_reconcile_ledger_path()),
        }

    def _ensure_reconcile_artifacts(self) -> None:
        out_dir = memory_reconcile_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        ledger_path = memory_reconcile_ledger_path()
        if ledger_path.exists():
            return
        with ledger_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(LEDGER_COLUMNS))
            writer.writeheader()

    def export_memory_git_queue(self, *, limit: int = 200, queue_path: Path | None = None) -> dict[str, Any]:
        self._ensure_reconcile_artifacts()
        rows = self.list_changes(limit=limit, request_kind="memory_git", open_only=True)
        path = queue_path or memory_reconcile_queue_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        lines = [
            "# Memory Git Reconciliation Queue",
            "",
            f"- generated_at_utc: `{_utc_now_iso()}`",
            f"- open_items: `{len(rows)}`",
            "",
        ]
        if not rows:
            lines.append("No open `memory_git` requests.")
        else:
            for item in rows:
                requester = requester_label(item.user_id)
                channel_ref = f"<#{item.channel}>" if item.channel else "unknown-channel"
                text = _normalize_text(item.text, max_chars=240)
                lines.append(
                    f"- `#{item.change_id}` [{item.status}] [{requester}] {channel_ref} at `{item.captured_at_utc}` | {text}"
                )
                if item.source_ref:
                    lines.append(f"  - source_ref: `{_normalize_text(item.source_ref, max_chars=280)}`")
                if item.related_commit:
                    lines.append(f"  - related_commit: `{item.related_commit}`")
                if item.implementation_note:
                    lines.append(f"  - note: `{_normalize_text(item.implementation_note, max_chars=280)}`")

        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {
            "ok": True,
            "queue_path": str(path),
            "count": len(rows),
            "change_ids": [item.change_id for item in rows],
        }

    def reconcile_link(
        self,
        *,
        ids: list[int],
        commit: str,
        resolved_by: str,
        note: str = "",
        mapped_paths: str = "",
        decision: str = "batch_session_linked",
    ) -> dict[str, Any]:
        commit_hash = _normalize_commit_hash(commit)
        if commit_hash is None:
            raise ValueError("commit is required")

        self._ensure_reconcile_artifacts()
        now = _utc_now_iso()
        updated: list[int] = []
        skipped: list[dict[str, Any]] = []
        ledger_rows: list[dict[str, str]] = []

        unique_ids = [int(item) for item in dict.fromkeys(ids)]

        with self._connect() as conn:
            for change_id in unique_ids:
                row = conn.execute(
                    "SELECT * FROM spencer_changes WHERE id = ? LIMIT 1",
                    (change_id,),
                ).fetchone()
                if row is None:
                    skipped.append({"change_id": change_id, "reason": "not_found"})
                    continue

                item = self._row_to_change(row)
                if item.request_kind != "memory_git":
                    skipped.append({"change_id": change_id, "reason": "not_memory_git"})
                    continue

                next_note = _normalize_text(note or "", max_chars=1000) or item.implementation_note
                conn.execute(
                    """
                    UPDATE spencer_changes
                    SET
                        status = 'implemented',
                        updated_at_utc = ?,
                        implemented_at_utc = COALESCE(implemented_at_utc, ?),
                        implementation_note = ?,
                        related_commit = ?
                    WHERE id = ?
                    """,
                    (
                        now,
                        now,
                        next_note,
                        commit_hash,
                        change_id,
                    ),
                )
                updated.append(change_id)
                ledger_rows.append(
                    {
                        "change_id": str(change_id),
                        "request_kind": item.request_kind,
                        "trigger_mode": item.trigger_mode,
                        "captured_at_utc": item.captured_at_utc,
                        "status_before": item.status,
                        "status_after": "implemented",
                        "decision": decision,
                        "source_ref": item.source_ref or "",
                        "mapped_paths": _normalize_text(mapped_paths, max_chars=500),
                        "related_commit": commit_hash,
                        "resolved_at_utc": now,
                        "resolved_by": _normalize_text(resolved_by or "unknown", max_chars=120),
                        "note": _normalize_text(note, max_chars=1000),
                    }
                )

            conn.commit()

        ledger_path = memory_reconcile_ledger_path()
        with ledger_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(LEDGER_COLUMNS))
            for row in ledger_rows:
                writer.writerow(row)

        return {
            "ok": True,
            "commit": commit_hash,
            "updated": updated,
            "skipped": skipped,
            "ledger_path": str(ledger_path),
            "resolved_at_utc": now,
        }


def format_changes(changes: list[SpencerChange], *, title: str = "Tracked Change Requests") -> str:
    if not changes:
        return f"{title}:\n- none captured yet."

    lines = [f"{title}:"]
    for item in changes:
        channel_ref = f"<#{item.channel}>" if item.channel else "unknown-channel"
        text = _normalize_text(item.text, max_chars=150)
        requester = requester_label(item.user_id)
        kind_label = ""
        if item.request_kind != DEFAULT_REQUEST_KIND:
            kind_label = f" [{item.request_kind}]"

        extras: list[str] = []
        if item.related_commit:
            extras.append(f"commit:{item.related_commit[:10]}")
        if item.implementation_note:
            extras.append(f"note:{_normalize_text(item.implementation_note, max_chars=90)}")

        extra_text = ""
        if extras:
            extra_text = " | " + " | ".join(extras)

        lines.append(
            f"- `#{item.change_id}` [{item.status}] [{requester}]{kind_label} {channel_ref} at `{item.captured_at_utc}` | {text}{extra_text}"
        )
    return "\n".join(lines)

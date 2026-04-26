from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _default_db_path() -> Path:
    data_root = Path(os.environ.get("SPCLAW_DATA_ROOT", "/opt/spclaw-data"))
    return Path(os.environ.get("SPCLAW_HFA_DB_PATH", str(data_root / "db/hf_analyst.sqlite")))


class HFStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _default_db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS hf_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    requested_by TEXT,
                    question TEXT,
                    trigger_mode TEXT NOT NULL,
                    model TEXT NOT NULL,
                    status TEXT NOT NULL,
                    summary_text TEXT,
                    artifact_path TEXT,
                    warnings_json TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    completed_at_utc TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_hf_runs_lookup ON hf_runs(channel, thread_ts, created_at_utc DESC);

                CREATE TABLE IF NOT EXISTS hf_run_inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    slack_file_id TEXT,
                    original_name TEXT NOT NULL,
                    mime_type TEXT,
                    local_path TEXT NOT NULL,
                    sha256 TEXT,
                    page_count INTEGER,
                    char_count INTEGER NOT NULL DEFAULT 0,
                    source_ts_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES hf_runs(run_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_hf_run_inputs_run ON hf_run_inputs(run_id);

                CREATE TABLE IF NOT EXISTS hf_run_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    section_key TEXT NOT NULL,
                    section_title TEXT NOT NULL,
                    section_text TEXT NOT NULL,
                    citations_json TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0.0,
                    created_at_utc TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES hf_runs(run_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_hf_run_sections_run ON hf_run_sections(run_id);

                CREATE TABLE IF NOT EXISTS hf_dm_autoruns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    file_set_hash TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE(channel, user_id, thread_ts, file_set_hash)
                );

                CREATE TABLE IF NOT EXISTS hf_podcast_inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    video_id TEXT NOT NULL,
                    title TEXT,
                    channel_name TEXT,
                    duration_sec INTEGER,
                    transcript_source TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES hf_runs(run_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_hf_podcast_inputs_run ON hf_podcast_inputs(run_id);

                CREATE TABLE IF NOT EXISTS hf_dm_podcast_autoruns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    thread_ts TEXT NOT NULL,
                    url_hash TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE(channel, user_id, thread_ts, url_hash)
                );
                """
            )
            cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(hf_runs)").fetchall()}
            if "run_kind" not in cols:
                conn.execute("ALTER TABLE hf_runs ADD COLUMN run_kind TEXT NOT NULL DEFAULT 'thread_docs'")

    def start_run(
        self,
        *,
        channel: str,
        thread_ts: str,
        requested_by: str | None,
        question: str | None,
        trigger_mode: str,
        model: str,
        run_kind: str = "thread_docs",
    ) -> int:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO hf_runs (
                    channel, thread_ts, requested_by, question, trigger_mode, model, run_kind, status,
                    created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel,
                    thread_ts,
                    requested_by,
                    question,
                    trigger_mode,
                    model,
                    run_kind,
                    "running",
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def add_input(
        self,
        *,
        run_id: int,
        slack_file_id: str | None,
        original_name: str,
        mime_type: str | None,
        local_path: str,
        sha256: str | None,
        page_count: int | None,
        char_count: int,
        source_ts_utc: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO hf_run_inputs (
                    run_id, slack_file_id, original_name, mime_type, local_path, sha256,
                    page_count, char_count, source_ts_utc, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    slack_file_id,
                    original_name,
                    mime_type,
                    local_path,
                    sha256,
                    page_count,
                    char_count,
                    source_ts_utc,
                    _utc_now_iso(),
                ),
            )

    def add_section(
        self,
        *,
        run_id: int,
        section_key: str,
        section_title: str,
        section_text: str,
        citations: list[dict[str, str]],
        confidence: float,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO hf_run_sections (
                    run_id, section_key, section_title, section_text, citations_json, confidence, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    section_key,
                    section_title,
                    section_text,
                    json.dumps(citations, ensure_ascii=False),
                    confidence,
                    _utc_now_iso(),
                ),
            )

    def complete_run(
        self,
        *,
        run_id: int,
        summary_text: str,
        artifact_path: str | None,
        warnings: list[str],
    ) -> None:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE hf_runs
                SET status=?, summary_text=?, artifact_path=?, warnings_json=?,
                    updated_at_utc=?, completed_at_utc=?
                WHERE run_id=?
                """,
                (
                    "completed",
                    summary_text[:5000],
                    artifact_path,
                    json.dumps(warnings, ensure_ascii=False),
                    now,
                    now,
                    run_id,
                ),
            )

    def fail_run(self, *, run_id: int, reason: str) -> None:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE hf_runs
                SET status=?, warnings_json=?, updated_at_utc=?, completed_at_utc=?
                WHERE run_id=?
                """,
                (
                    "failed",
                    json.dumps([reason], ensure_ascii=False),
                    now,
                    now,
                    run_id,
                ),
            )

    def recent_runs(self, *, channel: str | None = None, thread_ts: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if channel:
            where.append("channel = ?")
            params.append(channel)
        if thread_ts:
            where.append("thread_ts = ?")
            params.append(thread_ts)
        sql = (
            "SELECT run_id, channel, thread_ts, requested_by, question, trigger_mode, model, run_kind, status, "
            "summary_text, artifact_path, warnings_json, created_at_utc, updated_at_utc, completed_at_utc "
            "FROM hf_runs "
        )
        if where:
            sql += f"WHERE {' AND '.join(where)} "
        sql += "ORDER BY created_at_utc DESC LIMIT ?"
        params.append(max(1, min(200, int(limit))))
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            warnings_raw = payload.get("warnings_json")
            if isinstance(warnings_raw, str) and warnings_raw.strip():
                try:
                    payload["warnings"] = json.loads(warnings_raw)
                except Exception:
                    payload["warnings"] = [warnings_raw]
            else:
                payload["warnings"] = []
            payload.pop("warnings_json", None)
            out.append(payload)
        return out

    def has_dm_autorun(self, *, channel: str, user_id: str, thread_ts: str, file_set_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM hf_dm_autoruns
                WHERE channel=? AND user_id=? AND thread_ts=? AND file_set_hash=?
                LIMIT 1
                """,
                (channel, user_id, thread_ts, file_set_hash),
            ).fetchone()
        return row is not None

    def record_dm_autorun(self, *, channel: str, user_id: str, thread_ts: str, file_set_hash: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO hf_dm_autoruns (
                    channel, user_id, thread_ts, file_set_hash, created_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (channel, user_id, thread_ts, file_set_hash, _utc_now_iso()),
            )

    def add_podcast_input(
        self,
        *,
        run_id: int,
        url: str,
        video_id: str,
        title: str | None,
        channel_name: str | None,
        duration_sec: int | None,
        transcript_source: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO hf_podcast_inputs (
                    run_id, url, video_id, title, channel_name, duration_sec, transcript_source, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    url,
                    video_id,
                    title,
                    channel_name,
                    duration_sec,
                    transcript_source,
                    _utc_now_iso(),
                ),
            )

    def has_dm_podcast_autorun(self, *, channel: str, user_id: str, thread_ts: str, url_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM hf_dm_podcast_autoruns
                WHERE channel=? AND user_id=? AND thread_ts=? AND url_hash=?
                LIMIT 1
                """,
                (channel, user_id, thread_ts, url_hash),
            ).fetchone()
        return row is not None

    def record_dm_podcast_autorun(self, *, channel: str, user_id: str, thread_ts: str, url_hash: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO hf_dm_podcast_autoruns (
                    channel, user_id, thread_ts, url_hash, created_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (channel, user_id, thread_ts, url_hash, _utc_now_iso()),
            )

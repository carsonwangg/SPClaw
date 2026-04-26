from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any


MEMORY_TIERS: dict[str, int | None] = {
    "permanent": None,
    "stable": 90 * 24 * 3600,
    "active": 14 * 24 * 3600,
    "session": 24 * 3600,
    "checkpoint": 4 * 3600,
}

REFRESH_ON_ACCESS_TIERS = {"stable", "active"}
FTS_STOPWORDS = {
    "what",
    "is",
    "my",
    "the",
    "a",
    "an",
    "when",
    "do",
    "you",
    "remember",
    "please",
}


@dataclass(frozen=True)
class FactCandidate:
    category: str
    entity: str
    fact_key: str
    fact_value: str
    rationale: str | None
    source: str
    source_ts_utc: str
    tier: str
    confidence: float = 1.0


@dataclass(frozen=True)
class MemoryHit:
    memory_id: int
    category: str
    entity: str
    fact_key: str
    fact_value: str
    rationale: str | None
    source: str
    source_ts_utc: str | None
    tier: str
    confidence: float
    score: float


@dataclass(frozen=True)
class CheckpointRecord:
    checkpoint_id: int
    scope: str
    action: str
    state_json: str
    expected_outcome: str
    files_json: str
    source: str
    source_ts_utc: str
    created_at_utc: str
    expires_at_utc: str | None


@dataclass(frozen=True)
class MemoryStats:
    facts_total: int
    facts_by_tier: dict[str, int]
    checkpoints_total: int
    events_total: int


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_utc(dt: datetime | None = None) -> str:
    return (dt or _utc_now()).astimezone(UTC).isoformat()


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _memory_db_path() -> Path:
    data_root = Path(os.environ.get("SPCLAW_DATA_ROOT", "/opt/spclaw-data"))
    return Path(os.environ.get("SPCLAW_MEMORY_DB_PATH", str(data_root / "db/memory.sqlite")))


def _normalize_text(value: str) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    return text[:4000]


class MemoryStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or _memory_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    fact_key TEXT NOT NULL,
                    fact_value TEXT NOT NULL,
                    rationale TEXT,
                    source TEXT NOT NULL,
                    source_ts_utc TEXT,
                    confidence REAL NOT NULL DEFAULT 1.0,
                    tier TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    last_accessed_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_unique ON facts(category, entity, fact_key, fact_value);
                CREATE INDEX IF NOT EXISTS idx_facts_expires ON facts(expires_at_utc);
                CREATE INDEX IF NOT EXISTS idx_facts_tier ON facts(tier);

                CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
                    category,
                    entity,
                    fact_key,
                    fact_value,
                    rationale,
                    content='facts',
                    content_rowid='id'
                );

                CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
                    INSERT INTO facts_fts(rowid, category, entity, fact_key, fact_value, rationale)
                    VALUES (new.id, new.category, new.entity, new.fact_key, new.fact_value, COALESCE(new.rationale, ''));
                END;

                CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
                    INSERT INTO facts_fts(facts_fts, rowid, category, entity, fact_key, fact_value, rationale)
                    VALUES ('delete', old.id, old.category, old.entity, old.fact_key, old.fact_value, COALESCE(old.rationale, ''));
                END;

                CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN
                    INSERT INTO facts_fts(facts_fts, rowid, category, entity, fact_key, fact_value, rationale)
                    VALUES ('delete', old.id, old.category, old.entity, old.fact_key, old.fact_value, COALESCE(old.rationale, ''));
                    INSERT INTO facts_fts(rowid, category, entity, fact_key, fact_value, rationale)
                    VALUES (new.id, new.category, new.entity, new.fact_key, new.fact_value, COALESCE(new.rationale, ''));
                END;

                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL,
                    action TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    expected_outcome TEXT NOT NULL,
                    files_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_ts_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_checkpoints_scope_created ON checkpoints(scope, created_at_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_expires ON checkpoints(expires_at_utc);

                CREATE TABLE IF NOT EXISTS memory_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT,
                    user_id TEXT,
                    text TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_ts_utc TEXT,
                    created_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_memory_events_created ON memory_events(created_at_utc DESC);
                """
            )
            conn.commit()

    def _expires_at(self, *, tier: str, now: datetime) -> str | None:
        ttl = MEMORY_TIERS.get(tier)
        if ttl is None:
            return None
        return _iso_utc(now + timedelta(seconds=ttl))

    def upsert_fact(self, candidate: FactCandidate, *, now: datetime | None = None) -> int:
        now_dt = now or _utc_now()
        tier = candidate.tier if candidate.tier in MEMORY_TIERS else "stable"
        expires = self._expires_at(tier=tier, now=now_dt)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO facts (
                    category, entity, fact_key, fact_value, rationale, source, source_ts_utc,
                    confidence, tier, created_at_utc, updated_at_utc, last_accessed_at_utc, expires_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(category, entity, fact_key, fact_value)
                DO UPDATE SET
                    rationale=excluded.rationale,
                    source=excluded.source,
                    source_ts_utc=excluded.source_ts_utc,
                    confidence=excluded.confidence,
                    tier=excluded.tier,
                    updated_at_utc=excluded.updated_at_utc,
                    last_accessed_at_utc=excluded.last_accessed_at_utc,
                    expires_at_utc=excluded.expires_at_utc
                """,
                (
                    _normalize_text(candidate.category),
                    _normalize_text(candidate.entity),
                    _normalize_text(candidate.fact_key),
                    _normalize_text(candidate.fact_value),
                    _normalize_text(candidate.rationale or "") or None,
                    _normalize_text(candidate.source),
                    candidate.source_ts_utc,
                    candidate.confidence,
                    tier,
                    _iso_utc(now_dt),
                    _iso_utc(now_dt),
                    _iso_utc(now_dt),
                    expires,
                ),
            )
            row = conn.execute(
                """
                SELECT id FROM facts
                WHERE category=? AND entity=? AND fact_key=? AND fact_value=?
                """,
                (
                    _normalize_text(candidate.category),
                    _normalize_text(candidate.entity),
                    _normalize_text(candidate.fact_key),
                    _normalize_text(candidate.fact_value),
                ),
            ).fetchone()
            conn.commit()
            if not row:
                raise RuntimeError("Failed to persist memory fact")
            return int(row["id"])

    def upsert_facts(self, candidates: list[FactCandidate], *, now: datetime | None = None) -> list[int]:
        ids: list[int] = []
        for candidate in candidates:
            ids.append(self.upsert_fact(candidate, now=now))
        return ids

    def _refresh_access(self, ids: list[int], *, now: datetime) -> None:
        if not ids:
            return
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT id, tier FROM facts WHERE id IN ({','.join('?' for _ in ids)})",
                ids,
            ).fetchall()

            for row in rows:
                tier = str(row["tier"])
                memory_id = int(row["id"])
                if tier in REFRESH_ON_ACCESS_TIERS:
                    expires = self._expires_at(tier=tier, now=now)
                    conn.execute(
                        "UPDATE facts SET last_accessed_at_utc=?, expires_at_utc=? WHERE id=?",
                        (_iso_utc(now), expires, memory_id),
                    )
                else:
                    conn.execute(
                        "UPDATE facts SET last_accessed_at_utc=? WHERE id=?",
                        (_iso_utc(now), memory_id),
                    )
            conn.commit()

    def query_structured(self, query: str, *, limit: int = 5, now: datetime | None = None) -> list[MemoryHit]:
        now_dt = now or _utc_now()
        tokens = [
            token
            for token in re.findall(r"[a-zA-Z0-9_]+", query.lower())
            if len(token) > 1 and token not in FTS_STOPWORDS
        ]
        if not tokens:
            return []
        cleaned = " OR ".join(f'"{token}"' for token in tokens)

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    f.id,
                    f.category,
                    f.entity,
                    f.fact_key,
                    f.fact_value,
                    f.rationale,
                    f.source,
                    f.source_ts_utc,
                    f.tier,
                    f.confidence,
                    bm25(facts_fts) AS score
                FROM facts_fts
                JOIN facts f ON f.id = facts_fts.rowid
                WHERE facts_fts MATCH ?
                  AND (f.expires_at_utc IS NULL OR f.expires_at_utc > ?)
                ORDER BY score ASC, f.confidence DESC, f.updated_at_utc DESC
                LIMIT ?
                """,
                (cleaned, _iso_utc(now_dt), limit),
            ).fetchall()

        hits = [
            MemoryHit(
                memory_id=int(row["id"]),
                category=str(row["category"]),
                entity=str(row["entity"]),
                fact_key=str(row["fact_key"]),
                fact_value=str(row["fact_value"]),
                rationale=(str(row["rationale"]) if row["rationale"] is not None else None),
                source=str(row["source"]),
                source_ts_utc=(str(row["source_ts_utc"]) if row["source_ts_utc"] else None),
                tier=str(row["tier"]),
                confidence=float(row["confidence"]),
                score=float(row["score"]),
            )
            for row in rows
        ]

        self._refresh_access([hit.memory_id for hit in hits], now=now_dt)
        return hits

    def write_checkpoint(
        self,
        *,
        scope: str,
        action: str,
        state: dict[str, Any],
        expected_outcome: str,
        files: list[str],
        source: str,
        source_ts_utc: str,
        now: datetime | None = None,
    ) -> int:
        now_dt = now or _utc_now()
        expires = self._expires_at(tier="checkpoint", now=now_dt)
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO checkpoints (
                    scope, action, state_json, expected_outcome, files_json,
                    source, source_ts_utc, created_at_utc, expires_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _normalize_text(scope),
                    _normalize_text(action),
                    json.dumps(state, sort_keys=True),
                    _normalize_text(expected_outcome),
                    json.dumps(files),
                    _normalize_text(source),
                    source_ts_utc,
                    _iso_utc(now_dt),
                    expires,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def latest_checkpoint(self, *, scope: str | None = None, now: datetime | None = None) -> CheckpointRecord | None:
        now_iso = _iso_utc(now or _utc_now())
        params: list[Any] = [now_iso]
        sql = (
            "SELECT * FROM checkpoints WHERE (expires_at_utc IS NULL OR expires_at_utc > ?)"
            " ORDER BY created_at_utc DESC LIMIT 1"
        )
        if scope:
            sql = (
                "SELECT * FROM checkpoints WHERE scope=? AND (expires_at_utc IS NULL OR expires_at_utc > ?)"
                " ORDER BY created_at_utc DESC LIMIT 1"
            )
            params = [scope, now_iso]

        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        if not row:
            return None
        return CheckpointRecord(
            checkpoint_id=int(row["id"]),
            scope=str(row["scope"]),
            action=str(row["action"]),
            state_json=str(row["state_json"]),
            expected_outcome=str(row["expected_outcome"]),
            files_json=str(row["files_json"]),
            source=str(row["source"]),
            source_ts_utc=str(row["source_ts_utc"]),
            created_at_utc=str(row["created_at_utc"]),
            expires_at_utc=(str(row["expires_at_utc"]) if row["expires_at_utc"] else None),
        )

    def log_event(
        self,
        *,
        channel: str | None,
        user_id: str | None,
        text: str,
        source: str,
        source_ts_utc: str | None = None,
        now: datetime | None = None,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO memory_events (channel, user_id, text, source, source_ts_utc, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    channel,
                    user_id,
                    _normalize_text(text),
                    source,
                    source_ts_utc,
                    _iso_utc(now or _utc_now()),
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def events_since_days(self, days: int, *, now: datetime | None = None) -> list[dict[str, Any]]:
        now_dt = now or _utc_now()
        since = _iso_utc(now_dt - timedelta(days=max(days, 0)))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, channel, user_id, text, source, source_ts_utc, created_at_utc
                FROM memory_events
                WHERE created_at_utc >= ?
                ORDER BY created_at_utc DESC
                """,
                (since,),
            ).fetchall()
        return [dict(row) for row in rows]

    def prune_expired(self, *, now: datetime | None = None) -> dict[str, int]:
        now_iso = _iso_utc(now or _utc_now())
        with self._connect() as conn:
            facts_deleted = conn.execute(
                "DELETE FROM facts WHERE expires_at_utc IS NOT NULL AND expires_at_utc <= ?",
                (now_iso,),
            ).rowcount
            checkpoints_deleted = conn.execute(
                "DELETE FROM checkpoints WHERE expires_at_utc IS NOT NULL AND expires_at_utc <= ?",
                (now_iso,),
            ).rowcount
            conn.commit()
        return {
            "facts_deleted": int(facts_deleted or 0),
            "checkpoints_deleted": int(checkpoints_deleted or 0),
        }

    def stats(self, *, now: datetime | None = None) -> MemoryStats:
        now_iso = _iso_utc(now or _utc_now())
        with self._connect() as conn:
            facts_total = conn.execute(
                "SELECT COUNT(*) AS c FROM facts WHERE expires_at_utc IS NULL OR expires_at_utc > ?",
                (now_iso,),
            ).fetchone()["c"]
            tier_rows = conn.execute(
                """
                SELECT tier, COUNT(*) AS c
                FROM facts
                WHERE expires_at_utc IS NULL OR expires_at_utc > ?
                GROUP BY tier
                """,
                (now_iso,),
            ).fetchall()
            checkpoints_total = conn.execute(
                "SELECT COUNT(*) AS c FROM checkpoints WHERE expires_at_utc IS NULL OR expires_at_utc > ?",
                (now_iso,),
            ).fetchone()["c"]
            events_total = conn.execute("SELECT COUNT(*) AS c FROM memory_events").fetchone()["c"]

        by_tier = {str(row["tier"]): int(row["c"]) for row in tier_rows}
        return MemoryStats(
            facts_total=int(facts_total),
            facts_by_tier=by_tier,
            checkpoints_total=int(checkpoints_total),
            events_total=int(events_total),
        )

    def latest_fact_value(
        self,
        *,
        category: str,
        entity: str,
        fact_key: str,
        now: datetime | None = None,
    ) -> str | None:
        now_iso = _iso_utc(now or _utc_now())
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT fact_value
                FROM facts
                WHERE category=? AND entity=? AND fact_key=?
                  AND (expires_at_utc IS NULL OR expires_at_utc > ?)
                ORDER BY updated_at_utc DESC
                LIMIT 1
                """,
                (
                    _normalize_text(category),
                    _normalize_text(entity),
                    _normalize_text(fact_key),
                    now_iso,
                ),
            ).fetchone()
        if not row:
            return None
        return str(row["fact_value"])

    def expire_facts(
        self,
        *,
        category: str,
        entity: str,
        fact_key: str,
        now: datetime | None = None,
    ) -> int:
        now_iso = _iso_utc(now or _utc_now())
        with self._connect() as conn:
            count = conn.execute(
                """
                UPDATE facts
                SET expires_at_utc=?
                WHERE category=? AND entity=? AND fact_key=?
                  AND (expires_at_utc IS NULL OR expires_at_utc > ?)
                """,
                (
                    now_iso,
                    _normalize_text(category),
                    _normalize_text(entity),
                    _normalize_text(fact_key),
                    now_iso,
                ),
            ).rowcount
            conn.commit()
        return int(count or 0)

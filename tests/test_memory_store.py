from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from coatue_claw.memory_store import FactCandidate, MemoryStore


def _candidate(*, key: str, value: str, tier: str = "stable") -> FactCandidate:
    return FactCandidate(
        category="profile",
        entity="user",
        fact_key=key,
        fact_value=value,
        rationale="test",
        source="unit-test",
        source_ts_utc="2026-02-18T00:00:00+00:00",
        tier=tier,
        confidence=1.0,
    )


def test_structured_insert_and_retrieval(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))
    store = MemoryStore()

    store.upsert_fact(_candidate(key="daughter birthday", value="June 3rd"))
    hits = store.query_structured("what is my daughter birthday", limit=3)

    assert hits
    assert hits[0].fact_value == "June 3rd"


def test_refresh_on_access_for_stable_tier(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))
    store = MemoryStore()

    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    store.upsert_fact(_candidate(key="preferred theme", value="dark mode", tier="stable"), now=t0)

    with store._connect() as conn:  # noqa: SLF001
        before = conn.execute("SELECT expires_at_utc FROM facts").fetchone()["expires_at_utc"]

    t1 = t0 + timedelta(days=10)
    store.query_structured("preferred theme", limit=1, now=t1)

    with store._connect() as conn:  # noqa: SLF001
        after = conn.execute("SELECT expires_at_utc FROM facts").fetchone()["expires_at_utc"]

    assert before != after


def test_prune_expired_facts_and_checkpoints(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))
    store = MemoryStore()

    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    store.upsert_fact(_candidate(key="temporary debug note", value="foo", tier="session"), now=t0)
    store.write_checkpoint(
        scope="pipeline",
        action="deploy",
        state={"step": 1},
        expected_outcome="success",
        files=["foo.py"],
        source="unit-test",
        source_ts_utc=t0.isoformat(),
        now=t0,
    )

    result = store.prune_expired(now=t0 + timedelta(days=2))
    assert result["facts_deleted"] >= 1
    assert result["checkpoints_deleted"] >= 1


def test_latest_checkpoint(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))
    store = MemoryStore()

    store.write_checkpoint(
        scope="pipeline",
        action="deploy_latest",
        state={"before": "abc123"},
        expected_outcome="restart and healthy probe",
        files=["Makefile"],
        source="unit-test",
        source_ts_utc="2026-02-18T00:00:00+00:00",
    )

    checkpoint = store.latest_checkpoint(scope="pipeline")
    assert checkpoint is not None
    assert checkpoint.action == "deploy_latest"

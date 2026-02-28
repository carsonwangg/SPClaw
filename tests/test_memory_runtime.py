from __future__ import annotations

from pathlib import Path

from coatue_claw.memory_runtime import MemoryRuntime, RetrievedMemory
from coatue_claw.memory_store import FactCandidate, MemoryStore


class _NoSemantic:
    enabled = False
    reason = "disabled-for-test"

    def upsert(self, *, memory_id: int, candidate: FactCandidate) -> None:  # noqa: ARG002
        return

    def query(self, text: str, *, limit: int = 3) -> list[RetrievedMemory]:  # noqa: ARG002
        return []


def test_runtime_ingest_and_retrieve(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))

    runtime = MemoryRuntime(store=MemoryStore(), semantic=_NoSemantic())
    persisted = runtime.ingest_message(
        channel="C123",
        user_id="U123",
        text="My daughter's birthday is June 3rd",
        source="unit-test",
        source_ts_utc="2026-02-18T00:00:00+00:00",
    )

    assert persisted

    response = runtime.format_retrieval("daughter birthday", limit=5)
    assert "June 3rd" in response


def test_extract_daily_dry_run_and_insert(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))

    runtime = MemoryRuntime(store=MemoryStore(), semantic=_NoSemantic())
    runtime.store.log_event(
        channel="C123",
        user_id="U123",
        text="We decided to use SQLite + FTS5 because queries are structured",
        source="unit-test",
        source_ts_utc="2026-02-18T00:00:00+00:00",
    )

    dry = runtime.extract_daily(days=30, dry_run=True)
    assert dry["facts_extracted"] >= 1
    assert dry["inserted"] == 0

    persisted = runtime.extract_daily(days=30, dry_run=False)
    assert persisted["inserted"] >= 1


def test_hfa_output_control_set_get_clear(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))
    runtime = MemoryRuntime(store=MemoryStore(), semantic=_NoSemantic())

    inserted = runtime.set_hfa_output_control(
        requested_by="U123",
        mode="freeform",
        instruction="Use a tighter PM memo format.",
        source_ts_utc="2026-02-28T00:00:00+00:00",
    )
    assert inserted

    control = runtime.get_hfa_output_control()
    assert control.get("mode") == "freeform"
    assert "tighter PM memo" in str(control.get("instruction") or "")

    cleared = runtime.clear_hfa_output_control()
    assert (cleared.get("mode_expired") or 0) >= 1
    assert (cleared.get("instruction_expired") or 0) >= 1
    assert runtime.get_hfa_output_control() == {}


def test_hfa_output_control_rejects_non_freeform_mode(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "memory.sqlite"
    monkeypatch.setenv("COATUE_CLAW_MEMORY_DB_PATH", str(db_path))
    runtime = MemoryRuntime(store=MemoryStore(), semantic=_NoSemantic())
    try:
        runtime.set_hfa_output_control(requested_by="U123", mode="strict")
        assert False, "expected ValueError for strict mode"
    except ValueError as exc:
        assert "freeform" in str(exc)

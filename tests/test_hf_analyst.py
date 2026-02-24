from __future__ import annotations

from pathlib import Path

from coatue_claw.hf_analyst import analyze_thread, file_set_hash, parse_hfa_intent, record_dm_autorun, should_run_dm_autorun
from coatue_claw.hf_store import HFStore


class _FakeMemoryRuntime:
    def __init__(self) -> None:
        self.calls = 0

    def ingest_hfa_facts(self, **kwargs):  # noqa: ANN003, D401
        self.calls += 1
        return [1, 2, 3]


def test_parse_hfa_intent() -> None:
    assert parse_hfa_intent("hfa status") == ("status", None)
    assert parse_hfa_intent("hfa analyze") == ("analyze", None)
    assert parse_hfa_intent("hfa analyze focus on valuation") == ("analyze", "focus on valuation")
    assert parse_hfa_intent("diligence SNOW")[0] is None


def test_file_set_hash_is_stable() -> None:
    a = file_set_hash(["F2", "F1", "F3"])
    b = file_set_hash(["F3", "F2", "F1"])
    assert a == b


def test_dm_autorun_guard(tmp_path: Path) -> None:
    db_path = tmp_path / "hfa.sqlite"
    store = HFStore(db_path=db_path)
    file_ids = ["F1", "F2"]
    assert should_run_dm_autorun(channel="D1", user_id="U1", thread_ts="1.1", file_ids=file_ids, store=store)
    record_dm_autorun(channel="D1", user_id="U1", thread_ts="1.1", file_ids=file_ids, store=store)
    assert not should_run_dm_autorun(channel="D1", user_id="U1", thread_ts="1.1", file_ids=file_ids, store=store)


def test_analyze_thread_fallback_mode(tmp_path: Path, monkeypatch) -> None:
    doc_path = tmp_path / "memo.txt"
    doc_path.write_text("SNOW demand commentary and product execution notes.", encoding="utf-8")
    db_path = tmp_path / "hfa.sqlite"
    artifact_dir = tmp_path / "artifacts"

    monkeypatch.setenv("COATUE_CLAW_HFA_DB_PATH", str(db_path))
    monkeypatch.setenv("COATUE_CLAW_HFA_ARTIFACT_DIR", str(artifact_dir))

    monkeypatch.setattr(
        "coatue_claw.hf_analyst._thread_file_rows",
        lambda **kwargs: [
            {
                "slack_file_id": "F123",
                "original_name": "memo.txt",
                "mimetype": "text/plain",
                "local_path": str(doc_path),
                "sha256": "abc123",
                "ingested_at_utc": "2026-02-24T00:00:00+00:00",
            }
        ],
    )
    monkeypatch.setattr("coatue_claw.hf_analyst._model_draft", lambda **kwargs: None)
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._market_context",
        lambda tickers, as_of_utc: (["SNOW market context"], [f"market source (timestamp_utc: `{as_of_utc}`)"]),
    )
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._web_context",
        lambda tickers, as_of_utc: (["SNOW web context"], [f"web source (timestamp_utc: `{as_of_utc}`)"], []),
    )

    memory = _FakeMemoryRuntime()
    result = analyze_thread(
        channel="D123",
        thread_ts="1700000000.100",
        question="focus on asymmetric setup",
        requested_by="U123",
        trigger_mode="test",
        dry_run=False,
        slack_client=object(),
        memory_runtime=memory,
    )

    assert result.files_analyzed == 1
    assert result.artifact_path is not None
    assert Path(result.artifact_path).exists()
    assert "## 1. AAA Snapshot" in result.markdown
    assert "## 7. Sources" in result.markdown
    assert memory.calls == 1

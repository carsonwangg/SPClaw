from __future__ import annotations

from pathlib import Path

import pytest

from coatue_claw.hf_analyst import (
    HFAError,
    analyze_podcast_url,
    analyze_thread,
    extract_youtube_urls,
    file_set_hash,
    parse_hfa_intent,
    record_dm_autorun,
    record_dm_podcast_autorun,
    should_run_dm_autorun,
    should_run_dm_podcast_autorun,
)
from coatue_claw.hf_store import HFStore


class _FakeMemoryRuntime:
    def __init__(self) -> None:
        self.calls = 0

    def ingest_hfa_facts(self, **kwargs):  # noqa: ANN003, D401
        self.calls += 1
        return [1, 2, 3]

    def get_hfa_output_control(self) -> dict[str, str]:
        return {}


def test_parse_hfa_intent() -> None:
    assert parse_hfa_intent("hfa status") == ("status", None)
    assert parse_hfa_intent("status") == ("status", None)
    assert parse_hfa_intent("hfa analyze") == ("analyze", None)
    assert parse_hfa_intent("analyze") == ("analyze", None)
    assert parse_hfa_intent("hfa analyze focus on valuation") == ("analyze", "focus on valuation")
    assert parse_hfa_intent("analyze focus on valuation") == ("analyze", "focus on valuation")
    assert parse_hfa_intent("hfa podcast https://youtu.be/abcDEF12345 macro focus")[0] == "podcast"
    assert parse_hfa_intent("podcast https://youtu.be/abcDEF12345 macro focus")[0] == "podcast"
    assert parse_hfa_intent("quotes https://youtu.be/abcDEF12345")[0] == "podcast"
    assert parse_hfa_intent("analyze https://youtu.be/abcDEF12345")[0] == "podcast"
    assert parse_hfa_intent("hfa analyze this podcast https://youtu.be/abcDEF12345")[0] == "podcast"
    assert parse_hfa_intent("hfa quotes for this podcast")[0] == "podcast"
    assert parse_hfa_intent("hfa summarize this youtube interview https://youtu.be/abcDEF12345")[0] == "podcast"
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


def test_dm_podcast_autorun_guard(tmp_path: Path) -> None:
    store = HFStore(db_path=tmp_path / "hfa.sqlite")
    url = "https://youtu.be/abcDEF12345"
    assert should_run_dm_podcast_autorun(channel="D1", user_id="U1", thread_ts="2.2", url=url, store=store)
    record_dm_podcast_autorun(channel="D1", user_id="U1", thread_ts="2.2", url=url, store=store)
    assert not should_run_dm_podcast_autorun(channel="D1", user_id="U1", thread_ts="2.2", url=url, store=store)


def test_extract_youtube_urls() -> None:
    text = "check this https://youtu.be/abcDEF12345 and https://youtube.com/watch?v=ZYX98765432"
    urls = extract_youtube_urls(text)
    assert len(urls) == 2


def test_analyze_thread_model_failure_returns_reason(tmp_path: Path, monkeypatch) -> None:
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
    monkeypatch.setattr("coatue_claw.hf_analyst._model_freeform_markdown", lambda **kwargs: (None, "forced_model_failure"))
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._market_context",
        lambda tickers, as_of_utc: (["SNOW market context"], [f"market source (timestamp_utc: `{as_of_utc}`)"]),
    )
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._web_context",
        lambda tickers, as_of_utc: (["SNOW web context"], [f"web source (timestamp_utc: `{as_of_utc}`)"], []),
    )

    memory = _FakeMemoryRuntime()
    with pytest.raises(HFAError, match="analysis_generation_failed:forced_model_failure"):
        analyze_thread(
            channel="D123",
            thread_ts="1700000000.100",
            question="focus on asymmetric setup",
            requested_by="U123",
            trigger_mode="test",
            dry_run=False,
            slack_client=object(),
            memory_runtime=memory,
        )
    assert memory.calls == 0


def test_analyze_podcast_url(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_HFA_DB_PATH", str(tmp_path / "hfa.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_HFA_ARTIFACT_DIR", str(tmp_path / "artifacts"))

    class _Transcript:
        url = "https://youtu.be/abcDEF12345"
        video_id = "abcDEF12345"
        title = "Test Podcast"
        channel_name = "Test Channel"
        duration_sec = 3600
        transcript_source = "captions"
        segments = ()
        full_text = "sample full text"

    class _Analysis:
        executive_summary = ("Point one", "Point two", "Point three")
        key_themes = ("Theme 1", "Theme 2", "Theme 3")
        quotes = ()
        confidence_label = "Medium"
        warnings = ()

    monkeypatch.setattr("coatue_claw.hf_analyst.fetch_youtube_transcript", lambda url: _Transcript())
    monkeypatch.setattr("coatue_claw.hf_analyst.build_podcast_analysis", lambda transcript, question=None, output_instruction=None: _Analysis())

    memory = _FakeMemoryRuntime()
    result = analyze_podcast_url(
        url="https://youtu.be/abcDEF12345",
        question="focus on moat",
        requested_by="U1",
        channel="D1",
        thread_ts="3.3",
        trigger_mode="test",
        dry_run=False,
        memory_runtime=memory,
    )
    assert result.run_id > 0
    assert result.artifact_path is not None
    assert Path(result.artifact_path).exists()
    assert "HFA podcast complete" in result.summary_text


def test_analyze_thread_freeform_mode(tmp_path: Path, monkeypatch) -> None:
    doc_path = tmp_path / "memo.txt"
    doc_path.write_text("AI demand appears resilient with mixed macro.", encoding="utf-8")
    monkeypatch.setenv("COATUE_CLAW_HFA_DB_PATH", str(tmp_path / "hfa.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_HFA_ARTIFACT_DIR", str(tmp_path / "artifacts"))
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
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._model_freeform_markdown",
        lambda **kwargs: ("# Freeform\n\nCustom output from operator mode.", None),
    )
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._market_context",
        lambda tickers, as_of_utc: (["MKT"], [f"market source (timestamp_utc: `{as_of_utc}`)"]),
    )
    monkeypatch.setattr(
        "coatue_claw.hf_analyst._web_context",
        lambda tickers, as_of_utc: (["WEB"], [f"web source (timestamp_utc: `{as_of_utc}`)"], []),
    )

    class _FreeformMemory(_FakeMemoryRuntime):
        def get_hfa_output_control(self) -> dict[str, str]:
            return {"mode": "freeform", "instruction": "Use short bullets."}

    memory = _FreeformMemory()
    result = analyze_thread(
        channel="D123",
        thread_ts="1700000000.100",
        question="focus",
        requested_by="U123",
        trigger_mode="test",
        dry_run=False,
        slack_client=object(),
        memory_runtime=memory,
    )
    assert "Freeform" in result.markdown
    assert result.scorecard.confidence_label == "Medium"

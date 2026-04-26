from __future__ import annotations

from pathlib import Path

from spclaw.spencer_change_log import (
    SpencerChangeLog,
    format_changes,
    is_spencer_user,
    looks_like_change_request,
    requester_label,
)


def test_is_spencer_user_defaults() -> None:
    assert is_spencer_user("U0AFJ5RS31C") is True
    assert is_spencer_user("U0AFJ5T6JTY") is True
    assert is_spencer_user("U0AGD28QSQG") is True
    assert is_spencer_user("U123") is False


def test_looks_like_change_request() -> None:
    assert looks_like_change_request("Can you change the bot so it posts chart updates to #charting?") is True
    assert looks_like_change_request("thanks") is False


def test_capture_update_and_list(tmp_path: Path) -> None:
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    change_id = log.capture_request(
        user_id="U0AFJ5RS31C",
        channel="C123",
        thread_ts="111.222",
        message_ts="111.222",
        text="Please update the bot to move this to #anduril",
    )
    log.update_status(change_id, status="implemented", note="Moved thread and restarted runtime.")
    rows = log.list_changes(limit=10)
    assert len(rows) == 1
    assert rows[0].status == "implemented"
    rendered = format_changes(rows)
    assert "#1" in rendered
    assert "anduril" in rendered.lower()
    assert "Spencer Peterson" in rendered


def test_requester_label_defaults() -> None:
    assert requester_label("U0AFJ5RS31C") == "Spencer Peterson"
    assert requester_label("U0AGD28QSQG") == "Carson Wang"


def test_capture_memory_git_request_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    change_id = log.capture_request(
        user_id="U0AGD28QSQG",
        channel="C123",
        thread_ts="222.333",
        message_ts="222.333",
        text="set default board-seat format for openai channel",
        request_kind="memory_git",
        trigger_mode="git_memory_prefix",
        source_ref="slack://C123/222.333/222.333 | memory:/Users/spclaw/.openclaw/workspace/memory/2026-02-24.md",
    )
    row = log.get_change(change_id)
    assert row is not None
    assert row.request_kind == "memory_git"
    assert row.trigger_mode == "git_memory_prefix"
    assert "slack://" in str(row.source_ref)


def test_capture_memory_git_request_auto_behavior_trigger_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    change_id = log.capture_request(
        user_id="U123",
        channel="C123",
        thread_ts="333.444",
        message_ts="333.444",
        text="please change bot behavior for chart follow-up",
        request_kind="memory_git",
        trigger_mode="auto_behavior_request",
    )
    row = log.get_change(change_id)
    assert row is not None
    assert row.request_kind == "memory_git"
    assert row.trigger_mode == "auto_behavior_request"


def test_list_changes_filter_request_kind(tmp_path: Path) -> None:
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AFJ5RS31C",
        channel="C1",
        thread_ts="1.1",
        message_ts="1.1",
        text="Please change channel routing.",
    )
    log.capture_request(
        user_id="U0AGD28QSQG",
        channel="C2",
        thread_ts="2.2",
        message_ts="2.2",
        text="track this git memory request",
        request_kind="memory_git",
        trigger_mode="git_memory_prefix",
        source_ref="slack://C2/2.2/2.2 | memory:/Users/spclaw/.openclaw/workspace/memory/2026-02-24.md",
    )
    rows = log.list_changes(limit=20, request_kind="memory_git")
    assert len(rows) == 1
    assert rows[0].request_kind == "memory_git"


def test_export_memory_git_queue_writes_markdown(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    monkeypatch.setenv("SPCLAW_REPO_PATH", str(repo_root))
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AGD28QSQG",
        channel="C2",
        thread_ts="2.2",
        message_ts="2.2",
        text="chart prompt should ask follow-up by default",
        request_kind="memory_git",
        trigger_mode="git_memory_prefix",
        source_ref="slack://C2/2.2/2.2 | memory:/Users/spclaw/.openclaw/workspace/memory/2026-02-24.md",
    )
    payload = log.export_memory_git_queue(limit=20)
    queue_path = Path(payload["queue_path"])
    assert payload["ok"] is True
    assert payload["count"] == 1
    assert queue_path.exists()
    text = queue_path.read_text(encoding="utf-8")
    assert "Memory Git Reconciliation Queue" in text
    assert "#1" in text


def test_reconcile_link_sets_commit_and_ledger(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    monkeypatch.setenv("SPCLAW_REPO_PATH", str(repo_root))
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    change_id = log.capture_request(
        user_id="U0AGD28QSQG",
        channel="C2",
        thread_ts="2.2",
        message_ts="2.2",
        text="make board seat output use named citations",
        request_kind="memory_git",
        trigger_mode="git_memory_prefix",
        source_ref="slack://C2/2.2/2.2 | memory:/Users/spclaw/.openclaw/workspace/memory/2026-02-24.md",
    )
    result = log.reconcile_link(
        ids=[change_id],
        commit="42d00dbc83478e67e4177806c3c6fddeb2290ed4",
        resolved_by="codex",
        note="implemented in batch session",
        mapped_paths="src/spclaw/slack_bot.py,src/spclaw/spencer_change_log.py",
    )
    assert result["ok"] is True
    assert result["updated"] == [change_id]
    row = log.get_change(change_id)
    assert row is not None
    assert row.status == "implemented"
    assert row.related_commit == "42d00dbc83478e67e4177806c3c6fddeb2290ed4"
    ledger_path = Path(result["ledger_path"])
    assert ledger_path.exists()
    ledger_text = ledger_path.read_text(encoding="utf-8")
    assert "related_commit" in ledger_text
    assert "batch_session_linked" in ledger_text


def test_reconcile_status_counts_only_memory_git(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    monkeypatch.setenv("SPCLAW_REPO_PATH", str(repo_root))
    db_path = tmp_path / "spencer.sqlite"
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AFJ5RS31C",
        channel="C1",
        thread_ts="1.1",
        message_ts="1.1",
        text="Please change the chart formatter.",
    )
    log.capture_request(
        user_id="U0AGD28QSQG",
        channel="C2",
        thread_ts="2.2",
        message_ts="2.2",
        text="track this git memory request",
        request_kind="memory_git",
        trigger_mode="git_memory_prefix",
    )
    stats = log.reconcile_status()
    assert stats["ok"] is True
    assert stats["request_kind"] == "memory_git"
    assert stats["total"] == 1
    assert stats["open"] == 1

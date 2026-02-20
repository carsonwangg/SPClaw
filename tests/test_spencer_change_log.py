from __future__ import annotations

from pathlib import Path

from coatue_claw.spencer_change_log import (
    SpencerChangeLog,
    format_changes,
    is_spencer_user,
    looks_like_change_request,
)


def test_is_spencer_user_defaults() -> None:
    assert is_spencer_user("U0AFJ5RS31C") is True
    assert is_spencer_user("U0AFJ5T6JTY") is True
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

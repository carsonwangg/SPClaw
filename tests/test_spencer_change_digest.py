from __future__ import annotations

from pathlib import Path

from spclaw.spencer_change_digest import run_once, status
from spclaw.spencer_change_log import SpencerChangeLog


def test_run_once_dry_run(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "spencer_changes.sqlite"
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DB_PATH", str(db_path))
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AFJ5RS31C",
        channel="C123",
        thread_ts="1.2",
        message_ts="1.2",
        text="Please change the bot to post this in #anduril.",
    )

    payload = run_once(dry_run=True)
    assert payload["ok"] is True
    assert payload["open_count"] >= 1
    assert "preview" in payload
    assert "#1" in payload["preview"]
    assert "Spencer Peterson" in payload["preview"]


def test_run_once_dry_run_includes_carson_label(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "spencer_changes.sqlite"
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DB_PATH", str(db_path))
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AGD28QSQG",
        channel="C123",
        thread_ts="1.4",
        message_ts="1.4",
        text="Please change how this behaves in #charting.",
    )

    payload = run_once(dry_run=True)
    assert payload["ok"] is True
    assert "Carson Wang" in payload["preview"]


def test_run_once_sends_and_dedupes(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "spencer_changes.sqlite"
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DB_PATH", str(db_path))
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS", "U_CARSON")
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AFJ5T6JTY",
        channel="C123",
        thread_ts="1.3",
        message_ts="1.3",
        text="Can you update the bot settings for this channel?",
    )

    sent_messages: list[dict[str, str]] = []

    class FakeClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def conversations_open(self, users: str):
            return {"channel": {"id": "D123"}}

        def chat_postMessage(self, channel: str, text: str):
            sent_messages.append({"channel": channel, "text": text})
            return {"ok": True, "ts": "123.456"}

    monkeypatch.setattr("spclaw.spencer_change_digest.WebClient", FakeClient)
    monkeypatch.setattr("spclaw.spencer_change_digest._slack_tokens", lambda: ["xoxb-test"])

    first = run_once()
    second = run_once()
    assert len(first["sent"]) == 1
    assert len(first["skipped"]) == 0
    assert len(second["sent"]) == 0
    assert len(second["skipped"]) == 1
    assert len(sent_messages) == 1


def test_status_reports_recent_runs(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "spencer_changes.sqlite"
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DB_PATH", str(db_path))
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS", "U_CARSON")
    log = SpencerChangeLog(db_path=db_path)
    change_id = log.capture_request(
        user_id="U0AFJ5RS31C",
        channel="C555",
        thread_ts="9.9",
        message_ts="9.9",
        text="Please fix chart title overlap.",
    )
    log.update_status(change_id, status="implemented", note="done")

    payload = status()
    assert payload["ok"] is True
    assert payload["recipients"] == ["U_CARSON"]
    assert "recent_runs" in payload


def test_missing_scope_falls_back_to_user_channel_post(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "spencer_changes.sqlite"
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DB_PATH", str(db_path))
    monkeypatch.setenv("SPCLAW_SPENCER_CHANGE_DIGEST_DM_USER_IDS", "U_CARSON")
    log = SpencerChangeLog(db_path=db_path)
    log.capture_request(
        user_id="U0AFJ5RS31C",
        channel="C123",
        thread_ts="2.2",
        message_ts="2.2",
        text="Please update this workflow in Slack.",
    )

    class FakeSlackApiError(Exception):
        def __init__(self, error: str) -> None:
            self.response = {"error": error}

    posted_channels: list[str] = []

    class FakeClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def conversations_open(self, users: str):
            raise FakeSlackApiError("missing_scope")

        def chat_postMessage(self, channel: str, text: str):
            posted_channels.append(channel)
            return {"ok": True, "ts": "123.999"}

    monkeypatch.setattr("spclaw.spencer_change_digest.WebClient", FakeClient)
    monkeypatch.setattr("spclaw.spencer_change_digest.SlackApiError", FakeSlackApiError)
    monkeypatch.setattr("spclaw.spencer_change_digest._slack_tokens", lambda: ["xoxb-test"])

    payload = run_once(force=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert posted_channels == ["U_CARSON"]

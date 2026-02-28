from __future__ import annotations

import importlib
import sys

import pytest


pytest.importorskip("slack_bolt")


def _import_slack_bot(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-token")
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "test-signing-secret")
    sys.modules.pop("coatue_claw.slack_bot", None)
    return importlib.import_module("coatue_claw.slack_bot")


def test_explicit_bs_command_fail_closed_on_handler_miss(monkeypatch) -> None:
    slack_bot = _import_slack_bot(monkeypatch)

    sent: list[str] = []

    def _say(*, text: str, thread_ts: str | None = None) -> None:
        sent.append(text)

    monkeypatch.setattr(slack_bot, "_handle_file_ingest_event", lambda **kwargs: None)
    monkeypatch.setattr(slack_bot, "_maybe_auto_run_hfa_podcast_dm", lambda **kwargs: False)
    monkeypatch.setattr(slack_bot, "_handle_hfa_command", lambda **kwargs: False)
    monkeypatch.setattr(slack_bot, "_handle_board_seat_command", lambda **kwargs: False)
    monkeypatch.setattr(slack_bot, "is_explicit_board_seat_command", lambda _text: True)

    def _unexpected_fallthrough(*args, **kwargs):
        raise AssertionError("unexpected fallthrough past fail-closed bs routing")

    monkeypatch.setattr(slack_bot, "_capture_spencer_change_request", _unexpected_fallthrough)

    slack_bot._handle_slack_request_event(
        event={"channel": "C123", "user": "U123", "ts": "111.222", "thread_ts": "111.222", "text": "bs now"},
        say=_say,
        source_event="message",
        memory_source="slack_message",
    )

    assert sent
    assert sent[0].startswith("Board Seat command routing failed.")
    assert "- `bs now`" in sent[0]
    assert "- `bs now dry`" in sent[0]
    assert "- `bs now for <Company>`" in sent[0]
    assert "- `bs status`" in sent[0]

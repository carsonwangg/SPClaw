from __future__ import annotations

from pathlib import Path

from coatue_claw import board_seat_daily


def test_parse_portcos_defaults_include_portco_list() -> None:
    parsed = board_seat_daily._parse_portcos("")
    names = {company for company, _ in parsed}
    assert "Anduril" in names
    assert "Anthropic" in names
    assert "Cursor" in names
    assert "Neuralink" in names
    assert "OpenAI" in names
    assert "Physical Intelligence" in names
    assert "Ramp" in names
    assert "SpaceX" in names
    assert "Stripe" in names
    assert "Sunday Robotics" in names


def test_parse_portcos_custom_mapping() -> None:
    parsed = board_seat_daily._parse_portcos("Anduril:anduril,OpenAI:openai")
    assert parsed == [("Anduril", "anduril"), ("OpenAI", "openai")]


def test_run_once_posts_and_dedupes_daily(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])
    monkeypatch.setattr(board_seat_daily, "_llm_message", lambda **kwargs: None)

    sent: list[dict[str, str]] = []

    class FakeWebClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def conversations_list(self, **kwargs):
            return {"channels": [{"id": "C_ANDURIL", "name": "anduril"}], "response_metadata": {"next_cursor": ""}}

        def conversations_history(self, **kwargs):
            return {
                "messages": [
                    {"text": "Anduril won a new DoD contract and expanded backlog.", "user": "U1"},
                ],
                "response_metadata": {"next_cursor": ""},
            }

        def chat_postMessage(self, channel: str, text: str):
            sent.append({"channel": channel, "text": text})
            return {"ok": True, "ts": "1771600000.100000"}

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)

    first = board_seat_daily.run_once(force=False, dry_run=False)
    assert first["ok"] is True
    assert len(first["sent"]) == 1
    assert first["sent"][0]["channel_id"] == "C_ANDURIL"
    assert len(sent) == 1
    assert "Board Seat as a Service" in sent[0]["text"]

    second = board_seat_daily.run_once(force=False, dry_run=False)
    assert second["ok"] is True
    assert len(second["sent"]) == 0
    assert len(second["skipped"]) == 1
    assert second["skipped"][0]["reason"] == "already_posted_today"


def test_run_once_dry_run_without_slack_sdk(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Cursor:cursor")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)

    payload = board_seat_daily.run_once(force=False, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["company"] == "Cursor"
    assert "preview" in payload["sent"][0]

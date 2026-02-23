from __future__ import annotations

from datetime import UTC, datetime
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


def test_resolve_channel_id_falls_back_to_name_on_missing_scope(monkeypatch) -> None:
    class FakeSlackApiError(Exception):
        def __init__(self, error: str) -> None:
            self.response = {"error": error}
            super().__init__(error)

    class FakeClient:
        def conversations_list(self, **kwargs):
            raise FakeSlackApiError("missing_scope")

    monkeypatch.setattr(board_seat_daily, "SlackApiError", FakeSlackApiError)
    channel = board_seat_daily._resolve_channel_id(FakeClient(), "anduril")
    assert channel == "anduril"


def test_extract_investment_text_uses_structured_lines() -> None:
    message = "\n".join(
        [
            "*Board Seat as a Service — Anduril*",
            "- Signal: Anduril backlog accelerated after a new DoD contract.",
            "- Board lens: Underwrite duration and margin path of the new award mix.",
            "- Watchlist: Monitor conversion of backlog to recognized revenue.",
            "- Team ask: Pressure-test top 3 risks in execution.",
        ]
    )
    extracted = board_seat_daily._extract_investment_text(message)
    assert "backlog accelerated" in extracted.lower()
    assert "board lens" not in extracted.lower()


def test_run_once_skips_repeat_without_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])

    store = board_seat_daily.BoardSeatStore()
    prior_message = "\n".join(
        [
            "*Board Seat as a Service — Anduril*",
            "- Signal: Anduril won a major DoD contract and backlog expanded.",
            "- Board lens: Validate sustainability of backlog conversion and margin quality.",
            "- Watchlist: Track conversion cadence and gross margin trend.",
            "- Team ask: Identify top execution risks this quarter.",
        ]
    )
    prior_investment = board_seat_daily._extract_investment_text(prior_message)
    prior_signature = board_seat_daily._token_signature(prior_investment)
    store.record_pitch(
        company="Anduril",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        source="seed",
        message_ts="1771000000.100000",
        run_date_local="2026-02-20",
        posted_at_utc=datetime(2026, 2, 20, 12, 0, tzinfo=UTC).isoformat(),
        message_text=prior_message,
        investment_text=prior_investment,
        investment_hash=board_seat_daily._stable_hash(prior_signature or prior_investment),
        investment_signature=prior_signature,
        context_signature=board_seat_daily._context_signature_from_snippets(
            ["Anduril won a major DoD contract and backlog expanded."]
        ),
        context_snippets=["Anduril won a major DoD contract and backlog expanded."],
        significant_change=False,
    )

    class FakeWebClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def conversations_list(self, **kwargs):
            return {"channels": [{"id": "C_ANDURIL", "name": "anduril"}], "response_metadata": {"next_cursor": ""}}

        def conversations_history(self, **kwargs):
            return {
                "messages": [
                    {"text": "Anduril won a major DoD contract and backlog expanded.", "user": "U1"},
                ],
                "response_metadata": {"next_cursor": ""},
            }

        def chat_postMessage(self, channel: str, text: str):
            raise AssertionError("Should not post repeated investment without significant change")

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_message",
        lambda **kwargs: prior_message,
    )

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    assert payload["skipped"][0]["reason"] == "repeat_investment_without_significant_change"


def test_run_once_allows_repeat_when_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])

    store = board_seat_daily.BoardSeatStore()
    prior_message = "\n".join(
        [
            "*Board Seat as a Service — Anduril*",
            "- Signal: Anduril won a major DoD contract and backlog expanded.",
            "- Board lens: Validate sustainability of backlog conversion and margin quality.",
            "- Watchlist: Track conversion cadence and gross margin trend.",
            "- Team ask: Identify top execution risks this quarter.",
        ]
    )
    prior_investment = board_seat_daily._extract_investment_text(prior_message)
    prior_signature = board_seat_daily._token_signature(prior_investment)
    store.record_pitch(
        company="Anduril",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        source="seed",
        message_ts="1771000000.100000",
        run_date_local="2026-02-20",
        posted_at_utc=datetime(2026, 2, 20, 12, 0, tzinfo=UTC).isoformat(),
        message_text=prior_message,
        investment_text=prior_investment,
        investment_hash=board_seat_daily._stable_hash(prior_signature or prior_investment),
        investment_signature=prior_signature,
        context_signature=board_seat_daily._context_signature_from_snippets(
            ["Anduril won a major DoD contract and backlog expanded."]
        ),
        context_snippets=["Anduril won a major DoD contract and backlog expanded."],
        significant_change=False,
    )

    sent: list[dict[str, str]] = []

    class FakeWebClient:
        def __init__(self, token: str) -> None:
            self.token = token

        def conversations_list(self, **kwargs):
            return {"channels": [{"id": "C_ANDURIL", "name": "anduril"}], "response_metadata": {"next_cursor": ""}}

        def conversations_history(self, **kwargs):
            return {
                "messages": [
                    {"text": "Anduril closed a large multi-year international contract worth $1.2B.", "user": "U1"},
                ],
                "response_metadata": {"next_cursor": ""},
            }

        def chat_postMessage(self, channel: str, text: str):
            sent.append({"channel": channel, "text": text})
            return {"ok": True, "ts": "1771600000.100000"}

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_message",
        lambda **kwargs: prior_message,
    )

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["significant_change"] is True
    assert len(sent) == 1


def test_backfill_channel_pitches_parses_history(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))

    store = board_seat_daily.BoardSeatStore()

    class FakeWebClient:
        def conversations_history(self, **kwargs):
            return {
                "messages": [
                    {
                        "ts": "1771600000.100000",
                        "text": "\n".join(
                            [
                                "*Board Seat as a Service — Anduril*",
                                "- Signal: Backlog is accelerating with new awards.",
                                "- Board lens: Underwrite backlog conversion quality.",
                                "- Watchlist: Margin and delivery cadence.",
                                "- Team ask: Validate top execution risks.",
                            ]
                        ),
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }

    stats = board_seat_daily._backfill_channel_pitches(
        store=store,
        client=FakeWebClient(),
        company="Anduril",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        max_messages=200,
    )
    assert stats["scanned"] == 1
    assert stats["matched"] == 1
    assert stats["inserted"] == 1
    assert store.pitch_count(company="Anduril") == 1

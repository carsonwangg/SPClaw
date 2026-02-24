from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from coatue_claw import board_seat_daily


def _funding(
    *,
    history: str = "Raised multiple rounds over time.",
    latest_round: str = "Series D",
    latest_date: str = "2025",
    backers: list[str] | None = None,
    source_type: str = "cache",
    as_of_utc: str | None = None,
) -> board_seat_daily.FundingSnapshot:
    return board_seat_daily.FundingSnapshot(
        history=history,
        latest_round=latest_round,
        latest_date=latest_date,
        backers=backers or ["Founders Fund", "General Catalyst"],
        source_urls=["https://example.com/funding"],
        source_type=source_type,
        as_of_utc=as_of_utc or datetime.now(UTC).isoformat(),
        confidence=0.8,
    )


def _seed_prior_pitch(
    *,
    store: board_seat_daily.BoardSeatStore,
    message: str,
    context_snippets: list[str],
) -> None:
    investment_text = board_seat_daily._extract_investment_text(message)
    core_text = board_seat_daily._core_investment_text(message)
    signature = board_seat_daily._token_signature(core_text)
    store.record_pitch(
        company="Anduril",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        source="seed",
        message_ts="1771000000.100000",
        run_date_local="2026-02-20",
        posted_at_utc=datetime(2026, 2, 20, 12, 0, tzinfo=UTC).isoformat(),
        message_text=message,
        investment_text=investment_text,
        investment_hash=board_seat_daily._stable_hash(signature or core_text or investment_text),
        investment_signature=signature,
        context_signature=board_seat_daily._context_signature_from_snippets(context_snippets),
        context_snippets=context_snippets,
        significant_change=False,
    )


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


def test_v2_message_structure_enforced_with_section_headers() -> None:
    draft = board_seat_daily._fallback_draft(
        company="Anduril",
        snippets=["Anduril expanded backlog with a large contract."],
        funding=_funding(),
    )
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=draft)
    assert "*Board Seat as a Service — Anduril*" in message
    assert "*Thesis*" in message
    assert "*Anduril context*" in message
    assert "*Funding snapshot*" in message
    assert "- Signal:" not in message
    assert "- Board lens:" not in message
    assert "- Watchlist:" not in message
    assert "- Team ask:" not in message


def test_v2_bullet_caps_enforced_max_six() -> None:
    very_long = " ".join(["word"] * 40)
    dirty = board_seat_daily.BoardSeatDraft(
        thesis_bullets=[very_long, very_long, very_long],
        context_bullets=[very_long, very_long, very_long],
        funding_bullets=[very_long, very_long, very_long],
    )
    clean = board_seat_daily._sanitize_draft(company="Anduril", draft=dirty, funding=_funding())
    assert len(clean.thesis_bullets) <= board_seat_daily.MAX_THESIS_BULLETS
    assert len(clean.context_bullets) <= board_seat_daily.MAX_CONTEXT_BULLETS
    assert len(clean.funding_bullets) <= board_seat_daily.MAX_FUNDING_BULLETS
    total = len(clean.thesis_bullets) + len(clean.context_bullets) + len(clean.funding_bullets)
    assert total <= board_seat_daily.MAX_TOTAL_BULLETS
    for bullet in clean.thesis_bullets + clean.context_bullets + clean.funding_bullets:
        assert len(bullet.split()) <= board_seat_daily.MAX_BULLET_WORDS


def test_extract_investment_text_parses_v2_sections() -> None:
    message = "\n".join(
        [
            "*Board Seat as a Service — Anduril*",
            "*Thesis*",
            "- Counter-UAS demand is accelerating in priority defense budgets.",
            "*Anduril context*",
            "- Fits existing autonomous systems roadmap and customer pull.",
            "*Funding snapshot*",
            "- History: raised across seed to late-stage rounds.",
            "- Latest round Series F (2024) backers: Founders Fund, General Catalyst.",
        ]
    )
    extracted = board_seat_daily._extract_investment_text(message)
    core = board_seat_daily._core_investment_text(message)
    assert "counter-uas demand" in extracted.lower()
    assert "latest round series f" in extracted.lower()
    assert "counter-uas demand" in core.lower()
    assert "latest round series f" not in core.lower()


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


def test_repeat_guardrail_still_blocks_duplicate_without_significant_change_v2(tmp_path: Path, monkeypatch) -> None:
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
            "*Thesis*",
            "- Anduril won a major DoD contract and backlog expanded.",
            "*Anduril context*",
            "- Validate backlog conversion quality and margin durability.",
            "*Funding snapshot*",
            "- History: raised across multiple rounds.",
            "- Latest round Series F (2024) backers: Founders Fund.",
        ]
    )
    _seed_prior_pitch(
        store=store,
        message=prior_message,
        context_snippets=["Anduril won a major DoD contract and backlog expanded."],
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
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_draft",
        lambda **kwargs: board_seat_daily.BoardSeatDraft(
            thesis_bullets=["Anduril won a major DoD contract and backlog expanded."],
            context_bullets=["Validate backlog conversion quality and margin durability."],
            funding_bullets=["History: raised across multiple rounds.", "Latest round Series F (2024) backers: Founders Fund."],
        ),
    )

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    assert payload["skipped"][0]["reason"] == "repeat_investment_without_significant_change"


def test_repeat_guardrail_allows_post_with_significant_change_v2(tmp_path: Path, monkeypatch) -> None:
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
            "*Thesis*",
            "- Anduril won a major DoD contract and backlog expanded.",
            "*Anduril context*",
            "- Validate backlog conversion quality and margin durability.",
            "*Funding snapshot*",
            "- History: raised across multiple rounds.",
            "- Latest round Series F (2024) backers: Founders Fund.",
        ]
    )
    _seed_prior_pitch(
        store=store,
        message=prior_message,
        context_snippets=["Anduril won a major DoD contract and backlog expanded."],
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
                    {"text": "Anduril signed a new $1.2B multi-year international contract.", "user": "U1"},
                ],
                "response_metadata": {"next_cursor": ""},
            }

        def chat_postMessage(self, channel: str, text: str):
            sent.append({"channel": channel, "text": text})
            return {"ok": True, "ts": "1771600000.100000"}

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_draft",
        lambda **kwargs: board_seat_daily.BoardSeatDraft(
            thesis_bullets=["Anduril won a major DoD contract and backlog expanded."],
            context_bullets=["Validate backlog conversion quality and margin durability."],
            funding_bullets=["History: raised across multiple rounds.", "Latest round Series F (2024) backers: Founders Fund."],
        ),
    )

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["significant_change"] is True
    assert payload["sent"][0]["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
    assert len(sent) == 1


def test_funding_resolver_prefers_manual_seed_over_cache_and_web(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))

    manual_path = tmp_path / "funding_manual.json"
    manual_path.write_text(
        json.dumps(
            {
                "Anduril": {
                    "history": "Manual seeded funding history.",
                    "latest_round": "Series F",
                    "latest_date": "2024",
                    "backers": ["Founders Fund"],
                    "source_urls": ["https://manual.example"],
                    "confidence": 0.99,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH", str(manual_path))

    store = board_seat_daily.BoardSeatStore()
    store.upsert_funding_snapshot(company="Anduril", snapshot=_funding(history="Cached history."))

    called = {"web": 0}

    def _fake_refresh(**kwargs):
        called["web"] += 1
        return _funding(history="Web history.", source_type="web_refresh")

    monkeypatch.setattr(board_seat_daily, "_refresh_funding_snapshot_from_web", _fake_refresh)
    snapshot = board_seat_daily._resolve_funding_snapshot(store=store, company="Anduril")
    assert snapshot.source_type == "manual_seed"
    assert "manual seeded" in snapshot.history.lower()
    assert called["web"] == 0


def test_funding_resolver_uses_cache_when_fresh(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH", raising=False)
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_FUNDING_TTL_DAYS", "14")

    store = board_seat_daily.BoardSeatStore()
    cached = _funding(history="Fresh cache history.", source_type="web_refresh", as_of_utc=datetime.now(UTC).isoformat())
    store.upsert_funding_snapshot(company="Anduril", snapshot=cached)

    monkeypatch.setattr(
        board_seat_daily,
        "_refresh_funding_snapshot_from_web",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("web refresh should not run for fresh cache")),
    )

    snapshot = board_seat_daily._resolve_funding_snapshot(store=store, company="Anduril")
    assert snapshot.source_type == "cache"
    assert "fresh cache" in snapshot.history.lower()


def test_funding_resolver_web_refreshes_when_cache_stale(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH", raising=False)
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_FUNDING_TTL_DAYS", "1")

    store = board_seat_daily.BoardSeatStore()
    stale_as_of = (datetime.now(UTC) - timedelta(days=5)).isoformat()
    store.upsert_funding_snapshot(company="Anduril", snapshot=_funding(history="Stale cache history.", as_of_utc=stale_as_of))

    refreshed = _funding(history="Web refreshed history.", source_type="web_refresh")
    monkeypatch.setattr(board_seat_daily, "_refresh_funding_snapshot_from_web", lambda **kwargs: refreshed)

    snapshot = board_seat_daily._resolve_funding_snapshot(store=store, company="Anduril")
    assert snapshot.source_type == "web_refresh"
    assert "web refreshed" in snapshot.history.lower()
    cached = store.get_funding_snapshot(company="Anduril")
    assert cached is not None
    assert "web refreshed" in cached.history.lower()


def test_funding_resolver_returns_explicit_unknown_when_unavailable(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("COATUE_CLAW_BOARD_SEAT_FUNDING_MANUAL_PATH", raising=False)
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))

    store = board_seat_daily.BoardSeatStore()
    monkeypatch.setattr(board_seat_daily, "_refresh_funding_snapshot_from_web", lambda **kwargs: None)
    snapshot = board_seat_daily._resolve_funding_snapshot(store=store, company="Anduril")
    assert snapshot.source_type == "unknown"
    assert board_seat_daily._funding_bullets_from_snapshot(snapshot) == [board_seat_daily.UNKNOWN_FUNDING_TEXT]


def test_run_once_dry_run_v2_contains_thesis_context_funding(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Cursor:cursor")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))

    payload = board_seat_daily.run_once(force=False, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert row["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
    assert "*Thesis*" in row["preview"]
    assert "*Cursor context*" in row["preview"]
    assert "*Funding snapshot*" in row["preview"]


def test_run_once_applies_v2_to_all_portcos(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril,OpenAI:openai")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))

    payload = board_seat_daily.run_once(force=False, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 2
    for row in payload["sent"]:
        assert row["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
        assert "*Thesis*" in row["preview"]
        assert "*Funding snapshot*" in row["preview"]

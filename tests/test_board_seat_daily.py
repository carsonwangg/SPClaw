from __future__ import annotations

from datetime import UTC, datetime
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


def _v4_draft() -> board_seat_daily.BoardSeatDraft:
    return board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Saronic to accelerate autonomous maritime deployment advantage.",
        why_now="Defense demand is accelerating and budgets are prioritizing autonomous systems now.",
        whats_different="Anduril can deploy integrated hardware-software stacks faster than incumbents in contested environments.",
        mos_risks="Key risks are procurement delay, hardware margin volatility, and integration complexity at scale.",
        bottom_line="Prioritize one partner-led expansion plan with measurable milestones over the next 12 months.",
        context_current_efforts="Anduril already runs core autonomy programs across border, air-defense, and mission systems.",
        context_domain_fit_gaps="Best fit is strongest where current roadmap closes deployment gaps with channel partnerships.",
        funding_history="Raised capital across multiple rounds to support platform expansion.",
        funding_latest_round_backers="Series F (2024) led by Founders Fund and key strategic backers.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Reuters",
                title="Anduril explores defense acquisitions",
                url="https://www.reuters.com/markets/deals/example",
            ),
            board_seat_daily.SourceRef(
                name_or_publisher="TechCrunch",
                title="Saronic raises new funding round",
                url="https://techcrunch.com/example",
            ),
        ],
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


def test_v4_message_structure_includes_explicit_idea_line() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v4_draft())
    assert "*Board Seat as a Service — Anduril*" in message
    assert "*Thesis*" in message
    assert "*Idea:* Acquire Saronic to accelerate autonomous maritime deployment advantage." in message
    assert "*Anduril context*" in message
    assert "*Funding snapshot*" in message
    assert "*Sources*" in message
    assert "\n- " not in message


def test_v4_rejects_non_acquisition_idea_line() -> None:
    draft = _v4_draft()
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Build an internal product layer for identity controls.",
        why_now=draft.why_now,
        whats_different=draft.whats_different,
        mos_risks=draft.mos_risks,
        bottom_line=draft.bottom_line,
        context_current_efforts=draft.context_current_efforts,
        context_domain_fit_gaps=draft.context_domain_fit_gaps,
        funding_history=draft.funding_history,
        funding_latest_round_backers=draft.funding_latest_round_backers,
        source_refs=draft.source_refs,
    )
    errors = board_seat_daily._validate_draft(draft)
    assert "idea_line_invalid" in errors


def test_v4_best_effort_fallback_still_emits_acq_candidate() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Build it internally",
        why_now="AI workflow demand is growing quickly.",
        whats_different="Cross-product data plane creates leverage.",
        mos_risks="Execution and integration risk remain.",
        bottom_line="Move quickly with high-conviction focus.",
        context_current_efforts="OpenAI has enterprise channel momentum.",
        context_domain_fit_gaps="Gaps exist in external workflow ownership.",
        funding_history="Funding history available.",
        funding_latest_round_backers="Latest backers include strategic partners.",
        source_refs=[],
    )
    clean = board_seat_daily._sanitize_draft(
        company="OpenAI",
        draft=draft,
        funding=_funding(),
        acquisition_rows=[],
    )
    assert clean.idea_line.lower().startswith("acquire ")
    assert board_seat_daily._is_valid_acquisition_idea_line(clean.idea_line) is True


def test_sources_render_named_title_lines_not_source_numbers() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v4_draft())
    assert "*Reuters — Anduril explores defense acquisitions:* <https://www.reuters.com/markets/deals/example>" in message
    assert "*TechCrunch — Saronic raises new funding round:* <https://techcrunch.com/example>" in message


def test_sources_never_emit_source_number_labels() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v4_draft())
    assert "Source 1" not in message
    assert "Source 2" not in message
    assert "Source 3" not in message


def test_extract_investment_text_parses_v4_idea_line() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v4_draft())
    extracted = board_seat_daily._extract_investment_text(message)
    core = board_seat_daily._core_investment_text(message)
    assert "acquire saronic" in extracted.lower()
    assert "acquire saronic" in core.lower()
    assert "series f" in extracted.lower()
    assert "series f" not in core.lower()


def test_extract_investment_text_parses_legacy_v2_and_old5() -> None:
    v2_message = "\n".join(
        [
            "*Board Seat as a Service — Anduril*",
            "*Thesis*",
            "- Backlog accelerated after a large contract.",
            "*Anduril context*",
            "- Existing product roadmap supports deployment.",
            "*Funding snapshot*",
            "- History: raised across rounds.",
            "- Latest round Series F backers include Founders Fund.",
        ]
    )
    old_message = "\n".join(
        [
            "*Board Seat as a Service — Anduril*",
            "- Signal: Backlog accelerated after a large contract.",
            "- Board lens: Validate conversion quality and margin durability.",
            "- Watchlist: Margin and delivery cadence.",
            "- Team ask: Validate top execution risks.",
        ]
    )
    assert "backlog accelerated" in board_seat_daily._extract_investment_text(v2_message).lower()
    assert "backlog accelerated" in board_seat_daily._extract_investment_text(old_message).lower()


def test_repeat_guardrail_v4_still_blocks_duplicate_without_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])

    store = board_seat_daily.BoardSeatStore()
    prior_message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v4_draft())
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
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v4_draft())
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(board_seat_daily, "_build_novel_fallback_draft", lambda **kwargs: _v4_draft())

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    assert payload["skipped"][0]["reason"] == "repeat_investment_without_significant_change"


def test_repeat_guardrail_v4_allows_with_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])

    store = board_seat_daily.BoardSeatStore()
    prior_message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v4_draft())
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
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v4_draft())
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["significant_change"] is True
    assert payload["sent"][0]["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
    assert len(sent) == 1


def test_unknown_funding_renders_two_explicit_unknown_labeled_lines() -> None:
    unknown = board_seat_daily._empty_funding_snapshot(source_type="unknown")
    history, latest = board_seat_daily._funding_lines_from_snapshot(unknown)
    assert history == board_seat_daily.UNKNOWN_FUNDING_TEXT
    assert latest == board_seat_daily.UNKNOWN_FUNDING_TEXT


def test_line_word_caps_enforced_for_v4_fields() -> None:
    very_long = " ".join(["word"] * 40)
    draft = board_seat_daily.BoardSeatDraft(
        idea_line=very_long,
        why_now=very_long,
        whats_different=very_long,
        mos_risks=very_long,
        bottom_line=very_long,
        context_current_efforts=very_long,
        context_domain_fit_gaps=very_long,
        funding_history=very_long,
        funding_latest_round_backers=very_long,
        source_refs=[],
    )
    clean = board_seat_daily._sanitize_draft(company="Anduril", draft=draft, funding=_funding(), acquisition_rows=[])
    fields = [
        clean.idea_line,
        clean.why_now,
        clean.whats_different,
        clean.mos_risks,
        clean.bottom_line,
        clean.context_current_efforts,
        clean.context_domain_fit_gaps,
        clean.funding_history,
        clean.funding_latest_round_backers,
    ]
    assert all(len(item.split()) <= board_seat_daily.MAX_LINE_WORDS for item in fields)


def test_run_once_dry_run_v4_contains_hierarchy_sections(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Cursor:cursor")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])

    payload = board_seat_daily.run_once(force=False, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert row["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
    assert "*Thesis*" in row["preview"]
    assert "*Idea:*" in row["preview"]
    assert "*Current efforts:*" in row["preview"]
    assert "*Funding snapshot*" in row["preview"]
    assert "*Sources*" in row["preview"]


def test_render_board_seat_message_uses_fallback_sources_when_missing() -> None:
    message = board_seat_daily._render_board_seat_message(
        company="Anduril",
        draft=board_seat_daily.BoardSeatDraft(
            idea_line="Acquire Saronic to accelerate maritime autonomy programs.",
            why_now="Defense demand is accelerating.",
            whats_different="Integrated autonomy stack deploys quickly.",
            mos_risks="Execution and procurement timing remain risks.",
            bottom_line="High-upside candidate if milestones are validated.",
            context_current_efforts="Anduril is scaling deployed autonomy programs.",
            context_domain_fit_gaps="Fit is strong; gaps are partner coverage and sustainment.",
            funding_history="Funding details are currently unavailable.",
            funding_latest_round_backers="Funding details are currently unavailable.",
            source_refs=[],
        ),
    )
    assert "*Sources*" in message
    assert "*Google Search — Anduril acquisitions and acquihires:* <https://www.google.com/search?q=Anduril+acquisitions+acquihires+startup>" in message
    assert "Source 1" not in message


def test_backfill_channel_pitches_parses_legacy_history(tmp_path: Path, monkeypatch) -> None:
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

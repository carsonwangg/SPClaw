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
    source_urls: list[str] | None = None,
    source_type: str = "cache",
    as_of_utc: str | None = None,
) -> board_seat_daily.FundingSnapshot:
    return board_seat_daily.FundingSnapshot(
        history=history,
        latest_round=latest_round,
        latest_date=latest_date,
        backers=backers or ["Founders Fund", "General Catalyst"],
        source_urls=source_urls or ["https://example.com/funding"],
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


def _v6_draft() -> board_seat_daily.BoardSeatDraft:
    return board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Saronic to accelerate autonomous maritime deployment advantage.",
        target_does="Builds enterprise software and automation infrastructure for production workflows.",
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


def test_v6_message_structure_includes_target_does_line() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v6_draft())
    assert "*Board Seat as a Service — Anduril*" in message
    assert "*Thesis*" in message
    assert "*Idea:* Acquire Saronic to accelerate autonomous maritime deployment advantage." in message
    assert "*Target does:*" in message
    assert "*Anduril context*" in message
    assert "*Funding snapshot*" in message
    assert "*Sources*" in message
    assert "\n- " not in message


def test_v6_rejects_non_acquisition_idea_line() -> None:
    draft = _v6_draft()
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Build an internal product layer for identity controls.",
        target_does=draft.target_does,
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


def test_v6_best_effort_fallback_still_emits_acq_candidate() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Build it internally",
        target_does="Builds enterprise software and automation infrastructure for production workflows.",
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


def test_best_effort_idea_line_avoids_placeholder_no_target() -> None:
    line = board_seat_daily._best_effort_idea_line(
        company="OpenAI",
        seed_text="Over the past month, enterprise demand shifted toward agent reliability and governance.",
    )
    assert line.lower().startswith("acquire ")
    assert "acquire no " not in line.lower()
    assert "browserbase" in line.lower()
    assert board_seat_daily._is_valid_acquisition_idea_line(line) is True


def test_best_effort_idea_line_rejects_stealth_placeholder_and_uses_named_company() -> None:
    line = board_seat_daily._best_effort_idea_line(
        company="OpenAI",
        seed_text="Acquire Stealth AI Systems to improve reliability.",
    )
    assert "stealth ai systems" not in line.lower()
    assert "browserbase" in line.lower()


def test_sources_render_named_title_lines_not_source_numbers() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v6_draft())
    assert "*Reuters — Anduril explores defense acquisitions:* <https://www.reuters.com/markets/deals/example>" in message
    assert "*TechCrunch — Saronic raises new funding round:* <https://techcrunch.com/example>" in message


def test_sources_never_emit_source_number_labels() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v6_draft())
    assert "Source 1" not in message
    assert "Source 2" not in message
    assert "Source 3" not in message


def test_extract_investment_text_parses_v5_idea_line() -> None:
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v6_draft())
    extracted = board_seat_daily._extract_investment_text(message)
    core = board_seat_daily._core_investment_text(message)
    assert "acquire saronic" in extracted.lower()
    assert "acquire saronic" in core.lower()
    assert "series f" in extracted.lower()
    assert "series f" not in core.lower()


def test_validate_draft_rejects_24h_why_now_phrasing() -> None:
    draft = _v6_draft()
    bad = board_seat_daily.BoardSeatDraft(
        idea_line=draft.idea_line,
        target_does=draft.target_does,
        why_now="No high-signal updates surfaced for OpenAI in the last 24 hours.",
        whats_different=draft.whats_different,
        mos_risks=draft.mos_risks,
        bottom_line=draft.bottom_line,
        context_current_efforts=draft.context_current_efforts,
        context_domain_fit_gaps=draft.context_domain_fit_gaps,
        funding_history=draft.funding_history,
        funding_latest_round_backers=draft.funding_latest_round_backers,
        source_refs=draft.source_refs,
    )
    errors = board_seat_daily._validate_draft(bad)
    assert "why_now_24h_disallowed" in errors


def test_render_board_seat_blocks_headers_are_bold_and_underlined() -> None:
    blocks = board_seat_daily._render_board_seat_blocks(company="Anduril", draft=_v6_draft())
    header_texts = {"Board Seat as a Service — Anduril", "Thesis", "Anduril context", "Funding snapshot", "Sources"}
    seen: set[str] = set()
    for block in blocks:
        if block.get("type") != "rich_text":
            continue
        for element in block.get("elements", []):
            if element.get("type") != "rich_text_section":
                continue
            for text_el in element.get("elements", []):
                if text_el.get("type") != "text":
                    continue
                text = str(text_el.get("text") or "")
                style = text_el.get("style") or {}
                if text in header_texts:
                    assert style.get("bold") is True
                    assert style.get("underline") is True
                    seen.add(text)
    assert header_texts.issubset(seen)


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


def test_repeat_guardrail_v5_still_blocks_duplicate_without_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])

    store = board_seat_daily.BoardSeatStore()
    prior_message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v6_draft())
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

        def chat_postMessage(self, channel: str, text: str, **kwargs):
            raise AssertionError("Should not post repeated investment without significant change")

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(board_seat_daily, "_build_novel_fallback_draft", lambda **kwargs: _v6_draft())

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    assert payload["skipped"][0]["reason"] == "repeat_investment_without_significant_change"


def test_repeat_guardrail_v5_allows_with_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])

    store = board_seat_daily.BoardSeatStore()
    prior_message = board_seat_daily._render_board_seat_message(company="Anduril", draft=_v6_draft())
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

        def chat_postMessage(self, channel: str, text: str, **kwargs):
            sent.append({"channel": channel, "text": text, "blocks": kwargs.get("blocks")})
            return {"ok": True, "ts": "1771600000.100000"}

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())
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


def test_line_word_caps_enforced_for_v5_fields() -> None:
    very_long = " ".join(["word"] * 40)
    draft = board_seat_daily.BoardSeatDraft(
        idea_line=very_long,
        target_does="Builds enterprise software and automation infrastructure for production workflows.",
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


def test_run_once_dry_run_v5_contains_hierarchy_sections(tmp_path: Path, monkeypatch) -> None:
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
            target_does="Builds enterprise software and automation infrastructure for production workflows.",
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
    assert "*Google Search — Saronic enterprise fit:* <https://www.google.com/search?q=Saronic+company+product+customers>" in message
    assert "Source 1" not in message


def test_source_classifier_tags_target_parent_and_funding() -> None:
    target = "Browserbase"
    target_tokens = {"browserbase"}
    target_kind = board_seat_daily._classify_source_ref(
        company="OpenAI",
        target=target,
        target_tokens=target_tokens,
        text_blob="Browserbase launches enterprise browser automation runtime",
    )
    parent_kind = board_seat_daily._classify_source_ref(
        company="OpenAI",
        target=target,
        target_tokens=target_tokens,
        text_blob="OpenAI expands enterprise GTM motions with partners",
    )
    funding_kind = board_seat_daily._classify_source_ref(
        company="OpenAI",
        target=target,
        target_tokens=target_tokens,
        text_blob="Reuters reports SoftBank funding round valuation update",
    )
    assert target_kind == "target_direct"
    assert parent_kind == "parent_context"
    assert funding_kind == "funding_context"


def test_source_composer_v5_target_first_excludes_funding_links(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_SOURCE_POLICY", "target_first_3_1")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_INCLUDE_FUNDING_LINKS", "0")

    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Browserbase to harden enterprise browser-agent execution.",
        target_does="Builds enterprise software and automation infrastructure for production workflows.",
        why_now="Computer-use agents need reliable execution controls.",
        whats_different="Runtime and policy telemetry become a distribution moat.",
        mos_risks="Execution quality and platform bundling risk remain.",
        bottom_line="Own execution reliability for enterprise workflows.",
        context_current_efforts="OpenAI has enterprise distribution and model leadership.",
        context_domain_fit_gaps="Gap is deterministic browser automation and governance.",
        funding_history="Funding details are currently unavailable.",
        funding_latest_round_backers="Funding details are currently unavailable.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Reuters",
                title="OpenAI in talks to raise up to $40B led by SoftBank",
                url="https://www.reuters.com/technology/openai-talks-raise-up-40-billion-softbank-wsj-reports-2025-01-30/",
            )
        ],
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_target_search_rows",
        lambda **kwargs: [
            {
                "publisher": "Browserbase",
                "title": "Browserbase enterprise browser automation",
                "snippet": "Secure browser automation control plane for agents.",
                "url": "https://www.browserbase.com/",
            },
                {
                    "publisher": "Pulse2",
                    "title": "Browserbase expands enterprise automation controls",
                    "snippet": "Infrastructure for reliable browser tasks with stronger governance controls.",
                    "url": "https://pulse2.com/browserbase-web-browser-automation-company-raises-21-million-series-a/",
                },
            {
                "publisher": "TechCrunch",
                "title": "Browserbase launches cloud browser platform",
                "snippet": "Platform focused on auditable browser workflows.",
                "url": "https://techcrunch.com/example-browserbase-platform/",
            },
        ],
    )
    selection = board_seat_daily._build_source_refs(
        company="OpenAI",
        draft=draft,
        funding=_funding(
            source_urls=[
                "https://www.reuters.com/technology/openai-talks-raise-up-40-billion-softbank-wsj-reports-2025-01-30/"
            ]
        ),
        acquisition_rows=[
            {
                "publisher": "OpenTools",
                "title": "OpenAI evaluates browser runtime targets",
                "snippet": "Target would improve policy controls for agents.",
                "url": "https://opentools.ai/news/openai-evaluates-browser-runtime-targets",
            }
        ],
    )
    assert len(selection.refs) <= 4
    target, tokens = board_seat_daily._extract_target_tokens_from_idea(draft.idea_line)
    target_categories = [
        board_seat_daily._classify_source_ref(
            company="OpenAI",
            target=target,
            target_tokens=tokens,
            text_blob=f"{ref.title} {ref.url}",
        )
        for ref in selection.refs
    ]
    assert sum(category in {"target_direct", "target_proxy"} for category in target_categories) >= 3
    assert all("softbank" not in ref.title.lower() for ref in selection.refs)
    assert all("reuters.com/technology/openai-talks-raise-up-40-billion-softbank-wsj-reports-2025-01-30/" not in ref.url for ref in selection.refs)
    assert selection.confidence in {"High", "Medium"}


def test_low_signal_candidate_mode_sets_low_confidence_without_parent_funding(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_INCLUDE_FUNDING_LINKS", "0")
    monkeypatch.setattr(board_seat_daily, "_target_search_rows", lambda **kwargs: [])
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Browserbase to improve browser automation reliability.",
        target_does="Builds enterprise software and automation infrastructure for production workflows.",
        why_now="Need better runtime reliability for enterprise agents.",
        whats_different="Execution guardrails create defensibility.",
        mos_risks="Bundling risk from hyperscalers remains.",
        bottom_line="A focused target could close execution gaps quickly.",
        context_current_efforts="OpenAI has enterprise momentum.",
        context_domain_fit_gaps="Control plane capabilities remain limited.",
        funding_history="Funding details are currently unavailable.",
        funding_latest_round_backers="Funding details are currently unavailable.",
        source_refs=[],
    )
    selection = board_seat_daily._build_source_refs(
        company="OpenAI",
        draft=draft,
        funding=_funding(
            source_urls=["https://www.reuters.com/technology/openai-talks-raise-up-40-billion-softbank-wsj-reports-2025-01-30/"]
        ),
        acquisition_rows=[],
    )
    assert selection.confidence == "Low"
    assert len(selection.refs) >= 1
    assert all("funding latest round backers" not in ref.title.lower() for ref in selection.refs)


def test_resolve_funding_prefers_crunchbase_primary(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_CRUNCHBASE_ENABLED", "1")
    monkeypatch.setenv("COATUE_CLAW_CRUNCHBASE_API_KEY", "test-key")
    store = board_seat_daily.BoardSeatStore()
    crunchbase = board_seat_daily.FundingSnapshot(
        history="Browserbase has raised $21M.",
        latest_round="Series A $21M",
        latest_date="2024-10",
        backers=[],
        source_urls=["https://www.crunchbase.com/organization/browserbase"],
        source_type="crunchbase_api",
        as_of_utc=datetime.now(UTC).isoformat(),
        confidence=0.7,
    )
    web = board_seat_daily.FundingSnapshot(
        history="Browserbase funding referenced in web coverage.",
        latest_round="Series A",
        latest_date="2024",
        backers=["Kleiner Perkins"],
        source_urls=["https://example.com/browserbase-funding"],
        source_type="web_refresh",
        as_of_utc=datetime.now(UTC).isoformat(),
        confidence=0.5,
    )
    monkeypatch.setattr(board_seat_daily, "_target_funding_from_crunchbase", lambda *_args, **_kwargs: crunchbase)
    monkeypatch.setattr(board_seat_daily, "_refresh_funding_snapshot_from_web", lambda **_kwargs: web)
    snapshot = board_seat_daily._resolve_funding_snapshot(store=store, company="Browserbase")
    assert snapshot.source_type == "crunchbase_api"
    assert "browserbase has raised" in snapshot.history.lower()


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


def test_message_looks_like_board_seat_pitch_accepts_legacy_numbered_shape() -> None:
    text = "\n".join(
        [
            "1. Idea title",
            "Acquire Epirus to expand C-UAS stack.",
            "2. Why now",
            "3. Target(s) / sector",
            "Anduril",
        ]
    )
    assert board_seat_daily._message_looks_like_board_seat_pitch(company="Anduril", text=text) is True


def test_record_target_and_lock_lookup(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    store = board_seat_daily.BoardSeatStore()
    inserted = store.record_target(
        company="Anduril",
        target="Epirus",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        source="manual_seed",
        posted_at_utc=datetime.now(UTC).isoformat(),
        run_date_local="2026-02-24",
        message_ts=None,
    )
    assert inserted is True
    hit = store.recent_target_hit(
        company="Anduril",
        target_key=board_seat_daily._target_key("Epirus"),
        lookback_days=30,
    )
    assert hit is not None
    assert str(hit["target"]).lower() == "epirus"


def test_best_effort_target_skips_blocked_target() -> None:
    seed = "Acquire Epirus to accelerate Anduril execution."
    target = board_seat_daily._best_effort_target(
        company="Anduril",
        seed_text=seed,
        blocked_keys={board_seat_daily._target_key("Epirus")},
    )
    assert board_seat_daily._target_key(target) != board_seat_daily._target_key("Epirus")
    assert board_seat_daily._is_valid_target_name(company="Anduril", target=target) is True


def test_is_valid_target_name_rejects_pronoun_placeholders() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="This") is False
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="The") is False


def test_validate_rendered_message_format_rejects_numbered_template() -> None:
    bad = "\n".join(
        [
            "1. Idea title",
            "Acquire Epirus to expand C-UAS stack.",
            "2. Why now",
            "Demand is rising.",
        ]
    )
    errors = board_seat_daily._validate_rendered_message_format(company="Anduril", message=bad)
    assert "numbered_heading_disallowed" in errors


def test_write_target_ledger_writes_csv_json_and_mirror(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_LEDGER_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_ENABLED", "1")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_LEDGER_MIRROR_PATH", str(tmp_path / "mirror"))
    store = board_seat_daily.BoardSeatStore()
    store.record_target(
        company="Anduril",
        target="Epirus",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        source="manual_seed",
        posted_at_utc=datetime.now(UTC).isoformat(),
        run_date_local="2026-02-24",
        message_ts=None,
    )
    paths = board_seat_daily._write_target_ledger(store)
    assert Path(paths["csv_path"]).exists()
    assert Path(paths["json_path"]).exists()
    assert Path(paths["mirror_csv_path"]).exists()
    assert Path(paths["mirror_json_path"]).exists()

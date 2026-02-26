from __future__ import annotations

from datetime import UTC, datetime, timedelta
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


def test_validate_draft_semantic_recency_requires_dated_evidence() -> None:
    base = _v6_draft()
    draft = board_seat_daily.BoardSeatDraft(
        idea_line=base.idea_line,
        target_does=base.target_does,
        why_now="Over the past month, enterprise launch and partnership momentum accelerated adoption.",
        whats_different=base.whats_different,
        mos_risks=base.mos_risks,
        bottom_line=base.bottom_line,
        context_current_efforts=base.context_current_efforts,
        context_domain_fit_gaps=base.context_domain_fit_gaps,
        funding_history=base.funding_history,
        funding_latest_round_backers=base.funding_latest_round_backers,
        source_refs=base.source_refs,
    )
    now = datetime.now(UTC)
    recent_a = (now.replace(microsecond=0)).date().isoformat()
    recent_b = (now.replace(microsecond=0) - timedelta(days=8)).date().isoformat()
    stale = (now.replace(microsecond=0) - timedelta(days=200)).date().isoformat()
    good_pack = {
        "why_now_evidence": [
            {
                "title": "Sourcegraph launches new enterprise rollout",
                "snippet": "Enterprise adoption accelerated after a new launch.",
                "url": "https://www.reuters.com/example/sourcegraph-launch",
                "published_hint": recent_a,
                "tier": "tier_1",
                "page_type": "news_report",
            },
            {
                "title": "Company press release on partnership",
                "snippet": "Partnership and launch expanded customer demand.",
                "url": "https://techcrunch.com/example/sourcegraph-partnership",
                "published_hint": recent_b,
                "tier": "tier_1",
                "page_type": "press_release",
            },
        ]
    }
    stale_pack = {
        "why_now_evidence": [
            {
                "title": "Old article",
                "snippet": "Old trend from prior cycle.",
                "url": "https://www.reuters.com/example/sourcegraph-old",
                "published_hint": stale,
                "tier": "tier_1",
                "page_type": "news_report",
            }
        ]
    }
    assert "why_now_not_monthly_theme" not in board_seat_daily._validate_draft(draft, company="Anduril", evidence_pack=good_pack)
    bad_errors = board_seat_daily._validate_draft(draft, company="Anduril", evidence_pack=stale_pack)
    assert "why_now_not_monthly_theme" in bad_errors
    assert "why_now_recency_missing" in bad_errors


def test_llm_evidence_pack_builds_section_bundles_and_quality_flags() -> None:
    now = datetime.now(UTC)
    recent_a = now.date().isoformat()
    recent_b = (now - timedelta(days=7)).date().isoformat()
    target_rows = [
        {
            "publisher": "Reuters",
            "title": "Sourcegraph partners with enterprise software vendor",
            "snippet": "Partnership expands enterprise code intelligence adoption and integration.",
            "url": "https://www.reuters.com/example/sourcegraph-partners",
            "published_hint": recent_a,
        },
        {
            "publisher": "Sourcegraph",
            "title": "Sourcegraph docs",
            "snippet": "Product docs for enterprise deployment and governance.",
            "url": "https://docs.sourcegraph.com/admin/deploy",
            "published_hint": recent_b,
        },
    ]
    acquisition_rows = [
        {
            "publisher": "TechCrunch",
            "title": "Sourcegraph adds enterprise controls",
            "snippet": "Launch adds differentiated controls for enterprise repositories.",
            "url": "https://techcrunch.com/example/sourcegraph-controls",
            "published_hint": recent_b,
        }
    ]
    pack = board_seat_daily._llm_evidence_pack(
        company="Cursor",
        target="Sourcegraph",
        target_rows=target_rows,
        acquisition_rows=acquisition_rows,
        funding=_funding(),
    )
    assert pack["source_policy"] == board_seat_daily._source_policy()
    assert isinstance(pack["target_does_evidence"], list) and pack["target_does_evidence"]
    assert isinstance(pack["why_now_evidence"], list) and pack["why_now_evidence"]
    assert isinstance(pack["quality_required_evidence"], dict)
    assert "why_now" in pack["quality_required_evidence"]
    assert isinstance(pack["evidence_tier_mix"], dict)
    assert "tier_1" in pack["evidence_tier_mix"]
    assert isinstance(pack["fact_cards"], dict)
    assert isinstance(pack["fact_cards_count_by_field"], dict)


def test_build_source_refs_tiered_policy_excludes_tier3_rows(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_SOURCE_POLICY", "tiered_trusted_first")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_SOURCE_GATE_MODE", "soft_block")

    def _fake_target_rows(**kwargs):
        return [
            {
                "publisher": "Reddit",
                "title": "Book a Demo | Pricing",
                "snippet": "Book a Demo • See Pricing • Sign In",
                "url": "https://reddit.com/r/startups/example",
            },
            {
                "publisher": "Reuters",
                "title": "Sourcegraph expands enterprise code intelligence",
                "snippet": "Enterprise adoption rose after product launch and partnership.",
                "url": "https://www.reuters.com/example/sourcegraph-expands",
            },
        ]

    monkeypatch.setattr(board_seat_daily, "_target_search_rows", _fake_target_rows)
    selection = board_seat_daily._build_source_refs(
        company="Cursor",
        draft=board_seat_daily.BoardSeatDraft(
            idea_line="Acquire Sourcegraph to accelerate Cursor execution in a strategic wedge.",
            target_does="Sourcegraph sells enterprise code search and intelligence software.",
            why_now="Over the past month, enterprise demand accelerated after launch and partnership updates.",
            whats_different="Differentiated enterprise controls and repository-scale indexing.",
            mos_risks="Risks include integration complexity and enterprise change management.",
            bottom_line="Execute a measured integration plan.",
            context_current_efforts="Cursor has active enterprise adoption momentum.",
            context_domain_fit_gaps="Large-codebase retrieval and governance remain key gaps.",
            funding_history="Raised across multiple rounds.",
            funding_latest_round_backers="Series C with institutional backers.",
            source_refs=[
                board_seat_daily.SourceRef(
                    name_or_publisher="Reuters",
                    title="Sourcegraph expands enterprise code intelligence",
                    url="https://www.reuters.com/example/sourcegraph-expands",
                )
            ],
        ),
        funding=_funding(),
        acquisition_rows=[],
    )
    urls = [ref.url for ref in selection.refs]
    assert any("reuters.com" in url for url in urls)
    assert all("reddit.com" not in url for url in urls)


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
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
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


def test_hard_14_day_target_lock_cannot_be_bypassed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_ALLOW_REPEAT_TARGETS", "1")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())
    monkeypatch.setattr(board_seat_daily, "_best_effort_target", lambda **kwargs: "Saronic")

    store = board_seat_daily.BoardSeatStore()
    store.record_target(
        company="Anduril",
        target="Saronic",
        channel_ref="anduril",
        channel_id="C_ANDURIL",
        source="seed",
        posted_at_utc=datetime.now(UTC).isoformat(),
        run_date_local="2026-02-24",
        message_ts=None,
    )
    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert payload["skipped"][0]["reason"] == "repeat_target_within_lock_window"


def test_repeat_guardrail_v5_allows_with_significant_change(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
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
    monkeypatch.setattr(
        board_seat_daily,
        "_assess_repitch_significance",
        lambda **_kwargs: {
            "assessment_id": 1,
            "allow": True,
            "reason": "allow_repitch_exceptional_signal",
            "top_events": [
                {
                    "title": "Anduril wins major international defense contract",
                    "publisher": "Reuters",
                    "url": "https://www.reuters.com/example",
                    "event_at_utc": datetime.now(UTC).isoformat(),
                    "event_type": "major_contract",
                    "impact_score": 0.99,
                    "evidence_quality": 1.0,
                },
                {
                    "title": "Anduril announces acquisition discussions",
                    "publisher": "Bloomberg",
                    "url": "https://www.bloomberg.com/example",
                    "event_at_utc": datetime.now(UTC).isoformat(),
                    "event_type": "mna",
                    "impact_score": 0.98,
                    "evidence_quality": 1.0,
                },
            ],
            "aggregate_score": 2.9,
            "max_event_score": 0.99,
            "distinct_domains": 2,
        },
    )

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["significant_change"] is True
    assert payload["sent"][0]["is_repitch"] is True
    assert payload["sent"][0]["repitch_of_pitch_id"] is not None
    assert payload["sent"][0]["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION
    assert len(sent) == 1
    assert "Repitch note:" in sent[0]["text"]
    assert "New evidence:" in sent[0]["text"]


def test_repeat_with_significant_change_rejected_when_assessment_not_exceptional(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_BACKFILL_ENABLED", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
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
                    {"text": "Anduril signed a new $1.2B multi-year international contract.", "user": "U1"},
                ],
                "response_metadata": {"next_cursor": ""},
            }

        def chat_postMessage(self, channel: str, text: str, **kwargs):
            raise AssertionError("Should skip when repitch assessment is not exceptional")

    monkeypatch.setattr(board_seat_daily, "WebClient", FakeWebClient)
    monkeypatch.setattr(board_seat_daily, "SlackApiError", Exception)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(
        board_seat_daily,
        "_assess_repitch_significance",
        lambda **_kwargs: {
            "assessment_id": 2,
            "allow": False,
            "reason": "reject_repitch_not_exceptional_enough",
            "top_events": [],
            "aggregate_score": 0.9,
            "max_event_score": 0.5,
            "distinct_domains": 1,
        },
    )

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert payload["skipped"][0]["reason"] == "repitch_not_significant_enough"


def test_unknown_funding_renders_two_explicit_unknown_labeled_lines() -> None:
    unknown = board_seat_daily._empty_funding_snapshot(source_type="unknown")
    history, latest = board_seat_daily._funding_lines_from_snapshot(unknown)
    assert history == board_seat_daily.UNKNOWN_FUNDING_TEXT
    assert latest == board_seat_daily.UNKNOWN_FUNDING_TEXT


def test_line_word_caps_enforced_for_v5_fields(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS", "18")
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
    assert all(len(item.split()) <= 18 for item in fields)


def test_normalize_line_no_cap_preserves_long_line(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS", "0")
    line = board_seat_daily._normalize_line(" ".join(["word"] * 40))
    assert len(line.split()) == 40


def test_normalize_line_truncation_drops_partial_second_sentence_fragment(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS", "18")
    line = board_seat_daily._normalize_line(
        "As the creators of Next.js, no company is more integrated with both Next.js and React than Vercel. Migrate"
    )
    assert len(line.split()) <= 18
    assert "Migrate" not in line
    assert "Vercel" in line


def test_normalize_line_truncation_removes_dangling_tail_token() -> None:
    line = board_seat_daily._normalize_line(
        "Platform fit improves execution quality with stronger developer adoption and retention with",
        max_words=10,
    )
    assert len(line.split()) <= 10
    assert not line.lower().endswith(" with")


def test_strip_obvious_writing_artifacts_removes_html_and_menu_text() -> None:
    cleaned, tags = board_seat_daily._strip_obvious_writing_artifacts(
        "Get the full list » Browserbase is &lt;strong&gt;enterprise browser infra&lt;/strong&gt; ... Read more"
    )
    assert "Get the full list" not in cleaned
    assert "<strong>" not in cleaned
    assert "Read more" not in cleaned
    assert "html_unescape" in tags


def test_sanitize_draft_dedups_duplicate_thesis_fields() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
        target_does="Browserbase helps automate browser tasks for AI teams.",
        why_now="Browserbase helps automate browser tasks for AI teams.",
        whats_different="Browserbase helps automate browser tasks for AI teams.",
        mos_risks="Browserbase helps automate browser tasks for AI teams.",
        bottom_line="Execute one target-led move with milestones tied to quality and adoption.",
        context_current_efforts="OpenAI has active customer programs and product pathways.",
        context_domain_fit_gaps="Developer workflow reliability remains a gap.",
        funding_history="Unknown.",
        funding_latest_round_backers="Unknown.",
        source_refs=[],
    )
    clean = board_seat_daily._sanitize_draft(company="OpenAI", draft=draft, funding=_funding(), acquisition_rows=[])
    assert clean.target_does != clean.why_now
    assert clean.whats_different != clean.target_does
    assert clean.mos_risks != clean.target_does
    assert set(clean.writing_field_dedup_fixes) >= {"why_now", "whats_different", "mos_risks"}


def test_run_once_dry_run_v5_contains_hierarchy_sections(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Cursor:cursor")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
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


def test_build_draft_passes_evidence_pack_to_llm(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS", "0")
    monkeypatch.setattr(board_seat_daily, "_fetch_thematic_context", lambda **kwargs: [])
    monkeypatch.setattr(
        board_seat_daily,
        "_target_search_rows",
        lambda **kwargs: [
            {
                "publisher": "VentureBeat",
                "title": "Browserbase launches enterprise browser stack",
                "snippet": "Browserbase sells browser infrastructure to AI and enterprise developer teams.",
                "url": "https://venturebeat.com/example/browserbase-stack",
            }
        ],
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_acquisition_search_rows",
        lambda **kwargs: [
            {
                "publisher": "TechCrunch",
                "title": "OpenAI explores browser infrastructure targets",
                "snippet": "M&A path could speed enterprise deployment controls.",
                "url": "https://techcrunch.com/example/openai-browser-mna",
            }
        ],
    )
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding())
    captured: dict[str, object] = {}

    def _fake_llm_draft(**kwargs):
        captured["evidence_pack"] = kwargs.get("evidence_pack")
        return board_seat_daily.BoardSeatDraft(
            idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
            target_does="Browserbase sells enterprise browser infrastructure to AI engineering teams.",
            why_now="Over the past month, buyer demand shifted toward controlled browser execution for agents.",
            whats_different="Browserbase combines reliability controls with developer-friendly deployment speed.",
            mos_risks="Risks include integration sequencing, overlap with internal tooling, and enterprise support load.",
            bottom_line="Execute one target-led move with 12-month milestones tied to adoption and reliability.",
            context_current_efforts="OpenAI has active customer programs and product pathways where this target can fit now.",
            context_domain_fit_gaps="The gap is deterministic browser execution and governance at production scale.",
            funding_history="Raised multiple rounds over time.",
            funding_latest_round_backers="Series D led by Founders Fund and General Catalyst.",
            source_refs=[],
        )

    monkeypatch.setattr(board_seat_daily, "_llm_draft", _fake_llm_draft)
    store = board_seat_daily.BoardSeatStore()
    _ = board_seat_daily._build_draft(
        company="OpenAI",
        snippets=["Enterprise demand is shifting toward browser automation reliability this month."],
        store=store,
        recent_pitches=[],
    )
    evidence_pack = captured.get("evidence_pack")
    assert isinstance(evidence_pack, dict)
    assert evidence_pack.get("target_evidence")
    assert evidence_pack.get("acquisition_evidence")
    funding_summary = evidence_pack.get("funding_summary")
    assert isinstance(funding_summary, dict)
    assert funding_summary.get("latest_round") == "Series D"


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


def test_resolve_target_to_company_alias_maps_nextjs_to_vercel() -> None:
    target, reason = board_seat_daily._resolve_target_to_company(
        company="OpenAI",
        extracted_target="Next.js",
        blocked_keys=set(),
    )
    assert target == "Vercel"
    assert reason == "alias_mapped"


def test_resolve_target_to_company_non_company_unknown_falls_back_to_company() -> None:
    target, reason = board_seat_daily._resolve_target_to_company(
        company="OpenAI",
        extracted_target="Browser SDK",
        blocked_keys=set(),
    )
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target=target) is True
    assert board_seat_daily._is_non_company_target_shape(target) is False
    assert reason in {"fallback_rotation", "fallback_default"}


def test_resolve_target_to_company_valid_company_remains_as_extracted() -> None:
    target, reason = board_seat_daily._resolve_target_to_company(
        company="OpenAI",
        extracted_target="Browserbase",
        blocked_keys=set(),
    )
    assert target == "Browserbase"
    assert reason == "as_extracted"


def test_is_valid_target_name_rejects_pronoun_placeholders() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="This") is False
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="The") is False
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="There") is False
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="D2C") is False
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="Director") is False


def test_is_valid_target_name_rejects_ai_first_placeholder() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="AI-first") is False


def test_is_valid_target_name_rejects_possessive_company_variant() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="OpenAIs") is False


def test_is_valid_target_name_rejects_metric_token() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="ROI") is False


def test_is_valid_target_name_rejects_conceptual_llms_token() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="LLMs") is False


def test_is_valid_target_name_rejects_ai_focused_label() -> None:
    assert board_seat_daily._is_valid_target_name(company="OpenAI", target="AI-focused") is False


def test_sanitize_draft_retargets_conceptual_llms_target_to_concrete_company() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire LLMs to accelerate OpenAI execution in a strategic wedge.",
        target_does="LLMs provide model capability depth.",
        why_now="Over the past month, enterprise demand shifted toward reliable browser automation.",
        whats_different="A target can close runtime and governance gaps quickly.",
        mos_risks="Execution and integration quality remain key risks.",
        bottom_line="A focused target can improve deployment reliability.",
        context_current_efforts="OpenAI has active product distribution channels.",
        context_domain_fit_gaps="Browser reliability and controls remain a gap.",
        funding_history="Unknown.",
        funding_latest_round_backers="Unknown.",
        source_refs=[],
    )
    clean = board_seat_daily._sanitize_draft(company="OpenAI", draft=draft, funding=_funding(), acquisition_rows=[])
    assert clean.idea_line.startswith("Acquire ")
    assert "LLMs" not in clean.idea_line
    assert board_seat_daily._extract_acquisition_target(clean.idea_line) == "Browserbase"


def test_sanitize_draft_alias_maps_nextjs_target_to_vercel() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Next.js to accelerate OpenAI execution in a strategic wedge.",
        target_does="Next.js provides framework leverage.",
        why_now="Over the past month, enterprise demand shifted toward developer platform speed.",
        whats_different="A target can close distribution and execution gaps quickly.",
        mos_risks="Execution and integration quality remain key risks.",
        bottom_line="A focused target can improve deployment reliability.",
        context_current_efforts="OpenAI has active product distribution channels.",
        context_domain_fit_gaps="Developer ecosystem reach remains a gap.",
        funding_history="Unknown.",
        funding_latest_round_backers="Unknown.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Vercel",
                title="Towards the AI Cloud: Our Series F",
                url="https://vercel.com/blog/towards-the-ai-cloud-our-series-f",
            )
        ],
    )
    clean = board_seat_daily._sanitize_draft(company="OpenAI", draft=draft, funding=_funding(), acquisition_rows=[])
    assert board_seat_daily._extract_acquisition_target(clean.idea_line) == "Vercel"
    assert clean.target_original == "Next.js"
    assert clean.target_resolution_reason == "alias_mapped"


def test_high_conf_new_target_gate_allows_medium_new_target_broad_weighted_model(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "1")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_ALLOW_MEDIUM_NEW_TARGET", "1")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MODEL", "broad_weighted_v1")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_CONFIDENCE_HIGH_MIN", "2.40")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_CONFIDENCE_MEDIUM_MIN", "1.35")
    store = board_seat_daily.BoardSeatStore()
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
        target_does="Browserbase builds enterprise browser automation infrastructure.",
        why_now="Over the past month, enterprise demand shifted toward reliable browser automation.",
        whats_different="Browserbase can close runtime and governance gaps quickly.",
        mos_risks="Execution and integration quality remain key risks.",
        bottom_line="A focused target can improve deployment reliability.",
        context_current_efforts="OpenAI has active product distribution channels.",
        context_domain_fit_gaps="Browser reliability and controls remain a gap.",
        funding_history="Unknown.",
        funding_latest_round_backers="Unknown.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Startup News",
                title="Browserbase launches enterprise browser automation controls",
                url="https://startupnews.dev/browserbase-launch-controls",
            ),
            board_seat_daily.SourceRef(
                name_or_publisher="Builder Stack",
                title="Enterprise browser automation reliability guide",
                url="https://builderstack.dev/runtime-reliability-guide",
            ),
        ],
    )
    gate = board_seat_daily._high_conf_new_target_gate(store=store, company="OpenAI", draft=draft)
    assert gate["allow"] is True
    assert gate["reason"] == "ok"
    assert gate["target_confidence"] == "Medium"
    assert 1.35 <= float(gate["target_confidence_score"]) < 2.4


def test_high_conf_new_target_gate_rejects_low_confidence_target(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "1")
    store = board_seat_daily.BoardSeatStore()
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
        target_does="Browserbase builds enterprise browser automation infrastructure.",
        why_now="Over the past month, enterprise demand shifted toward reliable browser automation.",
        whats_different="Browserbase can close runtime and governance gaps quickly.",
        mos_risks="Execution and integration quality remain key risks.",
        bottom_line="A focused target can improve deployment reliability.",
        context_current_efforts="OpenAI has active product distribution channels.",
        context_domain_fit_gaps="Browser reliability and controls remain a gap.",
        funding_history="Unknown.",
        funding_latest_round_backers="Unknown.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Example",
                title="Automation trends",
                url="https://example.com/automation-trends",
            )
        ],
    )
    gate = board_seat_daily._high_conf_new_target_gate(store=store, company="OpenAI", draft=draft)
    assert gate["allow"] is False
    assert gate["reason"] == "target_confidence_not_high"
    assert gate["is_new_target"] is True
    assert float(gate["target_confidence_score"]) < 1.35


def test_high_conf_new_target_gate_rejects_non_new_target_even_if_high_conf(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "1")
    store = board_seat_daily.BoardSeatStore()
    store.record_target(
        company="OpenAI",
        target="Browserbase",
        channel_ref="openai",
        channel_id="C_OPENAI",
        source="seed",
        posted_at_utc=datetime.now(UTC).isoformat(),
        run_date_local="2026-02-25",
        message_ts=None,
    )
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
        target_does="Browserbase builds enterprise browser automation infrastructure.",
        why_now="Over the past month, enterprise demand shifted toward reliable browser automation.",
        whats_different="Browserbase can close runtime and governance gaps quickly.",
        mos_risks="Execution and integration quality remain key risks.",
        bottom_line="A focused target can improve deployment reliability.",
        context_current_efforts="OpenAI has active product distribution channels.",
        context_domain_fit_gaps="Browser reliability and controls remain a gap.",
        funding_history="Unknown.",
        funding_latest_round_backers="Unknown.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Browserbase",
                title="Browserbase enterprise automation platform",
                url="https://www.browserbase.com/",
            ),
            board_seat_daily.SourceRef(
                name_or_publisher="TechCrunch",
                title="Browserbase raises Series A",
                url="https://techcrunch.com/browserbase-series-a",
            ),
            board_seat_daily.SourceRef(
                name_or_publisher="Axios",
                title="OpenAI evaluates Browserbase integrations",
                url="https://www.axios.com/browserbase-openai",
            ),
        ],
    )
    gate = board_seat_daily._high_conf_new_target_gate(store=store, company="OpenAI", draft=draft)
    assert gate["allow"] is False
    assert gate["reason"] == "target_not_new"
    assert gate["is_new_target"] is False
    assert gate["target_confidence"] == "High"


def test_run_once_skips_when_no_high_confidence_new_target(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "1")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_ALLOW_MEDIUM_NEW_TARGET", "0")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_draft",
        lambda **kwargs: board_seat_daily.BoardSeatDraft(
            idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
            target_does="Browserbase builds enterprise browser automation infrastructure.",
            why_now="Over the past month, enterprise demand shifted toward reliable browser automation.",
            whats_different="Browserbase can close runtime and governance gaps quickly.",
            mos_risks="Execution and integration quality remain key risks.",
            bottom_line="A focused target can improve deployment reliability.",
            context_current_efforts="OpenAI has active product distribution channels.",
            context_domain_fit_gaps="Browser reliability and controls remain a gap.",
            funding_history="Unknown.",
            funding_latest_round_backers="Unknown.",
            source_refs=[
                board_seat_daily.SourceRef(
                    name_or_publisher="Example",
                    title="Automation trends",
                    url="https://example.com/automation-trends",
                )
            ],
        ),
    )
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert payload["skipped"][0]["reason"] == "no_high_confidence_new_target"
    assert payload["skipped"][0]["gate_reason"] in {"target_confidence_not_high", "target_not_new", "invalid_target"}
    assert "target_confidence_score" in payload["skipped"][0]
    assert "target_confidence_reasons" in payload["skipped"][0]
    assert "target_validation_reason" in payload["skipped"][0]
    assert "target_original" in payload["skipped"][0]
    assert "target_resolution_reason" in payload["skipped"][0]
    assert payload["skipped"][0]["writing_mode"] == "llm_passthrough"
    assert "writing_artifact_cleanups" in payload["skipped"][0]
    assert "writing_field_dedup_fixes" in payload["skipped"][0]


def test_run_once_dry_run_retargets_conceptual_target_before_gating(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_draft",
        lambda **kwargs: board_seat_daily.BoardSeatDraft(
            idea_line="Acquire LLMs to accelerate OpenAI execution in a strategic wedge.",
            target_does="LLMs provide model capability depth.",
            why_now="Over the past month, enterprise demand shifted toward reliable browser automation.",
            whats_different="A target can close runtime and governance gaps quickly.",
            mos_risks="Execution and integration quality remain key risks.",
            bottom_line="A focused target can improve deployment reliability.",
            context_current_efforts="OpenAI has active product distribution channels.",
            context_domain_fit_gaps="Browser reliability and controls remain a gap.",
            funding_history="Unknown.",
            funding_latest_round_backers="Unknown.",
            source_refs=[
                board_seat_daily.SourceRef(
                    name_or_publisher="TechCrunch",
                    title="Browserbase raises Series A",
                    url="https://techcrunch.com/browserbase-series-a",
                )
            ],
        ),
    )
    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["target"] == "Browserbase"
    assert "LLMs" not in payload["sent"][0]["preview"]
    assert "target_original" in payload["sent"][0]
    assert payload["sent"][0]["target_original"]
    assert payload["sent"][0]["target_resolution_reason"] in {
        "as_extracted",
        "alias_mapped",
        "fallback_rotation",
        "fallback_default",
        "invalid_after_resolution",
    }


def test_run_once_dry_run_payload_includes_target_resolution_fields(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_draft",
        lambda **kwargs: board_seat_daily.BoardSeatDraft(
            idea_line="Acquire Next.js to accelerate OpenAI execution in a strategic wedge.",
            target_does="Next.js provides frontend framework distribution leverage.",
            why_now="Over the past month, enterprise demand shifted toward developer platform speed.",
            whats_different="A focused target can close distribution and execution gaps quickly.",
            mos_risks="Execution and integration quality remain key risks.",
            bottom_line="A focused target can improve deployment reliability.",
            context_current_efforts="OpenAI has active product distribution channels.",
            context_domain_fit_gaps="Developer ecosystem reach remains a gap.",
            funding_history="Unknown.",
            funding_latest_round_backers="Unknown.",
            source_refs=[
                board_seat_daily.SourceRef(
                    name_or_publisher="Vercel",
                    title="Towards the AI Cloud: Our Series F",
                    url="https://vercel.com/blog/towards-the-ai-cloud-our-series-f",
                )
            ],
        ),
    )
    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert "target_original" in row
    assert row["target_original"]
    assert "target_resolution_reason" in row
    assert row["target_resolution_reason"] in {
        "as_extracted",
        "alias_mapped",
        "fallback_rotation",
        "fallback_default",
        "invalid_after_resolution",
    }


def test_run_once_dry_run_cleans_noisy_llm_fields_and_emits_writing_observability(tmp_path: Path, monkeypatch) -> None:
    recent_hint = datetime.now(UTC).date().isoformat()
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_MAX_LINE_WORDS", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_WRITING_MODE", "llm_passthrough")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_STRIP_OBVIOUS_ARTIFACTS", "1")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(
        board_seat_daily,
        "_acquisition_search_rows",
        lambda **kwargs: [
                {
                    "publisher": "TechCrunch",
                    "title": "OpenAI studies browser infrastructure M&A",
                    "snippet": "Deal could speed enterprise browser automation reliability.",
                    "url": "https://techcrunch.com/example/openai-mna",
                    "published_hint": recent_hint,
                }
            ],
        )
    monkeypatch.setattr(
        board_seat_daily,
        "_target_search_rows",
        lambda **kwargs: [
                {
                    "publisher": "Reuters",
                    "title": "Browserbase launches enterprise platform",
                    "snippet": "Browserbase provides browser automation infrastructure for enterprise AI teams.",
                    "url": "https://www.reuters.com/example/browserbase-platform",
                    "published_hint": recent_hint,
                }
            ],
        )
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_draft",
        lambda **kwargs: board_seat_daily.BoardSeatDraft(
            idea_line="Acquire Browserbase to accelerate OpenAI execution in a strategic wedge.",
            target_does="Get the full list » Book a Demo • See Pricing • Browserbase is &lt;strong&gt;enterprise browser infra&lt;/strong&gt; ... Read more",
            why_now="Get the full list » Book a Demo • See Pricing • Browserbase is &lt;strong&gt;enterprise browser infra&lt;/strong&gt; ... Read more",
            whats_different="Get the full list » Book a Demo • See Pricing • Browserbase is &lt;strong&gt;enterprise browser infra&lt;/strong&gt; ... Read more",
            mos_risks="Get the full list » Book a Demo • See Pricing • Browserbase is &lt;strong&gt;enterprise browser infra&lt;/strong&gt; ... Read more",
            bottom_line="Execute one target-led move with 12-month milestones tied to revenue velocity and margin quality.",
            context_current_efforts="OpenAI has active customer programs and product pathways where this target can be integrated now.",
            context_domain_fit_gaps="Focus on the highest-friction capability gap where acquisition beats internal build speed.",
            funding_history="Unknown.",
            funding_latest_round_backers="Unknown.",
            source_refs=[],
        ),
    )
    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert row["writing_mode"] == "llm_passthrough"
    assert row["writing_artifact_cleanups"]
    assert set(row["writing_field_dedup_fixes"]) >= {"why_now", "whats_different", "mos_risks"}
    assert "Get the full list" not in row["preview"]
    assert "Book a Demo" not in row["preview"]
    assert "See Pricing" not in row["preview"]
    assert "<strong>" not in row["preview"]


def test_best_effort_idea_line_rewrites_ai_first_to_concrete_target() -> None:
    line = board_seat_daily._best_effort_idea_line(
        company="OpenAI",
        seed_text="Acquire AI-first to accelerate OpenAI execution in a strategic wedge.",
    )
    assert "Acquire Browserbase" in line
    assert "AI-first" not in line


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


def test_store_connect_context_closes_connection(monkeypatch, tmp_path: Path) -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.row_factory = None
            self.closed = False
            self.committed = False
            self.rolled_back = False

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            self.rolled_back = True

        def close(self) -> None:
            self.closed = True

    fake = FakeConn()
    monkeypatch.setattr(board_seat_daily.sqlite3, "connect", lambda _path: fake)

    store = object.__new__(board_seat_daily.BoardSeatStore)
    store.db_path = tmp_path / "db.sqlite"

    with store._connect() as conn:
        assert conn is fake
        assert conn.row_factory == board_seat_daily.sqlite3.Row
        assert fake.closed is False

    assert fake.committed is True
    assert fake.rolled_back is False
    assert fake.closed is True


def test_prepare_funding_evidence_rows_dedupes_canonical_urls() -> None:
    rows = board_seat_daily._prepare_funding_evidence_rows(
        [
            {
                "publisher": "Example",
                "title": "Browserbase raises Series A",
                "snippet": "Browserbase raised $21M led by Kleiner Perkins.",
                "url": "https://example.com/browserbase?utm_source=newsletter",
            },
            {
                "publisher": "Example",
                "title": "Browserbase raises Series A",
                "snippet": "Browserbase raised $21M led by Kleiner Perkins.",
                "url": "https://www.example.com/browserbase/",
            },
        ]
    )
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/browserbase"


def test_funding_conflict_detection_flags_round_mismatch() -> None:
    conflicts = board_seat_daily._funding_evidence_conflicts(
        [
            {
                "title": "Company raises Series A",
                "snippet": "Raised $20M in 2024 led by investors.",
                "url": "https://a.example.com/funding",
            },
            {
                "title": "Company raises Series B",
                "snippet": "Raised $40M in 2025 with new investors.",
                "url": "https://b.example.com/funding",
            },
        ]
    )
    assert "major_round_mismatch" in conflicts
    assert "major_amount_mismatch" in conflicts


def test_funding_confidence_band_maps_levels() -> None:
    high = board_seat_daily.FundingSnapshot(
        history="Raised capital.",
        latest_round="Series A",
        latest_date="2025",
        backers=["A"],
        source_urls=["https://a.example.com", "https://b.example.com"],
        source_type="web_refresh",
        as_of_utc=datetime.now(UTC).isoformat(),
        confidence=0.9,
        evidence_count=3,
        distinct_domains=2,
        conflict_flags=[],
        verification_status="verified",
    )
    medium = board_seat_daily.FundingSnapshot(
        history="Raised capital.",
        latest_round="Series A",
        latest_date="2025",
        backers=["A"],
        source_urls=["https://a.example.com"],
        source_type="web_refresh",
        as_of_utc=datetime.now(UTC).isoformat(),
        confidence=0.7,
        evidence_count=1,
        distinct_domains=1,
        conflict_flags=["minor_date_variance"],
        verification_status="partial",
    )
    low = board_seat_daily.FundingSnapshot(
        history="Raised capital.",
        latest_round="Series A",
        latest_date="2025",
        backers=["A"],
        source_urls=["https://a.example.com"],
        source_type="web_refresh",
        as_of_utc=datetime.now(UTC).isoformat(),
        confidence=0.2,
        evidence_count=1,
        distinct_domains=1,
        conflict_flags=[],
        verification_status="weak",
    )
    assert board_seat_daily._funding_confidence_band(high) == "high"
    assert board_seat_daily._funding_confidence_band(medium) == "medium"
    assert board_seat_daily._funding_confidence_band(low) == "low"


def test_low_confidence_funding_adds_warning_line() -> None:
    draft = board_seat_daily._sanitize_draft(
        company="Anduril",
        draft=_v6_draft(),
        funding=board_seat_daily.FundingSnapshot(
            history="Sparse funding references.",
            latest_round="Series A",
            latest_date="2024",
            backers=[],
            source_urls=["https://a.example.com/funding"],
            source_type="web_refresh",
            as_of_utc=datetime.now(UTC).isoformat(),
            confidence=0.3,
            evidence_count=1,
            distinct_domains=1,
            conflict_flags=[],
            verification_status="weak",
        ),
        acquisition_rows=[],
    )
    message = board_seat_daily._render_board_seat_message(company="Anduril", draft=draft)
    assert "*Warning:* Funding data is low-confidence; verify before action." in message


def test_refresh_funding_snapshot_brave_plus_serp_sets_domain_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        board_seat_daily,
        "_funding_web_rows",
        lambda _company: [
            {
                "publisher": "TechCrunch",
                "title": "Browserbase raises Series A",
                "snippet": "Browserbase raised $21M led by Kleiner Perkins.",
                "url": "https://techcrunch.com/example-browserbase-round",
            },
            {
                "publisher": "Crunchbase News",
                "title": "Browserbase funding",
                "snippet": "Investors include Kleiner Perkins in 2024.",
                "url": "https://news.crunchbase.com/example-browserbase-round",
            },
        ],
    )
    monkeypatch.setattr(board_seat_daily, "_extract_funding_with_llm", lambda **_kwargs: None)
    snapshot = board_seat_daily._refresh_funding_snapshot_from_web(company="Browserbase")
    assert snapshot is not None
    assert snapshot.source_type == "web_refresh"
    assert snapshot.evidence_count >= 2
    assert snapshot.distinct_domains >= 2


def test_status_reports_funding_quality_metrics(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "Anduril:anduril")
    store = board_seat_daily.BoardSeatStore()
    store.upsert_funding_snapshot(
        company="Anduril",
        snapshot=board_seat_daily.FundingSnapshot(
            history="Anduril has raised substantial capital.",
            latest_round="Series F",
            latest_date="2024",
            backers=["Founders Fund"],
            source_urls=["https://a.example.com/funding", "https://b.example.com/funding"],
            source_type="web_refresh",
            as_of_utc=datetime.now(UTC).isoformat(),
            confidence=0.8,
            evidence_count=2,
            distinct_domains=2,
            conflict_flags=[],
            verification_status="verified",
        ),
    )
    payload = board_seat_daily.status()
    metrics = payload["funding_quality_metrics"]
    assert metrics["total_companies"] == 1
    assert metrics["verified_count"] == 1
    assert metrics["low_confidence_count"] == 0
    assert payload["require_high_conf_new_target"] is True
    assert payload["funding_verification_by_company"]["Anduril"]["verification_status"] == "verified"
    assert "quality_pass_rate_7d" in payload
    assert "top_failed_fields_7d" in payload
    assert "avg_rewrite_attempts_7d" in payload
    assert "delivery_mode" in payload
    assert "fact_card_mode" in payload
    assert "quote_overlap_max" in payload
    assert "diagnostic_fallback_count_7d" in payload


def test_brave_api_key_accepts_coatue_claw_alias(monkeypatch) -> None:
    monkeypatch.delenv("BRAVE_SEARCH_API_KEY", raising=False)
    monkeypatch.setenv("COATUE_CLAW_BRAVE_API_KEY", "alias-brave-key")
    assert board_seat_daily._brave_search_api_key() == "alias-brave-key"


def test_target_description_soft_block_skips_low_signal_rows(monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_SOURCE_GATE_MODE", "soft_block")
    description = board_seat_daily._target_description_from_rows(
        target="Sourcegraph",
        rows=[
            {
                "publisher": "Sourcegraph",
                "title": "Book a Demo",
                "snippet": "Book a Demo • See Pricing • Sign In • Product tour",
                "url": "https://sourcegraph.com/pricing",
            },
            {
                "publisher": "TechCrunch",
                "title": "Sourcegraph expands enterprise AI code search",
                "snippet": "Sourcegraph helps engineering teams search, understand, and refactor large codebases quickly.",
                "url": "https://techcrunch.com/example/sourcegraph-expands",
            },
        ],
    )
    normalized = description.lower()
    assert "book a demo" not in normalized
    assert "see pricing" not in normalized
    assert "sourcegraph" in normalized
    assert "codebases" in normalized


def test_assess_draft_quality_flags_near_duplicate_thesis_lines() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Sourcegraph to accelerate Cursor execution in a strategic wedge.",
        target_does="Sourcegraph gives engineering teams fast code search across very large codebases for daily development workflows.",
        why_now="Sourcegraph gives engineering teams fast code search across very large codebases for daily development workflows.",
        whats_different="Sourcegraph pairs enterprise controls with high-recall code intelligence for production teams.",
        mos_risks="Risks include migration complexity, overlap with internal roadmap, and enterprise support burden.",
        bottom_line="Execute one target-led move with measurable milestones over 12 months.",
        context_current_efforts="Cursor has active enterprise rollout and agent workflow momentum.",
        context_domain_fit_gaps="Codebase-scale retrieval and governance controls remain the primary gap.",
        funding_history="Raised multiple rounds over time.",
        funding_latest_round_backers="Series C led by strategic investors.",
        source_refs=[
            board_seat_daily.SourceRef(
                name_or_publisher="Reuters",
                title="Sourcegraph pushes enterprise code intelligence expansion",
                url="https://www.reuters.com/technology/sourcegraph-expands-enterprise-code-intelligence-2026-01-20/",
            ),
            board_seat_daily.SourceRef(
                name_or_publisher="TechCrunch",
                title="Sourcegraph adds enterprise code search controls",
                url="https://techcrunch.com/example/sourcegraph-controls",
            ),
        ],
    )
    assessment = board_seat_daily._assess_draft_quality(
        company="Cursor",
        draft=draft,
        evidence_pack={
            "target_evidence": [
                {
                    "publisher": "Reuters",
                    "title": "Sourcegraph pushes enterprise code intelligence expansion",
                    "snippet": "Sourcegraph helps developers navigate and refactor large repositories.",
                    "url": "https://www.reuters.com/technology/sourcegraph-expands-enterprise-code-intelligence-2026-01-20/",
                }
            ],
            "acquisition_evidence": [],
        },
    )
    assert any(reason.startswith("near_duplicate:target_does:why_now") for reason in assessment["reasons"])


def test_run_once_fail_closed_when_quality_gate_stays_failed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_QUALITY_FAIL_POLICY", "skip")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DELIVERY_MODE", "skip")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REWRITE_MAX_RETRIES", "2")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())
    monkeypatch.setattr(
        board_seat_daily,
        "_assess_draft_quality",
        lambda **kwargs: {"passed": False, "score": 0.25, "reasons": ["artifact_contamination:target_does"]},
    )
    monkeypatch.setattr(board_seat_daily, "_llm_revise_draft", lambda **kwargs: None)

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    row = payload["skipped"][0]
    assert row["reason"] == "quality_gate_failed"
    assert row["delivery_mode_applied"] == "skip"
    assert row["quality_blocked"] is True
    assert row["quality_gate_passed"] is False
    assert row["rewrite_attempts"] == 2
    assert row["quality_fail_stage"] in {"source_filter", "reviewer", "draft_validator"}
    assert "quality_score" in row
    assert "quality_reasons" in row
    assert "quality_field_scores" in row
    assert "quality_failed_fields" in row
    assert "quality_required_evidence" in row
    assert "evidence_tier_mix" in row
    assert "why_now_recency_passed" in row


def test_run_once_quality_fail_closed_applies_to_fallback_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_QUALITY_FAIL_POLICY", "skip")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DELIVERY_MODE", "skip")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REWRITE_MAX_RETRIES", "1")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: None)
    fallback_calls = {"count": 0}

    def _fake_fallback(**kwargs):
        fallback_calls["count"] += 1
        return _v6_draft()

    monkeypatch.setattr(board_seat_daily, "_fallback_draft", _fake_fallback)
    monkeypatch.setattr(
        board_seat_daily,
        "_assess_draft_quality",
        lambda **kwargs: {"passed": False, "score": 0.2, "reasons": ["blatant_evidence_mismatch"]},
    )
    monkeypatch.setattr(board_seat_daily, "_llm_revise_draft", lambda **kwargs: None)

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert fallback_calls["count"] >= 1
    assert payload["sent"] == []
    assert payload["skipped"][0]["reason"] == "quality_gate_failed"


def test_assess_draft_quality_flags_quote_overlap() -> None:
    draft = board_seat_daily.BoardSeatDraft(
        idea_line="Acquire Sourcegraph to accelerate Cursor execution in a strategic wedge.",
        target_does="Sourcegraph enterprise plan offers self-hosted code hosts and private code hosts for large teams.",
        why_now="Over the past month, enterprise demand for code intelligence accelerated after product launch updates.",
        whats_different="Sourcegraph combines retrieval quality with enterprise governance controls.",
        mos_risks="Risks include integration complexity and migration churn in enterprise environments.",
        bottom_line="Execute one target-led move with clear milestones.",
        context_current_efforts="Cursor already has enterprise design-partner momentum.",
        context_domain_fit_gaps="Repository-scale search and governance remain a key gap.",
        funding_history="Raised multiple rounds.",
        funding_latest_round_backers="Series C with institutional backers.",
        source_refs=[],
    )
    evidence_pack = {
        "all_evidence": [
            {
                "title": "Sourcegraph enterprise",
                "snippet": "Sourcegraph enterprise plan offers self-hosted code hosts and private code hosts for large teams.",
                "url": "https://example.com/sourcegraph-enterprise",
            }
        ],
        "quality_required_evidence": {
            "target_does": True,
            "why_now": True,
            "whats_different": True,
            "mos_risks": True,
        },
        "evidence_tier_mix": {"tier_1": 2, "tier_2": 0, "tier_3": 0},
        "fact_cards_count_by_field": {"target_does": 1, "why_now": 1, "whats_different": 1, "mos_risks": 1},
        "why_now_recency_passed": True,
    }
    assessment = board_seat_daily._assess_draft_quality(company="Cursor", draft=draft, evidence_pack=evidence_pack)
    assert any(reason.startswith("quote_overlap_high:target_does") for reason in assessment["reasons"])
    assert assessment["quote_overlap_by_field"]["target_does"] > board_seat_daily._quote_overlap_max()


def test_run_once_quality_failure_posts_diagnostic_when_enabled(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DELIVERY_MODE", "diagnostic_fallback")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REWRITE_MAX_RETRIES", "1")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())
    monkeypatch.setattr(
        board_seat_daily,
        "_assess_draft_quality",
        lambda **kwargs: {
            "passed": False,
            "score": 0.22,
            "reasons": ["quote_overlap_high:target_does"],
            "reason_codes": ["quote_overlap_high"],
            "field_scores": {"target_does": 0.2, "why_now": 0.8, "whats_different": 0.8, "mos_risks": 0.8},
            "failed_fields": ["target_does"],
            "quality_required_evidence": {"target_does": True, "why_now": True, "whats_different": True, "mos_risks": True},
            "evidence_tier_mix": {"tier_1": 2, "tier_2": 0, "tier_3": 0},
            "fact_cards_count_by_field": {"target_does": 1, "why_now": 1, "whats_different": 1, "mos_risks": 1},
            "quote_overlap_by_field": {"target_does": 0.9, "why_now": 0.1, "whats_different": 0.1, "mos_risks": 0.1},
            "why_now_recency_passed": True,
        },
    )
    monkeypatch.setattr(board_seat_daily, "_llm_revise_draft", lambda **kwargs: None)

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert payload["skipped"] == []
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert row["delivery_mode_applied"] == "diagnostic_fallback"
    assert row["quality_blocked"] is True
    assert "Quality block (no reliable thesis draft)" in row["preview"]


def test_run_once_sent_payload_includes_quality_fields(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_DB_PATH", str(tmp_path / "db/board.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_PORTCOS", "OpenAI:openai")
    monkeypatch.setenv("COATUE_CLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "0")
    monkeypatch.setattr(board_seat_daily, "WebClient", None)
    monkeypatch.setattr(board_seat_daily, "_resolve_funding_snapshot", lambda **kwargs: _funding(source_type="cache"))
    monkeypatch.setattr(board_seat_daily, "_acquisition_search_rows", lambda **kwargs: [])
    monkeypatch.setattr(board_seat_daily, "_llm_draft", lambda **kwargs: _v6_draft())

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert row["quality_gate_passed"] is True
    assert "quality_score" in row
    assert "quality_reasons" in row
    assert "rewrite_attempts" in row
    assert "quality_fail_stage" in row
    assert "quality_field_scores" in row
    assert "quality_failed_fields" in row
    assert "quality_required_evidence" in row
    assert "evidence_tier_mix" in row
    assert "why_now_recency_passed" in row

from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from spclaw import board_seat_daily


def _row(*, title: str, url: str, snippet: str = "") -> board_seat_daily.EvidenceRow:
    canonical = board_seat_daily._canonicalize_url(url)
    return board_seat_daily.EvidenceRow(
        title=title,
        snippet=snippet,
        url=url,
        canonical_url=canonical,
        publisher=board_seat_daily._domain_from_url(canonical),
        domain=board_seat_daily._domain_from_url(canonical),
        published_at_utc=None,
        backend="brave",
        quality=0.9,
    )


@pytest.fixture
def board_seat_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_root = tmp_path / "data"
    monkeypatch.setenv("SPCLAW_DATA_ROOT", str(data_root))
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_DB_PATH", str(data_root / "db" / "board_seat_daily.sqlite"))
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_RESET_MODE", "0")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_ENABLED", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_WEEKDAYS_ONLY", "0")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_TIME", "12:00")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SEARCH_ORDER", "brave,serp")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_REQUIRE_HIGH_CONF_NEW_TARGET", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_LLM_CANDIDATE_GEN_ENABLED", "0")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SIMPLE_MODE", "0")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_CHANNEL_DISCOVERY", "static")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_MEMORY_REWRITE_ON_FAIL", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SOURCES_IN_THREAD", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SIMPLE_USE_ALL_BACKENDS", "0")
    monkeypatch.delenv("SPCLAW_BOARD_SEAT_PORTCOS", raising=False)
    # Keep legacy-path unit tests deterministic; simple-mode tests override this.
    monkeypatch.setattr(board_seat_daily, "_simple_mode_enabled", lambda: False)
    return data_root


def test_parse_portcos_default() -> None:
    parsed = board_seat_daily._parse_portcos("")
    assert len(parsed) == 10
    assert parsed[0] == ("Anduril", "anduril")


def test_parse_portcos_custom() -> None:
    parsed = board_seat_daily._parse_portcos("Anduril:anduril,OpenAI:#openai")
    assert parsed == [("Anduril", "anduril"), ("OpenAI", "openai")]


def test_run_once_reset_mode_skips_all(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_RESET_MODE", "1")
    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["reset_mode"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == len(board_seat_daily._parse_portcos())
    assert {row["reason"] for row in payload["skipped"]} == {"feature_reset_in_progress"}


def test_run_once_disabled(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_ENABLED", "0")
    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert {row["reason"] for row in payload["skipped"]} == {"board_seat_disabled"}


def test_quality_gate_detects_artifacts() -> None:
    draft = (
        "*Thesis*\n"
        "- Read more to learn.\n"
        "*What the target does*\n"
        "- builds tools.\n"
        "*Why it’s a fit for portfolio company*\n"
        "- fit.\n"
        "*Risks*\n"
        "- risk.\n"
        "*Funding history and backers*\n"
        "- unknown."
    )
    ok, reasons = board_seat_daily._quality_gate(draft, source_rows=[])
    assert ok is False
    assert "artifact_term" in reasons


def test_deterministic_draft_includes_what_target_does_line() -> None:
    funding = board_seat_daily.FundingSnapshot(
        target="Databento",
        target_key="databento",
        total_raised="unknown",
        latest_round="unknown",
        latest_round_date="unknown",
        backers=(),
        evidence_count=0,
        distinct_domains=0,
        conflict_flags=(),
        verification_status="weak",
        source_rows=(),
    )
    draft = board_seat_daily._deterministic_draft(company="OpenAI", target="Databento", funding=funding, repitch_note=None)
    assert "*What target does*" in draft


def test_candidate_extraction_rejects_concepts() -> None:
    rows = [
        _row(title="OpenAI acquisition target: AI platform", url="https://example.com/a", snippet="OpenAI may acquire an AI platform"),
        _row(title="OpenAI acquires Databento", url="https://news.example.com/b", snippet="Acquisition rumors"),
    ]
    candidates = board_seat_daily._extract_candidates("OpenAI", rows)
    names = [c.target for c in candidates]
    assert "Databento" in names
    assert "AI" not in names


def test_target_validation_rejects_company_product_names() -> None:
    ok, reason = board_seat_daily._is_valid_target_name(target="Claude", company="Anthropic")
    assert ok is False
    assert reason == "product_not_company"

    ok2, reason2 = board_seat_daily._is_valid_target_name(target="ChatGPT", company="OpenAI")
    assert ok2 is False
    assert reason2 == "product_not_company"
    ok3, reason3 = board_seat_daily._is_valid_target_name(target="Lead", company="Anthropic")
    assert ok3 is False
    assert reason3 == "ambiguous_common_term"
    ok4, reason4 = board_seat_daily._is_valid_target_name(target="Corporate Development Integration", company="Anthropic")
    assert ok4 is False
    assert reason4 == "role_phrase_not_company"


def test_candidate_extraction_rejects_company_product_targets() -> None:
    rows = [
        _row(
            title="Anthropic acquires Claude product team",
            url="https://example.com/claude",
            snippet="Anthropic acquisition discussion around Claude expansion",
        ),
        _row(
            title="Anthropic acquires Vercept",
            url="https://example.com/vercept",
            snippet="Acquisition target Vercept",
        ),
    ]
    candidates = board_seat_daily._extract_candidates("Anthropic", rows)
    names = [c.target for c in candidates]
    assert "Claude" not in names
    assert "Vercept" in names


def test_filter_rows_for_target_drops_low_signal_sources() -> None:
    rows = [
        _row(title="Lead Funding, LLC profile", url="https://zoominfo.com/c/lead-funding", snippet="company profile"),
        _row(title="Lead raises $40M funding", url="https://techcrunch.com/2026/01/01/lead-raises", snippet="funding round"),
    ]
    filtered = board_seat_daily._filter_rows_for_target(target="Lead", rows=rows)
    assert len(filtered) == 1
    assert "techcrunch.com" in filtered[0].domain


def test_candidate_extraction_skips_job_intent_rows() -> None:
    rows = [
        _row(
            title="Job Application for Corporate Development Integration Lead at Anthropic",
            url="https://job-boards.greenhouse.io/anthropic/jobs/123",
            snippet="Apply to the role",
        ),
        _row(
            title="Anthropic acquires Vercept",
            url="https://techcrunch.com/2026/02/25/anthropic-acquires-vercept",
            snippet="acquisition details",
        ),
    ]
    candidates = board_seat_daily._extract_candidates("Anthropic", rows)
    names = [c.target for c in candidates]
    assert "Corporate Development Integration" not in names
    assert "Vercept" in names


def test_funding_snapshot_weak_adds_warning() -> None:
    rows = [
        _row(title="Sourcegraph raised $50M Series B led by Sequoia", url="https://a.com/x", snippet="funding details"),
        _row(title="Sourcegraph raised $250M Series D led by a16z", url="https://b.com/y", snippet="valuation update"),
    ]
    snap = board_seat_daily._funding_from_rows("Sourcegraph", rows)
    lines = board_seat_daily._render_funding_lines(snap)
    assert snap.verification_status in {"partial", "weak", "verified"}
    if snap.verification_status == "weak":
        assert any("low-confidence" in line.lower() for line in lines)


def test_money_parser_rejects_unbounded_plain_numbers() -> None:
    assert board_seat_daily._money_to_usd("2026") is None
    assert board_seat_daily._money_to_usd("$2026") is None
    assert board_seat_daily._money_to_usd("$40M") == 40_000_000


def test_extract_backers_filters_clause_fragments() -> None:
    text = (
        "Vercept raised $40M led by Fifty Years, with investors including Eric Schmidt, "
        "Jeff Dean, Google DeepMind, with participation from others."
    )
    backers = board_seat_daily._extract_backers(text)
    assert "with participation from others" not in " ".join(backers).lower()
    assert any("Fifty Years" == b for b in backers)


def test_pick_target_blocks_already_acquired(board_seat_env: Path) -> None:
    store = board_seat_daily.BoardSeatStore()
    rows = [
        _row(
            title="Anthropic acquires Vercept in early exit deal",
            url="https://news.example.com/a",
            snippet="Anthropic acquired Vercept after competitive process.",
        ),
        _row(
            title="Anthropic acquires Vercept to enhance Claude",
            url="https://news.example.com/b",
            snippet="Anthropic buys Vercept startup.",
        ),
    ]
    chosen, gate_reason, _, candidates, _ = board_seat_daily._pick_target(
        company="Anthropic",
        rows=rows,
        store=store,
        now_utc=board_seat_daily._utc_now(),
        run_date_local=board_seat_daily._today_key(),
    )
    assert chosen is None
    assert len(candidates) >= 1
    assert gate_reason == "target_already_acquired"


def test_build_draft_warning_only_when_quality_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def _bad_chat(*, prompt: str, system: str, temperature: float = 0.2, max_tokens: int = 700) -> str | None:
        return (
            "*Thesis*\n- Read more\n*What the target does*\n- x\n"
            "*Why it’s a fit for portfolio company*\n- x\n*Risks*\n- x\n"
            "*Funding history and backers*\n- x"
        )

    monkeypatch.setattr(board_seat_daily, "_chat_completion", _bad_chat)
    monkeypatch.setattr(board_seat_daily, "_max_web_rewrites", lambda: 1)

    funding = board_seat_daily.FundingSnapshot(
        target="Databento",
        target_key="databento",
        total_raised="unknown",
        latest_round="unknown",
        latest_round_date="unknown",
        backers=(),
        evidence_count=0,
        distinct_domains=0,
        conflict_flags=(),
        verification_status="weak",
        source_rows=(),
    )
    rows = [_row(title="OpenAI acquires Databento", url="https://a.com/1", snippet="analysis")]
    draft = board_seat_daily._build_draft(
        company="OpenAI",
        target="Databento",
        evidence_rows=rows,
        funding=funding,
        repitch_note=None,
    )
    assert draft.generation_mode == "web_synth_failed"
    assert draft.text == ""
    assert draft.memory_rewrite_used is False
    assert draft.quality_fail_codes


def test_run_once_skips_when_no_high_conf_target(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="OpenAI", channel_ref="openai", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: ([_row(title="OpenAI platform update", url="https://example.com/1", snippet="general update")], []),
    )

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["ok"] is True
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    assert payload["skipped"][0]["reason"] == "no_high_confidence_new_target"


def test_run_once_quality_failure_posts_diagnostic_and_skips(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, str | None]] = []

    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="OpenAI", channel_ref="openai", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: ([_row(title="OpenAI evaluates Databento partnership", url="https://news.one/a", snippet="candidate target Databento")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_build_candidate_pool",
        lambda **kwargs: [
            board_seat_daily.CandidateScore(
                target="Databento",
                target_key="databento",
                score=0.91,
                confidence="high",
                evidence_count=2,
                distinct_domains=2,
                row_indexes=(0,),
            )
        ],
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_verify_target_candidate",
        lambda **kwargs: (
            True,
            [
                _row(title="Databento raises $40M", url="https://techcrunch.com/d", snippet="funding and company profile"),
                _row(title="Databento funding profile", url="https://crunchbase.com/org/databento", snippet="company and investors"),
            ],
            "",
            0.91,
        ),
    )

    funding = board_seat_daily.FundingSnapshot(
        target="Databento",
        target_key="databento",
        total_raised="$40M",
        latest_round="Series B",
        latest_round_date="unknown",
        backers=("Sequoia",),
        evidence_count=2,
        distinct_domains=2,
        conflict_flags=(),
        verification_status="partial",
        source_rows=(),
    )
    monkeypatch.setattr(board_seat_daily, "_funding_snapshot_for_target", lambda **kwargs: funding)
    monkeypatch.setattr(
        board_seat_daily,
        "_build_draft",
        lambda **kwargs: board_seat_daily.DraftResult(
            text="",
            generation_mode="web_synth_failed",
            quality_fail_codes=("artifact_term",),
            memory_rewrite_used=False,
        ),
    )

    def _fake_post(*, channel_ref: str, text: str, thread_ts: str | None = None):
        calls.append({"channel_ref": channel_ref, "text": text, "thread_ts": thread_ts})
        idx = len(calls)
        return "C123", f"1700.00{idx}", None

    monkeypatch.setattr(board_seat_daily, "_post_to_slack", _fake_post)

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    row = payload["skipped"][0]
    assert row["reason"] == "draft_quality_failed"
    assert row["generation_mode"] == "web_synth_failed"
    assert "artifact_term" in row["quality_fail_codes"]
    assert len(calls) >= 1
    assert "failed quality checks" in str(calls[0].get("text") or "").lower()


def test_run_once_retries_next_target_when_first_skipped(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="Anthropic", channel_ref="anthropic", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: (
            [
                _row(title="Anthropic acquisition candidate AlphaAI", url="https://news.one/a", snippet="acquisition"),
                _row(title="Anthropic acquisition candidate BetaData", url="https://news.two/b", snippet="acquisition"),
            ],
            [],
        ),
    )

    monkeypatch.setattr(
        board_seat_daily,
        "_build_candidate_pool",
        lambda **kwargs: [
            board_seat_daily.CandidateScore(
                target="AlphaAI",
                target_key="alphaai",
                score=0.9,
                confidence="high",
                evidence_count=3,
                distinct_domains=2,
                row_indexes=(0,),
            ),
            board_seat_daily.CandidateScore(
                target="BetaData",
                target_key="betadata",
                score=0.88,
                confidence="high",
                evidence_count=3,
                distinct_domains=2,
                row_indexes=(1,),
            ),
        ],
    )

    def _fake_verify(**kwargs):
        if kwargs.get("target") == "AlphaAI":
            return False, [], "entity_unverified", 0.2
        return (
            True,
            [
                _row(title="BetaData funding", url="https://techcrunch.com/betadata", snippet="raised $60M"),
                _row(title="BetaData profile", url="https://crunchbase.com/organization/betadata", snippet="investors"),
            ],
            "",
            0.92,
        )

    monkeypatch.setattr(board_seat_daily, "_verify_target_candidate", _fake_verify)
    monkeypatch.setattr(
        board_seat_daily,
        "_funding_snapshot_for_target",
        lambda **kwargs: board_seat_daily.FundingSnapshot(
            target="BetaData",
            target_key="betadata",
            total_raised="$60M",
            latest_round="Series B",
            latest_round_date="unknown",
            backers=("Sequoia",),
            evidence_count=2,
            distinct_domains=2,
            conflict_flags=(),
            verification_status="partial",
            source_rows=(),
        ),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_build_draft",
        lambda **kwargs: board_seat_daily.DraftResult(
            text=(
                "*Board Seat as a Service — Anthropic*\n"
                "*Thesis*\n- Acquire BetaData.\n"
                "*What the target does*\n- Data tooling.\n"
                "*Why it’s a fit for portfolio company*\n- Improves stack.\n"
                "*Risks*\n- Integration risk.\n"
                "*Funding history and backers*\n- Total raised: $60M"
            ),
            generation_mode="web_synth",
            quality_fail_codes=(),
            memory_rewrite_used=False,
        ),
    )
    monkeypatch.setattr(board_seat_daily, "_post_to_slack", lambda **kwargs: ("C123", "100.1", None))

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert len(payload["sent"]) == 1
    assert payload["sent"][0]["target"] == "BetaData"
    assert payload["sent"][0]["candidates_evaluated_total"] >= 2


def test_run_once_replenishes_batches_until_winner(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="OpenAI", channel_ref="openai", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: (
            [
                _row(title="OpenAI explores DealA", url="https://one.example/a", snippet="acquisition"),
                _row(title="OpenAI explores DealB", url="https://two.example/b", snippet="acquisition"),
            ],
            [],
        ),
    )
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_MAX_LLM_BATCHES", "2")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_MAX_CANDIDATE_EVALS", "10")

    calls = {"n": 0}

    def _pool(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return [
                board_seat_daily.CandidateScore(
                    target="DealA",
                    target_key="deala",
                    score=0.8,
                    confidence="high",
                    evidence_count=1,
                    distinct_domains=1,
                    row_indexes=(0,),
                )
            ]
        return [
            board_seat_daily.CandidateScore(
                target="DealB",
                target_key="dealb",
                score=0.79,
                confidence="high",
                evidence_count=1,
                distinct_domains=1,
                row_indexes=(1,),
            )
        ]

    monkeypatch.setattr(board_seat_daily, "_build_candidate_pool", _pool)

    def _verify(**kwargs):
        if kwargs.get("target") == "DealA":
            return False, [], "entity_unverified", 0.2
        return (
            True,
            [
                _row(title="DealB raises $30M", url="https://techcrunch.com/dealb", snippet="funding"),
                _row(title="DealB company profile", url="https://crunchbase.com/organization/dealb", snippet="investors"),
            ],
            "",
            0.91,
        )

    monkeypatch.setattr(board_seat_daily, "_verify_target_candidate", _verify)
    monkeypatch.setattr(
        board_seat_daily,
        "_funding_snapshot_for_target",
        lambda **kwargs: board_seat_daily.FundingSnapshot(
            target="DealB",
            target_key="dealb",
            total_raised="$30M",
            latest_round="Series A",
            latest_round_date="unknown",
            backers=("Benchmark",),
            evidence_count=2,
            distinct_domains=2,
            conflict_flags=(),
            verification_status="partial",
            source_rows=(),
        ),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_build_draft",
        lambda **kwargs: board_seat_daily.DraftResult(
            text=(
                "*Board Seat as a Service — OpenAI*\n"
                "*Thesis*\n- Acquire DealB.\n"
                "*What the target does*\n- Workflow infra.\n"
                "*Why it’s a fit for portfolio company*\n- Platform leverage.\n"
                "*Risks*\n- Integration risk.\n"
                "*Funding history and backers*\n- Total raised: $30M"
            ),
            generation_mode="web_synth",
            quality_fail_codes=(),
            memory_rewrite_used=False,
        ),
    )
    monkeypatch.setattr(board_seat_daily, "_post_to_slack", lambda **kwargs: ("C123", "100.2", None))

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert len(payload["sent"]) == 1
    sent_row = payload["sent"][0]
    assert sent_row["target"] == "DealB"
    assert sent_row["llm_batches_used"] == 2
    assert sent_row["rejections_by_reason"].get("entity_unverified", 0) >= 1


def test_run_once_simple_mode_sends_valid_target(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(board_seat_daily, "_simple_mode_enabled", lambda: True)
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SIMPLE_MODE", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_LLM_CANDIDATE_GEN_ENABLED", "1")
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="Anduril", channel_ref="anduril", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_generate_candidate_batch",
        lambda **kwargs: [board_seat_daily.CandidateIdea(name="Saronic")],
    )

    def _collect(queries):
        q = " ".join(queries).lower()
        if "saronic" in q and "funding" in q:
            return ([_row(title="Saronic raises $600M", url="https://techcrunch.com/saronic", snippet="funding round")], [])
        if "saronic" in q and "company" in q:
            return ([_row(title="Saronic company profile", url="https://www.saronic.com", snippet="autonomous vessels company")], [])
        return ([_row(title="Anduril acquisition context", url="https://example.com/anduril", snippet="strategy")], [])

    monkeypatch.setattr(board_seat_daily, "_collect_web_rows", _collect)
    monkeypatch.setattr(
        board_seat_daily,
        "_build_draft_simple",
        lambda **kwargs: board_seat_daily.DraftResult(
            text=(
                "*Board Seat as a Service — Anduril*\n"
                "*Thesis*\n- Idea: Acquire/Acquihire Saronic.\n"
                "*What target does*\n- Builds autonomous vessel workflows.\n"
                "*Why now*\n- 2026 procurement momentum and budget visibility.\n"
                "*Fit + value creation*\n- Faster deployment and program attach.\n"
                "*Risks / kill criteria*\n- Integration and valuation risk.\n"
                "*Funding snapshot*\n- History: $600M disclosed.\n- Latest round/backers: Series C (2025), led by investor group."
            ),
            generation_mode="web_synth",
            quality_fail_codes=tuple(),
            memory_rewrite_used=False,
        ),
    )
    monkeypatch.setattr(board_seat_daily, "_post_to_slack", lambda **kwargs: ("C123", "100.3", None))

    payload = board_seat_daily.run_once(force=True, dry_run=False)
    assert len(payload["sent"]) == 1
    sent = payload["sent"][0]
    assert sent["selection_mode"] == "simple_llm"
    assert sent["target"] == "Saronic"
    assert sent["final_decision_path"] == "sent"


def test_run_once_simple_mode_respects_cooldown(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(board_seat_daily, "_simple_mode_enabled", lambda: True)
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SIMPLE_MODE", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_LLM_CANDIDATE_GEN_ENABLED", "1")
    store = board_seat_daily.BoardSeatStore()
    store.record_target(
        company="Anduril",
        target="Saronic",
        channel_ref="anduril",
        channel_id="C123",
        source="manual",
        posted_at_utc=board_seat_daily._utc_now_iso(),
        run_date_local=board_seat_daily._today_key(),
        message_ts="1.3",
    )

    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="Anduril", channel_ref="anduril", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_llm_generate_candidate_batch",
        lambda **kwargs: [board_seat_daily.CandidateIdea(name="Saronic")],
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: ([_row(title="Saronic company profile", url="https://www.saronic.com", snippet="company")], []),
    )

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    row = payload["skipped"][0]
    assert row["selection_mode"] == "simple_llm"
    assert any(r.get("reason") == "target_not_new" for r in row["candidate_rejections"])
    assert row["final_decision_path"] == "exhausted_no_valid_target"


def test_run_once_simple_mode_regenerates_batches(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(board_seat_daily, "_simple_mode_enabled", lambda: True)
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SIMPLE_MODE", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_LLM_CANDIDATE_GEN_ENABLED", "1")
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_SIMPLE_MAX_REGEN_BATCHES", "2")
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="OpenAI", channel_ref="openai", channel_id="C123")], []),
    )
    calls = {"n": 0}

    def _batch(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return [board_seat_daily.CandidateIdea(name="NonRealCo")]
        return [board_seat_daily.CandidateIdea(name="Databento")]

    monkeypatch.setattr(board_seat_daily, "_llm_generate_candidate_batch", _batch)

    def _collect(queries):
        q = " ".join(queries).lower()
        if "nonrealco" in q:
            return ([], [])
        if "databento" in q:
            return ([_row(title="Databento company profile", url="https://www.databento.com", snippet="company")], [])
        return ([_row(title="Databento raises funding", url="https://techcrunch.com/d", snippet="funding")], [])

    monkeypatch.setattr(board_seat_daily, "_collect_web_rows", _collect)
    monkeypatch.setattr(
        board_seat_daily,
        "_build_draft_simple",
        lambda **kwargs: board_seat_daily.DraftResult(
            text=(
                "*Board Seat as a Service — OpenAI*\n"
                "*Thesis*\n- Idea: Acquire/Acquihire Databento.\n"
                "*What target does*\n- Delivers market data workflows.\n"
                "*Why now*\n- 2026 API demand and contract velocity.\n"
                "*Fit + value creation*\n- Better attach and faster execution.\n"
                "*Risks / kill criteria*\n- Integration complexity.\n"
                "*Funding snapshot*\n- History: unknown.\n- Latest round/backers: unknown (unknown)."
            ),
            generation_mode="web_synth",
            quality_fail_codes=tuple(),
            memory_rewrite_used=False,
        ),
    )

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert len(payload["sent"]) == 1
    row = payload["sent"][0]
    assert row["selection_mode"] == "simple_llm"
    assert row["regen_batches_used"] == 2
    assert row["candidates_evaluated_total"] >= 2
    assert any(r.get("target") == "NonRealCo" for r in row["candidate_rejections"])


def test_run_once_skip_reports_rejection_breakdown(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="Anthropic", channel_ref="anthropic", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: ([_row(title="Anthropic update", url="https://example.com/1", snippet="general")], []),
    )
    monkeypatch.setenv("SPCLAW_BOARD_SEAT_MAX_LLM_BATCHES", "1")
    monkeypatch.setattr(
        board_seat_daily,
        "_build_candidate_pool",
        lambda **kwargs: [
            board_seat_daily.CandidateScore(
                target="Lead",
                target_key="lead",
                score=0.7,
                confidence="high",
                evidence_count=0,
                distinct_domains=0,
                row_indexes=(),
            ),
            board_seat_daily.CandidateScore(
                target="Vercept",
                target_key="vercept",
                score=0.69,
                confidence="high",
                evidence_count=0,
                distinct_domains=0,
                row_indexes=(),
            ),
        ],
    )
    monkeypatch.setattr(board_seat_daily, "_already_acquired_signal", lambda **kwargs: kwargs.get("target") == "Vercept")

    payload = board_seat_daily.run_once(force=True, dry_run=True)
    assert payload["sent"] == []
    assert len(payload["skipped"]) == 1
    skipped = payload["skipped"][0]
    assert skipped["candidates_scanned_total"] == 2
    assert skipped["candidates_evaluated_total"] == 2
    assert skipped["final_decision_path"] == "exhausted_no_valid_target"
    assert skipped["rejections_by_reason"].get("ambiguous_common_term", 0) == 1
    assert skipped["rejections_by_reason"].get("target_already_acquired", 0) == 1


def test_target_memory_lock_days_respected(board_seat_env: Path) -> None:
    store = board_seat_daily.BoardSeatStore()
    now_iso = board_seat_daily._utc_now_iso()
    store.record_target(
        company="OpenAI",
        target="Databento",
        channel_ref="openai",
        channel_id="C123",
        source="manual",
        posted_at_utc=now_iso,
        run_date_local=board_seat_daily._today_key(),
        message_ts="1.1",
    )
    last = store.latest_target_post(company="OpenAI", target_key="databento")
    assert last is not None
    assert board_seat_daily._should_block_recent(last_post=last, now_utc=board_seat_daily._utc_now()) is True


def test_refresh_funding_works(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = board_seat_daily.BoardSeatStore()
    store.record_target(
        company="OpenAI",
        target="Databento",
        channel_ref="openai",
        channel_id="C123",
        source="manual",
        posted_at_utc=board_seat_daily._utc_now_iso(),
        run_date_local=board_seat_daily._today_key(),
        message_ts="1.2",
    )

    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: ([_row(title="Databento raised $40M led by Sequoia", url="https://funding.com/1", snippet="Series B")], []),
    )

    payload = board_seat_daily._refresh_funding_payload(entities=["OpenAI"], include_recent_targets=True, report=True)
    assert payload["ok"] is True
    assert payload["count"] >= 1
    assert payload["report_path"]


def test_status_reports_metrics(board_seat_env: Path) -> None:
    payload = board_seat_daily.status()
    assert payload["ok"] is True
    assert "schedule_time" in payload
    assert "funding_confidence_distribution" in payload


def test_schema_migration_adds_missing_source_url(board_seat_env: Path) -> None:
    db_path = Path(os.environ["SPCLAW_BOARD_SEAT_DB_PATH"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE board_seat_target_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT NOT NULL,
            target TEXT NOT NULL,
            target_key TEXT NOT NULL,
            title TEXT,
            snippet TEXT,
            canonical_url TEXT,
            publisher TEXT,
            domain TEXT,
            significance REAL,
            occurred_at_utc TEXT,
            created_at_utc TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    store = board_seat_daily.BoardSeatStore(path=db_path)
    store.record_event(
        company="OpenAI",
        target="Databento",
        row=_row(title="OpenAI acquires Databento", url="https://news.example.com/1", snippet="deal"),
        significance=0.9,
    )

    conn = sqlite3.connect(db_path)
    columns = [r[1] for r in conn.execute("PRAGMA table_info(board_seat_target_events)").fetchall()]
    conn.close()
    assert "source_url" in columns


def test_schema_migration_adds_missing_board_seat_runs_columns(board_seat_env: Path) -> None:
    db_path = Path(os.environ["SPCLAW_BOARD_SEAT_DB_PATH"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE board_seat_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date_local TEXT NOT NULL,
            company TEXT NOT NULL,
            channel_ref TEXT,
            status TEXT NOT NULL,
            reason TEXT,
            target TEXT,
            target_key TEXT,
            message_ts TEXT,
            posted_at_utc TEXT,
            created_at_utc TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    store = board_seat_daily.BoardSeatStore(path=db_path)
    store.record_run(
        {
            "run_date_local": board_seat_daily._today_key(),
            "company": "OpenAI",
            "channel_ref": "openai",
            "channel_id": "C123",
            "status": "sent",
            "reason": "",
            "gate_reason": "",
            "target": "Databento",
            "target_key": "databento",
            "target_confidence": "high",
            "funding_confidence": "partial",
            "generation_mode": "web_synth",
            "quality_fail_codes": [],
            "memory_rewrite_used": False,
            "message_ts": "100.1",
            "sources_thread_ts": "100.2",
            "warning_message_ts": "",
            "posted_at_utc": board_seat_daily._utc_now_iso(),
        }
    )

    conn = sqlite3.connect(db_path)
    columns = [r[1] for r in conn.execute("PRAGMA table_info(board_seat_runs)").fetchall()]
    conn.close()
    assert "warning_message_ts" in columns
    assert "sources_thread_ts" in columns
    assert "generation_mode" in columns
    assert "rejections_by_reason" in columns
    assert "final_decision_path" in columns


def test_cli_status_json(board_seat_env: Path) -> None:
    env = os.environ.copy()
    cmd = [sys.executable, "-m", "spclaw.board_seat_daily", "status"]
    out = subprocess.check_output(cmd, text=True, env=env)
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION


def test_cli_run_once_json(board_seat_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        board_seat_daily,
        "_discover_channels_from_slack",
        lambda: ([board_seat_daily.DiscoveryChannel(company="OpenAI", channel_ref="openai", channel_id="C123")], []),
    )
    monkeypatch.setattr(
        board_seat_daily,
        "_collect_web_rows",
        lambda queries: ([_row(title="OpenAI and Databento", url="https://example.com/1", snippet="market data startup")], []),
    )

    env = os.environ.copy()
    cmd = [sys.executable, "-m", "spclaw.board_seat_daily", "run-once", "--force", "--dry-run"]
    out = subprocess.check_output(cmd, text=True, env=env)
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["format_version"] == board_seat_daily.BOARD_SEAT_FORMAT_VERSION


def test_post_to_slack_reads_ts_from_slackresponse_like_object(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        def __init__(self, payload: dict[str, str]) -> None:
            self.payload = payload

        def get(self, key: str, default: str = "") -> str:
            return self.payload.get(key, default)

    class _Client:
        calls: list[dict[str, str]] = []

        def __init__(self, token: str) -> None:
            self.token = token

        def chat_postMessage(self, **kwargs):
            _Client.calls.append({k: str(v) for k, v in kwargs.items()})
            return _Resp({"ts": "123.456"})

    monkeypatch.setattr(board_seat_daily, "WebClient", _Client)
    monkeypatch.setattr(board_seat_daily, "_slack_tokens", lambda: ["xoxb-test"])
    monkeypatch.setattr(board_seat_daily, "_resolve_channel_id", lambda _client, _ref: "C123")

    channel_id, ts, err = board_seat_daily._post_to_slack(channel_ref="openai", text="main message")
    assert err is None
    assert channel_id == "C123"
    assert ts == "123.456"

    _, thread_ts, _ = board_seat_daily._post_to_slack(channel_ref="openai", text="sources", thread_ts=ts)
    assert thread_ts == "123.456"
    assert any(call.get("thread_ts") == "123.456" for call in _Client.calls)

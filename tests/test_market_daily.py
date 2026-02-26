from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from coatue_claw.market_daily import (
    CatalystEvidence,
    EarningsPreviewItem,
    EarningsRecapRow,
    MarketDailyStore,
    QuoteSnapshot,
    debug_catalyst,
    _fetch_web_evidence_ddg,
    _fetch_web_evidence_google_serp,
    _fetch_yahoo_news,
    _session_anchor_start_utc,
    _build_message,
    _ensure_reason_like_line,
    _is_relevant_ticker_post,
    _is_relevant_ticker_headline,
    _merge_universe,
    _is_low_signal_x_post,
    _parse_times,
    _select_top_movers,
    _synthesize_earnings_bullets,
    refresh_coatue_holdings,
    run_earnings_recap,
    run_once,
)


@pytest.fixture(autouse=True)
def _legacy_catalyst_mode_default(monkeypatch):
    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "legacy_heuristic")


def test_parse_times_defaults_and_custom() -> None:
    assert _parse_times("07:00,14:15") == [(7, 0), (14, 15)]
    assert _parse_times("bad") == [(7, 0), (14, 15)]
    assert _parse_times("8:05,15:40") == [(8, 5), (15, 40)]


def test_merge_universe_overlay_and_overrides() -> None:
    top_seed = [
        QuoteSnapshot("AAPL", 300.0, 100.0, 99.0, 0.01, "2026-02-22T00:00:00+00:00"),
        QuoteSnapshot("MSFT", 290.0, 100.0, 99.0, 0.01, "2026-02-22T00:00:00+00:00"),
    ]
    merged, source_map = _merge_universe(
        top_seed=top_seed,
        coatue_tickers=["NFLX", "AAPL"],
        include_overrides={"SNOW"},
        exclude_overrides={"MSFT"},
    )
    assert merged == ["AAPL", "NFLX", "SNOW"]
    assert source_map == {
        "AAPL": "top40",
        "NFLX": "coatue_overlay",
        "SNOW": "override_include",
    }


def test_select_top_movers_tie_breaks_by_market_cap() -> None:
    snapshots = [
        QuoteSnapshot("A", 10.0, 1.1, 1.0, 0.10, "2026-02-22T00:00:00+00:00"),
        QuoteSnapshot("B", 20.0, 0.9, 1.0, -0.10, "2026-02-22T00:00:00+00:00"),
        QuoteSnapshot("C", 30.0, 1.2, 1.0, 0.20, "2026-02-22T00:00:00+00:00"),
    ]
    top = _select_top_movers(snapshots=snapshots, top_n=2)
    assert [x.ticker for x in top] == ["C", "B"]


def test_refresh_coatue_holdings_persists_rows(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "md.sqlite"
    store = MarketDailyStore(db_path=db)
    monkeypatch.setenv("COATUE_CLAW_MD_COATUE_CIK", "0001061768")

    monkeypatch.setattr(
        "coatue_claw.market_daily._latest_13f_filing",
        lambda cik: {
            "cik": "0001061768",
            "accession_no": "0001061768-26-000001",
            "filing_date": "2026-02-14",
            "primary_doc": "x.xml",
            "filing_base": "https://example.com/filing",
        },
    )
    monkeypatch.setattr("coatue_claw.market_daily._resolve_info_table_url", lambda filing_base: "https://example.com/infotable.xml")
    monkeypatch.setattr(
        "coatue_claw.market_daily._fetch_text",
        lambda url, headers: """
<informationTable xmlns=\"http://www.sec.gov/edgar/document/thirteenf/informationtable\">
  <infoTable>
    <nameOfIssuer>Snowflake Inc</nameOfIssuer>
    <cusip>833445109</cusip>
    <value>1000</value>
    <shrsOrPrnAmt><sshPrnamt>100</sshPrnamt></shrsOrPrnAmt>
  </infoTable>
  <infoTable>
    <nameOfIssuer>NVIDIA Corp</nameOfIssuer>
    <cusip>67066G104</cusip>
    <value>2000</value>
    <shrsOrPrnAmt><sshPrnamt>200</sshPrnamt></shrsOrPrnAmt>
  </infoTable>
</informationTable>
""",
    )

    def _openfigi(cusip: str):
        if cusip == "67066G104":
            return ("NVDA", "openfigi", 0.95)
        return (None, "openfigi_no_match", 0.0)

    monkeypatch.setattr("coatue_claw.market_daily._resolve_ticker_via_openfigi", _openfigi)
    monkeypatch.setattr("coatue_claw.market_daily._resolve_ticker_via_name", lambda issuer: ("SNOW", "name_search", 0.65))

    result = refresh_coatue_holdings(store=store)
    assert result["updated"] is True
    assert result["rows"] == 2
    assert store.coatue_tickers() == ["NVDA", "SNOW"]


def test_run_once_dry_run_writes_artifact_and_dedupes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_MD_DB_PATH", str(tmp_path / "db/market_daily.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_MD_ARTIFACT_DIR", str(tmp_path / "artifacts/market-daily"))
    monkeypatch.setenv("COATUE_CLAW_MD_CANDIDATE_SEED_PATH", str(tmp_path / "seed.csv"))
    monkeypatch.setenv("COATUE_CLAW_MD_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_MD_TOP_N", "3")
    monkeypatch.setenv("COATUE_CLAW_MD_TMT_TOP_K", "40")

    (tmp_path / "seed.csv").write_text("ticker\nAAPL\nMSFT\nNVDA\nMETA\n", encoding="utf-8")

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 2, 20, 7, 0, 0, tzinfo=UTC)
            if tz is None:
                return base
            return base.astimezone(tz)

    monkeypatch.setattr("coatue_claw.market_daily.datetime", Frozen)
    monkeypatch.setattr("coatue_claw.market_daily._is_market_closed_now", lambda now_local: False)
    monkeypatch.setattr("coatue_claw.market_daily._auto_refresh_holdings_if_stale", lambda store: None)

    def _fake_quotes(tickers: list[str]) -> list[QuoteSnapshot]:
        values = {
            "AAPL": (3000.0, 105.0, 100.0),
            "MSFT": (2900.0, 90.0, 100.0),
            "NVDA": (2800.0, 110.0, 100.0),
            "META": (1200.0, 102.0, 100.0),
        }
        out: list[QuoteSnapshot] = []
        for ticker in tickers:
            cap, last, prev = values[ticker]
            out.append(
                QuoteSnapshot(
                    ticker=ticker,
                    market_cap=cap,
                    last_price=last,
                    previous_close=prev,
                    pct_move=(last - prev) / prev,
                    as_of_utc="2026-02-20T07:00:00+00:00",
                )
            )
        return out

    monkeypatch.setattr("coatue_claw.market_daily._fetch_quote_snapshots", _fake_quotes)
    monkeypatch.setattr(
        "coatue_claw.market_daily._build_catalyst_rows",
        lambda movers, slot_name: (
            [
                CatalystEvidence(m.ticker, "x", "https://x.com/i/1", 5, "news", "https://example.com/news")
                for m in movers
            ],
            [f"Catalyst for {m.ticker}" for m in movers],
        ),
    )

    first = run_once(manual=True, force=False, dry_run=True)
    assert first["ok"] is True
    assert first["status"] == "dry_run"
    assert first["posted"] is False
    artifact = Path(first["artifact_path"])
    assert artifact.exists()

    second = run_once(manual=True, force=False, dry_run=True)
    assert second["posted"] is False
    assert second["reason"] == "slot_already_posted"


def test_build_message_format() -> None:
    now_local = datetime(2026, 2, 20, 7, 0, 0, tzinfo=UTC)
    movers = [
        QuoteSnapshot("NVDA", 1000.0, 110.0, 100.0, 0.10, "2026-02-20T07:00:00+00:00"),
    ]
    evidence = [
        CatalystEvidence(
            ticker="NVDA",
            x_text="AI capex demand",
            x_url="https://x.com/i/web/status/1",
            x_engagement=20,
            news_title="NVIDIA signs new cloud deal",
            news_url="https://example.com/news",
            confidence=0.8,
            cause_mode="direct_evidence",
        )
    ]
    text = _build_message(
        slot_name="open",
        now_local=now_local,
        universe_count=41,
        movers=movers,
        catalyst_rows=evidence,
        catalyst_lines=["After strong AI demand, enterprise orders accelerated."],
    )
    assert "MD — Market Open" in text
    assert "3 biggest movers this morning:" in text
    assert "📈" in text
    assert "NVDA +10.0%" in text
    assert "<https://example.com/news|[News]>" in text
    assert "[X]" not in text


def test_build_message_includes_earnings_after_close_section() -> None:
    now_local = datetime(2026, 2, 20, 7, 0, 0, tzinfo=UTC)
    movers = [QuoteSnapshot("NVDA", 1000.0, 110.0, 100.0, 0.10, "2026-02-20T07:00:00+00:00")]
    text = _build_message(
        slot_name="open",
        now_local=now_local,
        universe_count=41,
        movers=movers,
        catalyst_rows=[CatalystEvidence("NVDA", None, None, 0, None, None)],
        catalyst_lines=["Shares rose after earnings optimism."],
        earnings_after_close=[
            EarningsPreviewItem(
                ticker="NVDA",
                company="NVIDIA",
                earnings_date_et="2026-02-20",
                expected_session="after_close",
            )
        ],
    )
    assert "Earnings After Close Today:" in text
    assert "NVDA (NVIDIA)" in text


def test_fallback_line_hides_x_link_but_keeps_quality_news_web() -> None:
    now_local = datetime(2026, 2, 20, 14, 15, 0, tzinfo=UTC)
    movers = [QuoteSnapshot("AMD", 1000.0, 88.0, 100.0, -0.12, "2026-02-20T14:15:00+00:00")]
    text = _build_message(
        slot_name="close",
        now_local=now_local,
        universe_count=40,
        movers=movers,
        catalyst_rows=[
            CatalystEvidence(
                ticker="AMD",
                x_text="Join our discord for free signals $AMD $INTC",
                x_url="https://x.com/i/web/status/10",
                x_engagement=200,
                news_title="AMD slips after cautious guidance",
                news_url="https://finance.yahoo.com/news/amd-slips-after-cautious-guidance-120000000.html",
                web_title="AMD drops on weak outlook",
                web_url="https://www.reuters.com/world/us/amd-drops-on-weak-outlook-2026-02-20/",
                confidence=0.2,
                cause_mode="fallback",
            )
        ],
        catalyst_lines=["Likely positioning/flow; no single confirmed catalyst."],
    )
    assert "[X]" not in text
    assert "[News]" in text
    assert "[Web]" in text


def test_specific_line_omits_x_link_even_when_present_in_fixture() -> None:
    now_local = datetime(2026, 2, 20, 14, 15, 0, tzinfo=UTC)
    movers = [QuoteSnapshot("BKNG", 1000.0, 105.0, 100.0, 0.05, "2026-02-20T14:15:00+00:00")]
    text = _build_message(
        slot_name="close",
        now_local=now_local,
        universe_count=40,
        movers=movers,
        catalyst_rows=[
            CatalystEvidence(
                ticker="BKNG",
                x_text="BKNG shares rose after upbeat guidance and stronger bookings outlook.",
                x_url="https://x.com/i/web/status/11",
                x_engagement=80,
                news_title="Booking shares rise on outlook boost",
                news_url="https://finance.yahoo.com/news/booking-shares-rise-on-outlook-boost-120000000.html",
                confidence=0.82,
                cause_mode="direct_evidence",
            )
        ],
        catalyst_lines=["Shares rose after upbeat guidance and stronger bookings outlook."],
    )
    assert "[X]" not in text


def test_catalyst_sanitization_removes_tags_urls_and_extra_emoji() -> None:
    evidence = CatalystEvidence(
        ticker="NVDA",
        x_text="BREAKING: #AI $NVDA demand up https://x.com/test 🚀",
        x_url="https://x.com/i/web/status/1",
        x_engagement=20,
        news_title="NVIDIA signs new cloud deal",
        news_url="https://example.com/news",
    )
    line = _ensure_reason_like_line("BREAKING: #AI $NVDA up 🚀 https://x.com/test", evidence=evidence)
    assert "#" not in line
    assert "$NVDA" not in line
    assert "http" not in line
    assert "🚀" not in line


def test_relevant_ticker_post_filters_ambiguous_short_tickers() -> None:
    assert _is_relevant_ticker_post(
        text="$NET stock drops after earnings miss and weaker margin guidance",
        ticker="NET",
    )
    assert _is_relevant_ticker_post(
        text="Cloudflare stock falls after Anthropic launches Claude security tool",
        ticker="NET",
        aliases=["Cloudflare"],
    )
    assert not _is_relevant_ticker_post(
        text="India's net run rate collapsed after the match",
        ticker="NET",
    )


def test_relevant_ticker_headline_filters_unrelated_titles() -> None:
    assert _is_relevant_ticker_headline(
        text="Oracle Faces AI Lawsuits As Federal Cloud Contracts Expand",
        ticker="ORCL",
        aliases=["Oracle"],
    )
    assert not _is_relevant_ticker_headline(
        text="Mastercard Partnerships With Ericsson And Cloudflare Reshape Digital Finance Role",
        ticker="ORCL",
        aliases=["Oracle"],
    )


def test_x_promo_post_rejected_for_amd_like_spam() -> None:
    spam = "Join our discord free room now $AMD $INTC $NVDA $TSLA $AAPL $MSFT $META link below"
    assert _is_low_signal_x_post(spam)


def test_reason_line_uses_generic_fallback_for_vague_x_only_text() -> None:
    evidence = CatalystEvidence(
        ticker="ORCL",
        x_text="3 Under-the-Radar Earnings Surprises Could Signal a New Trend",
        x_url="https://x.com/i/web/status/1",
        x_engagement=20,
        news_title=None,
        news_url=None,
    )
    line = _ensure_reason_like_line("3 Under-the-Radar Earnings Surprises Could Signal a New Trend", evidence=evidence)
    assert "no single confirmed catalyst" in line.lower()


def test_extract_causal_clause_prefers_causal_segment() -> None:
    from coatue_claw import market_daily as md

    source = (
        "Intel Corporation (INTC) Stock Price, News, Quote & History - Yahoo Finance. "
        "Shares rose after management raised foundry margin outlook."
    )
    phrase = md._extract_causal_clause(source)
    assert phrase is not None
    assert "after management raised foundry margin outlook" in phrase.lower()
    assert "quote & history" not in phrase.lower()


def test_reason_quality_rejects_fragment_and_menu_text() -> None:
    from coatue_claw import market_daily as md

    menu_reasons = md._reason_phrase_quality_rejections("Intel stock price, news, quote & history")
    fragment_reasons = md._reason_phrase_quality_rejections("after guidance and")
    assert "quote_directory_title" in menu_reasons
    assert "dangling_ending" in fragment_reasons
    assert "too_short" in fragment_reasons


def test_fetch_yahoo_news_parses_nested_schema(monkeypatch) -> None:
    class FakeTicker:
        news = [
            {
                "content": {
                    "pubDate": "2026-02-20T10:11:00Z",
                    "title": "Mastercard Partnerships With Ericsson And Cloudflare Reshape Digital Finance Role",
                    "clickThroughUrl": {
                        "url": "https://finance.yahoo.com/news/mastercard-partnerships-ericsson-cloudflare-031333933.html"
                    },
                }
            },
            {
                "content": {
                    "pubDate": "2026-02-20T10:12:00Z",
                    "title": "Oracle Faces AI Lawsuits As Federal Cloud Contracts Expand",
                    "clickThroughUrl": {
                        "url": "https://finance.yahoo.com/news/oracle-faces-ai-lawsuits-federal-231206167.html"
                    },
                }
            }
        ]

    monkeypatch.setattr("coatue_claw.market_daily.yf.Ticker", lambda ticker: FakeTicker())
    title, url = _fetch_yahoo_news(
        ticker="ORCL",
        since_utc=datetime(2026, 2, 19, 0, 0, 0, tzinfo=UTC),
    )
    assert "Oracle Faces AI Lawsuits" in (title or "")
    assert (url or "") == "https://finance.yahoo.com/news/oracle-faces-ai-lawsuits-federal-231206167.html"


def test_session_anchor_open_uses_previous_market_close(monkeypatch) -> None:
    class FakeBars:
        empty = False
        index = [
            datetime(2026, 2, 20, 21, 0, 0, tzinfo=UTC),  # Friday
            datetime(2026, 2, 23, 21, 0, 0, tzinfo=UTC),  # Monday
        ]

    class FakeTicker:
        def history(self, **kwargs):
            return FakeBars()

    monkeypatch.setattr("coatue_claw.market_daily.yf.Ticker", lambda ticker: FakeTicker())
    since = _session_anchor_start_utc(
        slot_name="open",
        now_utc=datetime(2026, 2, 23, 15, 0, 0, tzinfo=UTC),
    )
    assert since == datetime(2026, 2, 20, 21, 0, 0, tzinfo=UTC)


def test_ddg_web_fallback_parses_result_links(monkeypatch) -> None:
    html = """
    <html><body>
      <a class="result__a" href="/l/?uddg=https%3A%2F%2Fstocktwits.com%2Fnews%2Fanthropic-net-drop">Cloudflare stock dropped after Anthropic launch</a>
    </body></html>
    """
    monkeypatch.setattr("coatue_claw.market_daily._fetch_text", lambda url, headers: html)
    rows = _fetch_web_evidence_ddg(
        ticker="NET",
        aliases=["Cloudflare"],
        since_utc=datetime(2026, 2, 19, 0, 0, 0, tzinfo=UTC),
    )
    assert rows
    assert rows[0].source_type == "web"
    assert "Anthropic" in rows[0].text
    assert rows[0].url == "https://stocktwits.com/news/anthropic-net-drop"


def test_ddg_resolve_handles_absolute_redirect_url() -> None:
    from coatue_claw import market_daily as md

    resolved = md._ddg_resolve_url(
        "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fstocktwits.com%2Fnews%2Fanthropic-net-drop"
    )
    assert resolved == "https://stocktwits.com/news/anthropic-net-drop"


def test_google_serp_web_parses_snippets_and_answer_box(monkeypatch) -> None:
    payload = {
        "answer_box": {
            "title": "Why BKNG stock is down",
            "snippet": "AI threat narrative around agents disrupting OTA margins pressured shares.",
            "link": "https://www.tikr.com/blog/booking-stock-tumbles-6-ai-panic",
        },
        "organic_results": [
            {
                "title": "Booking Holdings falls as AI agents threaten OTA economics",
                "snippet": "Investors cited AI threat and forward outlook pressure.",
                "link": "https://finance.yahoo.com/news/booking-holdings-falls-ai-agents-120000000.html",
            }
        ],
    }
    monkeypatch.setenv("COATUE_CLAW_MD_GOOGLE_SERP_API_KEY", "test-key")
    monkeypatch.setattr("coatue_claw.market_daily._http_json", lambda url, headers, params=None, method="GET", body=None: payload)
    rows = _fetch_web_evidence_google_serp(
        ticker="BKNG",
        aliases=["Booking Holdings", "Booking.com"],
        since_utc=datetime(2026, 2, 22, 0, 0, 0, tzinfo=UTC),
        pct_move=-0.07,
    )
    assert rows
    assert any("ai threat" in row.text.lower() for row in rows)
    assert any(row.backend == "google_serp" for row in rows)


def test_bkng_dominant_ai_threat_cluster_outputs_specific_reason(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="Booking Holdings shares fell after AI threat and agent disruption concerns hit OTA names.",
                    url="https://www.tikr.com/blog/booking-stock-tumbles-6-ai-panic",
                    published_at_utc=None,
                    score=0.9,
                    driver_keywords=("ota_ai_disruption",),
                    canonical_url="https://www.tikr.com/blog/booking-stock-tumbles-6-ai-panic",
                    domain="tikr.com",
                    backend="google_serp",
                ),
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="Booking Holdings drops as AI threat narrative pressures online travel agency outlook.",
                    url="https://finance.yahoo.com/news/booking-holdings-drops-ai-threat-130000000.html",
                    published_at_utc=datetime(2026, 2, 23, 12, 0, 0, tzinfo=UTC),
                    score=0.88,
                    driver_keywords=("ota_ai_disruption", "travel_demand_outlook"),
                    canonical_url="https://finance.yahoo.com/news/booking-holdings-drops-ai-threat-130000000.html",
                    domain="finance.yahoo.com",
                ),
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Booking Holdings", "Booking.com"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 23, 0, 0, 0, tzinfo=UTC))
    mover = QuoteSnapshot("BKNG", 100.0, 92.4, 100.0, -0.076, "2026-02-23T15:00:00+00:00")
    rows, lines = md._build_catalyst_rows(movers=[mover], slot_name="open")
    assert rows[0].confirmed_cluster == "ota_ai_disruption"
    assert "ai-agent disruption fears pressured online travel stocks" in lines[0].lower()


def test_direct_evidence_fallback_uses_specific_news_without_cluster_match(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="AMD shares fell after management issued softer-than-expected margin guidance for next quarter.",
                    url="https://finance.yahoo.com/news/amd-shares-fell-after-management-issued-softer-guidance-120000000.html",
                    published_at_utc=datetime(2026, 2, 23, 14, 0, 0, tzinfo=UTC),
                    score=0.86,
                    driver_keywords=(),
                    canonical_url="https://finance.yahoo.com/news/amd-shares-fell-after-management-issued-softer-guidance-120000000.html",
                    domain="finance.yahoo.com",
                )
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["AMD", "Advanced Micro Devices"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 23, 0, 0, 0, tzinfo=UTC))
    movers = [QuoteSnapshot("AMD", 100.0, 92.0, 100.0, -0.08, "2026-02-23T15:00:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert lines[0] != "Likely positioning/flow; no single confirmed catalyst."
    assert "shares fell after" in lines[0].lower()
    assert rows[0].cause_mode == "direct_evidence"


def test_direct_evidence_fallback_skips_quote_directory_wrapper_and_uses_causal_headline(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="Intel Corporation (INTC) Stock Price, News, Quote & History - Yahoo Finance",
                    url="https://finance.yahoo.com/quote/INTC/",
                    published_at_utc=datetime(2026, 2, 24, 14, 0, 0, tzinfo=UTC),
                    score=0.99,
                    driver_keywords=(),
                    canonical_url="https://finance.yahoo.com/quote/INTC/",
                    domain="finance.yahoo.com",
                ),
                md._EvidenceCandidate(
                    source_type="web",
                    text="Intel shares rose after management raised foundry margin outlook in late commentary.",
                    url="https://www.reuters.com/world/us/intel-shares-rise-after-margin-outlook-2026-02-24/",
                    published_at_utc=datetime(2026, 2, 24, 14, 10, 0, tzinfo=UTC),
                    score=0.78,
                    driver_keywords=("earnings_guidance",),
                    canonical_url="https://www.reuters.com/world/us/intel-shares-rise-after-margin-outlook-2026-02-24/",
                    domain="reuters.com",
                ),
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Intel", "Intel Corporation"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 24, 0, 0, 0, tzinfo=UTC))
    movers = [QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-24T22:05:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert "quote & history" not in lines[0].lower()
    assert "stock price, news, quote" not in lines[0].lower()
    assert lines[0] != md.FALLBACK_CAUSE_LINE
    assert "shares rose after" in lines[0].lower()
    assert rows[0].cause_mode in {"decisive_primary", "direct_evidence"}


def test_reason_line_falls_back_when_phrase_is_quote_directory_wrapper() -> None:
    from coatue_claw import market_daily as md

    line = md._build_reason_line_from_phrase(
        pct_move=0.057,
        phrase="Intel Corporation (INTC) Stock Price, News, Quote & History - Yahoo Finance",
    )
    assert line == md.FALLBACK_CAUSE_LINE


def test_hybrid_polish_accepts_clean_rewrite_when_deterministic_is_awkward(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    class _FakeCompletions:
        @staticmethod
        def create(*args, **kwargs):
            return type(
                "Resp",
                (),
                {
                    "choices": [
                        type(
                            "Choice",
                            (),
                            {"message": type("Msg", (), {"content": "management raised foundry margin outlook"})()},
                        )()
                    ]
                },
            )()

    class _FakeChat:
        completions = _FakeCompletions()

    class _FakeClient:
        chat = _FakeChat()

    monkeypatch.setenv("COATUE_CLAW_MD_REASON_QUALITY_MODE", "hybrid")
    monkeypatch.setenv("COATUE_CLAW_MD_REASON_POLISH_ENABLED", "1")
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: _FakeClient())

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="Intel shares rose after margin outlook and",
                    url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    published_at_utc=datetime(2026, 2, 24, 14, 0, 0, tzinfo=UTC),
                    score=0.82,
                    driver_keywords=(),
                    canonical_url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    domain="reuters.com",
                )
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Intel", "Intel Corporation"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 24, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-24T22:05:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].cause_render_mode == "llm_polish"
    assert lines[0] != md.FALLBACK_CAUSE_LINE
    assert "shares rose after" in lines[0].lower()


def test_hybrid_polish_falls_back_on_invalid_rewrite(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    class _FakeCompletions:
        @staticmethod
        def create(*args, **kwargs):
            return type(
                "Resp",
                (),
                {"choices": [type("Choice", (), {"message": type("Msg", (), {"content": "and for with to"})()})()]},
            )()

    class _FakeChat:
        completions = _FakeCompletions()

    class _FakeClient:
        chat = _FakeChat()

    monkeypatch.setenv("COATUE_CLAW_MD_REASON_QUALITY_MODE", "hybrid")
    monkeypatch.setenv("COATUE_CLAW_MD_REASON_POLISH_ENABLED", "1")
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: _FakeClient())

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="Intel shares after outlook and",
                    url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    published_at_utc=datetime(2026, 2, 24, 14, 0, 0, tzinfo=UTC),
                    score=0.8,
                    driver_keywords=(),
                    canonical_url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    domain="reuters.com",
                )
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Intel"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 24, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-24T22:05:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].cause_render_mode == "fallback"
    assert lines[0] == md.FALLBACK_CAUSE_LINE


def test_no_hallucination_guard_rejects_entity_drift(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    class _FakeCompletions:
        @staticmethod
        def create(*args, **kwargs):
            return type(
                "Resp",
                (),
                {
                    "choices": [
                        type("Choice", (), {"message": type("Msg", (), {"content": "NVIDIA raised margin outlook"})()})()
                    ]
                },
            )()

    class _FakeChat:
        completions = _FakeCompletions()

    class _FakeClient:
        chat = _FakeChat()

    monkeypatch.setenv("COATUE_CLAW_MD_REASON_QUALITY_MODE", "hybrid")
    monkeypatch.setenv("COATUE_CLAW_MD_REASON_POLISH_ENABLED", "1")
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: _FakeClient())

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="INTC shares rose after margin outlook and",
                    url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    published_at_utc=datetime(2026, 2, 24, 14, 0, 0, tzinfo=UTC),
                    score=0.82,
                    driver_keywords=(),
                    canonical_url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    domain="reuters.com",
                )
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Intel"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 24, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-24T22:05:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].cause_render_mode == "fallback"
    assert lines[0] == md.FALLBACK_CAUSE_LINE


def test_direct_evidence_line_is_fallback_when_only_low_quality_phrase_exists(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_REASON_QUALITY_MODE", "deterministic")
    monkeypatch.setenv("COATUE_CLAW_MD_REASON_POLISH_ENABLED", "0")

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="Intel shares after outlook and",
                    url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    published_at_utc=datetime(2026, 2, 24, 14, 0, 0, tzinfo=UTC),
                    score=0.81,
                    driver_keywords=(),
                    canonical_url="https://www.reuters.com/world/us/intel-update-2026-02-24/",
                    domain="reuters.com",
                )
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Intel"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 24, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-24T22:05:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].cause_mode == "direct_evidence"
    assert rows[0].cause_render_mode == "fallback"
    assert lines[0] == md.FALLBACK_CAUSE_LINE


def test_debug_catalyst_returns_expected_shape(monkeypatch) -> None:
    evidence = CatalystEvidence(
        ticker="NET",
        x_text="Cloudflare sold off after Anthropic announced Claude Code Security",
        x_url="https://x.com/i/web/status/1",
        x_engagement=12,
        news_title="Cybersecurity stocks slide after Anthropic launch",
        news_url="https://finance.yahoo.com/news/example",
        web_title="Cloudflare stock drops on Anthropic security release",
        web_url="https://stocktwits.com/news/example",
        confidence=0.8,
        chosen_source="yahoo_news",
        driver_keywords=("anthropic_claude",),
        top_evidence=("yahoo_news(0.80): sample",),
        rejected_reasons=("x:no_relevant_matches",),
        since_utc="2026-02-20T21:00:00+00:00",
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._build_catalyst_for_mover",
        lambda mover, slot_name, since_utc: (evidence, "After Anthropic launched Claude security tooling, NET sold off."),
    )
    monkeypatch.setattr("coatue_claw.market_daily._fetch_quote_snapshots", lambda tickers: [])
    payload = debug_catalyst(ticker="NET", slot_name="open")
    assert payload["ok"] is True
    assert payload["ticker"] == "NET"
    assert payload["chosen_source"] == "yahoo_news"
    assert "x" not in payload["links"]
    assert payload["links"]["web"] == "https://stocktwits.com/news/example"


def test_negative_mover_prefers_negative_driver_language(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    candidates = [
        md._EvidenceCandidate(
            source_type="web",
            text="Cloudflare and CrowdStrike sink after Anthropic unveils Claude Code Security",
            url="https://coincentral.com/cloudflare-net-stock-mastercard-partnership-and-market-selloff-explained/",
            published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
            score=0.9,
            driver_keywords=("anthropic_claude_cyber", "anthropic_claude"),
            canonical_url="https://coincentral.com/cloudflare-net-stock-mastercard-partnership-and-market-selloff-explained/",
            domain="coincentral.com",
        ),
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="Cybersecurity stocks drop as Anthropic launches Claude Code Security tool",
            url="https://stocktwits.com/news/anthropic",
            published_at_utc=datetime(2026, 2, 20, 10, 30, 0, tzinfo=UTC),
            score=0.9,
            driver_keywords=("anthropic_claude_cyber", "anthropic_claude"),
            canonical_url="https://stocktwits.com/news/anthropic",
            domain="stocktwits.com",
        ),
    ]

    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Cloudflare"])
    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", lambda ticker, aliases, since_utc, pct_move=None: (candidates, []))

    mover = QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00")
    rows, lines = md._build_catalyst_rows(movers=[mover], slot_name="open")
    assert rows[0].confirmed_cluster == "anthropic_claude_cyber"
    assert "anthropic launched claude code security" in lines[0].lower()


def test_collect_evidence_triggers_web_when_directional_signal_missing(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    yahoo_candidates = [
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="Cloudflare partnership update for enterprise security spending trends",
            url="https://finance.yahoo.com/news/cloudflare-partnership-update-120000000.html",
            published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
            score=0.92,
            driver_keywords=("cybersecurity_competition",),
            canonical_url="https://finance.yahoo.com/news/cloudflare-partnership-update-120000000.html",
            domain="finance.yahoo.com",
        )
    ]
    web_candidates = [
        md._EvidenceCandidate(
            source_type="web",
            text="Why did CRWD OKTA NET PANW cyber security stocks fall today",
            url="https://stocktwits.com/news/example",
            published_at_utc=datetime(2026, 2, 20, 11, 0, 0, tzinfo=UTC),
            score=0.7,
            driver_keywords=("anthropic_claude", "cybersecurity_competition"),
        )
    ]
    calls = {"web": 0}

    monkeypatch.setattr("coatue_claw.market_daily._fetch_yahoo_news_candidates", lambda ticker, aliases, since_utc: yahoo_candidates)

    def _fake_web(ticker, aliases, since_utc, pct_move=None):
        calls["web"] += 1
        return (web_candidates, "google_serp", [])

    monkeypatch.setattr("coatue_claw.market_daily._fetch_web_evidence", _fake_web)
    rows, _, web_backend = md._collect_evidence_for_ticker(
        ticker="NET",
        aliases=["Cloudflare"],
        since_utc=datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC),
        pct_move=-0.05,
    )
    assert calls["web"] == 1
    assert web_backend == "google_serp"
    assert any(r.source_type == "web" for r in rows)


def test_generic_wrapper_detection_blocks_tautologies() -> None:
    from coatue_claw import market_daily as md

    assert md._is_generic_headline_wrapper(
        text="Why NET stock is down today",
        ticker="NET",
        aliases=["Cloudflare"],
    )
    assert md._contains_disallowed_reason_phrasing("After NET stock is down today.")


def test_generic_wrapper_detection_blocks_quote_directory_title() -> None:
    from coatue_claw import market_daily as md

    title = "Intel Corporation (INTC) Stock Price, News, Quote & History - Yahoo Finance"
    assert md._is_quote_directory_title(title)
    assert md._is_generic_headline_wrapper(text=title, ticker="INTC", aliases=["Intel Corporation"])
    assert md._contains_disallowed_reason_phrasing(title)


def test_quote_directory_url_is_rejected_even_without_obvious_title_phrase() -> None:
    from coatue_claw import market_daily as md

    candidate = md._EvidenceCandidate(
        source_type="web",
        text="Intel investor updates and overview",
        url="https://finance.yahoo.com/quote/INTC/",
        published_at_utc=None,
        score=0.85,
    )
    rows = md._normalize_evidence_candidates(
        candidates=[candidate],
        ticker="INTC",
        aliases=["Intel Corporation", "Intel"],
    )
    assert rows
    assert rows[0].reject_reason == "generic_wrapper"


def test_anthropic_cluster_extraction_maps_keywords() -> None:
    from coatue_claw import market_daily as md

    keys = md._extract_driver_keywords("Cybersecurity stocks fell after Anthropic launched Claude Code Security tool")
    assert "anthropic_claude_cyber" in keys


def test_regulatory_probe_cluster_extraction_maps_keywords() -> None:
    from coatue_claw import market_daily as md

    keys = md._extract_driver_keywords("AppLovin shares slid after reports of an active SEC probe and ongoing investigation")
    assert "regulatory_probe" in keys


def test_barrons_domain_counts_as_quality_source() -> None:
    from coatue_claw import market_daily as md

    assert md._is_quality_domain("https://www.barrons.com/articles/applovin-stock-drops-probe-report-9e76d74f")


def test_corroboration_gate_requires_two_independent_sources() -> None:
    from coatue_claw import market_daily as md

    one_source = [
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="Cybersecurity stocks fell after Anthropic launched Claude Code Security.",
            url="https://finance.yahoo.com/news/a",
            published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
            score=0.9,
            driver_keywords=("anthropic_claude_cyber",),
            canonical_url="https://finance.yahoo.com/news/a",
            domain="finance.yahoo.com",
        )
    ]
    two_sources = one_source + [
        md._EvidenceCandidate(
            source_type="web",
            text="Cloudflare and CrowdStrike slide as Anthropic unveils Claude Code Security",
            url="https://stocktwits.com/news/b",
            published_at_utc=None,
            score=0.8,
            driver_keywords=("anthropic_claude_cyber",),
            canonical_url="https://stocktwits.com/news/b",
            domain="stocktwits.com",
        )
    ]
    assert not md._cluster_is_corroborated(one_source)
    assert md._cluster_is_corroborated(two_sources)


def test_net_crwd_shared_cluster_uses_specific_anthropic_reason(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="Cybersecurity stocks fell after Anthropic launched Claude Code Security tool",
                    url="https://finance.yahoo.com/news/anthropic-cyber",
                    published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
                    score=0.9,
                    driver_keywords=("anthropic_claude_cyber", "anthropic_claude"),
                    canonical_url="https://finance.yahoo.com/news/anthropic-cyber",
                    domain="finance.yahoo.com",
                ),
                md._EvidenceCandidate(
                    source_type="web",
                    text="Cloudflare and CrowdStrike drop after Anthropic Claude Code Security release",
                    url="https://stocktwits.com/news/anthropic-cyber-drop",
                    published_at_utc=None,
                    score=0.78,
                    driver_keywords=("anthropic_claude_cyber",),
                    canonical_url="https://stocktwits.com/news/anthropic-cyber-drop",
                    domain="stocktwits.com",
                ),
                md._EvidenceCandidate(
                    source_type="web",
                    text="Why NET stock is down today",
                    url="https://example.com/wrapper",
                    published_at_utc=None,
                    score=0.95,
                    driver_keywords=("anthropic_claude_cyber",),
                    reject_reason="generic_wrapper",
                    canonical_url="https://example.com/wrapper",
                    domain="example.com",
                ),
            ],
            [],
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: [ticker])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))

    movers = [
        QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00"),
        QuoteSnapshot("CRWD", 100.0, 92.1, 100.0, -0.079, "2026-02-20T12:00:00+00:00"),
    ]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].confirmed_cluster == "anthropic_claude_cyber"
    assert rows[1].confirmed_cluster == "anthropic_claude_cyber"
    assert lines[0] == lines[1]
    assert "anthropic launched claude code security" in lines[0].lower()


def test_single_source_only_uses_uncertainty_fallback(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="Cloudflare stock is down today after mixed sentiment",
                    url="https://finance.yahoo.com/news/net-down",
                    published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
                    score=0.9,
                    driver_keywords=("anthropic_claude_cyber",),
                    canonical_url="https://finance.yahoo.com/news/net-down",
                    domain="finance.yahoo.com",
                )
            ],
            [],
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: [ticker])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00")]
    _, lines = md._build_catalyst_rows(movers=movers, slot_name="open")
    assert lines[0] == "Likely positioning/flow; no single confirmed catalyst."


def test_app_regulatory_probe_cluster_outputs_specific_reason(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="AppLovin stock drops after report says SEC probe into ad practices remains active.",
                    url="https://www.barrons.com/articles/applovin-stock-drops-probe-report-9e76d74f",
                    published_at_utc=None,
                    score=0.93,
                    driver_keywords=("regulatory_probe",),
                    canonical_url="https://www.barrons.com/articles/applovin-stock-drops-probe-report-9e76d74f",
                    domain="barrons.com",
                    backend="google_serp",
                ),
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="AppLovin shares fall as SEC investigation overhang weighs on sentiment.",
                    url="https://finance.yahoo.com/news/applovin-shares-fall-sec-investigation-overhang-140000000.html",
                    published_at_utc=datetime(2026, 2, 23, 14, 0, 0, tzinfo=UTC),
                    score=0.89,
                    driver_keywords=("regulatory_probe",),
                    canonical_url="https://finance.yahoo.com/news/applovin-shares-fall-sec-investigation-overhang-140000000.html",
                    domain="finance.yahoo.com",
                ),
            ],
            [],
            "google_serp",
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["AppLovin"])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 23, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("APP", 100.0, 92.5, 100.0, -0.075, "2026-02-23T15:00:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="open")
    assert rows[0].confirmed_cluster == "regulatory_probe"
    assert "sec probe" in lines[0].lower()


def test_single_strong_quality_source_can_drive_decisive_primary_reason(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_collect(ticker, aliases, since_utc, pct_move=None):
        return (
            [
                md._EvidenceCandidate(
                    source_type="yahoo_news",
                    text="Cybersecurity stocks fell after Anthropic launched Claude Code Security tool",
                    url="https://finance.yahoo.com/news/cybersecurity-stocks-fall-anthropic-101200000.html",
                    published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
                    score=0.93,
                    driver_keywords=("anthropic_claude_cyber", "anthropic_claude"),
                    canonical_url="https://finance.yahoo.com/news/cybersecurity-stocks-fall-anthropic-101200000.html",
                    domain="finance.yahoo.com",
                ),
            ],
            [],
        )

    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", _fake_collect)
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: [ticker])
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))

    movers = [QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00")]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="open")
    assert rows[0].confirmed_cluster == "anthropic_claude_cyber"
    assert lines[0] == "Shares fell after Anthropic launched Claude Code Security, pressuring cybersecurity stocks."


def test_decisive_primary_reason_allows_strong_event_even_with_small_margin() -> None:
    from coatue_claw import market_daily as md

    candidate = md._EvidenceCandidate(
        source_type="yahoo_news",
        text="Oracle Faces AI Lawsuits As Federal Cloud Contracts Expand",
        url="https://finance.yahoo.com/news/oracle-faces-ai-lawsuits-federal-231206167.html",
        published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
        score=0.95,
        driver_keywords=("deal_contract",),
        canonical_url="https://finance.yahoo.com/news/oracle-faces-ai-lawsuits-federal-231206167.html",
        domain="finance.yahoo.com",
    )
    assert md._can_use_decisive_primary_reason(
        cluster_candidate=candidate,
        top_cluster_score=1.0,
        second_cluster_score=0.997,
        pct_move=-0.05,
    )


def test_cyber_basket_carries_anthropic_cause_to_net(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_build(mover, slot_name, since_utc):
        if mover.ticker == "CRWD":
            ev = CatalystEvidence(
                ticker="CRWD",
                x_text=None,
                x_url=None,
                x_engagement=0,
                news_title="Cybersecurity stocks fell after Anthropic launched Claude Code Security tool",
                news_url="https://finance.yahoo.com/news/anthropic-cyber",
                web_title=None,
                web_url=None,
                confidence=0.9,
                chosen_source="yahoo_news",
                driver_keywords=("anthropic_claude_cyber",),
                confirmed_cluster="anthropic_claude_cyber",
                confirmed_cause_phrase="Anthropic launched Claude Code Security, pressuring cybersecurity stocks.",
            )
            return ev, "Shares fell after Anthropic launched Claude Code Security, pressuring cybersecurity stocks."

        ev = CatalystEvidence(
            ticker="NET",
            x_text=None,
            x_url=None,
            x_engagement=0,
            news_title="Cloudflare and Mastercard announce strategic cybersecurity partnership",
            news_url="https://finance.yahoo.com/news/partnership",
            web_title=None,
            web_url=None,
            confidence=0.7,
            chosen_source="yahoo_news",
            driver_keywords=("deal_contract",),
            confirmed_cluster="deal_contract",
            confirmed_cause_phrase="a major deal or contract update changed sentiment.",
        )
        return ev, "Shares fell after a major deal or contract update changed sentiment."

    monkeypatch.setattr("coatue_claw.market_daily._build_catalyst_for_mover", _fake_build)
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))

    movers = [
        QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00"),
        QuoteSnapshot("CRWD", 100.0, 92.1, 100.0, -0.079, "2026-02-20T12:00:00+00:00"),
    ]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].confirmed_cluster == "anthropic_claude_cyber"
    assert "anthropic launched claude code security" in lines[0].lower()
    assert lines[0] == lines[1]


def test_generic_deal_contract_cluster_is_not_reused_across_movers(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    def _fake_build(mover, slot_name, since_utc):
        if mover.ticker == "NET":
            ev = CatalystEvidence(
                ticker="NET",
                x_text=None,
                x_url=None,
                x_engagement=0,
                news_title="Mastercard Partnerships With Ericsson And Cloudflare Reshape Digital Finance Role",
                news_url="https://finance.yahoo.com/news/mastercard-partnerships-ericsson-cloudflare-031333933.html",
                web_title=None,
                web_url=None,
                confidence=0.8,
                chosen_source="yahoo_news",
                driver_keywords=("deal_contract",),
                confirmed_cluster="deal_contract",
                confirmed_cause_phrase="Mastercard Partnerships With Ericsson And Cloudflare Reshape Digital Finance Role.",
            )
            return ev, "Shares fell after Mastercard Partnerships With Ericsson And Cloudflare Reshape Digital Finance Role."
        ev = CatalystEvidence(
            ticker="ORCL",
            x_text=None,
            x_url=None,
            x_engagement=0,
            news_title="Oracle Faces AI Lawsuits As Federal Cloud Contracts Expand",
            news_url="https://finance.yahoo.com/news/oracle-faces-ai-lawsuits-federal-231206167.html",
            web_title=None,
            web_url=None,
            confidence=0.9,
            chosen_source="yahoo_news",
            driver_keywords=("deal_contract",),
            confirmed_cluster="deal_contract",
            confirmed_cause_phrase="Oracle Faces AI Lawsuits As Federal Cloud Contracts Expand.",
        )
        return ev, "Shares fell after Oracle Faces AI Lawsuits As Federal Cloud Contracts Expand."

    monkeypatch.setattr("coatue_claw.market_daily._build_catalyst_for_mover", _fake_build)
    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))

    movers = [
        QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00"),
        QuoteSnapshot("ORCL", 100.0, 94.6, 100.0, -0.054, "2026-02-20T12:00:00+00:00"),
    ]
    rows, lines = md._build_catalyst_rows(movers=movers, slot_name="close")
    assert rows[0].confirmed_cluster == "deal_contract"
    assert rows[1].confirmed_cluster == "deal_contract"
    assert "mastercard partnerships" in lines[0].lower()
    assert "oracle faces ai lawsuits" in lines[1].lower()


def test_infer_expected_session_from_history() -> None:
    from coatue_claw import market_daily as md

    after_close = [
        (datetime(2026, 2, 20, 21, 5, tzinfo=UTC), None, None, None),
        (datetime(2026, 1, 20, 21, 15, tzinfo=UTC), None, None, None),
    ]
    before_open = [
        (datetime(2026, 2, 20, 12, 10, tzinfo=UTC), None, None, None),
        (datetime(2026, 1, 20, 12, 20, tzinfo=UTC), None, None, None),
    ]
    assert md._infer_expected_session(earnings_history=after_close) == "after_close"
    assert md._infer_expected_session(earnings_history=before_open) == "before_open"


def test_recap_llm_unavailable_uses_deterministic_backup_bullets(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    row = EarningsRecapRow(
        ticker="NVDA",
        company="NVIDIA",
        earnings_date_et="2026-02-20",
        inferred_session="after_close",
        market_cap=1000.0,
        last_price=112.0,
        regular_close=109.0,
        since_close_pct=(112.0 - 109.0) / 109.0,
        eps_estimate=1.2,
        reported_eps=1.3,
        surprise_pct=8.0,
    )
    anchor = md._EvidenceCandidate(
        source_type="web",
        text="NVIDIA rises after data-center demand commentary",
        context_text="NVIDIA shares climbed after strong datacenter demand commentary and upbeat AI spending signals.",
        url="https://example.com/nvda-anchor",
        published_at_utc=datetime(2026, 2, 20, 21, 0, 0, tzinfo=UTC),
        score=0.86,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([anchor], [anchor], [], "google_serp"),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)
    hydrated = md._hydrate_recap_row(
        row=row,
        since_utc=datetime(2026, 2, 20, 20, 0, 0, tzinfo=UTC),
        client=None,
    )
    assert 2 <= len(hydrated.bullets) <= 4
    assert hydrated.recap_generation_mode == "deterministic_backup"
    assert any("[S1]" in b for b in hydrated.bullets if "Key catalyst:" in b or "Since regular close" in b)


def test_run_earnings_recap_skips_when_no_reporters(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_MD_DB_PATH", str(tmp_path / "db/market_daily.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_MD_ARTIFACT_DIR", str(tmp_path / "artifacts/market-daily"))

    monkeypatch.setattr(
        "coatue_claw.market_daily._build_final_universe",
        lambda store, refresh_holdings=True: (
            [QuoteSnapshot("AAPL", 1.0, 101.0, 100.0, 0.01, "2026-02-20T00:00:00+00:00")],
            {"AAPL": "top40"},
            set(),
            set(),
            None,
        ),
    )
    monkeypatch.setattr("coatue_claw.market_daily._collect_reported_today_rows", lambda universe, now_utc=None: [])
    result = run_earnings_recap(manual=True, force=False, dry_run=True)
    assert result["ok"] is True
    assert result["posted"] is False
    assert result["reason"] == "no_reporters"


def test_run_earnings_recap_selects_top4_and_writes_artifact(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_MD_DB_PATH", str(tmp_path / "db/market_daily.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_MD_ARTIFACT_DIR", str(tmp_path / "artifacts/market-daily"))
    monkeypatch.setenv("COATUE_CLAW_MD_TZ", "UTC")
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)

    universe = [
        QuoteSnapshot("AAA", 100, 112, 100, 0.12, "2026-02-20T00:00:00+00:00"),
        QuoteSnapshot("BBB", 90, 93, 100, -0.07, "2026-02-20T00:00:00+00:00"),
        QuoteSnapshot("CCC", 80, 106, 100, 0.06, "2026-02-20T00:00:00+00:00"),
        QuoteSnapshot("DDD", 70, 96, 100, -0.04, "2026-02-20T00:00:00+00:00"),
        QuoteSnapshot("EEE", 60, 102, 100, 0.02, "2026-02-20T00:00:00+00:00"),
    ]
    monkeypatch.setattr(
        "coatue_claw.market_daily._build_final_universe",
        lambda store, refresh_holdings=True: (
            universe,
            {x.ticker: "top40" for x in universe},
            set(),
            set(),
            None,
        ),
    )
    rows = [
        EarningsRecapRow("AAA", "AAA Co", "2026-02-20", "after_close", 100.0, 112.0, 100.0, 0.12),
        EarningsRecapRow("BBB", "BBB Co", "2026-02-20", "after_close", 90.0, 93.0, 100.0, -0.07),
        EarningsRecapRow("CCC", "CCC Co", "2026-02-20", "after_close", 80.0, 106.0, 100.0, 0.06),
        EarningsRecapRow("DDD", "DDD Co", "2026-02-20", "after_close", 70.0, 96.0, 100.0, -0.04),
        EarningsRecapRow("EEE", "EEE Co", "2026-02-20", "after_close", 60.0, 102.0, 100.0, 0.02),
    ]
    monkeypatch.setattr("coatue_claw.market_daily._collect_reported_today_rows", lambda universe, now_utc=None: rows)
    monkeypatch.setattr(
        "coatue_claw.market_daily._hydrate_recap_row",
        lambda row, since_utc, client: row
        if row.bullets
        else replace(
            row,
            bullets=("Shares traded since close.", "Guidance tone was constructive."),
            evidence=("Coverage pointed to demand resilience.",),
            source_links=("https://example.com/source",),
        ),
    )
    result = run_earnings_recap(manual=True, force=False, dry_run=True)
    assert result["ok"] is True
    assert result["status"] == "dry_run"
    assert len(result["movers"]) == 4
    assert [x["ticker"] for x in result["movers"]] == ["AAA", "BBB", "CCC", "DDD"]
    artifact = Path(result["artifact_path"])
    assert artifact.exists()
    content = artifact.read_text(encoding="utf-8")
    assert "## Recap Rows" in content
    assert "Google web + Yahoo news evidence" in content
    assert "X/Yahoo/web evidence" not in content


def test_run_earnings_recap_manual_daytime_does_not_block_scheduled_slot(tmp_path: Path, monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COATUE_CLAW_MD_DB_PATH", str(tmp_path / "db/market_daily.sqlite"))
    monkeypatch.setenv("COATUE_CLAW_MD_ARTIFACT_DIR", str(tmp_path / "artifacts/market-daily"))
    monkeypatch.setenv("COATUE_CLAW_MD_TZ", "UTC")
    monkeypatch.setenv("COATUE_CLAW_MD_EARNINGS_RECAP_TIME", "19:00")
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)

    class Frozen(datetime):
        _calls = [
            datetime(2026, 2, 25, 10, 0, 0, tzinfo=UTC),  # manual daytime
            datetime(2026, 2, 25, 19, 0, 0, tzinfo=UTC),  # scheduled window
        ]
        _last = _calls[-1]

        @classmethod
        def now(cls, tz=None):
            if cls._calls:
                base = cls._calls.pop(0)
                cls._last = base
            else:
                base = cls._last
            return base if tz is None else base.astimezone(tz)

    monkeypatch.setattr("coatue_claw.market_daily.datetime", Frozen)
    monkeypatch.setattr(
        "coatue_claw.market_daily._build_final_universe",
        lambda store, refresh_holdings=True: (
            [QuoteSnapshot("CRM", 100.0, 103.0, 100.0, 0.03, "2026-02-25T19:00:00+00:00")],
            {"CRM": "top40"},
            set(),
            set(),
            None,
        ),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_reported_today_rows",
        lambda universe, now_utc=None: [
            EarningsRecapRow("CRM", "Salesforce", "2026-02-25", "after_close", 100.0, 103.0, 100.0, 0.03)
        ],
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._hydrate_recap_row",
        lambda row, since_utc, client: replace(
            row,
            bullets=("Key catalyst: Demand held up [S1].", "Since regular close, shares traded +3.0% [S1]."),
            evidence=("yahoo_news: CRM earnings beat",),
            source_links=("https://example.com/crm-earnings",),
        ),
    )

    manual_run = md.run_earnings_recap(manual=True, force=False, dry_run=True)
    assert manual_run["ok"] is True
    assert manual_run["slot"] == "earnings_recap_manual"
    assert manual_run["status"] == "dry_run"

    scheduled_run = md.run_earnings_recap(manual=False, force=False, dry_run=True)
    assert scheduled_run["ok"] is True
    assert scheduled_run["slot"] == "earnings_recap"
    assert scheduled_run["status"] == "dry_run"
    assert scheduled_run.get("reason") is None

    store = md.MarketDailyStore()
    slots = {(row["run_date_local"], row["slot_name"]) for row in store.latest_runs(limit=10)}
    assert ("2026-02-25", "earnings_recap_manual") in slots
    assert ("2026-02-25", "earnings_recap") in slots


def test_catalyst_mode_defaults_to_simple(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.delenv("COATUE_CLAW_MD_CATALYST_MODE", raising=False)
    assert md._catalyst_mode() == "simple_synthesis"


def test_simple_mode_outputs_full_sentence_not_after_wrapper(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.setenv("COATUE_CLAW_MD_REASON_OUTPUT_MODE", "free_sentence")
    mover = QuoteSnapshot("AMD", 100.0, 108.8, 100.0, 0.088, "2026-02-25T22:00:00+00:00")
    candidates = [
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="AMD shares rise after Meta signs AI accelerator supply deal",
            url="https://finance.yahoo.com/news/amd-meta-deal",
            published_at_utc=datetime(2026, 2, 25, 20, 0, 0, tzinfo=UTC),
            score=0.8,
            driver_keywords=("deal_contract",),
            canonical_url="https://finance.yahoo.com/news/amd-meta-deal",
            domain="finance.yahoo.com",
        ),
        md._EvidenceCandidate(
            source_type="web",
            text="Meta inks multi-year AI chip agreement with AMD",
            url="https://www.reuters.com/world/us/meta-amd-ai-chips-2026-02-25/",
            published_at_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
            score=0.77,
            driver_keywords=("deal_contract",),
            canonical_url="https://www.reuters.com/world/us/meta-amd-ai-chips-2026-02-25/",
            domain="reuters.com",
        ),
    ]

    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: (candidates, candidates, [], "google_serp"),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_catalyst_sentence_simple",
        lambda client, ticker, pct_move, anchor, supports: ("AMD rose as Meta agreed to buy up to $60 billion of AMD AI chips.", None),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: object())
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 13, 30, 0, tzinfo=UTC),
    )
    assert line == "AMD rose as Meta agreed to buy up to $60 billion of AMD AI chips."
    assert not line.startswith("Shares rose after ")
    assert evidence.cause_mode == "simple_synthesis"
    assert evidence.synth_generation_mode == "simple_synthesis"
    assert evidence.generation_format == "free_sentence"
    assert evidence.synth_candidates_used


def test_simple_synthesis_soft_domain_gate_prefers_quality(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_SYNTH_DOMAIN_GATE", "soft")
    monkeypatch.setenv("COATUE_CLAW_MD_SYNTH_MAX_RESULTS", "3")
    rows = [
        md._EvidenceCandidate("web", "a", "https://example-blog.com/a", None, 0.9, domain="example-blog.com"),
        md._EvidenceCandidate("web", "b", "https://www.reuters.com/b", None, 0.8, domain="reuters.com"),
        md._EvidenceCandidate("web", "c", "https://finance.yahoo.com/c", None, 0.7, domain="finance.yahoo.com"),
    ]
    out = md._apply_synth_domain_gate(candidates=rows, max_results=3)
    assert [x.domain for x in out][:2] == ["reuters.com", "finance.yahoo.com"]


def test_post_as_is_policy_keeps_non_empty_llm_sentence(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.setenv("COATUE_CLAW_MD_POST_AS_IS", "1")
    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    candidates = [
        md._EvidenceCandidate(
            source_type="web",
            text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
            context_text=(
                "Shares of Intel jumped as the semiconductor sector got a boost after AMD secured a large AI chip deal with Meta."
            ),
            url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
            published_at_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
            score=0.86,
            canonical_url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
            domain="finance.yahoo.com",
        ),
    ]
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: (candidates, candidates, [], "google_serp"),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_catalyst_sentence_simple",
        lambda client, ticker, pct_move, anchor, supports: (
            "According to Reuters, Intel and peers rose as semiconductor sentiment improved.",
            None,
        ),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: object())
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 13, 30, 0, tzinfo=UTC),
    )
    assert line == "Intel and peers rose as semiconductor sentiment improved."
    assert "Reuters" not in line
    assert evidence.cause_render_mode == "simple_llm"
    assert evidence.generation_policy == "post_as_is"
    assert evidence.attribution_stripped is True


def test_simple_synthesis_no_candidates_uses_fallback(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([], [], [], None),
    )
    mover = QuoteSnapshot("BKNG", 100.0, 105.1, 100.0, 0.051, "2026-02-25T22:00:00+00:00")
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 13, 30, 0, tzinfo=UTC),
    )
    assert line == md.FALLBACK_CAUSE_LINE
    assert "no_candidates" in evidence.rejected_reasons


def test_recap_end_to_end_uses_anchor_first_for_all_bullets(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    row = EarningsRecapRow("AMD", "AMD", "2026-02-25", "after_close", 100.0, 108.8, 100.0, 0.088)
    anchor = md._EvidenceCandidate(
        source_type="yahoo_news",
        text="AMD shares rise after Meta signs AI accelerator supply deal",
        context_text="AMD climbed after Meta agreed to a multi-year AI chip procurement arrangement.",
        url="https://finance.yahoo.com/news/amd-meta-deal",
        published_at_utc=datetime(2026, 2, 25, 20, 0, 0, tzinfo=UTC),
        score=0.8,
        canonical_url="https://finance.yahoo.com/news/amd-meta-deal",
        domain="finance.yahoo.com",
    )
    support = md._EvidenceCandidate(
        source_type="web",
        text="Semiconductor peers rose as AMD deal reinforced AI demand outlook",
        context_text="Semiconductor peers moved higher as the AMD-Meta agreement reinforced AI infrastructure demand expectations.",
        url="https://www.reuters.com/world/us/amd-meta-ai-2026-02-25/",
        published_at_utc=datetime(2026, 2, 25, 20, 30, 0, tzinfo=UTC),
        score=0.75,
        canonical_url="https://www.reuters.com/world/us/amd-meta-ai-2026-02-25/",
        domain="reuters.com",
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([anchor, support], [anchor, support], [], "google_serp"),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_earnings_recap_blocks",
        lambda client, row, anchor, supports: (
            (
                "Key catalyst: AMD rose after Meta committed to a multi-year AI chip program [S1].",
                "Since regular close, shares traded +8.8% as the market priced stronger AI demand [S1].",
                "Investor sentiment improved with read-through to AI infrastructure beneficiaries [S2].",
            ),
            "llm",
            (),
        ),
    )
    hydrated = md._hydrate_recap_row(
        row=row,
        since_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
        client=None,
    )
    assert len(hydrated.bullets) == 3
    assert all("[S" in b for b in hydrated.bullets)
    assert hydrated.source_links == (
        "https://finance.yahoo.com/news/amd-meta-deal",
        "https://www.reuters.com/world/us/amd-meta-ai-2026-02-25/",
    )
    assert hydrated.recap_anchor_url == "https://finance.yahoo.com/news/amd-meta-deal"


def test_web_candidate_without_publish_time_is_rejected_when_strict_enabled(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.setenv("COATUE_CLAW_MD_GOOGLE_SERP_API_KEY", "test-key")
    monkeypatch.setenv("COATUE_CLAW_MD_REQUIRE_IN_WINDOW_DATES", "1")
    monkeypatch.setenv("COATUE_CLAW_MD_ALLOW_UNDATED_FALLBACK", "0")
    monkeypatch.setenv("COATUE_CLAW_MD_PUBLISH_TIME_ENRICH_ENABLED", "0")
    monkeypatch.setattr("coatue_claw.market_daily._fetch_yahoo_news_candidates", lambda ticker, aliases, since_utc: [])
    monkeypatch.setattr(
        "coatue_claw.market_daily._fetch_web_evidence",
        lambda ticker, aliases, since_utc, pct_move=None: (
            [
                md._EvidenceCandidate(
                    source_type="web",
                    text="Why Intel (INTC) Stock Is Soaring Today - Yahoo Finance",
                    url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
                    published_at_utc=None,
                    score=0.9,
                )
            ],
            "google_serp",
            [],
        ),
    )
    _, selected, notes, _ = md._collect_synthesis_candidates(
        ticker="INTC",
        aliases=["Intel"],
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
        pct_move=0.05,
    )
    assert not selected
    assert any("publish_time_reject:undated_unverified" in n for n in notes)


def test_web_candidate_accepts_html_enriched_publish_time_in_window(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_REQUIRE_IN_WINDOW_DATES", "1")
    monkeypatch.setenv("COATUE_CLAW_MD_ALLOW_UNDATED_FALLBACK", "0")
    monkeypatch.setattr(
        "coatue_claw.market_daily._parse_published_at_from_article_html",
        lambda url: (datetime(2026, 2, 25, 20, 15, 0, tzinfo=UTC), "article_meta"),
    )
    rows, notes = md._enforce_time_integrity(
        candidates=[
            md._EvidenceCandidate(
                source_type="web",
                text="Why Intel (INTC) Stock Is Soaring Today - Yahoo Finance",
                url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
                published_at_utc=None,
                score=0.8,
            )
        ],
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
        pct_move=0.05,
        now_utc=datetime(2026, 2, 25, 22, 0, 0, tzinfo=UTC),
    )
    assert rows
    assert rows[0].published_at_utc == datetime(2026, 2, 25, 20, 15, 0, tzinfo=UTC)
    assert rows[0].published_source == "article_meta"
    assert not notes


def test_out_of_window_candidate_rejected_even_if_high_score(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_REQUIRE_IN_WINDOW_DATES", "1")
    rows, notes = md._enforce_time_integrity(
        candidates=[
            md._EvidenceCandidate(
                source_type="yahoo_news",
                text="Morgan Stanley Lifts Intel (INTC) to $41, Flags Near-Term Foundry Constraints",
                url="https://finance.yahoo.com/news/morgan-stanley-lifts-intel-intc-022928697.html",
                published_at_utc=datetime(2026, 1, 26, 20, 0, 0, tzinfo=UTC),
                score=1.0,
            )
        ],
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
        pct_move=0.05,
    )
    assert not rows
    assert any("publish_time_reject:out_of_window" in n for n in notes)


def test_historical_callback_text_rejected(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_REJECT_HISTORICAL_CALLBACK", "1")
    rows, notes = md._enforce_time_integrity(
        candidates=[
            md._EvidenceCandidate(
                source_type="yahoo_news",
                text="Morgan Stanley Lifts Intel (INTC) to $41, Flags Near-Term Foundry Constraints",
                context_text=(
                    "Morgan Stanley Lifts Intel. On January 26, Morgan Stanley raised its price target "
                    "while maintaining Equal Weight."
                ),
                url="https://finance.yahoo.com/news/morgan-stanley-lifts-intel-intc-022928697.html",
                published_at_utc=datetime(2026, 2, 25, 2, 29, 28, tzinfo=UTC),
                score=0.95,
            )
        ],
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
        pct_move=0.05,
        now_utc=datetime(2026, 2, 25, 22, 0, 0, tzinfo=UTC),
    )
    assert not rows
    assert any("historical_callback_reject" in n for n in notes)


def test_intc_regression_prefers_in_window_why_stock_soaring_link(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.setenv("COATUE_CLAW_MD_GOOGLE_SERP_API_KEY", "test-key")
    monkeypatch.setenv("COATUE_CLAW_MD_REQUIRE_IN_WINDOW_DATES", "1")
    monkeypatch.setenv("COATUE_CLAW_MD_ALLOW_UNDATED_FALLBACK", "0")
    monkeypatch.setenv("COATUE_CLAW_MD_REJECT_HISTORICAL_CALLBACK", "1")
    monkeypatch.setenv("COATUE_CLAW_MD_PUBLISH_TIME_ENRICH_ENABLED", "0")
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 2, 25, 22, 30, 0, tzinfo=UTC)
            if tz is None:
                return base
            return base.astimezone(tz)

    monkeypatch.setattr("coatue_claw.market_daily.datetime", Frozen)

    stale = md._EvidenceCandidate(
        source_type="yahoo_news",
        text="Morgan Stanley Lifts Intel (INTC) to $41, Flags Near-Term Foundry Constraints",
        context_text=(
            "On January 26, Morgan Stanley raised its price target on Intel to $41 from $38."
        ),
        url="https://finance.yahoo.com/news/morgan-stanley-lifts-intel-intc-022928697.html",
        published_at_utc=datetime(2026, 2, 25, 2, 29, 28, tzinfo=UTC),
        score=1.0,
        published_confidence="high",
        published_source="yahoo_feed",
    )
    fresh = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        context_text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.8,
        published_confidence="high",
        published_source="serp_date",
    )
    monkeypatch.setattr("coatue_claw.market_daily._fetch_yahoo_news_candidates", lambda ticker, aliases, since_utc: [stale])
    monkeypatch.setattr(
        "coatue_claw.market_daily._fetch_web_evidence",
        lambda ticker, aliases, since_utc, pct_move=None: ([fresh], "google_serp", []),
    )
    considered, selected, notes, _ = md._collect_synthesis_candidates(
        ticker="INTC",
        aliases=["Intel"],
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
        pct_move=0.057,
    )
    assert considered
    assert selected
    assert selected[0].url == "https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html"
    assert any("historical_callback_reject" in n for n in notes)


def test_consensus_support_prefers_deal_family_for_intc_case(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    pricing = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today",
        context_text=(
            "According to a Reuters report, Intel and a key rival planned to raise server CPU prices by as much as 10% in China."
        ),
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.94,
        domain="finance.yahoo.com",
    )
    deal_anchor = md._EvidenceCandidate(
        source_type="web",
        text="Intel stock price jumps as INTC bets on SambaNova AI tie-up",
        context_text="Intel shares jumped close to 6% Tuesday after the company announced a multi-year AI partnership with SambaNova.",
        url="https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/",
        published_at_utc=datetime(2026, 2, 25, 20, 29, 0, tzinfo=UTC),
        score=1.0,
        domain="bez-kabli.pl",
    )
    deal_support = md._EvidenceCandidate(
        source_type="web",
        text="Why Intel Rallied Today",
        context_text="Intel rallied after announcing a multi-year AI partnership with SambaNova and broader semiconductor momentum.",
        url="https://www.fool.com/investing/2026/02/24/why-intel-rallied-today/",
        published_at_utc=datetime(2026, 2, 25, 19, 29, 0, tzinfo=UTC),
        score=1.0,
        domain="fool.com",
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: (
            [deal_anchor, deal_support, pricing],
            [pricing, deal_anchor, deal_support],
            [],
            "google_serp",
        ),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_catalyst_sentence_simple",
        lambda client, ticker, pct_move, anchor, supports: (
            "According to Reuters, Intel shares rose after server CPU pricing headlines.",
            None,
        ),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: object())

    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
    )

    assert "SambaNova" in line
    assert "Reuters" not in line
    assert evidence.consensus_event_family == "deal_partnership"
    assert evidence.consensus_winner_url == deal_anchor.url
    assert evidence.cause_anchor_url == deal_anchor.url
    assert evidence.attribution_stripped is True
    assert any("consensus_family_mismatch" in reason for reason in evidence.rejected_reasons)


def test_links_follow_consensus_family_alignment(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    deal_anchor = md._EvidenceCandidate(
        source_type="web",
        text="Intel stock price jumps as INTC bets on SambaNova AI tie-up",
        context_text="Intel shares jumped close to 6% Tuesday after the company announced a multi-year AI partnership with SambaNova.",
        url="https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/",
        published_at_utc=datetime(2026, 2, 25, 20, 29, 0, tzinfo=UTC),
        score=1.0,
        domain="bez-kabli.pl",
    )
    deal_support = md._EvidenceCandidate(
        source_type="web",
        text="Why Intel Rallied Today",
        context_text="Intel rallied after announcing a multi-year AI partnership with SambaNova.",
        url="https://www.fool.com/investing/2026/02/24/why-intel-rallied-today/",
        published_at_utc=datetime(2026, 2, 25, 19, 29, 0, tzinfo=UTC),
        score=1.0,
        domain="fool.com",
    )
    pricing_outlier = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today",
        context_text="According to Reuters, Intel and peers plan to raise server CPU prices in China.",
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.94,
        domain="finance.yahoo.com",
    )

    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: (
            [deal_anchor, deal_support, pricing_outlier],
            [pricing_outlier, deal_anchor, deal_support],
            [],
            "google_serp",
        ),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_catalyst_sentence_simple",
        lambda client, ticker, pct_move, anchor, supports: (
            "Intel shares jumped close to 6% Tuesday after the company announced a multi-year AI partnership with SambaNova.",
            None,
        ),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: object())
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
    )
    text = md._build_message(
        slot_name="close",
        now_local=datetime(2026, 2, 25, 22, 0, 0, tzinfo=UTC),
        universe_count=40,
        movers=[mover],
        catalyst_rows=[evidence],
        catalyst_lines=[line],
    )

    assert "<https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/|[Web]>" in text
    assert "<https://www.fool.com/investing/2026/02/24/why-intel-rallied-today/|[Web]>" in text
    assert "<https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html|[News]>" not in text



def test_debug_payload_includes_consensus_fields(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    evidence = CatalystEvidence(
        ticker="INTC",
        x_text=None,
        x_url=None,
        x_engagement=0,
        news_title="Intel moves on partnership catalyst",
        news_url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        web_title="Intel stock price jumps as INTC bets on SambaNova AI tie-up",
        web_url="https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/",
        confidence=0.91,
        chosen_source="web",
        cause_mode="simple_synthesis",
        cause_render_mode="simple_consensus_backup",
        cause_anchor_url="https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/",
        cause_support_urls=("https://www.fool.com/investing/2026/02/24/why-intel-rallied-today/",),
        consensus_event_family="deal_partnership",
        consensus_winner_url="https://www.bez-kabli.pl/intel-stock-price-jumps-as-intc-bets-on-sambanova-ai-tie-up-what-investors-watch-next/",
        attribution_stripped=True,
        generation_format="free_sentence",
        generation_policy="post_as_is",
    )

    monkeypatch.setattr("coatue_claw.market_daily._fetch_quote_snapshots", lambda tickers: [mover])
    monkeypatch.setattr("coatue_claw.market_daily._build_catalyst_for_mover", lambda mover, slot_name, since_utc: (evidence, "Intel shares jumped on a SambaNova AI partnership catalyst."))

    payload = md.debug_catalyst(ticker="INTC", slot_name="close")
    assert payload["consensus_event_family"] == "deal_partnership"
    assert payload["consensus_winner_url"] == evidence.consensus_winner_url
    assert payload["attribution_stripped"] is True


def test_links_only_emit_time_valid_urls(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    valid = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        context_text="Shares of Intel jumped after AMD's large AI chip deal with Meta lifted semiconductor sentiment.",
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.8,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([valid], [valid], [], "google_serp"),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_catalyst_sentence_simple",
        lambda client, ticker, pct_move, anchor, supports: ("Intel gained as AMD's Meta deal boosted semiconductor sentiment.", None),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: object())
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
    )
    text = md._build_message(
        slot_name="close",
        now_local=datetime(2026, 2, 25, 22, 0, 0, tzinfo=UTC),
        universe_count=40,
        movers=[mover],
        catalyst_rows=[evidence],
        catalyst_lines=[line],
    )
    assert "why-intel-intc-stock-soaring-210238819" in text


def test_recap_uses_time_valid_evidence_only(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    row = EarningsRecapRow("INTC", "Intel", "2026-02-25", "after_close", 100.0, 105.7, 100.0, 0.057)
    fresh = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        context_text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.8,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([fresh], [fresh], [], "google_serp"),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)
    hydrated = md._hydrate_recap_row(
        row=row,
        since_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
        client=None,
    )
    assert hydrated.source_links == ("https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",)
    assert hydrated.bullets[0].startswith("Key catalyst:")
    assert hydrated.recap_generation_mode == "deterministic_backup"


def test_recap_citations_align_with_used_sources(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    row = EarningsRecapRow("INTC", "Intel", "2026-02-25", "after_close", 100.0, 105.7, 100.0, 0.057)
    anchor = md._EvidenceCandidate(
        source_type="yahoo_news",
        text="Why Is Intel (INTC) Stock Soaring Today",
        context_text="Intel rose with semis after AMD's deal with Meta.",
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.82,
    )
    support = md._EvidenceCandidate(
        source_type="web",
        text="Semiconductor stocks rise with AMD/Meta deal enthusiasm",
        context_text="Semiconductor peers moved higher as investors reacted to AMD's AI chip arrangement with Meta.",
        url="https://www.reuters.com/world/us/amd-meta-ai-chip-deal-2026-02-25/",
        published_at_utc=datetime(2026, 2, 25, 21, 10, 0, tzinfo=UTC),
        score=0.78,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([anchor, support], [anchor, support], [], "google_serp"),
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._synthesize_earnings_recap_blocks",
        lambda client, row, anchor, supports: (
            (
                "Key catalyst: Intel rose with semiconductor peers after AMD's Meta chip agreement [S1].",
                "Since regular close, shares traded +5.7% as momentum broadened across semis [S1].",
                "Investor sentiment improved as market participants leaned into AI supply-chain beneficiaries [S2].",
            ),
            "llm",
            (),
        ),
    )
    hydrated = md._hydrate_recap_row(
        row=row,
        since_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
        client=object(),
    )
    message = md._build_earnings_recap_message(rows=[hydrated], now_local=datetime(2026, 2, 25, 22, 0, 0, tzinfo=UTC))
    assert all(any(tag in b for tag in ("[S1]", "[S2]")) for b in hydrated.bullets)
    assert "<https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html|[S1]>" in message
    assert "<https://www.reuters.com/world/us/amd-meta-ai-chip-deal-2026-02-25/|[S2]>" in message


def test_recap_avoids_wrapper_text_in_bullets(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    wrapper = md._normalize_recap_sentence(
        "Intel stock price, news, quote & history [S1].",
        source_count=2,
        preferred_idx=1,
    )
    clean = md._normalize_recap_sentence(
        "Since regular close, shares traded +5.7% as semiconductor sentiment improved [S1].",
        source_count=2,
        preferred_idx=1,
    )
    assert wrapper is None
    assert clean is not None and "quote & history" not in clean.lower()


def test_recap_intc_like_sector_sympathy_reason_is_captured(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    row = EarningsRecapRow("INTC", "Intel", "2026-02-25", "after_close", 100.0, 105.7, 100.0, 0.057)
    anchor = md._EvidenceCandidate(
        source_type="yahoo_news",
        text="Why Is Intel (INTC) Stock Soaring Today",
        context_text=(
            "Shares of Intel jumped after the semiconductor sector got a boost as AMD secured a major AI chip deal with Meta."
        ),
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 2, 38, tzinfo=UTC),
        score=0.88,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([anchor], [anchor], [], "google_serp"),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)
    hydrated = md._hydrate_recap_row(
        row=row,
        since_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
        client=None,
    )
    assert any("amd" in b.lower() and "meta" in b.lower() for b in hydrated.bullets)


def test_simple_synthesis_google_missing_uses_yahoo_only_not_ddg(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.delenv("COATUE_CLAW_MD_GOOGLE_SERP_API_KEY", raising=False)
    monkeypatch.delenv("SERPAPI_API_KEY", raising=False)
    now_utc = datetime.now(UTC)
    since_utc = now_utc - timedelta(hours=6)
    yahoo = [
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="Intel shares rise after strong foundry update",
            url="https://finance.yahoo.com/news/intel-shares-rise-foundry-update-210000000.html",
            published_at_utc=now_utc - timedelta(minutes=30),
            score=0.8,
        )
    ]
    monkeypatch.setattr("coatue_claw.market_daily._fetch_yahoo_news_candidates", lambda ticker, aliases, since_utc: yahoo)
    called = {"web": 0}

    def _fake_web(ticker, aliases, since_utc, pct_move=None):
        called["web"] += 1
        return ([], "ddg_html", [])

    monkeypatch.setattr("coatue_claw.market_daily._fetch_web_evidence", _fake_web)
    _, selected, notes, backend = md._collect_synthesis_candidates(
        ticker="INTC",
        aliases=["Intel"],
        since_utc=since_utc,
        pct_move=0.05,
    )
    assert called["web"] == 0
    assert backend is None
    assert selected
    assert selected[0].source_type == "yahoo_news"
    assert "web:google_serp_required_missing" in notes


def test_model_unavailable_uses_anchor_backup_sentence(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    anchor = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        context_text=(
            "Shares of computer processor maker Intel jumped 5.4% in the afternoon session after the semiconductor sector "
            "received a major boost as AMD secured a large AI chip deal with Meta over five years."
        ),
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
        score=0.95,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([anchor], [anchor], [], "google_serp"),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
    )
    assert line != md.FALLBACK_CAUSE_LINE
    assert "after the semiconductor sector received a major boost" in line
    assert evidence.cause_render_mode == "simple_anchor_backup"


def test_llm_unavailable_strong_causal_candidate_still_specific(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    monkeypatch.setenv("COATUE_CLAW_MD_CATALYST_MODE", "simple_synthesis")
    monkeypatch.setenv("COATUE_CLAW_MD_SYNTH_FORCE_BEST_GUESS", "1")
    mover = QuoteSnapshot("INTC", 100.0, 105.7, 100.0, 0.057, "2026-02-25T22:00:00+00:00")
    strong = md._EvidenceCandidate(
        source_type="web",
        text="Why Is Intel (INTC) Stock Soaring Today - Yahoo Finance",
        url="https://finance.yahoo.com/news/why-intel-intc-stock-soaring-210238819.html",
        published_at_utc=datetime(2026, 2, 25, 21, 0, 0, tzinfo=UTC),
        score=0.88,
    )
    monkeypatch.setattr(
        "coatue_claw.market_daily._collect_synthesis_candidates",
        lambda ticker, aliases, since_utc, pct_move=None: ([strong], [strong], [], "google_serp"),
    )
    monkeypatch.setattr("coatue_claw.market_daily._openai_client", lambda: None)
    evidence, line = md._build_catalyst_for_mover(
        mover=mover,
        slot_name="close",
        since_utc=datetime(2026, 2, 25, 14, 30, 0, tzinfo=UTC),
    )
    assert line != md.FALLBACK_CAUSE_LINE
    assert evidence.cause_render_mode == "simple_anchor_backup"


def test_technical_analysis_soft_penalty_lowers_rank() -> None:
    from coatue_claw import market_daily as md

    now_utc = datetime.now(UTC)
    since = now_utc - timedelta(hours=6)
    published = now_utc - timedelta(minutes=30)
    ta = md._compute_evidence_score(
        source_type="web",
        text="Price Forecast: Breakout Holds as Bulls Defend Support",
        url="https://example.com/ta",
        published_at_utc=published,
        since_utc=since,
        ticker="INTC",
        aliases=["Intel"],
    )
    explainer = md._compute_evidence_score(
        source_type="web",
        text="Shares rose after management raised guidance today",
        url="https://example.com/explainer",
        published_at_utc=published,
        since_utc=since,
        ticker="INTC",
        aliases=["Intel"],
    )
    assert explainer > ta


def test_roundup_penalty_prevents_generic_analyst_calls_link_win() -> None:
    from coatue_claw import market_daily as md

    now_utc = datetime.now(UTC)
    since = now_utc - timedelta(hours=6)
    published = now_utc - timedelta(minutes=30)
    roundup = md._compute_evidence_score(
        source_type="web",
        text="Qualcomm, Nvidia upgraded: Wall Street's top analyst calls",
        url="https://example.com/roundup",
        published_at_utc=published,
        since_utc=since,
        ticker="BKNG",
        aliases=["Booking Holdings", "Booking.com"],
    )
    explainer = md._compute_evidence_score(
        source_type="web",
        text="Booking shares rose after stronger online travel demand guidance",
        url="https://example.com/explainer",
        published_at_utc=published,
        since_utc=since,
        ticker="BKNG",
        aliases=["Booking Holdings", "Booking.com"],
    )
    assert explainer > roundup


def test_sanitize_phrase_removes_aggregator_prefix() -> None:
    from coatue_claw import market_daily as md

    cleaned = md._sanitize_synth_phrase("FinancialContent - Why Is Intel (INTC) Stock Soaring Today")
    assert cleaned == "Why Is Intel (INTC) Stock Soaring Today"

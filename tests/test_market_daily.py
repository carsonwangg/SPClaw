from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

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
            published_at_utc=None,
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


def test_synthesize_earnings_bullets_fallback_shape() -> None:
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
        evidence=("Analysts highlighted stronger datacenter demand.",),
        source_links=("https://example.com/nvda",),
    )
    bullets = _synthesize_earnings_bullets(client=None, row=row)
    assert 2 <= len(bullets) <= 4
    assert any("Shares traded" in x for x in bullets)


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
    assert "Yahoo/web evidence" in content
    assert "X/Yahoo/web evidence" not in content

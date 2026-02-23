from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from coatue_claw.market_daily import (
    CatalystEvidence,
    MarketDailyStore,
    QuoteSnapshot,
    debug_catalyst,
    _fetch_web_evidence_ddg,
    _fetch_yahoo_news,
    _session_anchor_start_utc,
    _build_message,
    _ensure_reason_like_line,
    _is_relevant_ticker_post,
    _merge_universe,
    _parse_times,
    _select_top_movers,
    refresh_coatue_holdings,
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
    assert "<https://x.com/i/web/status/1|[X]>" in text
    assert "<https://example.com/news|[News]>" in text


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
    assert "No clear single catalyst" in line


def test_fetch_yahoo_news_parses_nested_schema(monkeypatch) -> None:
    class FakeTicker:
        news = [
            {
                "content": {
                    "pubDate": "2026-02-20T10:12:00Z",
                    "title": "Cybersecurity stocks fall after Anthropic launches Claude security tool",
                    "clickThroughUrl": {
                        "url": "https://finance.yahoo.com/news/cybersecurity-stocks-fall-anthropic-101200000.html"
                    },
                }
            }
        ]

    monkeypatch.setattr("coatue_claw.market_daily.yf.Ticker", lambda ticker: FakeTicker())
    title, url = _fetch_yahoo_news(
        ticker="NET",
        since_utc=datetime(2026, 2, 19, 0, 0, 0, tzinfo=UTC),
    )
    assert "Anthropic" in (title or "")
    assert (url or "").startswith("https://finance.yahoo.com/news/")


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
    assert payload["links"]["web"] == "https://stocktwits.com/news/example"


def test_negative_mover_prefers_negative_driver_language(monkeypatch) -> None:
    from coatue_claw import market_daily as md

    candidates = [
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="Cloudflare and Mastercard announce strategic cybersecurity partnership",
            url="https://finance.yahoo.com/news/partnership",
            published_at_utc=datetime(2026, 2, 20, 10, 0, 0, tzinfo=UTC),
            score=0.9,
            driver_keywords=("cybersecurity_competition",),
        ),
        md._EvidenceCandidate(
            source_type="yahoo_news",
            text="Cybersecurity stocks drop as Anthropic launches Claude Code Security tool",
            url="https://finance.yahoo.com/news/anthropic",
            published_at_utc=datetime(2026, 2, 20, 10, 30, 0, tzinfo=UTC),
            score=0.9,
            driver_keywords=("anthropic_claude", "cybersecurity_competition"),
        ),
    ]

    monkeypatch.setattr("coatue_claw.market_daily._session_window_since_utc", lambda slot_name: datetime(2026, 2, 20, 0, 0, 0, tzinfo=UTC))
    monkeypatch.setattr("coatue_claw.market_daily._company_aliases", lambda ticker: ["Cloudflare"])
    monkeypatch.setattr("coatue_claw.market_daily._collect_evidence_for_ticker", lambda ticker, aliases, since_utc: (candidates, []))

    mover = QuoteSnapshot("NET", 100.0, 92.0, 100.0, -0.08, "2026-02-20T12:00:00+00:00")
    rows, lines = md._build_catalyst_rows(movers=[mover], slot_name="open")
    assert "anthropic" in (rows[0].news_title or "").lower()
    assert ("after" in lines[0].lower()) or (" as " in lines[0].lower())

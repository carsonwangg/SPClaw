from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from spclaw.diligence_report import (
    LocalResearchLookup,
    LocalResearchReport,
    build_neutral_investment_memo,
)


class _FakeTicker:
    def __init__(self):
        self.info = {
            "symbol": "SNOW",
            "longName": "Snowflake Inc.",
            "longBusinessSummary": "Snowflake provides a cloud data platform for enterprise analytics and applications.",
            "sector": "Technology",
            "industry": "Software - Infrastructure",
            "country": "United States",
            "website": "https://www.snowflake.com",
            "fullTimeEmployees": 7500,
            "marketCap": 55_000_000_000,
            "enterpriseValue": 53_000_000_000,
            "sharesOutstanding": 340_000_000,
            "trailingPE": 120.0,
            "forwardPE": 80.0,
            "priceToSalesTrailing12Months": 15.0,
            "enterpriseToRevenue": 14.0,
            "enterpriseToEbitda": 75.0,
            "grossMargins": 0.69,
            "operatingMargins": -0.05,
            "ebitdaMargins": 0.03,
            "totalCash": 4_500_000_000,
            "totalDebt": 2_000_000_000,
            "currentRatio": 1.8,
            "debtToEquity": 45.0,
        }
        self.income_stmt = pd.DataFrame(
            {
                pd.Timestamp("2025-12-31", tz="UTC"): [3_000_000_000],
                pd.Timestamp("2024-12-31", tz="UTC"): [2_400_000_000],
                pd.Timestamp("2023-12-31", tz="UTC"): [1_900_000_000],
            },
            index=["Total Revenue"],
        )
        self.quarterly_income_stmt = pd.DataFrame(
            {
                pd.Timestamp("2025-09-30", tz="UTC"): [840_000_000],
                pd.Timestamp("2025-06-30", tz="UTC"): [780_000_000],
                pd.Timestamp("2025-03-31", tz="UTC"): [720_000_000],
            },
            index=["Total Revenue"],
        )
        self.news = [
            {
                "title": "Snowflake announces product expansion",
                "publisher": "Reuters",
                "link": "https://example.com/news-1",
                "providerPublishTime": 1760000000,
            },
            {
                "content": {
                    "title": "Enterprise software demand trends",
                    "provider": {"displayName": "Bloomberg"},
                    "clickThroughUrl": {"url": "https://example.com/news-2"},
                    "pubDate": "2026-01-10T00:00:00Z",
                }
            },
        ]


def test_build_neutral_investment_memo_has_required_sections():
    now = datetime(2026, 2, 18, 4, 0, tzinfo=UTC)
    memo = build_neutral_investment_memo("SNOW", ticker_factory=lambda _ticker: _FakeTicker(), now_utc=now)

    assert "# Neutral Investment Memo: Snowflake Inc. (SNOW)" in memo
    assert "## 1. Key Takeaways" in memo
    assert "## 2. Business Overview" in memo
    assert "## 3. Financials & Funding" in memo
    assert "### 3.1 Funding & Cap Table History" in memo
    assert "### 3.2 Revenue & Growth" in memo
    assert "### 3.3 Margins & Economics" in memo
    assert "### 3.4 Valuation Context" in memo
    assert "### 3.5 Balance Sheet & Cash" in memo
    assert "## 4. Market Overview" in memo
    assert "## 5. Company Strengths" in memo
    assert "## 6. Key Risks" in memo
    assert "## 7. Open Diligence Questions" in memo
    assert "## 8. Appendix" in memo


def test_build_neutral_investment_memo_contains_evidence_and_sources():
    now = datetime(2026, 2, 18, 4, 0, tzinfo=UTC)
    memo = build_neutral_investment_memo("SNOW", ticker_factory=lambda _ticker: _FakeTicker(), now_utc=now)

    assert "[Source: Yahoo Finance via yfinance" in memo
    assert "| Period | Revenue |" in memo
    assert "2025" in memo
    assert "Snowflake announces product expansion" in memo


def test_build_neutral_investment_memo_checks_local_reports_first():
    now = datetime(2026, 2, 18, 4, 0, tzinfo=UTC)
    lookup = LocalResearchLookup(
        query="SNOW",
        checked_at_utc="2026-02-18T03:59:00+00:00",
        reports=[
            LocalResearchReport(
                source="file_ingest",
                title="SNOW - prior diligence report",
                path="/opt/spclaw-data/files/incoming/Companies/SNOW-prior.md",
                category="Companies",
                recorded_at_utc="2026-02-17T20:00:00+00:00",
            )
        ],
    )

    memo = build_neutral_investment_memo(
        "SNOW",
        ticker_factory=lambda _ticker: _FakeTicker(),
        now_utc=now,
        local_report_lookup=lambda _query: lookup,
    )

    assert "Database-first check found `1` internal report(s) for `SNOW` before external data pull." in memo
    assert "Local report reference: `SNOW - prior diligence report`" in memo
    assert "Local research database (file ingest + prior packets)" in memo

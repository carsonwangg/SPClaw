from __future__ import annotations

from datetime import UTC, datetime, timedelta

from coatue_claw.valuation_chart import (
    ProviderSnapshot,
    _build_point,
    _format_readable_date,
)


def _snapshot(
    *,
    ticker: str = "TEST",
    currency: str = "USD",
    financial_currency: str = "USD",
    market_data_as_of: str | None = None,
    latest_quarter_end: str | None = None,
    total_debt: float | None = 200.0,
    ltm_revenue: float | None = 460.0,
    company_category: str | None = "Software",
) -> ProviderSnapshot:
    now_utc = datetime.now(UTC).replace(microsecond=0)
    now_iso = now_utc.isoformat()
    return ProviderSnapshot(
        ticker=ticker,
        provider="yahoo",
        fetched_at=now_iso,
        market_data_as_of=market_data_as_of or now_iso,
        currency=currency,
        financial_currency=financial_currency,
        market_cap=1000.0,
        total_debt=total_debt,
        cash_eq=100.0,
        preferred_equity=0.0,
        minority_interest=0.0,
        latest_quarter_end=latest_quarter_end or now_iso,
        revenue_q=120.0,
        revenue_q_1y=100.0,
        revenue_last_4q_sum=ltm_revenue,
        errors=[],
        raw_payload={},
        company_category=company_category,
    )


def test_build_point_success_regression_tolerance():
    snap = _snapshot()
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    # EV = 1000 + 200 - 100 = 1100; LTM = 460; multiple ~= 2.391304
    assert point.included is True
    assert point.exclusion_reason is None
    assert abs((point.ev_ltm_revenue or 0.0) - 2.3913043478) < 1e-6


def test_build_point_excludes_missing_ltm_revenue():
    snap = _snapshot(ltm_revenue=None)
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "missing_ltm_revenue"


def test_build_point_excludes_nonpositive_ltm_revenue():
    snap = _snapshot(ltm_revenue=0.0)
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "nonpositive_ltm_revenue"


def test_build_point_excludes_currency_mismatch():
    snap = _snapshot(currency="USD", financial_currency="EUR")
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "currency_mismatch"


def test_build_point_excludes_missing_debt():
    snap = _snapshot(total_debt=None)
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "missing_debt"


def test_build_point_excludes_stale_fundamentals():
    stale = (datetime.now(UTC) - timedelta(days=400)).replace(microsecond=0).isoformat()
    snap = _snapshot(latest_quarter_end=stale)
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "stale_fundamentals"


def test_build_point_propagates_category():
    snap = _snapshot(company_category="Infrastructure")
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        now_utc=datetime.now(UTC),
    )
    assert point.company_category == "Infrastructure"


def test_format_readable_date_uses_month_name():
    assert _format_readable_date("2026-02-18T13:20:00+00:00") == "Feb 18, 2026"

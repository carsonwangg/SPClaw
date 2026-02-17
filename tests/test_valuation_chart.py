from __future__ import annotations

from datetime import UTC, datetime

from coatue_claw.valuation_chart import (
    ProviderSnapshot,
    _build_point,
    _compute_ntm_revenue,
    run_valuation_chart,
)


def _snapshot(
    *,
    ticker: str = "TEST",
    currency: str = "USD",
    financial_currency: str = "USD",
    market_data_as_of: str | None = None,
    estimates_as_of: str | None = None,
    rev_estimates: dict[str, float] | None = None,
    total_debt: float | None = 200.0,
) -> ProviderSnapshot:
    now_iso = datetime.now(UTC).replace(microsecond=0).isoformat()
    return ProviderSnapshot(
        ticker=ticker,
        provider="yahoo",
        fetched_at=now_iso,
        market_data_as_of=market_data_as_of or now_iso,
        estimates_as_of=estimates_as_of or now_iso,
        currency=currency,
        financial_currency=financial_currency,
        market_cap=1000.0,
        total_debt=total_debt,
        cash_eq=100.0,
        preferred_equity=0.0,
        minority_interest=0.0,
        latest_quarter_end=now_iso,
        revenue_q=120.0,
        revenue_q_1y=100.0,
        revenue_estimates_quarterly=rev_estimates
        or {
            "0q": 100.0,
            "+1q": 110.0,
            "+2q": 120.0,
            "+3q": 130.0,
        },
        errors=[],
        raw_payload={},
    )


def test_compute_ntm_revenue_strict_success():
    ntm, method, flags = _compute_ntm_revenue(
        {
            "0q": 100.0,
            "+1q": 110.0,
            "+2q": 120.0,
            "+3q": 130.0,
        },
        ntm_mode="strict",
    )
    assert ntm == 460.0
    assert method == "strict_4q"
    assert flags == []


def test_compute_ntm_revenue_strict_missing():
    ntm, method, flags = _compute_ntm_revenue(
        {
            "0q": 100.0,
            "+1q": 110.0,
        },
        ntm_mode="strict",
    )
    assert ntm is None
    assert method == "strict_missing"
    assert "missing_ntm_estimates" in flags
    assert any(f.startswith("missing_estimate_periods:") for f in flags)


def test_build_point_excludes_missing_ntm_estimates():
    snap = _snapshot(
        rev_estimates={
            "0q": 100.0,
            "+1q": 110.0,
        }
    )
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        ntm_mode="strict",
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "missing_ntm_estimates"


def test_build_point_excludes_currency_mismatch():
    snap = _snapshot(currency="USD", financial_currency="EUR")
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        ntm_mode="strict",
        now_utc=datetime.now(UTC),
    )
    assert point.included is False
    assert point.exclusion_reason == "currency_mismatch"


def test_build_point_success_regression_tolerance():
    snap = _snapshot()
    point = _build_point(
        snap,
        request_at=datetime.now(UTC).replace(microsecond=0).isoformat(),
        ntm_mode="strict",
        now_utc=datetime.now(UTC),
    )
    # EV = 1000 + 200 - 100 = 1100; NTM = 460; multiple ~= 2.391304
    assert point.included is True
    assert point.exclusion_reason is None
    assert abs((point.ev_ntm_revenue or 0.0) - 2.3913043478) < 1e-6


def test_run_valuation_chart_rejects_non_strict_mode():
    try:
        run_valuation_chart(["SNOW"], ntm_mode="imputed")
    except ValueError as exc:
        assert "strict" in str(exc)
    else:
        raise AssertionError("Expected ValueError for non-strict ntm_mode")

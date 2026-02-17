from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf


ARTIFACTS_DIR = Path("/opt/coatue-claw-data/artifacts/charts")


class ProviderUnavailableError(RuntimeError):
    pass


@dataclass
class TickerPoint:
    ticker: str
    provider: str
    currency: str | None
    request_received_at: str
    market_data_as_of: str | None
    estimates_as_of: str | None
    latest_quarter_end: str | None
    revenue_q: float | None
    revenue_q_1y: float | None
    yoy_growth_pct: float | None
    ntm_revenue: float | None
    ntm_estimate_method: str
    market_cap: float | None
    total_debt: float | None
    preferred_equity: float | None
    minority_interest: float | None
    cash_eq: float | None
    enterprise_value: float | None
    ev_ntm_revenue: float | None
    quality_flags: list[str]
    included: bool
    exclusion_reason: str | None


@dataclass
class ChartResult:
    tickers: list[str]
    provider_requested: str
    provider_used: str
    provider_fallback_reason: str | None
    request_received_at: str
    market_data_as_of: str | None
    estimates_as_of: str | None
    chart_path: Path
    csv_path: Path
    json_path: Path
    included_count: int
    excluded_count: int
    points: list[TickerPoint]


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    if np.isnan(out) or np.isinf(out):
        return None
    return out


def _extract_series_row(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
    for key in candidates:
        if key in df.index:
            series = df.loc[key]
            if isinstance(series, pd.Series):
                series = series.dropna()
                if not series.empty:
                    return series
    return None


def _safe_iso(ts: Any) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, pd.Timestamp):
        dt = ts.to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.replace(microsecond=0).isoformat()
    if isinstance(ts, datetime):
        dt = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
        return dt.replace(microsecond=0).isoformat()
    try:
        dt = pd.Timestamp(ts).to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        return None


def _compute_ntm_from_yahoo_estimates(rev_est: pd.DataFrame) -> tuple[float | None, str, list[str]]:
    """
    Yahoo exposes 0q, +1q, 0y, +1y estimates.
    It does not expose +2q/+3q in this endpoint.
    We impute the missing two quarters from +1y residual after +1q.
    """
    flags: list[str] = []
    try:
        q0 = _as_float(rev_est.loc["0q", "avg"])
        q1 = _as_float(rev_est.loc["+1q", "avg"])
        y1 = _as_float(rev_est.loc["+1y", "avg"])
    except Exception:
        return None, "missing", ["missing_revenue_estimates"]

    if q0 is None or q1 is None or y1 is None:
        return None, "missing", ["missing_revenue_estimates"]

    residual = y1 - q1
    if residual <= 0:
        flags.append("ntm_imputation_residual_nonpositive")
        return None, "invalid", flags

    # Impute +2q and +3q as 2/3 of the residual (residual is +2q/+3q/+4q pool).
    ntm = q0 + q1 + (2.0 / 3.0) * residual
    flags.append("ntm_imputed_from_0q_1q_1y")
    flags.append("quarter_estimate_count_2")
    return ntm, "imputed", flags


def _fetch_yahoo_point(ticker: str, request_at: str) -> TickerPoint:
    yf_ticker = yf.Ticker(ticker)
    flags: list[str] = []

    price = yf_ticker.fast_info
    info = yf_ticker.info or {}
    history = yf_ticker.history(period="10d", interval="1d", auto_adjust=False)
    market_as_of = _safe_iso(history.index.max()) if not history.empty else None
    if market_as_of is None:
        flags.append("missing_market_data_as_of")

    currency = price.get("currency") or info.get("currency") or info.get("financialCurrency")

    market_cap = _as_float(price.get("marketCap")) or _as_float(info.get("marketCap"))
    total_debt = _as_float(info.get("totalDebt"))
    cash_eq = _as_float(info.get("totalCash"))
    preferred_equity = _as_float(info.get("preferredStock")) or 0.0
    minority_interest = _as_float(info.get("minorityInterest")) or 0.0

    qbs = yf_ticker.quarterly_balance_sheet
    if isinstance(qbs, pd.DataFrame) and not qbs.empty:
        qbs = qbs.sort_index(axis=1)
        if total_debt is None:
            debt_series = _extract_series_row(qbs, ["Total Debt"])
            total_debt = _as_float(debt_series.iloc[-1]) if debt_series is not None else None
        if cash_eq is None:
            cash_series = _extract_series_row(
                qbs,
                [
                    "Cash Cash Equivalents And Short Term Investments",
                    "Cash And Cash Equivalents",
                ],
            )
            cash_eq = _as_float(cash_series.iloc[-1]) if cash_series is not None else None
        if not minority_interest:
            mi_series = _extract_series_row(qbs, ["Minority Interest"])
            minority_interest = _as_float(mi_series.iloc[-1]) if mi_series is not None else 0.0
        if not preferred_equity:
            pref_series = _extract_series_row(qbs, ["Preferred Stock"])
            preferred_equity = _as_float(pref_series.iloc[-1]) if pref_series is not None else 0.0

    if market_cap is None:
        flags.append("missing_market_cap")
    if total_debt is None:
        flags.append("missing_total_debt")
    if cash_eq is None:
        flags.append("missing_cash_eq")

    ev = None
    if market_cap is not None and total_debt is not None and cash_eq is not None:
        ev = market_cap + total_debt + (preferred_equity or 0.0) + (minority_interest or 0.0) - cash_eq

    qis = yf_ticker.quarterly_income_stmt
    rev_q = None
    rev_q_1y = None
    latest_quarter_end = None
    yoy = None
    if isinstance(qis, pd.DataFrame) and not qis.empty:
        revenue_series = _extract_series_row(qis, ["Total Revenue", "Operating Revenue"])
        if revenue_series is not None:
            revenue_series = revenue_series.sort_index()
            if len(revenue_series) >= 5:
                rev_q = _as_float(revenue_series.iloc[-1])
                rev_q_1y = _as_float(revenue_series.iloc[-5])
                latest_quarter_end = _safe_iso(revenue_series.index[-1])
                if rev_q is not None and rev_q_1y and rev_q_1y > 0:
                    yoy = (rev_q / rev_q_1y) - 1.0
            else:
                flags.append("insufficient_quarterly_revenue_history")
        else:
            flags.append("missing_quarterly_revenue_row")
    else:
        flags.append("missing_quarterly_income_statement")

    estimates_as_of = _utc_now_iso()
    rev_est = yf_ticker.revenue_estimate
    ntm, ntm_method, ntm_flags = _compute_ntm_from_yahoo_estimates(rev_est)
    flags.extend(ntm_flags)

    multiple = None
    if ev is not None and ntm is not None and ntm > 0:
        multiple = ev / ntm
    elif ntm is None:
        flags.append("missing_ntm_revenue")
    elif ntm <= 0:
        flags.append("nonpositive_ntm_revenue")

    included = True
    reason = None
    hard_requirements = {
        "market_cap": market_cap,
        "total_debt": total_debt,
        "cash_eq": cash_eq,
        "yoy_growth_pct": yoy,
        "ntm_revenue": ntm,
        "ev_ntm_revenue": multiple,
    }
    missing = [k for k, v in hard_requirements.items() if v is None]
    if missing:
        included = False
        reason = "missing_required_fields:" + ",".join(missing)
    if multiple is not None and multiple <= 0:
        included = False
        reason = "nonpositive_ev_ntm_revenue"

    return TickerPoint(
        ticker=ticker.upper(),
        provider="yahoo",
        currency=currency,
        request_received_at=request_at,
        market_data_as_of=market_as_of,
        estimates_as_of=estimates_as_of,
        latest_quarter_end=latest_quarter_end,
        revenue_q=rev_q,
        revenue_q_1y=rev_q_1y,
        yoy_growth_pct=yoy,
        ntm_revenue=ntm,
        ntm_estimate_method=ntm_method,
        market_cap=market_cap,
        total_debt=total_debt,
        preferred_equity=preferred_equity,
        minority_interest=minority_interest,
        cash_eq=cash_eq,
        enterprise_value=ev,
        ev_ntm_revenue=multiple,
        quality_flags=flags,
        included=included,
        exclusion_reason=reason,
    )


def _try_google_provider(_tickers: list[str], _request_at: str) -> list[TickerPoint]:
    raise ProviderUnavailableError(
        "google_provider_unavailable_for_required_fields: "
        "Google Finance endpoint in this build cannot reliably provide EV components and NTM estimate inputs."
    )


def _render_chart(points: list[TickerPoint], out_path: Path, title_suffix: str) -> None:
    included = [p for p in points if p.included and p.yoy_growth_pct is not None and p.ev_ntm_revenue is not None]
    if not included:
        fig, ax = plt.subplots(figsize=(11, 7))
        ax.text(0.5, 0.5, "No valid points after quality filters", ha="center", va="center")
        ax.set_axis_off()
        fig.suptitle(f"EV/NTM Revenue vs YoY Revenue Growth ({title_suffix})")
        fig.savefig(out_path, dpi=160, bbox_inches="tight")
        plt.close(fig)
        return

    x = np.array([p.yoy_growth_pct * 100.0 for p in included], dtype=float)
    y = np.array([p.ev_ntm_revenue for p in included], dtype=float)

    fig, ax = plt.subplots(figsize=(11, 7))
    ax.scatter(x, y, s=64, alpha=0.9)
    for p, xv, yv in zip(included, x, y):
        ax.annotate(p.ticker, (xv, yv), textcoords="offset points", xytext=(5, 5), fontsize=9)

    if len(included) >= 2:
        coeff = np.polyfit(x, y, 1)
        line_x = np.linspace(float(np.min(x)), float(np.max(x)), 100)
        line_y = coeff[0] * line_x + coeff[1]
        ax.plot(line_x, line_y, linestyle="--", linewidth=1.8)

    ax.set_xlabel("YoY Revenue Growth (%)")
    ax.set_ylabel("EV / NTM Revenue (x)")
    ax.set_title(f"EV / NTM Revenue vs YoY Revenue Growth ({title_suffix})")
    ax.grid(alpha=0.25)

    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def run_valuation_chart(
    tickers: list[str],
    provider_preference: list[str] | None = None,
) -> ChartResult:
    provider_preference = provider_preference or ["google", "yahoo"]
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    if not tickers:
        raise ValueError("No tickers provided")

    request_at = _utc_now_iso()
    provider_used = None
    fallback_reason = None
    points: list[TickerPoint] = []

    for provider in provider_preference:
        if provider == "google":
            try:
                points = _try_google_provider(tickers, request_at)
                provider_used = "google"
                break
            except ProviderUnavailableError as exc:
                fallback_reason = str(exc)
                continue
        if provider == "yahoo":
            points = [_fetch_yahoo_point(t, request_at) for t in tickers]
            provider_used = "yahoo"
            break

    if provider_used is None:
        raise RuntimeError("No data provider succeeded")

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    chart_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}.png"
    csv_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}.csv"
    json_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}.json"

    df = pd.DataFrame([asdict(p) for p in points])
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    market_as_of = max((p.market_data_as_of for p in points if p.market_data_as_of), default=None)
    estimates_as_of = max((p.estimates_as_of for p in points if p.estimates_as_of), default=None)

    title_suffix = f"provider={provider_used}, market_as_of={market_as_of or 'n/a'}"
    _render_chart(points, chart_path, title_suffix)

    included_count = sum(1 for p in points if p.included)
    excluded_count = len(points) - included_count

    return ChartResult(
        tickers=tickers,
        provider_requested=provider_preference[0],
        provider_used=provider_used,
        provider_fallback_reason=fallback_reason,
        request_received_at=request_at,
        market_data_as_of=market_as_of,
        estimates_as_of=estimates_as_of,
        chart_path=chart_path,
        csv_path=csv_path,
        json_path=json_path,
        included_count=included_count,
        excluded_count=excluded_count,
        points=points,
    )

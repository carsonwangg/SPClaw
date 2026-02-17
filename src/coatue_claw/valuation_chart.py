from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import time
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf


ARTIFACTS_DIR = Path("/opt/coatue-claw-data/artifacts/charts")
CACHE_DIR = Path("/opt/coatue-claw-data/cache/valuation")
CACHE_SCHEMA_VERSION = "v3"
REQUIRED_NTM_PERIODS = ("0q", "+1q", "+2q", "+3q")

# Freshness policy.
MARKET_MAX_AGE_HOURS = 48
ESTIMATES_MAX_AGE_DAYS = 7


class ProviderUnavailableError(RuntimeError):
    pass


class ProviderFetchError(RuntimeError):
    pass


@dataclass
class ProviderSnapshot:
    ticker: str
    provider: str
    fetched_at: str
    market_data_as_of: str | None
    estimates_as_of: str | None
    currency: str | None
    financial_currency: str | None
    market_cap: float | None
    total_debt: float | None
    cash_eq: float | None
    preferred_equity: float | None
    minority_interest: float | None
    latest_quarter_end: str | None
    revenue_q: float | None
    revenue_q_1y: float | None
    revenue_estimates_quarterly: dict[str, float]
    errors: list[str]
    raw_payload: dict[str, Any]


@dataclass
class TickerPoint:
    ticker: str
    provider: str
    currency: str | None
    financial_currency: str | None
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
    ntm_mode: str
    request_received_at: str
    market_data_as_of: str | None
    estimates_as_of: str | None
    chart_path: Path
    csv_path: Path
    json_path: Path
    raw_path: Path
    included_count: int
    excluded_count: int
    points: list[TickerPoint]


class LocalTtlJsonCache:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.base_dir / f"{digest}.json"

    def get(self, key: str, ttl_seconds: int) -> Any | None:
        path = self._path(key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            stored_at = datetime.fromisoformat(data["stored_at"])
            if stored_at.tzinfo is None:
                stored_at = stored_at.replace(tzinfo=UTC)
            age = datetime.now(UTC) - stored_at.astimezone(UTC)
            if age.total_seconds() > ttl_seconds:
                return None
            return data.get("payload")
        except Exception:
            return None

    def set(self, key: str, payload: Any) -> None:
        path = self._path(key)
        wrapped = {
            "stored_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "payload": payload,
        }
        path.write_text(json.dumps(wrapped, indent=2))


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _safe_iso(ts: Any) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, pd.Timestamp):
        dt = ts.to_pydatetime()
    elif isinstance(ts, datetime):
        dt = ts
    else:
        try:
            dt = pd.Timestamp(ts).to_pydatetime()
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.replace(microsecond=0).isoformat()


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


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


def _retry_call(fn, *, label: str, max_attempts: int = 3, base_sleep_seconds: float = 0.7):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: PERF203
            last_error = exc
            if attempt == max_attempts:
                break
            sleep_s = base_sleep_seconds * (2 ** (attempt - 1))
            time.sleep(sleep_s)
    raise ProviderFetchError(f"{label} failed after retries: {last_error}")


def _compute_ntm_revenue(
    quarterly_estimates: dict[str, float],
    *,
    ntm_mode: str,
) -> tuple[float | None, str, list[str]]:
    if ntm_mode != "strict":
        raise ValueError("Only strict NTM mode is supported")

    flags: list[str] = []
    missing_required = [k for k in REQUIRED_NTM_PERIODS if quarterly_estimates.get(k) is None]

    if not missing_required:
        ntm = sum(float(quarterly_estimates[k]) for k in REQUIRED_NTM_PERIODS)
        return ntm, "strict_4q", flags

    flags.append("missing_ntm_estimates")
    flags.append("missing_estimate_periods:" + ",".join(missing_required))
    return None, "strict_missing", flags


def _is_market_data_stale(market_as_of: str | None, now_utc: datetime) -> bool:
    market_dt = _parse_iso(market_as_of)
    if market_dt is None:
        return True

    age = now_utc - market_dt
    # Allow extra buffer around weekends.
    max_age = timedelta(hours=MARKET_MAX_AGE_HOURS)
    if now_utc.weekday() == 0:  # Monday
        max_age = timedelta(hours=72)
    return age > max_age


def _is_estimates_data_stale(estimates_as_of: str | None, now_utc: datetime) -> bool:
    est_dt = _parse_iso(estimates_as_of)
    if est_dt is None:
        return True
    return (now_utc - est_dt) > timedelta(days=ESTIMATES_MAX_AGE_DAYS)


class GoogleAdapter:
    name = "google"

    def fetch_many(self, tickers: list[str], _cache: LocalTtlJsonCache, _request_at: str) -> list[ProviderSnapshot]:
        raise ProviderUnavailableError(
            "google_provider_unavailable_for_required_fields: "
            "Google Finance endpoint in this build cannot reliably provide EV components and explicit next 4 quarterly revenue estimates."
        )


class YahooAdapter:
    name = "yahoo"

    def __init__(self, cache_ttl_seconds: int = 300) -> None:
        self.cache_ttl_seconds = cache_ttl_seconds

    def fetch_many(self, tickers: list[str], cache: LocalTtlJsonCache, request_at: str) -> list[ProviderSnapshot]:
        return [self._fetch_one(t, cache, request_at) for t in tickers]

    def _fetch_one(self, ticker: str, cache: LocalTtlJsonCache, request_at: str) -> ProviderSnapshot:
        cache_key = f"{self.name}:{CACHE_SCHEMA_VERSION}:{ticker.upper()}"
        cached = cache.get(cache_key, self.cache_ttl_seconds)
        if isinstance(cached, dict):
            return ProviderSnapshot(**cached)

        fetched_at = _utc_now_iso()
        try:
            yf_ticker = yf.Ticker(ticker)

            history = _retry_call(
                lambda: yf_ticker.history(period="10d", interval="1d", auto_adjust=False),
                label=f"{ticker}:history",
            )
            market_data_as_of = _safe_iso(history.index.max()) if not history.empty else None

            info = _retry_call(lambda: yf_ticker.info or {}, label=f"{ticker}:info")
            fast_info = _retry_call(
                lambda: {
                    "marketCap": yf_ticker.fast_info.get("marketCap"),
                    "currency": yf_ticker.fast_info.get("currency"),
                    "lastPrice": yf_ticker.fast_info.get("lastPrice"),
                },
                label=f"{ticker}:fast_info",
            )

            qis = _retry_call(lambda: yf_ticker.quarterly_income_stmt, label=f"{ticker}:quarterly_income_stmt")
            qbs = _retry_call(lambda: yf_ticker.quarterly_balance_sheet, label=f"{ticker}:quarterly_balance_sheet")
            rev_est = _retry_call(lambda: yf_ticker.revenue_estimate, label=f"{ticker}:revenue_estimate")

            quote_currency = fast_info.get("currency") or info.get("currency")
            financial_currency = info.get("financialCurrency") or info.get("currency")
            currency = quote_currency or financial_currency
            market_cap = _as_float(fast_info.get("marketCap")) or _as_float(info.get("marketCap"))
            total_debt = _as_float(info.get("totalDebt"))
            cash_eq = _as_float(info.get("totalCash"))
            preferred_equity = _as_float(info.get("preferredStock")) or 0.0
            minority_interest = _as_float(info.get("minorityInterest")) or 0.0

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

            revenue_q = None
            revenue_q_1y = None
            latest_quarter_end = None
            revenue_series_export: list[dict[str, Any]] = []
            if isinstance(qis, pd.DataFrame) and not qis.empty:
                revenue_series = _extract_series_row(qis, ["Total Revenue", "Operating Revenue"])
                if revenue_series is not None:
                    revenue_series = revenue_series.sort_index()
                    for idx, value in revenue_series.items():
                        val = _as_float(value)
                        if val is not None:
                            revenue_series_export.append({"asOfDate": _safe_iso(idx), "revenue": val})
                    if len(revenue_series) >= 5:
                        revenue_q = _as_float(revenue_series.iloc[-1])
                        revenue_q_1y = _as_float(revenue_series.iloc[-5])
                        latest_quarter_end = _safe_iso(revenue_series.index[-1])

            estimates_as_of = fetched_at
            quarterly_estimates: dict[str, float] = {}
            if isinstance(rev_est, pd.DataFrame) and not rev_est.empty and "avg" in rev_est.columns:
                for key in ["0q", "+1q", "+2q", "+3q", "0y", "+1y"]:
                    if key in rev_est.index:
                        val = _as_float(rev_est.loc[key, "avg"])
                        if val is not None:
                            quarterly_estimates[key] = val

            raw_payload = {
                "market_data_as_of": market_data_as_of,
                "fast_info": fast_info,
                "info_selected": {
                    "marketCap": info.get("marketCap"),
                    "totalDebt": info.get("totalDebt"),
                    "totalCash": info.get("totalCash"),
                    "quoteCurrency": quote_currency,
                    "financialCurrency": financial_currency,
                },
                "revenue_estimate": quarterly_estimates,
                "quarterly_revenue_series": revenue_series_export,
            }

            snapshot = ProviderSnapshot(
                ticker=ticker.upper(),
                provider=self.name,
                fetched_at=fetched_at,
                market_data_as_of=market_data_as_of,
                estimates_as_of=estimates_as_of,
                currency=currency,
                financial_currency=financial_currency,
                market_cap=market_cap,
                total_debt=total_debt,
                cash_eq=cash_eq,
                preferred_equity=preferred_equity,
                minority_interest=minority_interest,
                latest_quarter_end=latest_quarter_end,
                revenue_q=revenue_q,
                revenue_q_1y=revenue_q_1y,
                revenue_estimates_quarterly=quarterly_estimates,
                errors=[],
                raw_payload=raw_payload,
            )
            cache.set(cache_key, asdict(snapshot))
            return snapshot
        except Exception as exc:
            snapshot = ProviderSnapshot(
                ticker=ticker.upper(),
                provider=self.name,
                fetched_at=fetched_at,
                market_data_as_of=None,
                estimates_as_of=None,
                currency=None,
                financial_currency=None,
                market_cap=None,
                total_debt=None,
                cash_eq=None,
                preferred_equity=None,
                minority_interest=None,
                latest_quarter_end=None,
                revenue_q=None,
                revenue_q_1y=None,
                revenue_estimates_quarterly={},
                errors=[f"provider_fetch_error:{exc.__class__.__name__}"],
                raw_payload={"error": str(exc)},
            )
            cache.set(cache_key, asdict(snapshot))
            return snapshot


def _build_point(
    snapshot: ProviderSnapshot,
    *,
    request_at: str,
    ntm_mode: str,
    now_utc: datetime,
) -> TickerPoint:
    flags: list[str] = list(snapshot.errors)

    yoy_growth = None
    if snapshot.revenue_q is not None and snapshot.revenue_q_1y is not None and snapshot.revenue_q_1y > 0:
        yoy_growth = (snapshot.revenue_q / snapshot.revenue_q_1y) - 1.0
    else:
        flags.append("missing_yoy_inputs")

    ntm_revenue, ntm_method, ntm_flags = _compute_ntm_revenue(
        snapshot.revenue_estimates_quarterly,
        ntm_mode=ntm_mode,
    )
    flags.extend(ntm_flags)
    if ntm_revenue is not None and ntm_revenue <= 0:
        flags.append("nonpositive_ntm_revenue")
        ntm_revenue = None

    ev = None
    if snapshot.market_cap is not None and snapshot.total_debt is not None and snapshot.cash_eq is not None:
        ev = (
            snapshot.market_cap
            + snapshot.total_debt
            + (snapshot.preferred_equity or 0.0)
            + (snapshot.minority_interest or 0.0)
            - snapshot.cash_eq
        )
    else:
        flags.append("missing_ev_inputs")

    if snapshot.currency and snapshot.financial_currency and snapshot.currency != snapshot.financial_currency:
        flags.append("currency_mismatch")

    multiple = None
    if ev is not None and ntm_revenue is not None and ntm_revenue > 0:
        multiple = ev / ntm_revenue
    else:
        flags.append("missing_ev_ntm_inputs")

    if _is_market_data_stale(snapshot.market_data_as_of, now_utc):
        flags.append("stale_market_data")
    if _is_estimates_data_stale(snapshot.estimates_as_of, now_utc):
        flags.append("stale_estimates")

    required_fields = {
        "market_cap": snapshot.market_cap,
        "total_debt": snapshot.total_debt,
        "cash_eq": snapshot.cash_eq,
        "yoy_growth_pct": yoy_growth,
        "ntm_revenue": ntm_revenue,
        "ev_ntm_revenue": multiple,
    }
    missing_required = [k for k, v in required_fields.items() if v is None]
    if "stale_market_data" in flags:
        exclusion_reason = "stale_market_data"
    elif "stale_estimates" in flags:
        exclusion_reason = "stale_estimates"
    elif "currency_mismatch" in flags:
        exclusion_reason = "currency_mismatch"
    elif "missing_ntm_estimates" in flags:
        exclusion_reason = "missing_ntm_estimates"
    elif "nonpositive_ntm_revenue" in flags:
        exclusion_reason = "nonpositive_ntm_revenue"
    elif snapshot.total_debt is None:
        exclusion_reason = "missing_debt"
    elif missing_required:
        exclusion_reason = "missing_required_fields:" + ",".join(missing_required)
    else:
        exclusion_reason = None
    included = exclusion_reason is None

    return TickerPoint(
        ticker=snapshot.ticker,
        provider=snapshot.provider,
        currency=snapshot.currency,
        financial_currency=snapshot.financial_currency,
        request_received_at=request_at,
        market_data_as_of=snapshot.market_data_as_of,
        estimates_as_of=snapshot.estimates_as_of,
        latest_quarter_end=snapshot.latest_quarter_end,
        revenue_q=snapshot.revenue_q,
        revenue_q_1y=snapshot.revenue_q_1y,
        yoy_growth_pct=yoy_growth,
        ntm_revenue=ntm_revenue,
        ntm_estimate_method=ntm_method,
        market_cap=snapshot.market_cap,
        total_debt=snapshot.total_debt,
        preferred_equity=snapshot.preferred_equity,
        minority_interest=snapshot.minority_interest,
        cash_eq=snapshot.cash_eq,
        enterprise_value=ev,
        ev_ntm_revenue=multiple,
        quality_flags=flags,
        included=included,
        exclusion_reason=exclusion_reason,
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
    *,
    ntm_mode: str = "strict",
) -> ChartResult:
    provider_preference = provider_preference or ["google", "yahoo"]
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    if not tickers:
        raise ValueError("No tickers provided")
    if ntm_mode != "strict":
        raise ValueError("ntm_mode must be 'strict'")

    request_at = _utc_now_iso()
    now_utc = datetime.now(UTC)

    cache = LocalTtlJsonCache(CACHE_DIR)
    adapters = {
        "google": GoogleAdapter(),
        "yahoo": YahooAdapter(),
    }

    provider_used = None
    fallback_reason = None
    snapshots: list[ProviderSnapshot] = []

    for provider in provider_preference:
        adapter = adapters.get(provider)
        if adapter is None:
            continue
        try:
            snapshots = adapter.fetch_many(tickers, cache, request_at)
            provider_used = provider
            break
        except ProviderUnavailableError as exc:
            fallback_reason = str(exc)
            continue

    if provider_used is None:
        raise RuntimeError("No data provider succeeded")

    points = [_build_point(s, request_at=request_at, ntm_mode=ntm_mode, now_utc=now_utc) for s in snapshots]

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    chart_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}.png"
    csv_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}.csv"
    json_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}.json"
    raw_path = ARTIFACTS_DIR / f"valuation-scatter-{ts}-raw.json"

    pd.DataFrame([asdict(p) for p in points]).to_csv(csv_path, index=False)
    pd.DataFrame([asdict(p) for p in points]).to_json(json_path, orient="records", indent=2)

    raw_export = {
        "field_contract": {
            "ntm_required_periods": list(REQUIRED_NTM_PERIODS),
            "strict_ntm_only": True,
            "freshness": {
                "market_max_age_hours": MARKET_MAX_AGE_HOURS,
                "estimates_max_age_days": ESTIMATES_MAX_AGE_DAYS,
            },
        },
        "provider_used": provider_used,
        "provider_fallback_reason": fallback_reason,
        "request_received_at": request_at,
        "ntm_mode": ntm_mode,
        "ticker_snapshots": [asdict(s) for s in snapshots],
    }
    raw_path.write_text(json.dumps(raw_export, indent=2))

    market_as_of = max((p.market_data_as_of for p in points if p.market_data_as_of), default=None)
    estimates_as_of = max((p.estimates_as_of for p in points if p.estimates_as_of), default=None)

    title_suffix = f"provider={provider_used}, ntm_mode={ntm_mode}, market_as_of={market_as_of or 'n/a'}"
    _render_chart(points, chart_path, title_suffix)

    included_count = sum(1 for p in points if p.included)
    excluded_count = len(points) - included_count

    return ChartResult(
        tickers=tickers,
        provider_requested=provider_preference[0],
        provider_used=provider_used,
        provider_fallback_reason=fallback_reason,
        ntm_mode=ntm_mode,
        request_received_at=request_at,
        market_data_as_of=market_as_of,
        estimates_as_of=estimates_as_of,
        chart_path=chart_path,
        csv_path=csv_path,
        json_path=json_path,
        raw_path=raw_path,
        included_count=included_count,
        excluded_count=excluded_count,
        points=points,
    )

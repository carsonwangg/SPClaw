from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Callable

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd
import yfinance as yf

from coatue_claw.chart_metrics import DEFAULT_X_METRIC, DEFAULT_Y_METRIC, METRIC_SPECS, metric_axis_kind, metric_label


ARTIFACTS_DIR = Path("/opt/coatue-claw-data/artifacts/charts")
CACHE_DIR = Path("/opt/coatue-claw-data/cache/valuation")
CACHE_SCHEMA_VERSION = "v5"

# Freshness policy.
MARKET_MAX_AGE_HOURS = 48
FUNDAMENTALS_MAX_AGE_DAYS = 180
COATUE_FONT_FAMILY = ["Avenir Next", "Avenir", "Helvetica Neue", "Arial", "DejaVu Sans"]


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
    revenue_last_4q_sum: float | None
    errors: list[str]
    raw_payload: dict[str, Any]
    company_category: str | None = None


@dataclass
class TickerPoint:
    ticker: str
    provider: str
    currency: str | None
    financial_currency: str | None
    request_received_at: str
    market_data_as_of: str | None
    fundamentals_as_of: str | None
    latest_quarter_end: str | None
    revenue_q: float | None
    revenue_q_1y: float | None
    yoy_growth_pct: float | None
    ltm_revenue: float | None
    ltm_method: str
    market_cap: float | None
    total_debt: float | None
    preferred_equity: float | None
    minority_interest: float | None
    cash_eq: float | None
    enterprise_value: float | None
    ev_ltm_revenue: float | None
    quality_flags: list[str]
    included: bool
    exclusion_reason: str | None
    company_category: str | None = None


@dataclass
class ChartResult:
    tickers: list[str]
    provider_requested: str
    provider_used: str
    provider_fallback_reason: str | None
    metric_mode: str
    x_metric: str
    y_metric: str
    request_received_at: str
    market_data_as_of: str | None
    fundamentals_as_of: str | None
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


def _format_readable_date(ts: str | None) -> str:
    dt = _parse_iso(ts)
    if dt is None:
        return "n/a"
    return dt.strftime("%b %d, %Y")


def _canonical_category(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return "Unclassified"
    return value


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


def _is_fundamentals_stale(fundamentals_as_of: str | None, now_utc: datetime) -> bool:
    fund_dt = _parse_iso(fundamentals_as_of)
    if fund_dt is None:
        return True
    return (now_utc - fund_dt) > timedelta(days=FUNDAMENTALS_MAX_AGE_DAYS)


class GoogleAdapter:
    name = "google"

    def fetch_many(self, tickers: list[str], _cache: LocalTtlJsonCache, _request_at: str) -> list[ProviderSnapshot]:
        raise ProviderUnavailableError(
            "google_provider_unavailable_for_required_fields: "
            "Google Finance endpoint in this build cannot reliably provide EV components and LTM revenue fields."
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

            quote_currency = fast_info.get("currency") or info.get("currency")
            financial_currency = info.get("financialCurrency") or info.get("currency")
            currency = quote_currency or financial_currency
            market_cap = _as_float(fast_info.get("marketCap")) or _as_float(info.get("marketCap"))
            total_debt = _as_float(info.get("totalDebt"))
            cash_eq = _as_float(info.get("totalCash"))
            preferred_equity = _as_float(info.get("preferredStock")) or 0.0
            minority_interest = _as_float(info.get("minorityInterest")) or 0.0
            company_category = _canonical_category(
                info.get("sectorDisp") or info.get("sector") or info.get("industryDisp") or info.get("industry")
            )

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
            revenue_last_4q_sum = None
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
                    if len(revenue_series) >= 1:
                        latest_quarter_end = _safe_iso(revenue_series.index[-1])
                    if len(revenue_series) >= 4:
                        recent_4 = [_as_float(v) for v in revenue_series.iloc[-4:]]
                        if all(v is not None for v in recent_4):
                            revenue_last_4q_sum = float(sum(v for v in recent_4 if v is not None))
                    if len(revenue_series) >= 5:
                        revenue_q = _as_float(revenue_series.iloc[-1])
                        revenue_q_1y = _as_float(revenue_series.iloc[-5])

            raw_payload = {
                "market_data_as_of": market_data_as_of,
                "fast_info": fast_info,
                "info_selected": {
                    "marketCap": info.get("marketCap"),
                    "totalDebt": info.get("totalDebt"),
                    "totalCash": info.get("totalCash"),
                    "quoteCurrency": quote_currency,
                    "financialCurrency": financial_currency,
                    "companyCategory": company_category,
                },
                "quarterly_revenue_series": revenue_series_export,
            }

            snapshot = ProviderSnapshot(
                ticker=ticker.upper(),
                provider=self.name,
                fetched_at=fetched_at,
                market_data_as_of=market_data_as_of,
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
                revenue_last_4q_sum=revenue_last_4q_sum,
                errors=[],
                raw_payload=raw_payload,
                company_category=company_category,
            )
            cache.set(cache_key, asdict(snapshot))
            return snapshot
        except Exception as exc:
            snapshot = ProviderSnapshot(
                ticker=ticker.upper(),
                provider=self.name,
                fetched_at=fetched_at,
                market_data_as_of=None,
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
                revenue_last_4q_sum=None,
                errors=[f"provider_fetch_error:{exc.__class__.__name__}"],
                raw_payload={"error": str(exc)},
                company_category=None,
            )
            cache.set(cache_key, asdict(snapshot))
            return snapshot


def _build_point(
    snapshot: ProviderSnapshot,
    *,
    request_at: str,
    now_utc: datetime,
) -> TickerPoint:
    flags: list[str] = list(snapshot.errors)

    yoy_growth = None
    if snapshot.revenue_q is not None and snapshot.revenue_q_1y is not None and snapshot.revenue_q_1y > 0:
        yoy_growth = (snapshot.revenue_q / snapshot.revenue_q_1y) - 1.0
    else:
        flags.append("missing_yoy_inputs")

    ltm_revenue = snapshot.revenue_last_4q_sum
    if ltm_revenue is None:
        flags.append("missing_ltm_revenue")
        ltm_method = "missing"
    elif ltm_revenue <= 0:
        flags.append("nonpositive_ltm_revenue")
        ltm_revenue = None
        ltm_method = "invalid"
    else:
        ltm_method = "reported_4q_sum"

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
    if ev is not None and ltm_revenue is not None and ltm_revenue > 0:
        multiple = ev / ltm_revenue
    else:
        flags.append("missing_ev_ltm_inputs")

    if _is_market_data_stale(snapshot.market_data_as_of, now_utc):
        flags.append("stale_market_data")
    if _is_fundamentals_stale(snapshot.latest_quarter_end, now_utc):
        flags.append("stale_fundamentals")

    required_fields = {
        "market_cap": snapshot.market_cap,
        "total_debt": snapshot.total_debt,
        "cash_eq": snapshot.cash_eq,
        "yoy_growth_pct": yoy_growth,
        "ltm_revenue": ltm_revenue,
        "ev_ltm_revenue": multiple,
    }
    missing_required = [k for k, v in required_fields.items() if v is None]

    if "stale_market_data" in flags:
        exclusion_reason = "stale_market_data"
    elif "stale_fundamentals" in flags:
        exclusion_reason = "stale_fundamentals"
    elif "currency_mismatch" in flags:
        exclusion_reason = "currency_mismatch"
    elif "missing_ltm_revenue" in flags:
        exclusion_reason = "missing_ltm_revenue"
    elif "nonpositive_ltm_revenue" in flags:
        exclusion_reason = "nonpositive_ltm_revenue"
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
        fundamentals_as_of=snapshot.latest_quarter_end,
        latest_quarter_end=snapshot.latest_quarter_end,
        revenue_q=snapshot.revenue_q,
        revenue_q_1y=snapshot.revenue_q_1y,
        yoy_growth_pct=yoy_growth,
        ltm_revenue=ltm_revenue,
        ltm_method=ltm_method,
        market_cap=snapshot.market_cap,
        total_debt=snapshot.total_debt,
        preferred_equity=snapshot.preferred_equity,
        minority_interest=snapshot.minority_interest,
        cash_eq=snapshot.cash_eq,
        enterprise_value=ev,
        ev_ltm_revenue=multiple,
        quality_flags=flags,
        included=included,
        exclusion_reason=exclusion_reason,
        company_category=_canonical_category(snapshot.company_category),
    )


def _resolve_metric_value(point: TickerPoint, metric_id: str) -> float | None:
    if metric_id == "ev_ltm_revenue":
        return point.ev_ltm_revenue
    if metric_id == "yoy_revenue_growth_pct":
        if point.yoy_growth_pct is None:
            return None
        return point.yoy_growth_pct * 100.0
    if metric_id == "ltm_revenue":
        return point.ltm_revenue
    if metric_id == "revenue_q":
        return point.revenue_q
    if metric_id == "enterprise_value":
        return point.enterprise_value
    if metric_id == "market_cap":
        return point.market_cap
    if metric_id == "total_debt":
        return point.total_debt
    if metric_id == "cash_eq":
        return point.cash_eq
    raise ValueError(f"Unsupported metric: {metric_id}")


def _format_usd_axis(value: float, _pos: int | None = None) -> str:
    abs_value = abs(value)
    if abs_value >= 1e12:
        return f"${value / 1e12:.1f}T"
    if abs_value >= 1e9:
        return f"${value / 1e9:.1f}B"
    if abs_value >= 1e6:
        return f"${value / 1e6:.0f}M"
    return f"${value:,.0f}"


def _resolve_axis_formatter(metric_id: str) -> Callable[[float, int | None], str]:
    axis_kind = metric_axis_kind(metric_id)
    if axis_kind == "multiple":
        return lambda v, _p: f"{v:.1f}x"
    if axis_kind == "percent":
        return lambda v, _p: f"{v:.0f}%"
    if axis_kind == "usd":
        return _format_usd_axis
    return lambda v, _p: f"{v:.2f}"


def _is_metric_eligible(point: TickerPoint, metric_id: str) -> bool:
    # Keep strict freshness gating for any plotted series.
    if "stale_market_data" in point.quality_flags:
        return False
    if "stale_fundamentals" in point.quality_flags:
        return False
    if metric_id == "ev_ltm_revenue" and "currency_mismatch" in point.quality_flags:
        return False
    return _resolve_metric_value(point, metric_id) is not None


def _render_chart(points: list[TickerPoint], out_path: Path, title_suffix: str, *, x_metric: str, y_metric: str) -> None:
    plt.rcParams["font.family"] = COATUE_FONT_FAMILY
    included_rows = []
    for point in points:
        if not _is_metric_eligible(point, x_metric):
            continue
        if not _is_metric_eligible(point, y_metric):
            continue
        x_value = _resolve_metric_value(point, x_metric)
        y_value = _resolve_metric_value(point, y_metric)
        if x_value is None or y_value is None:
            continue
        included_rows.append((point, x_value, y_value))
    included = [row[0] for row in included_rows]

    fig = plt.figure(figsize=(14, 9), facecolor="#E9EAED")
    ax = fig.add_axes([0.08, 0.14, 0.86, 0.56], facecolor="#F3F4F6")

    fig.text(
        0.05,
        0.92,
        f"{metric_label(x_metric)} vs. {metric_label(y_metric)}",
        fontsize=34,
        color="#191A2B",
        family=COATUE_FONT_FAMILY,
    )
    fig.text(
        0.05,
        0.81,
        "Public growth comp set (latest snapshot)",
        fontsize=17,
        color="#242637",
        family=COATUE_FONT_FAMILY,
    )
    fig.add_artist(Line2D([0.05, 0.95], [0.785, 0.785], transform=fig.transFigure, color="#202130", linewidth=2.0))

    if not included:
        ax.text(0.5, 0.5, "No valid points after quality filters", ha="center", va="center", color="#4B4D57", fontsize=16)
        ax.set_axis_off()
        fig.text(0.025, 0.04, "COATUE CLAW", fontsize=16, color="#121318", weight="bold")
        fig.text(0.14, 0.044, f"{title_suffix}", fontsize=9, color="#3C3E49")
        fig.savefig(out_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return

    x = np.array([row[1] for row in included_rows], dtype=float)
    y = np.array([row[2] for row in included_rows], dtype=float)

    x_span = float(np.max(x) - np.min(x)) if len(x) else 0.0
    y_span = float(np.max(y) - np.min(y)) if len(y) else 0.0
    x_pad = max(0.5, x_span * 0.10)
    y_pad = max(1.5, y_span * 0.16)

    x_min = float(np.min(x) - x_pad)
    x_max = float(np.max(x) + x_pad)
    y_min = float(np.min(y) - y_pad)
    y_max = float(np.max(y) + y_pad)
    if y_min > 0:
        y_min = min(-2.0, y_min)

    categories = [p.company_category or "Unclassified" for p in included]
    category_order = sorted(set(categories), key=str.casefold)
    palette = [
        "#1F5AA6",
        "#2A7F62",
        "#AA5A19",
        "#A02755",
        "#5A4699",
        "#7E8084",
        "#2687A8",
        "#7B6A34",
        "#BD3E2F",
        "#3F6E7A",
    ]
    color_map = {cat: palette[idx % len(palette)] for idx, cat in enumerate(category_order)}

    for category in category_order:
        idx = [i for i, c in enumerate(categories) if c == category]
        ax.scatter(
            x[idx],
            y[idx],
            s=54,
            color=color_map[category],
            alpha=0.96,
            zorder=3,
            label=category,
        )

    for p, xv, yv in zip(included, x, y):
        if len(included) <= 14:
            ax.annotate(p.ticker, (xv, yv), textcoords="offset points", xytext=(5, 5), fontsize=9, color="#2A2C36")

    if len(included) >= 2:
        coeff = np.polyfit(x, y, 1)
        line_x = np.linspace(x_min, x_max, 120)
        line_y = coeff[0] * line_x + coeff[1]
        ax.plot(line_x, line_y, color="#4B4D57", linewidth=1.6, alpha=0.9, zorder=2)

        y_hat = coeff[0] * x + coeff[1]
        ss_res = float(np.sum((y - y_hat) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = 0.0 if ss_tot <= 0 else max(0.0, min(1.0, 1.0 - (ss_res / ss_tot)))
        ax.text(0.995, 1.01, f"R^2 = {r2:.0%}", transform=ax.transAxes, ha="right", va="bottom", fontsize=16, color="#121318", weight="bold")

    if metric_axis_kind(x_metric) == "multiple":
        today_x = float(np.median(x))
        ax.axvline(today_x, linestyle=(0, (4, 4)), color="#39A778", linewidth=2.0, alpha=0.95, zorder=1)
        x_frac = (today_x - x_min) / (x_max - x_min) if x_max > x_min else 0.5
        x_frac = min(0.92, max(0.08, x_frac))
        ax.text(
            x_frac,
            1.005,
            f"{metric_label(x_metric)} median = {today_x:.1f}x",
            transform=ax.transAxes,
            ha="center",
            va="bottom",
            fontsize=13,
            color="#39A778",
            weight="bold",
        )

    ax.axhline(0, color="#B8BAC1", linewidth=1.0, zorder=1)

    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_min, y_max)
    ax.xaxis.set_major_formatter(FuncFormatter(_resolve_axis_formatter(x_metric)))
    ax.yaxis.set_major_formatter(FuncFormatter(_resolve_axis_formatter(y_metric)))
    ax.tick_params(axis="both", colors="#595B63", labelsize=11)

    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#BABCC3")
    ax.spines["bottom"].set_color("#BABCC3")
    ax.spines["left"].set_linewidth(1.0)
    ax.spines["bottom"].set_linewidth(1.0)

    ax.set_xlabel(metric_label(x_metric), color="#2B2D37", fontsize=12, labelpad=12)
    ax.set_ylabel(metric_label(y_metric), color="#2B2D37", fontsize=12, labelpad=12)
    legend = ax.legend(
        title="Category",
        loc="upper left",
        bbox_to_anchor=(0.0, 1.01),
        ncol=2,
        fontsize=9,
        title_fontsize=9,
        frameon=False,
    )
    if legend is not None:
        legend.get_title().set_color("#2B2D37")

    fig.text(0.025, 0.04, "COATUE CLAW", fontsize=16, color="#121318", weight="bold")
    fig.text(0.14, 0.044, f"{title_suffix}", fontsize=9, color="#3C3E49")

    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def run_valuation_chart(
    tickers: list[str],
    provider_preference: list[str] | None = None,
    *,
    x_metric: str = DEFAULT_X_METRIC,
    y_metric: str = DEFAULT_Y_METRIC,
) -> ChartResult:
    provider_preference = provider_preference or ["google", "yahoo"]
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    if not tickers:
        raise ValueError("No tickers provided")
    if x_metric not in METRIC_SPECS:
        raise ValueError(f"Unsupported x_metric: {x_metric}")
    if y_metric not in METRIC_SPECS:
        raise ValueError(f"Unsupported y_metric: {y_metric}")

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

    points = [_build_point(s, request_at=request_at, now_utc=now_utc) for s in snapshots]

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
            "metric_mode": f"{x_metric}_vs_{y_metric}",
            "x_metric": x_metric,
            "y_metric": y_metric,
            "denominator": "sum_last_4_reported_quarters",
            "freshness": {
                "market_max_age_hours": MARKET_MAX_AGE_HOURS,
                "fundamentals_max_age_days": FUNDAMENTALS_MAX_AGE_DAYS,
            },
        },
        "provider_used": provider_used,
        "provider_fallback_reason": fallback_reason,
        "request_received_at": request_at,
        "ticker_snapshots": [asdict(s) for s in snapshots],
    }
    raw_path.write_text(json.dumps(raw_export, indent=2))

    market_as_of = max((p.market_data_as_of for p in points if p.market_data_as_of), default=None)
    fundamentals_as_of = max((p.fundamentals_as_of for p in points if p.fundamentals_as_of), default=None)

    market_as_of_display = _format_readable_date(market_as_of)
    title_suffix = f"Provider: {provider_used} | X: {metric_label(x_metric)} | Y: {metric_label(y_metric)} | As of: {market_as_of_display}"
    _render_chart(points, chart_path, title_suffix, x_metric=x_metric, y_metric=y_metric)

    included_count = sum(1 for p in points if p.included)
    excluded_count = len(points) - included_count

    return ChartResult(
        tickers=tickers,
        provider_requested=provider_preference[0],
        provider_used=provider_used,
        provider_fallback_reason=fallback_reason,
        metric_mode=f"{x_metric}_vs_{y_metric}",
        x_metric=x_metric,
        y_metric=y_metric,
        request_received_at=request_at,
        market_data_as_of=market_as_of,
        fundamentals_as_of=fundamentals_as_of,
        chart_path=chart_path,
        csv_path=csv_path,
        json_path=json_path,
        raw_path=raw_path,
        included_count=included_count,
        excluded_count=excluded_count,
        points=points,
    )

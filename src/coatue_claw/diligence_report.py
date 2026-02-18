from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import math
import os
from pathlib import Path
import sqlite3
from typing import Any, Callable

import pandas as pd
import yfinance as yf

SOURCE_NAME = "Yahoo Finance via yfinance"
LOCAL_SOURCE_NAME = "Local research database (file ingest + prior packets)"


@dataclass(frozen=True)
class RevenuePoint:
    period: str
    value: float


@dataclass(frozen=True)
class NewsPoint:
    title: str
    publisher: str
    url: str
    published_at: str


@dataclass(frozen=True)
class LocalResearchReport:
    source: str
    title: str
    path: str
    category: str | None
    recorded_at_utc: str | None


@dataclass(frozen=True)
class LocalResearchLookup:
    query: str
    checked_at_utc: str
    reports: list[LocalResearchReport]


@dataclass(frozen=True)
class DiligenceSnapshot:
    requested_ticker: str
    ticker: str
    company_name: str
    generated_at_utc: str
    local_research: LocalResearchLookup
    summary: str
    sector: str | None
    industry: str | None
    country: str | None
    website: str | None
    full_time_employees: int | None
    market_cap: float | None
    enterprise_value: float | None
    shares_outstanding: float | None
    trailing_pe: float | None
    forward_pe: float | None
    price_to_sales: float | None
    ev_to_revenue: float | None
    ev_to_ebitda: float | None
    gross_margin: float | None
    operating_margin: float | None
    ebitda_margin: float | None
    total_cash: float | None
    total_debt: float | None
    current_ratio: float | None
    debt_to_equity: float | None
    annual_revenue: list[RevenuePoint]
    quarterly_revenue: list[RevenuePoint]
    news: list[NewsPoint]


def build_neutral_investment_memo(
    ticker_or_company: str,
    *,
    ticker_factory: Callable[[str], Any] | None = None,
    now_utc: datetime | None = None,
    local_report_lookup: Callable[[str], LocalResearchLookup] | None = None,
) -> str:
    ticker_factory = ticker_factory or yf.Ticker
    now = now_utc or datetime.now(UTC)
    local_lookup = (
        local_report_lookup(ticker_or_company)
        if local_report_lookup is not None
        else find_local_research_reports(ticker_or_company)
    )
    snapshot = _load_snapshot(
        ticker_or_company=ticker_or_company,
        ticker_factory=ticker_factory,
        now=now,
        local_research=local_lookup,
    )
    return _render_memo(snapshot)


def _file_ingest_db_path() -> Path:
    data_root = Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))
    return Path(os.environ.get("COATUE_CLAW_FILE_INGEST_DB_PATH", str(data_root / "db/file_ingest.sqlite")))


def _packets_dir() -> Path:
    data_root = Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))
    return Path(os.environ.get("COATUE_CLAW_PACKETS_DIR", str(data_root / "artifacts/packets")))


def _search_tokens(query: str) -> list[str]:
    raw = query.strip().upper().lstrip("$")
    if not raw:
        return []
    tokens = [token.lower() for token in raw.replace("-", " ").replace("_", " ").split() if token.strip()]
    if raw.lower() not in tokens:
        tokens.insert(0, raw.lower())
    return tokens


def _lookup_file_ingest_reports(query: str, *, limit: int) -> list[LocalResearchReport]:
    db_path = _file_ingest_db_path()
    if not db_path.exists():
        return []

    tokens = _search_tokens(query)
    if not tokens:
        return []

    clauses: list[str] = []
    params: list[Any] = []
    for token in tokens:
        pattern = f"%{token}%"
        for col in ("original_name", "title", "source_text", "local_path", "drive_path"):
            clauses.append(f"lower(coalesce({col}, '')) LIKE ?")
            params.append(pattern)

    sql = (
        "SELECT original_name, title, category, local_path, drive_path, ingested_at_utc "
        "FROM slack_file_ingest "
        f"WHERE {' OR '.join(clauses)} "
        "ORDER BY ingested_at_utc DESC "
        "LIMIT ?"
    )
    params.append(limit)

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
    except sqlite3.Error:
        return []

    out: list[LocalResearchReport] = []
    for row in rows:
        drive_path = str(row["drive_path"] or "").strip()
        local_path = str(row["local_path"] or "").strip()
        path = drive_path or local_path
        if not path:
            continue
        out.append(
            LocalResearchReport(
                source="file_ingest",
                title=str(row["title"] or row["original_name"] or "untitled"),
                path=path,
                category=(str(row["category"]) if row["category"] else None),
                recorded_at_utc=(str(row["ingested_at_utc"]) if row["ingested_at_utc"] else None),
            )
        )
    return out


def _lookup_packet_reports(query: str, *, limit: int) -> list[LocalResearchReport]:
    packets = _packets_dir()
    if not packets.exists():
        return []

    tokens = _search_tokens(query)
    if not tokens:
        return []

    candidates: list[Path] = sorted(
        (path for path in packets.glob("*.md") if path.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    out: list[LocalResearchReport] = []
    for path in candidates:
        name = path.name.lower()
        if not any(token in name for token in tokens):
            continue
        out.append(
            LocalResearchReport(
                source="packets",
                title=path.name,
                path=str(path),
                category="Packets",
                recorded_at_utc=datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat(),
            )
        )
        if len(out) >= limit:
            break
    return out


def find_local_research_reports(query: str, *, limit: int = 8, now_utc: datetime | None = None) -> LocalResearchLookup:
    now = now_utc or datetime.now(UTC)
    reports = _lookup_file_ingest_reports(query, limit=limit)
    if len(reports) < limit:
        reports.extend(_lookup_packet_reports(query, limit=limit - len(reports)))

    dedupe: dict[str, LocalResearchReport] = {}
    for report in reports:
        key = report.path.lower()
        if key in dedupe:
            continue
        dedupe[key] = report

    return LocalResearchLookup(
        query=query.strip().upper().lstrip("$"),
        checked_at_utc=now.isoformat(),
        reports=list(dedupe.values())[:limit],
    )


def _safe_getattr(obj: Any, name: str, default: Any) -> Any:
    try:
        return getattr(obj, name)
    except Exception:
        return default


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _to_int(value: Any) -> int | None:
    out = _to_float(value)
    if out is None:
        return None
    return int(out)


def _fmt_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "Not disclosed"
    return f"{value:,.{digits}f}"


def _fmt_currency(value: float | None) -> str:
    if value is None:
        return "Not disclosed"
    absolute = abs(value)
    sign = "-" if value < 0 else ""
    if absolute >= 1_000_000_000_000:
        return f"{sign}${absolute/1_000_000_000_000:.2f}T"
    if absolute >= 1_000_000_000:
        return f"{sign}${absolute/1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{sign}${absolute/1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{sign}${absolute/1_000:.2f}K"
    return f"{sign}${absolute:.2f}"


def _fmt_percent(value: float | None) -> str:
    if value is None:
        return "Not disclosed"
    return f"{value * 100:.1f}%"


def _fmt_date(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return "Unknown"
    return ts.strftime("%Y-%m-%d")


def _find_revenue_row(frame: Any) -> pd.Series | None:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None

    row_name = None
    for idx in frame.index:
        text = str(idx).strip().lower()
        norm = text.replace(" ", "")
        if norm in {"totalrevenue", "revenue"}:
            row_name = idx
            break
    if row_name is None:
        for idx in frame.index:
            if "revenue" in str(idx).strip().lower():
                row_name = idx
                break
    if row_name is None:
        return None

    row = frame.loc[row_name]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]
    if not isinstance(row, pd.Series):
        return None
    return row


def _extract_revenue_points(frame: Any, *, quarterly: bool, limit: int = 6) -> list[RevenuePoint]:
    row = _find_revenue_row(frame)
    if row is None:
        return []

    entries: list[tuple[pd.Timestamp, float]] = []
    for label, raw in row.items():
        value = _to_float(raw)
        if value is None:
            continue
        ts = pd.to_datetime(label, errors="coerce", utc=True)
        if pd.isna(ts):
            continue
        entries.append((ts, value))

    entries.sort(key=lambda item: item[0], reverse=True)
    out: list[RevenuePoint] = []
    for ts, value in entries[:limit]:
        if quarterly:
            quarter = ((ts.month - 1) // 3) + 1
            period = f"{ts.year}-Q{quarter}"
        else:
            period = f"{ts.year}"
        out.append(RevenuePoint(period=period, value=value))
    return out


def _extract_news(news_raw: Any, *, limit: int = 5) -> list[NewsPoint]:
    if not isinstance(news_raw, list):
        return []

    points: list[NewsPoint] = []
    for item in news_raw[:limit * 2]:
        if not isinstance(item, dict):
            continue

        content = item.get("content") if isinstance(item.get("content"), dict) else {}
        title = item.get("title") or content.get("title")

        publisher = item.get("publisher")
        if not publisher:
            provider = content.get("provider") if isinstance(content.get("provider"), dict) else {}
            publisher = provider.get("displayName")

        link = item.get("link")
        if not link:
            click = content.get("clickThroughUrl") if isinstance(content.get("clickThroughUrl"), dict) else {}
            link = click.get("url")

        published = item.get("providerPublishTime")
        if published is None:
            pub_time = content.get("pubDate")
            published = pub_time

        if not title:
            continue

        published_str = "Unknown"
        if isinstance(published, (int, float)):
            published_str = datetime.fromtimestamp(published, tz=UTC).strftime("%Y-%m-%d")
        elif isinstance(published, str):
            published_str = _fmt_date(published)

        points.append(
            NewsPoint(
                title=str(title).strip(),
                publisher=str(publisher or "Unknown").strip(),
                url=str(link or "").strip(),
                published_at=published_str,
            )
        )
        if len(points) >= limit:
            break

    return points


def _load_snapshot(
    ticker_or_company: str,
    ticker_factory: Callable[[str], Any],
    now: datetime,
    *,
    local_research: LocalResearchLookup,
) -> DiligenceSnapshot:
    requested = ticker_or_company.strip().upper().lstrip("$")
    ticker_obj = ticker_factory(requested)

    info = _safe_dict(_safe_getattr(ticker_obj, "info", {}))
    ticker = str(info.get("symbol") or requested).upper()
    company_name = str(info.get("longName") or info.get("shortName") or ticker)

    income_stmt = _safe_getattr(ticker_obj, "income_stmt", None)
    quarterly_income_stmt = _safe_getattr(ticker_obj, "quarterly_income_stmt", None)

    news_raw = _safe_getattr(ticker_obj, "news", [])

    return DiligenceSnapshot(
        requested_ticker=requested,
        ticker=ticker,
        company_name=company_name,
        generated_at_utc=now.isoformat(),
        local_research=local_research,
        summary=str(info.get("longBusinessSummary") or "Business summary not available from public sources reviewed."),
        sector=(str(info.get("sector")) if info.get("sector") else None),
        industry=(str(info.get("industry")) if info.get("industry") else None),
        country=(str(info.get("country")) if info.get("country") else None),
        website=(str(info.get("website")) if info.get("website") else None),
        full_time_employees=_to_int(info.get("fullTimeEmployees")),
        market_cap=_to_float(info.get("marketCap")),
        enterprise_value=_to_float(info.get("enterpriseValue")),
        shares_outstanding=_to_float(info.get("sharesOutstanding")),
        trailing_pe=_to_float(info.get("trailingPE")),
        forward_pe=_to_float(info.get("forwardPE")),
        price_to_sales=_to_float(info.get("priceToSalesTrailing12Months")),
        ev_to_revenue=_to_float(info.get("enterpriseToRevenue")),
        ev_to_ebitda=_to_float(info.get("enterpriseToEbitda")),
        gross_margin=_to_float(info.get("grossMargins")),
        operating_margin=_to_float(info.get("operatingMargins")),
        ebitda_margin=_to_float(info.get("ebitdaMargins")),
        total_cash=_to_float(info.get("totalCash")),
        total_debt=_to_float(info.get("totalDebt")),
        current_ratio=_to_float(info.get("currentRatio")),
        debt_to_equity=_to_float(info.get("debtToEquity")),
        annual_revenue=_extract_revenue_points(income_stmt, quarterly=False),
        quarterly_revenue=_extract_revenue_points(quarterly_income_stmt, quarterly=True),
        news=_extract_news(news_raw),
    )


def _growth(latest: RevenuePoint | None, previous: RevenuePoint | None) -> float | None:
    if latest is None or previous is None:
        return None
    if previous.value == 0:
        return None
    return (latest.value / previous.value) - 1


def _top_news_lines(points: list[NewsPoint], limit: int = 3) -> list[str]:
    lines: list[str] = []
    for point in points[:limit]:
        if point.url:
            lines.append(f"- {point.title} ({point.publisher}, {point.published_at}) - {point.url}")
        else:
            lines.append(f"- {point.title} ({point.publisher}, {point.published_at})")
    if not lines:
        lines.append("- No recent headline metadata was returned by public sources reviewed.")
    return lines


def _build_key_takeaways(s: DiligenceSnapshot) -> list[str]:
    takeaways: list[str] = []

    if s.local_research.reports:
        takeaways.append(
            f"Database-first check found `{len(s.local_research.reports)}` internal report(s) for `{s.requested_ticker}` before external data pull. "
            f"[Source: {LOCAL_SOURCE_NAME}, checked {s.local_research.checked_at_utc}]"
        )
    else:
        takeaways.append(
            f"Database-first check found no prior internal reports for `{s.requested_ticker}` before external data pull. "
            f"[Source: {LOCAL_SOURCE_NAME}, checked {s.local_research.checked_at_utc}]"
        )

    annual_latest = s.annual_revenue[0] if s.annual_revenue else None
    annual_prev = s.annual_revenue[1] if len(s.annual_revenue) > 1 else None
    annual_growth = _growth(annual_latest, annual_prev)

    if annual_latest:
        growth_text = _fmt_percent(annual_growth) if annual_growth is not None else "not available"
        takeaways.append(
            f"Revenue evidence: latest annual revenue is {_fmt_currency(annual_latest.value)} ({annual_latest.period}); measured annual growth is {growth_text}. "
            f"[Source: {SOURCE_NAME}, accessed {s.generated_at_utc}]"
        )

    if s.gross_margin is not None or s.operating_margin is not None:
        takeaways.append(
            f"Margin profile: gross margin {_fmt_percent(s.gross_margin)} and operating margin {_fmt_percent(s.operating_margin)}. "
            f"[Source: {SOURCE_NAME}, accessed {s.generated_at_utc}]"
        )

    if s.total_cash is not None or s.total_debt is not None:
        takeaways.append(
            f"Balance sheet signal: cash {_fmt_currency(s.total_cash)} versus total debt {_fmt_currency(s.total_debt)}. "
            f"[Source: {SOURCE_NAME}, accessed {s.generated_at_utc}]"
        )

    takeaways.append(
        "Funding/cap-table detail is limited from the public dataset used here; private round-by-round terms are not directly disclosed in this feed. "
        f"[Source: {SOURCE_NAME}, accessed {s.generated_at_utc}]"
    )

    if s.news:
        takeaways.append(
            f"Recent external reporting exists ({len(s.news)} headlines captured), which may affect sentiment and near-term narratives. "
            f"[Source: {SOURCE_NAME}, accessed {s.generated_at_utc}]"
        )

    return takeaways[:5]


def _revenue_table(points: list[RevenuePoint]) -> str:
    if not points:
        return "No revenue history was returned by public sources reviewed."
    lines = ["| Period | Revenue |", "|---|---:|"]
    for point in points:
        lines.append(f"| {point.period} | {_fmt_currency(point.value)} |")
    return "\n".join(lines)


def _render_memo(s: DiligenceSnapshot) -> str:
    takeaways = _build_key_takeaways(s)
    annual_latest = s.annual_revenue[0] if s.annual_revenue else None
    annual_prev = s.annual_revenue[1] if len(s.annual_revenue) > 1 else None
    annual_growth = _growth(annual_latest, annual_prev)

    quarterly_latest = s.quarterly_revenue[0] if s.quarterly_revenue else None
    quarterly_prev = s.quarterly_revenue[1] if len(s.quarterly_revenue) > 1 else None
    quarterly_growth = _growth(quarterly_latest, quarterly_prev)

    source_line = f"[Source: {SOURCE_NAME}, accessed {s.generated_at_utc}]"

    lines: list[str] = []
    lines.append(f"# Neutral Investment Memo: {s.company_name} ({s.ticker})")
    lines.append("")
    lines.append("> This memo is structured to be neutral and evidence-based. It is not an investment recommendation.")
    lines.append("")
    lines.append("## 1. Key Takeaways")
    lines.append("")
    for point in takeaways:
        lines.append(f"- {point}")
    lines.append("")

    lines.append("## 2. Business Overview")
    lines.append("")
    lines.append(f"- Internal database-first check: `{len(s.local_research.reports)}` matching report artifact(s) identified before online research. [Source: {LOCAL_SOURCE_NAME}, checked {s.local_research.checked_at_utc}]")
    if s.local_research.reports:
        for report in s.local_research.reports[:5]:
            lines.append(
                f"- Local report reference: `{report.title}` ({report.source}, {report.category or 'Uncategorized'}) -> `{report.path}`"
            )
    lines.append(f"- What the company does: {s.summary} {source_line}")
    lines.append(f"- Core value delivery context: sector `{s.sector or 'Not disclosed'}`, industry `{s.industry or 'Not disclosed'}`, geography `{s.country or 'Not disclosed'}`. {source_line}")
    lines.append(f"- Target customer and acquisition details are not fully disclosed in the dataset reviewed; additional channel checks are required. {source_line}")
    lines.append(f"- Business model evidence from public profile is limited in granularity; pricing/contract structure should be validated via filings and investor materials. {source_line}")
    lines.append(f"- Structural problem addressed (inferred from business description): company positioning suggests focus on {s.industry or 'its stated category'} workflows. {source_line}")
    lines.append(f"- Scale metrics snapshot: employees `{_fmt_number(float(s.full_time_employees) if s.full_time_employees is not None else None, 0)}`, market cap `{_fmt_currency(s.market_cap)}`, shares outstanding `{_fmt_number(s.shares_outstanding, 0)}`. {source_line}")
    if s.website:
        lines.append(f"- Corporate website: {s.website} {source_line}")
    lines.append("")

    lines.append("## 3. Financials & Funding")
    lines.append("")
    lines.append("### 3.1 Funding & Cap Table History")
    lines.append("")
    lines.append(f"- Public listing context: this memo uses public-market data for `{s.ticker}`; private round history (seed/Series A/B) is not directly disclosed in this feed. {source_line}")
    lines.append(f"- Capital structure proxies: shares outstanding `{_fmt_number(s.shares_outstanding, 0)}`, market cap `{_fmt_currency(s.market_cap)}`, enterprise value `{_fmt_currency(s.enterprise_value)}`. {source_line}")
    lines.append("- Notable secondary/private transactions: not available from the public dataset reviewed.")
    lines.append("")

    lines.append("### 3.2 Revenue & Growth")
    lines.append("")
    lines.append(f"- Latest annual revenue: `{_fmt_currency(annual_latest.value) if annual_latest else 'Not disclosed'}` ({annual_latest.period if annual_latest else 'n/a'}). {source_line}")
    lines.append(f"- Latest quarterly revenue: `{_fmt_currency(quarterly_latest.value) if quarterly_latest else 'Not disclosed'}` ({quarterly_latest.period if quarterly_latest else 'n/a'}). {source_line}")
    lines.append(f"- Annual growth (latest vs prior): `{_fmt_percent(annual_growth) if annual_growth is not None else 'Not disclosed'}`. {source_line}")
    lines.append(f"- Quarterly growth (latest vs prior): `{_fmt_percent(quarterly_growth) if quarterly_growth is not None else 'Not disclosed'}`. {source_line}")
    lines.append("- Revenue segmentation and formal guidance: not fully available in this feed; validate via filings/earnings transcripts.")
    lines.append("")

    lines.append("### 3.3 Margins & Economics")
    lines.append("")
    lines.append(f"- Gross margin: `{_fmt_percent(s.gross_margin)}`. {source_line}")
    lines.append(f"- Operating margin: `{_fmt_percent(s.operating_margin)}`. {source_line}")
    lines.append(f"- EBITDA margin: `{_fmt_percent(s.ebitda_margin)}`. {source_line}")
    lines.append("- Contribution margin / unit economics / CAC / payback / LTV: not disclosed in this source set.")
    lines.append("")

    lines.append("### 3.4 Valuation Context")
    lines.append("")
    lines.append(f"- EV/Revenue: `{_fmt_number(s.ev_to_revenue)}`; EV/EBITDA: `{_fmt_number(s.ev_to_ebitda)}`. {source_line}")
    lines.append(f"- P/S (trailing): `{_fmt_number(s.price_to_sales)}`; P/E trailing: `{_fmt_number(s.trailing_pe)}`; P/E forward: `{_fmt_number(s.forward_pe)}`. {source_line}")
    lines.append("- Public comp set is not auto-selected in this report; relative multiple context should be built against a defined peer basket.")
    lines.append("")

    lines.append("### 3.5 Balance Sheet & Cash")
    lines.append("")
    lines.append(f"- Cash on hand: `{_fmt_currency(s.total_cash)}`. {source_line}")
    lines.append(f"- Total debt: `{_fmt_currency(s.total_debt)}`; debt-to-equity: `{_fmt_number(s.debt_to_equity)}`; current ratio: `{_fmt_number(s.current_ratio)}`. {source_line}")
    lines.append("- Runway estimate is not directly disclosed; estimate requires burn/FCF profile from filings.")
    lines.append("")

    lines.append("## 4. Market Overview")
    lines.append("")
    lines.append(f"- Market category context: sector `{s.sector or 'Not disclosed'}`, industry `{s.industry or 'Not disclosed'}`. {source_line}")
    lines.append("- TAM estimates are not directly available in this data feed; investor materials and third-party market studies are needed for a bounded TAM range.")
    lines.append("- Structural drivers and trend signals from recent reporting:")
    lines.extend(_top_news_lines(s.news))
    lines.append(f"{source_line}")
    lines.append("- Competitive structure, pricing pressure, and substitution risk should be tested against named peers in a dedicated comp framework.")
    lines.append("")

    lines.append("## 5. Company Strengths")
    lines.append("")
    lines.append(f"- Scale and relevance: market capitalization `{_fmt_currency(s.market_cap)}` indicates material public-market scale. Why it matters: larger scale can support operating leverage and financing flexibility. {source_line}")
    lines.append(f"- Margin structure: gross margin `{_fmt_percent(s.gross_margin)}` suggests the economics of the core offering. Why it matters: higher gross margins can absorb GTM and R&D spend. {source_line}")
    lines.append(f"- Revenue base visibility: latest annual revenue `{_fmt_currency(annual_latest.value) if annual_latest else 'Not disclosed'}`. Why it matters: establishes baseline for scenario analysis. {source_line}")
    lines.append("- External signal flow: recent headline coverage provides ongoing datapoints for narrative tracking and potential demand/competitive signals. Why it matters: supports continuous diligence updates.")
    lines.append("")

    lines.append("## 6. Key Risks")
    lines.append("")
    lines.append(f"- Growth durability risk: annual growth is `{_fmt_percent(annual_growth) if annual_growth is not None else 'Not disclosed'}` and should be monitored for deceleration. Potential impact: multiple compression and lower operating leverage. {source_line}")
    lines.append(f"- Balance sheet risk: debt `{_fmt_currency(s.total_debt)}` vs cash `{_fmt_currency(s.total_cash)}` may constrain flexibility depending on maturity profile and rates. Potential impact: higher financing risk. {source_line}")
    lines.append("- Disclosure granularity risk: customer concentration, CAC/payback, and segment-level profitability are not fully available here. Potential impact: hidden fragility in unit economics.")
    lines.append("- Market structure risk: pricing pressure and incumbent response are not yet quantified in this memo. Potential impact: margin pressure and slower growth.")
    lines.append("- Macro/regulatory sensitivity: should be tested against regional exposure and policy shifts not fully captured in this data extract.")
    lines.append("")

    lines.append("## 7. Open Diligence Questions")
    lines.append("")
    lines.append("- What percent of revenue is recurring vs transactional, and how has that mix shifted over the last 8 quarters?")
    lines.append("- What are net revenue retention, gross retention, and logo churn by customer cohort and segment?")
    lines.append("- How concentrated is revenue in top-10 customers and top verticals, and what is the trend?")
    lines.append("- What is fully-loaded CAC payback by channel, and how sensitive is it to sales efficiency changes?")
    lines.append("- What assumptions are required to justify current valuation multiples versus peers under base/bear/bull growth paths?")
    lines.append("- What specific product/roadmap milestones are critical for sustaining margin and growth, and what is their execution risk?")
    lines.append("")

    lines.append("## 8. Appendix")
    lines.append("")
    lines.append("### Revenue History (Annual)")
    lines.append("")
    lines.append(_revenue_table(s.annual_revenue))
    lines.append("")
    lines.append("### Revenue History (Quarterly)")
    lines.append("")
    lines.append(_revenue_table(s.quarterly_revenue))
    lines.append("")
    lines.append("### Key Market / Financial Metrics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Market Cap | {_fmt_currency(s.market_cap)} |")
    lines.append(f"| Enterprise Value | {_fmt_currency(s.enterprise_value)} |")
    lines.append(f"| Trailing P/E | {_fmt_number(s.trailing_pe)} |")
    lines.append(f"| Forward P/E | {_fmt_number(s.forward_pe)} |")
    lines.append(f"| Price/Sales (TTM) | {_fmt_number(s.price_to_sales)} |")
    lines.append(f"| EV/Revenue | {_fmt_number(s.ev_to_revenue)} |")
    lines.append(f"| EV/EBITDA | {_fmt_number(s.ev_to_ebitda)} |")
    lines.append(f"| Gross Margin | {_fmt_percent(s.gross_margin)} |")
    lines.append(f"| Operating Margin | {_fmt_percent(s.operating_margin)} |")
    lines.append(f"| Cash | {_fmt_currency(s.total_cash)} |")
    lines.append(f"| Total Debt | {_fmt_currency(s.total_debt)} |")
    lines.append("")
    lines.append("### Recent Reporting Headlines")
    lines.append("")
    lines.extend(_top_news_lines(s.news, limit=5))
    lines.append("")
    lines.append("### Sources")
    lines.append("")
    lines.append(f"- {LOCAL_SOURCE_NAME}; check timestamp (UTC): `{s.local_research.checked_at_utc}`.")
    if s.local_research.reports:
        for report in s.local_research.reports[:8]:
            lines.append(f"- Local report: `{report.title}` ({report.source}, {report.category or 'Uncategorized'}) -> `{report.path}`")
    lines.append(f"- {SOURCE_NAME} company profile, valuation, balance-sheet, margins, and statement metadata for `{s.ticker}`.")
    lines.append(f"- Access timestamp (UTC): `{s.generated_at_utc}`")

    return "\n".join(lines) + "\n"

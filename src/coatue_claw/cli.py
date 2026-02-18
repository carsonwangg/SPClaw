from __future__ import annotations

import argparse
from datetime import UTC, datetime
import logging
from pathlib import Path

from coatue_claw.chart_metrics import DEFAULT_X_METRIC, DEFAULT_Y_METRIC, METRIC_SPECS
from coatue_claw.diligence_report import build_neutral_investment_memo
from coatue_claw.valuation_chart import run_valuation_chart

logger = logging.getLogger(__name__)


def run_diligence(ticker: str) -> Path:
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    out_dir = Path("/opt/coatue-claw-data/artifacts/packets")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{ticker.upper()}-{ts}.md"
    try:
        memo = build_neutral_investment_memo(ticker)
    except Exception as exc:
        logger.exception("Failed to generate deep diligence memo for %s", ticker)
        memo = (
            f"# Neutral Investment Memo: {ticker.upper()}\n\n"
            "> Diligence memo generation failed before full research completion.\n\n"
            "## Error Context\n\n"
            f"- runtime error: `{type(exc).__name__}: {exc}`\n"
            f"- generated_at_utc: `{datetime.now(UTC).isoformat()}`\n\n"
            "## Next Action\n\n"
            "- Re-run `diligence TICKER` after validating network/data-provider connectivity.\n"
            "- If this persists, inspect runtime logs and source data availability.\n"
        )
    out_file.write_text(memo, encoding="utf-8")
    return out_file


def _parse_tickers(value: str) -> list[str]:
    tickers = [x.strip().upper().lstrip("$") for x in value.split(",") if x.strip()]
    if not tickers:
        raise argparse.ArgumentTypeError("Tickers cannot be empty")
    return tickers


def main():
    parser = argparse.ArgumentParser("coatue-claw")
    sub = parser.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("diligence")
    d.add_argument("ticker")

    g = sub.add_parser("valuation-chart")
    g.add_argument("tickers", type=_parse_tickers, help="Comma-separated tickers, e.g. SNOW,MDB,DDOG")
    g.add_argument("--x-metric", choices=sorted(METRIC_SPECS.keys()), default=DEFAULT_X_METRIC)
    g.add_argument("--y-metric", choices=sorted(METRIC_SPECS.keys()), default=DEFAULT_Y_METRIC)
    g.add_argument("--title-context", default=None, help="Optional chart title context (e.g. 'Defense Stocks')")

    args = parser.parse_args()

    if args.cmd == "diligence":
        path = run_diligence(args.ticker)
        print(f"created: {path}")
        return

    if args.cmd == "valuation-chart":
        result = run_valuation_chart(
            args.tickers,
            x_metric=args.x_metric,
            y_metric=args.y_metric,
            title_context=args.title_context,
        )
        print(f"provider_requested: {result.provider_requested}")
        print(f"provider_used: {result.provider_used}")
        if result.provider_fallback_reason:
            print(f"provider_fallback_reason: {result.provider_fallback_reason}")
        print(f"metric_mode: {result.metric_mode}")
        print(f"x_metric: {result.x_metric}")
        print(f"y_metric: {result.y_metric}")
        if result.title_context:
            print(f"title_context: {result.title_context}")
        print(f"request_received_at: {result.request_received_at}")
        print(f"market_data_as_of: {result.market_data_as_of}")
        print(f"fundamentals_as_of: {result.fundamentals_as_of}")
        print(f"included: {result.included_count} excluded: {result.excluded_count}")
        print(f"chart: {result.chart_path}")
        print(f"csv: {result.csv_path}")
        print(f"json: {result.json_path}")
        print(f"raw: {result.raw_path}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import logging
from pathlib import Path
import json

from coatue_claw.chart_metrics import DEFAULT_X_METRIC, DEFAULT_Y_METRIC, METRIC_SPECS
from coatue_claw.diligence_report import build_neutral_investment_memo
from coatue_claw.memory_runtime import MemoryRuntime
from coatue_claw.valuation_chart import run_valuation_chart
from coatue_claw.x_chart_daily import add_source as add_x_chart_source
from coatue_claw.x_chart_daily import list_sources as list_x_chart_sources
from coatue_claw.x_chart_daily import run_chart_scout_once
from coatue_claw.x_chart_daily import status as x_chart_status
from coatue_claw.x_digest import build_x_digest

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


def _run_memory_command(args) -> None:
    memory = MemoryRuntime()

    if args.memory_cmd == "status":
        print(json.dumps(memory.stats(), indent=2, sort_keys=True))
        return

    if args.memory_cmd == "query":
        print(memory.format_retrieval(args.query, limit=args.limit))
        return

    if args.memory_cmd == "prune":
        result = memory.store.prune_expired()
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if args.memory_cmd == "extract-daily":
        result = memory.extract_daily(days=args.days, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if args.memory_cmd == "checkpoint":
        print(memory.latest_checkpoint_summary(scope=args.scope))
        return


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

    m = sub.add_parser("memory")
    memory_sub = m.add_subparsers(dest="memory_cmd", required=True)

    memory_sub.add_parser("status")

    mq = memory_sub.add_parser("query")
    mq.add_argument("query")
    mq.add_argument("--limit", type=int, default=6)

    memory_sub.add_parser("prune")

    med = memory_sub.add_parser("extract-daily")
    med.add_argument("--days", type=int, default=14)
    med.add_argument("--dry-run", action="store_true")

    mc = memory_sub.add_parser("checkpoint")
    mc.add_argument("--scope", default=None)

    x = sub.add_parser("x-digest")
    x.add_argument("query", help="Topic, ticker, handle, or boolean query")
    x.add_argument("--hours", type=int, default=24, help="Lookback window in hours (1-168)")
    x.add_argument("--limit", type=int, default=50, help="X API max results (10-100)")

    xc = sub.add_parser("x-chart")
    xc_sub = xc.add_subparsers(dest="x_chart_cmd", required=True)

    xcr = xc_sub.add_parser("run-once")
    xcr.add_argument("--manual", action="store_true")
    xcr.add_argument("--dry-run", action="store_true")

    xc_sub.add_parser("status")

    xcl = xc_sub.add_parser("list-sources")
    xcl.add_argument("--limit", type=int, default=50)

    xca = xc_sub.add_parser("add-source")
    xca.add_argument("handle")
    xca.add_argument("--priority", type=float, default=1.0)

    args = parser.parse_args()

    if args.cmd == "diligence":
        path = run_diligence(args.ticker)
        print(f"created: {path}")
        return

    if args.cmd == "memory":
        _run_memory_command(args)
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
        return

    if args.cmd == "x-digest":
        result = build_x_digest(args.query, hours=args.hours, max_results=args.limit)
        print(f"query: {result.query}")
        print(f"window_hours: {result.hours}")
        print(f"posts_analyzed: {result.post_count}")
        if result.top_post_url:
            print(f"top_post: {result.top_post_url}")
        print(f"generated_at_utc: {result.generated_at_utc}")
        print(f"report: {result.output_path}")
        return

    if args.cmd == "x-chart":
        if args.x_chart_cmd == "run-once":
            print(json.dumps(run_chart_scout_once(manual=bool(args.manual), dry_run=bool(args.dry_run)), indent=2, sort_keys=True))
            return
        if args.x_chart_cmd == "status":
            print(json.dumps(x_chart_status(), indent=2, sort_keys=True))
            return
        if args.x_chart_cmd == "list-sources":
            print(json.dumps(list_x_chart_sources(limit=max(1, min(500, int(args.limit)))), indent=2, sort_keys=True))
            return
        if args.x_chart_cmd == "add-source":
            print(json.dumps(add_x_chart_source(args.handle, priority=float(args.priority)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

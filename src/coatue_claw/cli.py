from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from coatue_claw.valuation_chart import run_valuation_chart


def run_diligence(ticker: str) -> Path:
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    out_dir = Path("/opt/coatue-claw-data/artifacts/packets")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{ticker.upper()}-{ts}.md"
    out_file.write_text(
        f"# Diligence Packet: {ticker.upper()}\n\n"
        "## Summary\nTBD\n\n"
        "## Bull Case\nTBD\n\n"
        "## Bear Case\nTBD\n\n"
        "## Key Risks\nTBD\n\n"
        "## Peer Comparison\nTBD\n\n",
        encoding="utf-8",
    )
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

    args = parser.parse_args()

    if args.cmd == "diligence":
        path = run_diligence(args.ticker)
        print(f"created: {path}")
        return

    if args.cmd == "valuation-chart":
        result = run_valuation_chart(args.tickers)
        print(f"provider_requested: {result.provider_requested}")
        print(f"provider_used: {result.provider_used}")
        if result.provider_fallback_reason:
            print(f"provider_fallback_reason: {result.provider_fallback_reason}")
        print(f"metric_mode: {result.metric_mode}")
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

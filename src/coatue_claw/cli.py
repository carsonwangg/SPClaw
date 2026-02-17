from pathlib import Path
from datetime import datetime
import argparse

def run_diligence(ticker: str) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
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

def main():
    parser = argparse.ArgumentParser("coatue-claw")
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("diligence")
    d.add_argument("ticker")
    args = parser.parse_args()

    if args.cmd == "diligence":
        path = run_diligence(args.ticker)
        print(f"created: {path}")

if __name__ == "__main__":
    main()

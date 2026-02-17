from __future__ import annotations

import logging
import os
import re
from typing import Optional

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from coatue_claw.cli import run_diligence
from coatue_claw.valuation_chart import run_valuation_chart

load_dotenv("/opt/coatue-claw/.env.prod")

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

app = App(token=os.environ["SLACK_BOT_TOKEN"], signing_secret=os.environ["SLACK_SIGNING_SECRET"])


@app.use
def log_incoming_requests(body, next, logger):
    event_type = None
    if isinstance(body, dict):
        event = body.get("event")
        if isinstance(event, dict):
            event_type = event.get("type")
    logger.info("incoming slack request event_type=%s", event_type)
    return next()


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def _extract_diligence_ticker(text: str) -> Optional[str]:
    normalized = _strip_slack_mentions(text).lower()
    normalized = re.sub(r"[^a-z0-9$._-]+", " ", normalized)
    parts = normalized.split()

    if "diligence" not in parts:
        return None

    i = parts.index("diligence")
    if i + 1 >= len(parts):
        return None

    ticker = parts[i + 1].upper().lstrip("$").strip(".,;:!?)]}")
    return ticker or None


def _extract_chart_tickers(text: str) -> list[str]:
    stripped = _strip_slack_mentions(text)
    lower = stripped.lower()
    if "graph" not in lower and "chart" not in lower:
        return []

    if "ev" not in lower and "ntm" not in lower and "growth" not in lower:
        return []

    cleaned = re.sub(r"\b(graph|chart|valuation|ev|ntm|revenue|vs|yoy|growth)\b", " ", stripped, flags=re.IGNORECASE)
    candidates = re.findall(r"\$?[A-Za-z][A-Za-z.\-]{0,9}", cleaned)
    out = []
    seen = set()
    for c in candidates:
        t = c.upper().lstrip("$").strip(".,;:!?)]}")
        if t in {"AND", "WITH", "FROM", "THE", "LINE", "BEST", "FIT", "OF"}:
            continue
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _format_chart_summary(result) -> str:
    lines = []
    lines.append("*Valuation chart generated* (EV/NTM vs YoY growth)")
    lines.append(f"- Provider requested: `{result.provider_requested}`")
    lines.append(f"- Provider used: `{result.provider_used}`")
    if result.provider_fallback_reason:
        lines.append(f"- Fallback reason: `{result.provider_fallback_reason}`")
    lines.append(f"- Request time (UTC): `{result.request_received_at}`")
    lines.append(f"- Market data as-of: `{result.market_data_as_of}`")
    lines.append(f"- Estimates as-of: `{result.estimates_as_of}`")
    lines.append(f"- Included: `{result.included_count}` | Excluded: `{result.excluded_count}`")

    excluded = [p for p in result.points if not p.included]
    if excluded:
        lines.append("- Exclusions:")
        for p in excluded[:8]:
            lines.append(f"  - `{p.ticker}`: `{p.exclusion_reason}`")

    lines.append("- NTM method: Yahoo only exposes `0q/+1q/+1y`; this build imputes missing +2q/+3q and flags it in output artifacts.")
    return "\n".join(lines)


@app.event("app_mention")
def handle_mention(event, say):
    text = event.get("text") or ""
    channel = event.get("channel")
    thread_ts = event.get("thread_ts") or event.get("ts")
    logger.info("app_mention received channel=%s ts=%s text=%r", event.get("channel"), event.get("ts"), text)

    chart_tickers = _extract_chart_tickers(text)
    if chart_tickers:
        try:
            result = run_valuation_chart(chart_tickers)
        except Exception:
            logger.exception("Failed to build valuation chart for tickers=%s", chart_tickers)
            say(
                text="Failed to build valuation chart. Check bot logs for details.",
                thread_ts=thread_ts,
            )
            return

        summary = _format_chart_summary(result)
        say(text=summary, thread_ts=thread_ts)

        try:
            app.client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                file=str(result.chart_path),
                title="EV/NTM vs YoY Growth (with line of best fit)",
            )
            app.client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                file=str(result.csv_path),
                title="valuation-chart-data.csv",
            )
            app.client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                file=str(result.json_path),
                title="valuation-chart-data.json",
            )
        except Exception:
            logger.exception("Failed to upload valuation chart artifacts to Slack")
            say(
                text=(
                    "Chart generated but file upload failed. "
                    f"Chart: `{result.chart_path}` CSV: `{result.csv_path}` JSON: `{result.json_path}`"
                ),
                thread_ts=thread_ts,
            )
        return

    ticker = _extract_diligence_ticker(text)
    if not ticker:
        say(
            text=(
                "Usage:\n"
                "- `diligence TICKER`\n"
                "- `graph ev ntm growth SNOW,MDB,DDOG`"
            ),
            thread_ts=thread_ts,
        )
        return

    try:
        out = run_diligence(ticker)
    except Exception:
        logger.exception("Failed to build diligence packet for ticker=%s", ticker)
        say(
            text=f"Failed to build diligence packet for `{ticker}`. Check bot logs for details.",
            thread_ts=thread_ts,
        )
        return

    say(
        text=f"Diligence packet created for *{ticker}*: `{out}`",
        thread_ts=thread_ts,
    )


if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

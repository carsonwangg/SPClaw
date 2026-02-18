from __future__ import annotations

import logging
import os
import re
from typing import Optional

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from coatue_claw.chart_intent import parse_chart_intent
from coatue_claw.chart_metrics import METRIC_SPECS, metric_label
from coatue_claw.cli import run_diligence
from coatue_claw.valuation_chart import _format_readable_date, run_valuation_chart

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


def _format_chart_summary(result) -> str:
    lines = []
    lines.append("*Valuation chart generated*")
    lines.append(f"- X-axis: `{metric_label(result.x_metric)}`")
    lines.append(f"- Y-axis: `{metric_label(result.y_metric)}`")
    lines.append(f"- Provider requested: `{result.provider_requested}`")
    lines.append(f"- Provider used: `{result.provider_used}`")
    if result.provider_fallback_reason:
        lines.append(f"- Fallback reason: `{result.provider_fallback_reason}`")
    lines.append(f"- Request time (UTC): `{result.request_received_at}`")
    lines.append(f"- Market data as-of: `{_format_readable_date(result.market_data_as_of)}`")
    lines.append(f"- Fundamentals as-of: `{_format_readable_date(result.fundamentals_as_of)}`")
    lines.append(f"- Included: `{result.included_count}` | Excluded: `{result.excluded_count}`")

    excluded = [p for p in result.points if not p.included]
    if excluded:
        lines.append("- Exclusions:")
        for p in excluded[:8]:
            lines.append(f"  - `{p.ticker}`: `{p.exclusion_reason}`")

    lines.append("- LTM method: sum of last 4 reported quarterly revenues.")
    return "\n".join(lines)


@app.event("app_mention")
def handle_mention(event, say):
    text = event.get("text") or ""
    channel = event.get("channel")
    thread_ts = event.get("thread_ts") or event.get("ts")
    logger.info("app_mention received channel=%s ts=%s text=%r", event.get("channel"), event.get("ts"), text)

    chart_intent = parse_chart_intent(text)
    if chart_intent is not None:
        if not chart_intent.tickers:
            metric_examples = ", ".join(METRIC_SPECS.keys())
            say(
                text=(
                    "I detected a chart request but could not find tickers.\n"
                    "Please include explicit tickers, e.g. `SNOW,MDB,DDOG`.\n"
                    "Metric ids: "
                    f"`{metric_examples}`\n"
                    "Default behavior: YoY Revenue Growth on y-axis unless you specify otherwise."
                ),
                thread_ts=thread_ts,
            )
            return

        try:
            result = run_valuation_chart(
                chart_intent.tickers,
                x_metric=chart_intent.x_metric,
                y_metric=chart_intent.y_metric,
            )
        except Exception:
            logger.exception(
                "Failed to build valuation chart for tickers=%s x_metric=%s y_metric=%s",
                chart_intent.tickers,
                chart_intent.x_metric,
                chart_intent.y_metric,
            )
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
                title=f"{metric_label(result.y_metric)} vs {metric_label(result.x_metric)} (with line of best fit)",
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
            app.client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                file=str(result.raw_path),
                title="valuation-chart-provider-raw.json",
            )
        except Exception:
            logger.exception("Failed to upload valuation chart artifacts to Slack")
            say(
                text=(
                    "Chart generated but file upload failed. "
                    f"Chart: `{result.chart_path}` CSV: `{result.csv_path}` JSON: `{result.json_path}` RAW: `{result.raw_path}`"
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
                "- `graph ev ltm growth SNOW,MDB,DDOG`\n"
                "- natural language: `@Coatue Claw plot EV/Revenue multiples vs revenue growth for SNOW,MDB,DDOG`\n"
                "- default: YoY Revenue Growth is y-axis unless you specify axes"
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

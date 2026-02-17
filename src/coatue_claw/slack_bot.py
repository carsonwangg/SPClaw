import logging
import os
import re
from typing import Optional

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from coatue_claw.cli import run_diligence

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


def _extract_diligence_ticker(text: str) -> Optional[str]:
    # Slack mentions arrive as "<@U123> diligence SNOW", sometimes with punctuation/symbols.
    normalized = (text or "").lower()
    normalized = re.sub(r"<@[^>]+>", " ", normalized)
    normalized = re.sub(r"[^a-z0-9$._-]+", " ", normalized)
    parts = normalized.split()

    if "diligence" not in parts:
        return None

    i = parts.index("diligence")
    if i + 1 >= len(parts):
        return None

    ticker = parts[i + 1].upper().lstrip("$").strip(".,;:!?)]}")
    return ticker or None


@app.event("app_mention")
def handle_mention(event, say):
    text = event.get("text") or ""
    thread_ts = event.get("thread_ts") or event.get("ts")
    logger.info("app_mention received channel=%s ts=%s text=%r", event.get("channel"), event.get("ts"), text)

    ticker = _extract_diligence_ticker(text)
    if not ticker:
        say(
            text="Usage: `diligence TICKER` (example: `diligence SNOW`)",
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

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import re
from typing import Optional

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from coatue_claw.chart_intent import parse_chart_intent
from coatue_claw.chart_metrics import METRIC_SPECS, metric_label
from coatue_claw.chart_title_context import infer_chart_title_context
from coatue_claw.cli import run_diligence
from coatue_claw.online_universe import discover_online_tickers
from coatue_claw.universe_store import (
    add_to_universe,
    find_relevant_universe_name,
    list_universes,
    load_universe,
    parse_tickers,
    remove_from_universe,
    save_universe,
    universe_path,
)
from coatue_claw.valuation_chart import _format_readable_date, run_valuation_chart

load_dotenv("/opt/coatue-claw/.env.prod")

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

app = App(token=os.environ["SLACK_BOT_TOKEN"], signing_secret=os.environ["SLACK_SIGNING_SECRET"])


@dataclass
class PendingChartChoice:
    x_metric: str
    y_metric: str
    title_context: str | None
    query: str
    seed_tickers: list[str]
    suggested_universe: str | None


@dataclass
class PendingChartFeedback:
    tickers: list[str]
    x_metric: str
    y_metric: str
    title_context: str | None
    source_label: str | None


PENDING_CHART_CHOICES: dict[str, PendingChartChoice] = {}
PENDING_CHART_FEEDBACK: dict[str, PendingChartFeedback] = {}


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


def _build_chart_query(text: str) -> str:
    stripped = _strip_slack_mentions(text).lower()
    cleaned = re.sub(
        r"\b(plot|chart|graph|scatter|valuation|make|me|a|for|with|x|y|axis|vs|versus|against|ev|ltm|revenue|growth|multiple|multiples)\b",
        " ",
        stripped,
    )
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or "equity peers"


def _extract_universe_name(text: str) -> str | None:
    stripped = _strip_slack_mentions(text)
    m = re.search(r"(?:universe|database|csv)\s+([a-zA-Z0-9][a-zA-Z0-9 _-]{1,48})", stripped, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def _is_chart_peer_expansion_request(text: str, tickers: list[str]) -> bool:
    lower = _strip_slack_mentions(text).lower()
    if not tickers:
        return True
    if len(tickers) <= 1 and re.search(r"\b(other|relevant|peer|peers|comps|basket|universe)\b", lower):
        return True
    return False


def _parse_universe_choice(text: str, suggested_universe: str | None) -> tuple[str, str | None] | None:
    stripped = _strip_slack_mentions(text).strip()
    lower = stripped.lower()
    if re.search(r"\bonline\b|\bpull\b|\bweb\b", lower):
        return ("online", None)
    explicit = _extract_universe_name(stripped)
    if explicit:
        return ("universe", explicit)
    if suggested_universe and re.search(r"\b(use|saved|database|csv|yes)\b", lower):
        return ("universe", suggested_universe)
    return None


def _merge_unique_tickers(primary: list[str], secondary: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for ticker in primary + secondary:
        t = ticker.upper().strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _extract_feedback_changes(text: str) -> tuple[list[str], list[str]]:
    stripped = _strip_slack_mentions(text)
    include: list[str] = []
    exclude: list[str] = []
    for line in re.split(r"[\n;]+", stripped):
        lower = line.lower()
        if re.search(r"\b(exclude|remove|shouldn't include|should not include|drop)\b", lower):
            exclude.extend(parse_tickers(line))
            continue
        if re.search(r"\b(include|add|should include|keep)\b", lower):
            include.extend(parse_tickers(line))
            continue
    return include, exclude


def _format_chart_usage() -> str:
    return (
        "Usage:\n"
        "- `diligence TICKER`\n"
        "- `graph ev ltm growth SNOW,MDB,DDOG`\n"
        "- natural language: `@Coatue Claw plot EV/Revenue multiples vs revenue growth for SNOW,MDB,DDOG`\n"
        "- create universe: `@Coatue Claw create universe defense with PLTR,LMT,RTX,NOC,GD,LDOS`\n"
        "- list universes: `@Coatue Claw list universes`\n"
        "- default: YoY Revenue Growth is y-axis unless you specify axes"
    )


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


def _handle_universe_command(text: str, *, thread_ts: str, say) -> bool:
    stripped = _strip_slack_mentions(text).strip()
    lower = stripped.lower()

    if re.fullmatch(r"(list|show)\s+(all\s+)?universes", lower):
        names = list_universes()
        if not names:
            say(text="No saved universes yet. Create one with `create universe NAME with TICK1,TICK2`.", thread_ts=thread_ts)
            return True
        say(text="Saved universes:\n- " + "\n- ".join(f"`{n}`" for n in names), thread_ts=thread_ts)
        return True

    show_match = re.search(r"\b(show|view|open)\s+universe\s+([a-zA-Z0-9][a-zA-Z0-9 _-]{1,48})", stripped, re.IGNORECASE)
    if show_match:
        name = show_match.group(2).strip()
        tickers = load_universe(name)
        if not tickers:
            say(text=f"Universe `{name}` not found or empty.", thread_ts=thread_ts)
            return True
        say(
            text=(
                f"Universe `{name}` ({len(tickers)} tickers)\n"
                f"- CSV: `{universe_path(name)}`\n"
                f"- Tickers: `{','.join(tickers)}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    create_match = re.search(
        r"\b(create|make|build)\s+(?:a\s+)?(?:universe|list)\s+([a-zA-Z0-9][a-zA-Z0-9 _-]{1,48})(?:\s*(?:with|:)\s*(.+))?$",
        stripped,
        re.IGNORECASE,
    )
    if create_match:
        name = create_match.group(2).strip()
        ticker_text = create_match.group(3) or ""
        tickers = parse_tickers(ticker_text)
        if not tickers:
            say(
                text=f"Please include tickers to create `{name}`. Example: `create universe {name} with PLTR,LMT,RTX`",
                thread_ts=thread_ts,
            )
            return True
        path = save_universe(name, tickers, source="slack-manual")
        say(text=f"Created universe `{name}` with {len(tickers)} tickers.\nCSV: `{path}`", thread_ts=thread_ts)
        return True

    add_match = re.search(
        r"\b(add|include)\s+(.+?)\s+(?:to|into)\s+(?:the\s+)?(?:universe|list)\s+([a-zA-Z0-9][a-zA-Z0-9 _-]{1,48})",
        stripped,
        re.IGNORECASE,
    )
    if add_match:
        ticker_text = add_match.group(2)
        name = add_match.group(3).strip()
        tickers = parse_tickers(ticker_text)
        if not tickers:
            say(text=f"No valid tickers found to add to `{name}`.", thread_ts=thread_ts)
            return True
        path, added = add_to_universe(name, tickers, source="slack-manual")
        say(
            text=(
                f"Updated universe `{name}`.\n"
                f"- Added: `{','.join(added) if added else 'none'}`\n"
                f"- CSV: `{path}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    remove_match = re.search(
        r"\b(remove|exclude|drop)\s+(.+?)\s+from\s+(?:the\s+)?(?:universe|list)\s+([a-zA-Z0-9][a-zA-Z0-9 _-]{1,48})",
        stripped,
        re.IGNORECASE,
    )
    if remove_match:
        ticker_text = remove_match.group(2)
        name = remove_match.group(3).strip()
        tickers = parse_tickers(ticker_text)
        if not tickers:
            say(text=f"No valid tickers found to remove from `{name}`.", thread_ts=thread_ts)
            return True
        path, removed = remove_from_universe(name, tickers, source="slack-manual")
        say(
            text=(
                f"Updated universe `{name}`.\n"
                f"- Removed: `{','.join(removed) if removed else 'none'}`\n"
                f"- CSV: `{path}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    return False


def _run_chart_and_respond(
    *,
    say,
    channel: str,
    thread_ts: str,
    tickers: list[str],
    x_metric: str,
    y_metric: str,
    title_context: str | None,
    source_label: str | None,
) -> bool:
    effective_title_context = title_context or infer_chart_title_context("", source_label)
    try:
        result = run_valuation_chart(
            tickers,
            x_metric=x_metric,
            y_metric=y_metric,
            title_context=effective_title_context,
        )
    except Exception:
        logger.exception("Failed to build valuation chart for tickers=%s x_metric=%s y_metric=%s", tickers, x_metric, y_metric)
        say(text="Failed to build valuation chart. Check bot logs for details.", thread_ts=thread_ts)
        return False

    summary = _format_chart_summary(result)
    if source_label:
        summary = f"{summary}\n- Universe source: `{source_label}`"
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
        return False

    PENDING_CHART_FEEDBACK[thread_ts] = PendingChartFeedback(
        tickers=result.tickers,
        x_metric=result.x_metric,
        y_metric=result.y_metric,
        title_context=result.title_context,
        source_label=source_label,
    )
    say(
        text=(
            "Any adjustments to the stock screen or data you'd like me to double-check?\n"
            "Formatting tweaks too. Reply in-thread with updates like:\n"
            "- `@Coatue Claw include AVAV,HII`\n"
            "- `@Coatue Claw exclude GD`"
        ),
        thread_ts=thread_ts,
    )
    return True


@app.event("app_mention")
def handle_mention(event, say):
    text = event.get("text") or ""
    channel = event.get("channel")
    thread_ts = event.get("thread_ts") or event.get("ts")
    logger.info("app_mention received channel=%s ts=%s text=%r", event.get("channel"), event.get("ts"), text)

    if _handle_universe_command(text, thread_ts=thread_ts, say=say):
        return

    pending_choice = PENDING_CHART_CHOICES.get(thread_ts)
    if pending_choice is not None:
        choice = _parse_universe_choice(text, pending_choice.suggested_universe)
        if choice is not None:
            mode, selected_name = choice
            if mode == "online":
                discovered = discover_online_tickers(pending_choice.query, limit=8)
                merged = []
                seen = set()
                for ticker in pending_choice.seed_tickers + discovered:
                    t = ticker.upper()
                    if t not in seen:
                        seen.add(t)
                        merged.append(t)
                if len(merged) < 2:
                    say(
                        text=(
                            "Online discovery did not find enough tickers.\n"
                            "Reply with explicit tickers or use a saved universe, e.g. "
                            "`@Coatue Claw use universe defense`."
                        ),
                        thread_ts=thread_ts,
                    )
                    return
                PENDING_CHART_CHOICES.pop(thread_ts, None)
                _run_chart_and_respond(
                    say=say,
                    channel=channel,
                    thread_ts=thread_ts,
                    tickers=merged,
                    x_metric=pending_choice.x_metric,
                    y_metric=pending_choice.y_metric,
                    title_context=pending_choice.title_context,
                    source_label=f"online:{pending_choice.query}",
                )
                return

            use_name = selected_name or pending_choice.suggested_universe
            if not use_name:
                say(text="Please specify which universe to use, e.g. `use universe defense`.", thread_ts=thread_ts)
                return
            universe_tickers = load_universe(use_name)
            if len(universe_tickers) < 2:
                say(
                    text=(
                        f"Universe `{use_name}` is empty or too small.\n"
                        "Add tickers first, e.g. `add PLTR,LMT to universe defense`."
                    ),
                    thread_ts=thread_ts,
                )
                return
            PENDING_CHART_CHOICES.pop(thread_ts, None)
            _run_chart_and_respond(
                say=say,
                channel=channel,
                thread_ts=thread_ts,
                tickers=universe_tickers,
                x_metric=pending_choice.x_metric,
                y_metric=pending_choice.y_metric,
                title_context=pending_choice.title_context,
                source_label=f"universe:{use_name}",
            )
            return

    chart_intent = parse_chart_intent(text)
    if chart_intent is not None:
        if _is_chart_peer_expansion_request(text, chart_intent.tickers):
            suggested_universe = find_relevant_universe_name(text)
            title_context = infer_chart_title_context(text)
            query = _build_chart_query(text)

            if suggested_universe:
                universe_tickers = load_universe(suggested_universe)
                auto_tickers = _merge_unique_tickers(chart_intent.tickers, universe_tickers)
                if len(auto_tickers) >= 2:
                    _run_chart_and_respond(
                        say=say,
                        channel=channel,
                        thread_ts=thread_ts,
                        tickers=auto_tickers,
                        x_metric=chart_intent.x_metric,
                        y_metric=chart_intent.y_metric,
                        title_context=title_context,
                        source_label=f"universe:{suggested_universe}",
                    )
                    return

            discovered = discover_online_tickers(query, limit=8)
            auto_tickers = _merge_unique_tickers(chart_intent.tickers, discovered)
            if len(auto_tickers) >= 2:
                _run_chart_and_respond(
                    say=say,
                    channel=channel,
                    thread_ts=thread_ts,
                    tickers=auto_tickers,
                    x_metric=chart_intent.x_metric,
                    y_metric=chart_intent.y_metric,
                    title_context=title_context,
                    source_label=f"online:{query}",
                )
                return

            metric_examples = ", ".join(sorted(METRIC_SPECS.keys()))
            suggested_line = ""
            if suggested_universe:
                suggested_count = len(load_universe(suggested_universe))
                suggested_line = f"- Saved universe match: `{suggested_universe}` ({suggested_count} tickers)\n"
            PENDING_CHART_CHOICES[thread_ts] = PendingChartChoice(
                x_metric=chart_intent.x_metric,
                y_metric=chart_intent.y_metric,
                title_context=title_context,
                query=query,
                seed_tickers=chart_intent.tickers,
                suggested_universe=suggested_universe,
            )
            say(
                text=(
                    "I couldn’t confidently build a full ticker set from that prompt.\n"
                    "Please choose one of these:\n"
                    "- `@Coatue Claw online`\n"
                    "- `@Coatue Claw use universe NAME`\n"
                    f"{suggested_line}"
                    f"Metric ids: `{metric_examples}`\n"
                    "Default behavior: YoY Revenue Growth on y-axis unless you specify otherwise."
                ),
                thread_ts=thread_ts,
            )
            return

        _run_chart_and_respond(
            say=say,
            channel=channel,
            thread_ts=thread_ts,
            tickers=chart_intent.tickers,
            x_metric=chart_intent.x_metric,
            y_metric=chart_intent.y_metric,
            title_context=infer_chart_title_context(text),
            source_label="explicit_tickers",
        )
        return

    pending_feedback = PENDING_CHART_FEEDBACK.get(thread_ts)
    if pending_feedback is not None:
        include, exclude = _extract_feedback_changes(text)
        if include or exclude:
            current = pending_feedback.tickers
            include_set = {t.upper() for t in include}
            exclude_set = {t.upper() for t in exclude}
            next_tickers = [t for t in current if t not in exclude_set]
            for ticker in include:
                t = ticker.upper()
                if t not in next_tickers:
                    next_tickers.append(t)
            if len(next_tickers) < 2:
                say(text="Need at least 2 tickers after feedback changes. Please add more tickers.", thread_ts=thread_ts)
                return
            _run_chart_and_respond(
                say=say,
                channel=channel,
                thread_ts=thread_ts,
                tickers=next_tickers,
                x_metric=pending_feedback.x_metric,
                y_metric=pending_feedback.y_metric,
                title_context=pending_feedback.title_context,
                source_label=(pending_feedback.source_label or "feedback_update"),
            )
            return

    ticker = _extract_diligence_ticker(text)
    if not ticker:
        say(text=_format_chart_usage(), thread_ts=thread_ts)
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

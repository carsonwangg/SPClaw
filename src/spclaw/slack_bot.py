from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
import logging
import os
from pathlib import Path
import re
import threading
import time
from typing import Optional

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError

from spclaw.chart_intent import parse_chart_intent
from spclaw.chart_metrics import METRIC_SPECS, metric_label
from spclaw.chart_title_context import infer_chart_title_context
from spclaw.cli import run_diligence
from spclaw.dev_buzz import DevBuzzError
from spclaw.dev_buzz import add_keyword as dev_buzz_add_keyword
from spclaw.dev_buzz import add_source as dev_buzz_add_source
from spclaw.dev_buzz import collect as dev_buzz_collect
from spclaw.dev_buzz import drop as dev_buzz_drop
from spclaw.dev_buzz import explain as dev_buzz_explain
from spclaw.dev_buzz import format_shortlist as dev_buzz_format_shortlist
from spclaw.dev_buzz import pin as dev_buzz_pin
from spclaw.dev_buzz import publish as dev_buzz_publish
from spclaw.dev_buzz import remove_keyword as dev_buzz_remove_keyword
from spclaw.dev_buzz import remove_source as dev_buzz_remove_source
from spclaw.dev_buzz import shortlist as dev_buzz_shortlist
from spclaw.dev_buzz import status as dev_buzz_status
from spclaw.hf_analyst import HFAError, analyze_podcast_url as run_hfa_podcast
from spclaw.hf_analyst import analyze_thread as run_hfa_thread
from spclaw.hf_analyst import extract_youtube_urls
from spclaw.hf_analyst import format_hfa_slack_summary, hfa_status as hfa_status_lookup
from spclaw.hf_analyst import parse_hfa_control_instruction, parse_hfa_intent, record_dm_autorun, record_dm_podcast_autorun, should_run_dm_autorun, should_run_dm_podcast_autorun
from spclaw.memory_extraction import parse_memory_lookup_query
from spclaw.memory_runtime import MemoryRuntime
from spclaw.market_daily import MarketDailyError
from spclaw.market_daily import debug_catalyst as market_daily_debug_catalyst
from spclaw.market_daily import holdings as market_daily_holdings
from spclaw.market_daily import refresh_coatue_holdings as market_daily_refresh_holdings
from spclaw.market_daily import run_earnings_recap as run_market_daily_earnings_recap
from spclaw.market_daily import run_once as run_market_daily_once
from spclaw.market_daily import set_override as market_daily_set_override
from spclaw.market_daily import status as market_daily_status
from spclaw.online_universe import discover_online_tickers
from spclaw.runtime_settings import (
    PromotionError,
    RuntimeSettingsError,
    format_settings_summary,
    list_promotion_history,
    load_runtime_settings,
    promote_current_settings_to_main,
    undo_last_settings_promotion,
    update_runtime_setting,
)
from spclaw.spencer_change_log import (
    SpencerChangeLog,
    format_changes as format_spencer_changes,
    is_spencer_user,
    looks_like_change_request,
)
from spclaw.slack_channel_access import channels_to_join, parse_created_channel_id
from spclaw.slack_config_intent import parse_config_intent
from spclaw.slack_dev_buzz_intent import parse_dev_buzz_intent
from spclaw.slack_file_ingest import ingest_slack_files
from spclaw.slack_pipeline import (
    PipelineError,
    deploy_history,
    format_pipeline_result,
    pipeline_status,
    run_build_request,
    run_checks,
    run_deploy_latest,
    undo_last_deploy,
)
from spclaw.slack_pipeline_intent import parse_pipeline_intent
from spclaw.slack_routing import is_explicit_board_seat_command, is_explicit_hfa_command, should_default_route_message, should_route_message_event
from spclaw.slack_x_chart_intent import parse_x_chart_post_intent
from spclaw.slack_x_intent import parse_x_digest_intent
from spclaw.universe_store import (
    add_to_universe,
    find_relevant_universe_name,
    list_universes,
    load_universe,
    parse_tickers,
    remove_from_universe,
    save_universe,
    universe_path,
)
from spclaw.valuation_chart import _format_readable_date, run_valuation_chart
from spclaw.x_chart_daily import XChartError, add_source as add_x_chart_source
from spclaw.x_chart_daily import list_sources as list_x_chart_sources
from spclaw.x_chart_daily import run_chart_for_post_url
from spclaw.x_chart_daily import run_chart_scout_once
from spclaw.x_chart_daily import status as x_chart_status
from spclaw.x_digest import XDigestError, build_x_digest, format_x_digest_summary

load_dotenv("/opt/spclaw/.env.prod")

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
PIPELINE_LOCK = threading.Lock()
_MEMORY_RUNTIME: MemoryRuntime | None = None
_SPENCER_CHANGE_LOG: SpencerChangeLog | None = None


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

    keyword = None
    for candidate in ("diligence", "dilligence"):
        if candidate in parts:
            keyword = candidate
            break
    if keyword is None:
        return None

    i = parts.index(keyword)
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
        "- `hfa analyze [optional question]` / `analyze [optional question]`\n"
        "- `hfa podcast <youtube-url> [optional question]` / `podcast <youtube-url> [optional question]`\n"
        "- `quotes <youtube-url>` or `analyze <youtube-url>` for podcast quote mode\n"
        "- `hfa status` / `status`\n"
        "- `hfa control show|clear|mode freeform|instruction <text>`\n"
        "- `md now` / `md status` / `md holdings refresh`\n"
        "- `x digest <topic|ticker|handle> [last 24h] [limit 50]`\n"
        "- `x chart now` (run chart-scout winner now)\n"
        "- `x chart sources` / `x chart add @handle priority 1.2`\n"
        "- URL post: `x chart from https://x.com/<handle>/status/<id> title: <full sentence>`\n"
        "- `graph ev ltm growth SNOW,MDB,DDOG`\n"
        "- natural language: `plot EV/Revenue multiples vs revenue growth for SNOW,MDB,DDOG`\n"
        "- create universe: `create universe defense with PLTR,LMT,RTX,NOC,GD,LDOS`\n"
        "- list universes: `list universes`\n"
        "- settings: `show my settings` or `going forward look for 12 peers`\n"
        "- pipeline: `deploy latest` or `undo last deploy`\n"
        "- memory: `memory status` or `what is my daughter's birthday?`\n"
        "- file ingest: upload a file in Slack and I'll auto-sort it into the knowledge folders\n"
        "- routing: messages default to OpenClaw unless you @ another user\n"
        "- governance: `spencer changes`, `change requests`, `spencer changes memory`\n"
        "- default: YoY Revenue Growth is y-axis unless you specify axes"
    )


def _extract_event_files(event: dict) -> list[dict]:
    files = event.get("files")
    if isinstance(files, list):
        return [item for item in files if isinstance(item, dict)]
    message = event.get("message")
    if isinstance(message, dict):
        files = message.get("files")
        if isinstance(files, list):
            return [item for item in files if isinstance(item, dict)]
    return []


def _handle_file_ingest_event(
    *,
    event: dict,
    source_event: str,
    thread_ts: str | None,
    reply_in_thread: bool = True,
    say,
) -> None:
    files = _extract_event_files(event)
    if not files:
        return

    channel = event.get("channel")
    message = event.get("message") if isinstance(event.get("message"), dict) else {}
    user_id = event.get("user") or message.get("user")
    text = (event.get("text") or message.get("text") or "").strip()
    message_ts = event.get("ts") or event.get("event_ts") or message.get("ts")
    effective_thread_ts = None
    if reply_in_thread:
        effective_thread_ts = thread_ts or event.get("thread_ts") or message.get("thread_ts") or message_ts

    result = ingest_slack_files(
        files=files,
        channel=channel,
        user_id=user_id,
        message_ts=message_ts,
        message_text=text,
        source_event=source_event,
    )

    processed_count = int(result.get("processed_count") or 0)
    errors = list(result.get("errors") or [])
    if processed_count == 0 and not errors:
        return

    lines = [
        "File ingest:",
        f"- processed: `{processed_count}`",
        f"- skipped: `{len(result.get('skipped') or [])}`",
        f"- errors: `{len(errors)}`",
    ]

    processed = list(result.get("processed") or [])
    for item in processed[:5]:
        lines.append(
            f"- `{item['original_name']}` -> `{item['category']}`"
        )

    if errors:
        lines.append(f"- first_error: `{errors[0]}`")

    if effective_thread_ts:
        say(text="\n".join(lines), thread_ts=effective_thread_ts)
    else:
        say(text="\n".join(lines))


def _memory_runtime() -> MemoryRuntime | None:
    global _MEMORY_RUNTIME
    if _MEMORY_RUNTIME is not None:
        return _MEMORY_RUNTIME
    try:
        _MEMORY_RUNTIME = MemoryRuntime()
    except Exception:
        logger.exception("Failed to initialize memory runtime")
        _MEMORY_RUNTIME = None
    return _MEMORY_RUNTIME


def _spencer_change_log() -> SpencerChangeLog | None:
    global _SPENCER_CHANGE_LOG
    if _SPENCER_CHANGE_LOG is not None:
        return _SPENCER_CHANGE_LOG
    try:
        _SPENCER_CHANGE_LOG = SpencerChangeLog()
    except Exception:
        logger.exception("Failed to initialize spencer change log")
        _SPENCER_CHANGE_LOG = None
    return _SPENCER_CHANGE_LOG


def _is_dm_event(event: dict) -> bool:
    channel_type = str(event.get("channel_type") or "").strip().lower()
    if channel_type == "im":
        return True
    channel_id = str(event.get("channel") or "").strip()
    return channel_id.startswith("D")


def _handle_hfa_command(
    *,
    text: str,
    channel: str | None,
    thread_ts: str,
    user_id: str | None,
    say,
) -> bool:
    cleaned = _strip_slack_mentions(text).strip()
    control = re.search(r"^\s*hfa\s+control\s+(show|clear|mode|instruction)\b(.*)$", cleaned, re.IGNORECASE)
    if control:
        action = str(control.group(1) or "").strip().lower()
        tail = str(control.group(2) or "").strip()
        memory = _memory_runtime()
        if memory is None:
            say(text="HFA control failed: memory runtime unavailable.", thread_ts=thread_ts)
            return True
        if action == "show":
            payload = memory.get_hfa_output_control()
            mode = "freeform"
            instruction = str(payload.get("instruction") or "")
            lines = ["HFA output control:", f"- mode: `{mode}`"]
            lines.append(f"- instruction: `{instruction}`" if instruction else "- instruction: `<none>`")
            say(text="\n".join(lines), thread_ts=thread_ts)
            return True
        if action == "clear":
            expired = memory.clear_hfa_output_control()
            say(
                text=(
                    "HFA output control cleared.\n"
                    f"- mode_expired: `{expired.get('mode_expired', 0)}`\n"
                    f"- instruction_expired: `{expired.get('instruction_expired', 0)}`"
                ),
                thread_ts=thread_ts,
            )
            return True
        if action == "mode":
            mode = tail.lower()
            if mode != "freeform":
                say(text="HFA now uses a single mode: `freeform`.", thread_ts=thread_ts)
                return True
            try:
                memory.set_hfa_output_control(requested_by=user_id, mode=mode)
            except Exception as exc:
                say(text=f"HFA control failed: `{exc}`", thread_ts=thread_ts)
                return True
            say(text="HFA output mode is `freeform`.", thread_ts=thread_ts)
            return True
        if action == "instruction":
            if not tail:
                say(text="Usage: `hfa control instruction <text>`", thread_ts=thread_ts)
                return True
            try:
                memory.set_hfa_output_control(requested_by=user_id, instruction=tail)
            except Exception as exc:
                say(text=f"HFA control failed: `{exc}`", thread_ts=thread_ts)
                return True
            say(text="HFA output instruction updated.", thread_ts=thread_ts)
            return True

    kind, tail = parse_hfa_intent(text)
    if kind is None:
        return False

    if not channel:
        say(text="HFA command failed: missing Slack channel context.", thread_ts=thread_ts)
        return True

    if kind == "status":
        status = hfa_status_lookup(channel=channel, thread_ts=thread_ts, limit=5)
        runs = list(status.get("runs") or [])
        if not runs:
            say(text="No HFA runs recorded for this thread yet.", thread_ts=thread_ts)
            return True
        latest = runs[0]
        lines = [
            "HFA status:",
            f"- run_id: `{latest.get('run_id')}`",
            f"- run_kind: `{latest.get('run_kind')}`",
            f"- status: `{latest.get('status')}`",
            f"- created_at_utc: `{latest.get('created_at_utc')}`",
        ]
        artifact = str(latest.get("artifact_path") or "").strip()
        if artifact:
            lines.append(f"- artifact: `{artifact}`")
        say(text="\n".join(lines), thread_ts=thread_ts)
        return True

    if kind == "analyze":
        implicit_instruction = parse_hfa_control_instruction(text)
        if implicit_instruction:
            memory = _memory_runtime()
            if memory is None:
                say(text="HFA control failed: memory runtime unavailable.", thread_ts=thread_ts)
                return True
            try:
                memory.set_hfa_output_control(
                    requested_by=user_id,
                    instruction=implicit_instruction,
                    source="slack-hfa-implicit-control",
                )
            except Exception as exc:
                say(text=f"HFA control failed: `{exc}`", thread_ts=thread_ts)
                return True
            say(
                text=(
                    "Updated HFA output instruction from your message.\n"
                    "Next `hfa analyze` calls will follow it."
                ),
                thread_ts=thread_ts,
            )
            return True
        analyze_urls = extract_youtube_urls(tail or "")
        if analyze_urls:
            url = analyze_urls[0]
            question = (tail or "").replace(url, "").strip() or None
            try:
                result = run_hfa_podcast(
                    url=url,
                    question=question,
                    requested_by=user_id,
                    channel=channel,
                    thread_ts=thread_ts,
                    trigger_mode="slack_podcast_alias_from_analyze",
                    dry_run=False,
                    memory_runtime=_memory_runtime(),
                )
            except HFAError as exc:
                say(text=f"HFA podcast failed: `{exc}`", thread_ts=thread_ts)
                return True
            say(text=format_hfa_slack_summary(result), thread_ts=thread_ts)
            return True
        try:
            result = run_hfa_thread(
                channel=channel,
                thread_ts=thread_ts,
                question=tail,
                requested_by=user_id,
                trigger_mode="slack_command",
                dry_run=False,
                slack_client=app.client,
                memory_runtime=_memory_runtime(),
            )
        except HFAError as exc:
            say(text=f"HFA analyze failed: `{exc}`", thread_ts=thread_ts)
            return True
        say(text=format_hfa_slack_summary(result), thread_ts=thread_ts)
        return True

    if kind == "podcast":
        urls = extract_youtube_urls(tail or "")
        if (not urls) and channel:
            # Conversational commands like "hfa quotes for this podcast" may rely on
            # a prior YouTube URL in the same thread; resolve from thread history.
            try:
                messages = _thread_messages(slack_client=app.client, channel=channel, thread_ts=thread_ts)
            except Exception:
                messages = []
            blob = " ".join(str(item.get("text") or "") for item in messages if isinstance(item, dict))
            urls = extract_youtube_urls(blob)
        if not urls:
            say(text="HFA podcast command requires a valid YouTube URL.", thread_ts=thread_ts)
            return True
        url = urls[0]
        question = (tail or "").replace(url, "").strip() or None
        try:
            result = run_hfa_podcast(
                url=url,
                question=question,
                requested_by=user_id,
                channel=channel,
                thread_ts=thread_ts,
                trigger_mode="slack_podcast_command",
                dry_run=False,
                memory_runtime=_memory_runtime(),
            )
        except HFAError as exc:
            say(text=f"HFA podcast failed: `{exc}`", thread_ts=thread_ts)
            return True
        say(text=format_hfa_slack_summary(result), thread_ts=thread_ts)
        return True

    return False


def _handle_hfa_implicit_instruction_update(
    *,
    text: str,
    channel: str | None,
    thread_ts: str,
    user_id: str | None,
    event_ts: str | None,
    say,
) -> bool:
    instruction = parse_hfa_control_instruction(text)
    if not instruction:
        return False
    lower = _strip_slack_mentions(text).lower()
    in_hfa_context = bool(re.search(r"\bhfa\s+analyze\b", lower))
    if (not in_hfa_context) and channel:
        runs = list(hfa_status_lookup(channel=channel, thread_ts=thread_ts, limit=1).get("runs") or [])
        in_hfa_context = bool(runs)
    if not in_hfa_context:
        return False
    memory = _memory_runtime()
    if memory is None:
        say(text="HFA control failed: memory runtime unavailable.", thread_ts=thread_ts)
        return True
    try:
        memory.set_hfa_output_control(
            requested_by=user_id,
            instruction=instruction,
            source="slack-hfa-implicit-control",
            source_ts_utc=event_ts,
        )
    except Exception as exc:
        say(text=f"HFA control failed: `{exc}`", thread_ts=thread_ts)
        return True
    say(
        text=(
            "Got it. I saved this as the HFA output format instruction.\n"
            "It will apply to future `hfa analyze` runs."
        ),
        thread_ts=thread_ts,
    )
    return True


def _maybe_auto_run_hfa_dm(
    *,
    event: dict,
    thread_ts: str,
    channel: str | None,
    user_id: str | None,
    say,
) -> bool:
    if not _is_dm_event(event):
        return False
    if not channel or not user_id:
        return False
    files = _extract_event_files(event)
    file_ids = [str(item.get("id") or "").strip() for item in files if isinstance(item, dict) and str(item.get("id") or "").strip()]
    if not file_ids:
        return False
    if not should_run_dm_autorun(channel=channel, user_id=user_id, thread_ts=thread_ts, file_ids=file_ids):
        return False
    try:
        result = run_hfa_thread(
            channel=channel,
            thread_ts=thread_ts,
            question=None,
            requested_by=user_id,
            trigger_mode="dm_auto",
            dry_run=False,
            slack_client=app.client,
            memory_runtime=_memory_runtime(),
        )
    except HFAError as exc:
        say(text=f"HFA auto-run failed: `{exc}`", thread_ts=thread_ts)
        return True
    record_dm_autorun(channel=channel, user_id=user_id, thread_ts=thread_ts, file_ids=file_ids)
    say(
        text=f"{format_hfa_slack_summary(result)}\n- trigger_mode: `dm_auto`",
        thread_ts=thread_ts,
    )
    return True


def _maybe_auto_run_hfa_podcast_dm(
    *,
    event: dict,
    text: str,
    thread_ts: str,
    channel: str | None,
    user_id: str | None,
    say,
) -> bool:
    if not _is_dm_event(event):
        return False
    if not channel or not user_id:
        return False
    urls = extract_youtube_urls(text)
    if not urls:
        return False
    url = urls[0]
    if not should_run_dm_podcast_autorun(channel=channel, user_id=user_id, thread_ts=thread_ts, url=url):
        return False
    try:
        result = run_hfa_podcast(
            url=url,
            question=None,
            requested_by=user_id,
            channel=channel,
            thread_ts=thread_ts,
            trigger_mode="dm_podcast_auto",
            dry_run=False,
            memory_runtime=_memory_runtime(),
        )
    except HFAError as exc:
        say(text=f"HFA podcast auto-run failed: `{exc}`", thread_ts=thread_ts)
        return True
    record_dm_podcast_autorun(channel=channel, user_id=user_id, thread_ts=thread_ts, url=url)
    say(
        text=f"{format_hfa_slack_summary(result)}\n- trigger_mode: `dm_podcast_auto`",
        thread_ts=thread_ts,
    )
    return True


def _capture_spencer_change_request(
    *,
    user_id: str | None,
    channel: str | None,
    thread_ts: str | None,
    message_ts: str | None,
    text: str,
) -> int | None:
    if not is_spencer_user(user_id):
        return None
    if not looks_like_change_request(text):
        return None
    tracker = _spencer_change_log()
    if tracker is None or not user_id:
        return None
    try:
        return tracker.capture_request(
            user_id=user_id,
            channel=channel,
            thread_ts=thread_ts,
            message_ts=message_ts,
            text=text,
        )
    except Exception:
        logger.exception("Failed to capture spencer change request")
        return None


def _parse_git_memory_request_text(text: str) -> str | None:
    stripped = _strip_slack_mentions(text).strip()
    prefix = "git-memory:"
    if not stripped.lower().startswith(prefix):
        return None
    body = stripped[len(prefix):].strip()
    return body or ""


def _git_memory_source_ref(
    *,
    channel: str | None,
    thread_ts: str | None,
    message_ts: str | None,
    source_ts_utc: str | None,
) -> str:
    day_key = datetime.now(UTC).strftime("%Y-%m-%d")
    if source_ts_utc:
        try:
            day_key = datetime.fromtimestamp(float(source_ts_utc), tz=UTC).strftime("%Y-%m-%d")
        except Exception:
            pass
    channel_part = (channel or "unknown-channel").strip() or "unknown-channel"
    thread_part = (thread_ts or "no-thread-ts").strip() or "no-thread-ts"
    message_part = (message_ts or "no-message-ts").strip() or "no-message-ts"
    memory_file = f"/Users/spclaw/.openclaw/workspace/memory/{day_key}.md"
    return f"slack://{channel_part}/{thread_part}/{message_part} | memory:{memory_file}"


def _capture_git_memory_request(
    *,
    user_id: str | None,
    channel: str | None,
    thread_ts: str | None,
    message_ts: str | None,
    source_ts_utc: str | None,
    text: str,
    trigger_mode: str = "git_memory_prefix",
) -> int | None:
    tracker = _spencer_change_log()
    if tracker is None:
        return None
    source_ref = _git_memory_source_ref(
        channel=channel,
        thread_ts=thread_ts,
        message_ts=message_ts,
        source_ts_utc=source_ts_utc,
    )
    payload = text if text else "No details provided."
    try:
        return tracker.capture_request(
            user_id=(user_id or "unknown-user"),
            channel=channel,
            thread_ts=thread_ts,
            message_ts=message_ts,
            text=payload,
            request_kind="memory_git",
            trigger_mode=trigger_mode,
            source_ref=source_ref,
        )
    except Exception:
        logger.exception("Failed to capture git-memory request")
        return None


def _change_notify_user_ids() -> list[str]:
    raw = (os.environ.get("SPCLAW_CHANGE_NOTIFY_USER_IDS", "U0AGD28QSQG") or "").strip()
    ids = [item.strip() for item in raw.split(",") if item.strip()]
    return ids or ["U0AGD28QSQG"]


def _memory_md_path() -> Path:
    return Path(
        os.environ.get(
            "SPCLAW_CHANGE_MEMORY_MD_PATH",
            "/Users/spclaw/.openclaw/workspace/MEMORY.md",
        )
    )


def _append_change_to_memory_md(
    *,
    change_id: int,
    user_id: str | None,
    channel: str | None,
    text: str,
) -> str:
    path = _memory_md_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    requester = user_id or "unknown-user"
    channel_ref = channel or "unknown-channel"
    line = f"- {ts} [change #{change_id}] requester={requester} channel={channel_ref} request={text}\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
    return str(path)


def _dm_user(*, user_id: str, text: str) -> bool:
    try:
        dm = app.client.conversations_open(users=user_id)
        channel_id = str((dm.get("channel") or {}).get("id") or "").strip()
        if channel_id:
            app.client.chat_postMessage(channel=channel_id, text=text)
            return True
    except Exception:
        logger.exception("Failed to open DM for user_id=%s", user_id)
    try:
        app.client.chat_postMessage(channel=user_id, text=text)
        return True
    except Exception:
        logger.exception("Failed to send fallback DM for user_id=%s", user_id)
    return False


def _notify_change_capture(
    *,
    change_id: int,
    user_id: str | None,
    channel: str | None,
    text: str,
    memory_md_path: str | None,
    queue_path: str | None,
) -> None:
    requester = f"<@{user_id}>" if user_id else "unknown-user"
    channel_ref = f"<#{channel}>" if channel else "unknown-channel"
    summary = (
        "Bot behavior change request captured.\n"
        f"- queue_id: `#{change_id}`\n"
        f"- requester: {requester}\n"
        f"- channel: {channel_ref}\n"
        f"- requested_change: {text}\n"
        f"- A) memory.md: {'written' if memory_md_path else 'write_failed'}"
    )
    if memory_md_path:
        summary += f"\n  path: `{memory_md_path}`"
    summary += f"\n- B) git upload queue: {'refreshed' if queue_path else 'refresh_failed'}"
    if queue_path:
        summary += f"\n  queue_snapshot: `{queue_path}`"
    summary += "\n- next: queued for Codex reconciliation + commit linking."

    for notify_user in _change_notify_user_ids():
        _dm_user(user_id=notify_user, text=summary)


def _capture_and_notify_memory_git_change(
    *,
    user_id: str | None,
    channel: str | None,
    thread_ts: str | None,
    message_ts: str | None,
    source_ts_utc: str | None,
    request_text: str,
    trigger_mode: str,
) -> int | None:
    queued_id = _capture_git_memory_request(
        user_id=user_id,
        channel=channel,
        thread_ts=thread_ts,
        message_ts=message_ts,
        source_ts_utc=source_ts_utc,
        text=request_text,
        trigger_mode=trigger_mode,
    )
    if queued_id is None:
        return None

    tracker = _spencer_change_log()
    memory_path: str | None = None
    queue_path: str | None = None
    try:
        memory_path = _append_change_to_memory_md(
            change_id=queued_id,
            user_id=user_id,
            channel=channel,
            text=request_text,
        )
    except Exception:
        logger.exception("Failed to append change to memory.md change_id=%s", queued_id)

    try:
        if tracker is not None:
            export = tracker.export_memory_git_queue(limit=200)
            queue_path = str(export.get("queue_path") or "")
    except Exception:
        logger.exception("Failed to refresh memory-git queue snapshot after capture change_id=%s", queued_id)

    try:
        if tracker is not None:
            tracker.update_status(
                queued_id,
                status="captured",
                note=f"Captured via {trigger_mode}; memory+queue mirror attempted.",
            )
    except Exception:
        logger.exception("Failed to annotate memory-git capture note change_id=%s", queued_id)

    _notify_change_capture(
        change_id=queued_id,
        user_id=user_id,
        channel=channel,
        text=request_text,
        memory_md_path=memory_path,
        queue_path=(queue_path or None),
    )
    return queued_id


def _mark_spencer_change(change_id: int | None, *, status: str, note: str) -> None:
    if change_id is None:
        return
    tracker = _spencer_change_log()
    if tracker is None:
        return
    try:
        tracker.update_status(change_id, status=status, note=note)
    except Exception:
        logger.exception("Failed to update spencer change status change_id=%s", change_id)


def _handle_spencer_change_command(*, text: str, thread_ts: str, say) -> bool:
    stripped = _strip_slack_mentions(text).strip()
    lower = stripped.lower()
    is_command = (
        lower.startswith("spencer changes")
        or lower.startswith("change requests")
        or lower.startswith("tracked changes")
    )
    if not is_command:
        return False
    tracker = _spencer_change_log()
    if tracker is None:
        say(text="Change tracker is unavailable right now.", thread_ts=thread_ts)
        return True

    status: str | None = None
    request_kind: str | None = None
    if re.search(r"\b(open|pending)\b", lower):
        status = "captured"
    elif re.search(r"\b(implemented|done)\b", lower):
        status = "implemented"
    elif re.search(r"\b(blocked)\b", lower):
        status = "blocked"
    if re.search(r"\bmemory\b", lower):
        request_kind = "memory_git"

    limit = 20
    m = re.search(r"\blast\s+(\d{1,3})\b", lower)
    if m:
        try:
            limit = max(1, min(100, int(m.group(1))))
        except Exception:
            limit = 20

    rows = tracker.list_changes(limit=limit, status=status, request_kind=request_kind)
    title = "Tracked change requests (Spencer + Carson)"
    if request_kind == "memory_git":
        title = "Tracked git-memory reconciliation requests"
    if status:
        title = f"Tracked change requests ({status})"
        if request_kind == "memory_git":
            title = f"Tracked git-memory reconciliation requests ({status})"
    say(text=format_spencer_changes(rows, title=title), thread_ts=thread_ts)
    return True


def _is_settings_admin(user_id: str | None) -> bool:
    allowed_raw = os.environ.get("SLACK_CONFIG_ADMINS", "").strip()
    if not allowed_raw:
        return True
    if not user_id:
        return False
    allowed = {item.strip() for item in allowed_raw.split(",") if item.strip()}
    return user_id in allowed


def _is_pipeline_admin(user_id: str | None) -> bool:
    allowed_raw = os.environ.get("SLACK_PIPELINE_ADMINS", "").strip()
    if not allowed_raw:
        allowed_raw = os.environ.get("SLACK_CONFIG_ADMINS", "").strip()
    if not allowed_raw:
        return True
    if not user_id:
        return False
    allowed = {item.strip() for item in allowed_raw.split(",") if item.strip()}
    return user_id in allowed


def _auto_join_channel(channel_id: str) -> tuple[bool, str]:
    cid = str(channel_id or "").strip()
    if not cid:
        return False, "missing_channel_id"
    try:
        app.client.conversations_join(channel=cid)
        return True, "joined"
    except SlackApiError as exc:
        err = ""
        try:
            err = str(exc.response.get("error") or "")
        except Exception:
            err = ""
        if err in {"already_in_channel", "method_not_supported_for_channel_type", "is_archived"}:
            return True, err
        logger.warning("Failed to auto-join channel=%s error=%s", cid, err or str(exc))
        return False, (err or "slack_api_error")
    except Exception:
        logger.exception("Unexpected failure auto-joining channel=%s", cid)
        return False, "unexpected_error"


def _bootstrap_public_channel_access() -> None:
    enabled_raw = os.environ.get("SPCLAW_SLACK_AUTOJOIN_PUBLIC_CHANNELS", "1").strip().lower()
    enabled = enabled_raw not in {"0", "false", "no", "off"}
    if not enabled:
        logger.info("Slack public-channel auto-join disabled by SPCLAW_SLACK_AUTOJOIN_PUBLIC_CHANNELS=%s", enabled_raw)
        return

    cursor: str | None = None
    joined = 0
    checked = 0
    try:
        while True:
            kwargs: dict[str, object] = {
                "types": "public_channel",
                "exclude_archived": True,
                "limit": 200,
            }
            if cursor:
                kwargs["cursor"] = cursor
            response = app.client.conversations_list(**kwargs)
            channels = response.get("channels") if isinstance(response, dict) else None
            channel_rows = channels if isinstance(channels, list) else []
            to_join = channels_to_join(channel_rows)
            checked += len(channel_rows)
            for channel_id in to_join:
                ok, _ = _auto_join_channel(channel_id)
                if ok:
                    joined += 1
            meta = response.get("response_metadata") if isinstance(response, dict) else None
            cursor = str(meta.get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
            if not cursor:
                break
    except Exception:
        logger.exception("Failed during Slack public-channel auto-join bootstrap")
        return
    logger.info("Slack public-channel auto-join bootstrap complete checked=%s joined=%s", checked, joined)


def _friendly_metric_label(metric_id: str) -> str:
    return METRIC_SPECS[metric_id].label


def _format_promotion_history(limit: int = 5) -> str:
    entries = list_promotion_history(limit=limit)
    if not entries:
        return "No settings promotions have been recorded yet."
    lines = ["Recent settings promotions:"]
    for entry in reversed(entries):
        commit = entry.get("commit", "unknown")
        actor = entry.get("actor", "unknown")
        ts = entry.get("timestamp_utc", "unknown")
        reverted_by = entry.get("reverted_by")
        status = "reverted" if reverted_by else "active"
        lines.append(f"- `{commit[:10]}` by `{actor}` at `{ts}` ({status})")
    return "\n".join(lines)


def _handle_settings_command(*, text: str, user_id: str | None, thread_ts: str, say) -> bool:
    intent = parse_config_intent(text)
    if intent is None:
        return False

    if not _is_settings_admin(user_id):
        say(
            text="You are not authorized to change bot settings. Ask an admin to run this command.",
            thread_ts=thread_ts,
        )
        return True

    actor = user_id or "unknown"

    if intent.kind == "show":
        settings = load_runtime_settings()
        say(text=format_settings_summary(settings), thread_ts=thread_ts)
        return True

    if intent.kind == "set":
        assert intent.key is not None
        assert intent.value is not None
        try:
            settings, audit_path = update_runtime_setting(
                key=intent.key,
                value=intent.value,
                actor=actor,
                source_text=text,
            )
        except RuntimeSettingsError as exc:
            say(text=f"I couldn't apply that settings change: {exc}", thread_ts=thread_ts)
            return True

        if intent.key == "peer_discovery_limit":
            response = f"Done. Going forward I'll target `{settings.peer_discovery_limit}` peers by default."
        elif intent.key == "default_x_metric":
            response = f"Done. Going forward default x-axis is `{_friendly_metric_label(settings.default_x_metric)}`."
        elif intent.key == "default_y_metric":
            response = f"Done. Going forward default y-axis is `{_friendly_metric_label(settings.default_y_metric)}`."
        elif intent.key == "followup_prompt":
            response = f"Done. I updated the post-chart follow-up to: `{settings.followup_prompt}`"
        else:
            response = "Done. Settings updated."

        response = f"{response}\nAudit: `{audit_path}`"
        say(text=response, thread_ts=thread_ts)
        return True

    if intent.kind == "promote":
        try:
            result = promote_current_settings_to_main(actor=actor)
        except PromotionError as exc:
            say(text=f"Promotion failed: {exc}", thread_ts=thread_ts)
            return True
        say(
            text=(
                "Promoted current runtime settings to `main`.\n"
                f"- commit: `{result.commit}`\n"
                f"- defaults file: `{result.repo_defaults_path}`\n"
                f"- restart_ok: `{result.restart_ok}`\n"
                f"- slack_status_ok: `{result.status_ok}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if intent.kind == "undo_promotion":
        try:
            result = undo_last_settings_promotion(actor=actor)
        except PromotionError as exc:
            say(text=f"Undo failed: {exc}", thread_ts=thread_ts)
            return True
        say(
            text=(
                "Undid the last settings promotion.\n"
                f"- reverted promotion commit: `{result.reverted_target_commit}`\n"
                f"- new revert commit: `{result.revert_commit}`\n"
                f"- restart_ok: `{result.restart_ok}`\n"
                f"- slack_status_ok: `{result.status_ok}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if intent.kind == "history":
        say(text=_format_promotion_history(limit=5), thread_ts=thread_ts)
        return True

    if intent.kind == "help":
        say(
            text=(
                "I can update these settings in plain English:\n"
                "- peer count target (example: `going forward look for 12 peers`)\n"
                "- default x/y axis (example: `use market cap as the default x-axis`)\n"
                "- post-chart follow-up wording (example: `after each chart ask if we want ticker changes`)\n"
                "- promote settings to code (`promote current settings`)\n"
                "- undo last promotion (`undo last promotion`)"
            ),
            thread_ts=thread_ts,
        )
        return True

    return False


def _handle_memory_command(
    *,
    text: str,
    channel: str | None,
    user_id: str | None,
    event_ts: str | None,
    thread_ts: str,
    say,
) -> bool:
    memory = _memory_runtime()
    if memory is None:
        return False

    stripped = _strip_slack_mentions(text).strip()
    lower = stripped.lower()

    if lower.startswith("remember "):
        persisted = memory.ingest_message(
            channel=channel,
            user_id=user_id,
            text=stripped,
            source="slack-manual-memory",
            source_ts_utc=event_ts,
        )
        say(
            text=(
                f"Captured memory candidates: `{len(persisted)}`. "
                "Use `memory query <phrase>` to verify retrieval."
            ),
            thread_ts=thread_ts,
        )
        return True

    if re.fullmatch(r"(show )?memory( status)?", lower):
        stats = memory.stats()
        say(
            text=(
                "Memory status:\n"
                f"- facts_total: `{stats['facts_total']}`\n"
                f"- facts_by_tier: `{stats['facts_by_tier']}`\n"
                f"- checkpoints_total: `{stats['checkpoints_total']}`\n"
                f"- events_total: `{stats['events_total']}`\n"
                f"- semantic_enabled: `{stats['semantic_enabled']}` ({stats['semantic_reason']})"
            ),
            thread_ts=thread_ts,
        )
        return True

    if lower.startswith("memory prune"):
        if not _is_pipeline_admin(user_id):
            say(text="You are not authorized to prune memory.", thread_ts=thread_ts)
            return True
        result = memory.store.prune_expired()
        say(
            text=(
                "Memory prune completed:\n"
                f"- facts_deleted: `{result['facts_deleted']}`\n"
                f"- checkpoints_deleted: `{result['checkpoints_deleted']}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    extract_match = re.search(r"memory extract daily(?:\\s+days\\s+(\\d+))?", lower)
    if extract_match:
        if not _is_pipeline_admin(user_id):
            say(text="You are not authorized to run memory extraction.", thread_ts=thread_ts)
            return True
        days = int(extract_match.group(1) or "14")
        dry_run = "dry" in lower
        result = memory.extract_daily(days=days, dry_run=dry_run)
        say(
            text=(
                "Memory extract-daily completed:\n"
                f"- events_scanned: `{result['events_scanned']}`\n"
                f"- facts_extracted: `{result['facts_extracted']}`\n"
                f"- inserted: `{result['inserted']}`\n"
                f"- dry_run: `{result['dry_run']}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if lower.startswith("memory checkpoint"):
        say(text=memory.latest_checkpoint_summary(scope="pipeline"), thread_ts=thread_ts)
        return True

    lookup_query = parse_memory_lookup_query(stripped)
    if lookup_query:
        say(text=memory.format_retrieval(lookup_query, limit=6), thread_ts=thread_ts)
        return True

    return False


def _handle_pipeline_command(*, text: str, user_id: str | None, thread_ts: str, say) -> bool:
    intent = parse_pipeline_intent(text)
    if intent is None:
        return False

    if not _is_pipeline_admin(user_id):
        say(
            text="You are not authorized to run deploy pipeline commands. Ask an admin to run this command.",
            thread_ts=thread_ts,
        )
        return True

    if intent.kind == "help":
        say(
            text=(
                "Deploy pipeline commands:\n"
                "- `deploy latest`\n"
                "- `undo last deploy`\n"
                "- `run checks`\n"
                "- `show pipeline status`\n"
                "- `show deploy history`\n"
                "- `build: <request>`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if not PIPELINE_LOCK.acquire(blocking=False):
        say(text="A pipeline job is already running. Please retry in a minute.", thread_ts=thread_ts)
        return True

    actor = user_id or "unknown"
    try:
        if intent.kind == "deploy_latest":
            say(text="Starting deploy now: pulling latest, restarting, and checking Slack health.", thread_ts=thread_ts)
            result = run_deploy_latest(actor=actor)
            say(text=format_pipeline_result(result), thread_ts=thread_ts)
            return True

        if intent.kind == "undo_last_deploy":
            say(text="Starting undo now: reverting last deploy, pushing, restarting, and checking Slack health.", thread_ts=thread_ts)
            result = undo_last_deploy(actor=actor)
            say(text=format_pipeline_result(result), thread_ts=thread_ts)
            return True

        if intent.kind == "run_checks":
            say(text="Running checks now (`PYTHONPATH=src pytest -q`).", thread_ts=thread_ts)
            result = run_checks()
            say(text=format_pipeline_result(result), thread_ts=thread_ts)
            return True

        if intent.kind == "status":
            say(text=pipeline_status(), thread_ts=thread_ts)
            return True

        if intent.kind == "history":
            say(text=deploy_history(limit=8), thread_ts=thread_ts)
            return True

        if intent.kind == "build_request":
            request = (intent.request or "").strip()
            if not request:
                say(text="Please provide a build request, e.g. `build: add XYZ behavior`.", thread_ts=thread_ts)
                return True
            say(text=f"Starting build request: `{request}`", thread_ts=thread_ts)
            result = run_build_request(request=request, actor=actor)
            say(text=format_pipeline_result(result), thread_ts=thread_ts)
            return True
    except PipelineError as exc:
        say(text=f"Pipeline failed: {exc}", thread_ts=thread_ts)
        return True
    finally:
        PIPELINE_LOCK.release()

    return False


def _handle_x_digest_command(*, text: str, channel: str | None, thread_ts: str, say) -> bool:
    intent = parse_x_digest_intent(text)
    if intent is None:
        return False

    if intent.kind == "help":
        say(
            text=(
                "X digest commands:\n"
                "- `x digest SNOW`\n"
                "- `x digest @snowflakedb last 48h`\n"
                "- `x digest (snowflake OR databricks) ai data cloud last 24h limit 80`\n"
                "- `x status`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if intent.kind == "status":
        configured = any(
            bool(os.environ.get(key, "").strip())
            for key in ("SPCLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "SPCLAW_TWITTER_BEARER_TOKEN")
        )
        say(
            text=(
                "X digest status:\n"
                f"- bearer_token_configured: `{configured}`\n"
                f"- api_base: `{os.environ.get('SPCLAW_X_API_BASE', 'https://api.x.com')}`\n"
                f"- digest_dir: `{os.environ.get('SPCLAW_X_DIGEST_DIR', '/opt/spclaw-data/artifacts/x-digest')}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    assert intent.kind == "digest"
    query = intent.query or ""
    say(
        text=f"Running X digest for `{query}` (last `{intent.hours}h`, limit `{intent.limit}`)...",
        thread_ts=thread_ts,
    )
    try:
        result = build_x_digest(query, hours=intent.hours, max_results=intent.limit)
    except XDigestError as exc:
        say(text=f"X digest failed: {exc}", thread_ts=thread_ts)
        return True
    except Exception:
        logger.exception("Unexpected failure generating X digest for query=%r", query)
        say(text="X digest failed unexpectedly. Check bot logs for details.", thread_ts=thread_ts)
        return True

    _post_thread_message(
        say=say,
        channel=channel,
        thread_ts=thread_ts,
        text=format_x_digest_summary(result),
    )

    if channel:
        try:
            app.client.files_upload_v2(
                channel=channel,
                thread_ts=thread_ts,
                file=str(result.output_path),
                title=f"x-digest-{result.query}",
            )
        except Exception:
            logger.exception("Failed to upload X digest artifact to Slack")
            _post_thread_message(
                say=say,
                channel=channel,
                thread_ts=thread_ts,
                text=f"Digest saved locally: `{result.output_path}`",
            )
    return True


def _handle_dev_buzz_command(*, text: str, channel: str | None, thread_ts: str, say) -> bool:
    intent = parse_dev_buzz_intent(text)
    if intent is None:
        return False

    try:
        if intent.kind == "help":
            say(
                text=(
                    "Dev Buzz commands:\n"
                    "- `dev buzz status`\n"
                    "- `dev buzz shortlist`\n"
                    "- `dev buzz collect now`\n"
                    "- `dev buzz dry run`\n"
                    "- `dev buzz publish now`\n"
                    "- `dev buzz add source @handle` / `dev buzz remove source @handle`\n"
                    "- `dev buzz add keyword <query>` / `dev buzz remove keyword <query>`\n"
                    "- `dev buzz pin <item_id>` / `dev buzz drop <item_id>`\n"
                    "- `dev buzz explain <item_id>`"
                ),
                thread_ts=thread_ts,
            )
            return True
        if intent.kind == "status":
            payload = dev_buzz_status()
            say(
                text=(
                    "Dev Buzz status:\n"
                    f"- items: `{payload['items']}` observations: `{payload['observations']}` shortlist: `{payload['shortlist_items']}`\n"
                    f"- sources: `{payload['active_sources']}` keywords: `{payload['active_keywords']}`\n"
                    f"- schedule: daily `{payload['daily_collect_time']}` PT, Friday `{payload['weekly_publish_time']}` PT\n"
                    f"- channel: `{payload['slack_channel']}` model: `{payload['model']}`\n"
                    f"- db: `{payload['db_path']}`"
                ),
                thread_ts=thread_ts,
            )
            return True
        if intent.kind == "shortlist":
            payload = dev_buzz_shortlist(limit=10)
            say(text=dev_buzz_format_shortlist(payload.get("items", [])), thread_ts=thread_ts)
            return True
        if intent.kind == "collect":
            say(text="Running Dev Buzz collection + LLM editor pass now...", thread_ts=thread_ts)
            payload = dev_buzz_collect(manual=True)
            say(
                text=(
                    "Dev Buzz collection complete:\n"
                    f"- queries: `{payload['queries']}`\n"
                    f"- observations inserted: `{payload['observations_inserted']}`\n"
                    f"- editor mode: `{payload['editor_mode']}`\n"
                    f"- snapshot: `{payload['snapshot_id']}`"
                ),
                thread_ts=thread_ts,
            )
            return True
        if intent.kind == "publish_dry_run":
            payload = dev_buzz_publish(dry_run=True, force=True, channel_override=channel)
            say(
                text=(
                    "Dev Buzz dry-run generated:\n"
                    f"- artifact: `{payload['artifact_path']}`\n\n"
                    f"{payload.get('preview') or ''}"
                ),
                thread_ts=thread_ts,
            )
            return True
        if intent.kind == "publish_force":
            payload = dev_buzz_publish(dry_run=False, force=True, channel_override=channel)
            say(
                text=(
                    "Dev Buzz published:\n"
                    f"- channel: `{payload['channel']}`\n"
                    f"- artifact: `{payload['artifact_path']}`"
                ),
                thread_ts=thread_ts,
            )
            return True
        if intent.kind == "add_source":
            payload = dev_buzz_add_source(intent.value or "")
            say(text=f"Dev Buzz source added: `@{payload['handle']}`", thread_ts=thread_ts)
            return True
        if intent.kind == "remove_source":
            payload = dev_buzz_remove_source(intent.value or "")
            say(text=f"Dev Buzz source removed: `@{payload['handle']}`", thread_ts=thread_ts)
            return True
        if intent.kind == "add_keyword":
            payload = dev_buzz_add_keyword(intent.value or "")
            say(text=f"Dev Buzz keyword added: `{payload['keyword']}`", thread_ts=thread_ts)
            return True
        if intent.kind == "remove_keyword":
            payload = dev_buzz_remove_keyword(intent.value or "")
            say(text=f"Dev Buzz keyword removed: `{payload['keyword']}`", thread_ts=thread_ts)
            return True
        if intent.kind == "pin":
            payload = dev_buzz_pin(intent.value or "")
            say(text=f"Dev Buzz item pinned: `{payload['item_id']}`", thread_ts=thread_ts)
            return True
        if intent.kind == "drop":
            payload = dev_buzz_drop(intent.value or "")
            say(text=f"Dev Buzz item dropped for this week: `{payload['item_id']}`", thread_ts=thread_ts)
            return True
        if intent.kind == "explain":
            payload = dev_buzz_explain(intent.value or "")
            item = payload["item"]
            headline = item.get("headline") or item.get("top_text") or item.get("item_id")
            say(
                text=(
                    f"*{headline}*\n"
                    f"- item_id: `{item.get('item_id')}`\n"
                    f"- rank: `{item.get('editorial_rank') or '-'}` confidence: `{item.get('confidence') or '-'}`\n"
                    f"- why: {item.get('why_matters') or 'n/a'}\n"
                    f"- rationale: {item.get('rationale') or 'n/a'}\n"
                    f"- source: {item.get('top_x_url') or 'n/a'}"
                ),
                thread_ts=thread_ts,
            )
            return True
    except DevBuzzError as exc:
        say(text=f"Dev Buzz failed: {exc}", thread_ts=thread_ts)
        return True
    except Exception:
        logger.exception("Unexpected Dev Buzz command failure")
        say(text="Dev Buzz failed unexpectedly. Check bot logs for details.", thread_ts=thread_ts)
        return True

    return False


def _handle_market_daily_command(*, text: str, channel: str | None, thread_ts: str, say) -> bool:
    stripped = _strip_slack_mentions(text).strip()
    lower = stripped.lower()
    if not re.match(r"^md\b", lower):
        return False

    if re.fullmatch(r"md(\s+help)?", lower):
        say(
            text=(
                "MD commands:\n"
                "- `md now`\n"
                "- `md now force`\n"
                "- `md earnings now`\n"
                "- `md earnings now force`\n"
                "- `md status`\n"
                "- `md holdings refresh`\n"
                "- `md holdings show`\n"
                "- `md include TICKER`\n"
                "- `md exclude TICKER`\n"
                "- `md debug TICKER [open|close]`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if re.fullmatch(r"md\s+status", lower):
        try:
            payload = market_daily_status()
        except Exception as exc:
            say(text=f"MD status failed: {exc}", thread_ts=thread_ts)
            return True
        recent = payload.get("recent_runs") or []
        lines = [
            "MD status:",
            f"- timezone: `{payload.get('timezone')}`",
            f"- times: `{payload.get('times')}`",
            f"- channel: `{payload.get('channel')}`",
            f"- earnings_recap_time: `{payload.get('earnings_recap_time', '19:00')}`",
            f"- top_n: `{payload.get('top_n')}`",
            f"- top_k: `{payload.get('top_k')}`",
            f"- holdings_count: `{payload.get('holdings_count')}`",
            f"- overrides: `{len(payload.get('overrides') or [])}`",
            f"- recent_runs: `{len(recent)}`",
        ]
        if recent:
            latest = recent[0]
            lines.append(
                f"- latest: `{latest.get('run_date_local')}` `{latest.get('slot_name')}` `{latest.get('status')}`"
            )
        say(text="\n".join(lines), thread_ts=thread_ts)
        return True

    if re.fullmatch(r"md\s+holdings\s+refresh", lower):
        say(text="Refreshing Coatue holdings from latest 13F...", thread_ts=thread_ts)
        try:
            payload = market_daily_refresh_holdings()
        except Exception as exc:
            say(text=f"MD holdings refresh failed: {exc}", thread_ts=thread_ts)
            return True
        say(
            text=(
                "MD holdings refresh result:\n"
                f"- updated: `{payload.get('updated')}`\n"
                f"- reason: `{payload.get('reason', 'n/a')}`\n"
                f"- rows: `{payload.get('rows', 0)}`\n"
                f"- resolved_rows: `{payload.get('resolved_rows', 0)}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if re.fullmatch(r"md\s+holdings(\s+show)?", lower):
        try:
            payload = market_daily_holdings()
        except Exception as exc:
            say(text=f"MD holdings lookup failed: {exc}", thread_ts=thread_ts)
            return True
        tickers = payload.get("tickers") or []
        preview = ",".join(tickers[:40]) if tickers else "none"
        say(
            text=(
                "MD holdings:\n"
                f"- count: `{payload.get('count', 0)}`\n"
                f"- last_updated_utc: `{payload.get('last_updated_utc', 'n/a')}`\n"
                f"- tickers: `{preview}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    debug_match = re.fullmatch(r"md\s+debug\s+([A-Za-z.$-]{1,12})(?:\s+(open|close))?", stripped, flags=re.IGNORECASE)
    if debug_match:
        ticker = debug_match.group(1)
        slot = (debug_match.group(2) or "open").lower()
        try:
            payload = market_daily_debug_catalyst(ticker=ticker, slot_name=slot)
        except Exception as exc:
            say(text=f"MD debug failed: {exc}", thread_ts=thread_ts)
            return True
        lines = [
            f"MD debug `{payload.get('ticker')}` (`{payload.get('slot')}`):",
            f"- confidence: `{float(payload.get('confidence') or 0.0):.2f}`",
            f"- chosen_source: `{payload.get('chosen_source') or 'none'}`",
            f"- line: {payload.get('line')}",
        ]
        links = payload.get("links") if isinstance(payload.get("links"), dict) else {}
        if links:
            if links.get("news"):
                lines.append(f"- news: {links['news']}")
            if links.get("web"):
                lines.append(f"- web: {links['web']}")
        top = payload.get("top_evidence") if isinstance(payload.get("top_evidence"), list) else []
        for entry in top[:3]:
            lines.append(f"- evidence: {entry}")
        rejected = payload.get("rejected_reasons") if isinstance(payload.get("rejected_reasons"), list) else []
        if rejected:
            lines.append(f"- rejected: {', '.join(str(x) for x in rejected)}")
        say(text="\n".join(lines), thread_ts=thread_ts)
        return True

    include_match = re.fullmatch(r"md\s+include\s+([A-Za-z.$-]{1,12})", stripped, flags=re.IGNORECASE)
    if include_match:
        ticker = include_match.group(1)
        try:
            payload = market_daily_set_override(ticker=ticker, action="include", updated_by="slack")
        except MarketDailyError as exc:
            say(text=f"MD include failed: {exc}", thread_ts=thread_ts)
            return True
        say(
            text=(
                f"Included `{payload.get('ticker')}` in MD universe overrides.\n"
                f"- overrides_count: `{len(payload.get('overrides') or [])}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    exclude_match = re.fullmatch(r"md\s+exclude\s+([A-Za-z.$-]{1,12})", stripped, flags=re.IGNORECASE)
    if exclude_match:
        ticker = exclude_match.group(1)
        try:
            payload = market_daily_set_override(ticker=ticker, action="exclude", updated_by="slack")
        except MarketDailyError as exc:
            say(text=f"MD exclude failed: {exc}", thread_ts=thread_ts)
            return True
        say(
            text=(
                f"Excluded `{payload.get('ticker')}` from MD universe overrides.\n"
                f"- overrides_count: `{len(payload.get('overrides') or [])}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    earnings_now_match = re.fullmatch(r"md\s+earnings\s+now(\s+force)?", lower)
    if earnings_now_match:
        forced = "force" in lower
        say(text=f"Running MD earnings recap now (force={forced})...", thread_ts=thread_ts)
        try:
            result = run_market_daily_earnings_recap(
                manual=True,
                force=forced,
                dry_run=False,
                channel_override=None,
            )
        except MarketDailyError as exc:
            say(text=f"MD earnings recap failed: {exc}", thread_ts=thread_ts)
            return True
        except Exception:
            logger.exception("Unexpected MD earnings recap failure")
            say(text="MD earnings recap failed unexpectedly. Check logs.", thread_ts=thread_ts)
            return True

        if result.get("posted"):
            say(
                text=(
                    "MD earnings recap posted.\n"
                    f"- run_id: `{result.get('run_id')}`\n"
                    f"- reporters: `{result.get('reporters', 0)}`\n"
                    f"- artifact: `{result.get('artifact_path')}`"
                ),
                thread_ts=thread_ts,
            )
            return True
        say(
            text=(
                "MD earnings recap did not post.\n"
                f"- reason: `{result.get('reason', result.get('status', 'unknown'))}`\n"
                f"- slot: `{result.get('slot', 'earnings_recap')}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    now_match = re.fullmatch(r"md\s+now(\s+force)?", lower)
    if now_match:
        forced = "force" in lower
        say(text=f"Running MD now (manual slot, force={forced})...", thread_ts=thread_ts)
        try:
            result = run_market_daily_once(
                manual=True,
                force=forced,
                dry_run=False,
                channel_override=None,
            )
        except MarketDailyError as exc:
            say(text=f"MD run failed: {exc}", thread_ts=thread_ts)
            return True
        except Exception:
            logger.exception("Unexpected MD run failure")
            say(text="MD run failed unexpectedly. Check logs.", thread_ts=thread_ts)
            return True

        if result.get("posted"):
            movers = result.get("movers") or []
            top = movers[0] if movers else {}
            say(
                text=(
                    "MD posted.\n"
                    f"- slot: `{result.get('slot')}`\n"
                    f"- run_id: `{result.get('run_id')}`\n"
                    f"- top_mover: `{top.get('ticker', 'n/a')}` `{top.get('pct_move', 'n/a')}`\n"
                    f"- artifact: `{result.get('artifact_path')}`"
                ),
                thread_ts=thread_ts,
            )
            return True
        say(
            text=(
                "MD did not post.\n"
                f"- reason: `{result.get('reason', result.get('status', 'unknown'))}`\n"
                f"- slot: `{result.get('slot', 'n/a')}`"
            ),
            thread_ts=thread_ts,
        )
        return True

    say(text="Try `md help` for market-daily commands.", thread_ts=thread_ts)
    return True


def _handle_x_chart_command(*, text: str, channel: str | None, thread_ts: str, say) -> bool:
    stripped = _strip_slack_mentions(text).strip()
    lower = stripped.lower()
    if not re.search(r"\bx\s+chart\b", lower):
        return False

    if re.search(r"\bx\s+chart\s+help\b", lower):
        say(
            text=(
                "X chart scout commands:\n"
                "- `x chart now`\n"
                "- `x chart status`\n"
                "- `x chart sources`\n"
                "- `x chart add @fiscal_AI priority 1.6`\n"
                "- `x chart from https://x.com/<handle>/status/<id> title: <full sentence>`"
            ),
            thread_ts=thread_ts,
        )
        return True

    if re.search(r"\bx\s+chart\s+status\b", lower):
        try:
            s = x_chart_status()
        except Exception as exc:
            say(text=f"X chart status failed: {exc}", thread_ts=thread_ts)
            return True
        recent = s.get("recent_posts") or []
        lines = [
            "X chart scout status:",
            f"- render_mode: `{s.get('render_mode')}`",
            f"- timezone: `{s.get('timezone')}`",
            f"- windows: `{s.get('windows')}`",
            f"- slack_channel: `{s.get('slack_channel')}`",
            f"- sources_count: `{s.get('sources_count')}`",
            f"- recent_posts: `{len(recent)}`",
        ]
        if recent:
            latest = recent[0]
            lines.append(f"- latest: `{latest.get('slot_key')}` {latest.get('url')}")
        say(text="\n".join(lines), thread_ts=thread_ts)
        return True

    if re.search(r"\bx\s+chart\s+sources\b", lower):
        try:
            payload = list_x_chart_sources(limit=30)
        except Exception as exc:
            say(text=f"Failed to list X chart sources: {exc}", thread_ts=thread_ts)
            return True
        sources = payload.get("sources") or []
        if not sources:
            say(text="No X chart sources configured.", thread_ts=thread_ts)
            return True
        lines = ["Top X chart sources:"]
        for item in sources[:15]:
            lines.append(
                f"- `@{item.get('handle')}` priority `{float(item.get('priority') or 0):.2f}` trust `{float(item.get('trust_score') or 0):.2f}`"
            )
        say(text="\n".join(lines), thread_ts=thread_ts)
        return True

    add_match = re.search(r"\bx\s+chart\s+add\s+@?([A-Za-z0-9_]+)(?:\s+priority\s+([0-9]*\.?[0-9]+))?", stripped, re.IGNORECASE)
    if add_match:
        handle = add_match.group(1)
        priority = float(add_match.group(2) or "1.0")
        try:
            result = add_x_chart_source(handle, priority=priority)
        except XChartError as exc:
            say(text=f"Could not add source: {exc}", thread_ts=thread_ts)
            return True
        say(
            text=f"Added X chart source `@{result['handle']}` with priority `{result['priority']:.2f}`.",
            thread_ts=thread_ts,
        )
        return True

    if re.search(r"\bx\s+chart\s+(now|run)\b", lower):
        try:
            result = run_chart_scout_once(manual=True, dry_run=False, channel_override=channel)
        except XChartError as exc:
            say(text=f"X chart run failed: {exc}", thread_ts=thread_ts)
            return True
        except Exception:
            logger.exception("Unexpected x-chart failure")
            say(text="X chart run failed unexpectedly. Check logs.", thread_ts=thread_ts)
            return True

        if result.get("posted"):
            winner = result.get("winner") or {}
            say(
                text=(
                    "Posted chart scout winner.\n"
                    f"- slot: `{result.get('slot_key')}`\n"
                    f"- source: `{winner.get('source')}`\n"
                    f"- url: {winner.get('url')}"
                ),
                thread_ts=thread_ts,
            )
            return True
        say(
            text=f"X chart run completed with no post (`{result.get('reason', 'unknown')}`).",
            thread_ts=thread_ts,
        )
        return True

    say(text="Try `x chart help` for available commands.", thread_ts=thread_ts)
    return True


def _handle_x_post_compound_command(*, text: str, channel: str | None, thread_ts: str, say) -> bool:
    intent = parse_x_chart_post_intent(text)
    if intent is None:
        return False

    lines: list[str] = ["Handled your X post request:"]

    if intent.add_source:
        priority = float(intent.priority or 1.0)
        try:
            add_result = add_x_chart_source(intent.handle, priority=priority)
            lines.append(f"- Added `@{add_result['handle']}` to X scout sources (priority `{add_result['priority']:.2f}`).")
        except XChartError as exc:
            lines.append(f"- Source add failed for `@{intent.handle}`: `{exc}`")

    if intent.run_chart:
        try:
            result = run_chart_for_post_url(
                post_url=intent.post_url,
                channel_override=channel,
                title_override=intent.title_override,
            )
            winner = result.get("winner") or {}
            lines.append(
                "- Posted Coatue-style chart from linked post:\n"
                f"  - source: `{winner.get('source')}`\n"
                f"  - url: {winner.get('url')}"
            )
            if intent.title_override:
                lines.append(f"- Title override applied: `{intent.title_override}`")
        except XChartError as exc:
            lines.append(f"- Chart generation failed: `{exc}`")
        except Exception:
            logger.exception("Unexpected failure for compound X post command")
            lines.append("- Chart generation failed unexpectedly. Check logs.")

    if len(lines) == 1:
        return False
    say(text="\n".join(lines), thread_ts=thread_ts)
    return True


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


def _post_thread_message(
    *,
    say,
    channel: str | None,
    thread_ts: str,
    text: str,
) -> None:
    if channel:
        for attempt in range(1, 4):
            try:
                app.client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text=text,
                )
                return
            except SlackApiError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 429 and attempt < 3:
                    retry_after_raw = "1"
                    if exc.response is not None:
                        retry_after_raw = exc.response.headers.get("Retry-After", "1")
                    retry_after = max(1, int(retry_after_raw))
                    logger.warning(
                        "Rate limited posting thread message (attempt=%s), sleeping %ss",
                        attempt,
                        retry_after,
                    )
                    time.sleep(retry_after)
                    continue
                logger.exception("chat_postMessage failed for thread=%s", thread_ts)
                break
            except Exception:
                logger.exception("chat_postMessage failed for thread=%s", thread_ts)
                break

    try:
        say(text=text, thread_ts=thread_ts)
    except Exception:
        logger.exception("Failed to post thread message via both chat_postMessage and say thread=%s", thread_ts)


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
    followup_prompt: str,
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
    _post_thread_message(
        say=say,
        channel=channel,
        thread_ts=thread_ts,
        text=summary,
    )

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
        _post_thread_message(
            say=say,
            channel=channel,
            thread_ts=thread_ts,
            text=(
                "Chart generated but file upload failed. "
                f"Chart: `{result.chart_path}` CSV: `{result.csv_path}` JSON: `{result.json_path}` RAW: `{result.raw_path}`"
            ),
        )
        return False

    PENDING_CHART_FEEDBACK[thread_ts] = PendingChartFeedback(
        tickers=result.tickers,
        x_metric=result.x_metric,
        y_metric=result.y_metric,
        title_context=result.title_context,
        source_label=source_label,
    )
    _post_thread_message(
        say=say,
        channel=channel,
        thread_ts=thread_ts,
        text=followup_prompt,
    )
    return True


@app.event("message")
def handle_message(event, say):
    subtype = str(event.get("subtype") or "")
    if subtype in {"bot_message", "message_deleted"}:
        return
    if event.get("bot_id"):
        return
    text = event.get("text") or ""
    channel_type = str(event.get("channel_type") or "")
    if should_route_message_event(text=text, channel_type=channel_type):
        source_event = "slack-message-dm" if channel_type.lower() == "im" else "slack-message-default"
        _handle_slack_request_event(
            event=event,
            say=say,
            source_event=source_event,
            memory_source=source_event,
        )
        return
    # Not default-routed (typically because another user was @mentioned), but still ingest files.
    if _extract_event_files(event):
        _handle_file_ingest_event(
            event=event,
            source_event="slack-message",
            thread_ts=event.get("thread_ts") or event.get("ts"),
            say=say,
        )


@app.event("file_shared")
def handle_file_shared(event, say):
    file_id = str(event.get("file_id") or "").strip()
    if not file_id:
        return
    try:
        info = app.client.files_info(file=file_id)
        file_obj = info.get("file") if isinstance(info, dict) else None
    except SlackApiError:
        logger.exception("Failed to load Slack file info for file_id=%s", file_id)
        return
    if not isinstance(file_obj, dict):
        return

    enriched = dict(event)
    enriched["files"] = [file_obj]
    enriched["channel"] = event.get("channel_id")
    enriched["user"] = event.get("user_id")
    _handle_file_ingest_event(
        event=enriched,
        source_event="slack-file-shared",
        thread_ts=None,
        reply_in_thread=False,
        say=say,
    )


def _handle_slack_request_event(*, event, say, source_event: str, memory_source: str) -> None:
    text = event.get("text") or ""
    channel = event.get("channel")
    user_id = event.get("user")
    event_ts = event.get("ts")
    thread_ts = event.get("thread_ts") or event.get("ts")
    logger.info("%s received channel=%s ts=%s text=%r", source_event, event.get("channel"), event.get("ts"), text)

    _handle_file_ingest_event(
        event=event,
        source_event=source_event,
        thread_ts=thread_ts,
        say=say,
    )

    if not text.strip():
        _maybe_auto_run_hfa_dm(
            event=event,
            thread_ts=thread_ts,
            channel=channel,
            user_id=user_id,
            say=say,
        )
        return

    if _maybe_auto_run_hfa_podcast_dm(
        event=event,
        text=text,
        thread_ts=thread_ts,
        channel=channel,
        user_id=user_id,
        say=say,
    ):
        return

    # Fast-path HFA commands before change-request heuristics/conversational fallbacks.
    # This avoids ambiguous generic responses when users explicitly invoke HFA commands.
    hfa_command_requested = is_explicit_hfa_command(text)
    if _handle_hfa_command(text=text, channel=channel, thread_ts=thread_ts, user_id=user_id, say=say):
        return
    if hfa_command_requested:
        logger.error(
            "HFA command routing failed channel=%s thread_ts=%s text=%r",
            channel,
            thread_ts,
            text,
        )
        say(
            text=(
                "HFA command routing failed.\n"
                "I detected an HFA command but could not route it to the HFA handler.\n"
                "Try again with one of:\n"
                "- `hfa analyze`\n"
                "- `hfa podcast <youtube-url>`\n"
                "- `hfa status`"
            ),
            thread_ts=thread_ts,
        )
        return

    if _handle_hfa_implicit_instruction_update(
        text=text,
        channel=channel,
        thread_ts=thread_ts,
        user_id=user_id,
        event_ts=event_ts,
        say=say,
    ):
        return

    bs_command_requested = is_explicit_board_seat_command(text)
    if bs_command_requested:
        if _handle_board_seat_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
            return
        logger.error(
            "Board Seat command routing failed channel=%s thread_ts=%s text=%r",
            channel,
            thread_ts,
            text,
        )
        say(
            text=(
                "Board Seat command routing failed.\n"
                "Valid commands:\n"
                "- `bs now`\n"
                "- `bs now dry`\n"
                "- `bs now for <Company>`\n"
                "- `bs status`"
            ),
            thread_ts=thread_ts,
        )
        return

    git_memory_text = _parse_git_memory_request_text(text)
    if git_memory_text is not None:
        memory = _memory_runtime()
        if memory is not None:
            try:
                memory.ingest_message(
                    channel=channel,
                    user_id=user_id,
                    text=text,
                    source=memory_source,
                    source_ts_utc=event_ts,
                )
            except Exception:
                logger.exception("Failed to ingest git-memory message into runtime memory")
        queued_id = _capture_and_notify_memory_git_change(
            user_id=user_id,
            channel=channel,
            thread_ts=thread_ts,
            message_ts=event_ts,
            source_ts_utc=event_ts,
            request_text=git_memory_text or "No details provided.",
            trigger_mode="git_memory_prefix",
        )
        if queued_id is None:
            say(
                text=(
                    "I couldn't queue that `git-memory:` request right now. "
                    "Tracker storage is unavailable."
                ),
                thread_ts=thread_ts,
            )
            return
        say(
            text=(
                "Queued for memory-to-git reconciliation.\n"
                f"- id: `#{queued_id}`\n"
                "- status: `captured`\n"
                "- kind: `memory_git`\n"
                "- A) change mirrored to `memory.md`\n"
                "- B) queue snapshot refreshed for git reconciliation\n"
                "Use `spencer changes memory` to review."
            ),
            thread_ts=thread_ts,
        )
        return

    stripped_text = _strip_slack_mentions(text).strip()
    if looks_like_change_request(stripped_text):
        queued_id = _capture_and_notify_memory_git_change(
            user_id=user_id,
            channel=channel,
            thread_ts=thread_ts,
            message_ts=event_ts,
            source_ts_utc=event_ts,
            request_text=stripped_text,
            trigger_mode="auto_behavior_request",
        )
        if queued_id is None:
            say(
                text=(
                    "I detected a bot behavior change request, but couldn't queue it right now.\n"
                    "Please retry with `git-memory:` prefix."
                ),
                thread_ts=thread_ts,
            )
            return
        say(
            text=(
                "I logged this as a bot behavior change request.\n"
                f"- id: `#{queued_id}`\n"
                "- A) Added to `memory.md`\n"
                "- B) Added to memory→git reconciliation queue\n"
                "Carson has been DM'd with the request details.\n"
                "Use `spencer changes memory` to review."
            ),
            thread_ts=thread_ts,
        )
        return

    change_id = _capture_spencer_change_request(
        user_id=user_id,
        channel=channel,
        thread_ts=thread_ts,
        message_ts=event_ts,
        text=text,
    )

    if _handle_spencer_change_command(text=text, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="handled", note="Reviewed via spencer changes command.")
        return

    if _handle_memory_command(
        text=text,
        channel=channel,
        user_id=user_id,
        event_ts=event_ts,
        thread_ts=thread_ts,
        say=say,
    ):
        _mark_spencer_change(change_id, status="handled", note="Handled by memory workflow.")
        return

    memory = _memory_runtime()
    if memory is not None:
        try:
            memory.ingest_message(
                channel=channel,
                user_id=user_id,
                text=text,
                source=memory_source,
                source_ts_utc=event_ts,
            )
        except Exception:
            logger.exception("Failed to ingest memory from mention")

    if _handle_settings_command(text=text, user_id=user_id, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by settings workflow.")
        return

    if _handle_pipeline_command(text=text, user_id=user_id, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by pipeline workflow.")
        return

    if _handle_x_post_compound_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by X post compound workflow.")
        return

    if _handle_x_chart_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by X chart workflow.")
        return

    if _handle_x_digest_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by X digest workflow.")
        return

    if _handle_dev_buzz_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by Dev Buzz workflow.")
        return

    if _handle_market_daily_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by market daily workflow.")
        return

    if _handle_board_seat_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by board seat workflow.")
        return

    try:
        settings = load_runtime_settings()
    except RuntimeSettingsError as exc:
        logger.exception("Failed to load runtime settings: %s", exc)
        say(text=f"Failed to load runtime settings: {exc}", thread_ts=thread_ts)
        _mark_spencer_change(change_id, status="blocked", note="Runtime settings failed to load.")
        return

    if _handle_universe_command(text, thread_ts=thread_ts, say=say):
        _mark_spencer_change(change_id, status="implemented", note="Handled by universe workflow.")
        return

    pending_choice = PENDING_CHART_CHOICES.get(thread_ts)
    if pending_choice is not None:
        choice = _parse_universe_choice(text, pending_choice.suggested_universe)
        if choice is not None:
            mode, selected_name = choice
            if mode == "online":
                discovered = discover_online_tickers(pending_choice.query, limit=settings.peer_discovery_limit)
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
                            "`@SPClaw use universe defense`."
                        ),
                        thread_ts=thread_ts,
                    )
                    _mark_spencer_change(change_id, status="blocked", note="Insufficient tickers from online discovery.")
                    return
                PENDING_CHART_CHOICES.pop(thread_ts, None)
                ok = _run_chart_and_respond(
                    say=say,
                    channel=channel,
                    thread_ts=thread_ts,
                    tickers=merged,
                    x_metric=pending_choice.x_metric,
                    y_metric=pending_choice.y_metric,
                    title_context=pending_choice.title_context,
                    source_label=f"online:{pending_choice.query}",
                    followup_prompt=settings.followup_prompt,
                )
                _mark_spencer_change(
                    change_id,
                    status=("implemented" if ok else "blocked"),
                    note=("Chart rendered from online discovery." if ok else "Chart rendering failed."),
                )
                return

            use_name = selected_name or pending_choice.suggested_universe
            if not use_name:
                say(text="Please specify which universe to use, e.g. `use universe defense`.", thread_ts=thread_ts)
                _mark_spencer_change(change_id, status="needs_followup", note="Needs explicit universe choice.")
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
                _mark_spencer_change(change_id, status="blocked", note="Selected universe has too few tickers.")
                return
            PENDING_CHART_CHOICES.pop(thread_ts, None)
            ok = _run_chart_and_respond(
                say=say,
                channel=channel,
                thread_ts=thread_ts,
                tickers=universe_tickers,
                x_metric=pending_choice.x_metric,
                y_metric=pending_choice.y_metric,
                title_context=pending_choice.title_context,
                source_label=f"universe:{use_name}",
                followup_prompt=settings.followup_prompt,
            )
            _mark_spencer_change(
                change_id,
                status=("implemented" if ok else "blocked"),
                note=(f"Chart rendered from universe {use_name}." if ok else "Chart rendering failed."),
            )
            return

    chart_intent = parse_chart_intent(
        text,
        default_x_metric=settings.default_x_metric,
        default_y_metric=settings.default_y_metric,
    )
    if chart_intent is not None:
        if _is_chart_peer_expansion_request(text, chart_intent.tickers):
            suggested_universe = find_relevant_universe_name(text)
            title_context = infer_chart_title_context(text)
            query = _build_chart_query(text)

            if suggested_universe:
                universe_tickers = load_universe(suggested_universe)
                auto_tickers = _merge_unique_tickers(chart_intent.tickers, universe_tickers)
                if len(auto_tickers) >= 2:
                    ok = _run_chart_and_respond(
                        say=say,
                        channel=channel,
                        thread_ts=thread_ts,
                        tickers=auto_tickers,
                        x_metric=chart_intent.x_metric,
                        y_metric=chart_intent.y_metric,
                        title_context=title_context,
                        source_label=f"universe:{suggested_universe}",
                        followup_prompt=settings.followup_prompt,
                    )
                    _mark_spencer_change(
                        change_id,
                        status=("implemented" if ok else "blocked"),
                        note=(f"Chart rendered from suggested universe {suggested_universe}." if ok else "Chart rendering failed."),
                    )
                    return

            discovered = discover_online_tickers(query, limit=settings.peer_discovery_limit)
            auto_tickers = _merge_unique_tickers(chart_intent.tickers, discovered)
            if len(auto_tickers) >= 2:
                ok = _run_chart_and_respond(
                    say=say,
                    channel=channel,
                    thread_ts=thread_ts,
                    tickers=auto_tickers,
                    x_metric=chart_intent.x_metric,
                    y_metric=chart_intent.y_metric,
                    title_context=title_context,
                    source_label=f"online:{query}",
                    followup_prompt=settings.followup_prompt,
                )
                _mark_spencer_change(
                    change_id,
                    status=("implemented" if ok else "blocked"),
                    note=("Chart rendered from online peer discovery." if ok else "Chart rendering failed."),
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
                    "- `@SPClaw online`\n"
                    "- `@SPClaw use universe NAME`\n"
                    f"{suggested_line}"
                    f"Metric ids: `{metric_examples}`\n"
                    "Default behavior: YoY Revenue Growth on y-axis unless you specify otherwise."
                ),
                thread_ts=thread_ts,
            )
            _mark_spencer_change(change_id, status="needs_followup", note="Awaiting ticker source selection.")
            return

        ok = _run_chart_and_respond(
            say=say,
            channel=channel,
            thread_ts=thread_ts,
            tickers=chart_intent.tickers,
            x_metric=chart_intent.x_metric,
            y_metric=chart_intent.y_metric,
            title_context=infer_chart_title_context(text),
            source_label="explicit_tickers",
            followup_prompt=settings.followup_prompt,
        )
        _mark_spencer_change(
            change_id,
            status=("implemented" if ok else "blocked"),
            note=("Chart rendered from explicit tickers." if ok else "Chart rendering failed."),
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
                _mark_spencer_change(change_id, status="needs_followup", note="Feedback removed too many tickers.")
                return
            ok = _run_chart_and_respond(
                say=say,
                channel=channel,
                thread_ts=thread_ts,
                tickers=next_tickers,
                x_metric=pending_feedback.x_metric,
                y_metric=pending_feedback.y_metric,
                title_context=pending_feedback.title_context,
                source_label=(pending_feedback.source_label or "feedback_update"),
                followup_prompt=settings.followup_prompt,
            )
            _mark_spencer_change(
                change_id,
                status=("implemented" if ok else "blocked"),
                note=("Chart rerendered with feedback changes." if ok else "Chart rendering failed."),
            )
            return

    ticker = _extract_diligence_ticker(text)
    if not ticker:
        say(text=_format_chart_usage(), thread_ts=thread_ts)
        _mark_spencer_change(change_id, status="needs_followup", note="No recognizable command/ticker found.")
        return

    try:
        out = run_diligence(ticker)
    except Exception:
        logger.exception("Failed to build diligence packet for ticker=%s", ticker)
        say(
            text=f"Failed to build diligence packet for `{ticker}`. Check bot logs for details.",
            thread_ts=thread_ts,
        )
        _mark_spencer_change(change_id, status="blocked", note=f"Diligence failed for ticker {ticker}.")
        return

    say(
        text=f"Diligence packet created for *{ticker}*: `{out}`",
        thread_ts=thread_ts,
    )
    _mark_spencer_change(change_id, status="implemented", note=f"Diligence generated for ticker {ticker}.")


@app.event("app_mention")
def handle_mention(event, say):
    _handle_slack_request_event(
        event=event,
        say=say,
        source_event="slack-app-mention",
        memory_source="slack-app-mention",
    )


@app.event("channel_created")
def handle_channel_created(event, say):
    channel_id = parse_created_channel_id(event if isinstance(event, dict) else {})
    if not channel_id:
        return
    ok, reason = _auto_join_channel(channel_id)
    if ok:
        logger.info("Auto-joined new public channel=%s reason=%s", channel_id, reason)
    else:
        logger.warning("Could not auto-join new channel=%s reason=%s", channel_id, reason)


if __name__ == "__main__":
    _bootstrap_public_channel_access()
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

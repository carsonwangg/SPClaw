from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import re
import threading
import time
from typing import Optional

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError

from coatue_claw.chart_intent import parse_chart_intent
from coatue_claw.chart_metrics import METRIC_SPECS, metric_label
from coatue_claw.chart_title_context import infer_chart_title_context
from coatue_claw.cli import run_diligence
from coatue_claw.memory_extraction import parse_memory_lookup_query
from coatue_claw.memory_runtime import MemoryRuntime
from coatue_claw.online_universe import discover_online_tickers
from coatue_claw.runtime_settings import (
    PromotionError,
    RuntimeSettingsError,
    format_settings_summary,
    list_promotion_history,
    load_runtime_settings,
    promote_current_settings_to_main,
    undo_last_settings_promotion,
    update_runtime_setting,
)
from coatue_claw.slack_config_intent import parse_config_intent
from coatue_claw.slack_file_ingest import ingest_slack_files
from coatue_claw.slack_pipeline import (
    PipelineError,
    deploy_history,
    format_pipeline_result,
    pipeline_status,
    run_build_request,
    run_checks,
    run_deploy_latest,
    undo_last_deploy,
)
from coatue_claw.slack_pipeline_intent import parse_pipeline_intent
from coatue_claw.slack_routing import should_default_route_message
from coatue_claw.slack_x_chart_intent import parse_x_chart_post_intent
from coatue_claw.slack_x_intent import parse_x_digest_intent
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
from coatue_claw.x_chart_daily import XChartError, add_source as add_x_chart_source
from coatue_claw.x_chart_daily import list_sources as list_x_chart_sources
from coatue_claw.x_chart_daily import run_chart_for_post_url
from coatue_claw.x_chart_daily import run_chart_scout_once
from coatue_claw.x_chart_daily import status as x_chart_status
from coatue_claw.x_digest import XDigestError, build_x_digest, format_x_digest_summary

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
PIPELINE_LOCK = threading.Lock()
_MEMORY_RUNTIME: MemoryRuntime | None = None


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
        "- `x digest <topic|ticker|handle> [last 24h] [limit 50]`\n"
        "- `x chart now` (run chart-scout winner now)\n"
        "- `x chart sources` / `x chart add @handle priority 1.2`\n"
        "- `graph ev ltm growth SNOW,MDB,DDOG`\n"
        "- natural language: `plot EV/Revenue multiples vs revenue growth for SNOW,MDB,DDOG`\n"
        "- create universe: `create universe defense with PLTR,LMT,RTX,NOC,GD,LDOS`\n"
        "- list universes: `list universes`\n"
        "- settings: `show my settings` or `going forward look for 12 peers`\n"
        "- pipeline: `deploy latest` or `undo last deploy`\n"
        "- memory: `memory status` or `what is my daughter's birthday?`\n"
        "- file ingest: upload a file in Slack and I'll auto-sort it into the knowledge folders\n"
        "- routing: messages default to OpenClaw unless you @ another user\n"
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
            for key in ("COATUE_CLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "COATUE_CLAW_TWITTER_BEARER_TOKEN")
        )
        say(
            text=(
                "X digest status:\n"
                f"- bearer_token_configured: `{configured}`\n"
                f"- api_base: `{os.environ.get('COATUE_CLAW_X_API_BASE', 'https://api.x.com')}`\n"
                f"- digest_dir: `{os.environ.get('COATUE_CLAW_X_DIGEST_DIR', '/opt/coatue-claw-data/artifacts/x-digest')}`"
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
                "- `x chart add @fiscal_AI priority 1.6`"
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
            result = run_chart_for_post_url(post_url=intent.post_url, channel_override=channel)
            winner = result.get("winner") or {}
            lines.append(
                "- Posted Coatue-style chart from linked post:\n"
                f"  - source: `{winner.get('source')}`\n"
                f"  - url: {winner.get('url')}"
            )
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
    if should_default_route_message(text):
        _handle_slack_request_event(
            event=event,
            say=say,
            source_event="slack-message-default",
            memory_source="slack-message-default",
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
        return

    if _handle_memory_command(
        text=text,
        channel=channel,
        user_id=user_id,
        event_ts=event_ts,
        thread_ts=thread_ts,
        say=say,
    ):
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
        return

    if _handle_pipeline_command(text=text, user_id=user_id, thread_ts=thread_ts, say=say):
        return

    if _handle_x_post_compound_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        return

    if _handle_x_chart_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        return

    if _handle_x_digest_command(text=text, channel=channel, thread_ts=thread_ts, say=say):
        return

    try:
        settings = load_runtime_settings()
    except RuntimeSettingsError as exc:
        logger.exception("Failed to load runtime settings: %s", exc)
        say(text=f"Failed to load runtime settings: {exc}", thread_ts=thread_ts)
        return

    if _handle_universe_command(text, thread_ts=thread_ts, say=say):
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
                    followup_prompt=settings.followup_prompt,
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
                followup_prompt=settings.followup_prompt,
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
                    _run_chart_and_respond(
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
                    return

            discovered = discover_online_tickers(query, limit=settings.peer_discovery_limit)
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
                    followup_prompt=settings.followup_prompt,
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
            followup_prompt=settings.followup_prompt,
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
                followup_prompt=settings.followup_prompt,
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


@app.event("app_mention")
def handle_mention(event, say):
    _handle_slack_request_event(
        event=event,
        say=say,
        source_event="slack-app-mention",
        memory_source="slack-app-mention",
    )


if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

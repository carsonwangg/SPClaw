from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
import io
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import textwrap
from typing import Any
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]


load_dotenv("/opt/coatue-claw/.env.prod")

logger = logging.getLogger(__name__)

DEFAULT_WINDOWS = "09:00,12:00,18:00"
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_CONVENTION_NAMES = ("Morning", "Afternoon", "Evening")


DEFAULT_PRIORITY_SOURCES: list[tuple[str, float]] = [
    ("fiscal_AI", 1.6),
    ("cloudedjudgment", 1.5),
    ("KobeissiLetter", 1.3),
    ("charliebilello", 1.25),
    ("Barchart", 1.2),
    ("bespokeinvest", 1.2),
    ("biancoresearch", 1.2),
    ("LizAnnSonders", 1.15),
    ("Yardeni", 1.15),
    ("AswathDamodaran", 1.1),
    ("BloombergGraphics", 1.05),
    ("WSJGraphics", 1.0),
    ("OurWorldInData", 1.0),
]

THEME_KEYWORDS = (
    "ai",
    "semiconductor",
    "software",
    "saas",
    "consumer",
    "macro",
    "cloud",
    "gpu",
    "valuation",
    "growth",
    "margin",
    "demand",
    "supply",
)

CHART_SIGNAL_KEYWORDS = (
    "chart",
    "graph",
    "data",
    "trend",
    "yoy",
    "qoq",
    "cagr",
    "growth",
    "revenue",
    "margin",
    "valuation",
    "multiple",
    "ev",
    "ebitda",
    "sales",
    "earnings",
    "gdp",
    "inflation",
    "unemployment",
    "index",
    "forecast",
    "capex",
    "guidance",
    "consensus",
)

US_STRONG_SIGNAL_KEYWORDS = (
    "s&p",
    "spx",
    "nasdaq",
    "nyse",
    "dow jones",
    "russell 2000",
    "fomc",
    "federal reserve",
    "fed funds",
    "treasury",
    "u.s. treasury",
    "10y",
    "2y",
    "nonfarm payrolls",
    "jobless claims",
    "cpi",
    "pce",
    "core inflation",
    "u.s. consumer",
    "u.s. earnings",
    "saaS",
    "software multiple",
    "ai capex",
)

US_WEAK_SIGNAL_KEYWORDS = (
    "u.s.",
    "united states",
    "american",
    "wall street",
)

FOREX_NON_US_KEYWORDS = (
    "forex",
    "fx",
    "eur/usd",
    "usd/jpy",
    "gbp/usd",
    "aud/usd",
    "cad/usd",
    "usd/chf",
    "us dollar index",
    "turkish lira",
    "lira",
    "yuan",
    "renminbi",
    "rupee",
    "peso",
    "real",
    "rand",
)

NON_US_GEOGRAPHY_KEYWORDS = (
    "europe",
    "eurozone",
    "uk ",
    "united kingdom",
    "china",
    "japan",
    "india",
    "brazil",
    "turkey",
    "germany",
    "france",
    "canada",
    "mexico",
    "emerging markets",
)

TREND_SIGNAL_KEYWORDS = (
    "up",
    "down",
    "higher",
    "lower",
    "record high",
    "record low",
    "all-time high",
    "all-time low",
    "accelerating",
    "slowing",
    "rebound",
    "decline",
)

SLIDE_JARGON_KEYWORDS = (
    "bull case",
    "bear case",
    "peer comparison",
    "valuation framework",
    "investment recommendation",
    "operating leverage",
    "contribution margin",
)

BAR_HINT_KEYWORDS = (
    "bar chart",
    "bar graph",
    "histogram",
    "by country",
    "by state",
    "by cohort",
    "top 10",
    "top ten",
    "ranked",
    "ranking",
)

POSITIVE_MOVE_VERBS = ("surged", "rose", "jumped", "climbed", "rebounded", "accelerated", "increased", "grew")
NEGATIVE_MOVE_VERBS = ("fell", "dropped", "declined", "slowed", "rolled over", "sank", "contracted", "decelerated")
NEUTRAL_MOVE_VERBS = ("hit", "reached", "stands at", "is at")
NEWS_PREFIX_RE = re.compile(r"^(breaking|update|new chart|chart|alert)\s*:\s*", re.IGNORECASE)
TICKER_NAME_HINTS: dict[str, str] = {
    "AMZN": "Amazon",
    "MSFT": "Microsoft",
    "GOOGL": "Google",
    "GOOG": "Google",
    "META": "Meta",
    "AAPL": "Apple",
    "NVDA": "NVIDIA",
}

TRAILING_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}

SUBJECT_TRAILING_ACTION_VERBS = (
    "sold",
    "bought",
    "buying",
    "selling",
    "hit",
    "reached",
    "stands",
    "standing",
    "is",
    "are",
    "was",
    "were",
    "be",
)

PLURAL_SUBJECT_HINTS = {
    "investors",
    "clients",
    "funds",
    "flows",
    "receipts",
    "duties",
    "sales",
    "earnings",
    "jobs",
    "rates",
    "prices",
    "stocks",
    "etfs",
}


class XChartError(RuntimeError):
    pass


@dataclass(frozen=True)
class Candidate:
    candidate_key: str
    source_type: str
    source_id: str
    author: str
    title: str
    text: str
    url: str
    image_url: str | None
    created_at: str | None
    engagement: int
    source_priority: float
    score: float


@dataclass(frozen=True)
class StyleDraft:
    headline: str
    chart_label: str
    takeaway: str
    why_now: str
    iteration: int
    checks: dict[str, bool]
    score: float


@dataclass(frozen=True)
class RebuiltSeries:
    label: str
    x: list[float]
    y: list[float]
    color: str
    weight: float


@dataclass(frozen=True)
class RebuiltBars:
    labels: list[str]
    values: list[float]
    color: str
    y_label: str
    normalized: bool
    source: str
    confidence: float
    primary_label: str | None = None
    secondary_values: list[float] | None = None
    secondary_color: str | None = None
    secondary_label: str | None = None


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_X_CHART_DB_PATH",
            str(_data_root() / "db/x_chart_daily.sqlite"),
        )
    )


def _x_api_base() -> str:
    return (os.environ.get("COATUE_CLAW_X_API_BASE", "https://api.x.com").strip() or "https://api.x.com").rstrip("/")


def _output_dir() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_X_CHART_DIR",
            str(_data_root() / "artifacts/x-chart-daily"),
        )
    )


def _resolve_bearer_token() -> str:
    for key in ("COATUE_CLAW_X_BEARER_TOKEN", "X_BEARER_TOKEN", "COATUE_CLAW_TWITTER_BEARER_TOKEN"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    raise XChartError("X bearer token missing. Set COATUE_CLAW_X_BEARER_TOKEN in .env.prod.")


def _slack_tokens() -> list[str]:
    tokens: list[str] = []
    env_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if env_token:
        tokens.append(env_token)
    config_path = Path.home() / ".openclaw/openclaw.json"
    if config_path.exists():
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            cfg_token = str(
                (
                    payload.get("channels", {})
                    .get("slack", {})
                    .get("botToken", "")
                )
            ).strip()
        except Exception:
            cfg_token = ""
        if cfg_token:
            tokens.append(cfg_token)
    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
    if not unique:
        raise XChartError("Slack bot token missing (env SLACK_BOT_TOKEN or ~/.openclaw/openclaw.json channels.slack.botToken).")
    return unique


def _slack_channel() -> str:
    channel = os.environ.get("COATUE_CLAW_X_CHART_SLACK_CHANNEL", "").strip()
    if not channel:
        raise XChartError("COATUE_CLAW_X_CHART_SLACK_CHANNEL missing (set Slack channel id).")
    return channel


def _timezone() -> ZoneInfo:
    tz_name = os.environ.get("COATUE_CLAW_X_CHART_TIMEZONE", DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(tz_name)
    except Exception as exc:
        raise XChartError(f"Invalid timezone: {tz_name}") from exc


def _parse_windows(raw: str | None = None) -> list[tuple[int, int]]:
    value = (raw or os.environ.get("COATUE_CLAW_X_CHART_WINDOWS", DEFAULT_WINDOWS) or DEFAULT_WINDOWS).strip()
    out: list[tuple[int, int]] = []
    for part in value.split(","):
        p = part.strip()
        if not p:
            continue
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", p)
        if not m:
            continue
        h = int(m.group(1))
        minute = int(m.group(2))
        if 0 <= h <= 23 and 0 <= minute <= 59:
            out.append((h, minute))
    if not out:
        out = [(9, 0), (12, 0), (18, 0)]
    out.sort()
    return out


def _slot_name_for_hour(hour: int) -> str:
    if 5 <= hour < 12:
        return "Morning"
    if 12 <= hour < 17:
        return "Afternoon"
    return "Evening"


def _slot_name_for_key(*, slot_key: str, now_local: datetime, windows: list[tuple[int, int]]) -> str:
    if slot_key.startswith("manual"):
        return _slot_name_for_hour(now_local.hour)
    m = re.search(r"-(\d{2}):(\d{2})$", slot_key)
    if not m:
        return _slot_name_for_hour(now_local.hour)
    target = (int(m.group(1)), int(m.group(2)))
    ordered = sorted(windows)
    for idx, item in enumerate(ordered):
        if item != target:
            continue
        if idx < len(DEFAULT_CONVENTION_NAMES):
            return DEFAULT_CONVENTION_NAMES[idx]
        return _slot_name_for_hour(target[0])
    return _slot_name_for_hour(target[0])


def _convention_name(*, slot_key: str, now_local: datetime, windows: list[tuple[int, int]]) -> str:
    return f"Coatue Chart of the {_slot_name_for_key(slot_key=slot_key, now_local=now_local, windows=windows)}"


def _canonical_handle(handle: str) -> str:
    out = handle.strip().lstrip("@")
    out = re.sub(r"[^A-Za-z0-9_]+", "", out)
    return out


def _normalize_render_text(text: str) -> str:
    cleaned = re.sub(r"https?://\S+", "", text or "")
    cleaned = re.sub(r"[\U00010000-\U0010ffff]", "", cleaned)
    cleaned = cleaned.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    cleaned = cleaned.replace("\u2013", "-").replace("\u2014", "-")
    cleaned = unicodedata.normalize("NFKD", cleaned)
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    cleaned = cleaned.replace("\ufffd", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _shorten_without_ellipsis(text: str, *, max_chars: int) -> str:
    cleaned = _normalize_render_text(text).replace("...", " ").strip(". ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    words = cleaned.split(" ")
    out: list[str] = []
    for word in words:
        candidate = ((" ".join(out) + " " + word).strip() if out else word)
        if len(candidate) <= max_chars:
            out.append(word)
            continue
        break
    if out:
        return " ".join(out).strip(". ")
    return cleaned[:max_chars].rstrip(". ")


def _extract_first_sentence(text: str) -> str:
    normalized = _normalize_render_text(text)
    if not normalized:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", normalized)
    if not parts:
        return normalized
    first = parts[0].strip()
    return first or normalized


def _strip_news_prefix(text: str) -> str:
    return NEWS_PREFIX_RE.sub("", _normalize_render_text(text)).strip()


def _mode_hint_from_text(candidate: Candidate) -> str:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    if any(token in merged for token in BAR_HINT_KEYWORDS):
        return "bar"
    return "line"


def _extract_subject_and_verb(sentence: str) -> tuple[str, str]:
    clean = _strip_news_prefix(sentence)
    patterns = list(POSITIVE_MOVE_VERBS) + list(NEGATIVE_MOVE_VERBS) + list(NEUTRAL_MOVE_VERBS)
    for verb in patterns:
        m = re.search(rf"^(.*?)\b{re.escape(verb)}\b", clean, flags=re.IGNORECASE)
        if m:
            subject = m.group(1).strip(" ,:-")
            if subject:
                return subject, verb
    words = clean.split(" ")
    if len(words) >= 4:
        return " ".join(words[:4]).strip(" ,:-"), ""
    return clean.strip(" ,:-"), ""


def _extract_timeframe_snippet(sentence: str) -> str:
    lower = sentence.lower()
    m = re.search(r"\bin the first [a-z0-9\s-]{3,28}\b", lower)
    if m:
        frag = m.group(0).replace("in the ", "").strip()
        return frag.title()
    m = re.search(r"\bsince \d{4}\b", lower)
    if m:
        return m.group(0).title()
    m = re.search(r"\b(last|past)\s+\d+\s+(months|years|quarters|weeks)\b", lower)
    if m:
        return m.group(0).title()
    return ""


def _entity_hint_from_text(*, sentence: str, fallback: str) -> str:
    ticker_match = re.search(r"\$([A-Z]{1,5})\b", sentence)
    if ticker_match:
        ticker = ticker_match.group(1).upper()
        return TICKER_NAME_HINTS.get(ticker, ticker)
    clean_fallback = _shorten_without_ellipsis(_strip_news_prefix(fallback), max_chars=24)
    clean_fallback = clean_fallback.strip(" ,:-")
    if clean_fallback:
        return clean_fallback
    return "US trend"


def _infer_units_snippet(sentence: str) -> str:
    lower = sentence.lower()
    if ("billion" in lower or "million" in lower or "$" in sentence or "usd" in lower):
        return "US$"
    if "yoy" in lower or "qoq" in lower or "%" in sentence:
        return "YoY %"
    if "p/e" in lower or "multiple" in lower or "x" in lower:
        return "x"
    if "index" in lower:
        return "Index"
    return ""


def _extract_years_from_text(text: str) -> list[int]:
    years: list[int] = []
    for m in re.finditer(r"\b(19\d{2}|20\d{2})\b", text):
        try:
            value = int(m.group(1))
        except Exception:
            continue
        if 1900 <= value <= 2100:
            years.append(value)
    dedup: list[int] = []
    seen: set[int] = set()
    for y in years:
        if y in seen:
            continue
        seen.add(y)
        dedup.append(y)
    return dedup


def _infer_bar_labels_from_text(*, candidate: Candidate | None, count: int) -> list[str]:
    if count <= 0:
        return []
    if candidate is None:
        return []
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}")
    years = sorted(_extract_years_from_text(merged))
    if len(years) >= 2:
        min_y, max_y = years[0], years[-1]
        span = max_y - min_y + 1
        if span == count:
            return [str(y) for y in range(min_y, max_y + 1)]
        if span > count and count >= 4:
            start = max_y - count + 1
            if start >= 1900:
                return [str(y) for y in range(start, max_y + 1)]
    if len(years) == 1 and 4 <= count <= 20:
        end_y = years[0]
        start_y = end_y - count + 1
        if start_y >= 1900:
            return [str(y) for y in range(start_y, end_y + 1)]
    return []


def _fallback_bar_labels(*, candidate: Candidate | None, count: int) -> list[str]:
    labels = _infer_bar_labels_from_text(candidate=candidate, count=count)
    if len(labels) == count:
        return labels
    year_hint: int | None = None
    if candidate is not None:
        merged = _normalize_render_text(f"{candidate.title} {candidate.text}")
        years = sorted(_extract_years_from_text(merged))
        if years:
            year_hint = years[-1]
        elif candidate.created_at:
            try:
                year_hint = datetime.fromisoformat(candidate.created_at.replace("Z", "+00:00")).year
            except Exception:
                year_hint = None
    if year_hint is None:
        year_hint = datetime.now(UTC).year
    if 4 <= count <= 20:
        start_y = year_hint - count + 1
        if start_y >= 1900:
            return [str(y) for y in range(start_y, year_hint + 1)]
    return [f"P{i+1}" for i in range(count)]


def _is_employees_robots_chart(candidate: Candidate | None) -> bool:
    if candidate is None:
        return False
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    return ("employees" in merged) and ("robots" in merged)


def _labels_are_placeholder(labels: list[str]) -> bool:
    if not labels:
        return True
    return all(bool(re.fullmatch(r"p\d+", str(label).strip().lower())) for label in labels)


def _labels_are_monotonic_years(labels: list[str]) -> bool:
    years: list[int] = []
    for label in labels:
        text = str(label).strip()
        if not re.fullmatch(r"\d{4}", text):
            return False
        years.append(int(text))
    if len(years) < 4:
        return False
    return all(years[i] < years[i + 1] for i in range(len(years) - 1))


def _extract_employee_robot_latest_millions(text: str) -> tuple[float | None, float | None]:
    lower = _normalize_render_text(text).lower()
    emp_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+employees", lower)
    rob_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+robots", lower)
    emp = float(emp_m.group(1)) if emp_m else None
    rob = float(rob_m.group(1)) if rob_m else None
    return emp, rob


def _extract_employees_robots_bars_cv(*, rgb, candidate: Candidate) -> RebuiltBars | None:
    try:
        import numpy as np
        from matplotlib.colors import rgb_to_hsv
    except Exception:
        return None

    h, w, _ = rgb.shape
    crop = rgb[int(h * 0.16) : int(h * 0.93), int(w * 0.07) : int(w * 0.97), :]
    ch, cw, _ = crop.shape
    if ch < 200 or cw < 260:
        return None
    analysis = crop[int(ch * 0.18) :, :, :]
    ah, aw, _ = analysis.shape
    hsv = rgb_to_hsv(np.clip(analysis, 0.0, 1.0))
    hue = hsv[:, :, 0]
    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]

    # ARK-style bars are predominantly blue/purple with similar hue and different brightness.
    base = (sat > 0.15) & (hue >= 0.57) & (hue <= 0.80)
    dark_mask = base & (val < 0.56)
    light_mask = base & (val >= 0.56)

    def _series(mask):
        cols = mask.sum(axis=0)
        col_threshold = max(4, int(ah * 0.030))
        flags = cols >= col_threshold
        spans: list[tuple[int, int]] = []
        start = -1
        min_width = max(2, int(aw * 0.006))
        for i, flag in enumerate(flags.tolist()):
            if flag and start < 0:
                start = i
            elif (not flag) and start >= 0:
                if (i - start) >= min_width:
                    spans.append((start, i - 1))
                start = -1
        if start >= 0 and (len(flags) - start) >= min_width:
            spans.append((start, len(flags) - 1))
        centers: list[float] = []
        heights: list[float] = []
        bottoms: list[float] = []
        for s, e in spans:
            seg = mask[:, s : e + 1]
            y_idx = np.where(seg.any(axis=1))[0]
            if y_idx.size == 0:
                continue
            top = float(y_idx.min())
            bottom = float(y_idx.max())
            centers.append(float((s + e) / 2.0))
            heights.append(max(0.0, bottom - top))
            bottoms.append(bottom)
        return centers, heights, bottoms

    dark_x, dark_h, dark_b = _series(dark_mask)
    light_x, light_h, light_b = _series(light_mask)
    if len(dark_h) < 8 or len(light_h) < 8:
        return None

    # Keep the best-aligned segment count between the two series.
    n = min(len(dark_h), len(light_h))
    if n < 8:
        return None
    dark_vals = dark_h[-n:]
    light_vals = light_h[-n:]

    # Convert relative heights to "thousands" using latest values from post text.
    latest_emp_m, latest_rob_m = _extract_employee_robot_latest_millions(f"{candidate.title} {candidate.text}")
    if latest_emp_m is None or latest_rob_m is None:
        return None
    if dark_vals[-1] <= 0.0 or light_vals[-1] <= 0.0:
        return None
    emp_scale = (latest_emp_m * 1000.0) / float(dark_vals[-1])
    rob_scale = (latest_rob_m * 1000.0) / float(light_vals[-1])
    scale = float((emp_scale + rob_scale) / 2.0)
    if scale <= 0.0:
        return None

    emp_values = [round(float(v) * scale, 1) for v in dark_vals]
    rob_values = [round(float(v) * scale, 1) for v in light_vals]
    labels = _fallback_bar_labels(candidate=candidate, count=n)
    bars = RebuiltBars(
        labels=labels,
        values=emp_values,
        color="#1F2452",
        y_label="Number (thousands)",
        normalized=False,
        source="cv",
        confidence=0.64,
        primary_label="Employees",
        secondary_values=rob_values,
        secondary_color="#6D63E7",
        secondary_label="Robots",
    )
    if _bar_data_quality_errors(candidate=candidate, bars=bars):
        return None
    return bars


def _normalize_grouped_bar_metadata(*, candidate: Candidate | None, bars: RebuiltBars) -> RebuiltBars:
    if candidate is None or (not _is_employees_robots_chart(candidate)):
        return bars
    if not bars.secondary_values or len(bars.secondary_values) != len(bars.values):
        return bars

    primary_values = list(bars.values)
    secondary_values = list(bars.secondary_values)
    p_label = (bars.primary_label or "").strip().lower()
    s_label = (bars.secondary_label or "").strip().lower()
    primary_sum = float(sum(primary_values))
    secondary_sum = float(sum(secondary_values))
    should_swap = False
    if ("robot" in p_label and "employee" in s_label) or ("employee" in s_label and "employee" not in p_label):
        should_swap = True
    elif ("robot" not in s_label and "employee" not in p_label) and (primary_sum < secondary_sum):
        should_swap = True

    if should_swap:
        primary_values, secondary_values = secondary_values, primary_values

    y_label = bars.y_label
    y_lower = (y_label or "").strip().lower()
    if (not y_lower) or ("index" in y_lower) or (y_lower == "value"):
        y_label = "Number (thousands)"
    return replace(
        bars,
        values=primary_values,
        secondary_values=secondary_values,
        primary_label="Employees",
        secondary_label="Robots",
        color="#1F2452",
        secondary_color="#6D63E7",
        y_label=y_label,
    )


def _bar_data_quality_errors(*, candidate: Candidate | None, bars: RebuiltBars | None) -> list[str]:
    if bars is None:
        return ["missing rebuilt bars"]
    errors: list[str] = []
    n = len(bars.values)
    if n < 4:
        errors.append("insufficient bar count")
    if len(bars.labels) != n:
        errors.append("x-axis labels do not match bar count")
    if _labels_are_placeholder(bars.labels):
        errors.append("placeholder x-axis labels")
    if not (bars.y_label or "").strip():
        errors.append("missing y-axis label")
    if _is_employees_robots_chart(candidate):
        if not bars.secondary_values or len(bars.secondary_values) != n:
            errors.append("grouped chart requires two aligned series")
        if bars.normalized:
            errors.append("grouped chart must not use normalized values")
        if max(bars.values or [0.0]) < 200.0 or max(bars.secondary_values or [0.0]) < 100.0:
            errors.append("grouped chart values are too small for employee/robot units")
        y_lower = (bars.y_label or "").lower()
        if "index" in y_lower:
            errors.append("grouped chart y-axis label must be unit-based")
        if bars.labels and (not _labels_are_monotonic_years(bars.labels)):
            errors.append("grouped chart requires monotonic year x-axis labels")
    return errors


def _style_copy_quality_errors(style_draft: StyleDraft) -> list[str]:
    errors: list[str] = []
    if "..." in style_draft.headline or "..." in style_draft.chart_label or "..." in style_draft.takeaway:
        errors.append("style copy contains ellipsis")
    if len(_normalize_render_text(style_draft.headline)) > 58:
        errors.append("headline too long")
    if len(_normalize_render_text(style_draft.chart_label)) > 62:
        errors.append("chart label too long")
    if len(_normalize_render_text(style_draft.takeaway)) > 68:
        errors.append("takeaway too long")
    if _has_incoherent_headline(style_draft.headline):
        errors.append("headline grammar invalid")
    return errors


def _post_publish_checklist(
    *,
    candidate: Candidate,
    style_draft: StyleDraft,
    styled_path: Path,
    render_qa: dict[str, Any],
) -> dict[str, Any]:
    headline = _normalize_render_text(style_draft.headline)
    chart_label = _normalize_render_text(style_draft.chart_label)
    takeaway = _normalize_render_text(style_draft.takeaway)
    reconstruction_mode = str(render_qa.get("reconstruction_mode") or "").strip().lower()
    grouped_required = _is_employees_robots_chart(candidate)
    file_size = styled_path.stat().st_size if styled_path.exists() else 0
    checks = {
        "us_relevant": bool(style_draft.checks.get("us_relevant", False)),
        "trend_explicit": bool(style_draft.checks.get("trend_explicit", False)),
        "plain_language": bool(style_draft.checks.get("plain_language", False)),
        "no_ellipsis": ("..." not in headline and "..." not in chart_label and "..." not in takeaway),
        "headline_len_ok": (0 < len(headline) <= 58),
        "chart_label_len_ok": (0 < len(chart_label) <= 62),
        "takeaway_len_ok": (0 < len(takeaway) <= 68),
        "reconstruction_mode_ok": reconstruction_mode in {"bar", "line"},
        "x_axis_labels_present": bool(render_qa.get("x_axis_labels_present", False)),
        "y_axis_labels_present": bool(render_qa.get("y_axis_labels_present", False)),
        "grouped_series_valid": (not grouped_required) or bool(render_qa.get("grouped_two_series", False)),
        "artifact_nonempty": file_size >= 25_000,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "passed": len(failed) == 0,
        "failed": failed,
        "checks": checks,
        "style_score": float(style_draft.score),
        "style_iteration": int(style_draft.iteration),
        "render_qa": render_qa,
        "artifact_path": str(styled_path),
        "artifact_size_bytes": int(file_size),
    }


def _nice_tick_step(value: float) -> float:
    if not math.isfinite(value) or value <= 0:
        return 1.0
    base = 10.0 ** math.floor(math.log10(value))
    for mult in (1.0, 2.0, 2.5, 5.0, 10.0):
        step = base * mult
        if value <= step:
            return step
    return base * 10.0


def _compute_y_ticks(*, y_min: float, y_max: float, normalized: bool) -> list[float]:
    if normalized:
        return [0.0, 20.0, 40.0, 60.0, 80.0, 100.0]
    low = float(min(y_min, y_max))
    high = float(max(y_min, y_max))
    span = max(1.0, high - low)
    rough = span / 5.0
    step = _nice_tick_step(rough)
    start = math.floor(low / step) * step
    end = math.ceil(high / step) * step
    ticks: list[float] = []
    v = start
    guard = 0
    while v <= end + (step * 0.25) and guard < 32:
        ticks.append(round(float(v), 6))
        v += step
        guard += 1
    if len(ticks) < 3:
        mid = (low + high) / 2.0
        ticks = [round(low, 6), round(mid, 6), round(high, 6)]
    return ticks


def _format_numeric_tick(value: float) -> str:
    if abs(value - round(value)) < 1e-6:
        return f"{int(round(value)):,}"
    if abs(value) >= 100.0:
        return f"{value:,.0f}"
    if abs(value) >= 10.0:
        return f"{value:,.1f}"
    return f"{value:,.2f}"


def _vision_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_VISION_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _extract_rebuilt_bars_via_vision(*, candidate: Candidate) -> RebuiltBars | None:
    if not _vision_enabled():
        return None
    if OpenAI is None:
        return None
    image_url = (candidate.image_url or "").strip()
    if not image_url:
        return None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None

    payload, mime = _fetch_image_bytes(image_url)
    if not payload:
        return None
    b64 = base64.b64encode(payload).decode("ascii")
    image_data_url = f"data:{mime};base64,{b64}"

    try:
        client = OpenAI(api_key=api_key)
        model = os.environ.get("COATUE_CLAW_X_CHART_VISION_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
        force_grouped = _is_employees_robots_chart(candidate)
        prompt = (
            "Extract bar-chart data from this image.\n"
            "Return strict JSON only with keys:\n"
            "{"
            "\"chart_type\":\"bar\","
            "\"x_labels\":[\"...\"],"
            "\"values\":[number],"
            "\"series\":[{\"name\":\"...\",\"values\":[number]}],"
            "\"y_label\":\"...\","
            "\"normalized\":true|false,"
            "\"confidence\":0.0-1.0"
            "}.\n"
            "Rules: Use bars left-to-right. Include negatives if present. "
            "If there are grouped bars with multiple series, provide `series` with 2 entries and aligned values. "
            "If there is only one series, use `values` and omit `series`. "
            "If exact units are visible, set y_label accordingly (e.g., 'US$ Billions'). "
            "If units are unclear, use 'Index (normalized)' and normalized=true."
        )
        if force_grouped:
            prompt += (
                " This chart is expected to have two series (Employees and Robots). "
                "You MUST return `series` with exactly two aligned value arrays and omit `values`."
            )
        response = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a precise chart-data extraction assistant."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_data_url}},
                    ],
                },
            ],
        )
        raw = ""
        if response.choices and response.choices[0].message:
            raw = str(response.choices[0].message.content or "").strip()
        if not raw:
            return None
        payload = json.loads(raw)
    except Exception as exc:
        logger.debug("Vision extraction failed: %s", exc)
        return None

    labels_raw = payload.get("x_labels") if isinstance(payload, dict) else None
    series_raw = payload.get("series") if isinstance(payload, dict) else None
    values_raw = payload.get("values") if isinstance(payload, dict) else None

    values: list[float] = []
    secondary_values: list[float] | None = None
    primary_label: str | None = None
    secondary_label: str | None = None
    if isinstance(series_raw, list) and len(series_raw) >= 2:
        series_values: list[tuple[str | None, list[float]]] = []
        for entry in series_raw[:2]:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name") or "").strip() or None
            raw_vals = entry.get("values")
            if not isinstance(raw_vals, list):
                continue
            parsed: list[float] = []
            for item in raw_vals:
                try:
                    parsed.append(float(item))
                except Exception:
                    continue
            if parsed:
                series_values.append((name, parsed))
        if len(series_values) >= 2:
            a_name, a_vals = series_values[0]
            b_name, b_vals = series_values[1]
            n = min(len(a_vals), len(b_vals))
            if 4 <= n <= 20:
                values = a_vals[:n]
                secondary_values = b_vals[:n]
                primary_label = a_name
                secondary_label = b_name
    if force_grouped and (secondary_values is None):
        return None
    if not values:
        if not isinstance(values_raw, list):
            return None
        for item in values_raw:
            try:
                values.append(float(item))
            except Exception:
                continue
        if not (4 <= len(values) <= 20):
            return None

    labels: list[str] = []
    if isinstance(labels_raw, list):
        for item in labels_raw:
            label = _shorten_without_ellipsis(str(item), max_chars=12)
            if label:
                labels.append(label)
    if labels and len(labels) != len(values):
        n = min(len(labels), len(values))
        labels = labels[:n]
        values = values[:n]
        if secondary_values is not None:
            secondary_values = secondary_values[:n]
    if not labels:
        labels = _fallback_bar_labels(candidate=candidate, count=len(values))
    if len(labels) not in {0, len(values)}:
        labels = []
    if secondary_values is not None and len(secondary_values) != len(values):
        n = min(len(values), len(secondary_values))
        values = values[:n]
        secondary_values = secondary_values[:n]
        labels = labels[:n] if labels else _fallback_bar_labels(candidate=candidate, count=n)

    y_label = _shorten_without_ellipsis(str(payload.get("y_label") or "Value"), max_chars=22)
    if not y_label:
        y_label = "Value"
    normalized = bool(payload.get("normalized")) if isinstance(payload, dict) else False
    try:
        confidence = float(payload.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    if confidence < 0.58:
        return None
    bars = RebuiltBars(
        labels=labels,
        values=values,
        color="#2F6ABF",
        y_label=y_label,
        normalized=normalized,
        source="vision",
        confidence=confidence,
        primary_label=primary_label,
        secondary_values=secondary_values,
        secondary_color="#5AA88A" if secondary_values is not None else None,
        secondary_label=secondary_label,
    )
    bars = _normalize_grouped_bar_metadata(candidate=candidate, bars=bars)
    if _bar_data_quality_errors(candidate=candidate, bars=bars):
        return None
    return bars


def _synthesize_chart_label(*, subject: str, sentence: str, mode_hint: str) -> str:
    lower_sentence = sentence.lower()
    if "employees" in lower_sentence and "robots" in lower_sentence:
        entity = _entity_hint_from_text(sentence=sentence, fallback=subject)
        years = sorted(_extract_years_from_text(sentence))
        if len(years) >= 2:
            base = f"{entity} employees vs robots ({years[0]}-{years[-1]})"
        else:
            base = f"{entity} employees vs robots"
        return _shorten_without_ellipsis(base, max_chars=62)

    core = _shorten_without_ellipsis(_strip_news_prefix(subject), max_chars=46)
    if not core:
        core = "Chart Context"
    timeframe = _extract_timeframe_snippet(sentence)
    units = _infer_units_snippet(sentence)
    if mode_hint == "bar" and timeframe:
        base = f"{core} ({timeframe})"
    else:
        base = core
    if units and units.lower() not in base.lower():
        base = f"{base} ({units})"
    return _shorten_without_ellipsis(base, max_chars=62)


def _synthesize_narrative_title(*, subject: str, verb: str, sentence: str) -> str:
    subject_core = _clean_subject_for_headline(subject=subject, sentence=sentence)
    s_lower = subject_core.lower()
    v_lower = verb.lower()
    sentence_lower = sentence.lower()
    copula = "are" if _subject_is_plural(subject_core) else "is"
    if "employees" in sentence_lower and "robots" in sentence_lower:
        entity = _entity_hint_from_text(sentence=sentence, fallback=subject_core)
        if "ratio" in sentence_lower or "replacing humans" in sentence_lower:
            return _shorten_without_ellipsis(f"{entity} is increasing automation intensity", max_chars=56)
        return _shorten_without_ellipsis(f"{entity} is scaling robots faster than headcount", max_chars=56)
    if (
        "institutional" in sentence_lower
        and ("net sellers" in sentence_lower or "sold a net" in sentence_lower or "biggest net sellers" in sentence_lower)
    ):
        return "Institutional selling is at an extreme"
    if (
        "institutional" in sentence_lower
        and ("net buyers" in sentence_lower or "bought a net" in sentence_lower or "biggest net buyers" in sentence_lower)
    ):
        return "Institutional buying is accelerating"
    if "etf inflows" in s_lower:
        if v_lower in POSITIVE_MOVE_VERBS or "record" in sentence.lower():
            return "ETF appetite is re-accelerating"
        if v_lower in NEGATIVE_MOVE_VERBS:
            return "ETF appetite is cooling"
    if "consumer sentiment" in s_lower:
        if v_lower in NEGATIVE_MOVE_VERBS:
            return "Consumer confidence is weakening"
        if v_lower in POSITIVE_MOVE_VERBS:
            return "Consumer confidence is improving"
    if v_lower in POSITIVE_MOVE_VERBS:
        return _shorten_without_ellipsis(f"{subject_core} {copula} inflecting higher", max_chars=56)
    if v_lower in NEGATIVE_MOVE_VERBS:
        return _shorten_without_ellipsis(f"{subject_core} {copula} rolling over", max_chars=56)
    if "record" in sentence.lower() or v_lower in NEUTRAL_MOVE_VERBS:
        return _shorten_without_ellipsis(f"{subject_core} {copula} at an extreme", max_chars=56)
    return _shorten_without_ellipsis(_strip_news_prefix(sentence), max_chars=56)


def _trim_trailing_stopwords(text: str) -> str:
    parts = [p for p in _normalize_render_text(text).split(" ") if p]
    while parts and parts[-1].lower() in TRAILING_STOPWORDS:
        parts.pop()
    return " ".join(parts).strip()


def _is_low_signal_phrase(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    if not lower:
        return True
    if lower.startswith(("it's official", "breaking", "update", "new chart", "alert")):
        return True
    if "one of the most anticipated rulings" in lower:
        return True
    if lower in {"chart context", "us trend snapshot"}:
        return True
    return False


def _clean_subject_for_headline(*, subject: str, sentence: str) -> str:
    subject_core = _trim_trailing_stopwords(_shorten_without_ellipsis(_strip_news_prefix(subject), max_chars=40))
    if subject_core:
        subject_core = re.sub(
            rf"\b(?:{'|'.join(re.escape(v) for v in SUBJECT_TRAILING_ACTION_VERBS)})\b(?:\s+\w+){{0,2}}$",
            "",
            subject_core,
            flags=re.IGNORECASE,
        ).strip(" ,:-")
        subject_core = _trim_trailing_stopwords(subject_core)
    if len(subject_core.split()) >= 2:
        return subject_core

    sentence_core = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(sentence), flags=re.IGNORECASE).strip()
    sentence_core = re.sub(
        r"\b(?:surged|rose|jumped|climbed|rebounded|accelerated|increased|grew|fell|dropped|declined|slowed|rolled over|sank|contracted|decelerated|sold|bought|buying|selling|hit|reached|stands at|is at)\b.*$",
        "",
        sentence_core,
        flags=re.IGNORECASE,
    ).strip(" ,:-")
    sentence_core = _trim_trailing_stopwords(_shorten_without_ellipsis(sentence_core, max_chars=40))
    if len(sentence_core.split()) >= 2:
        return sentence_core
    return subject_core or "US data"


def _subject_is_plural(subject: str) -> bool:
    lower = _normalize_render_text(subject).lower()
    if not lower:
        return False
    if " and " in lower:
        return True
    words = [w for w in re.split(r"\s+", lower) if w]
    if not words:
        return False
    tail = words[-1].strip(".,:;")
    if tail in PLURAL_SUBJECT_HINTS:
        return True
    if tail.endswith("s") and not tail.endswith(("ss", "is", "us")):
        return True
    return False


def _has_incoherent_headline(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    if not lower:
        return True
    bad_patterns = (
        r"\b(a|an|the)\s+(is|are|was|were)\b",
        r"\b(sold|bought|buying|selling)\s+(a|an|the)\s+(is|are|was|were)\b",
        r"\b(is|are|was|were)\s+(is|are|was|were)\b",
    )
    return any(re.search(pat, lower) is not None for pat in bad_patterns)


def _keyword_style_override(candidate: Candidate) -> tuple[str, str, str] | None:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    if "tariff" in merged and ("customs" in merged or "duties" in merged):
        return (
            "US tariff receipts are surging",
            "Monthly US customs duties (US$B)",
            "US customs-duty collections just hit a new high.",
        )
    return None


def _extract_chart_title_hint_via_vision(candidate: Candidate) -> str | None:
    if OpenAI is None:
        return None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    image_url = (candidate.image_url or "").strip()
    if not api_key or not image_url:
        return None
    payload, mime = _fetch_image_bytes(image_url)
    if not payload:
        return None
    try:
        client = OpenAI(api_key=api_key)
        model = os.environ.get("COATUE_CLAW_X_CHART_TITLE_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
        b64 = base64.b64encode(payload).decode("ascii")
        data_url = f"data:{mime};base64,{b64}"
        response = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You extract concise chart text cues."},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Read this chart image and return strict JSON: "
                                '{"chart_title":"...", "metric_label":"..."} '
                                "Use visible chart title and metric label only."
                            ),
                        },
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
        )
        raw = ""
        if response.choices and response.choices[0].message:
            raw = str(response.choices[0].message.content or "").strip()
        if not raw:
            return None
        parsed = json.loads(raw)
    except Exception:
        return None
    hint = _shorten_without_ellipsis(str(parsed.get("chart_title") or ""), max_chars=80)
    return hint or None


def _sanitize_style_copy(*, candidate: Candidate, headline: str, chart_label: str, takeaway: str) -> tuple[str, str, str]:
    override = _keyword_style_override(candidate)
    if override is not None:
        headline, chart_label, takeaway = override

    headline = _trim_trailing_stopwords(_shorten_without_ellipsis(headline, max_chars=48))
    chart_label = _trim_trailing_stopwords(_shorten_without_ellipsis(chart_label, max_chars=56))
    takeaway = _trim_trailing_stopwords(_shorten_without_ellipsis(takeaway, max_chars=62))

    source_text = _normalize_render_text(candidate.text or candidate.title)
    source_text = re.sub(r"^@\w+:\s*", "", _strip_news_prefix(source_text), flags=re.IGNORECASE).strip()
    need_hint = _is_low_signal_phrase(headline) or _is_low_signal_phrase(chart_label) or _is_low_signal_phrase(takeaway)
    chart_hint = _extract_chart_title_hint_via_vision(candidate) if need_hint else None
    merged_hint = _normalize_render_text(f"{chart_hint or ''} {source_text}").lower()

    if _is_low_signal_phrase(headline):
        subject = _shorten_without_ellipsis(source_text, max_chars=36)
        if "tariff" in merged_hint or "customs" in merged_hint or "duties" in merged_hint:
            headline = "US tariff receipts are surging"
        elif chart_hint:
            headline = _shorten_without_ellipsis(_strip_news_prefix(chart_hint), max_chars=48)
        else:
            headline = _shorten_without_ellipsis(f"{subject} trend is accelerating", max_chars=48) if subject else "US trend is inflecting"
        headline = _trim_trailing_stopwords(headline)

    if _is_low_signal_phrase(chart_label):
        if "tariff" in merged_hint or "customs" in merged_hint or "duties" in merged_hint:
            chart_label = "Monthly US customs duties (US$B)"
        elif chart_hint:
            chart_label = _trim_trailing_stopwords(_shorten_without_ellipsis(_strip_news_prefix(chart_hint), max_chars=56))

    if _is_low_signal_phrase(takeaway):
        merged_context = _normalize_render_text(f"{headline} {chart_label} {merged_hint}").lower()
        if "tariff" in merged_context or "customs" in merged_context or "duties" in merged_context:
            takeaway = "US customs-duty collections just hit a new high."
        elif chart_hint:
            takeaway = _shorten_without_ellipsis(_strip_news_prefix(chart_hint), max_chars=62)
        else:
            core = _shorten_without_ellipsis(source_text, max_chars=62)
            takeaway = core or "US data trend moved sharply higher."
        takeaway = _trim_trailing_stopwords(takeaway)
    if _has_incoherent_headline(headline):
        merged_context = _normalize_render_text(f"{source_text} {chart_hint or ''}").lower()
        if "institutional" in merged_context and ("seller" in merged_context or "sold" in merged_context):
            headline = "Institutional selling is at an extreme"
        elif "institutional" in merged_context and ("buyer" in merged_context or "bought" in merged_context):
            headline = "Institutional buying is accelerating"
        elif "seller" in merged_context or "sold" in merged_context:
            headline = "Selling pressure is at an extreme"
        elif "buyer" in merged_context or "bought" in merged_context:
            headline = "Buying pressure is accelerating"
        elif chart_hint:
            headline = _shorten_without_ellipsis(_strip_news_prefix(chart_hint), max_chars=48)
        else:
            headline = "US trend is at an extreme"
        headline = _trim_trailing_stopwords(_shorten_without_ellipsis(headline, max_chars=48))
    return headline, chart_label, takeaway


def _employees_robots_takeaway(sentence: str) -> str:
    s = _normalize_render_text(sentence)
    lower = s.lower()
    emp_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+employees", lower)
    rob_m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*million\s+robots", lower)
    if emp_m and rob_m:
        emp = emp_m.group(1)
        rob = rob_m.group(1)
        return _shorten_without_ellipsis(f"Amazon has {emp}M employees and {rob}M robots deployed.", max_chars=68)
    if "ratio" in lower and "declined" in lower:
        return _shorten_without_ellipsis("Amazon's human-to-robot ratio is tightening quickly.", max_chars=68)
    return _shorten_without_ellipsis("Amazon is scaling robots faster than employee growth.", max_chars=68)


def _llm_title_enabled() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_LLM_TITLES_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _require_reconstruction() -> bool:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_REQUIRE_REBUILD", "1") or "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _synthesize_style_via_llm(candidate: Candidate) -> dict[str, str] | None:
    if not _llm_title_enabled():
        return None
    if OpenAI is None:
        return None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    text = _normalize_render_text(candidate.text)
    title = _normalize_render_text(candidate.title)
    if not (text or title):
        return None
    model = os.environ.get("COATUE_CLAW_X_CHART_TITLE_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
    prompt = (
        "Create Coatue-style chart copy from this X post.\n"
        "Return strict JSON with keys: headline, chart_label, takeaway.\n"
        "Rules:\n"
        "- headline: <=56 chars, narrative/thematic takeaway.\n"
        "- chart_label: <=62 chars, technical description of what chart shows.\n"
        "- takeaway: <=68 chars, plain language.\n"
        "- No @handles. No 'BREAKING'. No ellipsis. No emojis.\n"
        "- Avoid generic labels like 'Chart Context'.\n"
        f"Post title: {title}\n"
        f"Post text: {text}\n"
    )
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You write concise institutional chart titles."},
                {"role": "user", "content": prompt},
            ],
        )
        raw = ""
        if response.choices and response.choices[0].message:
            raw = str(response.choices[0].message.content or "").strip()
        if not raw:
            return None
        payload = json.loads(raw)
    except Exception as exc:
        logger.debug("LLM style synthesis failed: %s", exc)
        return None

    headline = _shorten_without_ellipsis(str(payload.get("headline") or ""), max_chars=56)
    chart_label = _shorten_without_ellipsis(str(payload.get("chart_label") or ""), max_chars=62)
    takeaway = _shorten_without_ellipsis(str(payload.get("takeaway") or ""), max_chars=68)
    if not headline or not chart_label or not takeaway:
        return None
    if "@" in headline or "@" in chart_label or "@" in takeaway:
        return None
    if "..." in headline or "..." in chart_label or "..." in takeaway:
        return None
    return {"headline": headline, "chart_label": chart_label, "takeaway": takeaway}


def _is_us_relevant_post(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    strong = any(token in lower for token in US_STRONG_SIGNAL_KEYWORDS)
    weak = any(token in lower for token in US_WEAK_SIGNAL_KEYWORDS)
    if re.search(r"\bus\b", lower):
        weak = True
    forex = any(token in lower for token in FOREX_NON_US_KEYWORDS)
    non_us_geo = any(token in lower for token in NON_US_GEOGRAPHY_KEYWORDS)
    if re.search(r"\b[a-z]{3}/[a-z]{3}\b", lower):
        forex = True

    if forex and not strong:
        return False
    if non_us_geo and not (strong or weak):
        return False
    if strong:
        return True
    return weak and not forex


def _truncate_words(text: str, *, max_words: int, max_chars: int) -> str:
    words = [w for w in _normalize_render_text(text).split(" ") if w]
    if not words:
        return ""
    clipped = " ".join(words[:max_words]).strip()
    if len(clipped) > max_chars:
        clipped = _shorten_without_ellipsis(clipped, max_chars=max_chars)
    return clipped.strip(". ")


def _contains_trend_signal(text: str) -> bool:
    lower = _normalize_render_text(text).lower()
    if re.search(r"\b\d+(\.\d+)?%|\b\d+(\.\d+)?x\b|\$\s?\d", lower):
        return True
    return any(token in lower for token in TREND_SIGNAL_KEYWORDS)


class XChartStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()
        self._seed_default_sources()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sources (
                    handle TEXT PRIMARY KEY,
                    priority REAL NOT NULL,
                    trust_score REAL NOT NULL DEFAULT 0,
                    manual INTEGER NOT NULL DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 1,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posted_slots (
                    slot_key TEXT PRIMARY KEY,
                    posted_at_utc TEXT NOT NULL,
                    candidate_key TEXT NOT NULL,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    score REAL NOT NULL,
                    channel TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posted_items (
                    candidate_key TEXT PRIMARY KEY,
                    first_posted_at_utc TEXT NOT NULL,
                    last_posted_at_utc TEXT NOT NULL,
                    posts_count INTEGER NOT NULL DEFAULT 1
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS observed_candidates (
                    candidate_key TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    author TEXT NOT NULL,
                    title TEXT NOT NULL,
                    text TEXT NOT NULL,
                    url TEXT NOT NULL,
                    image_url TEXT,
                    created_at TEXT,
                    engagement INTEGER NOT NULL,
                    source_priority REAL NOT NULL,
                    score REAL NOT NULL,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS post_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_key TEXT NOT NULL,
                    reviewed_at_utc TEXT NOT NULL,
                    candidate_key TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    passed INTEGER NOT NULL,
                    failed_checks_json TEXT NOT NULL,
                    checks_json TEXT NOT NULL,
                    artifact_path TEXT NOT NULL,
                    artifact_size_bytes INTEGER NOT NULL,
                    style_score REAL NOT NULL,
                    style_iteration INTEGER NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_post_reviews_recent ON post_reviews(reviewed_at_utc DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_post_reviews_source ON post_reviews(source_id, reviewed_at_utc DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_observed_candidates_last_seen ON observed_candidates(last_seen_utc DESC);")

    def _seed_default_sources(self) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            for handle, priority in DEFAULT_PRIORITY_SOURCES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO sources (handle, priority, trust_score, manual, active, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, 2, 1, 1, ?, ?)
                    """,
                    (_canonical_handle(handle), float(priority), now, now),
                )

    def upsert_source(self, handle: str, *, priority: float, manual: bool) -> None:
        clean = _canonical_handle(handle)
        if not clean:
            raise XChartError("Invalid source handle.")
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            row = conn.execute("SELECT handle, manual, trust_score FROM sources WHERE handle = ?", (clean,)).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO sources (handle, priority, trust_score, manual, active, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, ?, ?, 1, ?, ?)
                    """,
                    (clean, float(priority), (2.0 if manual else 0.5), (1 if manual else 0), now, now),
                )
                return
            new_manual = 1 if (manual or bool(row["manual"])) else 0
            new_priority = float(priority)
            conn.execute(
                """
                UPDATE sources
                SET priority = ?, manual = ?, active = 1, last_seen_utc = ?
                WHERE handle = ?
                """,
                (new_priority, new_manual, now, clean),
            )

    def note_candidate_observed(self, handle: str, *, engagement: int) -> None:
        clean = _canonical_handle(handle)
        if not clean:
            return
        now = datetime.now(UTC).isoformat()
        boost = 0.05 + min(0.75, max(0.0, float(engagement)) / 1000.0)
        with self._connect() as conn:
            row = conn.execute("SELECT priority, trust_score, manual FROM sources WHERE handle = ?", (clean,)).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO sources (handle, priority, trust_score, manual, active, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, ?, 0, 1, ?, ?)
                    """,
                    (clean, 0.45, boost, now, now),
                )
                return
            trust = float(row["trust_score"] or 0.0) + boost
            priority = float(row["priority"] or 0.5)
            manual = bool(row["manual"])
            if (not manual) and trust >= 3.0:
                priority = max(priority, min(1.2, 0.5 + trust / 5.0))
            conn.execute(
                """
                UPDATE sources
                SET trust_score = ?, priority = ?, active = 1, last_seen_utc = ?
                WHERE handle = ?
                """,
                (trust, priority, now, clean),
            )

    def top_sources(self, *, limit: int = 25) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT handle, priority, trust_score, manual, last_seen_utc
                FROM sources
                WHERE active = 1
                ORDER BY priority DESC, trust_score DESC, manual DESC, handle ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_sources(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.top_sources(limit=limit)

    def was_slot_posted(self, slot_key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM posted_slots WHERE slot_key = ? LIMIT 1", (slot_key,)).fetchone()
        return row is not None

    def was_item_posted_recently(self, candidate_key: str, *, days: int = 30) -> bool:
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM posted_items WHERE candidate_key = ? AND last_posted_at_utc >= ? LIMIT 1",
                (candidate_key, cutoff),
            ).fetchone()
        return row is not None

    def record_post(self, *, slot_key: str, channel: str, candidate: Candidate) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO posted_slots (slot_key, posted_at_utc, candidate_key, url, title, source, score, channel)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slot_key,
                    now,
                    candidate.candidate_key,
                    candidate.url,
                    candidate.title,
                    f"{candidate.source_type}:{candidate.source_id}",
                    float(candidate.score),
                    channel,
                ),
            )
            existing = conn.execute(
                "SELECT candidate_key, posts_count FROM posted_items WHERE candidate_key = ?",
                (candidate.candidate_key,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO posted_items (candidate_key, first_posted_at_utc, last_posted_at_utc, posts_count)
                    VALUES (?, ?, ?, 1)
                    """,
                    (candidate.candidate_key, now, now),
                )
            else:
                conn.execute(
                    "UPDATE posted_items SET last_posted_at_utc = ?, posts_count = posts_count + 1 WHERE candidate_key = ?",
                    (now, candidate.candidate_key),
                )

    def latest_posts(self, *, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT slot_key, posted_at_utc, title, url, source, score, channel
                FROM posted_slots
                ORDER BY posted_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_observed_candidates(self, candidates: list[Candidate]) -> int:
        if not candidates:
            return 0
        now = datetime.now(UTC).isoformat()
        count = 0
        with self._connect() as conn:
            for candidate in candidates:
                row = conn.execute(
                    "SELECT first_seen_utc FROM observed_candidates WHERE candidate_key = ? LIMIT 1",
                    (candidate.candidate_key,),
                ).fetchone()
                first_seen = str(row["first_seen_utc"]) if row is not None else now
                conn.execute(
                    """
                    INSERT OR REPLACE INTO observed_candidates (
                        candidate_key, source_type, source_id, author, title, text, url, image_url, created_at,
                        engagement, source_priority, score, first_seen_utc, last_seen_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate.candidate_key,
                        candidate.source_type,
                        candidate.source_id,
                        candidate.author,
                        candidate.title,
                        candidate.text,
                        candidate.url,
                        candidate.image_url,
                        candidate.created_at,
                        int(candidate.engagement),
                        float(candidate.source_priority),
                        float(candidate.score),
                        first_seen,
                        now,
                    ),
                )
                count += 1
        return count

    def latest_scheduled_posted_at_utc(self) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT posted_at_utc
                FROM posted_slots
                WHERE slot_key NOT LIKE 'manual%%'
                ORDER BY posted_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        value = str(row["posted_at_utc"] or "").strip()
        return value or None

    def observed_candidates_since(self, *, since_utc: str | None, limit: int = 400) -> list[Candidate]:
        safe_limit = max(20, min(2000, int(limit)))
        with self._connect() as conn:
            if since_utc:
                rows = conn.execute(
                    """
                    SELECT candidate_key, source_type, source_id, author, title, text, url, image_url, created_at,
                           engagement, source_priority, score, first_seen_utc, last_seen_utc
                    FROM observed_candidates
                    WHERE last_seen_utc > ?
                    ORDER BY score DESC, last_seen_utc DESC
                    LIMIT ?
                    """,
                    (since_utc, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT candidate_key, source_type, source_id, author, title, text, url, image_url, created_at,
                           engagement, source_priority, score, first_seen_utc, last_seen_utc
                    FROM observed_candidates
                    ORDER BY score DESC, last_seen_utc DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        out: list[Candidate] = []
        for row in rows:
            out.append(
                Candidate(
                    candidate_key=str(row["candidate_key"]),
                    source_type=str(row["source_type"]),
                    source_id=str(row["source_id"]),
                    author=str(row["author"]),
                    title=str(row["title"]),
                    text=str(row["text"]),
                    url=str(row["url"]),
                    image_url=(str(row["image_url"]) if row["image_url"] is not None else None),
                    created_at=(str(row["created_at"]) if row["created_at"] is not None else None),
                    engagement=int(row["engagement"] or 0),
                    source_priority=float(row["source_priority"] or 0.0),
                    score=float(row["score"] or 0.0),
                )
            )
        return out

    def observed_candidates_count_since(self, *, since_utc: str | None) -> int:
        with self._connect() as conn:
            if since_utc:
                row = conn.execute(
                    "SELECT COUNT(1) AS c FROM observed_candidates WHERE last_seen_utc > ?",
                    (since_utc,),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(1) AS c FROM observed_candidates").fetchone()
        return int((row["c"] if row is not None else 0) or 0)

    def prune_observed_candidates(self, *, keep_days: int = 10) -> int:
        cutoff = (datetime.now(UTC) - timedelta(days=max(2, keep_days))).isoformat()
        with self._connect() as conn:
            before = conn.total_changes
            conn.execute("DELETE FROM observed_candidates WHERE last_seen_utc < ?", (cutoff,))
            after = conn.total_changes
        return max(0, int(after - before))

    def record_post_review(
        self,
        *,
        slot_key: str,
        channel: str,
        candidate: Candidate,
        review: dict[str, Any],
    ) -> None:
        now = datetime.now(UTC).isoformat()
        failed = review.get("failed") if isinstance(review.get("failed"), list) else []
        checks = review.get("checks") if isinstance(review.get("checks"), dict) else {}
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO post_reviews (
                    slot_key, reviewed_at_utc, candidate_key, source_id, channel, passed,
                    failed_checks_json, checks_json, artifact_path, artifact_size_bytes, style_score, style_iteration
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slot_key,
                    now,
                    candidate.candidate_key,
                    _canonical_handle(candidate.source_id),
                    channel,
                    1 if bool(review.get("passed")) else 0,
                    json.dumps(failed, sort_keys=True),
                    json.dumps(checks, sort_keys=True),
                    str(review.get("artifact_path") or ""),
                    int(review.get("artifact_size_bytes") or 0),
                    float(review.get("style_score") or 0.0),
                    int(review.get("style_iteration") or 0),
                ),
            )

    def apply_review_feedback(self, *, source_id: str, passed: bool, failed_checks: list[str]) -> None:
        handle = _canonical_handle(source_id)
        if not handle:
            return
        with self._connect() as conn:
            row = conn.execute(
                "SELECT priority, trust_score, manual FROM sources WHERE handle = ? LIMIT 1",
                (handle,),
            ).fetchone()
            if row is None:
                return
            priority = float(row["priority"] or 0.5)
            trust = float(row["trust_score"] or 0.0)
            if passed:
                new_priority = min(2.5, priority + 0.01)
                new_trust = min(12.0, trust + 0.05)
            else:
                penalty = min(0.35, 0.05 * max(1, len(failed_checks)))
                new_priority = max(0.20, priority - penalty)
                new_trust = max(-3.0, trust - (0.12 * max(1, len(failed_checks))))
            conn.execute(
                "UPDATE sources SET priority = ?, trust_score = ? WHERE handle = ?",
                (new_priority, new_trust, handle),
            )

    def recent_review_summary(self, *, limit: int = 20) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT passed, failed_checks_json
                FROM post_reviews
                ORDER BY reviewed_at_utc DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        fail_counts: dict[str, int] = {}
        passed = 0
        for row in rows:
            if int(row["passed"] or 0) == 1:
                passed += 1
            failed_json = str(row["failed_checks_json"] or "[]")
            try:
                failed = json.loads(failed_json)
            except Exception:
                failed = []
            if isinstance(failed, list):
                for item in failed:
                    key = str(item).strip()
                    if not key:
                        continue
                    fail_counts[key] = int(fail_counts.get(key, 0)) + 1
        return {
            "reviews": len(rows),
            "pass_count": passed,
            "fail_count": max(0, len(rows) - passed),
            "top_fail_checks": sorted(fail_counts.items(), key=lambda kv: kv[1], reverse=True)[:5],
        }


def _http_json(*, url: str, headers: dict[str, str], params: dict[str, str]) -> dict[str, Any]:
    full_url = f"{url}?{urlencode(params)}"
    req = Request(full_url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            payload = resp.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise XChartError(f"X API request failed ({exc.code}): {detail[:500]}") from exc
    except URLError as exc:
        raise XChartError(f"X API request failed: {exc.reason}") from exc
    try:
        data = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise XChartError("Failed to parse X API response.") from exc
    if isinstance(data, dict) and data.get("errors"):
        raise XChartError(f"X API errors: {data['errors']}")
    return data if isinstance(data, dict) else {}


def _x_search_recent(query: str, *, hours: int, max_results: int, token: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    params = {
        "query": query,
        "start_time": start_time,
        "max_results": str(max(10, min(100, max_results))),
        "tweet.fields": "author_id,created_at,public_metrics,attachments,lang",
        "expansions": "author_id,attachments.media_keys",
        "user.fields": "name,username,verified",
        "media.fields": "type,url,preview_image_url,width,height",
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return _http_json(
        url=f"{_x_api_base()}/2/tweets/search/recent",
        headers=headers,
        params=params,
    )


def _parse_x_candidates(payload: dict[str, Any], *, priority_by_handle: dict[str, float]) -> list[Candidate]:
    includes = payload.get("includes") if isinstance(payload.get("includes"), dict) else {}
    users_by_id: dict[str, dict[str, Any]] = {}
    for user in includes.get("users", []) if isinstance(includes.get("users"), list) else []:
        if isinstance(user, dict):
            uid = str(user.get("id") or "").strip()
            if uid:
                users_by_id[uid] = user
    media_by_key: dict[str, dict[str, Any]] = {}
    for media in includes.get("media", []) if isinstance(includes.get("media"), list) else []:
        if isinstance(media, dict):
            key = str(media.get("media_key") or "").strip()
            if key:
                media_by_key[key] = media

    out: list[Candidate] = []
    rows = payload.get("data")
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        tweet_id = str(row.get("id") or "").strip()
        text = str(row.get("text") or "").strip()
        if not tweet_id or not text:
            continue
        author_id = str(row.get("author_id") or "").strip()
        user = users_by_id.get(author_id, {})
        handle = _canonical_handle(str(user.get("username") or ""))
        if not handle:
            continue
        media_url: str | None = None
        attachments = row.get("attachments")
        media_keys: list[str] = []
        if isinstance(attachments, dict) and isinstance(attachments.get("media_keys"), list):
            media_keys = [str(x) for x in attachments.get("media_keys") if str(x).strip()]
        for mk in media_keys:
            media = media_by_key.get(mk) or {}
            mtype = str(media.get("type") or "")
            if mtype != "photo":
                continue
            media_url = str(media.get("url") or media.get("preview_image_url") or "").strip() or None
            if media_url:
                break
        if not media_url:
            continue

        metrics = row.get("public_metrics") if isinstance(row.get("public_metrics"), dict) else {}
        engagement = 0
        for key in ("like_count", "retweet_count", "reply_count", "quote_count"):
            try:
                engagement += int(metrics.get(key, 0) or 0)
            except (TypeError, ValueError):
                pass
        created_at = str(row.get("created_at")) if row.get("created_at") else None
        url = f"https://x.com/{handle}/status/{tweet_id}"
        title = _build_x_title(handle=handle, text=text)
        if not _is_chart_like_post(text, handle=handle):
            continue
        if not _is_us_relevant_post(f"{title} {text}"):
            continue
        priority = float(priority_by_handle.get(handle.lower(), 0.45))
        score = _score_candidate(
            title=title,
            text=text,
            engagement=engagement,
            source_priority=priority,
            created_at=created_at,
            has_image=True,
        )
        out.append(
            Candidate(
                candidate_key=f"x:{tweet_id}",
                source_type="x",
                source_id=handle,
                author=f"@{handle}",
                title=title,
                text=text,
                url=url,
                image_url=media_url,
                created_at=created_at,
                engagement=engagement,
                source_priority=priority,
                score=score,
            )
        )
    out.sort(key=lambda c: c.score, reverse=True)
    return out


def _parse_x_post_url(post_url: str) -> tuple[str, str] | None:
    m = re.search(
        r"https?://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)",
        post_url.strip(),
        re.IGNORECASE,
    )
    if not m:
        return None
    handle = _canonical_handle(m.group(1))
    tweet_id = m.group(2)
    if not handle or not tweet_id:
        return None
    return handle, tweet_id


def _fetch_vxtwitter_post_candidate(*, handle: str, tweet_id: str) -> Candidate | None:
    api_url = f"https://api.vxtwitter.com/{handle}/status/{tweet_id}"
    req = Request(api_url, headers={"User-Agent": "coatue-claw/1.0", "Accept": "application/json"}, method="GET")
    try:
        with urlopen(req, timeout=25) as resp:
            payload = json.loads((resp.read() or b"{}").decode("utf-8", errors="ignore"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    media_urls = payload.get("mediaURLs")
    image_url = None
    if isinstance(media_urls, list):
        for item in media_urls:
            value = str(item or "").strip()
            if value:
                image_url = value
                break
    if not image_url:
        media_extended = payload.get("media_extended")
        if isinstance(media_extended, list):
            for item in media_extended:
                if not isinstance(item, dict):
                    continue
                value = str(item.get("url") or item.get("thumbnail_url") or "").strip()
                if value:
                    image_url = value
                    break
    if not image_url:
        return None

    text = _normalize_render_text(str(payload.get("text") or "").strip())
    if not text:
        return None
    user_screen = _canonical_handle(str(payload.get("user_screen_name") or handle))
    if not user_screen:
        user_screen = handle
    engagement = 0
    for key in ("likes", "retweets", "replies", "qrt"):
        try:
            engagement += int(payload.get(key, 0) or 0)
        except Exception:
            continue
    created_at = None
    raw_date = str(payload.get("date") or "").strip()
    if raw_date:
        for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                created_at = datetime.strptime(raw_date, fmt).astimezone(UTC).isoformat()
                break
            except Exception:
                continue
    priority = 1.0
    score = _score_candidate(
        source_priority=priority,
        engagement=engagement,
        created_at=created_at,
        title=text,
        text=text,
        has_image=True,
    )
    return Candidate(
        candidate_key=f"x:{tweet_id}",
        source_type="x",
        source_id=user_screen,
        author=f"@{user_screen}",
        title=text,
        text=text,
        url=f"https://x.com/{user_screen}/status/{tweet_id}",
        image_url=image_url,
        created_at=created_at,
        engagement=engagement,
        source_priority=priority,
        score=score,
    )


def _is_chart_like_post(text: str, *, handle: str) -> bool:
    lower = (text or "").lower()
    signal = 0
    for token in CHART_SIGNAL_KEYWORDS:
        if token in lower:
            signal += 1
    for token in THEME_KEYWORDS:
        if token.lower() in lower:
            signal += 1
    if re.search(r"\b\d+(\.\d+)?%|\b\d+(\.\d+)?x\b|\$\s?\d", lower):
        signal += 1
    if re.search(r"\b\d{1,3}(?:,\d{3})+\b", lower):
        signal += 1

    high_trust_handles = {
        "fiscal_ai",
        "cloudedjudgment",
        "charliebilello",
        "ourworldindata",
        "bespokeinvest",
    }
    if handle.lower() in high_trust_handles and signal >= 1:
        return True
    return signal >= 2


def _build_x_title(*, handle: str, text: str) -> str:
    snippet = _shorten_without_ellipsis(_normalize_render_text(text), max_chars=95)
    return f"@{handle}: {snippet}".strip()


def _keyword_score(text: str) -> float:
    lower = (text or "").lower()
    score = 0.0
    for kw in THEME_KEYWORDS:
        if kw.lower() in lower:
            score += 2.0
    return min(14.0, score)


def _freshness_score(created_at: str | None) -> float:
    if not created_at:
        return 0.0
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return 0.0
    age_hours = max(0.0, (datetime.now(UTC) - dt).total_seconds() / 3600.0)
    return max(0.0, 16.0 - (age_hours / 3.0))


def _score_candidate(
    *,
    title: str,
    text: str,
    engagement: int,
    source_priority: float,
    created_at: str | None,
    has_image: bool,
) -> float:
    engagement_component = math.log1p(max(0, engagement)) * 6.0
    priority_component = source_priority * 20.0
    keyword_component = _keyword_score(f"{title} {text}")
    freshness_component = _freshness_score(created_at)
    image_component = 5.0 if has_image else 0.0
    return priority_component + engagement_component + keyword_component + freshness_component + image_component


def _chunks(items: list[str], size: int) -> list[list[str]]:
    out: list[list[str]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _fetch_x_candidates_from_sources(*, handles: list[str], token: str, hours: int = 48) -> list[Candidate]:
    if not handles:
        return []
    priority_map = {_canonical_handle(h).lower(): p for h, p in DEFAULT_PRIORITY_SOURCES}
    all_handles = [_canonical_handle(h) for h in handles if _canonical_handle(h)]
    out: list[Candidate] = []
    for handle in all_handles:
        query = f"(from:{handle}) has:images -is:retweet -is:reply lang:en"
        try:
            payload = _x_search_recent(query, hours=hours, max_results=25, token=token)
        except XChartError as exc:
            msg = str(exc).lower()
            if "invalid username value" in msg or "not a parsable user name" in msg:
                logger.warning("Skipping invalid source handle for x-chart scout: @%s", handle)
                continue
            raise
        parsed = _parse_x_candidates(payload, priority_by_handle=priority_map)
        if parsed:
            out.extend(parsed)
    return out


def _discover_new_sources(*, token: str) -> list[tuple[str, int]]:
    query = os.environ.get(
        "COATUE_CLAW_X_CHART_DISCOVERY_QUERY",
        "(ai OR software OR semiconductor OR macro OR consumer) has:images -is:retweet -is:reply lang:en",
    ).strip()
    payload = _x_search_recent(query, hours=24, max_results=60, token=token)
    parsed = _parse_x_candidates(payload, priority_by_handle={})
    seen: dict[str, int] = {}
    for item in parsed:
        handle = _canonical_handle(item.source_id)
        if not handle:
            continue
        prev = seen.get(handle, 0)
        if item.engagement > prev:
            seen[handle] = item.engagement
    ranked = sorted(seen.items(), key=lambda pair: pair[1], reverse=True)
    return ranked[:12]


def _fetch_visualcapitalist_candidates(*, max_items: int = 20) -> list[Candidate]:
    feed_url = os.environ.get("COATUE_CLAW_VISUALCAPITALIST_FEED_URL", "https://www.visualcapitalist.com/feed/").strip()
    req = Request(feed_url, headers={"User-Agent": "coatue-claw/1.0"}, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            content = resp.read()
    except Exception as exc:
        logger.warning("visualcapitalist feed fetch failed: %s", exc)
        return []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    ns = {
        "content": "http://purl.org/rss/1.0/modules/content/",
        "media": "http://search.yahoo.com/mrss/",
    }
    out: list[Candidate] = []
    for item in root.findall(".//item")[:max_items]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = (item.findtext("description") or "").strip()
        text = re.sub(r"<[^>]+>", " ", desc)
        text = re.sub(r"\s+", " ", text).strip()
        if not title or not link:
            continue
        if not _is_us_relevant_post(f"{title} {text}"):
            continue
        image_url: str | None = None
        media_content = item.find("media:content", ns)
        if media_content is not None:
            image_url = (media_content.attrib.get("url") or "").strip() or None
        if not image_url:
            m = re.search(r"""<img[^>]+src=["']([^"']+)["']""", desc, re.IGNORECASE)
            if m:
                image_url = m.group(1).strip()

        score = _score_candidate(
            title=title,
            text=text,
            engagement=40,  # neutral baseline for trusted curated source
            source_priority=1.25,
            created_at=None,
            has_image=bool(image_url),
        )
        out.append(
            Candidate(
                candidate_key=f"vc:{hash(link)}",
                source_type="web",
                source_id="visualcapitalist.com",
                author="Visual Capitalist",
                title=title,
                text=text,
                url=link,
                image_url=image_url,
                created_at=pub or None,
                engagement=40,
                source_priority=1.25,
                score=score,
            )
        )
    out.sort(key=lambda c: c.score, reverse=True)
    return out


def _slot_key(*, now_local: datetime, windows: list[tuple[int, int]], manual: bool) -> str | None:
    if manual:
        return f"manual-{now_local.strftime('%Y%m%d-%H%M%S')}"
    for hour, minute in windows:
        if now_local.hour == hour and abs(now_local.minute - minute) <= 20:
            return f"{now_local.strftime('%Y-%m-%d')}-{hour:02d}:{minute:02d}"
    return None


def _dedupe_candidates(candidates: list[Candidate]) -> list[Candidate]:
    seen: set[str] = set()
    out: list[Candidate] = []
    for item in sorted(candidates, key=lambda c: c.score, reverse=True):
        key = item.url.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _normalize_posted_source(source: str) -> str:
    raw = str(source or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        left, right = raw.split(":", 1)
        if left.strip().lower() == "x":
            return _canonical_handle(right)
    return _canonical_handle(raw) or raw.lower()


def _source_variety_params() -> tuple[int, float]:
    lookback_raw = (os.environ.get("COATUE_CLAW_X_CHART_SOURCE_VARIETY_LOOKBACK", "6") or "6").strip()
    floor_raw = (os.environ.get("COATUE_CLAW_X_CHART_SOURCE_VARIETY_SCORE_FLOOR", "0.90") or "0.90").strip()
    try:
        lookback = max(2, min(20, int(lookback_raw)))
    except Exception:
        lookback = 6
    try:
        floor = float(floor_raw)
    except Exception:
        floor = 0.90
    floor = max(0.75, min(0.99, floor))
    return lookback, floor


def _pick_winner(*, store: XChartStore, candidates: list[Candidate]) -> Candidate | None:
    pool: list[Candidate] = []
    for item in candidates:
        if store.was_item_posted_recently(item.candidate_key, days=30):
            continue
        pool.append(item)
    if not pool:
        return None

    # Keep "highest score" behavior but add source variety when alternatives are near the top score.
    top = pool[0]
    lookback, floor = _source_variety_params()
    recent = store.latest_posts(limit=lookback)
    counts: dict[str, int] = {}
    for row in recent:
        key = _normalize_posted_source(str(row.get("source") or ""))
        if not key:
            continue
        counts[key] = int(counts.get(key, 0)) + 1

    score_floor = float(top.score) * float(floor)
    near_top = [c for c in pool if float(c.score) >= score_floor]
    if len(near_top) <= 1:
        return top

    def _rank_key(c: Candidate) -> tuple[int, float]:
        source_key = _canonical_handle(c.source_id) or c.source_id.lower()
        return (int(counts.get(source_key, 0)), -float(c.score))

    near_top.sort(key=_rank_key)
    return near_top[0]


def _build_takeaways(candidate: Candidate) -> list[str]:
    text = _strip_news_prefix(candidate.text)
    title = _normalize_render_text(candidate.title)
    if _is_employees_robots_chart(candidate):
        excerpt = _employees_robots_takeaway(text or title)
    else:
        excerpt = _truncate_words(text or title, max_words=9, max_chars=68)
    if not excerpt:
        excerpt = "Fresh US-focused chart signal from a prioritized source."
    tone_line = "Keep the takeaway simple and explicit for fast read in a feed."
    return [
        excerpt,
        "US relevance check passed and trend is explicit.",
        tone_line,
    ]


def _build_style_draft(candidate: Candidate, *, iteration: int) -> StyleDraft:
    title_text = _normalize_render_text(candidate.title)
    body_text = _strip_news_prefix(candidate.text)
    first_sentence = _extract_first_sentence(body_text or title_text)
    title_core = re.sub(r"^@\w+:\s*", "", title_text).strip()
    first_core = re.sub(r"^@\w+:\s*", "", first_sentence).strip()
    mode_hint = _mode_hint_from_text(candidate)
    subject, verb = _extract_subject_and_verb(first_core or title_core or title_text)
    chart_label = _synthesize_chart_label(subject=subject, sentence=first_core or title_core or title_text, mode_hint=mode_hint)
    narrative = _synthesize_narrative_title(subject=subject, verb=verb, sentence=first_core or title_core or title_text)
    is_employee_robot = _is_employees_robots_chart(candidate)

    llm_style = _synthesize_style_via_llm(candidate) if iteration == 1 else None

    if iteration == 1 and llm_style:
        headline = _shorten_without_ellipsis(llm_style["headline"], max_chars=56)
        chart_label = _shorten_without_ellipsis(llm_style["chart_label"], max_chars=62)
        takeaway = _employees_robots_takeaway(first_core or body_text or title_text) if is_employee_robot else _shorten_without_ellipsis(llm_style["takeaway"], max_chars=68)
        why_now = "Narrative + technical label generated for feed readability."
    elif iteration == 1:
        headline = _shorten_without_ellipsis(narrative, max_chars=56)
        takeaway = _employees_robots_takeaway(first_core or body_text or title_text) if is_employee_robot else _truncate_words(body_text or title_core or title_text, max_words=9, max_chars=68)
        why_now = "Clear US trend; chart carries the story."
    elif iteration == 2:
        headline = _shorten_without_ellipsis(_synthesize_narrative_title(subject=subject, verb="", sentence=title_core or first_core or title_text), max_chars=52)
        takeaway = _truncate_words(first_core or body_text, max_words=8, max_chars=62)
        why_now = "Fast read in a feed."
    else:
        anchor = _shorten_without_ellipsis(subject or first_core or title_core or title_text, max_chars=42)
        headline = anchor or "US Trend Snapshot"
        takeaway = _truncate_words(body_text or title_core or title_text, max_words=7, max_chars=56)
        why_now = "Simple trend read."

    headline, chart_label, takeaway = _sanitize_style_copy(
        candidate=candidate,
        headline=headline or "US Trend Snapshot",
        chart_label=chart_label or "Chart Context",
        takeaway=takeaway or "New US-facing data point with clear directional movement.",
    )

    combined = " ".join([headline, takeaway, why_now]).strip()
    checks = {
        "us_relevant": _is_us_relevant_post(f"{candidate.title} {candidate.text}"),
        "headline_short": bool(headline) and len(headline) <= 72,
        "headline_grammar": not _has_incoherent_headline(headline),
        "takeaway_short": bool(takeaway) and len(takeaway) <= 68,
        "trend_explicit": _contains_trend_signal(f"{candidate.title} {candidate.text}"),
        "plain_language": not any(term in combined.lower() for term in SLIDE_JARGON_KEYWORDS),
        "clean_characters": "\ufffd" not in combined and "??" not in combined and "  " not in combined,
        "graph_first_copy": len(combined.split()) <= 30,
    }
    score = float(sum(1.0 for passed in checks.values() if passed))
    return StyleDraft(
        headline=_shorten_without_ellipsis(headline or "US Trend Snapshot", max_chars=58),
        chart_label=_shorten_without_ellipsis(chart_label or "Chart Context", max_chars=62),
        takeaway=takeaway or "New US-facing data point with clear directional movement.",
        why_now=why_now,
        iteration=iteration,
        checks=checks,
        score=score,
    )


def _select_style_draft(candidate: Candidate, *, max_iterations: int = 3) -> StyleDraft:
    best = _build_style_draft(candidate, iteration=1)
    target_score = 6.0
    if best.score >= target_score and best.checks.get("us_relevant", False):
        return best
    for iteration in range(2, max_iterations + 1):
        draft = _build_style_draft(candidate, iteration=iteration)
        if draft.score > best.score:
            best = draft
        if draft.score >= target_score and draft.checks.get("us_relevant", False):
            return draft
    return best


def _has_reconstructable_chart_data(candidate: Candidate) -> bool:
    image = _safe_image_from_url(candidate.image_url)
    mode = _infer_chart_mode(candidate=candidate, image=image)
    if mode == "bar":
        vision_bars = _extract_rebuilt_bars_via_vision(candidate=candidate)
        if vision_bars is not None and (not _bar_data_quality_errors(candidate=candidate, bars=vision_bars)):
            return True
        cv_bars = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False)
        return cv_bars is not None and (not _bar_data_quality_errors(candidate=candidate, bars=cv_bars))
    rebuilt = _extract_rebuilt_series(candidate=candidate, image=image)
    return bool(rebuilt)


def _fetch_image_bytes(url: str | None) -> tuple[bytes | None, str]:
    if not url:
        return None, "image/png"
    req = Request(url, headers={"User-Agent": "coatue-claw/1.0", "Accept": "image/*"}, method="GET")
    try:
        with urlopen(req, timeout=30) as resp:
            payload = resp.read() or b""
            ctype = str(resp.headers.get("Content-Type") or "image/png").split(";")[0].strip() or "image/png"
    except Exception:
        return None, "image/png"
    if not payload:
        return None, ctype
    return payload, ctype


def _guess_image_extension(*, image_url: str | None, content_type: str) -> str:
    ctype = (content_type or "").strip().lower()
    if ctype in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    if ctype == "image/png":
        return ".png"
    if ctype == "image/webp":
        return ".webp"
    if image_url:
        path = urlparse(image_url).path or ""
        suffix = Path(path).suffix.lower()
        if suffix in {".jpg", ".jpeg"}:
            return ".jpg"
        if suffix in {".png", ".webp"}:
            return suffix
    return ".png"


def _write_source_chart_image(*, candidate: Candidate, slot_key: str) -> Path:
    payload, ctype = _fetch_image_bytes(candidate.image_url)
    if not payload:
        raise XChartError("No chart image found for this X post.")
    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = _guess_image_extension(image_url=candidate.image_url, content_type=ctype)
    out_path = output_dir / f"{slot_key}-source{suffix}"
    out_path.write_bytes(payload)
    return out_path


def _render_source_snip_card(*, candidate: Candidate, slot_key: str, style_draft: StyleDraft, source_path: Path) -> Path:
    try:
        import matplotlib.image as mpimg
        import matplotlib.pyplot as plt
        from matplotlib.lines import Line2D
    except Exception as exc:
        raise XChartError("Chart card renderer unavailable (matplotlib missing).") from exc
    from coatue_claw.valuation_chart import COATUE_FONT_FAMILY

    if not source_path.exists():
        raise XChartError("Source chart image is missing for card render.")
    image = mpimg.imread(str(source_path))
    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-source-card.png"

    plt.rcParams["font.family"] = COATUE_FONT_FAMILY
    fig = plt.figure(figsize=(15, 8.4), facecolor="#DCDDDF")

    headline_text = _shorten_without_ellipsis(_normalize_render_text(style_draft.headline), max_chars=56)
    takeaway_text = _shorten_without_ellipsis(_normalize_render_text(style_draft.takeaway), max_chars=68)

    headline_obj = fig.text(
        0.05,
        0.935,
        headline_text,
        ha="left",
        va="center",
        fontsize=28,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="medium",
    )
    fig.add_artist(Line2D([0.05, 0.95], [0.892, 0.892], transform=fig.transFigure, color="#2F3745", linewidth=1.1))

    # Prevent clipped title by auto-fitting to available card width.
    for _ in range(8):
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        fig_bbox = fig.bbox
        max_x = fig_bbox.x0 + (fig_bbox.width * 0.95)

        h_bb = headline_obj.get_window_extent(renderer=renderer)
        if h_bb.x1 > max_x:
            current_size = float(headline_obj.get_fontsize())
            if current_size > 22.0:
                headline_obj.set_fontsize(current_size - 1.0)
            headline_text = _shorten_without_ellipsis(headline_text, max_chars=max(28, len(headline_text) - 4))
            headline_obj.set_text(headline_text)
            continue
        break

    # Hard fail-safe: if still overflowing after iterative fitting, force concise one-line copy.
    for _ in range(12):
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        fig_bbox = fig.bbox
        max_x = fig_bbox.x0 + (fig_bbox.width * 0.95)

        h_bb = headline_obj.get_window_extent(renderer=renderer)
        if h_bb.x1 > max_x:
            headline_obj.set_fontsize(max(18.0, float(headline_obj.get_fontsize()) - 1.0))
            headline_text = _shorten_without_ellipsis(headline_text, max_chars=max(22, len(headline_text) - 3))
            headline_obj.set_text(headline_text)
            continue
        break

    chart_ax = fig.add_axes([0.05, 0.19, 0.90, 0.68], facecolor="#F4F5F6")
    chart_ax.imshow(image)
    chart_ax.set_xticks([])
    chart_ax.set_yticks([])
    for spine in chart_ax.spines.values():
        spine.set_color("#E1E4EA")
        spine.set_linewidth(1.2)

    fig.text(
        0.05,
        0.115,
        f"Takeaway: {takeaway_text}",
        fontsize=10.8,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="bold",
        va="top",
    )
    fig.text(
        0.05,
        0.045,
        f"Source: {candidate.url}",
        fontsize=9.2,
        color="#4B5563",
        family=COATUE_FONT_FAMILY,
    )

    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_path


def _safe_image_from_url(url: str | None):
    if not url:
        return None
    payload, _ = _fetch_image_bytes(url)
    if not payload:
        return None

    try:
        import matplotlib.image as mpimg

        return mpimg.imread(io.BytesIO(payload))
    except Exception:
        pass

    try:
        from PIL import Image
        import numpy as np

        image = Image.open(io.BytesIO(payload)).convert("RGB")
        return np.asarray(image)
    except Exception:
        return None


def _infer_series_labels(*, candidate: Candidate, count: int) -> list[str]:
    merged = f"{candidate.title} {candidate.text}".lower()
    if count >= 2 and ("stockholder" in merged or "asset owner" in merged):
        return ["Asset owners", "Non-asset owners"][:count]
    if count >= 2 and ("bull" in merged and "bear" in merged):
        return ["Bull trend", "Bear trend"][:count]
    return [f"Series {idx}" for idx in range(1, count + 1)]


def _extract_rebuilt_series(*, candidate: Candidate, image) -> list[RebuiltSeries]:
    try:
        import numpy as np
        from matplotlib.colors import rgb_to_hsv
    except Exception:
        return []

    if image is None:
        return []
    arr = np.asarray(image)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return []
    if arr.dtype.kind in {"u", "i"}:
        rgb = arr[:, :, :3].astype(float) / 255.0
    else:
        rgb = np.clip(arr[:, :, :3].astype(float), 0.0, 1.0)
    h, w, _ = rgb.shape
    if h < 200 or w < 300:
        return []

    y0, y1 = int(h * 0.18), int(h * 0.90)
    x0, x1 = int(w * 0.08), int(w * 0.96)
    if y1 - y0 < 80 or x1 - x0 < 120:
        return []
    crop = rgb[y0:y1, x0:x1, :]
    ch, cw, _ = crop.shape

    inner_y0, inner_y1 = int(ch * 0.06), int(ch * 0.94)
    inner_x0, inner_x1 = int(cw * 0.04), int(cw * 0.98)
    work = crop[inner_y0:inner_y1, inner_x0:inner_x1, :]
    wh, ww, _ = work.shape
    if wh < 40 or ww < 80:
        return []

    hsv = rgb_to_hsv(work)
    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]
    hue = hsv[:, :, 0]

    color_mask = (sat > 0.25) & (val > 0.12) & (val < 0.95)
    dark_mask = (sat < 0.25) & (val > 0.05) & (val < 0.40)
    color_pixels = int(color_mask.sum())
    if color_pixels < 250:
        return []

    hist, edges = np.histogram(hue[color_mask], bins=24, range=(0.0, 1.0))
    top_bins = np.argsort(hist)[::-1][:4]
    masks: list[tuple[np.ndarray, str, float]] = []
    palette = ("#2F6ABF", "#5AA88A", "#D16F4B", "#7C4D9D")
    for idx, b in enumerate(top_bins):
        if int(hist[b]) < 120:
            continue
        lo, hi = float(edges[b]), float(edges[b + 1])
        mask = color_mask & (hue >= lo) & (hue < hi)
        if mask.sum() < 100:
            continue
        masks.append((mask, palette[idx % len(palette)], 2.8 if idx == 0 else 2.4))
    if dark_mask.sum() > 180:
        masks.append((dark_mask, "#232A35", 2.1))

    def _series_from_mask(mask: np.ndarray) -> list[float]:
        ys = np.full((ww,), np.nan, dtype=float)
        for xi in range(ww):
            y_idx = np.where(mask[:, xi])[0]
            if y_idx.size >= 2:
                ys[xi] = float(np.median(y_idx))
        valid = np.where(~np.isnan(ys))[0]
        if valid.size < max(18, int(ww * 0.15)):
            return []
        all_idx = np.arange(ww, dtype=float)
        interp = np.interp(all_idx, valid.astype(float), ys[valid])
        kernel = np.ones((5,), dtype=float) / 5.0
        smooth = np.convolve(interp, kernel, mode="same")
        return smooth.tolist()

    raw_series: list[tuple[list[float], str, float]] = []
    for mask, color, weight in masks:
        series = _series_from_mask(mask)
        if series:
            raw_series.append((series, color, weight))
    if not raw_series:
        return []

    try:
        import numpy as np

        stacked = np.asarray([s for s, _, _ in raw_series], dtype=float)
    except Exception:
        return []
    y_min = float(np.nanmin(stacked))
    y_max = float(np.nanmax(stacked))
    spread = max(1e-6, y_max - y_min)
    labels = _infer_series_labels(candidate=candidate, count=len(raw_series))
    out: list[RebuiltSeries] = []
    for idx, (series, color, weight) in enumerate(raw_series):
        arr_y = np.asarray(series, dtype=float)
        norm = (1.0 - ((arr_y - y_min) / spread)) * 100.0
        norm = np.clip(norm, 0.0, 100.0)
        x_vals = (np.arange(arr_y.size, dtype=float) / max(1.0, float(arr_y.size - 1))) * 100.0
        out.append(
            RebuiltSeries(
                label=labels[idx] if idx < len(labels) else f"Series {idx+1}",
                x=[float(v) for v in x_vals.tolist()],
                y=[float(v) for v in norm.tolist()],
                color=color,
                weight=weight,
            )
        )
    out.sort(key=lambda s: s.weight, reverse=True)
    return out[:2]


def _looks_like_bar_chart(image) -> bool:
    try:
        import numpy as np
    except Exception:
        return False
    if image is None:
        return False
    arr = np.asarray(image)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return False
    if arr.dtype.kind in {"u", "i"}:
        rgb = arr[:, :, :3].astype(float) / 255.0
    else:
        rgb = np.clip(arr[:, :, :3].astype(float), 0.0, 1.0)
    h, w, _ = rgb.shape
    if h < 200 or w < 280:
        return False
    crop = rgb[int(h * 0.18) : int(h * 0.92), int(w * 0.08) : int(w * 0.96), :]
    ch, cw, _ = crop.shape
    hsv = None
    try:
        from matplotlib.colors import rgb_to_hsv

        hsv = rgb_to_hsv(crop)
    except Exception:
        hsv = None
    if hsv is None:
        gray = crop.mean(axis=2)
        dark = gray < 0.55
        col_density = dark.sum(axis=0).astype(float) / max(1.0, float(ch))
        strong_cols = col_density > 0.22
        transitions = int((strong_cols[1:] != strong_cols[:-1]).sum())
        return int(strong_cols.sum()) >= max(6, int(cw * 0.06)) and transitions >= 6

    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]
    colorful = (sat > 0.45) & (val > 0.20)
    col_density = colorful.sum(axis=0).astype(float) / max(1.0, float(ch))
    strong_cols = col_density > 0.08
    if int(strong_cols.sum()) < max(10, int(cw * 0.07)):
        return False
    transitions = int((strong_cols[1:] != strong_cols[:-1]).sum())
    return transitions >= 8


def _infer_chart_mode(*, candidate: Candidate, image) -> str:
    merged = _normalize_render_text(f"{candidate.title} {candidate.text}").lower()
    if any(token in merged for token in BAR_HINT_KEYWORDS):
        return "bar"
    bars = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False)
    if bars is not None and len(bars.values) >= 4:
        return "bar"
    if _looks_like_bar_chart(image):
        return "bar"
    return "line"


def _extract_rebuilt_bars(*, image, candidate: Candidate | None = None, allow_vision: bool = True) -> RebuiltBars | None:
    if allow_vision and candidate is not None:
        vision = _extract_rebuilt_bars_via_vision(candidate=candidate)
        if vision is not None:
            return vision
    try:
        import numpy as np
    except Exception:
        return None
    if image is None:
        return None
    arr = np.asarray(image)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return None
    if arr.dtype.kind in {"u", "i"}:
        rgb = arr[:, :, :3].astype(float) / 255.0
    else:
        rgb = np.clip(arr[:, :, :3].astype(float), 0.0, 1.0)
    if candidate is not None and _is_employees_robots_chart(candidate):
        grouped = _extract_employees_robots_bars_cv(rgb=rgb, candidate=candidate)
        if grouped is not None:
            return grouped
    h, w, _ = rgb.shape
    if h < 220 or w < 320:
        return None
    crop = rgb[int(h * 0.16) : int(h * 0.93), int(w * 0.07) : int(w * 0.97), :]
    ch, cw, _ = crop.shape
    try:
        from matplotlib.colors import rgb_to_hsv

        hsv = rgb_to_hsv(crop)
    except Exception:
        hsv = None
    if hsv is None:
        return None

    sat = hsv[:, :, 1]
    val = hsv[:, :, 2]
    hue = hsv[:, :, 0]
    colorful = (sat > 0.45) & (val > 0.18)

    # Prefer dominant colored hue (works for Bloomberg-style orange bars).
    if int(colorful.sum()) < max(250, int(ch * cw * 0.008)):
        return None
    hist, edges = np.histogram(hue[colorful], bins=30, range=(0.0, 1.0))
    top_bin = int(np.argmax(hist))
    lo = float(edges[max(0, top_bin - 1)])
    hi = float(edges[min(len(edges) - 1, top_bin + 2)])
    bar_mask = colorful & (hue >= lo) & (hue < hi)
    if int(bar_mask.sum()) < max(180, int(ch * cw * 0.005)):
        bar_mask = colorful

    cols = bar_mask.sum(axis=0)
    col_threshold = max(6, int(ch * 0.06))
    bar_cols = cols >= col_threshold

    spans: list[tuple[int, int]] = []
    start = -1
    min_width = max(3, int(cw * 0.010))
    for i, flag in enumerate(bar_cols.tolist()):
        if flag and start < 0:
            start = i
        elif (not flag) and start >= 0:
            if (i - start) >= min_width:
                spans.append((start, i - 1))
            start = -1
    if start >= 0 and (len(bar_cols) - start) >= min_width:
        spans.append((start, len(bar_cols) - 1))
    if not (4 <= len(spans) <= 40):
        return None

    bottoms: list[float] = []
    tops: list[float] = []
    for s, e in spans:
        seg = bar_mask[:, s : e + 1]
        y_idx = np.where(seg.any(axis=1))[0]
        if y_idx.size == 0:
            continue
        tops.append(float(y_idx.min()))
        bottoms.append(float(y_idx.max()))
    if len(tops) < 3:
        return None
    base_row = float(np.percentile(np.asarray(bottoms, dtype=float), 85))
    values_raw: list[float] = []
    for top in tops:
        values_raw.append(max(0.0, base_row - top))

    if len(values_raw) < 4:
        return None
    primary_values = values_raw
    secondary_values: list[float] | None = None
    require_grouped = _is_employees_robots_chart(candidate)
    # Grouped bars often appear as alternating pairs (for example employees vs robots).
    if len(values_raw) >= 16 and (len(values_raw) % 2 == 0):
        left = values_raw[0::2]
        right = values_raw[1::2]
        if len(left) >= 4 and len(right) == len(left):
            if sum(left) >= sum(right):
                primary_values = left
                secondary_values = right
            else:
                primary_values = right
                secondary_values = left
    if require_grouped and (secondary_values is None):
        return None

    max_v = max(primary_values + (secondary_values or [])) if primary_values else 0.0
    if max_v <= 2.0:
        return None
    norm = [float((v / max_v) * 100.0) for v in primary_values]
    norm_secondary = [float((v / max_v) * 100.0) for v in secondary_values] if secondary_values else None
    labels = _fallback_bar_labels(candidate=candidate, count=len(norm))
    spread = max(norm) - min(norm)
    if spread < 8.0:
        return None
    primary_label = None
    secondary_label = None
    merged = _normalize_render_text(f"{candidate.title if candidate else ''} {candidate.text if candidate else ''}").lower()
    if "employees" in merged and "robots" in merged and secondary_values is not None:
        primary_label = "Employees"
        secondary_label = "Robots"
    bars = RebuiltBars(
        labels=labels,
        values=norm,
        color="#2F6ABF",
        y_label="Index (normalized)",
        normalized=True,
        source="cv",
        confidence=0.62,
        primary_label=primary_label,
        secondary_values=norm_secondary,
        secondary_color="#5AA88A" if norm_secondary is not None else None,
        secondary_label=secondary_label,
    )
    bars = _normalize_grouped_bar_metadata(candidate=candidate, bars=bars)
    if _bar_data_quality_errors(candidate=candidate, bars=bars):
        return None
    return bars


def _render_chart_of_day_style(
    *,
    candidate: Candidate,
    slot_key: str,
    windows_text: str,
    style_draft: StyleDraft,
    qa_sink: dict[str, Any] | None = None,
) -> Path:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import FuncFormatter
    from coatue_claw.valuation_chart import COATUE_FONT_FAMILY

    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-styled.png"

    copy_errors = _style_copy_quality_errors(style_draft)
    if copy_errors:
        raise XChartError(f"Chart copy QA failed: {copy_errors[0]}")

    plt.rcParams["font.family"] = COATUE_FONT_FAMILY

    fig = plt.figure(figsize=(15, 8.4), facecolor="#DCDDDF")
    headline_text = _shorten_without_ellipsis(_normalize_render_text(style_draft.headline), max_chars=52)
    headline_obj = fig.text(
        0.05,
        0.935,
        headline_text,
        ha="left",
        va="center",
        fontsize=27,
        color="#1F2430",
        family=COATUE_FONT_FAMILY,
        weight="medium",
    )
    fig.add_artist(Line2D([0.05, 0.95], [0.892, 0.892], transform=fig.transFigure, color="#2F3745", linewidth=1.1))
    chart_label_text = _shorten_without_ellipsis(_normalize_render_text(style_draft.chart_label), max_chars=62)
    chart_label_obj = fig.text(
        0.05,
        0.876,
        chart_label_text,
        ha="left",
        va="center",
        fontsize=10.8,
        color="#2F3745",
        family=COATUE_FONT_FAMILY,
    )

    chart_ax = fig.add_axes([0.05, 0.20, 0.90, 0.64], facecolor="#F4F5F6")
    chart_ax.set_xticks([])
    chart_ax.set_yticks([])
    for spine in chart_ax.spines.values():
        spine.set_color("#E1E4EA")
        spine.set_linewidth(1.2)

    image = _safe_image_from_url(candidate.image_url)
    vision_bars = _extract_rebuilt_bars_via_vision(candidate=candidate)
    if vision_bars is not None:
        mode = "bar"
        rebuilt_bars = vision_bars
    else:
        mode = _infer_chart_mode(candidate=candidate, image=image)
        rebuilt_bars = _extract_rebuilt_bars(image=image, candidate=candidate, allow_vision=False) if mode == "bar" else None
    if rebuilt_bars is not None:
        rebuilt_bars = _normalize_grouped_bar_metadata(candidate=candidate, bars=rebuilt_bars)
        bar_errors = _bar_data_quality_errors(candidate=candidate, bars=rebuilt_bars)
        if bar_errors:
            raise XChartError(f"Chart QA failed: {bar_errors[0]}")
    rebuilt = _extract_rebuilt_series(candidate=candidate, image=image) if (mode != "bar" and rebuilt_bars is None) else []
    if rebuilt_bars is not None:
        if qa_sink is not None:
            qa_sink["reconstruction_mode"] = "bar"
        try:
            import numpy as np
        except Exception:
            np = None
        if np is None:
            raise XChartError("Chart reconstruction unavailable (numpy missing).")
        else:
            xs = np.arange(len(rebuilt_bars.values))
            if rebuilt_bars.secondary_values and len(rebuilt_bars.secondary_values) == len(rebuilt_bars.values):
                width = 0.38
                chart_ax.bar(
                    xs - (width / 2),
                    rebuilt_bars.values,
                    color=rebuilt_bars.color,
                    alpha=0.9,
                    width=width,
                    edgecolor="#214E93",
                    linewidth=0.4,
                    label=(rebuilt_bars.primary_label or "Series A"),
                )
                chart_ax.bar(
                    xs + (width / 2),
                    rebuilt_bars.secondary_values,
                    color=(rebuilt_bars.secondary_color or "#5AA88A"),
                    alpha=0.9,
                    width=width,
                    edgecolor="#2E6B54",
                    linewidth=0.4,
                    label=(rebuilt_bars.secondary_label or "Series B"),
                )
                chart_ax.legend(loc="upper left", fontsize=8, frameon=False)
                if qa_sink is not None:
                    qa_sink["grouped_two_series"] = True
            else:
                chart_ax.bar(xs, rebuilt_bars.values, color=rebuilt_bars.color, alpha=0.88, width=0.72, edgecolor="#214E93", linewidth=0.4)
                if qa_sink is not None:
                    qa_sink["grouped_two_series"] = False
            chart_ax.set_xlim(-0.6, max(0.6, float(len(xs) - 0.4)))
            all_vals = list(rebuilt_bars.values) + (list(rebuilt_bars.secondary_values) if rebuilt_bars.secondary_values else [])
            y_min = float(min(all_vals))
            y_max = float(max(all_vals))
            y_span = max(1.0, y_max - y_min)
            if y_min < 0:
                chart_ax.set_ylim(y_min - (0.15 * y_span), y_max + (0.18 * y_span))
            elif rebuilt_bars.normalized:
                chart_ax.set_ylim(0.0, max(100.0, y_max * 1.15))
            else:
                chart_ax.set_ylim(0.0, y_max + (0.18 * y_span))
            y_lo, y_hi = chart_ax.get_ylim()
            y_ticks = _compute_y_ticks(y_min=float(y_lo), y_max=float(y_hi), normalized=bool(rebuilt_bars.normalized))
            chart_ax.set_yticks(y_ticks)
            if rebuilt_bars.normalized:
                chart_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: _format_numeric_tick(v)))
            else:
                chart_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: _format_numeric_tick(v)))
            chart_ax.grid(axis="y", color="#D9DEE7", linewidth=0.8, alpha=0.9)
            chart_ax.tick_params(axis="both", labelsize=9, colors="#4A4F59")
            xlabels = rebuilt_bars.labels if len(rebuilt_bars.labels) == len(rebuilt_bars.values) else []
            if not xlabels:
                xlabels = _fallback_bar_labels(candidate=candidate, count=len(rebuilt_bars.values))
            if len(xlabels) == len(rebuilt_bars.values):
                rotation = 0 if len(xlabels) <= 12 else 35
                chart_ax.set_xticks(xs)
                chart_ax.set_xticklabels(xlabels, rotation=rotation, ha=("center" if rotation == 0 else "right"), fontsize=9)
            else:
                chart_ax.set_xticks(xs)
                chart_ax.set_xticklabels([f"P{i+1}" for i in range(len(rebuilt_bars.values))], fontsize=9)
            chart_ax.set_ylabel(rebuilt_bars.y_label, fontsize=10, color="#4A4F59", labelpad=8)
    elif rebuilt:
        if qa_sink is not None:
            qa_sink["reconstruction_mode"] = "line"
            qa_sink["grouped_two_series"] = False
        for series in rebuilt:
            chart_ax.plot(series.x, series.y, color=series.color, linewidth=series.weight, alpha=0.98)
            chart_ax.scatter([series.x[-1]], [series.y[-1]], s=70, color=series.color, zorder=6)
        chart_ax.set_xlim(0.0, 100.0)
        chart_ax.set_ylim(0.0, 100.0)
        chart_ax.grid(axis="y", color="#D9DEE7", linewidth=0.8, alpha=0.85)
        chart_ax.set_ylabel("Index (normalized)", fontsize=10, color="#4A4F59", labelpad=8)
        chart_ax.tick_params(axis="both", labelsize=9, colors="#4A4F59")
        chart_ax.set_xticks([0, 25, 50, 75, 100])
        chart_ax.set_xticklabels(["Start", "Q1", "Q2", "Q3", "Now"])
        chart_ax.set_yticks([0, 20, 40, 60, 80, 100])
    else:
        raise XChartError("Chart reconstruction unavailable; screenshot fallback disabled.")

    # Readability guardrail: reconstructed bar charts must include usable x-axis and y-axis labels.
    if rebuilt_bars is not None:
        tick_text = [t.get_text().strip() for t in chart_ax.get_xticklabels()]
        non_empty = [t for t in tick_text if t]
        if qa_sink is not None:
            qa_sink["x_axis_labels_present"] = len(non_empty) >= min(4, len(rebuilt_bars.values))
        if len(non_empty) < min(4, len(rebuilt_bars.values)):
            raise XChartError("Rebuilt bar chart missing x-axis labels; screenshot fallback disabled.")
        y_tick_text = [t.get_text().strip() for t in chart_ax.get_yticklabels()]
        y_non_empty = [t for t in y_tick_text if t]
        if qa_sink is not None:
            qa_sink["y_axis_labels_present"] = len(y_non_empty) >= 3
        if len(y_non_empty) < 3:
            raise XChartError("Rebuilt bar chart missing y-axis tick labels; screenshot fallback disabled.")
    else:
        x_non_empty = [t.get_text().strip() for t in chart_ax.get_xticklabels() if t.get_text().strip()]
        y_non_empty = [t.get_text().strip() for t in chart_ax.get_yticklabels() if t.get_text().strip()]
        if qa_sink is not None:
            qa_sink["x_axis_labels_present"] = len(x_non_empty) >= 4
            qa_sink["y_axis_labels_present"] = len(y_non_empty) >= 3

    takeaway_text = _shorten_without_ellipsis(_normalize_render_text(style_draft.takeaway), max_chars=68)
    takeaway_lines = "\n".join(textwrap.wrap(f"Takeaway: {takeaway_text}", width=90)[:1])
    takeaway_obj = fig.text(0.05, 0.118, takeaway_lines, fontsize=10.6, color="#1F2430", family=COATUE_FONT_FAMILY, weight="bold", va="top")
    source_obj = fig.text(0.05, 0.045, f"Source: {candidate.url}", fontsize=9, color="#4B5563", family=COATUE_FONT_FAMILY)

    # Prevent overlapping labels by shrinking header/plot region if needed.
    for _ in range(4):
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        fig_bbox = fig.bbox
        if headline_obj.get_window_extent(renderer=renderer).x1 > fig_bbox.x0 + (fig_bbox.width * 0.95):
            headline_text = _shorten_without_ellipsis(headline_text, max_chars=max(26, len(headline_text) - 6))
            headline_obj.set_text(headline_text)
            headline_obj.set_fontsize(max(21, float(headline_obj.get_fontsize()) - 1))
            continue
        if chart_label_obj.get_window_extent(renderer=renderer).x1 > fig_bbox.x0 + (fig_bbox.width * 0.95):
            chart_label_text = _shorten_without_ellipsis(chart_label_text, max_chars=max(28, len(chart_label_text) - 8))
            chart_label_obj.set_text(chart_label_text)
            continue
        chart_bb = chart_ax.get_tightbbox(renderer=renderer)
        take_bb = takeaway_obj.get_window_extent(renderer=renderer)
        src_bb = source_obj.get_window_extent(renderer=renderer)
        label_bb = chart_label_obj.get_window_extent(renderer=renderer)
        if label_bb.y0 < chart_bb.y1 + 4:
            pos = chart_ax.get_position()
            chart_ax.set_position([pos.x0, max(0.16, pos.y0 - 0.01), pos.width, max(0.58, pos.height - 0.02)])
            continue
        if take_bb.y1 > chart_bb.y0 - 4:
            pos = chart_ax.get_position()
            delta = min(0.02, max(0.008, (take_bb.y1 - chart_bb.y0 + 6) / fig_bbox.height))
            new_height = max(0.50, pos.height - delta)
            chart_ax.set_position([pos.x0, pos.y0 + delta, pos.width, new_height])
            continue
        if src_bb.y1 > take_bb.y0 - 4:
            source_obj.set_y(max(0.025, source_obj.get_position()[1] - 0.01))
            continue
        break

    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_path


def _post_winner_to_slack(
    *,
    candidate: Candidate,
    channel: str,
    slot_key: str,
    windows_text: str,
    convention_name: str | None = None,
    style_draft: StyleDraft | None = None,
    store: XChartStore | None = None,
) -> dict[str, Any]:
    tokens = _slack_tokens()
    style_draft = style_draft or _select_style_draft(candidate)
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    source_path = _write_source_chart_image(candidate=candidate, slot_key=slot_key)
    artifact_path = source_path
    render_mode = "source-snip"
    try:
        artifact_path = _render_source_snip_card(
            candidate=candidate,
            slot_key=slot_key,
            style_draft=style_draft,
            source_path=source_path,
        )
        render_mode = "source-snip-card"
    except Exception as exc:
        logger.warning("source snip card render failed, falling back to raw snip: %s", exc)
        artifact_path = source_path
        render_mode = "source-snip"
    file_size = artifact_path.stat().st_size if artifact_path.exists() else 0
    review = {
        "passed": file_size > 0,
        "failed": ([] if file_size > 0 else ["source_image_missing"]),
        "checks": {
            "source_image_available": file_size > 0,
            "artifact_nonempty": file_size > 0,
            "render_mode_source_snip": render_mode in {"source-snip", "source-snip-card"},
        },
        "style_score": float(style_draft.score),
        "style_iteration": int(style_draft.iteration),
        "render_qa": {"render_mode": render_mode},
        "artifact_path": str(artifact_path),
        "artifact_size_bytes": int(file_size),
    }
    clean_author = _normalize_render_text(candidate.author)
    clean_takeaway = _shorten_without_ellipsis(_normalize_render_text(style_draft.takeaway), max_chars=68)
    title_text = _normalize_render_text(convention_name or "Coatue Chart")
    text_lines = [
        f"*{title_text}*",
        f"- Source: `{clean_author}`",
        f"- Title: {style_draft.headline}",
        f"- Key takeaway: {clean_takeaway}",
        f"- Link: {candidate.url}",
        f"- Render: `{render_mode}`",
    ]
    last_error: str | None = None
    for token in tokens:
        client = WebClient(token=token)
        try:
            response = client.files_upload_v2(
                channel=channel,
                file=str(artifact_path),
                title=title_text,
                initial_comment="\n".join(text_lines),
            )
            if store is not None:
                store.record_post_review(slot_key=slot_key, channel=channel, candidate=candidate, review=review)
                store.apply_review_feedback(
                    source_id=candidate.source_id,
                    passed=bool(review.get("passed")),
                    failed_checks=[str(x) for x in (review.get("failed") or [])],
                )
            file_obj = response.get("file") if isinstance(response, dict) else None
            file_id = ""
            if isinstance(file_obj, dict):
                file_id = str(file_obj.get("id") or "")
            return {
                "ok": bool(response.get("ok")),
                "ts": None,
                "channel": channel,
                "file_id": file_id,
                "source_artifact": str(source_path),
                "styled_artifact": str(artifact_path),
                "style_audit": {
                    "iteration": style_draft.iteration,
                    "score": style_draft.score,
                    "checks": style_draft.checks,
                },
                "post_publish_review": review,
            }
        except SlackApiError as exc:
            err = str(exc.response.get("error", "")) if exc.response is not None else str(exc)
            last_error = err or str(exc)
            if err in {"account_inactive", "invalid_auth", "token_revoked"}:
                logger.warning("x-chart slack token rejected (%s), trying next token if available", err)
                continue
            raise
    raise XChartError(f"Slack post failed for all available tokens: {last_error or 'unknown_error'}")


def run_chart_scout_once(
    *,
    manual: bool = False,
    dry_run: bool = False,
    channel_override: str | None = None,
) -> dict[str, Any]:
    store = XChartStore()
    now_utc = datetime.now(UTC)
    tz = _timezone()
    now_local = now_utc.astimezone(tz)
    windows = _parse_windows()
    slot_key = _slot_key(now_local=now_local, windows=windows, manual=manual)
    windows_text = ",".join(f"{h:02d}:{m:02d}" for h, m in windows)
    token = _resolve_bearer_token()
    source_limit = max(8, min(60, int(os.environ.get("COATUE_CLAW_X_CHART_SOURCE_LIMIT", "25"))))
    top_sources = store.top_sources(limit=source_limit)
    handles = [_canonical_handle(str(item["handle"])) for item in top_sources if str(item.get("handle") or "").strip()]
    x_candidates = _fetch_x_candidates_from_sources(handles=handles, token=token, hours=48)
    vc_candidates = _fetch_visualcapitalist_candidates(max_items=20)
    all_candidates = _dedupe_candidates(x_candidates + vc_candidates)
    observed_count = store.upsert_observed_candidates(all_candidates)
    pool_prune_days = max(2, min(30, int(os.environ.get("COATUE_CLAW_X_CHART_POOL_KEEP_DAYS", "10"))))
    pruned_count = store.prune_observed_candidates(keep_days=pool_prune_days)

    for item in all_candidates[:80]:
        if item.source_type == "x":
            store.note_candidate_observed(item.source_id, engagement=item.engagement)

    discovery = _discover_new_sources(token=token)
    for handle, engagement in discovery:
        if engagement >= int(os.environ.get("COATUE_CLAW_X_CHART_DISCOVERY_MIN_ENGAGEMENT", "120")):
            store.note_candidate_observed(handle, engagement=engagement)

    if slot_key is None:
        return {
            "ok": True,
            "posted": False,
            "reason": "scouted_pool_updated",
            "now_local": now_local.isoformat(),
            "windows": windows_text,
            "candidates_scanned": len(all_candidates),
            "candidates_observed": observed_count,
            "pool_pruned": pruned_count,
        }

    if (not manual) and store.was_slot_posted(slot_key):
        return {
            "ok": True,
            "posted": False,
            "reason": "slot_already_posted",
            "slot_key": slot_key,
            "candidates_scanned": len(all_candidates),
            "candidates_observed": observed_count,
            "pool_pruned": pruned_count,
        }

    since_utc = None if manual else store.latest_scheduled_posted_at_utc()
    pool_limit = max(50, min(2000, int(os.environ.get("COATUE_CLAW_X_CHART_POOL_LIMIT", "600"))))
    pool_candidates = store.observed_candidates_since(since_utc=since_utc, limit=pool_limit)
    ranking_pool = _dedupe_candidates(pool_candidates) if pool_candidates else all_candidates

    winner = _pick_winner(store=store, candidates=ranking_pool)
    if winner is None:
        return {
            "ok": True,
            "posted": False,
            "reason": "no_candidate_available",
            "slot_key": slot_key,
            "candidates_scanned": len(all_candidates),
            "candidates_observed": observed_count,
            "pool_candidates": len(ranking_pool),
            "since_utc": since_utc,
            "pool_pruned": pruned_count,
        }
    style_draft = _select_style_draft(winner)
    convention_name = _convention_name(slot_key=slot_key, now_local=now_local, windows=windows)

    output_dir = _output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slot_key}-{winner.source_type}.md"
    out_path.write_text(
        "\n".join(
            [
                f"# Coatue Chart Scout Winner",
                "",
                f"- slot_key: `{slot_key}`",
                f"- generated_at_utc: `{now_utc.isoformat()}`",
                f"- convention: `{convention_name}`",
                f"- source: `{winner.source_type}:{winner.source_id}`",
                f"- author: `{winner.author}`",
                f"- score: `{winner.score:.2f}`",
                f"- url: {winner.url}",
                f"- image_url: {winner.image_url or 'n/a'}",
                f"- style_iteration: `{style_draft.iteration}`",
                f"- style_score: `{style_draft.score:.1f}/7`",
                "",
                "## Notes",
                _normalize_render_text(winner.text),
                "",
                "## Style Audit",
                f"- headline: {_normalize_render_text(style_draft.headline)}",
                f"- chart_label: {_normalize_render_text(style_draft.chart_label)}",
                f"- takeaway: {_normalize_render_text(style_draft.takeaway)}",
                f"- why_now: {_normalize_render_text(style_draft.why_now)}",
                f"- checks: {json.dumps(style_draft.checks, sort_keys=True)}",
            ]
        ),
        encoding="utf-8",
    )

    if dry_run:
        return {
            "ok": True,
            "posted": False,
            "reason": "dry_run",
            "slot_key": slot_key,
            "convention": convention_name,
            "winner": {
                "source": f"{winner.source_type}:{winner.source_id}",
                "author": winner.author,
                "title": winner.title,
                "url": winner.url,
                "score": winner.score,
                "style_score": style_draft.score,
                "style_iteration": style_draft.iteration,
            },
            "artifact": str(out_path),
            "pool_candidates": len(ranking_pool),
            "since_utc": since_utc,
        }

    channel = (channel_override or "").strip() or _slack_channel()
    post = _post_winner_to_slack(
        candidate=winner,
        channel=channel,
        slot_key=slot_key,
        windows_text=windows_text,
        convention_name=convention_name,
        style_draft=style_draft,
        store=store,
    )
    store.record_post(slot_key=slot_key, channel=channel, candidate=winner)
    return {
        "ok": True,
        "posted": True,
        "slot_key": slot_key,
        "convention": convention_name,
        "channel": channel,
        "post": post,
        "winner": {
            "source": f"{winner.source_type}:{winner.source_id}",
            "author": winner.author,
            "title": winner.title,
            "url": winner.url,
            "score": winner.score,
            "style_score": style_draft.score,
            "style_iteration": style_draft.iteration,
        },
        "artifact": str(out_path),
        "pool_candidates": len(ranking_pool),
        "since_utc": since_utc,
    }


def run_chart_for_post_url(
    *,
    post_url: str,
    channel_override: str | None = None,
) -> dict[str, Any]:
    parsed = _parse_x_post_url(post_url)
    if parsed is None:
        raise XChartError("Invalid X post URL. Expected format like https://x.com/<handle>/status/<id>.")
    handle, tweet_id = parsed

    store = XChartStore()
    token = _resolve_bearer_token()
    payload = _http_json(
        url=f"{_x_api_base()}/2/tweets",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={
            "ids": tweet_id,
            "expansions": "author_id,attachments.media_keys",
            "tweet.fields": "author_id,created_at,public_metrics,attachments,lang",
            "user.fields": "name,username,verified",
            "media.fields": "type,url,preview_image_url,width,height",
        },
    )
    candidates = _parse_x_candidates(payload, priority_by_handle={handle.lower(): 1.0})
    winner = next((c for c in candidates if c.candidate_key == f"x:{tweet_id}"), None)
    if winner is None:
        if candidates:
            winner = candidates[0]
        else:
            winner = _fetch_vxtwitter_post_candidate(handle=handle, tweet_id=tweet_id)
            if winner is None:
                raise XChartError("No chart candidate found in that X post (missing image or inaccessible tweet).")

    style_draft = _select_style_draft(winner)
    channel = (channel_override or "").strip() or _slack_channel()
    now_local = datetime.now(UTC).astimezone(_timezone())
    slot_key = f"manual-url-{now_local.strftime('%Y%m%d-%H%M%S')}"
    windows = _parse_windows()
    windows_text = ",".join(f"{h:02d}:{m:02d}" for h, m in windows)
    convention_name = _convention_name(slot_key=slot_key, now_local=now_local, windows=windows)
    post = _post_winner_to_slack(
        candidate=winner,
        channel=channel,
        slot_key=slot_key,
        windows_text=windows_text,
        convention_name=convention_name,
        style_draft=style_draft,
        store=store,
    )
    store.record_post(slot_key=slot_key, channel=channel, candidate=winner)
    return {
        "ok": True,
        "posted": True,
        "slot_key": slot_key,
        "convention": convention_name,
        "channel": channel,
        "post": post,
        "winner": {
            "source": f"{winner.source_type}:{winner.source_id}",
            "author": winner.author,
            "title": winner.title,
            "url": winner.url,
            "score": winner.score,
            "style_score": style_draft.score,
            "style_iteration": style_draft.iteration,
        },
    }


def status() -> dict[str, Any]:
    store = XChartStore()
    last_scheduled = store.latest_scheduled_posted_at_utc()
    return {
        "ok": True,
        "render_mode": "source-snip-card",
        "schedule_mode": "hourly-scout+windowed-post",
        "db_path": str(store.db_path),
        "timezone": os.environ.get("COATUE_CLAW_X_CHART_TIMEZONE", DEFAULT_TIMEZONE),
        "windows": ",".join(f"{h:02d}:{m:02d}" for h, m in _parse_windows()),
        "slack_channel": os.environ.get("COATUE_CLAW_X_CHART_SLACK_CHANNEL", ""),
        "sources_count": len(store.list_sources(limit=1000)),
        "last_scheduled_posted_at_utc": last_scheduled,
        "pool_candidates_since_last_post": store.observed_candidates_count_since(since_utc=last_scheduled),
        "recent_posts": store.latest_posts(limit=5),
        "review_summary": store.recent_review_summary(limit=20),
    }


def add_source(handle: str, *, priority: float = 1.0) -> dict[str, Any]:
    store = XChartStore()
    clean = _canonical_handle(handle)
    if not clean:
        raise XChartError("Invalid X handle.")
    store.upsert_source(clean, priority=priority, manual=True)
    return {"ok": True, "handle": clean, "priority": float(priority)}


def list_sources(*, limit: int = 50) -> dict[str, Any]:
    store = XChartStore()
    return {"ok": True, "sources": store.list_sources(limit=limit)}


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-x-chart-daily")
    sub = parser.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run-once")
    r.add_argument("--manual", action="store_true")
    r.add_argument("--dry-run", action="store_true")

    sub.add_parser("status")

    ls = sub.add_parser("list-sources")
    ls.add_argument("--limit", type=int, default=50)

    add = sub.add_parser("add-source")
    add.add_argument("handle")
    add.add_argument("--priority", type=float, default=1.0)

    rpu = sub.add_parser("run-post-url")
    rpu.add_argument("post_url")
    rpu.add_argument("--channel", default="", help="Override Slack channel id for posting")

    args = parser.parse_args()
    if args.cmd == "run-once":
        result = run_chart_scout_once(manual=bool(args.manual), dry_run=bool(args.dry_run))
    elif args.cmd == "status":
        result = status()
    elif args.cmd == "list-sources":
        result = list_sources(limit=max(1, min(500, int(args.limit))))
    elif args.cmd == "run-post-url":
        result = run_chart_for_post_url(
            post_url=str(args.post_url).strip(),
            channel_override=(str(args.channel).strip() or None),
        )
    else:
        result = add_source(args.handle, priority=float(args.priority))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
import re
from typing import Any

from coatue_claw.hf_youtube_transcript import PodcastTranscript, TranscriptSegment

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]


@dataclass(frozen=True)
class PodcastQuote:
    quote: str
    timestamp_sec: float
    why_it_matters: str


@dataclass(frozen=True)
class PodcastAnalysis:
    executive_summary: tuple[str, ...]
    key_themes: tuple[str, ...]
    quotes: tuple[PodcastQuote, ...]
    confidence_label: str
    warnings: tuple[str, ...] = ()


def format_timestamp(seconds: float) -> str:
    total = int(max(0.0, seconds))
    hh = total // 3600
    mm = (total % 3600) // 60
    ss = total % 60
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return f"{mm:02d}:{ss:02d}"


def normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def clip(text: str, *, max_chars: int) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip()


def _looks_like_weak_question(text: str) -> bool:
    t = (text or "").strip()
    if not t.endswith("?"):
        return False
    lower = t.lower()
    weak_starts = (
        "how much",
        "can you",
        "what do you",
        "do you",
        "is it",
        "are you",
        "would you",
        "could you",
        "should we",
        "why don't",
    )
    return any(lower.startswith(prefix) for prefix in weak_starts)


def _insight_score(text: str) -> float:
    t = (text or "").strip()
    if not t:
        return 0.0
    lower = t.lower()
    words = t.split()
    score = 0.0
    if 10 <= len(words) <= 55:
        score += 1.0
    if re.search(r"\b\d+(?:\.\d+)?%\b", lower):
        score += 1.4
    if re.search(r"\b\d+(?:\.\d+)?\s*(?:x|bps|bp|million|billion|k)\b", lower):
        score += 0.8
    claim_tokens = (
        "because",
        "therefore",
        "means",
        "implies",
        "so that",
        "the reason",
        "tradeoff",
        "moat",
        "distribution",
        "pricing",
        "margin",
        "retention",
        "churn",
        "ltv",
        "cac",
        "workflow",
        "system of record",
        "agent",
        "automation",
        "deployment",
        "inference",
        "token",
        "csat",
    )
    score += sum(0.35 for tok in claim_tokens if tok in lower)
    if _looks_like_weak_question(t):
        score -= 1.8
    # de-emphasize fluffy language
    fluffy = ("great question", "super exciting", "really cool", "kind of", "sort of")
    score -= sum(0.4 for tok in fluffy if tok in lower)
    return score


def _topic_bucket(text: str) -> str:
    lower = (text or "").lower()
    if any(k in lower for k in ("valuation", "multiple", "stock", "market", "macro")):
        return "market"
    if any(k in lower for k in ("customer", "sales", "lead", "g tm", "go-to-market", "pricing")):
        return "gtm"
    if any(k in lower for k in ("model", "token", "inference", "agent", "workflow", "deployment", "ci/cd")):
        return "product"
    if any(k in lower for k in ("margin", "csat", "retention", "churn", "efficiency", "cost")):
        return "unit_econ"
    return "other"


def transcript_excerpt(transcript: PodcastTranscript, *, max_chars: int = 80_000) -> str:
    joined = "\n".join(
        f"[{format_timestamp(segment.start_sec)}] {segment.text.strip()}"
        for segment in transcript.segments
        if segment.text.strip()
    )
    return joined[:max_chars]


def _fallback_quotes(transcript: PodcastTranscript, *, count: int = 5) -> tuple[PodcastQuote, ...]:
    scored: list[tuple[float, TranscriptSegment]] = []
    for segment in transcript.segments:
        text = segment.text.strip()
        if not text:
            continue
        if len(text.split()) < 8:
            continue
        score = _insight_score(text)
        scored.append((score, segment))

    scored.sort(key=lambda item: item[0], reverse=True)
    out: list[PodcastQuote] = []
    seen: set[str] = set()
    bucket_counts: dict[str, int] = {}
    for score, segment in scored:
        quote = clip(segment.text, max_chars=260)
        key = normalize_for_match(quote)
        if key in seen:
            continue
        bucket = _topic_bucket(quote)
        # diversity soft-cap: allow at most 2 per bucket while alternatives exist
        if bucket_counts.get(bucket, 0) >= 2 and len(out) < count - 1:
            continue
        # strict default: avoid weak question-style quotes unless we need fallback coverage
        if _looks_like_weak_question(quote) and len(out) < count - 1:
            continue
        # minimum insight bar; soft fallback allows last slot if needed
        min_bar = 1.0 if len(out) < count - 1 else 0.2
        if score < min_bar:
            continue
        seen.add(key)
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        out.append(
            PodcastQuote(
                quote=quote,
                timestamp_sec=segment.start_sec,
                why_it_matters="Concrete, investor-relevant claim that adds mechanism, evidence, or measurable implication.",
            )
        )
        if len(out) >= count:
            break
    return tuple(out)


def _quote_present_in_transcript(quote: str, transcript_text: str) -> bool:
    q = normalize_for_match(quote)
    t = normalize_for_match(transcript_text)
    return bool(q and t and q in t)


def _validate_quotes(quotes: list[PodcastQuote], transcript: PodcastTranscript) -> tuple[tuple[PodcastQuote, ...], list[str]]:
    full = transcript.full_text
    out: list[PodcastQuote] = []
    warnings: list[str] = []
    for row in quotes:
        quote = clip(row.quote, max_chars=300)
        if not quote:
            continue
        if not _quote_present_in_transcript(quote, full):
            warnings.append("quote_not_verbatim_dropped")
            continue
        out.append(
            PodcastQuote(
                quote=quote,
                timestamp_sec=max(0.0, float(row.timestamp_sec)),
                why_it_matters=clip(row.why_it_matters, max_chars=220) or "Interesting directional signal.",
            )
        )
        if len(out) >= 5:
            break
    return (tuple(out), warnings)


def _fallback_analysis(transcript: PodcastTranscript, *, question: str | None) -> PodcastAnalysis:
    raw_words = len(transcript.full_text.split())
    low_signal = raw_words < 500
    summary = [
        f"Episode transcript analyzed from `{transcript.transcript_source}` with ~{raw_words} words.",
        ("Signal quality is limited; treat conclusions as preliminary." if low_signal else "Transcript had enough depth for directional takeaways."),
        (f"Requested focus: {question}" if question else "No specific focus question was provided."),
    ]
    themes = [
        "Core thesis and strategic narrative from the speaker.",
        "Operational or market signals that may affect forward outlook.",
        "Risks, caveats, and uncertainty points raised in discussion.",
    ]
    quotes = _fallback_quotes(transcript, count=5)
    warnings: list[str] = []
    if len(quotes) < 5:
        warnings.append("fewer_than_5_quotes_due_to_short_transcript")
    if low_signal:
        warnings.append("low_evidence_mode_enabled")
    return PodcastAnalysis(
        executive_summary=tuple(summary),
        key_themes=tuple(themes),
        quotes=quotes,
        confidence_label=("Low" if low_signal else "Medium"),
        warnings=tuple(warnings),
    )


def _parse_model_quotes(rows: Any) -> list[PodcastQuote]:
    out: list[PodcastQuote] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        quote = str(row.get("quote") or "").strip()
        why = str(row.get("why_it_matters") or "").strip()
        ts_raw = row.get("timestamp_sec")
        try:
            ts = float(ts_raw)
        except Exception:
            ts = 0.0
        if quote:
            out.append(PodcastQuote(quote=quote, timestamp_sec=ts, why_it_matters=why))
    return out


def _model_analysis(transcript: PodcastTranscript, *, question: str | None, output_instruction: str | None = None) -> PodcastAnalysis | None:
    if OpenAI is None:
        return None
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    model = (os.environ.get("COATUE_CLAW_HFA_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest").strip()
    client = OpenAI(api_key=api_key)
    text = transcript_excerpt(transcript, max_chars=90_000)
    prompt_parts: list[str] = [
        "You are summarizing a podcast transcript for a hedge-fund analyst.\n"
        "Return strict JSON with keys:\n"
        "executive_summary (array of 3 short bullets),\n"
        "key_themes (array of 3 short bullets),\n"
        "quotes (array up to 5 objects with quote, timestamp_sec, why_it_matters),\n"
        "confidence_label (High|Medium|Low).\n"
        "Rules:\n"
        "- Quotes must be verbatim from transcript text.\n"
        "- Prefer most interesting/decision-relevant statements.\n"
        "- If evidence is weak, be explicit in summary and lower confidence.\n"
    ]
    if output_instruction and output_instruction.strip():
        prompt_parts.append(f"Operator output instruction (highest priority): {output_instruction.strip()}\n")
    prompt_parts.extend(
        [
            f"Focus question: {question or 'none'}\n\n",
            f"Transcript:\n{text}\n",
        ]
    )
    prompt = "".join(prompt_parts)
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.1,
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
    except Exception:
        return None
    content = ""
    if response and response.choices:
        content = str(response.choices[0].message.content or "").strip()
    if not content:
        return None
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = __import__("json").loads(content[start : end + 1])
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    summary = tuple(clip(str(x), max_chars=220) for x in list(payload.get("executive_summary") or []) if str(x).strip())[:3]
    themes = tuple(clip(str(x), max_chars=220) for x in list(payload.get("key_themes") or []) if str(x).strip())[:5]
    parsed_quotes = _parse_model_quotes(payload.get("quotes"))
    validated, warnings = _validate_quotes(parsed_quotes, transcript)
    if len(validated) < 5:
        fallback_quotes = _fallback_quotes(transcript, count=5)
        merged = list(validated)
        seen = {normalize_for_match(item.quote) for item in merged}
        for item in fallback_quotes:
            key = normalize_for_match(item.quote)
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= 5:
                break
        validated = tuple(merged[:5])
        warnings.append("model_quotes_backfilled_with_fallback")
        if len(validated) < 5:
            warnings.append("insight_density_low_soft_fallback")
    return PodcastAnalysis(
        executive_summary=(summary if summary else _fallback_analysis(transcript, question=question).executive_summary),
        key_themes=(themes if themes else _fallback_analysis(transcript, question=question).key_themes),
        quotes=validated,
        confidence_label=(str(payload.get("confidence_label") or "Low").title() if str(payload.get("confidence_label") or "").strip() else "Low"),
        warnings=tuple(warnings),
    )


def build_podcast_analysis(
    transcript: PodcastTranscript,
    *,
    question: str | None = None,
    output_instruction: str | None = None,
) -> PodcastAnalysis:
    modeled = _model_analysis(transcript, question=question, output_instruction=output_instruction)
    if modeled is not None:
        return modeled
    return _fallback_analysis(transcript, question=question)


def render_podcast_summary_markdown(
    *,
    transcript: PodcastTranscript,
    analysis: PodcastAnalysis,
    generated_at_utc: str,
    source_lines: tuple[str, ...] = (),
) -> str:
    lines: list[str] = []
    lines.append(f"# HFA Podcast Summary — {transcript.title}")
    lines.append("")
    lines.append("## Podcast Metadata")
    lines.append("")
    lines.append(f"- url: {transcript.url}")
    lines.append(f"- video_id: `{transcript.video_id}`")
    lines.append(f"- channel: `{transcript.channel_name}`")
    lines.append(f"- duration_sec: `{transcript.duration_sec}`")
    lines.append(f"- transcript_source: `{transcript.transcript_source}`")
    lines.append(f"- generated_at_utc: `{generated_at_utc}`")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    for item in analysis.executive_summary:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Key Themes")
    lines.append("")
    for item in analysis.key_themes:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Top 5 Interesting Quotes")
    lines.append("")
    if not analysis.quotes:
        lines.append("- No validated verbatim quotes were available.")
    for idx, quote in enumerate(analysis.quotes[:5], start=1):
        lines.append(f"{idx}. \"{quote.quote}\"")
        lines.append(f"   - timestamp: `{format_timestamp(quote.timestamp_sec)}`")
        lines.append(f"   - why_it_matters: {quote.why_it_matters}")
    lines.append("")
    lines.append("## Risks / Uncertainty Notes")
    lines.append("")
    if analysis.warnings:
        for warning in analysis.warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- No critical uncertainty flags.")
    lines.append("")
    lines.append("## Sources")
    lines.append("")
    lines.append(f"- YouTube: {transcript.url}")
    lines.append(f"- Transcript source: `{transcript.transcript_source}`")
    lines.append(f"- Generated UTC: `{generated_at_utc}`")
    for item in source_lines:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def render_full_transcript_markdown(*, transcript: PodcastTranscript, generated_at_utc: str) -> str:
    lines: list[str] = []
    lines.append(f"# HFA Podcast Transcript — {transcript.title}")
    lines.append("")
    lines.append(f"- url: {transcript.url}")
    lines.append(f"- video_id: `{transcript.video_id}`")
    lines.append(f"- channel: `{transcript.channel_name}`")
    lines.append(f"- duration_sec: `{transcript.duration_sec}`")
    lines.append(f"- transcript_source: `{transcript.transcript_source}`")
    lines.append(f"- generated_at_utc: `{generated_at_utc}`")
    lines.append("")
    lines.append("## Transcript")
    lines.append("")
    for segment in transcript.segments:
        text = segment.text.strip()
        if not text:
            continue
        lines.append(f"- [{format_timestamp(segment.start_sec)}] {text}")
    return "\n".join(lines) + "\n"

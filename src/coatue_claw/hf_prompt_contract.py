from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any


@dataclass(frozen=True)
class CitationRef:
    source_ref: str
    source_ts_utc: str


@dataclass(frozen=True)
class ScoreRow:
    factor: str
    score: int
    weight: float
    weighted_points: float


@dataclass(frozen=True)
class HFScorecard:
    rows: tuple[ScoreRow, ...]
    weighted_total: float
    confidence_label: str


@dataclass(frozen=True)
class PromptDraft:
    at_a_glance: tuple[str, ...]
    actionable: str
    asymmetric_insight: str
    variant_view: tuple[str, ...]
    scorecard: HFScorecard
    catalysts_timeline: tuple[str, ...]
    key_risks: tuple[str, ...]
    verify_next: tuple[str, ...]
    section_citations: dict[str, tuple[CitationRef, ...]]
    warnings: tuple[str, ...] = ()


_SCORE_WEIGHTS: dict[str, float] = {
    "growth": 0.25,
    "quality": 0.20,
    "valuation": 0.20,
    "catalyst": 0.20,
    "risk": 0.15,
}


def _clip_lines(values: list[str], *, min_items: int, max_items: int) -> tuple[str, ...]:
    out: list[str] = []
    for raw in values:
        line = str(raw or "").strip()
        if line:
            out.append(line)
        if len(out) >= max_items:
            break
    if len(out) < min_items:
        while len(out) < min_items:
            out.append("Insufficient evidence to support additional detail.")
    return tuple(out)


def _normalize_confidence(raw: str | None) -> str:
    value = str(raw or "").strip().lower()
    if value in {"high", "medium", "low"}:
        return value.title()
    return "Low"


def _score_int(raw: Any) -> int:
    try:
        value = int(float(raw))
    except Exception:
        return 3
    return max(1, min(5, value))


def build_scorecard(*, growth: Any, quality: Any, valuation: Any, catalyst: Any, risk: Any, confidence_label: str | None) -> HFScorecard:
    scores: dict[str, int] = {
        "growth": _score_int(growth),
        "quality": _score_int(quality),
        "valuation": _score_int(valuation),
        "catalyst": _score_int(catalyst),
        "risk": _score_int(risk),
    }
    rows: list[ScoreRow] = []
    total = 0.0
    for factor in ("growth", "quality", "valuation", "catalyst", "risk"):
        score = scores[factor]
        weight = _SCORE_WEIGHTS[factor]
        weighted = round((score / 5.0) * 100.0 * weight, 2)
        total += weighted
        rows.append(
            ScoreRow(
                factor=factor.title(),
                score=score,
                weight=weight,
                weighted_points=weighted,
            )
        )
    return HFScorecard(
        rows=tuple(rows),
        weighted_total=round(total, 2),
        confidence_label=_normalize_confidence(confidence_label),
    )


def validate_section_citations(section_citations: dict[str, tuple[CitationRef, ...]]) -> list[str]:
    errors: list[str] = []
    required = ("aaa_snapshot", "variant_view", "scorecard", "catalysts", "risks", "verify_next")
    for key in required:
        refs = section_citations.get(key, ())
        if not refs:
            errors.append(f"missing_citations:{key}")
            continue
        for idx, ref in enumerate(refs):
            if not ref.source_ref.strip():
                errors.append(f"missing_source_ref:{key}:{idx}")
            if not ref.source_ts_utc.strip():
                errors.append(f"missing_source_ts:{key}:{idx}")
    return errors


def _parse_citation_row(row: Any) -> CitationRef | None:
    if isinstance(row, dict):
        source_ref = str(row.get("source_ref") or row.get("source") or "").strip()
        source_ts = str(row.get("source_ts_utc") or row.get("timestamp_utc") or "").strip()
        if source_ref and source_ts:
            return CitationRef(source_ref=source_ref, source_ts_utc=source_ts)
    return None


def parse_model_json(raw: str) -> PromptDraft | None:
    text = str(raw or "").strip()
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = json.loads(text[start : end + 1])
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    scorecard_payload = payload.get("scorecard") if isinstance(payload.get("scorecard"), dict) else {}
    scorecard = build_scorecard(
        growth=scorecard_payload.get("growth"),
        quality=scorecard_payload.get("quality"),
        valuation=scorecard_payload.get("valuation"),
        catalyst=scorecard_payload.get("catalyst"),
        risk=scorecard_payload.get("risk"),
        confidence_label=scorecard_payload.get("confidence"),
    )

    citations_payload = payload.get("section_citations") if isinstance(payload.get("section_citations"), dict) else {}
    section_citations: dict[str, tuple[CitationRef, ...]] = {}
    for key in ("aaa_snapshot", "variant_view", "scorecard", "catalysts", "risks", "verify_next"):
        rows = citations_payload.get(key) if isinstance(citations_payload, dict) else None
        refs: list[CitationRef] = []
        if isinstance(rows, list):
            for row in rows:
                ref = _parse_citation_row(row)
                if ref is not None:
                    refs.append(ref)
        section_citations[key] = tuple(refs[:8])

    warnings: list[str] = []
    warnings.extend(validate_section_citations(section_citations))

    return PromptDraft(
        at_a_glance=_clip_lines(list(payload.get("at_a_glance") or []), min_items=3, max_items=5),
        actionable=str(payload.get("actionable") or "Insufficient evidence for a high-conviction positioning call.").strip(),
        asymmetric_insight=str(payload.get("asymmetric_insight") or "Asymmetric edge remains unclear with current inputs.").strip(),
        variant_view=_clip_lines(list(payload.get("variant_view") or []), min_items=3, max_items=3),
        scorecard=scorecard,
        catalysts_timeline=_clip_lines(list(payload.get("catalysts_timeline") or []), min_items=3, max_items=6),
        key_risks=_clip_lines(list(payload.get("key_risks") or []), min_items=3, max_items=6),
        verify_next=_clip_lines(list(payload.get("verify_next") or []), min_items=3, max_items=6),
        section_citations=section_citations,
        warnings=tuple(warnings),
    )


def _render_section_citations(refs: tuple[CitationRef, ...]) -> list[str]:
    if not refs:
        return ["- Source coverage is incomplete; verify before action."]
    out: list[str] = []
    for ref in refs:
        out.append(f"- {ref.source_ref} (timestamp_utc: `{ref.source_ts_utc}`)")
    return out


def render_markdown(*, title: str, generated_at_utc: str, draft: PromptDraft, source_summary: tuple[str, ...] = ()) -> str:
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"Generated UTC: `{generated_at_utc}`")
    lines.append("Framework: `AAA (At-a-Glance, Actionable, Asymmetric Insight)`")
    lines.append("")
    lines.append("## 1. AAA Snapshot")
    lines.append("")
    lines.append("### At-a-Glance")
    for line in draft.at_a_glance:
        lines.append(f"- {line}")
    lines.append("")
    lines.append("### Actionable")
    lines.append(f"- {draft.actionable}")
    lines.append("")
    lines.append("### Asymmetric Insight")
    lines.append(f"- {draft.asymmetric_insight}")
    lines.append("")
    lines.append("#### Section Sources")
    lines.extend(_render_section_citations(draft.section_citations.get("aaa_snapshot", ())))
    lines.append("")
    lines.append("## 2. Variant View")
    lines.append("")
    for line in draft.variant_view:
        lines.append(f"- {line}")
    lines.append("")
    lines.append("#### Section Sources")
    lines.extend(_render_section_citations(draft.section_citations.get("variant_view", ())))
    lines.append("")
    lines.append("## 3. 5-Factor Scorecard")
    lines.append("")
    lines.append("| Factor | Score (1-5) | Weight | Weighted Points |")
    lines.append("|---|---:|---:|---:|")
    for row in draft.scorecard.rows:
        lines.append(f"| {row.factor} | {row.score} | {row.weight:.2f} | {row.weighted_points:.2f} |")
    lines.append("")
    lines.append(f"- Weighted total: `{draft.scorecard.weighted_total:.2f}/100`")
    lines.append(f"- Confidence: `{draft.scorecard.confidence_label}`")
    lines.append("")
    lines.append("#### Section Sources")
    lines.extend(_render_section_citations(draft.section_citations.get("scorecard", ())))
    lines.append("")
    lines.append("## 4. Catalysts & Timeline (30/90/180d)")
    lines.append("")
    for line in draft.catalysts_timeline:
        lines.append(f"- {line}")
    lines.append("")
    lines.append("#### Section Sources")
    lines.extend(_render_section_citations(draft.section_citations.get("catalysts", ())))
    lines.append("")
    lines.append("## 5. Key Risks / Break Conditions")
    lines.append("")
    for line in draft.key_risks:
        lines.append(f"- {line}")
    lines.append("")
    lines.append("#### Section Sources")
    lines.extend(_render_section_citations(draft.section_citations.get("risks", ())))
    lines.append("")
    lines.append("## 6. What To Verify Next")
    lines.append("")
    for line in draft.verify_next:
        lines.append(f"- {line}")
    lines.append("")
    lines.append("#### Section Sources")
    lines.extend(_render_section_citations(draft.section_citations.get("verify_next", ())))
    lines.append("")
    lines.append("## 7. Sources")
    lines.append("")
    for line in source_summary:
        lines.append(f"- {line}")
    if not source_summary:
        lines.append("- No external source summary captured.")
    if draft.warnings:
        lines.append("")
        lines.append("## Warnings")
        lines.append("")
        for warning in draft.warnings:
            lines.append(f"- {warning}")
    return "\n".join(lines) + "\n"

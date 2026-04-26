from __future__ import annotations

from datetime import UTC, datetime
import re

from spclaw.memory_store import FactCandidate


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _infer_tier(category: str, fact_key: str, raw_value: str) -> str:
    key = fact_key.lower()
    value = raw_value.lower()

    if category in {"decision", "convention"}:
        return "permanent"
    if any(token in key for token in ("birthday", "name", "email", "endpoint", "api", "phone")):
        return "permanent"
    if any(token in value for token in ("today", "this week", "currently", "right now", "tomorrow")):
        return "active"
    if any(token in key for token in ("task", "ticket", "branch", "deploy")):
        return "active"
    return "stable"


def _candidate(
    *,
    category: str,
    entity: str,
    fact_key: str,
    fact_value: str,
    rationale: str | None,
    source: str,
    source_ts_utc: str,
    confidence: float,
) -> FactCandidate:
    tier = _infer_tier(category, fact_key, fact_value)
    return FactCandidate(
        category=category,
        entity=_normalize_text(entity),
        fact_key=_normalize_text(fact_key),
        fact_value=_normalize_text(fact_value),
        rationale=_normalize_text(rationale or "") or None,
        source=source,
        source_ts_utc=source_ts_utc,
        tier=tier,
        confidence=confidence,
    )


def extract_fact_candidates(
    text: str,
    *,
    source: str,
    source_ts_utc: str | None = None,
    default_entity: str = "user",
) -> list[FactCandidate]:
    stripped = _normalize_text(text)
    if not stripped:
        return []

    ts = source_ts_utc or _utc_now_iso()
    out: list[FactCandidate] = []

    patterns = [
        (
            re.compile(r"\bmy\s+([a-z][a-z0-9 _-]{1,40})\s+is\s+([^.;\n]{2,160})", re.IGNORECASE),
            lambda m: _candidate(
                category="profile",
                entity=default_entity,
                fact_key=m.group(1),
                fact_value=m.group(2),
                rationale="Explicit user profile statement",
                source=source,
                source_ts_utc=ts,
                confidence=0.95,
            ),
        ),
        (
            re.compile(r"\b([A-Za-z][A-Za-z0-9 ._-]{0,60})'s\s+([a-z][a-z0-9 _-]{1,40})\s+is\s+([^.;\n]{2,160})", re.IGNORECASE),
            lambda m: _candidate(
                category="relationship",
                entity=m.group(1),
                fact_key=m.group(2),
                fact_value=m.group(3),
                rationale="Possessive relationship fact statement",
                source=source,
                source_ts_utc=ts,
                confidence=0.93,
            ),
        ),
        (
            re.compile(r"\bwe\s+decided\s+to\s+use\s+(.+?)\s+because\s+(.+?)(?:[.?!]|$)", re.IGNORECASE),
            lambda m: _candidate(
                category="decision",
                entity="decision",
                fact_key=f"use {m.group(1)}",
                fact_value=m.group(2),
                rationale="Decision language with rationale",
                source=source,
                source_ts_utc=ts,
                confidence=0.98,
            ),
        ),
        (
            re.compile(r"\bchose\s+(.+?)\s+over\s+(.+?)\s+for\s+(.+?)(?:[.?!]|$)", re.IGNORECASE),
            lambda m: _candidate(
                category="decision",
                entity="decision",
                fact_key=f"{m.group(1)} over {m.group(2)}",
                fact_value=m.group(3),
                rationale="Tradeoff decision language with rationale",
                source=source,
                source_ts_utc=ts,
                confidence=0.97,
            ),
        ),
        (
            re.compile(r"\b(always|never)\s+(?:do|run|use|ship)\s+(.+?)(?:[.?!]|$)", re.IGNORECASE),
            lambda m: _candidate(
                category="convention",
                entity="convention",
                fact_key=m.group(2),
                fact_value=m.group(1).lower(),
                rationale="Convention language",
                source=source,
                source_ts_utc=ts,
                confidence=0.9,
            ),
        ),
    ]

    for pattern, builder in patterns:
        for match in pattern.finditer(stripped):
            try:
                candidate = builder(match)
            except Exception:
                continue
            if len(candidate.fact_value) < 2:
                continue
            out.append(candidate)

    # Deduplicate within the extraction pass.
    unique: dict[tuple[str, str, str, str], FactCandidate] = {}
    for candidate in out:
        key = (candidate.category, candidate.entity.lower(), candidate.fact_key.lower(), candidate.fact_value.lower())
        unique[key] = candidate
    return list(unique.values())


def parse_memory_lookup_query(text: str) -> str | None:
    stripped = _normalize_text(text)
    lower = stripped.lower()

    patterns = [
        r"\bwhat(?:'s| is)\s+my\s+(.+?)\??$",
        r"\bwhen\s+is\s+my\s+(.+?)\??$",
        r"\bwhat(?:'s| is)\s+([A-Za-z][A-Za-z0-9 ._-]{0,60})'s\s+(.+?)\??$",
        r"\bdo\s+you\s+remember\s+(.+?)\??$",
        r"\bmemory\s+query\s+(.+)$",
    ]

    for raw in patterns:
        match = re.search(raw, lower, re.IGNORECASE)
        if not match:
            continue
        if len(match.groups()) == 2:
            return f"{match.group(1)} {match.group(2)}"
        return match.group(1).strip()

    return None

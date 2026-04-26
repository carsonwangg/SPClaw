from __future__ import annotations

from spclaw.memory_extraction import extract_fact_candidates, parse_memory_lookup_query


def test_extract_decision_and_convention_candidates():
    text = "We decided to use SQLite + FTS5 because most queries are structured. We always run tests before deploying."
    candidates = extract_fact_candidates(text, source="unit-test", source_ts_utc="2026-02-18T00:00:00+00:00")

    categories = {c.category for c in candidates}
    assert "decision" in categories
    assert "convention" in categories

    decision = next(c for c in candidates if c.category == "decision")
    assert decision.tier == "permanent"
    assert "structured" in decision.fact_value.lower()


def test_extract_relationship_fact_candidate():
    text = "My daughter's birthday is June 3rd"
    candidates = extract_fact_candidates(text, source="unit-test", source_ts_utc="2026-02-18T00:00:00+00:00")
    assert candidates
    top = candidates[0]
    assert top.tier == "permanent"
    assert "birthday" in top.fact_key.lower()


def test_parse_memory_lookup_query():
    assert parse_memory_lookup_query("what is my daughter's birthday?") == "daughter's birthday"
    assert parse_memory_lookup_query("memory query sqlite decision") == "sqlite decision"
    assert parse_memory_lookup_query("show me a chart") is None

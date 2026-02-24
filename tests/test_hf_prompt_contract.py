from __future__ import annotations

from coatue_claw.hf_prompt_contract import CitationRef, build_scorecard, validate_section_citations


def test_build_scorecard_bounds_and_weighted_total() -> None:
    scorecard = build_scorecard(
        growth=7,
        quality=-5,
        valuation=3,
        catalyst=2,
        risk=4,
        confidence_label="medium",
    )
    rows = {row.factor: row.score for row in scorecard.rows}
    assert rows["Growth"] == 5
    assert rows["Quality"] == 1
    assert rows["Valuation"] == 3
    assert rows["Catalyst"] == 2
    assert rows["Risk"] == 4
    assert 0 <= scorecard.weighted_total <= 100
    assert scorecard.confidence_label == "Medium"


def test_validate_section_citations_rejects_missing_fields() -> None:
    section_citations = {
        "aaa_snapshot": (CitationRef(source_ref="file-1", source_ts_utc="2026-02-24T00:00:00+00:00"),),
        "variant_view": (CitationRef(source_ref="", source_ts_utc="2026-02-24T00:00:00+00:00"),),
        "scorecard": (CitationRef(source_ref="file-2", source_ts_utc=""),),
        "catalysts": (),
        "risks": (),
        "verify_next": (),
    }
    errors = validate_section_citations(section_citations)
    assert any("missing_source_ref:variant_view" in err for err in errors)
    assert any("missing_source_ts:scorecard" in err for err in errors)
    assert any("missing_citations:catalysts" in err for err in errors)

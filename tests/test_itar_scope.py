from __future__ import annotations

import csv
import json
from pathlib import Path

from coatue_claw.itar_scope import build_itar_scope_dataset, diff_snapshots, parse_year_snapshot


def _wrap_extract(year: int, extract_body: str) -> str:
    return f"""<?xml version="1.0"?>
<CFRGRANULE>
  <FDSYS>
    <DATE>{year}-04-01</DATE>
  </FDSYS>
  <SUBCHAP TYPE="P">
    <PART>
      <EAR>Pt. 121</EAR>
      <SECTION>
        <SECTNO>§ 121.1</SECTNO>
        <SUBJECT>The United States Munitions List.</SUBJECT>
        <EXTRACT>
          {extract_body}
        </EXTRACT>
      </SECTION>
    </PART>
  </SUBCHAP>
</CFRGRANULE>
"""


def test_parse_year_snapshot_counts_only_product_leaf_entries() -> None:
    xml_text = _wrap_extract(
        2024,
        """
<HD SOURCE="HD1">Category I—Firearms and Related Articles</HD>
<P>(a) Foo firearms.</P>
<P>(b) Parts, components, accessories, and attachments, as follows:</P>
<P>(1) Barrels.</P>
<P>(2) Bolts.</P>
<P>(c) Technical data directly related to the defense articles in this category.</P>
<P>(d) The following interpretations explain the terms used in this category:</P>
<P>(1) A firearm means a weapon.</P>
<NOTE>
  <HD SOURCE="HED">Note 1 to Category I:</HD>
  <P>(1) This note should be ignored.</P>
</NOTE>
<HD SOURCE="HD1">Category XIV—Toxicological Agents</HD>
<P>(d) Protective equipment, as follows:</P>
<P>(4)(i) Individual protection against agents.</P>
""",
    )

    snapshot = parse_year_snapshot(2024, xml_text=xml_text)
    counted_ids = [entry.entry_id for entry in snapshot.counted_entries]
    all_leaf_ids = {entry.entry_id: entry for entry in snapshot.leaf_entries}

    assert snapshot.snapshot_date == "2024-04-01"
    assert counted_ids == [
        "Category I(a)",
        "Category I(b)(1)",
        "Category I(b)(2)",
        "Category XIV(d)(4)(i)",
    ]
    assert all_leaf_ids["Category I(c)"].exclude_reason == "non_product_clause"
    assert all_leaf_ids["Category I(d)(1)"].exclude_reason == "interpretive_branch"


def test_parse_year_snapshot_ignores_section_121_16_prefix_collision() -> None:
    xml_text = """<?xml version="1.0"?>
<CFRGRANULE>
  <FDSYS><DATE>2020-04-01</DATE></FDSYS>
  <SUBCHAP TYPE="P">
    <PART>
      <EAR>Pt. 121</EAR>
      <SECTION>
        <SECTNO>§ 121.16</SECTNO>
        <EXTRACT>
          <HD SOURCE="HD1">Item 1—Category I</HD>
          <P>(a) This should not be parsed.</P>
        </EXTRACT>
      </SECTION>
      <SECTION>
        <SECTNO>§ 121.1</SECTNO>
        <EXTRACT>
          <HD SOURCE="HD1">Category I—Firearms and Related Articles</HD>
          <P>(a) Foo firearms.</P>
        </EXTRACT>
      </SECTION>
    </PART>
  </SUBCHAP>
</CFRGRANULE>
"""

    snapshot = parse_year_snapshot(2020, xml_text=xml_text)

    assert [entry.entry_id for entry in snapshot.counted_entries] == ["Category I(a)"]


def test_diff_snapshots_tracks_add_remove_and_modified_classifications() -> None:
    previous_xml = _wrap_extract(
        2024,
        """
<HD SOURCE="HD1">Category I—Firearms and Related Articles</HD>
<P>(a) Foo firearms.</P>
<P>(b) Parts, components, accessories, and attachments, as follows:</P>
<P>(1) Barrels.</P>
<P>(2) Bolts.</P>
""",
    )
    current_xml = _wrap_extract(
        2025,
        """
<HD SOURCE="HD1">Category I—Firearms and Related Articles</HD>
<P>(a) Foo firearms, excluding prototypes.</P>
<P>(b) Parts, components, accessories, and attachments, as follows:</P>
<P>(1) Barrels and bolts.</P>
<P>(3) Slides.</P>
""",
    )

    previous = parse_year_snapshot(2024, xml_text=previous_xml)
    current = parse_year_snapshot(2025, xml_text=current_xml)
    changes, ambiguous_rewrites, panel_row = diff_snapshots(previous, current)

    added = [change.entry_id for change in changes if change.change_type == "added"]
    removed = [change.entry_id for change in changes if change.change_type == "removed"]
    modified = {change.entry_id: change.classification for change in changes if change.change_type == "modified"}

    assert added == ["Category I(b)(3)"]
    assert removed == ["Category I(b)(2)"]
    assert modified["Category I(a)"] == "narrowed"
    assert modified["Category I(b)(1)"] == "broadened"
    assert panel_row.entries_added == 1
    assert panel_row.entries_removed == 1
    assert panel_row.entries_broadened == 1
    assert panel_row.entries_narrowed == 1
    assert panel_row.net_entry_change == 0
    assert ambiguous_rewrites == []


def test_build_itar_scope_dataset_writes_artifacts_and_split_audit(tmp_path: Path) -> None:
    xml_by_year = {
        2024: _wrap_extract(
            2024,
            """
<HD SOURCE="HD1">Category II—Guns and Armament</HD>
<P>(a) Guidance and control components.</P>
""",
        ),
        2025: _wrap_extract(
            2025,
            """
<HD SOURCE="HD1">Category II—Guns and Armament</HD>
<P>(a) Components, as follows:</P>
<P>(1) Guidance components.</P>
<P>(2) Control components.</P>
""",
        ),
    }

    result = build_itar_scope_dataset(start_year=2024, end_year=2025, artifact_dir=tmp_path, xml_by_year=xml_by_year)

    assert result.entries_csv.exists()
    assert result.yearly_panel_csv.exists()
    assert result.changes_csv.exists()
    assert result.ambiguous_rewrites_csv.exists()
    assert result.summary_json.exists()
    assert result.summary_markdown.exists()

    with result.yearly_panel_csv.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["entries_total"] == "1"
    assert rows[1]["entries_total"] == "2"
    assert rows[1]["entries_added"] == "2"
    assert rows[1]["entries_removed"] == "1"

    with result.ambiguous_rewrites_csv.open(encoding="utf-8", newline="") as handle:
        rewrites = list(csv.DictReader(handle))
    assert rewrites[0]["audit_type"] == "split"
    assert rewrites[0]["previous_entry_ids"] == "Category II(a)"
    assert rewrites[0]["current_entry_ids"] == "Category II(a)(1);Category II(a)(2)"

    payload = json.loads(result.summary_json.read_text(encoding="utf-8"))
    assert payload["snapshot_count"] == 2
    assert payload["change_counts"]["added"] == 2
    assert payload["change_counts"]["removed"] == 1

from __future__ import annotations

import csv
import json
from pathlib import Path

from coatue_claw.itar_scope import (
    build_corrected_scope_dataset,
    build_itar_scope_dataset,
    diff_snapshots,
    official_scope_events,
    parse_year_snapshot,
    plot_itar_scope_yearly_changes,
)


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
    assert result.net_entry_change_chart_png.exists()

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


def test_plot_itar_scope_yearly_changes_writes_png(tmp_path: Path) -> None:
    panel_csv = tmp_path / "yearly_panel.csv"
    with panel_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "year",
                "snapshot_date",
                "source_url",
                "rule_citations",
                "usml_categories_total",
                "entries_total",
                "entries_added",
                "entries_removed",
                "entries_broadened",
                "entries_narrowed",
                "entries_moved_to_ear",
                "entries_moved_from_ear",
                "net_entry_change",
                "net_scope_change",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "year": "2022",
                "snapshot_date": "2022-04-01",
                "source_url": "https://example.com/2022",
                "rule_citations": "https://example.com/2022",
                "usml_categories_total": "21",
                "entries_total": "810",
                "entries_added": "0",
                "entries_removed": "0",
                "entries_broadened": "0",
                "entries_narrowed": "0",
                "entries_moved_to_ear": "0",
                "entries_moved_from_ear": "0",
                "net_entry_change": "0",
                "net_scope_change": "0",
            }
        )
        writer.writerow(
            {
                "year": "2023",
                "snapshot_date": "2023-04-01",
                "source_url": "https://example.com/2023",
                "rule_citations": "https://example.com/2023",
                "usml_categories_total": "21",
                "entries_total": "812",
                "entries_added": "24",
                "entries_removed": "22",
                "entries_broadened": "0",
                "entries_narrowed": "0",
                "entries_moved_to_ear": "0",
                "entries_moved_from_ear": "0",
                "net_entry_change": "2",
                "net_scope_change": "2",
            }
        )

    output_png = tmp_path / "net_entry_change.png"
    result = plot_itar_scope_yearly_changes(panel_csv=panel_csv, output_png=output_png, metric="net_entry_change")

    assert result.output_png == output_png
    assert result.row_count == 2
    assert output_png.exists()
    assert output_png.stat().st_size > 0


def test_official_scope_events_include_expected_negative_and_positive_years() -> None:
    events = official_scope_events()

    assert any(event.year == 2014 and event.net_scope_delta < 0 for event in events)
    assert any(event.year == 2020 and event.net_scope_delta < 0 for event in events)
    assert any(event.year == 2025 and event.net_scope_delta > 0 for event in events)


def test_build_corrected_scope_dataset_writes_yearly_and_chart_outputs(tmp_path: Path) -> None:
    result = build_corrected_scope_dataset(start_year=2013, end_year=2025, artifact_dir=tmp_path)

    assert result.events_csv.exists()
    assert result.yearly_csv.exists()
    assert result.added_removed_chart_png.exists()
    assert result.net_change_chart_png.exists()
    assert result.cumulative_chart_png.exists()
    assert result.summary_markdown.exists()

    with result.yearly_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = {int(row["year"]): row for row in csv.DictReader(handle)}

    assert rows[2013]["net_scope_delta"] == "-2"
    assert rows[2014]["net_scope_delta"] == "-10"
    assert rows[2020]["net_scope_delta"] == "-3"
    assert rows[2025]["net_scope_delta"] == "1"

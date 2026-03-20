from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
import json
import logging
import os
from pathlib import Path
import re
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

GOVINFO_USML_URL = "https://www.govinfo.gov/content/pkg/CFR-{year}-title22-vol1/xml/CFR-{year}-title22-vol1-chapI-subchapM.xml"
GOVINFO_USML_SECTION_URL = "https://www.govinfo.gov/content/pkg/CFR-{year}-title22-vol1/xml/CFR-{year}-title22-vol1-sec121-1.xml"
DEFAULT_ITAR_SCOPE_ARTIFACT_DIR = Path(
    os.getenv("COATUE_CLAW_ITAR_SCOPE_ARTIFACT_DIR", "/opt/coatue-claw-data/artifacts/itar-scope")
)

ENTRY_PREFIX_RE = re.compile(r"^\*?\s*((?:\([A-Za-z0-9ivxlcdmIVXLCDM]+\))+)")
ENTRY_TOKEN_RE = re.compile(r"\(([A-Za-z0-9ivxlcdmIVXLCDM]+)\)")
CATEGORY_HEADING_RE = re.compile(r"^Category\s+([IVXLCDM]+)[—-](.+)$")
WHITESPACE_RE = re.compile(r"\s+")
WORD_RE = re.compile(r"\b[\w.-]+\b")
SECTION_121_1_RE = re.compile(r"(^|[^\d])121\.1([^\d]|$)")
ROMAN_SUBLEVEL_TOKENS = {
    "i",
    "ii",
    "iii",
    "iv",
    "v",
    "vi",
    "vii",
    "viii",
    "ix",
    "x",
    "xi",
    "xii",
    "xiii",
    "xiv",
    "xv",
    "xvi",
    "xvii",
    "xviii",
    "xix",
    "xx",
}

NON_PRODUCT_PATTERNS = (
    re.compile(r"\btechnical data\b", re.IGNORECASE),
    re.compile(r"\bdefense services?\b", re.IGNORECASE),
    re.compile(r"\bsoftware\b", re.IGNORECASE),
    re.compile(r"\btechnology\b", re.IGNORECASE),
    re.compile(r"\bsubject to the EAR\b", re.IGNORECASE),
)
INTERPRETIVE_BRANCH_PATTERNS = (
    re.compile(r"\bthe following interpretations explain\b", re.IGNORECASE),
    re.compile(r"\bthe following interpretations explain and amplify\b", re.IGNORECASE),
    re.compile(r"\bfor purposes of this category\b", re.IGNORECASE),
    re.compile(r"\bmeans a\b", re.IGNORECASE),
    re.compile(r"\bmeans an\b", re.IGNORECASE),
)
EAR_MARKER_PATTERNS = (
    re.compile(r"\bsubject to the EAR\b", re.IGNORECASE),
    re.compile(r"\bECCN\b", re.IGNORECASE),
    re.compile(r"\bCommerce Control List\b", re.IGNORECASE),
    re.compile(r"\bEAR99\b", re.IGNORECASE),
)
NARROWING_MARKERS = ("except", "excluding", "other than", "limited to", "only")


@dataclass(frozen=True)
class UsmlLeafEntry:
    year: int
    category: str
    entry_path: tuple[str, ...]
    entry_id: str
    entry_text: str
    is_product_entry: bool
    source_url: str
    exclude_reason: str


@dataclass(frozen=True)
class UsmlEntryNode:
    category: str
    entry_path: tuple[str, ...]
    entry_id: str
    entry_text: str
    parent_entry_id: str | None
    has_children: bool
    interpretive_branch: bool


@dataclass(frozen=True)
class YearSnapshot:
    year: int
    snapshot_date: str
    source_url: str
    category_count: int
    leaf_entries: tuple[UsmlLeafEntry, ...]

    @property
    def counted_entries(self) -> tuple[UsmlLeafEntry, ...]:
        return tuple(entry for entry in self.leaf_entries if entry.is_product_entry)


@dataclass(frozen=True)
class EntryChange:
    year: int
    previous_year: int
    category: str
    entry_id: str
    change_type: str
    classification: str
    previous_text: str
    current_text: str
    source_url: str


@dataclass(frozen=True)
class AmbiguousRewrite:
    year: int
    previous_year: int
    audit_type: str
    category: str
    previous_entry_ids: str
    current_entry_ids: str
    similarity_score: float


@dataclass(frozen=True)
class YearPanelRow:
    year: int
    snapshot_date: str
    source_url: str
    rule_citations: str
    usml_categories_total: int
    entries_total: int
    entries_added: int
    entries_removed: int
    entries_broadened: int
    entries_narrowed: int
    entries_moved_to_ear: int
    entries_moved_from_ear: int
    net_entry_change: int
    net_scope_change: int


@dataclass(frozen=True)
class ItarScopeBuildResult:
    start_year: int
    end_year: int
    artifact_dir: Path
    entries_csv: Path
    yearly_panel_csv: Path
    changes_csv: Path
    ambiguous_rewrites_csv: Path
    summary_json: Path
    summary_markdown: Path
    net_entry_change_chart_png: Path
    snapshot_count: int


@dataclass(frozen=True)
class ItarScopePlotResult:
    panel_csv: Path
    output_png: Path
    metric: str
    row_count: int


@dataclass(frozen=True)
class OfficialScopeEvent:
    effective_date: str
    year: int
    title: str
    positive_units: int
    negative_units: int
    net_scope_delta: int
    source_label: str
    source_url: str
    notes: str


@dataclass(frozen=True)
class CorrectedScopeYearRow:
    year: int
    positive_units: int
    negative_units: int
    net_scope_delta: int
    cumulative_scope_index: int


@dataclass(frozen=True)
class CorrectedScopeBuildResult:
    artifact_dir: Path
    events_csv: Path
    yearly_csv: Path
    added_removed_chart_png: Path
    net_change_chart_png: Path
    cumulative_chart_png: Path
    summary_markdown: Path


def _normalize_space(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def official_scope_events() -> tuple[OfficialScopeEvent, ...]:
    return (
        OfficialScopeEvent(
            effective_date="2013-10-15",
            year=2013,
            title="Export Control Reform tranche for Categories VIII and XIX",
            positive_units=0,
            negative_units=2,
            net_scope_delta=-2,
            source_label="BIS — 600 Series Items FAQs",
            source_url="https://www.bis.gov/media/documents/600-series-items-faqs.pdf",
            notes="Effective date shown for Aircraft (Category VIII) and Gas Turbine Engines (Category XIX) transition-related 600-series controls.",
        ),
        OfficialScopeEvent(
            effective_date="2014-01-06",
            year=2014,
            title="Export Control Reform tranche for Categories VI, VII, XIII, and XX",
            positive_units=0,
            negative_units=4,
            net_scope_delta=-4,
            source_label="BIS — 600 Series Items FAQs",
            source_url="https://www.bis.gov/media/documents/600-series-items-faqs.pdf",
            notes="Effective date shown for Vessels/Submersibles (VI/XX), Military Vehicles (VII), and Auxiliary Military Equipment (XIII).",
        ),
        OfficialScopeEvent(
            effective_date="2014-07-01",
            year=2014,
            title="Export Control Reform tranche for Categories IV, V, IX, and X",
            positive_units=0,
            negative_units=4,
            net_scope_delta=-4,
            source_label="BIS — 600 Series Items FAQs",
            source_url="https://www.bis.gov/media/documents/600-series-items-faqs.pdf",
            notes="Effective date shown for Rockets/Missiles (IV), Explosives/Energetics (V), Training Equipment (IX), and Personal Protective Equipment (X).",
        ),
        OfficialScopeEvent(
            effective_date="2014-11-10",
            year=2014,
            title="Export Control Reform tranche for Category XV spacecraft systems",
            positive_units=0,
            negative_units=1,
            net_scope_delta=-1,
            source_label="BIS — 600 Series Items FAQs",
            source_url="https://www.bis.gov/media/documents/600-series-items-faqs.pdf",
            notes="FAQ table lists November 10, 2014 as the effective date for all other Category XV items beyond the earlier rad-hard tranche.",
        ),
        OfficialScopeEvent(
            effective_date="2014-12-30",
            year=2014,
            title="Export Control Reform tranche for Category XI military electronics",
            positive_units=0,
            negative_units=1,
            net_scope_delta=-1,
            source_label="BIS — 600 Series Items FAQs",
            source_url="https://www.bis.gov/media/documents/600-series-items-faqs.pdf",
            notes="FAQ table lists December 30, 2014 as the effective date for Category XI military electronics transition-related controls.",
        ),
        OfficialScopeEvent(
            effective_date="2016-12-31",
            year=2016,
            title="Export Control Reform tranche for Categories XII, XIV, and XVIII",
            positive_units=0,
            negative_units=3,
            net_scope_delta=-3,
            source_label="BIS — 600 Series Items FAQs",
            source_url="https://www.bis.gov/media/documents/600-series-items-faqs.pdf",
            notes="FAQ table lists December 31, 2016 as the effective date for Sensors/Night Vision (XII), Toxicological Agents (XIV), and Directed Energy Weapons (XVIII).",
        ),
        OfficialScopeEvent(
            effective_date="2020-03-09",
            year=2020,
            title="Firearms rule moving Categories I, II, and III items to Commerce control",
            positive_units=0,
            negative_units=3,
            net_scope_delta=-3,
            source_label="BIS — Firearms Final Rule Summary",
            source_url="https://www.bis.gov/86-fr-46590-control-firearms-guns-ammunition-related-articles-president-determines-no-longer-warrant-control-under",
            notes="BIS states the January 23, 2020 firearms final rule moved items that no longer warranted USML control from Categories I-III to the CCL; the rule became effective March 9, 2020.",
        ),
        OfficialScopeEvent(
            effective_date="2025-09-15",
            year=2025,
            title="USML targeted revisions package with mixed but net-positive ITAR effect",
            positive_units=3,
            negative_units=2,
            net_scope_delta=1,
            source_label="Federal Register — USML Targeted Revisions (2025 final rule)",
            source_url="https://public-inspection.federalregister.gov/2025-16382.pdf",
            notes="Final rule states it both removes and adds USML items. This implementation scores the package as net +1 because the rule adds or reinforces ITAR coverage in Categories VIII, X, and XX while removing or narrowing items in Categories III and XI.",
        ),
    )


def _element_text(element: ET.Element) -> str:
    return _normalize_space(" ".join(text for text in element.itertext() if text and text.strip()))


def _fetch_url_text(url: str, timeout_seconds: int = 30) -> str:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload = response.read()
    except HTTPError as exc:
        raise RuntimeError(f"Failed to fetch {url}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc.reason}") from exc
    return payload.decode("utf-8")


def fetch_year_snapshot_xml(year: int) -> str:
    url = GOVINFO_USML_URL.format(year=year)
    return _fetch_url_text(url)


def fetch_year_section_xml(year: int) -> str:
    url = GOVINFO_USML_SECTION_URL.format(year=year)
    return _fetch_url_text(url)


def _extract_snapshot_date(root: ET.Element) -> str:
    date_text = root.findtext("./FDSYS/DATE") or root.findtext(".//DATE") or ""
    return date_text.strip()


def _category_heading_count(container: ET.Element) -> int:
    count = 0
    for child in list(container):
        if child.tag != "HD":
            continue
        if CATEGORY_HEADING_RE.match(_element_text(child)):
            count += 1
    return count


def _find_part_121_container(root: ET.Element) -> ET.Element:
    best_container: ET.Element | None = None
    best_score = -1
    for part in root.findall(".//PART"):
        if (part.findtext("EAR") or "").strip() != "Pt. 121":
            continue
        for section in part.findall(".//SECTION"):
            sectno = (section.findtext("SECTNO") or "").strip()
            if not SECTION_121_1_RE.search(sectno):
                continue
            extract = section.find("EXTRACT")
            container = extract if extract is not None else section
            score = _category_heading_count(container)
            if score > best_score:
                best_container = container
                best_score = score
    if best_container is None:
        raise ValueError("Could not locate 22 CFR 121.1 extract in the GovInfo XML payload")
    return best_container


def _build_snapshot_from_container(year: int, root: ET.Element, container: ET.Element, source_url: str) -> YearSnapshot:
    snapshot_date = _extract_snapshot_date(root)

    nodes: dict[str, UsmlEntryNode] = {}
    children: dict[str, set[str]] = {}
    current_category = ""
    current_path: tuple[str, ...] = ()
    category_labels: set[str] = set()

    for child in list(container):
        if child.tag == "HD":
            heading = _element_text(child)
            match = CATEGORY_HEADING_RE.match(heading)
            if match:
                current_category = f"Category {match.group(1)}"
                current_path = ()
                category_labels.add(current_category)
            continue
        if child.tag != "P" or not current_category:
            continue

        text = _element_text(child)
        parsed = _parse_prefix_tokens(text)
        if parsed is None:
            continue
        tokens, body = parsed
        path = _resolve_entry_path(tokens, current_path)
        current_path = path
        entry_id = _entry_id(current_category, path)
        parent_id: str | None = None
        if len(path) > 1:
            for index in range(len(path) - 1, 0, -1):
                candidate = _entry_id(current_category, path[:index])
                if candidate in nodes:
                    parent_id = candidate
                    break
        nodes[entry_id] = UsmlEntryNode(
            category=current_category,
            entry_path=path,
            entry_id=entry_id,
            entry_text=body,
            parent_entry_id=parent_id,
            has_children=False,
            interpretive_branch=False,
        )
        if parent_id:
            children.setdefault(parent_id, set()).add(entry_id)

    finalized_nodes: dict[str, UsmlEntryNode] = {}
    for entry_id, node in sorted(nodes.items(), key=lambda item: (item[1].category, len(item[1].entry_path), item[1].entry_path)):
        parent_interpretive = finalized_nodes[node.parent_entry_id].interpretive_branch if node.parent_entry_id else False
        interpretive_branch = parent_interpretive or _is_interpretive_branch(node.entry_text)
        finalized_nodes[entry_id] = UsmlEntryNode(
            category=node.category,
            entry_path=node.entry_path,
            entry_id=node.entry_id,
            entry_text=node.entry_text,
            parent_entry_id=node.parent_entry_id,
            has_children=bool(children.get(entry_id)),
            interpretive_branch=interpretive_branch,
        )

    leaf_entries: list[UsmlLeafEntry] = []
    for entry_id, node in finalized_nodes.items():
        if node.has_children:
            continue
        exclude_reason = _exclude_reason(node.entry_text, node.interpretive_branch)
        leaf_entries.append(
            UsmlLeafEntry(
                year=year,
                category=node.category,
                entry_path=node.entry_path,
                entry_id=node.entry_id,
                entry_text=node.entry_text,
                is_product_entry=exclude_reason == "",
                source_url=source_url,
                exclude_reason=exclude_reason,
            )
        )

    leaf_entries.sort(key=lambda item: (item.category, item.entry_path))
    return YearSnapshot(
        year=year,
        snapshot_date=snapshot_date,
        source_url=source_url,
        category_count=len(category_labels),
        leaf_entries=tuple(leaf_entries),
    )


def _parse_prefix_tokens(text: str) -> tuple[list[str], str] | None:
    match = ENTRY_PREFIX_RE.match(text)
    if not match:
        return None
    tokens = [token.lower() for token in ENTRY_TOKEN_RE.findall(match.group(1))]
    body = _normalize_space(text[match.end() :])
    return tokens, body


def _resolve_entry_path(tokens: list[str], current_path: tuple[str, ...]) -> tuple[str, ...]:
    if not tokens:
        return current_path
    first_token = tokens[0]
    prefix: tuple[str, ...] = ()
    if first_token.isdigit():
        prefix = current_path[:1] if current_path else ()
    elif first_token in ROMAN_SUBLEVEL_TOKENS and len(current_path) >= 2 and current_path[1].isdigit():
        prefix = current_path[:2]
    return prefix + tuple(tokens)


def _entry_id(category: str, path: Iterable[str]) -> str:
    suffix = "".join(f"({token})" for token in path)
    return f"{category}{suffix}"


def _is_interpretive_branch(text: str) -> bool:
    normalized = _normalize_space(text)
    return any(pattern.search(normalized) for pattern in INTERPRETIVE_BRANCH_PATTERNS)


def _exclude_reason(text: str, interpretive_branch: bool) -> str:
    normalized = _normalize_space(text)
    if "[Reserved]" in normalized:
        return "reserved"
    if interpretive_branch:
        return "interpretive_branch"
    for pattern in NON_PRODUCT_PATTERNS:
        if pattern.search(normalized):
            return "non_product_clause"
    return ""


def parse_year_snapshot(year: int, xml_text: str | None = None) -> YearSnapshot:
    source_url = GOVINFO_USML_URL.format(year=year)
    payload = xml_text if xml_text is not None else fetch_year_snapshot_xml(year)
    root = ET.fromstring(payload)
    try:
        container = _find_part_121_container(root)
        snapshot = _build_snapshot_from_container(year=year, root=root, container=container, source_url=source_url)
        if xml_text is not None:
            return snapshot
        if snapshot.category_count >= 5 and len(snapshot.leaf_entries) >= 50:
            return snapshot
        logger.warning("Primary GovInfo package XML for %s looked incomplete; falling back to section XML", year)
    except ValueError:
        if xml_text is not None:
            raise
        logger.warning("Primary GovInfo package XML for %s did not expose 121.1 cleanly; falling back to section XML", year)

    section_source_url = GOVINFO_USML_SECTION_URL.format(year=year)
    section_payload = fetch_year_section_xml(year)
    section_root = ET.fromstring(section_payload)
    container = _find_part_121_container(section_root)
    return _build_snapshot_from_container(year=year, root=section_root, container=container, source_url=section_source_url)


def _canonical_text(text: str) -> str:
    return " ".join(WORD_RE.findall(_normalize_space(text).lower()))


def _contains_ear_marker(text: str) -> bool:
    return any(pattern.search(text) for pattern in EAR_MARKER_PATTERNS)


def classify_modified_entry(previous_text: str, current_text: str) -> str:
    prev_canonical = _canonical_text(previous_text)
    curr_canonical = _canonical_text(current_text)
    if prev_canonical == curr_canonical:
        return "editorial_only"

    prev_has_ear = _contains_ear_marker(previous_text)
    curr_has_ear = _contains_ear_marker(current_text)
    if not prev_has_ear and curr_has_ear:
        return "jurisdiction_shift_to_ear"
    if prev_has_ear and not curr_has_ear:
        return "jurisdiction_shift_from_ear"

    prev_words = prev_canonical.split()
    curr_words = curr_canonical.split()
    prev_narrow = sum(marker in prev_canonical for marker in NARROWING_MARKERS)
    curr_narrow = sum(marker in curr_canonical for marker in NARROWING_MARKERS)
    similarity = SequenceMatcher(None, prev_canonical, curr_canonical).ratio()

    if similarity >= 0.97:
        return "editorial_only"
    if curr_narrow > prev_narrow or len(curr_words) < len(prev_words) * 0.9:
        return "narrowed"
    if curr_narrow < prev_narrow or len(curr_words) > len(prev_words) * 1.1:
        return "broadened"
    return "editorial_only" if similarity >= 0.92 else "narrowed"


def diff_snapshots(previous: YearSnapshot, current: YearSnapshot) -> tuple[list[EntryChange], list[AmbiguousRewrite], YearPanelRow]:
    previous_map = {entry.entry_id: entry for entry in previous.counted_entries}
    current_map = {entry.entry_id: entry for entry in current.counted_entries}

    added_ids = sorted(set(current_map) - set(previous_map))
    removed_ids = sorted(set(previous_map) - set(current_map))
    shared_ids = sorted(set(previous_map) & set(current_map))

    changes: list[EntryChange] = []
    broadened = 0
    narrowed = 0
    moved_to_ear = 0
    moved_from_ear = 0

    for entry_id in added_ids:
        entry = current_map[entry_id]
        changes.append(
            EntryChange(
                year=current.year,
                previous_year=previous.year,
                category=entry.category,
                entry_id=entry_id,
                change_type="added",
                classification="added",
                previous_text="",
                current_text=entry.entry_text,
                source_url=current.source_url,
            )
        )

    for entry_id in removed_ids:
        entry = previous_map[entry_id]
        changes.append(
            EntryChange(
                year=current.year,
                previous_year=previous.year,
                category=entry.category,
                entry_id=entry_id,
                change_type="removed",
                classification="removed",
                previous_text=entry.entry_text,
                current_text="",
                source_url=current.source_url,
            )
        )

    for entry_id in shared_ids:
        previous_entry = previous_map[entry_id]
        current_entry = current_map[entry_id]
        classification = classify_modified_entry(previous_entry.entry_text, current_entry.entry_text)
        if classification == "editorial_only" and _canonical_text(previous_entry.entry_text) == _canonical_text(current_entry.entry_text):
            continue
        if classification == "broadened":
            broadened += 1
        elif classification == "narrowed":
            narrowed += 1
        elif classification == "jurisdiction_shift_to_ear":
            moved_to_ear += 1
        elif classification == "jurisdiction_shift_from_ear":
            moved_from_ear += 1
        changes.append(
            EntryChange(
                year=current.year,
                previous_year=previous.year,
                category=current_entry.category,
                entry_id=entry_id,
                change_type="modified",
                classification=classification,
                previous_text=previous_entry.entry_text,
                current_text=current_entry.entry_text,
                source_url=current.source_url,
            )
        )

    ambiguous_rewrites = _build_ambiguous_rewrite_audit(previous, current, previous_map, current_map)

    panel_row = YearPanelRow(
        year=current.year,
        snapshot_date=current.snapshot_date,
        source_url=current.source_url,
        rule_citations=current.source_url,
        usml_categories_total=current.category_count,
        entries_total=len(current_map),
        entries_added=len(added_ids),
        entries_removed=len(removed_ids),
        entries_broadened=broadened,
        entries_narrowed=narrowed,
        entries_moved_to_ear=moved_to_ear,
        entries_moved_from_ear=moved_from_ear,
        net_entry_change=len(added_ids) - len(removed_ids),
        net_scope_change=len(added_ids) + moved_from_ear - len(removed_ids) - moved_to_ear,
    )
    return changes, ambiguous_rewrites, panel_row


def _text_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, _canonical_text(left), _canonical_text(right)).ratio()


def _build_ambiguous_rewrite_audit(
    previous: YearSnapshot,
    current: YearSnapshot,
    previous_map: dict[str, UsmlLeafEntry],
    current_map: dict[str, UsmlLeafEntry],
) -> list[AmbiguousRewrite]:
    removed = [entry for entry in previous.counted_entries if entry.entry_id not in current_map]
    added = [entry for entry in current.counted_entries if entry.entry_id not in previous_map]
    candidate_links: list[tuple[UsmlLeafEntry, UsmlLeafEntry, float]] = []

    for previous_entry in removed:
        for current_entry in added:
            if previous_entry.category != current_entry.category:
                continue
            similarity = _text_similarity(previous_entry.entry_text, current_entry.entry_text)
            if similarity >= 0.55:
                candidate_links.append((previous_entry, current_entry, similarity))

    rewrites: list[AmbiguousRewrite] = []
    by_removed: dict[str, list[tuple[str, float]]] = {}
    by_added: dict[str, list[tuple[str, float]]] = {}
    for previous_entry, current_entry, similarity in candidate_links:
        by_removed.setdefault(previous_entry.entry_id, []).append((current_entry.entry_id, similarity))
        by_added.setdefault(current_entry.entry_id, []).append((previous_entry.entry_id, similarity))

    for previous_entry_id, matches in by_removed.items():
        if len(matches) < 2:
            continue
        current_ids = sorted(match_id for match_id, _ in matches)
        average_similarity = sum(score for _, score in matches) / len(matches)
        rewrites.append(
            AmbiguousRewrite(
                year=current.year,
                previous_year=previous.year,
                audit_type="split",
                category=previous_map[previous_entry_id].category,
                previous_entry_ids=previous_entry_id,
                current_entry_ids=";".join(current_ids),
                similarity_score=round(average_similarity, 4),
            )
        )

    for current_entry_id, matches in by_added.items():
        if len(matches) < 2:
            continue
        previous_ids = sorted(match_id for match_id, _ in matches)
        average_similarity = sum(score for _, score in matches) / len(matches)
        rewrites.append(
            AmbiguousRewrite(
                year=current.year,
                previous_year=previous.year,
                audit_type="merge",
                category=current_map[current_entry_id].category,
                previous_entry_ids=";".join(previous_ids),
                current_entry_ids=current_entry_id,
                similarity_score=round(average_similarity, 4),
            )
        )

    rewrites.sort(key=lambda item: (item.year, item.audit_type, item.category, item.previous_entry_ids, item.current_entry_ids))
    return rewrites


def build_itar_scope_dataset(
    start_year: int = 2010,
    end_year: int = 2025,
    artifact_dir: Path | str | None = None,
    xml_by_year: dict[int, str] | None = None,
) -> ItarScopeBuildResult:
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    out_dir = Path(artifact_dir) if artifact_dir is not None else DEFAULT_ITAR_SCOPE_ARTIFACT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    snapshots: list[YearSnapshot] = []
    for year in range(start_year, end_year + 1):
        snapshot = parse_year_snapshot(year, xml_text=(xml_by_year or {}).get(year))
        snapshots.append(snapshot)

    panel_rows: list[YearPanelRow] = [
        YearPanelRow(
            year=snapshot.year,
            snapshot_date=snapshot.snapshot_date,
            source_url=snapshot.source_url,
            rule_citations=snapshot.source_url,
            usml_categories_total=snapshot.category_count,
            entries_total=len(snapshot.counted_entries),
            entries_added=0,
            entries_removed=0,
            entries_broadened=0,
            entries_narrowed=0,
            entries_moved_to_ear=0,
            entries_moved_from_ear=0,
            net_entry_change=0,
            net_scope_change=0,
        )
        for snapshot in snapshots[:1]
    ]
    changes: list[EntryChange] = []
    ambiguous_rewrites: list[AmbiguousRewrite] = []

    for previous, current in zip(snapshots, snapshots[1:]):
        year_changes, year_ambiguous, panel_row = diff_snapshots(previous, current)
        changes.extend(year_changes)
        ambiguous_rewrites.extend(year_ambiguous)
        panel_rows.append(panel_row)

    entries_csv = out_dir / "usml_entries.csv"
    yearly_panel_csv = out_dir / "yearly_panel.csv"
    changes_csv = out_dir / "entry_changes.csv"
    ambiguous_rewrites_csv = out_dir / "ambiguous_rewrites.csv"
    summary_json = out_dir / "summary.json"
    summary_markdown = out_dir / "summary.md"
    net_entry_change_chart_png = out_dir / "net_entry_change.png"

    _write_entries_csv(entries_csv, snapshots)
    _write_yearly_panel_csv(yearly_panel_csv, panel_rows)
    _write_changes_csv(changes_csv, changes)
    _write_ambiguous_rewrites_csv(ambiguous_rewrites_csv, ambiguous_rewrites)
    _write_summary_files(summary_json, summary_markdown, snapshots, panel_rows, changes, ambiguous_rewrites)
    plot_itar_scope_yearly_changes(
        panel_csv=yearly_panel_csv,
        output_png=net_entry_change_chart_png,
        metric="net_entry_change",
    )

    return ItarScopeBuildResult(
        start_year=start_year,
        end_year=end_year,
        artifact_dir=out_dir,
        entries_csv=entries_csv,
        yearly_panel_csv=yearly_panel_csv,
        changes_csv=changes_csv,
        ambiguous_rewrites_csv=ambiguous_rewrites_csv,
        summary_json=summary_json,
        summary_markdown=summary_markdown,
        net_entry_change_chart_png=net_entry_change_chart_png,
        snapshot_count=len(snapshots),
    )


def build_corrected_scope_dataset(
    start_year: int = 2010,
    end_year: int = 2025,
    artifact_dir: Path | str | None = None,
) -> CorrectedScopeBuildResult:
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    base_dir = Path(artifact_dir) if artifact_dir is not None else DEFAULT_ITAR_SCOPE_ARTIFACT_DIR
    out_dir = base_dir / "corrected-scope"
    out_dir.mkdir(parents=True, exist_ok=True)

    events = tuple(event for event in official_scope_events() if start_year <= event.year <= end_year)
    yearly_rows = _build_corrected_scope_year_rows(start_year=start_year, end_year=end_year, events=events)

    events_csv = out_dir / "official_scope_events.csv"
    yearly_csv = out_dir / "corrected_scope_yearly.csv"
    added_removed_chart_png = out_dir / "corrected_added_removed_by_year.png"
    net_change_chart_png = out_dir / "corrected_net_scope_change.png"
    cumulative_chart_png = out_dir / "corrected_cumulative_scope_index.png"
    summary_markdown = out_dir / "corrected_scope_summary.md"

    _write_corrected_scope_events_csv(events_csv, events)
    _write_corrected_scope_yearly_csv(yearly_csv, yearly_rows)
    plot_corrected_scope_added_removed(
        yearly_csv=yearly_csv,
        output_png=added_removed_chart_png,
    )
    plot_corrected_scope_years(
        yearly_csv=yearly_csv,
        output_png=net_change_chart_png,
        metric="net_scope_delta",
    )
    plot_corrected_scope_years(
        yearly_csv=yearly_csv,
        output_png=cumulative_chart_png,
        metric="cumulative_scope_index",
    )
    _write_corrected_scope_summary(summary_markdown, yearly_rows, events)

    return CorrectedScopeBuildResult(
        artifact_dir=out_dir,
        events_csv=events_csv,
        yearly_csv=yearly_csv,
        added_removed_chart_png=added_removed_chart_png,
        net_change_chart_png=net_change_chart_png,
        cumulative_chart_png=cumulative_chart_png,
        summary_markdown=summary_markdown,
    )


def plot_itar_scope_yearly_changes(
    panel_csv: Path | str,
    output_png: Path | str,
    metric: str = "net_entry_change",
) -> ItarScopePlotResult:
    panel_path = Path(panel_csv)
    output_path = Path(output_png)
    metric_key = str(metric).strip()
    metric_labels = {
        "net_entry_change": "Net Entry Change",
        "net_scope_change": "Net Scope Change",
    }
    if metric_key not in metric_labels:
        raise ValueError(f"Unsupported metric: {metric_key}")

    rows = _read_yearly_panel_csv(panel_path)
    years = [int(row["year"]) for row in rows]
    values = [int(row[metric_key]) for row in rows]

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    colors = ["#2f6f4f" if value >= 0 else "#8b2e2e" for value in values]
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(years, values, color=colors, edgecolor="#1f1f1f", linewidth=0.6)
    ax.axhline(0, color="#1f1f1f", linewidth=1.0)
    ax.set_title(f"ITAR USML Year-over-Year {metric_labels[metric_key]}", fontsize=14, pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel(metric_labels[metric_key])
    ax.set_xticks(years)
    ax.set_xticklabels([str(year) for year in years], rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.25, linewidth=0.7)

    value_span = max([abs(value) for value in values] + [1])
    offset = max(1.0, value_span * 0.03)
    for bar, value in zip(bars, values):
        if value >= 0:
            y_pos = value + offset
            va = "bottom"
        else:
            y_pos = value - offset
            va = "top"
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            y_pos,
            f"{value:+d}",
            ha="center",
            va=va,
            fontsize=8,
            color="#1f1f1f",
        )

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)

    return ItarScopePlotResult(
        panel_csv=panel_path,
        output_png=output_path,
        metric=metric_key,
        row_count=len(rows),
    )


def plot_corrected_scope_years(
    yearly_csv: Path | str,
    output_png: Path | str,
    metric: str,
) -> ItarScopePlotResult:
    yearly_path = Path(yearly_csv)
    output_path = Path(output_png)
    metric_key = str(metric).strip()
    metric_labels = {
        "net_scope_delta": "Corrected Net Scope Delta",
        "cumulative_scope_index": "Corrected Cumulative Scope Index",
    }
    if metric_key not in metric_labels:
        raise ValueError(f"Unsupported corrected scope metric: {metric_key}")

    rows = _read_csv_rows(yearly_path)
    years = [int(row["year"]) for row in rows]
    values = [int(row[metric_key]) for row in rows]

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 6))
    if metric_key == "net_scope_delta":
        colors = ["#2f6f4f" if value >= 0 else "#8b2e2e" for value in values]
        bars = ax.bar(years, values, color=colors, edgecolor="#1f1f1f", linewidth=0.6)
        ax.axhline(0, color="#1f1f1f", linewidth=1.0)
        value_span = max([abs(value) for value in values] + [1])
        offset = max(0.15, value_span * 0.08)
        for bar, value in zip(bars, values):
            if value >= 0:
                y_pos = value + offset
                va = "bottom"
            else:
                y_pos = value - offset
                va = "top"
            ax.text(bar.get_x() + bar.get_width() / 2, y_pos, f"{value:+d}", ha="center", va=va, fontsize=8)
    else:
        ax.plot(years, values, color="#204f86", linewidth=2.4, marker="o", markersize=5)
        ax.axhline(0, color="#1f1f1f", linewidth=1.0, alpha=0.7)
        for year, value in zip(years, values):
            ax.text(year, value + 0.18, f"{value:+d}", ha="center", va="bottom", fontsize=8)

    ax.set_title(f"ITAR {metric_labels[metric_key]} (Official Rule Event Model)", fontsize=14, pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel(metric_labels[metric_key])
    ax.set_xticks(years)
    ax.set_xticklabels([str(year) for year in years], rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.25, linewidth=0.7)
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)

    return ItarScopePlotResult(
        panel_csv=yearly_path,
        output_png=output_path,
        metric=metric_key,
        row_count=len(rows),
    )


def plot_corrected_scope_added_removed(
    yearly_csv: Path | str,
    output_png: Path | str,
) -> ItarScopePlotResult:
    yearly_path = Path(yearly_csv)
    output_path = Path(output_png)
    rows = _read_csv_rows(yearly_path)
    years = [int(row["year"]) for row in rows]
    added_values = [int(row["positive_units"]) for row in rows]
    removed_values = [-int(row["negative_units"]) for row in rows]

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 6))
    width = 0.38
    x_positions = list(range(len(years)))
    added_bars = ax.bar(
        [x - width / 2 for x in x_positions],
        added_values,
        width=width,
        color="#2f6f4f",
        edgecolor="#1f1f1f",
        linewidth=0.6,
        label="Categories / tranches added",
    )
    removed_bars = ax.bar(
        [x + width / 2 for x in x_positions],
        removed_values,
        width=width,
        color="#8b2e2e",
        edgecolor="#1f1f1f",
        linewidth=0.6,
        label="Categories / tranches removed",
    )
    ax.axhline(0, color="#1f1f1f", linewidth=1.0)
    ax.set_title("ITAR Categories Added vs Removed by Year (Official Rule Event Model)", fontsize=14, pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel("Category / tranche count")
    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(year) for year in years], rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.25, linewidth=0.7)
    ax.legend(frameon=False)

    max_abs = max(added_values + [abs(value) for value in removed_values] + [1])
    offset = max(0.12, max_abs * 0.05)
    for bar, value in zip(added_bars, added_values):
        if value == 0:
            continue
        ax.text(bar.get_x() + bar.get_width() / 2, value + offset, f"+{value}", ha="center", va="bottom", fontsize=8)
    for bar, raw_value in zip(removed_bars, removed_values):
        if raw_value == 0:
            continue
        ax.text(bar.get_x() + bar.get_width() / 2, raw_value - offset, f"{raw_value}", ha="center", va="top", fontsize=8)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)

    return ItarScopePlotResult(
        panel_csv=yearly_path,
        output_png=output_path,
        metric="added_removed_by_year",
        row_count=len(rows),
    )


def _write_entries_csv(path: Path, snapshots: Iterable[YearSnapshot]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "year",
                "entry_id",
                "category",
                "entry_text",
                "is_product_entry",
                "exclude_reason",
                "source_url",
            ],
        )
        writer.writeheader()
        for snapshot in snapshots:
            for entry in snapshot.leaf_entries:
                writer.writerow(
                    {
                        "year": entry.year,
                        "entry_id": entry.entry_id,
                        "category": entry.category,
                        "entry_text": entry.entry_text,
                        "is_product_entry": int(entry.is_product_entry),
                        "exclude_reason": entry.exclude_reason,
                        "source_url": entry.source_url,
                    }
                )


def _write_yearly_panel_csv(path: Path, rows: Iterable[YearPanelRow]) -> None:
    fieldnames = [field.name for field in YearPanelRow.__dataclass_fields__.values()]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: getattr(row, field) for field in fieldnames})


def _write_changes_csv(path: Path, changes: Iterable[EntryChange]) -> None:
    fieldnames = [field.name for field in EntryChange.__dataclass_fields__.values()]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for change in changes:
            writer.writerow({field: getattr(change, field) for field in fieldnames})


def _write_ambiguous_rewrites_csv(path: Path, rewrites: Iterable[AmbiguousRewrite]) -> None:
    fieldnames = [field.name for field in AmbiguousRewrite.__dataclass_fields__.values()]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for rewrite in rewrites:
            writer.writerow({field: getattr(rewrite, field) for field in fieldnames})


def _write_summary_files(
    summary_json_path: Path,
    summary_markdown_path: Path,
    snapshots: list[YearSnapshot],
    panel_rows: list[YearPanelRow],
    changes: list[EntryChange],
    ambiguous_rewrites: list[AmbiguousRewrite],
) -> None:
    generated_at = datetime.now(UTC).isoformat()
    payload = {
        "generated_at_utc": generated_at,
        "snapshot_count": len(snapshots),
        "years": [snapshot.year for snapshot in snapshots],
        "panel_rows": [row.__dict__ for row in panel_rows],
        "change_counts": {
            "added": sum(1 for change in changes if change.change_type == "added"),
            "removed": sum(1 for change in changes if change.change_type == "removed"),
            "modified": sum(1 for change in changes if change.change_type == "modified"),
        },
        "ambiguous_rewrite_count": len(ambiguous_rewrites),
    }
    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# ITAR Scope Dataset Summary",
        "",
        f"- generated_at_utc: `{generated_at}`",
        f"- years: `{snapshots[0].year}` to `{snapshots[-1].year}`",
        f"- snapshots: `{len(snapshots)}`",
        f"- total_added_entries: `{payload['change_counts']['added']}`",
        f"- total_removed_entries: `{payload['change_counts']['removed']}`",
        f"- total_modified_entries: `{payload['change_counts']['modified']}`",
        f"- ambiguous_rewrites: `{len(ambiguous_rewrites)}`",
        "",
        "## Sources",
        "",
    ]
    for snapshot in snapshots:
        lines.append(f"- {snapshot.year} annual CFR snapshot: {snapshot.source_url}")
    summary_markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_corrected_scope_year_rows(
    start_year: int,
    end_year: int,
    events: tuple[OfficialScopeEvent, ...],
) -> list[CorrectedScopeYearRow]:
    event_map: dict[int, list[OfficialScopeEvent]] = {}
    for event in events:
        event_map.setdefault(event.year, []).append(event)

    cumulative = 0
    rows: list[CorrectedScopeYearRow] = []
    for year in range(start_year, end_year + 1):
        positive_units = sum(event.positive_units for event in event_map.get(year, []))
        negative_units = sum(event.negative_units for event in event_map.get(year, []))
        net_scope_delta = sum(event.net_scope_delta for event in event_map.get(year, []))
        cumulative += net_scope_delta
        rows.append(
            CorrectedScopeYearRow(
                year=year,
                positive_units=positive_units,
                negative_units=negative_units,
                net_scope_delta=net_scope_delta,
                cumulative_scope_index=cumulative,
            )
        )
    return rows


def _write_corrected_scope_events_csv(path: Path, events: tuple[OfficialScopeEvent, ...]) -> None:
    fieldnames = [field.name for field in OfficialScopeEvent.__dataclass_fields__.values()]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow({field: getattr(event, field) for field in fieldnames})


def _write_corrected_scope_yearly_csv(path: Path, rows: list[CorrectedScopeYearRow]) -> None:
    fieldnames = [field.name for field in CorrectedScopeYearRow.__dataclass_fields__.values()]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: getattr(row, field) for field in fieldnames})


def _write_corrected_scope_summary(
    path: Path,
    yearly_rows: list[CorrectedScopeYearRow],
    events: tuple[OfficialScopeEvent, ...],
) -> None:
    generated_at = datetime.now(UTC).isoformat()
    lines = [
        "# Corrected ITAR Scope Event Model",
        "",
        f"- generated_at_utc: `{generated_at}`",
        "- methodology: `event-weighted category/tranche model across all cited ITAR changes based on official effective dates, not raw hardware-only USML line counts`",
        "- primary_chart: `corrected_added_removed_by_year.png` (annual categories/tranches added above zero and removed below zero)",
        "- audit_companions: `corrected_net_scope_change.png`, `corrected_cumulative_scope_index.png`",
        "",
        "## Official Events",
        "",
    ]
    for event in events:
        lines.append(
            f"- {event.effective_date}: `{event.net_scope_delta:+d}` — {event.title}. "
            f"Source: {event.source_label}: {event.source_url}"
        )
    lines.extend(["", "## Yearly Rows", ""])
    for row in yearly_rows:
        lines.append(
            f"- {row.year}: added_units=`{row.positive_units}`, removed_units=`{row.negative_units}`, "
            f"net_scope_delta=`{row.net_scope_delta:+d}`, cumulative_scope_index_audit=`{row.cumulative_scope_index:+d}`"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_yearly_panel_csv(path: Path) -> list[dict[str, str]]:
    return _read_csv_rows(path)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))

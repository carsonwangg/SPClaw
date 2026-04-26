from __future__ import annotations

from pathlib import Path

from spclaw.hf_document_extract import extract_document


def test_extract_txt_document(tmp_path: Path) -> None:
    file_path = tmp_path / "notes.txt"
    file_path.write_text("alpha\nbeta\n", encoding="utf-8")
    doc = extract_document(
        file_id="F1",
        name="notes.txt",
        mime_type="text/plain",
        local_path=str(file_path),
        source_ts_utc="2026-02-24T00:00:00+00:00",
    )
    assert doc.char_count > 0
    assert "alpha" in doc.extracted_text
    assert not doc.warnings


def test_extract_unsupported_extension(tmp_path: Path) -> None:
    file_path = tmp_path / "blob.bin"
    file_path.write_bytes(b"\x00\x01\x02")
    doc = extract_document(
        file_id="F2",
        name="blob.bin",
        mime_type="application/octet-stream",
        local_path=str(file_path),
        source_ts_utc=None,
    )
    assert doc.extracted_text == ""
    assert any("unsupported_file_type" in warning for warning in doc.warnings)


def test_extract_missing_path() -> None:
    doc = extract_document(
        file_id="F3",
        name="missing.pdf",
        mime_type="application/pdf",
        local_path="/tmp/does-not-exist.pdf",
        source_ts_utc=None,
    )
    assert doc.char_count == 0
    assert "missing_local_path" in doc.warnings


def test_dispatch_pdf_extractor(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "test.pdf"
    file_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        "spclaw.hf_document_extract._extract_pdf",
        lambda path: ("pdf text", 2, []),
    )
    doc = extract_document(
        file_id="F4",
        name="test.pdf",
        mime_type="application/pdf",
        local_path=str(file_path),
        source_ts_utc=None,
    )
    assert doc.extracted_text == "pdf text"
    assert doc.page_count == 2


def test_dispatch_docx_extractor(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "test.docx"
    file_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        "spclaw.hf_document_extract._extract_docx",
        lambda path: ("docx text", 1, []),
    )
    doc = extract_document(
        file_id="F5",
        name="test.docx",
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        local_path=str(file_path),
        source_ts_utc=None,
    )
    assert doc.extracted_text == "docx text"
    assert doc.page_count == 1


def test_dispatch_pptx_extractor(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "test.pptx"
    file_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        "spclaw.hf_document_extract._extract_pptx",
        lambda path: ("pptx text", 5, []),
    )
    doc = extract_document(
        file_id="F6",
        name="test.pptx",
        mime_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        local_path=str(file_path),
        source_ts_utc=None,
    )
    assert doc.extracted_text == "pptx text"
    assert doc.page_count == 5

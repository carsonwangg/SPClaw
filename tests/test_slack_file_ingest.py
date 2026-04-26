from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from spclaw.slack_file_ingest import classify_category, ingest_slack_files


def _write_file_bridge_config(path: Path, *, root: Path) -> None:
    payload = {
        "mode": "local-drive-copy",
        "local": {
            "working": str(root / "local/working"),
            "archive": str(root / "local/archive"),
            "published": str(root / "local/published"),
            "incoming": str(root / "local/incoming"),
        },
        "drive": {
            "root": str(root / "drive"),
            "latest": "02_READ_ONLY_Latest_AUTO",
            "archive": "03_READ_ONLY_Archive_AUTO",
            "incoming": "01_DROP_HERE_Incoming",
            "incoming_reference_from_latest": True,
            "incoming_reference_folder": "_Latest_Reference_READ_ONLY",
        },
        "rclone": {
            "enabled": False,
            "remote_root": "",
            "latest": "Latest",
            "archive": "Archive",
            "incoming": "Incoming",
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_classify_category() -> None:
    assert classify_category(filename="AAPL-10Q.pdf") == "Companies"
    assert classify_category(filename="Q4 earnings transcript.pdf") == "Companies"
    assert classify_category(filename="valuation_model.xlsx") == "Companies"
    assert classify_category(filename="whatever.bin") == "Companies"
    assert classify_category(filename="random.pdf", message_text="category: macro") == "Industries"
    assert classify_category(filename="basket.csv", message_text="please add to universes") == "Universes"


def test_ingest_slack_files_downloads_and_dedupes(tmp_path: Path, monkeypatch) -> None:
    cfg_path = tmp_path / "file-bridge.json"
    _write_file_bridge_config(cfg_path, root=tmp_path)

    db_path = tmp_path / "db/file_ingest.sqlite"
    monkeypatch.setenv("SPCLAW_FILE_BRIDGE_CONFIG", str(cfg_path))
    monkeypatch.setenv("SPCLAW_FILE_INGEST_DB_PATH", str(db_path))

    files = [
        {
            "id": "F123",
            "name": "MSFT-10Q.pdf",
            "title": "MSFT filing",
            "mimetype": "application/pdf",
            "filetype": "pdf",
            "url_private_download": "https://example.test/file.pdf",
        }
    ]

    result = ingest_slack_files(
        files=files,
        channel="C1",
        user_id="U1",
        message_ts="100.1",
        message_text="please add to filings",
        source_event="test",
        token="xoxb-test",
        downloader=lambda _url, _token: b"hello world",
    )
    assert result["processed_count"] == 1
    assert not result["errors"]
    item = result["processed"][0]
    assert item["category"] == "Companies"
    assert Path(item["local_path"]).exists()
    assert Path(item["drive_path"]).exists()

    dup = ingest_slack_files(
        files=files,
        channel="C1",
        user_id="U1",
        message_ts="100.2",
        message_text="duplicate event",
        source_event="test",
        token="xoxb-test",
        downloader=lambda _url, _token: b"hello world",
    )
    assert dup["processed_count"] == 0
    assert any("already_ingested" in entry for entry in dup["skipped"])

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM slack_file_ingest").fetchone()[0]
    assert count == 1

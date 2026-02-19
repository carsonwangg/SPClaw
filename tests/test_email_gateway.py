from __future__ import annotations

import json
from pathlib import Path

from coatue_claw.email_gateway import (
    EmailAttachment,
    EmailCommand,
    EmailGatewayStore,
    _ingest_email_attachments,
    _handle_command,
    parse_email_command,
    run_once,
)


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


def test_parse_email_command() -> None:
    assert parse_email_command("diligence SNOW", "").kind == "diligence"
    assert parse_email_command("dilligence MDB", "").arg == "MDB"
    assert parse_email_command("Testing Dilligence", "Diligence SNOW please").arg == "SNOW"
    assert parse_email_command("Quick request", "can you do diligence on $NOW for me").arg == "NOW"
    assert parse_email_command("", "memory status").kind == "memory_status"
    cmd = parse_email_command("", "memory query daughter's birthday")
    assert cmd.kind == "memory_query"
    assert "birthday" in (cmd.arg or "")
    assert parse_email_command("random", "nonsense").kind == "help"


def test_diligence_email_reply_is_readable_and_attached(tmp_path: Path, monkeypatch) -> None:
    memo = tmp_path / "SNOW-20260219.md"
    memo.write_text(
        "\n".join(
            [
                "# Neutral Investment Memo: Snowflake Inc. (SNOW)",
                "",
                "## 1. Key Takeaways",
                "- Revenue grew 29.2% year over year. [Source: Test dataset]",
                "- Net revenue retention remains above 120%.",
                "",
                "## 6. Key Risks",
                "- Consumption slowdown may pressure near-term growth. [Source: Test dataset]",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("coatue_claw.email_gateway.run_diligence", lambda ticker: memo)
    reply = _handle_command(EmailCommand(kind="diligence", arg="SNOW"))

    assert "Quick Takeaways:" in reply.body_text
    assert "Revenue grew 29.2% year over year." in reply.body_text
    assert "[Source:" not in reply.body_text
    assert reply.body_html is not None
    assert "<ul>" in (reply.body_html or "")
    assert len(reply.attachments) == 1
    assert reply.attachments[0].filename == "SNOW-20260219.md"
    assert reply.attachments[0].content_type == "text/markdown"


def test_run_once_disabled(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("COATUE_CLAW_EMAIL_ENABLED", "false")
    monkeypatch.setenv("COATUE_CLAW_EMAIL_DB_PATH", str(tmp_path / "email.sqlite"))
    result = run_once()
    assert result["ok"] is False
    assert result["reason"] == "email_disabled"


def test_ingest_email_attachments(tmp_path: Path, monkeypatch) -> None:
    cfg_path = tmp_path / "file-bridge.json"
    _write_file_bridge_config(cfg_path, root=tmp_path)
    monkeypatch.setenv("COATUE_CLAW_FILE_BRIDGE_CONFIG", str(cfg_path))

    db_path = tmp_path / "email.sqlite"
    store = EmailGatewayStore(db_path=db_path)
    attachments = [
        EmailAttachment(
            filename="AAPL-10Q.pdf",
            content_type="application/pdf",
            payload=b"test filing content",
        )
    ]
    result = _ingest_email_attachments(
        attachments=attachments,
        body_text="please ingest this filing",
        message_id="<m1@example.com>",
        store=store,
    )
    assert len(result) == 1
    item = result[0]
    assert item["category"] == "Filings"
    assert Path(item["local_path"]).exists()
    assert Path(item["drive_path"]).exists()
    stats = store.stats()
    assert stats["attachments_total"] == 1

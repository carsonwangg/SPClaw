from __future__ import annotations

import json
from pathlib import Path

from coatue_claw.file_bridge import build_index, init_layout, load_config, status, sync_pull, sync_push


def _write_config(path: Path, *, root: Path) -> None:
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
            "latest": "READ_ONLY_Latest_AUTO",
            "archive": "READ_ONLY_Archive_AUTO",
            "incoming": "DROP_HERE_Incoming",
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


def test_init_status_push_pull_index(tmp_path: Path):
    cfg_path = tmp_path / "file-bridge.json"
    _write_config(cfg_path, root=tmp_path)

    config = load_config(cfg_path)

    init = init_layout(config)
    assert init["ok"] is True

    (config.local.published / "reports").mkdir(parents=True, exist_ok=True)
    (config.local.published / "reports/a.md").write_text("alpha", encoding="utf-8")
    (config.local.archive / "old.txt").write_text("archive", encoding="utf-8")

    push = sync_push(config)
    assert push["latest"]["copied"] >= 1
    assert (config.drive.latest / "reports/a.md").exists()
    assert (config.drive.archive / "old.txt").exists()
    assert config.drive.incoming_latest_reference is not None
    assert (config.drive.incoming_latest_reference / "reports/a.md").exists()

    (config.drive.incoming / "new.txt").write_text("incoming", encoding="utf-8")
    assert config.drive.incoming_latest_reference is not None
    (config.drive.incoming_latest_reference / "ignore.txt").write_text("mirror-reference", encoding="utf-8")
    pull = sync_pull(config)
    assert pull["incoming"]["copied"] >= 1
    assert (config.local.incoming / "new.txt").exists()
    assert not (config.local.incoming / "_Latest_Reference_READ_ONLY/ignore.txt").exists()

    idx = build_index(config)
    assert idx["ok"] is True
    assert (config.local.published / "index.json").exists()
    assert (config.local.published / "index.md").exists()

    st = status(config)
    assert st["local"]["published"]["files"] >= 1

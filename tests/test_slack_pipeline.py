from __future__ import annotations

import json
from pathlib import Path

from coatue_claw.slack_pipeline import PipelineResult, PipelineStep, deploy_history, format_pipeline_result


def test_deploy_history_formats_entries(tmp_path: Path, monkeypatch):
    history_path = tmp_path / "deploy-history.json"
    history_payload = [
        {
            "timestamp_utc": "2026-02-18T02:00:00Z",
            "actor": "U123",
            "action": "deploy_latest",
            "before_head": "abc1234",
            "after_head": "def5678",
            "undone_by": None,
        }
    ]
    history_path.write_text(json.dumps(history_payload), encoding="utf-8")
    monkeypatch.setenv("COATUE_CLAW_DEPLOY_HISTORY_PATH", str(history_path))

    text = deploy_history(limit=5)
    assert "deploy_latest" in text
    assert "abc1234->def5678" in text


def test_format_pipeline_result():
    result = PipelineResult(
        action="deploy_latest",
        message="Deploy completed.",
        steps=[
            PipelineStep(label="git", command="git pull --ff-only origin main", returncode=0, stdout="ok", stderr=""),
            PipelineStep(label="make", command="make openclaw-restart", returncode=1, stdout="", stderr="boom"),
        ],
    )
    text = format_pipeline_result(result)
    assert "Deploy completed." in text
    assert "git pull --ff-only origin main" in text
    assert "boom" in text

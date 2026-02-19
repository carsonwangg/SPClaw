from __future__ import annotations

import json
from pathlib import Path

import coatue_claw.slack_pipeline as slack_pipeline
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


def test_run_build_request_prompt_includes_rg_fallback(monkeypatch):
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], *, cwd: Path):
        calls.append(cmd)
        if cmd[:3] == ["/bin/zsh", "-lc", "command -v codex"]:
            return PipelineStep(label="zsh", command="command -v codex", returncode=0, stdout="/usr/local/bin/codex", stderr="")
        if cmd and cmd[0] == "codex":
            return PipelineStep(label="codex", command=" ".join(cmd), returncode=0, stdout="ok", stderr="")
        return PipelineStep(label=cmd[0], command=" ".join(cmd), returncode=0, stdout="", stderr="")

    monkeypatch.setattr(slack_pipeline, "_run", fake_run)
    monkeypatch.setattr(slack_pipeline, "_write_pipeline_checkpoint", lambda **_: None)
    monkeypatch.setenv("COATUE_CLAW_REPO_PATH", "/tmp/repo")

    result = slack_pipeline.run_build_request(request="refine chart filtering", actor="U123")

    assert result.action == "build_request"
    codex_calls = [cmd for cmd in calls if cmd and cmd[0] == "codex"]
    assert codex_calls
    prompt = codex_calls[0][-1]
    assert "If `rg` is unavailable on the runtime host, use `grep -R`" in prompt

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shlex
import subprocess
from typing import Any

from coatue_claw.memory_runtime import MemoryRuntime

class PipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class PipelineStep:
    label: str
    command: str
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class PipelineResult:
    action: str
    message: str
    steps: list[PipelineStep]


def _write_pipeline_checkpoint(
    *,
    action: str,
    actor: str,
    state: dict[str, Any],
    expected_outcome: str,
    files: list[str],
) -> None:
    try:
        memory = MemoryRuntime()
        memory.write_checkpoint(
            scope="pipeline",
            action=action,
            state={"actor": actor, **state},
            expected_outcome=expected_outcome,
            files=files,
            source="slack-pipeline",
            source_ts_utc=_utc_iso(),
        )
    except Exception:
        # Memory checkpointing should never block primary pipeline actions.
        return


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _repo_path() -> Path:
    return Path(os.environ.get("COATUE_CLAW_REPO_PATH", "/opt/coatue-claw"))


def _history_path() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DEPLOY_HISTORY_PATH", str(_data_root() / "db/deploy-history.json")))


def _pipeline_env() -> dict[str, str]:
    env = os.environ.copy()
    path = env.get("PATH", "")
    if "/opt/homebrew/bin" not in path.split(":"):
        env["PATH"] = f"/opt/homebrew/bin:{path}" if path else "/opt/homebrew/bin"
    return env


def _truncate(text: str, limit: int = 800) -> str:
    txt = (text or "").strip()
    if len(txt) <= limit:
        return txt
    return txt[: limit - 3] + "..."


def _run(cmd: list[str], *, cwd: Path) -> PipelineStep:
    process = subprocess.run(cmd, cwd=str(cwd), env=_pipeline_env(), text=True, capture_output=True, check=False)
    return PipelineStep(
        label=cmd[0],
        command=" ".join(shlex.quote(part) for part in cmd),
        returncode=process.returncode,
        stdout=_truncate(process.stdout),
        stderr=_truncate(process.stderr),
    )


def _ensure_ok(step: PipelineStep, *, label: str) -> None:
    if step.returncode != 0:
        detail = step.stderr or step.stdout or f"exit code {step.returncode}"
        raise PipelineError(f"{label} failed: {detail}")


def _load_history() -> list[dict[str, Any]]:
    path = _history_path()
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise PipelineError(f"Invalid deploy history format: {path}")
    return payload


def _save_history(entries: list[dict[str, Any]]) -> None:
    path = _history_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(entries, handle, indent=2)
        handle.write("\n")
    tmp.replace(path)


def _parse_slack_probe_ok(status_json_text: str) -> bool | None:
    if not status_json_text.strip().startswith("{"):
        return None
    try:
        payload = json.loads(status_json_text)
    except json.JSONDecodeError:
        return None
    channels = payload.get("channels") or {}
    slack = channels.get("slack") or {}
    probe = slack.get("probe") or {}
    ok = probe.get("ok")
    return bool(ok) if isinstance(ok, bool) else None


def run_deploy_latest(*, actor: str) -> PipelineResult:
    repo = _repo_path()
    if not repo.exists():
        raise PipelineError(f"Repo path does not exist: {repo}")

    steps: list[PipelineStep] = []

    before = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo)
    _ensure_ok(before, label="git rev-parse before")
    steps.append(before)
    before_head = before.stdout.strip()
    _write_pipeline_checkpoint(
        action="deploy_latest",
        actor=actor,
        state={"before_head": before_head, "repo_path": str(repo)},
        expected_outcome="Fast-forward pull, restart runtime, and healthy Slack probe",
        files=["Makefile", "docs/handoffs/live-session.md", "docs/handoffs/current-plan.md"],
    )

    pull = _run(["git", "pull", "--ff-only", "origin", "main"], cwd=repo)
    _ensure_ok(pull, label="git pull")
    steps.append(pull)

    after = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo)
    _ensure_ok(after, label="git rev-parse after")
    steps.append(after)
    after_head = after.stdout.strip()

    restart = _run(["make", "openclaw-restart"], cwd=repo)
    _ensure_ok(restart, label="make openclaw-restart")
    steps.append(restart)

    status = _run(["make", "openclaw-slack-status"], cwd=repo)
    _ensure_ok(status, label="make openclaw-slack-status")
    steps.append(status)

    probe_ok = _parse_slack_probe_ok(status.stdout)

    history = _load_history()
    history.append(
        {
            "timestamp_utc": _utc_iso(),
            "actor": actor,
            "action": "deploy_latest",
            "before_head": before_head,
            "after_head": after_head,
            "probe_ok": probe_ok,
            "undone_by": None,
            "undone_at_utc": None,
        }
    )
    _save_history(history)

    message = (
        "Deploy completed.\n"
        f"- before: `{before_head}`\n"
        f"- after: `{after_head}`\n"
        f"- slack_probe_ok: `{probe_ok}`"
    )
    return PipelineResult(action="deploy_latest", message=message, steps=steps)


def undo_last_deploy(*, actor: str) -> PipelineResult:
    history = _load_history()
    target_idx = None
    for idx in range(len(history) - 1, -1, -1):
        entry = history[idx]
        if entry.get("action") == "deploy_latest" and not entry.get("undone_by"):
            target_idx = idx
            break

    if target_idx is None:
        raise PipelineError("No deploy entry found to undo.")

    target = history[target_idx]
    target_head = str(target.get("after_head") or "").strip()
    if not target_head:
        raise PipelineError("Deploy history entry missing target commit.")
    _write_pipeline_checkpoint(
        action="undo_last_deploy",
        actor=actor,
        state={"target_head": target_head, "history_index": target_idx},
        expected_outcome="Revert target deploy commit, push to main, restart runtime, and healthy Slack probe",
        files=["docs/handoffs/live-session.md", "docs/handoffs/current-plan.md"],
    )

    repo = _repo_path()
    steps: list[PipelineStep] = []

    pull = _run(["git", "pull", "--ff-only", "origin", "main"], cwd=repo)
    _ensure_ok(pull, label="git pull")
    steps.append(pull)

    revert = _run(["git", "revert", "--no-edit", target_head], cwd=repo)
    _ensure_ok(revert, label=f"git revert {target_head}")
    steps.append(revert)

    push = _run(["git", "push", "origin", "main"], cwd=repo)
    _ensure_ok(push, label="git push")
    steps.append(push)

    new_head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo)
    _ensure_ok(new_head, label="git rev-parse")
    steps.append(new_head)
    revert_head = new_head.stdout.strip()

    restart = _run(["make", "openclaw-restart"], cwd=repo)
    _ensure_ok(restart, label="make openclaw-restart")
    steps.append(restart)

    status = _run(["make", "openclaw-slack-status"], cwd=repo)
    _ensure_ok(status, label="make openclaw-slack-status")
    steps.append(status)

    probe_ok = _parse_slack_probe_ok(status.stdout)

    history[target_idx]["undone_by"] = revert_head
    history[target_idx]["undone_at_utc"] = _utc_iso()
    history[target_idx]["undone_actor"] = actor
    history.append(
        {
            "timestamp_utc": _utc_iso(),
            "actor": actor,
            "action": "undo_last_deploy",
            "target_head": target_head,
            "revert_head": revert_head,
            "probe_ok": probe_ok,
        }
    )
    _save_history(history)

    message = (
        "Undo deploy completed.\n"
        f"- reverted deploy commit: `{target_head}`\n"
        f"- new head: `{revert_head}`\n"
        f"- slack_probe_ok: `{probe_ok}`"
    )
    return PipelineResult(action="undo_last_deploy", message=message, steps=steps)


def run_checks() -> PipelineResult:
    repo = _repo_path()
    step = _run(["/bin/zsh", "-lc", "PYTHONPATH=src pytest -q"], cwd=repo)
    _ensure_ok(step, label="pytest")
    return PipelineResult(
        action="run_checks",
        message="Checks passed (`PYTHONPATH=src pytest -q`).",
        steps=[step],
    )


def run_build_request(*, request: str, actor: str) -> PipelineResult:
    repo = _repo_path()
    custom_cmd = os.environ.get("COATUE_CLAW_SLACK_BUILD_COMMAND", "").strip()
    _write_pipeline_checkpoint(
        action="build_request",
        actor=actor,
        state={"request": request, "repo_path": str(repo)},
        expected_outcome="Implement request, validate, ship to main, and update handoffs",
        files=["docs/handoffs/live-session.md", "docs/handoffs/current-plan.md"],
    )

    if custom_cmd:
        command = custom_cmd.replace("{request}", request)
        step = _run(["/bin/zsh", "-lc", command], cwd=repo)
        _ensure_ok(step, label="custom build command")
        return PipelineResult(
            action="build_request",
            message=f"Build request completed for: `{request}`",
            steps=[step],
        )

    codex_check = _run(["/bin/zsh", "-lc", "command -v codex"], cwd=repo)
    if codex_check.returncode != 0:
        raise PipelineError(
            "Build runner is not configured. Set `COATUE_CLAW_SLACK_BUILD_COMMAND` on the runtime host, "
            "or install Codex CLI and retry."
        )

    prompt = (
        "Read /opt/coatue-claw/AGENTS.md and /opt/coatue-claw/docs/handoffs/live-session.md, then continue from there. "
        "Use /opt/coatue-claw as the active repo. Ship every change to git with handoff updates. "
        f"User request: {request}. Requested by Slack user {actor}."
    )
    step = _run(["codex", "exec", "--cwd", str(repo), prompt], cwd=repo)
    _ensure_ok(step, label="codex exec")

    return PipelineResult(
        action="build_request",
        message=f"Build request completed for: `{request}`",
        steps=[step],
    )


def pipeline_status() -> str:
    repo = _repo_path()
    head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo)
    status = _run(["make", "openclaw-slack-status"], cwd=repo)

    head_value = head.stdout.strip() if head.returncode == 0 else "unknown"
    probe_ok = _parse_slack_probe_ok(status.stdout) if status.returncode == 0 else None

    return (
        "Pipeline status:\n"
        f"- repo_head: `{head_value}`\n"
        f"- slack_probe_ok: `{probe_ok}`\n"
        f"- deploy_history_file: `{_history_path()}`"
    )


def deploy_history(limit: int = 5) -> str:
    entries = _load_history()
    if not entries:
        return "No deploy history yet."

    lines = ["Recent deploy history:"]
    for entry in reversed(entries[-limit:]):
        action = entry.get("action", "unknown")
        ts = entry.get("timestamp_utc", "unknown")
        actor = entry.get("actor", "unknown")
        if action == "deploy_latest":
            before = entry.get("before_head", "?")
            after = entry.get("after_head", "?")
            undone = entry.get("undone_by")
            suffix = f", undone_by={undone}" if undone else ""
            lines.append(f"- {ts} `{action}` by `{actor}` ({before}->{after}{suffix})")
        elif action == "undo_last_deploy":
            target = entry.get("target_head", "?")
            revert_head = entry.get("revert_head", "?")
            lines.append(f"- {ts} `{action}` by `{actor}` (target={target}, revert={revert_head})")
        else:
            lines.append(f"- {ts} `{action}` by `{actor}`")
    return "\n".join(lines)


def format_pipeline_result(result: PipelineResult) -> str:
    lines = [result.message]
    for step in result.steps:
        state = "ok" if step.returncode == 0 else "failed"
        lines.append(f"- {state}: `{step.command}`")
        if step.returncode != 0 and step.stderr:
            lines.append(f"  stderr: `{step.stderr}`")
    return "\n".join(lines)

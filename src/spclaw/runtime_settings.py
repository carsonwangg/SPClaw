from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
from typing import Any

from spclaw.chart_metrics import DEFAULT_X_METRIC, DEFAULT_Y_METRIC, METRIC_SPECS

DEFAULT_FOLLOWUP_PROMPT = (
    "Any adjustments to the stock screen or data you'd like me to double-check?\n"
    "Formatting tweaks too. Reply in-thread with updates like:\n"
    "- `@SPClaw include AVAV,HII`\n"
    "- `@SPClaw exclude GD`"
)


class RuntimeSettingsError(RuntimeError):
    pass


class PromotionError(RuntimeError):
    pass


@dataclass(frozen=True)
class RuntimeSettings:
    default_x_metric: str = DEFAULT_X_METRIC
    default_y_metric: str = DEFAULT_Y_METRIC
    peer_discovery_limit: int = 8
    followup_prompt: str = DEFAULT_FOLLOWUP_PROMPT


@dataclass(frozen=True)
class PromotionResult:
    commit: str
    repo_defaults_path: Path
    restart_ok: bool
    status_ok: bool


@dataclass(frozen=True)
class UndoPromotionResult:
    reverted_target_commit: str
    revert_commit: str
    restart_ok: bool
    status_ok: bool


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_utc_now() -> str:
    return _utc_now().isoformat()


def data_root() -> Path:
    return Path(os.environ.get("SPCLAW_DATA_ROOT", "/opt/spclaw-data"))


def runtime_settings_path() -> Path:
    return Path(os.environ.get("SPCLAW_RUNTIME_SETTINGS_PATH", str(data_root() / "db/runtime-settings.json")))


def runtime_settings_backup_dir() -> Path:
    return Path(os.environ.get("SPCLAW_RUNTIME_SETTINGS_BACKUP_DIR", str(data_root() / "db/runtime-settings-backups")))


def runtime_audit_dir() -> Path:
    return Path(os.environ.get("SPCLAW_CONFIG_AUDIT_DIR", str(data_root() / "artifacts/config-audit")))


def promotion_ledger_path() -> Path:
    return Path(os.environ.get("SPCLAW_PROMOTION_LEDGER_PATH", str(data_root() / "db/settings-promotions.json")))


def repo_path() -> Path:
    return Path(os.environ.get("SPCLAW_REPO_PATH", "/opt/spclaw"))


def repo_defaults_path() -> Path:
    return Path(os.environ.get("SPCLAW_REPO_DEFAULTS_PATH", str(repo_path() / "config/runtime-defaults.json")))


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeSettingsError(f"Expected JSON object at {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    tmp.replace(path)


def _coerce_settings(payload: dict[str, Any]) -> RuntimeSettings:
    x_metric = str(payload.get("default_x_metric", DEFAULT_X_METRIC))
    y_metric = str(payload.get("default_y_metric", DEFAULT_Y_METRIC))
    peer_limit = int(payload.get("peer_discovery_limit", 8))
    followup_prompt = str(payload.get("followup_prompt", DEFAULT_FOLLOWUP_PROMPT)).strip()

    if x_metric not in METRIC_SPECS:
        raise RuntimeSettingsError(f"Unknown default_x_metric: {x_metric}")
    if y_metric not in METRIC_SPECS:
        raise RuntimeSettingsError(f"Unknown default_y_metric: {y_metric}")
    if peer_limit < 2 or peer_limit > 30:
        raise RuntimeSettingsError("peer_discovery_limit must be between 2 and 30")
    if not followup_prompt:
        raise RuntimeSettingsError("followup_prompt cannot be empty")

    return RuntimeSettings(
        default_x_metric=x_metric,
        default_y_metric=y_metric,
        peer_discovery_limit=peer_limit,
        followup_prompt=followup_prompt,
    )


def load_runtime_settings() -> RuntimeSettings:
    runtime_path = runtime_settings_path()
    if runtime_path.exists():
        return _coerce_settings(_read_json(runtime_path))

    defaults_path = repo_defaults_path()
    if defaults_path.exists():
        return _coerce_settings(_read_json(defaults_path))

    return RuntimeSettings()


def _write_audit_markdown(*, action: str, actor: str, before: RuntimeSettings, after: RuntimeSettings, source_text: str | None) -> Path:
    changed_lines: list[str] = []
    before_dict = asdict(before)
    after_dict = asdict(after)
    for key in ("default_x_metric", "default_y_metric", "peer_discovery_limit", "followup_prompt"):
        if before_dict[key] != after_dict[key]:
            changed_lines.append(f"- `{key}`: `{before_dict[key]}` -> `{after_dict[key]}`")

    if not changed_lines:
        changed_lines.append("- no-op")

    ts = _utc_now().strftime("%Y%m%d-%H%M%S")
    out = runtime_audit_dir() / f"{ts}-{action.replace(' ', '-')}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        "\n".join(
            [
                f"# Runtime Settings Change ({action})",
                "",
                f"- timestamp_utc: `{_iso_utc_now()}`",
                f"- actor: `{actor}`",
                "",
                "## Changes",
                *changed_lines,
                "",
                "## Source",
                f"- request: `{(source_text or '').strip()}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return out


def save_runtime_settings(*, settings: RuntimeSettings, actor: str, source_text: str | None = None) -> Path:
    before = load_runtime_settings()

    backup_dir = runtime_settings_backup_dir()
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = backup_dir / f"{_utc_now().strftime('%Y%m%d-%H%M%S')}.json"
    _write_json(backup_file, asdict(before))

    _write_json(runtime_settings_path(), asdict(settings))
    return _write_audit_markdown(action="update", actor=actor, before=before, after=settings, source_text=source_text)


def update_runtime_setting(*, key: str, value: str | int, actor: str, source_text: str | None = None) -> tuple[RuntimeSettings, Path]:
    current = load_runtime_settings()
    if key == "default_x_metric":
        updated = replace(current, default_x_metric=str(value))
    elif key == "default_y_metric":
        updated = replace(current, default_y_metric=str(value))
    elif key == "peer_discovery_limit":
        updated = replace(current, peer_discovery_limit=int(value))
    elif key == "followup_prompt":
        updated = replace(current, followup_prompt=str(value))
    else:
        raise RuntimeSettingsError(f"Unsupported setting: {key}")

    validated = _coerce_settings(asdict(updated))
    audit_path = save_runtime_settings(settings=validated, actor=actor, source_text=source_text)
    return validated, audit_path


def format_settings_summary(settings: RuntimeSettings) -> str:
    return (
        "Here are my current defaults:\n"
        f"- Default x-axis: `{settings.default_x_metric}`\n"
        f"- Default y-axis: `{settings.default_y_metric}`\n"
        f"- Peer discovery target: `{settings.peer_discovery_limit}`\n"
        f"- Post-chart follow-up: `{settings.followup_prompt}`"
    )


def _run_command(cmd: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, check=False)


def _assert_ok(process: subprocess.CompletedProcess[str], *, label: str) -> None:
    if process.returncode != 0:
        stderr = (process.stderr or "").strip()
        stdout = (process.stdout or "").strip()
        detail = stderr or stdout or f"exit_code={process.returncode}"
        raise PromotionError(f"{label} failed: {detail}")


def _load_promotion_ledger() -> list[dict[str, Any]]:
    path = promotion_ledger_path()
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise PromotionError(f"Invalid promotion ledger: {path}")
    return payload


def _save_promotion_ledger(entries: list[dict[str, Any]]) -> None:
    path = promotion_ledger_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(entries, handle, indent=2)
        handle.write("\n")
    tmp.replace(path)


def _run_optional_make(command: str, *, cwd: Path) -> bool:
    process = _run_command(shlex.split(command), cwd=cwd)
    return process.returncode == 0


def promote_current_settings_to_main(*, actor: str) -> PromotionResult:
    settings = load_runtime_settings()
    defaults_path = repo_defaults_path()
    repo_dir = repo_path()
    if not repo_dir.exists():
        raise PromotionError(f"Repo path does not exist: {repo_dir}")

    _write_json(defaults_path, asdict(settings))
    try:
        rel_defaults = str(defaults_path.relative_to(repo_dir))
    except ValueError as exc:
        raise PromotionError(f"Repo defaults path must be inside repo path: {defaults_path}") from exc

    _assert_ok(_run_command(["git", "add", rel_defaults], cwd=repo_dir), label="git add")

    diff = _run_command(["git", "diff", "--cached", "--quiet", "--", rel_defaults], cwd=repo_dir)
    if diff.returncode == 0:
        raise PromotionError("No settings changes to promote.")
    if diff.returncode > 1:
        _assert_ok(diff, label="git diff --cached")

    safe_actor = re.sub(r"[^a-zA-Z0-9._-]", "_", actor)
    commit_msg = f"Promote runtime settings from Slack ({safe_actor})"
    _assert_ok(_run_command(["git", "commit", "-m", commit_msg], cwd=repo_dir), label="git commit")
    _assert_ok(_run_command(["git", "push", "origin", "main"], cwd=repo_dir), label="git push")

    head = _run_command(["git", "rev-parse", "HEAD"], cwd=repo_dir)
    _assert_ok(head, label="git rev-parse")
    commit = (head.stdout or "").strip()

    entries = _load_promotion_ledger()
    entries.append(
        {
            "timestamp_utc": _iso_utc_now(),
            "actor": actor,
            "commit": commit,
            "reverted_by": None,
            "reverted_at_utc": None,
        }
    )
    _save_promotion_ledger(entries)

    restart_ok = _run_optional_make(os.environ.get("SPCLAW_RESTART_COMMAND", "make openclaw-restart"), cwd=repo_dir)
    status_ok = _run_optional_make(os.environ.get("SPCLAW_STATUS_COMMAND", "make openclaw-slack-status"), cwd=repo_dir)

    return PromotionResult(
        commit=commit,
        repo_defaults_path=defaults_path,
        restart_ok=restart_ok,
        status_ok=status_ok,
    )


def undo_last_settings_promotion(*, actor: str) -> UndoPromotionResult:
    entries = _load_promotion_ledger()
    target_index = None
    for idx in range(len(entries) - 1, -1, -1):
        if not entries[idx].get("reverted_by"):
            target_index = idx
            break
    if target_index is None:
        raise PromotionError("No prior settings promotion found to undo.")

    target_commit = str(entries[target_index].get("commit") or "").strip()
    if not target_commit:
        raise PromotionError("Promotion ledger entry missing commit hash.")

    repo_dir = repo_path()
    _assert_ok(_run_command(["git", "revert", "--no-edit", target_commit], cwd=repo_dir), label="git revert")
    _assert_ok(_run_command(["git", "push", "origin", "main"], cwd=repo_dir), label="git push")

    head = _run_command(["git", "rev-parse", "HEAD"], cwd=repo_dir)
    _assert_ok(head, label="git rev-parse")
    revert_commit = (head.stdout or "").strip()

    entries[target_index]["reverted_by"] = revert_commit
    entries[target_index]["reverted_at_utc"] = _iso_utc_now()
    entries[target_index]["reverted_actor"] = actor
    _save_promotion_ledger(entries)

    restart_ok = _run_optional_make(os.environ.get("SPCLAW_RESTART_COMMAND", "make openclaw-restart"), cwd=repo_dir)
    status_ok = _run_optional_make(os.environ.get("SPCLAW_STATUS_COMMAND", "make openclaw-slack-status"), cwd=repo_dir)

    return UndoPromotionResult(
        reverted_target_commit=target_commit,
        revert_commit=revert_commit,
        restart_ok=restart_ok,
        status_ok=status_ok,
    )


def list_promotion_history(limit: int = 5) -> list[dict[str, Any]]:
    entries = _load_promotion_ledger()
    if limit <= 0:
        return []
    return entries[-limit:]

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import subprocess
from typing import Any

from dotenv import load_dotenv

load_dotenv("/opt/coatue-claw/.env.prod")

EMAIL_LABEL = "com.coatueclaw.email-gateway"
MEMORY_PRUNE_LABEL = "com.coatueclaw.memory-prune"
X_CHART_LABEL = "com.coatueclaw.x-chart-daily"
SPENCER_CHANGE_DIGEST_LABEL = "com.coatueclaw.spencer-change-digest"


def _repo_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_REPO_ROOT", "/opt/coatue-claw")).expanduser().resolve()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data")).expanduser().resolve()


def _python_bin() -> str:
    return os.environ.get("COATUE_CLAW_PYTHON_BIN", str(_repo_root() / ".venv/bin/python"))


def _launch_agents_dir() -> Path:
    raw = os.environ.get("COATUE_CLAW_LAUNCHAGENTS_DIR")
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.home() / "Library/LaunchAgents").resolve()


def _runtime_env() -> dict[str, str]:
    repo = _repo_root()
    return {
        "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        "PYTHONPATH": str(repo / "src"),
        "COATUE_CLAW_REPO_ROOT": str(repo),
    }


def _x_chart_hourly_interval_seconds() -> int:
    raw = (os.environ.get("COATUE_CLAW_X_CHART_SCOUT_INTERVAL_SECONDS", "3600") or "3600").strip()
    try:
        seconds = int(raw)
    except Exception:
        seconds = 3600
    return max(300, min(86400, seconds))


def _spencer_digest_schedule() -> list[dict[str, int]]:
    raw = (os.environ.get("COATUE_CLAW_SPENCER_CHANGE_DIGEST_TIME", "18:00") or "").strip()
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if not m:
        return [{"Hour": 18, "Minute": 0}]
    hour = int(m.group(1))
    minute = int(m.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return [{"Hour": 18, "Minute": 0}]
    return [{"Hour": hour, "Minute": minute}]


def _service_specs() -> dict[str, dict[str, Any]]:
    repo = _repo_root()
    data = _data_root()
    python_bin = _python_bin()
    logs_dir = data / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    poller = {
        "Label": EMAIL_LABEL,
        "ProgramArguments": [python_bin, "-m", "coatue_claw.email_gateway", "serve"],
        "WorkingDirectory": str(repo),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 30,
        "ProcessType": "Background",
        "StandardOutPath": str(logs_dir / "email-gateway.stdout.log"),
        "StandardErrorPath": str(logs_dir / "email-gateway.stderr.log"),
        "EnvironmentVariables": _runtime_env(),
    }

    prune_interval = max(300, int(os.environ.get("COATUE_CLAW_MEMORY_PRUNE_INTERVAL_SECONDS", "3600")))
    prune = {
        "Label": MEMORY_PRUNE_LABEL,
        "ProgramArguments": [python_bin, "-m", "coatue_claw.cli", "memory", "prune"],
        "WorkingDirectory": str(repo),
        "RunAtLoad": True,
        "StartInterval": prune_interval,
        "ProcessType": "Background",
        "StandardOutPath": str(logs_dir / "memory-prune.stdout.log"),
        "StandardErrorPath": str(logs_dir / "memory-prune.stderr.log"),
        "EnvironmentVariables": _runtime_env(),
    }

    x_chart = {
        "Label": X_CHART_LABEL,
        "ProgramArguments": [python_bin, "-m", "coatue_claw.x_chart_daily", "run-once"],
        "WorkingDirectory": str(repo),
        "RunAtLoad": True,
        "StartInterval": _x_chart_hourly_interval_seconds(),
        "ProcessType": "Background",
        "StandardOutPath": str(logs_dir / "x-chart-daily.stdout.log"),
        "StandardErrorPath": str(logs_dir / "x-chart-daily.stderr.log"),
        "EnvironmentVariables": _runtime_env(),
    }

    spencer_digest = {
        "Label": SPENCER_CHANGE_DIGEST_LABEL,
        "ProgramArguments": [python_bin, "-m", "coatue_claw.spencer_change_digest", "run-once"],
        "WorkingDirectory": str(repo),
        "RunAtLoad": False,
        "StartCalendarInterval": _spencer_digest_schedule(),
        "ProcessType": "Background",
        "StandardOutPath": str(logs_dir / "spencer-change-digest.stdout.log"),
        "StandardErrorPath": str(logs_dir / "spencer-change-digest.stderr.log"),
        "EnvironmentVariables": _runtime_env(),
    }

    return {
        EMAIL_LABEL: poller,
        MEMORY_PRUNE_LABEL: prune,
        X_CHART_LABEL: x_chart,
        SPENCER_CHANGE_DIGEST_LABEL: spencer_digest,
    }


def _plist_path(label: str) -> Path:
    return _launch_agents_dir() / f"{label}.plist"


def write_service_plists() -> dict[str, str]:
    out: dict[str, str] = {}
    launch_agents = _launch_agents_dir()
    launch_agents.mkdir(parents=True, exist_ok=True)
    for label, spec in _service_specs().items():
        path = _plist_path(label)
        with path.open("wb") as f:
            plistlib.dump(spec, f, sort_keys=True)
        out[label] = str(path)
    return out


def _launchctl_domains() -> list[str]:
    override = os.environ.get("COATUE_CLAW_LAUNCHCTL_DOMAIN", "").strip()
    if override:
        return [override]
    uid = os.getuid()
    return [f"gui/{uid}", f"user/{uid}"]


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}")
    return proc


def _bootout(label: str) -> None:
    for domain in _launchctl_domains():
        _run(["launchctl", "bootout", f"{domain}/{label}"], check=False)


def _bootstrap(path: str) -> str:
    errors: list[str] = []
    for domain in _launchctl_domains():
        proc = _run(["launchctl", "bootstrap", domain, path], check=False)
        if proc.returncode == 0:
            return domain
        err = proc.stderr.strip() or proc.stdout.strip() or f"exit={proc.returncode}"
        errors.append(f"{domain}: {err}")
    raise RuntimeError("bootstrap failed across launchctl domains: " + " | ".join(errors))


def enable_services(*, services: list[str]) -> dict[str, Any]:
    plists = write_service_plists()
    changed: list[dict[str, str]] = []
    for label in services:
        path = plists[label]
        _bootout(label)
        domain = _bootstrap(path)
        changed.append({"label": label, "plist": path, "domain": domain, "action": "enabled"})
    return {"ok": True, "services": changed}


def disable_services(*, services: list[str], remove_plists: bool = False) -> dict[str, Any]:
    changed: list[dict[str, str]] = []
    for label in services:
        _bootout(label)
        path = _plist_path(label)
        if remove_plists and path.exists():
            path.unlink()
        changed.append({"label": label, "plist": str(path), "action": "disabled"})
    return {"ok": True, "services": changed}


def service_status(label: str) -> dict[str, Any]:
    path = _plist_path(label)
    info: dict[str, Any] = {
        "label": label,
        "plist": str(path),
        "plist_exists": path.exists(),
        "loaded": False,
    }
    errors: list[str] = []
    proc: subprocess.CompletedProcess[str] | None = None
    for candidate in _launchctl_domains():
        check = _run(["launchctl", "print", f"{candidate}/{label}"], check=False)
        if check.returncode == 0:
            proc = check
            info["loaded"] = True
            info["domain"] = candidate
            break
        err = check.stderr.strip() or check.stdout.strip() or f"exit={check.returncode}"
        errors.append(f"{candidate}: {err}")
    if proc is None:
        info["error"] = " | ".join(errors) if errors else "not_loaded"
        return info

    text = proc.stdout
    pid_match = re.search(r"\bpid\s*=\s*(\d+)", text)
    if pid_match:
        info["pid"] = int(pid_match.group(1))
    state_match = re.search(r"\bstate\s*=\s*([^\n]+)", text)
    if state_match:
        info["state"] = state_match.group(1).strip()
    last_exit = re.search(r"\blast\s+exit\s+code\s*=\s*(-?\d+)", text)
    if last_exit:
        info["last_exit_code"] = int(last_exit.group(1))
    return info


def status(*, services: list[str]) -> dict[str, Any]:
    return {
        "ok": True,
        "services": [service_status(label) for label in services],
    }


def _resolve_services(raw: str) -> list[str]:
    value = raw.strip().lower()
    if value in {"all", "24x7", "default"}:
        return [EMAIL_LABEL, MEMORY_PRUNE_LABEL, X_CHART_LABEL, SPENCER_CHANGE_DIGEST_LABEL]
    if value == "email":
        return [EMAIL_LABEL]
    if value in {"memory", "memory-prune", "prune"}:
        return [MEMORY_PRUNE_LABEL]
    if value in {"x", "xchart", "chart", "x-chart"}:
        return [X_CHART_LABEL]
    if value in {"spencer", "spencer-digest", "changes"}:
        return [SPENCER_CHANGE_DIGEST_LABEL]
    raise ValueError(f"unknown service selector: {raw}")


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-launchd-runtime")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("enable", "disable", "status"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--service", default="all", choices=["all", "email", "memory", "xchart", "spencer"])  # simplified UX
        if name == "disable":
            cmd.add_argument("--remove-plists", action="store_true")

    args = parser.parse_args()
    services = _resolve_services(args.service)

    if args.command == "enable":
        result = enable_services(services=services)
    elif args.command == "disable":
        result = disable_services(services=services, remove_plists=bool(args.remove_plists))
    else:
        result = status(services=services)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

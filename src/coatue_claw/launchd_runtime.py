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

    return {
        EMAIL_LABEL: poller,
        MEMORY_PRUNE_LABEL: prune,
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


def _launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}")
    return proc


def _bootout(label: str) -> None:
    _run(["launchctl", "bootout", f"{_launchctl_domain()}/{label}"], check=False)


def _bootstrap(path: str) -> None:
    _run(["launchctl", "bootstrap", _launchctl_domain(), path], check=True)


def enable_services(*, services: list[str]) -> dict[str, Any]:
    plists = write_service_plists()
    changed: list[dict[str, str]] = []
    for label in services:
        path = plists[label]
        _bootout(label)
        _bootstrap(path)
        changed.append({"label": label, "plist": path, "action": "enabled"})
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
    proc = _run(["launchctl", "print", f"{_launchctl_domain()}/{label}"], check=False)
    path = _plist_path(label)
    info: dict[str, Any] = {
        "label": label,
        "plist": str(path),
        "plist_exists": path.exists(),
        "loaded": proc.returncode == 0,
    }
    if proc.returncode != 0:
        info["error"] = proc.stderr.strip() or proc.stdout.strip() or "not_loaded"
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
        return [EMAIL_LABEL, MEMORY_PRUNE_LABEL]
    if value == "email":
        return [EMAIL_LABEL]
    if value in {"memory", "memory-prune", "prune"}:
        return [MEMORY_PRUNE_LABEL]
    raise ValueError(f"unknown service selector: {raw}")


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-launchd-runtime")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("enable", "disable", "status"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--service", default="all", choices=["all", "email", "memory"])  # simplified UX
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

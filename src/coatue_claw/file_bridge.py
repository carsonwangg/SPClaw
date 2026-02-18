from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _default_config_path() -> Path:
    return Path(os.environ.get("COATUE_CLAW_FILE_BRIDGE_CONFIG", "/opt/coatue-claw/config/file-bridge.json"))


@dataclass(frozen=True)
class LocalPaths:
    working: Path
    archive: Path
    published: Path
    incoming: Path


@dataclass(frozen=True)
class DrivePaths:
    root: Path
    latest: Path
    archive: Path
    incoming: Path


@dataclass(frozen=True)
class RcloneConfig:
    enabled: bool
    remote_root: str
    latest: str
    archive: str
    incoming: str


@dataclass(frozen=True)
class FileBridgeConfig:
    mode: str
    local: LocalPaths
    drive: DrivePaths
    rclone: RcloneConfig


@dataclass(frozen=True)
class SyncResult:
    copied: int
    skipped: int
    deleted: int


class FileBridgeError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise FileBridgeError(f"Expected JSON object in {path}")
    return payload


def _as_path(value: str | Path) -> Path:
    return Path(str(value)).expanduser().resolve()


def load_config(path: Path | None = None) -> FileBridgeConfig:
    config_path = path or _default_config_path()
    if not config_path.exists():
        raise FileBridgeError(f"File bridge config not found: {config_path}")

    payload = _read_json(config_path)

    local_raw = payload.get("local") or {}
    drive_raw = payload.get("drive") or {}
    rclone_raw = payload.get("rclone") or {}

    local = LocalPaths(
        working=_as_path(local_raw.get("working", "/opt/coatue-claw-data/files/working")),
        archive=_as_path(local_raw.get("archive", "/opt/coatue-claw-data/files/archive")),
        published=_as_path(local_raw.get("published", "/opt/coatue-claw-data/files/published")),
        incoming=_as_path(local_raw.get("incoming", "/opt/coatue-claw-data/files/incoming")),
    )

    drive_root = _as_path(drive_raw.get("root", "/opt/coatue-claw-data/files/drive-share"))
    drive = DrivePaths(
        root=drive_root,
        latest=drive_root / str(drive_raw.get("latest", "Latest")),
        archive=drive_root / str(drive_raw.get("archive", "Archive")),
        incoming=drive_root / str(drive_raw.get("incoming", "Incoming")),
    )

    rclone = RcloneConfig(
        enabled=bool(rclone_raw.get("enabled", False)),
        remote_root=str(rclone_raw.get("remote_root", "")).strip(),
        latest=str(rclone_raw.get("latest", "Latest")).strip(),
        archive=str(rclone_raw.get("archive", "Archive")).strip(),
        incoming=str(rclone_raw.get("incoming", "Incoming")).strip(),
    )

    mode = str(payload.get("mode", "local-drive-copy")).strip() or "local-drive-copy"

    return FileBridgeConfig(mode=mode, local=local, drive=drive, rclone=rclone)


def _file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _same_file(src: Path, dst: Path) -> bool:
    if not dst.exists() or not dst.is_file():
        return False
    src_stat = src.stat()
    dst_stat = dst.stat()
    if src_stat.st_size != dst_stat.st_size:
        return False
    if int(src_stat.st_mtime) == int(dst_stat.st_mtime):
        return True
    return _file_hash(src) == _file_hash(dst)


def _sync_dir(src_root: Path, dst_root: Path, *, delete: bool = False) -> SyncResult:
    copied = 0
    skipped = 0
    deleted = 0

    src_root.mkdir(parents=True, exist_ok=True)
    dst_root.mkdir(parents=True, exist_ok=True)

    src_files: set[Path] = set()
    for src in src_root.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(src_root)
        src_files.add(rel)
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if _same_file(src, dst):
            skipped += 1
            continue
        shutil.copy2(src, dst)
        copied += 1

    if delete:
        for dst in dst_root.rglob("*"):
            if not dst.is_file():
                continue
            rel = dst.relative_to(dst_root)
            if rel not in src_files:
                dst.unlink()
                deleted += 1

    return SyncResult(copied=copied, skipped=skipped, deleted=deleted)


def _run_rclone(args: list[str]) -> dict[str, Any]:
    process = subprocess.run(args, text=True, capture_output=True, check=False)
    return {
        "command": " ".join(args),
        "returncode": process.returncode,
        "stdout": (process.stdout or "").strip(),
        "stderr": (process.stderr or "").strip(),
    }


def init_layout(config: FileBridgeConfig) -> dict[str, Any]:
    paths = [
        config.local.working,
        config.local.archive,
        config.local.published,
        config.local.incoming,
        config.drive.root,
        config.drive.latest,
        config.drive.archive,
        config.drive.incoming,
    ]
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "ok": True,
        "created": [str(path) for path in paths],
        "timestamp_utc": _now_utc_iso(),
    }


def sync_push(config: FileBridgeConfig, *, delete: bool = False) -> dict[str, Any]:
    if config.rclone.enabled and config.rclone.remote_root:
        latest_remote = f"{config.rclone.remote_root.rstrip('/')}/{config.rclone.latest}"
        archive_remote = f"{config.rclone.remote_root.rstrip('/')}/{config.rclone.archive}"
        latest_cmd = ["rclone", "sync" if delete else "copy", str(config.local.published), latest_remote]
        archive_cmd = ["rclone", "sync" if delete else "copy", str(config.local.archive), archive_remote]
        return {
            "mode": "rclone",
            "latest": _run_rclone(latest_cmd),
            "archive": _run_rclone(archive_cmd),
            "timestamp_utc": _now_utc_iso(),
        }

    latest_result = _sync_dir(config.local.published, config.drive.latest, delete=delete)
    archive_result = _sync_dir(config.local.archive, config.drive.archive, delete=delete)
    return {
        "mode": "local-drive-copy",
        "latest": latest_result.__dict__,
        "archive": archive_result.__dict__,
        "timestamp_utc": _now_utc_iso(),
    }


def sync_pull(config: FileBridgeConfig, *, delete: bool = False) -> dict[str, Any]:
    if config.rclone.enabled and config.rclone.remote_root:
        incoming_remote = f"{config.rclone.remote_root.rstrip('/')}/{config.rclone.incoming}"
        incoming_cmd = ["rclone", "sync" if delete else "copy", incoming_remote, str(config.local.incoming)]
        return {
            "mode": "rclone",
            "incoming": _run_rclone(incoming_cmd),
            "timestamp_utc": _now_utc_iso(),
        }

    incoming_result = _sync_dir(config.drive.incoming, config.local.incoming, delete=delete)
    return {
        "mode": "local-drive-copy",
        "incoming": incoming_result.__dict__,
        "timestamp_utc": _now_utc_iso(),
    }


def _build_index_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        rows.append(
            {
                "relative_path": str(path.relative_to(root)),
                "size_bytes": stat.st_size,
                "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
            }
        )
    return rows


def build_index(config: FileBridgeConfig) -> dict[str, Any]:
    config.local.published.mkdir(parents=True, exist_ok=True)
    archive_rows = _build_index_rows(config.local.archive)
    published_rows = _build_index_rows(config.local.published)

    payload = {
        "generated_at_utc": _now_utc_iso(),
        "published_root": str(config.local.published),
        "archive_root": str(config.local.archive),
        "published_files": published_rows,
        "archive_files": archive_rows,
    }

    index_json = config.local.published / "index.json"
    index_md = config.local.published / "index.md"

    with index_json.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    lines = [
        "# Coatue Claw Published Files Index",
        "",
        f"- generated_at_utc: `{payload['generated_at_utc']}`",
        f"- published_root: `{payload['published_root']}`",
        f"- archive_root: `{payload['archive_root']}`",
        "",
        "## Published Files",
        "",
        "| Path | Size (bytes) | Modified (UTC) |",
        "|---|---:|---|",
    ]

    for row in published_rows:
        lines.append(f"| {row['relative_path']} | {row['size_bytes']} | {row['modified_utc']} |")

    if not published_rows:
        lines.append("| _none_ | 0 | n/a |")

    lines.extend([
        "",
        "## Archive Files",
        "",
        "| Path | Size (bytes) | Modified (UTC) |",
        "|---|---:|---|",
    ])

    for row in archive_rows:
        lines.append(f"| {row['relative_path']} | {row['size_bytes']} | {row['modified_utc']} |")

    if not archive_rows:
        lines.append("| _none_ | 0 | n/a |")

    lines.append("")

    index_md.write_text("\n".join(lines), encoding="utf-8")

    return {
        "ok": True,
        "index_json": str(index_json),
        "index_md": str(index_md),
        "published_count": len(published_rows),
        "archive_count": len(archive_rows),
        "timestamp_utc": _now_utc_iso(),
    }


def status(config: FileBridgeConfig) -> dict[str, Any]:
    def _count_files(path: Path) -> int:
        if not path.exists():
            return 0
        return sum(1 for p in path.rglob("*") if p.is_file())

    return {
        "mode": config.mode,
        "timestamp_utc": _now_utc_iso(),
        "local": {
            "working": {"path": str(config.local.working), "exists": config.local.working.exists(), "files": _count_files(config.local.working)},
            "archive": {"path": str(config.local.archive), "exists": config.local.archive.exists(), "files": _count_files(config.local.archive)},
            "published": {"path": str(config.local.published), "exists": config.local.published.exists(), "files": _count_files(config.local.published)},
            "incoming": {"path": str(config.local.incoming), "exists": config.local.incoming.exists(), "files": _count_files(config.local.incoming)},
        },
        "drive": {
            "root": {"path": str(config.drive.root), "exists": config.drive.root.exists()},
            "latest": {"path": str(config.drive.latest), "exists": config.drive.latest.exists(), "files": _count_files(config.drive.latest)},
            "archive": {"path": str(config.drive.archive), "exists": config.drive.archive.exists(), "files": _count_files(config.drive.archive)},
            "incoming": {"path": str(config.drive.incoming), "exists": config.drive.incoming.exists(), "files": _count_files(config.drive.incoming)},
        },
        "rclone": {
            "enabled": config.rclone.enabled,
            "remote_root": config.rclone.remote_root,
        },
    }


def run_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser("coatue-claw-file-bridge")
    parser.add_argument("--config", default=None, help="Path to file bridge JSON config")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-layout")
    sub.add_parser("status")
    sub.add_parser("index")

    push = sub.add_parser("push")
    push.add_argument("--delete", action="store_true", help="Delete files at destination missing from source")

    pull = sub.add_parser("pull")
    pull.add_argument("--delete", action="store_true", help="Delete files at destination missing from source")

    sync = sub.add_parser("sync")
    sync.add_argument("--delete-push", action="store_true")
    sync.add_argument("--delete-pull", action="store_true")

    args = parser.parse_args(argv)
    config = load_config(Path(args.config) if args.config else None)

    if args.cmd == "init-layout":
        print(json.dumps(init_layout(config), indent=2, sort_keys=True))
        return 0
    if args.cmd == "status":
        print(json.dumps(status(config), indent=2, sort_keys=True))
        return 0
    if args.cmd == "index":
        print(json.dumps(build_index(config), indent=2, sort_keys=True))
        return 0
    if args.cmd == "push":
        print(json.dumps(sync_push(config, delete=bool(args.delete)), indent=2, sort_keys=True))
        return 0
    if args.cmd == "pull":
        print(json.dumps(sync_pull(config, delete=bool(args.delete)), indent=2, sort_keys=True))
        return 0
    if args.cmd == "sync":
        result = {
            "pull": sync_pull(config, delete=bool(args.delete_pull)),
            "push": sync_push(config, delete=bool(args.delete_push)),
            "index": build_index(config),
            "timestamp_utc": _now_utc_iso(),
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    raise FileBridgeError(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(run_cli())

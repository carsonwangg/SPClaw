from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv("/opt/coatue-claw/.env.prod")

DEFAULT_TZ = "America/Los_Angeles"
DEFAULT_PORTCOS: list[tuple[str, str]] = [
    ("Anduril", "anduril"),
    ("Anthropic", "anthropic"),
    ("Cursor", "cursor"),
    ("Neuralink", "neuralink"),
    ("OpenAI", "openai"),
    ("Physical Intelligence", "physical-intelligence"),
    ("Ramp", "ramp"),
    ("SpaceX", "spacex"),
    ("Stripe", "stripe"),
    ("Sunday Robotics", "sunday-robotics"),
]

BOARD_SEAT_FORMAT_VERSION = "v0_reset_scaffold"
RESET_REASON = "feature_reset_in_progress"


@dataclass(frozen=True)
class SeedTargetResult:
    inserted: bool
    company: str
    target: str
    target_key: str
    posted_at_utc: str


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _timezone() -> ZoneInfo:
    name = (os.environ.get("COATUE_CLAW_BOARD_SEAT_TZ", DEFAULT_TZ) or "").strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)


def _today_key() -> str:
    return datetime.now(_timezone()).strftime("%Y-%m-%d")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_BOARD_SEAT_DB_PATH",
            str(_data_root() / "db/board_seat_daily.sqlite"),
        )
    )


def _fallback_db_path() -> Path:
    return Path.home() / ".coatue-claw-data" / "db" / "board_seat_daily.sqlite"


def _reset_mode_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_RESET_MODE", True)


def _board_seat_enabled() -> bool:
    return _env_flag("COATUE_CLAW_BOARD_SEAT_ENABLED", False)


def _slug_company(company: str) -> str:
    return re.sub(r"[^a-z0-9]", "", company.lower())


def _target_key(target: str) -> str:
    return re.sub(r"[^a-z0-9]", "", target.lower())


def _parse_portcos(raw: str | None = None) -> list[tuple[str, str]]:
    data = raw if raw is not None else os.environ.get("COATUE_CLAW_BOARD_SEAT_PORTCOS", "")
    if not data:
        return list(DEFAULT_PORTCOS)
    parsed: list[tuple[str, str]] = []
    for chunk in data.split(","):
        item = chunk.strip()
        if not item:
            continue
        if ":" in item:
            company, channel = item.split(":", 1)
            company = company.strip()
            channel = channel.strip().lstrip("#")
            if company and channel:
                parsed.append((company, channel))
            continue
        channel = item.lstrip("#")
        parsed.append((item, channel))
    return parsed or list(DEFAULT_PORTCOS)


class BoardSeatStore:
    def __init__(self, path: Path | None = None) -> None:
        chosen_path = path or _db_path()
        try:
            chosen_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            chosen_path = _fallback_db_path()
            chosen_path.parent.mkdir(parents=True, exist_ok=True)
        self.path = chosen_path
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_seat_target_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company TEXT NOT NULL,
                    target TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    channel_ref TEXT,
                    source TEXT,
                    posted_at_utc TEXT,
                    run_date_local TEXT,
                    message_ts TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_board_seat_target_memory_company
                ON board_seat_target_memory(company)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_board_seat_target_memory_target_key
                ON board_seat_target_memory(target_key)
                """
            )

    def record_target(
        self,
        *,
        company: str,
        target: str,
        channel_ref: str,
        channel_id: str | None,
        source: str,
        posted_at_utc: str,
        run_date_local: str,
        message_ts: str | None,
    ) -> bool:
        company_text = str(company or "").strip()
        target_text = str(target or "").strip()
        if not company_text or not target_text:
            return False
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO board_seat_target_memory (
                    company,
                    target,
                    target_key,
                    channel_ref,
                    source,
                    posted_at_utc,
                    run_date_local,
                    message_ts,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company_text,
                    target_text,
                    _target_key(target_text),
                    str(channel_ref or "").strip(),
                    str(source or "manual").strip(),
                    str(posted_at_utc or "").strip(),
                    str(run_date_local or _today_key()).strip(),
                    str(message_ts or "").strip(),
                    _utc_now_iso(),
                ),
            )
        return True

    def target_ledger_rows(self, *, company: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        query = """
            SELECT company, target, target_key, channel_ref, source, posted_at_utc, run_date_local, message_ts, created_at_utc
            FROM board_seat_target_memory
        """
        params: list[Any] = []
        if company:
            query += " WHERE lower(company) = ?"
            params.append(str(company).strip().lower())
        query += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(5000, int(limit))))
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def target_memory_count(self, *, company: str | None = None) -> int:
        with self._connect() as conn:
            if company:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM board_seat_target_memory WHERE lower(company)=?",
                    (str(company).strip().lower(),),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM board_seat_target_memory").fetchone()
        return int(row["c"]) if row is not None else 0


def run_once(*, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    del force
    portcos = _parse_portcos()
    result: dict[str, Any] = {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "run_date_local": _today_key(),
        "timezone": str(_timezone()),
        "board_seat_enabled": _board_seat_enabled(),
        "reset_mode": _reset_mode_enabled(),
        "sent": [],
        "skipped": [],
        "portcos": [{"company": company, "channel_ref": channel_ref} for company, channel_ref in portcos],
    }

    for company, channel_ref in portcos:
        if _reset_mode_enabled():
            result["skipped"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "reason": RESET_REASON,
                    "detail": "Board Seat has been intentionally reset and is disabled while we rebuild from scratch.",
                    "delivery_mode_applied": "skip",
                }
            )
            continue
        if not _board_seat_enabled():
            result["skipped"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "reason": "board_seat_disabled",
                    "detail": "Enable COATUE_CLAW_BOARD_SEAT_ENABLED=1 after implementing the new pipeline.",
                    "delivery_mode_applied": "skip",
                }
            )
            continue
        preview = (
            f"Board Seat as a Service — {company}\n"
            "Status: Rebuild mode active\n"
            "No production draft is generated in scaffold mode."
        )
        if dry_run:
            result["sent"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "preview": preview,
                    "delivery_mode_applied": "dry_run_preview",
                }
            )
        else:
            result["skipped"].append(
                {
                    "company": company,
                    "channel_ref": channel_ref,
                    "reason": "scaffold_mode_no_live_post",
                    "detail": "Scaffold mode only supports dry-run preview until v1 rebuild ships.",
                    "delivery_mode_applied": "skip",
                }
            )
    return result


def status() -> dict[str, Any]:
    store = BoardSeatStore()
    portcos = _parse_portcos()
    return {
        "ok": True,
        "format_version": BOARD_SEAT_FORMAT_VERSION,
        "run_date_local": _today_key(),
        "timezone": str(_timezone()),
        "reset_mode": _reset_mode_enabled(),
        "board_seat_enabled": _board_seat_enabled(),
        "status": "reset_scaffold",
        "next_step": "Implement new candidate-first writer pipeline before re-enabling live posts.",
        "portcos": [{"company": company, "channel_ref": channel_ref} for company, channel_ref in portcos],
        "target_memory_counts": {
            "total": store.target_memory_count(),
            "by_company": {company: store.target_memory_count(company=company) for company, _ in portcos},
        },
        "hard_gates": ["company_only_target", "cooldown_repeat_block"],
    }


def _refresh_funding_payload(*, entities: list[str], report: bool) -> dict[str, Any]:
    action = "funding-quality-report" if report else "refresh-funding"
    return {
        "ok": True,
        "status": "not_implemented",
        "action": action,
        "entities": entities,
        "detail": "Funding pipeline is intentionally removed in reset scaffold and will be rebuilt.",
    }


def _funding_entities(all_portcos: bool, company: str) -> list[str]:
    if all_portcos:
        return [item[0] for item in _parse_portcos()]
    clean_company = str(company or "").strip()
    return [clean_company] if clean_company else []


def _seed_target(*, company: str, target: str, channel_ref: str) -> SeedTargetResult:
    store = BoardSeatStore()
    now_iso = _utc_now_iso()
    clean_company = str(company or "").strip()
    clean_target = str(target or "").strip()
    inserted = store.record_target(
        company=clean_company,
        target=clean_target,
        channel_ref=str(channel_ref or "manual").strip() or "manual",
        channel_id=None,
        source="manual_seed",
        posted_at_utc=now_iso,
        run_date_local=_today_key(),
        message_ts=None,
    )
    return SeedTargetResult(
        inserted=inserted,
        company=clean_company,
        target=clean_target,
        target_key=_target_key(clean_target),
        posted_at_utc=now_iso,
    )


def main() -> None:
    parser = argparse.ArgumentParser("coatue-claw-board-seat-daily")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run-once")
    run.add_argument("--force", action="store_true")
    run.add_argument("--dry-run", action="store_true")

    sub.add_parser("status")

    seed = sub.add_parser("seed-target")
    seed.add_argument("--company", required=True)
    seed.add_argument("--target", required=True)
    seed.add_argument("--channel-ref", default="manual")

    export = sub.add_parser("export-ledger")
    export.add_argument("--company", default="")

    memory = sub.add_parser("target-memory")
    memory.add_argument("--company", default="")
    memory.add_argument("--limit", type=int, default=200)

    refresh = sub.add_parser("refresh-funding")
    refresh.add_argument("--all-portcos", action="store_true")
    refresh.add_argument("--company", default="")
    refresh.add_argument("--include-recent-targets", action="store_true")

    report = sub.add_parser("funding-quality-report")
    report.add_argument("--all-portcos", action="store_true")
    report.add_argument("--company", default="")
    report.add_argument("--include-recent-targets", action="store_true")

    args = parser.parse_args()

    if args.command == "run-once":
        payload = run_once(force=bool(args.force), dry_run=bool(args.dry_run))
    elif args.command == "status":
        payload = status()
    elif args.command == "seed-target":
        seeded = _seed_target(company=args.company, target=args.target, channel_ref=args.channel_ref)
        payload = {
            "ok": True,
            "inserted": seeded.inserted,
            "company": seeded.company,
            "target": seeded.target,
            "target_key": seeded.target_key,
            "posted_at_utc": seeded.posted_at_utc,
        }
    elif args.command == "export-ledger":
        store = BoardSeatStore()
        rows = store.target_ledger_rows(company=(args.company or None), limit=5000)
        payload = {
            "ok": True,
            "status": "reset_scaffold",
            "rows": rows,
            "count": len(rows),
            "company_filter": args.company or "",
        }
    elif args.command == "target-memory":
        store = BoardSeatStore()
        rows = store.target_ledger_rows(company=(args.company or None), limit=max(1, min(5000, int(args.limit))))
        payload = {
            "ok": True,
            "target_lock_days": int(os.environ.get("COATUE_CLAW_BOARD_SEAT_TARGET_LOCK_DAYS", "14") or "14"),
            "status": "reset_scaffold",
            "company_filter": args.company or "",
            "count": len(rows),
            "rows": rows,
        }
    elif args.command == "refresh-funding":
        entities = _funding_entities(bool(args.all_portcos), str(args.company or ""))
        payload = _refresh_funding_payload(entities=entities, report=False)
    elif args.command == "funding-quality-report":
        entities = _funding_entities(bool(args.all_portcos), str(args.company or ""))
        payload = _refresh_funding_payload(entities=entities, report=True)
    else:
        payload = {"ok": False, "error": "unknown_command"}

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

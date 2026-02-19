from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import logging
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable
from urllib.request import Request, urlopen

from coatue_claw.file_bridge import FileBridgeError, load_config

logger = logging.getLogger(__name__)

KNOWN_CATEGORIES = (
    "Universes",
    "Companies",
    "Industries",
)

_CATEGORY_BY_KEYWORD: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Universes", ("universe", "watchlist", "basket", "coverage list", "screen", "constituent")),
    (
        "Companies",
        (
            "transcript",
            "call transcript",
            "earnings call transcript",
            "10-k",
            "10k",
            "10-q",
            "10q",
            "8-k",
            "8k",
            "s-1",
            "sec filing",
            "form ",
            "earnings",
            "quarterly results",
            "q1",
            "q2",
            "q3",
            "q4",
            "deck",
            "presentation",
            "investor presentation",
            "pitch",
            "model",
            "valuation",
            "forecast",
            "assumption",
            "scenario",
            "call notes",
            "investor call",
            "management call",
            "analyst call",
            "memo",
            "notes",
            "summary",
            "meeting notes",
            "company",
            "ticker",
            "profile",
            "nda",
            "invoice",
            "agreement",
            "contract",
        ),
    ),
    ("Industries", ("sector", "industry", "theme", "narrative", "trend", "thesis", "macro", "cpi", "inflation", "fed", "fomc", "gdp", "rates")),
)

_CATEGORY_BY_EXTENSION: dict[str, str] = {
    ".xlsx": "Companies",
    ".xls": "Companies",
    ".csv": "Companies",
    ".pptx": "Companies",
    ".ppt": "Companies",
    ".key": "Companies",
    ".docx": "Companies",
    ".doc": "Companies",
    ".md": "Companies",
    ".txt": "Companies",
}

_CATEGORY_ALIASES: dict[str, str] = {
    "universe": "Universes",
    "universes": "Universes",
    "watchlist": "Universes",
    "basket": "Universes",
    "company": "Companies",
    "companies": "Companies",
    "ticker": "Companies",
    "earnings": "Companies",
    "filings": "Companies",
    "filing": "Companies",
    "transcripts": "Companies",
    "transcript": "Companies",
    "decks": "Companies",
    "deck": "Companies",
    "models": "Companies",
    "model": "Companies",
    "notes": "Companies",
    "calls": "Companies",
    "call": "Companies",
    "macro": "Industries",
    "theme": "Industries",
    "themes": "Industries",
    "sector": "Industries",
    "sectors": "Industries",
    "industry": "Industries",
    "industries": "Industries",
}


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _default_db_path() -> Path:
    data_root = Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))
    return Path(os.environ.get("COATUE_CLAW_FILE_INGEST_DB_PATH", str(data_root / "db/file_ingest.sqlite")))


def _sanitize_filename(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._ -]+", "-", name).strip()
    safe = re.sub(r"\s+", " ", safe)
    if not safe:
        return "upload.bin"
    return safe


def _pick_unique_path(root: Path, filename: str, file_id: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    candidate = root / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for i in range(1, 1000):
        if i == 1:
            alt = root / f"{stem}--{file_id[:8]}{suffix}"
        else:
            alt = root / f"{stem}--{file_id[:8]}-{i}{suffix}"
        if not alt.exists():
            return alt
    return root / f"{stem}--{file_id[:8]}--{int(datetime.now(UTC).timestamp())}{suffix}"


def _download_slack_file(url: str, token: str) -> bytes:
    req = Request(url, headers={"Authorization": f"Bearer {token}"})
    with urlopen(req, timeout=60) as resp:
        return resp.read()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _normalize_category(name: str) -> str | None:
    lowered = name.strip().lower()
    for cat in KNOWN_CATEGORIES:
        if cat.lower() == lowered:
            return cat
    alias = _CATEGORY_ALIASES.get(lowered)
    if alias:
        return alias
    return None


def _explicit_category_from_text(text: str | None) -> str | None:
    if not text:
        return None
    lowered = text.lower()
    by_label = re.search(r"category\s*[:=]\s*([a-z][a-z_-]*)", lowered)
    if by_label:
        norm = _normalize_category(by_label.group(1))
        if norm:
            return norm
    by_phrase = re.search(r"\b(?:to|in|under)\s+([a-z][a-z_-]*)\b", lowered)
    if by_phrase:
        norm = _normalize_category(by_phrase.group(1))
        if norm:
            return norm
    return None


def classify_category(
    *,
    filename: str,
    title: str | None = None,
    message_text: str | None = None,
    mimetype: str | None = None,
    filetype: str | None = None,
) -> str:
    explicit = _explicit_category_from_text(message_text)
    if explicit:
        return explicit

    blob = " ".join(part for part in [filename, title or "", message_text or "", mimetype or "", filetype or ""]).lower()
    for category, keywords in _CATEGORY_BY_KEYWORD:
        if any(keyword in blob for keyword in keywords):
            return category

    ext = Path(filename).suffix.lower()
    mapped = _CATEGORY_BY_EXTENSION.get(ext)
    if mapped:
        return mapped

    return "Companies"


@dataclass(frozen=True)
class IngestedFile:
    file_id: str
    original_name: str
    category: str
    local_path: str
    drive_path: str
    size_bytes: int
    sha256: str


class FileIngestStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _default_db_path()).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS slack_file_ingest (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slack_file_id TEXT NOT NULL UNIQUE,
                    slack_channel TEXT,
                    slack_user_id TEXT,
                    slack_message_ts TEXT,
                    source_event TEXT NOT NULL,
                    original_name TEXT NOT NULL,
                    title TEXT,
                    mimetype TEXT,
                    filetype TEXT,
                    category TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    drive_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    source_text TEXT,
                    ingested_at_utc TEXT NOT NULL
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_slack_file_ingest_when ON slack_file_ingest(ingested_at_utc DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_slack_file_ingest_category ON slack_file_ingest(category);")

    def is_ingested(self, slack_file_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM slack_file_ingest WHERE slack_file_id = ? LIMIT 1",
                (slack_file_id,),
            ).fetchone()
        return row is not None

    def insert(
        self,
        *,
        file_id: str,
        channel: str | None,
        user_id: str | None,
        message_ts: str | None,
        source_event: str,
        original_name: str,
        title: str | None,
        mimetype: str | None,
        filetype: str | None,
        category: str,
        local_path: str,
        drive_path: str,
        size_bytes: int,
        sha256: str,
        source_text: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO slack_file_ingest (
                    slack_file_id, slack_channel, slack_user_id, slack_message_ts, source_event,
                    original_name, title, mimetype, filetype, category, local_path, drive_path,
                    size_bytes, sha256, source_text, ingested_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    channel,
                    user_id,
                    message_ts,
                    source_event,
                    original_name,
                    title,
                    mimetype,
                    filetype,
                    category,
                    local_path,
                    drive_path,
                    size_bytes,
                    sha256,
                    source_text,
                    _now_utc_iso(),
                ),
            )


def ingest_slack_files(
    *,
    files: list[dict[str, Any]],
    channel: str | None,
    user_id: str | None,
    message_ts: str | None,
    message_text: str | None,
    source_event: str,
    token: str | None = None,
    downloader: Callable[[str, str], bytes] | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    if not files:
        return {"processed_count": 0, "processed": [], "skipped": [], "errors": []}

    bot_token = (token or os.environ.get("SLACK_BOT_TOKEN", "")).strip()
    if not bot_token:
        return {
            "processed_count": 0,
            "processed": [],
            "skipped": [],
            "errors": ["missing_slack_bot_token"],
        }

    try:
        bridge = load_config()
    except FileBridgeError as exc:
        return {
            "processed_count": 0,
            "processed": [],
            "skipped": [],
            "errors": [f"file_bridge_config_error: {exc}"],
        }

    max_mb = int(os.environ.get("COATUE_CLAW_SLACK_FILE_MAX_MB", "50"))
    max_bytes = max_mb * 1024 * 1024

    store = FileIngestStore(db_path=db_path)
    fetch = downloader or _download_slack_file
    processed: list[IngestedFile] = []
    skipped: list[str] = []
    errors: list[str] = []

    for file_obj in files:
        file_id = str(file_obj.get("id") or "").strip()
        if not file_id:
            skipped.append("missing_file_id")
            continue
        if store.is_ingested(file_id):
            skipped.append(f"{file_id}:already_ingested")
            continue

        mode = str(file_obj.get("mode") or "").lower()
        if mode == "external":
            skipped.append(f"{file_id}:external_mode_not_supported")
            continue

        url = str(file_obj.get("url_private_download") or file_obj.get("url_private") or "").strip()
        if not url:
            skipped.append(f"{file_id}:missing_download_url")
            continue

        original_name = str(file_obj.get("name") or file_obj.get("title") or f"{file_id}.bin")
        title = str(file_obj.get("title") or "") or None
        safe_name = _sanitize_filename(original_name)
        category = classify_category(
            filename=original_name,
            title=title,
            message_text=message_text,
            mimetype=str(file_obj.get("mimetype") or ""),
            filetype=str(file_obj.get("filetype") or ""),
        )

        try:
            data = fetch(url, bot_token)
        except Exception as exc:
            logger.exception("Slack file download failed file_id=%s", file_id)
            errors.append(f"{file_id}:download_failed:{exc}")
            continue

        if len(data) > max_bytes:
            skipped.append(f"{file_id}:too_large>{max_mb}mb")
            continue

        local_dir = bridge.local.incoming / category
        local_path = _pick_unique_path(local_dir, safe_name, file_id)
        local_path.write_bytes(data)

        drive_dir = bridge.drive.incoming / category
        drive_path = _pick_unique_path(drive_dir, local_path.name, file_id)
        drive_path.write_bytes(data)

        digest = _sha256(data)

        store.insert(
            file_id=file_id,
            channel=channel,
            user_id=user_id,
            message_ts=message_ts,
            source_event=source_event,
            original_name=original_name,
            title=title,
            mimetype=str(file_obj.get("mimetype") or "") or None,
            filetype=str(file_obj.get("filetype") or "") or None,
            category=category,
            local_path=str(local_path),
            drive_path=str(drive_path),
            size_bytes=len(data),
            sha256=digest,
            source_text=message_text,
        )

        processed.append(
            IngestedFile(
                file_id=file_id,
                original_name=original_name,
                category=category,
                local_path=str(local_path),
                drive_path=str(drive_path),
                size_bytes=len(data),
                sha256=digest,
            )
        )

    return {
        "processed_count": len(processed),
        "processed": [
            {
                "file_id": item.file_id,
                "original_name": item.original_name,
                "category": item.category,
                "local_path": item.local_path,
                "drive_path": item.drive_path,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
            }
            for item in processed
        ],
        "skipped": skipped,
        "errors": errors,
    }

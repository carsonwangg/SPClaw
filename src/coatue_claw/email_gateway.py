from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email import policy
from email.message import EmailMessage, Message
from email.parser import BytesParser
from email.utils import make_msgid, parseaddr
import html as html_lib
import hashlib
import imaplib
import json
import logging
import os
from pathlib import Path
import re
import smtplib
import sqlite3
import ssl
import textwrap
import time
from typing import Any

from dotenv import load_dotenv

from coatue_claw.cli import run_diligence
from coatue_claw.file_bridge import FileBridgeError, load_config
from coatue_claw.memory_runtime import MemoryRuntime
from coatue_claw.slack_file_ingest import classify_category

load_dotenv("/opt/SPClaw/.env.prod")

logger = logging.getLogger(__name__)


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_EMAIL_DB_PATH",
            str(_data_root() / "db/email_gateway.sqlite"),
        )
    )


@dataclass(frozen=True)
class EmailConfig:
    enabled: bool
    imap_host: str
    imap_port: int
    imap_user: str
    imap_password: str
    imap_mailbox: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    from_address: str
    poll_seconds: int
    allowed_senders: set[str]
    max_attachment_mb: int


@dataclass(frozen=True)
class EmailAttachment:
    filename: str
    content_type: str
    payload: bytes


@dataclass(frozen=True)
class EmailCommand:
    kind: str
    arg: str | None = None


@dataclass(frozen=True)
class OutboundAttachment:
    filename: str
    content_type: str
    payload: bytes


@dataclass(frozen=True)
class EmailReply:
    body_text: str
    body_html: str | None = None
    attachments: tuple[OutboundAttachment, ...] = ()


class EmailGatewayStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = (db_path or _db_path()).expanduser().resolve()
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
                CREATE TABLE IF NOT EXISTS email_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL UNIQUE,
                    sender TEXT,
                    subject TEXT,
                    received_at_utc TEXT,
                    processed_at_utc TEXT NOT NULL,
                    status TEXT NOT NULL,
                    summary TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS email_attachment_ingest (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    category TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    drive_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    stored_at_utc TEXT NOT NULL,
                    UNIQUE(message_id, filename, sha256)
                );
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_messages_processed ON email_messages(processed_at_utc DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_email_attach_message ON email_attachment_ingest(message_id);")

    def is_processed(self, message_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM email_messages WHERE message_id = ? LIMIT 1",
                (message_id,),
            ).fetchone()
        return row is not None

    def record_message(
        self,
        *,
        message_id: str,
        sender: str | None,
        subject: str | None,
        received_at_utc: str | None,
        status: str,
        summary: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO email_messages (
                    message_id, sender, subject, received_at_utc, processed_at_utc, status, summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    sender,
                    subject,
                    received_at_utc,
                    _now_utc_iso(),
                    status,
                    summary[:1500],
                ),
            )

    def record_attachment(
        self,
        *,
        message_id: str,
        filename: str,
        category: str,
        local_path: str,
        drive_path: str,
        size_bytes: int,
        sha256: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO email_attachment_ingest (
                    message_id, filename, category, local_path, drive_path, size_bytes, sha256, stored_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    filename,
                    category,
                    local_path,
                    drive_path,
                    size_bytes,
                    sha256,
                    _now_utc_iso(),
                ),
            )

    def stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            messages = int(conn.execute("SELECT COUNT(*) AS c FROM email_messages").fetchone()["c"])
            attachments = int(conn.execute("SELECT COUNT(*) AS c FROM email_attachment_ingest").fetchone()["c"])
            latest = conn.execute(
                "SELECT message_id, sender, subject, processed_at_utc, status FROM email_messages ORDER BY processed_at_utc DESC LIMIT 5"
            ).fetchall()
        return {
            "db_path": str(self.db_path),
            "messages_total": messages,
            "attachments_total": attachments,
            "latest": [dict(row) for row in latest],
        }


def load_email_config() -> EmailConfig:
    enabled = os.environ.get("COATUE_CLAW_EMAIL_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    allowed_raw = os.environ.get("COATUE_CLAW_EMAIL_ALLOWED_SENDERS", "").strip()
    allowed = {item.strip().lower() for item in allowed_raw.split(",") if item.strip()}
    return EmailConfig(
        enabled=enabled,
        imap_host=os.environ.get("COATUE_CLAW_EMAIL_IMAP_HOST", "").strip(),
        imap_port=int(os.environ.get("COATUE_CLAW_EMAIL_IMAP_PORT", "993")),
        imap_user=os.environ.get("COATUE_CLAW_EMAIL_IMAP_USER", "").strip(),
        imap_password=os.environ.get("COATUE_CLAW_EMAIL_IMAP_PASSWORD", "").strip(),
        imap_mailbox=os.environ.get("COATUE_CLAW_EMAIL_IMAP_MAILBOX", "INBOX").strip() or "INBOX",
        smtp_host=os.environ.get("COATUE_CLAW_EMAIL_SMTP_HOST", "").strip(),
        smtp_port=int(os.environ.get("COATUE_CLAW_EMAIL_SMTP_PORT", "587")),
        smtp_user=os.environ.get("COATUE_CLAW_EMAIL_SMTP_USER", "").strip(),
        smtp_password=os.environ.get("COATUE_CLAW_EMAIL_SMTP_PASSWORD", "").strip(),
        from_address=os.environ.get("COATUE_CLAW_EMAIL_FROM", "").strip(),
        poll_seconds=max(15, int(os.environ.get("COATUE_CLAW_EMAIL_POLL_SECONDS", "60"))),
        allowed_senders=allowed,
        max_attachment_mb=max(1, int(os.environ.get("COATUE_CLAW_EMAIL_MAX_ATTACHMENT_MB", "25"))),
    )


def _config_errors(cfg: EmailConfig) -> list[str]:
    missing: list[str] = []
    for key, value in (
        ("COATUE_CLAW_EMAIL_IMAP_HOST", cfg.imap_host),
        ("COATUE_CLAW_EMAIL_IMAP_USER", cfg.imap_user),
        ("COATUE_CLAW_EMAIL_IMAP_PASSWORD", cfg.imap_password),
        ("COATUE_CLAW_EMAIL_SMTP_HOST", cfg.smtp_host),
        ("COATUE_CLAW_EMAIL_SMTP_USER", cfg.smtp_user),
        ("COATUE_CLAW_EMAIL_SMTP_PASSWORD", cfg.smtp_password),
    ):
        if not value:
            missing.append(key)
    if not cfg.from_address:
        missing.append("COATUE_CLAW_EMAIL_FROM")
    return missing


def _extract_sender(msg: Message) -> str:
    sender_raw = msg.get("From", "")
    _, addr = parseaddr(sender_raw)
    return addr.strip().lower()


def _extract_message_id(msg: Message) -> str:
    msg_id = (msg.get("Message-ID") or "").strip()
    if msg_id:
        return msg_id
    digest = hashlib.sha256((msg.as_string() or "").encode("utf-8")).hexdigest()
    return f"<fallback-{digest[:24]}@coatue-claw>"


def _extract_subject(msg: Message) -> str:
    return (msg.get("Subject") or "").strip()


def _extract_received_at(msg: Message) -> str | None:
    date = (msg.get("Date") or "").strip()
    if not date:
        return None
    return date


def _extract_body_text(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            content_type = (part.get_content_type() or "").lower()
            disposition = (part.get_content_disposition() or "").lower()
            if disposition == "attachment":
                continue
            if content_type != "text/plain":
                continue
            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"
            try:
                return payload.decode(charset, errors="replace").strip()
            except LookupError:
                return payload.decode("utf-8", errors="replace").strip()
    payload = msg.get_payload(decode=True) or b""
    charset = msg.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace").strip()
    except LookupError:
        return payload.decode("utf-8", errors="replace").strip()


def _sanitize_filename(filename: str) -> str:
    out = re.sub(r"[^A-Za-z0-9._ -]+", "-", filename).strip()
    out = re.sub(r"\s+", " ", out)
    return out or "attachment.bin"


def _pick_unique_path(root: Path, filename: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    candidate = root / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for i in range(1, 1000):
        alt = root / f"{stem}--{i}{suffix}"
        if not alt.exists():
            return alt
    return root / f"{stem}--{int(datetime.now(UTC).timestamp())}{suffix}"


def _extract_attachments(msg: Message, *, max_attachment_mb: int) -> list[EmailAttachment]:
    out: list[EmailAttachment] = []
    max_bytes = max_attachment_mb * 1024 * 1024
    if not msg.is_multipart():
        return out

    for part in msg.walk():
        disposition = (part.get_content_disposition() or "").lower()
        if disposition != "attachment":
            continue
        filename = _sanitize_filename(part.get_filename() or "attachment.bin")
        payload = part.get_payload(decode=True) or b""
        if not payload or len(payload) > max_bytes:
            continue
        out.append(
            EmailAttachment(
                filename=filename,
                content_type=(part.get_content_type() or "application/octet-stream"),
                payload=payload,
            )
        )
    return out


def parse_email_command(subject: str, body: str) -> EmailCommand:
    parts = [part.strip() for part in [body, subject] if part and part.strip()]
    text = "\n".join(parts)
    lowered = text.lower()

    # Extract a likely ticker near the diligence keyword and ignore common filler words.
    ticker_stopwords = {
        "diligence",
        "dilligence",
        "please",
        "pls",
        "help",
        "for",
        "on",
        "about",
        "the",
        "a",
        "an",
        "me",
        "us",
    }

    def _clean_token(raw: str) -> str:
        token = raw.strip().strip(".,;:!?()[]{}<>\"'`").lstrip("$")
        return token.upper()

    def _extract_diligence_ticker(part: str) -> str | None:
        for match in re.finditer(r"\b(?:diligence|dilligence)\b(?P<trailing>(?:\s+\S+){1,8})", part, re.IGNORECASE):
            trailing = match.group("trailing") or ""
            for raw in re.split(r"\s+", trailing.strip()):
                token = _clean_token(raw)
                if not token:
                    continue
                if token.lower() in ticker_stopwords:
                    continue
                if re.fullmatch(r"[A-Z][A-Z0-9.-]{0,11}", token):
                    return token
        return None

    for part in parts:
        ticker = _extract_diligence_ticker(part)
        if ticker:
            return EmailCommand(kind="diligence", arg=ticker)

    m = re.search(r"\bmemory\s+status\b", lowered)
    if m:
        return EmailCommand(kind="memory_status")
    m = re.search(r"\bmemory\s+query\s+(.+)$", text, re.IGNORECASE | re.MULTILINE)
    if m:
        return EmailCommand(kind="memory_query", arg=m.group(1).strip())
    if re.search(r"\bfiles?\s+status\b", lowered):
        return EmailCommand(kind="files_status")
    if re.search(r"\bhelp\b", lowered):
        return EmailCommand(kind="help")
    return EmailCommand(kind="help")


def _ingest_email_attachments(
    *,
    attachments: list[EmailAttachment],
    body_text: str,
    message_id: str,
    store: EmailGatewayStore,
) -> list[dict[str, Any]]:
    if not attachments:
        return []
    try:
        bridge = load_config()
    except FileBridgeError:
        logger.exception("Email attachment ingest failed: missing file bridge config")
        return []

    ingested: list[dict[str, Any]] = []
    for attachment in attachments:
        category = classify_category(
            filename=attachment.filename,
            message_text=body_text,
            mimetype=attachment.content_type,
            filetype=Path(attachment.filename).suffix.lower().lstrip("."),
        )
        local_path = _pick_unique_path(bridge.local.incoming / category, attachment.filename)
        drive_path = _pick_unique_path(bridge.drive.incoming / category, local_path.name)

        local_path.write_bytes(attachment.payload)
        drive_path.write_bytes(attachment.payload)

        digest = hashlib.sha256(attachment.payload).hexdigest()
        store.record_attachment(
            message_id=message_id,
            filename=attachment.filename,
            category=category,
            local_path=str(local_path),
            drive_path=str(drive_path),
            size_bytes=len(attachment.payload),
            sha256=digest,
        )
        ingested.append(
            {
                "filename": attachment.filename,
                "category": category,
                "local_path": str(local_path),
                "drive_path": str(drive_path),
                "size_bytes": len(attachment.payload),
            }
        )
    return ingested


def _format_help() -> str:
    return (
        "Coatue Claw Email Commands\n\n"
        "Examples:\n"
        "- diligence SNOW\n"
        "- dilligence MDB\n"
        "- memory status\n"
        "- memory query my daughter's birthday\n"
        "- files status\n\n"
        "You can also attach files. Attachments are auto-sorted into knowledge folders."
    )


def _extract_section_bullets(lines: list[str], heading: str, *, limit: int) -> list[str]:
    in_section = False
    out: list[str] = []
    heading_prefix = heading.strip().lower()
    for raw in lines:
        line = raw.strip()
        if line.lower().startswith("## "):
            if line.lower().startswith(heading_prefix):
                in_section = True
                continue
            if in_section:
                break
        if not in_section:
            continue
        if line.startswith("- "):
            out.append(line[2:].strip())
            if len(out) >= limit:
                break
    return out


def _extract_title(lines: list[str], *, fallback: str) -> str:
    for raw in lines:
        line = raw.strip()
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def _clean_consumer_line(text: str, *, max_len: int = 220) -> str:
    cleaned = re.sub(r"\s*\[Source:.*$", "", text, flags=re.IGNORECASE).strip()
    cleaned = cleaned.replace("`", "")
    cleaned = cleaned.replace("->", "to")
    cleaned = re.sub(r"\s+", " ", cleaned)
    # Remove local filesystem leakage from consumer-facing output.
    cleaned = re.sub(r"/opt/[^\s)]+", "", cleaned)
    cleaned = cleaned.strip(" -")
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 3].rstrip() + "..."
    return cleaned


def _render_memo_pdf(
    *,
    title: str,
    ticker: str,
    backdrop: str,
    sections: list[tuple[str, list[str]]],
) -> bytes:
    # Use matplotlib's PDF backend (already a core dependency) for a clean, sectioned brief.
    import matplotlib

    matplotlib.use("Agg")
    import io

    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.patches import FancyBboxPatch

    page_width, page_height = 8.5, 11.0  # US Letter inches
    left = 0.08
    right = 0.92
    top = 0.96
    bottom = 0.07
    chars_per_line = 92
    blue = "#2B78B6"
    dark = "#0F172A"
    muted = "#64748B"

    def wrapped(line: str, *, indent_chars: int = 0) -> list[str]:
        if not line:
            return [""]
        width = max(30, chars_per_line - indent_chars)
        return textwrap.wrap(line, width=width, break_long_words=False, break_on_hyphens=False) or [line]

    def pdf_safe(text: str) -> str:
        # Matplotlib text treats $...$ as math text; escape literal dollars used in finance values.
        return text.replace("$", r"\$")

    def new_page() -> tuple[Any, Any]:
        fig, ax = plt.subplots(figsize=(page_width, page_height))
        ax.axis("off")
        return fig, ax

    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        page_num = 1
        fig, ax = new_page()
        y = top

        def draw_header(*, first_page: bool) -> float:
            if first_page:
                ax.text(0.5, 0.95, pdf_safe(title), transform=ax.transAxes, fontsize=24, color=blue, fontweight="bold", va="top", ha="center")
                ax.text(
                    0.5,
                    0.915,
                    f"Generated {datetime.now(UTC).strftime('%B %d, %Y at %I:%M %p UTC')} | Coatue Claw",
                    transform=ax.transAxes,
                    fontsize=10.5,
                    color=muted,
                    va="top",
                    ha="center",
                )
                ax.text(
                    0.5,
                    0.892,
                    "Models: OpenClaw research stack (neutral memo mode)",
                    transform=ax.transAxes,
                    fontsize=9.8,
                    color=muted,
                    va="top",
                    ha="center",
                )
                ax.plot([left, right], [0.868, 0.868], transform=ax.transAxes, color=blue, linewidth=2.2)
                return 0.83
            ax.text(left, 0.955, pdf_safe(title), transform=ax.transAxes, fontsize=12, color=blue, fontweight="bold", va="top", ha="left")
            ax.plot([left, right], [0.94, 0.94], transform=ax.transAxes, color="#D6E6F2", linewidth=1.5)
            return 0.91

        def save_current_page() -> None:
            ax.text(left, 0.03, "Coatue Claw Diligence Brief", transform=ax.transAxes, fontsize=8.8, color=muted, va="bottom", ha="left")
            ax.text(right, 0.03, f"Page {page_num}", transform=ax.transAxes, fontsize=8.8, color=muted, va="bottom", ha="right")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        def start_new_page() -> None:
            nonlocal fig, ax, y, page_num
            save_current_page()
            page_num += 1
            fig, ax = new_page()
            y = draw_header(first_page=False)

        def ensure_space(required: float) -> None:
            nonlocal y
            if y - required < bottom:
                start_new_page()

        y = draw_header(first_page=True)

        # Backdrop callout
        callout_lines = wrapped(backdrop, indent_chars=0)
        callout_h = 0.03 + (0.023 * len(callout_lines)) + 0.03
        ensure_space(callout_h + 0.025)
        box_bottom = y - callout_h
        box = FancyBboxPatch(
            (left, box_bottom),
            right - left,
            callout_h,
            boxstyle="round,pad=0.004,rounding_size=0.01",
            transform=ax.transAxes,
            facecolor="#EAF4FB",
            edgecolor="#8EC5E8",
            linewidth=1.2,
        )
        ax.add_patch(box)
        ax.text(left + 0.018, y - 0.022, "Backdrop", transform=ax.transAxes, fontsize=11.5, fontweight="bold", color=dark, va="top", ha="left")
        ly = y - 0.048
        for line in callout_lines:
            ax.text(left + 0.018, ly, pdf_safe(line), transform=ax.transAxes, fontsize=10.7, color=dark, va="top", ha="left")
            ly -= 0.023
        y = box_bottom - 0.03

        ensure_space(0.05)
        ax.text(left, y, "Executive Summary", transform=ax.transAxes, fontsize=14.8, fontweight="bold", color=blue, va="top", ha="left")
        y -= 0.042

        for section_title, items in sections:
            if not items:
                continue
            ensure_space(0.05)
            ax.text(left, y, section_title, transform=ax.transAxes, fontsize=12.3, fontweight="bold", color=dark, va="top", ha="left")
            y -= 0.03
            for item in items:
                chunks = wrapped(item, indent_chars=2)
                ensure_space(0.022 * (len(chunks) + 0.5))
                for idx, chunk in enumerate(chunks):
                    prefix = "• " if idx == 0 else "  "
                    ax.text(left + 0.006, y, pdf_safe(prefix + chunk), transform=ax.transAxes, fontsize=10.6, color=dark, va="top", ha="left")
                    y -= 0.021
                y -= 0.006
            y -= 0.012

        save_current_page()

    return buf.getvalue()


def _format_diligence_reply(*, ticker: str, path: Path) -> EmailReply:
    lines = path.read_text(encoding="utf-8").splitlines()
    title = f"{ticker} Diligence Report"
    key_takeaways = _extract_section_bullets(lines, "## 1. Key Takeaways", limit=5)
    business = _extract_section_bullets(lines, "## 2. Business Overview", limit=6)
    financials = _extract_section_bullets(lines, "## 3. Financials & Funding", limit=6)
    strengths = _extract_section_bullets(lines, "## 5. Company Strengths", limit=5)
    risks = _extract_section_bullets(lines, "## 6. Key Risks", limit=5)
    questions = _extract_section_bullets(lines, "## 7. Open Diligence Questions", limit=4)

    key_takeaways_clean = [_clean_consumer_line(item) for item in key_takeaways if _clean_consumer_line(item)]
    risks_clean = [_clean_consumer_line(item) for item in risks if _clean_consumer_line(item)]
    business_clean = [_clean_consumer_line(item) for item in business if _clean_consumer_line(item)]
    financials_clean = [_clean_consumer_line(item) for item in financials if _clean_consumer_line(item)]
    strengths_clean = [_clean_consumer_line(item) for item in strengths if _clean_consumer_line(item)]
    questions_clean = [_clean_consumer_line(item) for item in questions if _clean_consumer_line(item)]

    text_lines = [
        f"Diligence report is ready for {ticker}.",
        f"Title: {title}",
        "",
        "Quick Takeaways:",
    ]
    if key_takeaways_clean:
        text_lines.extend(f"- {item}" for item in key_takeaways_clean)
    else:
        text_lines.append("- Key takeaway extraction unavailable; see attached report.")

    if risks_clean:
        text_lines.extend(["", "Top Risks:"])
        text_lines.extend(f"- {item}" for item in risks_clean)

    pdf_name = f"{path.stem}.pdf"
    text_lines.extend(["", f"Attachment: {pdf_name}"])
    body_text = "\n".join(text_lines)

    items_html = "".join(f"<li>{html_lib.escape(item)}</li>" for item in key_takeaways_clean) or (
        "<li>Key takeaway extraction unavailable; see attached report.</li>"
    )
    risks_html = ""
    if risks_clean:
        risks_html = (
            "<h3>Top Risks</h3><ul>"
            + "".join(f"<li>{html_lib.escape(item)}</li>" for item in risks_clean)
            + "</ul>"
        )
    body_html = (
        f"<h2>Diligence Report: {html_lib.escape(ticker)}</h2>"
        f"<p><strong>{html_lib.escape(title)}</strong></p>"
        "<h3>Quick Takeaways</h3>"
        f"<ul>{items_html}</ul>"
        f"{risks_html}"
        f"<p><strong>Attachment:</strong> {html_lib.escape(pdf_name)}</p>"
    )

    sections: list[tuple[str, list[str]]] = [
        ("Key Takeaways", key_takeaways_clean),
        ("Business Overview", business_clean),
        ("Financial Snapshot", financials_clean),
        ("Company Strengths", strengths_clean),
        ("Key Risks", risks_clean),
        ("Open Questions", questions_clean),
    ]

    backdrop_candidates = (
        key_takeaways_clean[:2]
        + business_clean[:1]
        + financials_clean[:1]
        + risks_clean[:1]
    )
    backdrop = " ".join(backdrop_candidates).strip()
    if not backdrop:
        backdrop = f"This brief summarizes the latest diligence on {ticker}, including business context, financial profile, strengths, risks, and open questions."
    backdrop = _clean_consumer_line(backdrop, max_len=700)

    pdf_bytes = _render_memo_pdf(title=title, ticker=ticker, backdrop=backdrop, sections=sections)
    attachment = OutboundAttachment(
        filename=pdf_name,
        content_type="application/pdf",
        payload=pdf_bytes,
    )
    return EmailReply(
        body_text=body_text,
        body_html=body_html,
        attachments=(attachment,),
    )


def _handle_command(command: EmailCommand) -> EmailReply:
    if command.kind == "help":
        return EmailReply(body_text=_format_help())
    if command.kind == "diligence":
        assert command.arg
        path = run_diligence(command.arg)
        return _format_diligence_reply(ticker=command.arg, path=path)
    if command.kind == "memory_status":
        memory = MemoryRuntime()
        stats = memory.stats()
        return EmailReply(
            body_text=(
            "Memory status:\n"
            + json.dumps(stats, indent=2, sort_keys=True)
            )
        )
    if command.kind == "memory_query":
        assert command.arg
        memory = MemoryRuntime()
        return EmailReply(body_text=memory.format_retrieval(command.arg, limit=6))
    if command.kind == "files_status":
        bridge = load_config()
        from coatue_claw.file_bridge import status as file_status

        return EmailReply(body_text="File bridge status:\n" + json.dumps(file_status(bridge), indent=2, sort_keys=True))
    return EmailReply(body_text=_format_help())


def _build_reply_email(
    *,
    cfg: EmailConfig,
    to_address: str,
    subject: str,
    body: str,
    body_html: str | None,
    attachments: tuple[OutboundAttachment, ...],
    in_reply_to: str | None,
) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = cfg.from_address
    msg["To"] = to_address
    msg["Subject"] = subject
    msg["Message-ID"] = make_msgid(domain=cfg.from_address.split("@")[-1] if "@" in cfg.from_address else None)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = in_reply_to
    msg.set_content(body)
    if body_html:
        msg.add_alternative(body_html, subtype="html")
    for attachment in attachments:
        maintype, subtype = "application", "octet-stream"
        if "/" in attachment.content_type:
            maintype, subtype = attachment.content_type.split("/", 1)
        msg.add_attachment(
            attachment.payload,
            maintype=maintype,
            subtype=subtype,
            filename=attachment.filename,
        )
    return msg


def _send_reply(
    cfg: EmailConfig,
    *,
    to_address: str,
    subject: str,
    body: str,
    body_html: str | None,
    attachments: tuple[OutboundAttachment, ...],
    in_reply_to: str | None,
) -> None:
    msg = _build_reply_email(
        cfg=cfg,
        to_address=to_address,
        subject=subject,
        body=body,
        body_html=body_html,
        attachments=attachments,
        in_reply_to=in_reply_to,
    )
    context = ssl.create_default_context()
    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=30) as smtp:
        smtp.starttls(context=context)
        smtp.login(cfg.smtp_user, cfg.smtp_password)
        smtp.send_message(msg)


def _is_sender_allowed(cfg: EmailConfig, sender: str) -> bool:
    if not cfg.allowed_senders:
        return True
    return sender.lower() in cfg.allowed_senders


def _process_email_message(
    *,
    cfg: EmailConfig,
    msg: Message,
    store: EmailGatewayStore,
) -> tuple[str, str]:
    sender = _extract_sender(msg)
    subject = _extract_subject(msg)
    message_id = _extract_message_id(msg)
    received_at = _extract_received_at(msg)

    if store.is_processed(message_id):
        return ("skipped_duplicate", "Message already processed")

    if not sender:
        store.record_message(
            message_id=message_id,
            sender=sender,
            subject=subject,
            received_at_utc=received_at,
            status="error",
            summary="Missing sender address",
        )
        return ("error", "Missing sender address")

    if not _is_sender_allowed(cfg, sender):
        store.record_message(
            message_id=message_id,
            sender=sender,
            subject=subject,
            received_at_utc=received_at,
            status="blocked_sender",
            summary=f"Sender not allowed: {sender}",
        )
        return ("blocked_sender", f"Sender not allowed: {sender}")

    body = _extract_body_text(msg)
    attachments = _extract_attachments(msg, max_attachment_mb=cfg.max_attachment_mb)
    ingested = _ingest_email_attachments(
        attachments=attachments,
        body_text=body,
        message_id=message_id,
        store=store,
    )

    command = parse_email_command(subject, body)
    command_output = _handle_command(command)
    ingest_block_text = ""
    ingest_block_html = ""
    if ingested:
        lines = ["", "", "Attachment ingest:"]
        html_lines = ["<h3>Attachment Ingest</h3>", "<ul>"]
        for item in ingested[:10]:
            lines.append(
                f"- {item['filename']} -> {item['category']} ({item['size_bytes']} bytes)"
            )
            html_lines.append(
                f"<li>{html_lib.escape(item['filename'])} → {html_lib.escape(item['category'])} ({int(item['size_bytes'])} bytes)</li>"
            )
        html_lines.append("</ul>")
        ingest_block_text = "\n".join(lines)
        ingest_block_html = "".join(html_lines)

    reply_body = f"{command_output.body_text}{ingest_block_text}"
    reply_html = None
    if command_output.body_html:
        reply_html = f"{command_output.body_html}{ingest_block_html}"
    reply_subject = f"Re: {subject}" if subject else "Re: Coatue Claw"
    _send_reply(
        cfg,
        to_address=sender,
        subject=reply_subject,
        body=reply_body,
        body_html=reply_html,
        attachments=command_output.attachments,
        in_reply_to=message_id,
    )

    store.record_message(
        message_id=message_id,
        sender=sender,
        subject=subject,
        received_at_utc=received_at,
        status="processed",
        summary=f"command={command.kind}, attachments={len(ingested)}",
    )
    return ("processed", f"command={command.kind}, attachments={len(ingested)}")


def run_once() -> dict[str, Any]:
    cfg = load_email_config()
    store = EmailGatewayStore()
    if not cfg.enabled:
        return {
            "ok": False,
            "reason": "email_disabled",
            "hint": "Set COATUE_CLAW_EMAIL_ENABLED=true",
            "stats": store.stats(),
            "timestamp_utc": _now_utc_iso(),
        }

    errors = _config_errors(cfg)
    if errors:
        return {
            "ok": False,
            "reason": "email_config_missing",
            "missing": errors,
            "stats": store.stats(),
            "timestamp_utc": _now_utc_iso(),
        }

    processed = 0
    skipped = 0
    failures: list[str] = []

    with imaplib.IMAP4_SSL(cfg.imap_host, cfg.imap_port) as imap:
        imap.login(cfg.imap_user, cfg.imap_password)
        imap.select(cfg.imap_mailbox)
        status, data = imap.search(None, "UNSEEN")
        if status != "OK":
            return {
                "ok": False,
                "reason": "imap_search_failed",
                "timestamp_utc": _now_utc_iso(),
                "stats": store.stats(),
            }

        uids = data[0].split() if data and data[0] else []
        for uid in uids:
            try:
                fetch_status, payload = imap.fetch(uid, "(RFC822)")
                if fetch_status != "OK" or not payload:
                    skipped += 1
                    continue
                raw = payload[0][1] if isinstance(payload[0], tuple) else b""
                if not raw:
                    skipped += 1
                    continue
                msg = BytesParser(policy=policy.default).parsebytes(raw)
                result, detail = _process_email_message(cfg=cfg, msg=msg, store=store)
                if result == "processed":
                    processed += 1
                elif result.startswith("skipped"):
                    skipped += 1
                else:
                    failures.append(detail)
            except Exception as exc:
                logger.exception("Email processing failed for uid=%s", uid)
                failures.append(str(exc))
            finally:
                try:
                    imap.store(uid, "+FLAGS", "\\Seen")
                except Exception:
                    logger.exception("Failed to mark message seen uid=%s", uid)

    return {
        "ok": len(failures) == 0,
        "processed": processed,
        "skipped": skipped,
        "failures": failures,
        "stats": store.stats(),
        "timestamp_utc": _now_utc_iso(),
    }


def run_forever() -> None:
    cfg = load_email_config()
    while True:
        try:
            result = run_once()
            logger.info("email run_once result: %s", result)
        except Exception:
            logger.exception("email gateway iteration failed")
        time.sleep(cfg.poll_seconds)


def status_snapshot() -> dict[str, Any]:
    cfg = load_email_config()
    store = EmailGatewayStore()
    return {
        "enabled": cfg.enabled,
        "imap_host": cfg.imap_host,
        "imap_port": cfg.imap_port,
        "imap_mailbox": cfg.imap_mailbox,
        "smtp_host": cfg.smtp_host,
        "smtp_port": cfg.smtp_port,
        "from_address": cfg.from_address,
        "allowed_senders": sorted(cfg.allowed_senders),
        "poll_seconds": cfg.poll_seconds,
        "max_attachment_mb": cfg.max_attachment_mb,
        "config_missing": _config_errors(cfg),
        "stats": store.stats(),
        "timestamp_utc": _now_utc_iso(),
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser("coatue-claw-email-gateway")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("run-once")
    sub.add_parser("serve")

    args = parser.parse_args(argv)
    if args.cmd == "status":
        print(json.dumps(status_snapshot(), indent=2, sort_keys=True))
        return 0
    if args.cmd == "run-once":
        print(json.dumps(run_once(), indent=2, sort_keys=True))
        return 0
    if args.cmd == "serve":
        run_forever()
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

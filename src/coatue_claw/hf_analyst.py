from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any

import yfinance as yf

from coatue_claw.hf_document_extract import ExtractedDocument, extract_documents
from coatue_claw.hf_prompt_contract import CitationRef, HFScorecard, PromptDraft, build_scorecard, parse_model_json, render_markdown
from coatue_claw.hf_store import HFStore
from coatue_claw.slack_file_ingest import ingest_slack_files

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]

try:
    from slack_sdk import WebClient
except Exception:  # pragma: no cover - optional dependency
    WebClient = None  # type: ignore[assignment]


class HFAError(RuntimeError):
    pass


@dataclass(frozen=True)
class HFInputDocument:
    file_id: str
    name: str
    mime_type: str | None
    local_path: str
    source_ts_utc: str | None
    extracted_text: str
    page_count: int | None
    char_count: int
    sha256: str | None
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class HFEvidenceItem:
    section: str
    claim: str
    source_ref: str
    source_ts_utc: str
    confidence: float


@dataclass(frozen=True)
class HFAnalysisResult:
    run_id: int
    markdown: str
    summary_text: str
    artifact_path: str | None
    scorecard: HFScorecard
    memory_facts: tuple[str, ...]
    warnings: tuple[str, ...]
    files_analyzed: int


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _data_root() -> Path:
    return Path(os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data"))


def _artifact_dir() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_HFA_ARTIFACT_DIR",
            str(_data_root() / "artifacts/hf-analyst"),
        )
    )


def _hfa_model() -> str:
    return (os.environ.get("COATUE_CLAW_HFA_MODEL", "gpt-5.2-chat-latest") or "gpt-5.2-chat-latest").strip()


def _file_ingest_db_path() -> Path:
    return Path(
        os.environ.get(
            "COATUE_CLAW_FILE_INGEST_DB_PATH",
            str(_data_root() / "db/file_ingest.sqlite"),
        )
    )


def _resolve_slack_token() -> str:
    direct = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if direct:
        return direct
    cfg_path = Path.home() / ".openclaw/openclaw.json"
    if cfg_path.exists():
        try:
            payload = json.loads(cfg_path.read_text(encoding="utf-8"))
            from_cfg = str((payload.get("channels", {}).get("slack", {}).get("botToken", ""))).strip()
            if from_cfg:
                return from_cfg
        except Exception:
            pass
    raise HFAError("Slack bot token missing (set SLACK_BOT_TOKEN or ~/.openclaw/openclaw.json).")


def _slack_client_from_env() -> Any:
    if WebClient is None:
        raise HFAError("slack_sdk not installed; cannot run HFA Slack workflow.")
    return WebClient(token=_resolve_slack_token())


def _thread_messages(*, slack_client: Any, channel: str, thread_ts: str) -> list[dict[str, Any]]:
    cursor: str | None = None
    rows: list[dict[str, Any]] = []
    while True:
        kwargs: dict[str, Any] = {"channel": channel, "ts": thread_ts, "limit": 200}
        if cursor:
            kwargs["cursor"] = cursor
        response = slack_client.conversations_replies(**kwargs)
        messages = response.get("messages") if isinstance(response, dict) else []
        if isinstance(messages, list):
            for message in messages:
                if isinstance(message, dict):
                    rows.append(message)
        meta = response.get("response_metadata") if isinstance(response, dict) else {}
        next_cursor = str((meta or {}).get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
        if not next_cursor:
            break
        cursor = next_cursor
    return rows


def _collect_thread_files(*, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for message in messages:
        files = message.get("files")
        if not isinstance(files, list):
            continue
        for file_obj in files:
            if not isinstance(file_obj, dict):
                continue
            file_id = str(file_obj.get("id") or "").strip()
            if not file_id or file_id in seen:
                continue
            seen.add(file_id)
            out.append(
                {
                    "file_id": file_id,
                    "file": file_obj,
                    "message_ts": str(message.get("ts") or ""),
                    "message_text": str(message.get("text") or ""),
                    "user_id": str(message.get("user") or ""),
                }
            )
    return out


def _lookup_ingested_rows(file_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not file_ids:
        return {}
    db_path = _file_ingest_db_path()
    if not db_path.exists():
        return {}
    placeholders = ",".join("?" for _ in file_ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT slack_file_id, original_name, title, mimetype, filetype, local_path, drive_path,
                   size_bytes, sha256, ingested_at_utc
            FROM slack_file_ingest
            WHERE slack_file_id IN ({placeholders})
            """,
            file_ids,
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(row)
        out[str(payload.get("slack_file_id") or "")] = payload
    return out


def _ensure_ingested(
    *,
    slack_client: Any,
    channel: str,
    thread_files: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    file_ids = [str(item.get("file_id") or "") for item in thread_files if str(item.get("file_id") or "").strip()]
    existing = _lookup_ingested_rows(file_ids)
    missing = [item for item in thread_files if str(item.get("file_id") or "") not in existing]
    if not missing:
        return existing

    token = str(getattr(slack_client, "token", "") or os.environ.get("SLACK_BOT_TOKEN", "")).strip()
    for item in missing:
        ingest_slack_files(
            files=[item["file"]],
            channel=channel,
            user_id=(item.get("user_id") or None),
            message_ts=(item.get("message_ts") or None),
            message_text=(item.get("message_text") or None),
            source_event="hfa-thread-reconcile",
            token=token or None,
        )
    return _lookup_ingested_rows(file_ids)


def _safe_sha256(path: str) -> str | None:
    p = Path(path)
    if not p.exists() or (not p.is_file()):
        return None
    hasher = hashlib.sha256()
    with p.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _infer_tickers(text: str) -> list[str]:
    tokens = re.findall(r"\$?[A-Z][A-Z0-9]{1,5}", text.upper())
    deny = {"THE", "THIS", "WITH", "FROM", "WHAT", "WHEN", "WILL", "THAT", "FOR", "AND", "NOT", "HAS", "HAVE", "ARE"}
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        value = token.lstrip("$")
        if value in deny:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
        if len(out) >= 8:
            break
    return out


def _market_context(tickers: list[str], *, as_of_utc: str) -> tuple[list[str], list[str]]:
    lines: list[str] = []
    source_summary: list[str] = []
    for ticker in tickers[:5]:
        try:
            info = yf.Ticker(ticker).info or {}
        except Exception:
            continue
        short = str(info.get("shortName") or info.get("longName") or ticker).strip()
        mcap = info.get("marketCap")
        pe = info.get("forwardPE") or info.get("trailingPE")
        ps = info.get("priceToSalesTrailing12Months")
        line = f"{ticker} ({short}): marketCap={mcap}, PE={pe}, P/S={ps}"
        lines.append(line)
        source_summary.append(f"Yahoo Finance via yfinance ({ticker}) (timestamp_utc: `{as_of_utc}`)")
    return lines, source_summary


def _web_context(tickers: list[str], *, as_of_utc: str) -> tuple[list[str], list[str], list[str]]:
    rows: list[str] = []
    sources: list[str] = []
    warnings: list[str] = []
    if not tickers:
        return rows, sources, warnings
    try:
        from coatue_claw import market_daily as md
    except Exception:
        warnings.append("web_context_unavailable:market_daily_import_failed")
        return rows, sources, warnings

    since_utc = datetime.now(UTC) - timedelta(days=7)
    for ticker in tickers[:4]:
        try:
            aliases = md._company_aliases(ticker)  # type: ignore[attr-defined]
            evidence, backend, notes = md._fetch_web_evidence(  # type: ignore[attr-defined]
                ticker=ticker,
                aliases=aliases,
                since_utc=since_utc,
                pct_move=None,
            )
        except Exception as exc:
            warnings.append(f"web_context_failed:{ticker}:{type(exc).__name__}")
            continue
        if notes:
            warnings.extend(str(note) for note in notes[:3])
        for item in evidence[:3]:
            text = str(getattr(item, "text", "") or "").strip()
            url = str(getattr(item, "url", "") or "").strip()
            if not text:
                continue
            rows.append(f"{ticker}: {text}")
            if url:
                sources.append(f"{url} (timestamp_utc: `{as_of_utc}`)")
        if backend:
            sources.append(f"web backend `{backend}` for `{ticker}` (timestamp_utc: `{as_of_utc}`)")
    return rows, sources, warnings


def _doc_source_summary(docs: list[HFInputDocument]) -> list[str]:
    out: list[str] = []
    for doc in docs:
        ts = doc.source_ts_utc or _utc_now_iso()
        out.append(f"{doc.name} -> {doc.local_path} (timestamp_utc: `{ts}`)")
    return out


def _base_section_citations(source_summary: list[str], *, generated_at_utc: str) -> dict[str, tuple[CitationRef, ...]]:
    refs: list[CitationRef] = []
    for line in source_summary[:8]:
        refs.append(CitationRef(source_ref=line, source_ts_utc=generated_at_utc))
    mapped = tuple(refs) if refs else (CitationRef(source_ref="No strong citation evidence found.", source_ts_utc=generated_at_utc),)
    return {
        "aaa_snapshot": mapped,
        "variant_view": mapped,
        "scorecard": mapped,
        "catalysts": mapped,
        "risks": mapped,
        "verify_next": mapped,
    }


def _fallback_draft(
    *,
    docs: list[HFInputDocument],
    question: str | None,
    market_lines: list[str],
    web_lines: list[str],
    source_summary: list[str],
    generated_at_utc: str,
) -> PromptDraft:
    raw_chars = sum(doc.char_count for doc in docs)
    evidence_lines = [line for line in market_lines[:3] + web_lines[:3] if line.strip()]
    low_signal = raw_chars < 700 or len(evidence_lines) < 2

    first_doc = docs[0] if docs else None
    top_doc_line = ""
    if first_doc and first_doc.extracted_text:
        top_doc_line = first_doc.extracted_text.replace("\n", " ").strip()[:260]

    at_a_glance = [
        f"Analyzed `{len(docs)}` thread document(s) with `{raw_chars}` extracted characters.",
        (f"Primary doc signal: {top_doc_line}" if top_doc_line else "Primary doc signal is sparse; extraction coverage is limited."),
        ("Web/market context found directional corroboration." if evidence_lines else "External corroboration is weak across web/market context."),
    ]
    actionable = (
        "Position sizing should stay conservative until confirmation checks in Section 6 are resolved."
        if low_signal
        else "Lean toward a starter position only if next catalyst checks confirm demand durability."
    )
    asymmetric = (
        "Current pricing may underweight downside scenario quality; asymmetric edge is not yet decisive."
        if low_signal
        else "Market appears to underweight the pace of catalyst realization versus consensus narrative."
    )
    variant = [
        "Base (50%): fundamentals progress unevenly; valuation range stays contained.",
        "Bull (30%): execution and catalysts convert faster than expected, expanding upside skew.",
        "Bear (20%): macro and execution misses compress multiple and challenge thesis durability.",
    ]
    scorecard = build_scorecard(
        growth=(2 if low_signal else 3),
        quality=(3 if low_signal else 4),
        valuation=(2 if low_signal else 3),
        catalyst=(2 if low_signal else 4),
        risk=(2 if low_signal else 3),
        confidence_label=("low" if low_signal else "medium"),
    )
    catalysts = [
        "30d: management commentary / near-term KPI disclosures clarify trajectory.",
        "90d: earnings or customer updates validate demand quality and margin progression.",
        "180d: sustained execution determines whether re-rating is justified.",
    ]
    risks = [
        "Break condition: KPI trend weakens versus management narrative.",
        "Break condition: valuation remains rich without matching earnings quality.",
        "Break condition: web/news signal stays contradictory across sources.",
    ]
    verify_next = [
        "Obtain segment-level revenue and margin details from primary filings/transcripts.",
        "Validate customer concentration and retention trend with reported disclosures.",
        "Re-check catalysts against updated market and web evidence before sizing up.",
    ]
    warnings: list[str] = []
    if low_signal:
        warnings.append("low_evidence_mode_enabled")
    citations = _base_section_citations(source_summary, generated_at_utc=generated_at_utc)
    return PromptDraft(
        at_a_glance=tuple(at_a_glance),
        actionable=actionable,
        asymmetric_insight=asymmetric,
        variant_view=tuple(variant),
        scorecard=scorecard,
        catalysts_timeline=tuple(catalysts),
        key_risks=tuple(risks),
        verify_next=tuple(verify_next),
        section_citations=citations,
        warnings=tuple(warnings),
    )


def _model_draft(
    *,
    docs: list[HFInputDocument],
    question: str | None,
    market_lines: list[str],
    web_lines: list[str],
    source_summary: list[str],
) -> PromptDraft | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if OpenAI is None or (not api_key):
        return None
    model = _hfa_model()
    client = OpenAI(api_key=api_key)

    doc_excerpt_lines: list[str] = []
    for doc in docs[:6]:
        text = doc.extracted_text.replace("\n", " ").strip()
        excerpt = text[:850]
        doc_excerpt_lines.append(f"- doc={doc.name} excerpt={excerpt}")
    prompt = (
        "You are generating a hedge-fund decision memo for crossover TMT.\n"
        "Return strict JSON only with keys:\n"
        "at_a_glance (array 3-5), actionable (string), asymmetric_insight (string),\n"
        "variant_view (array with Base/Bull/Bear lines),\n"
        "scorecard (object growth,quality,valuation,catalyst,risk each 1-5 and confidence high|medium|low),\n"
        "catalysts_timeline (array), key_risks (array), verify_next (array),\n"
        "section_citations (object keys aaa_snapshot, variant_view, scorecard, catalysts, risks, verify_next;\n"
        "each value is array of {source_ref, source_ts_utc}).\n"
        "Use explicit uncertainty if evidence is weak or conflicting.\n"
        "Question context:\n"
        f"{(question or 'none')}\n\n"
        "Document excerpts:\n"
        f"{chr(10).join(doc_excerpt_lines)}\n\n"
        "Market context lines:\n"
        f"{chr(10).join('- ' + item for item in market_lines[:8])}\n\n"
        "Web context lines:\n"
        f"{chr(10).join('- ' + item for item in web_lines[:8])}\n\n"
        "Available source refs:\n"
        f"{chr(10).join('- ' + item for item in source_summary[:12])}\n"
    )
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.1,
            messages=[
                {"role": "system", "content": "Return valid JSON only. No markdown."},
                {"role": "user", "content": prompt},
            ],
        )
    except Exception:
        return None
    text = ""
    if response and response.choices:
        text = str(response.choices[0].message.content or "").strip()
    if not text:
        return None
    return parse_model_json(text)


def _artifact_path(*, channel: str, thread_ts: str, now: datetime) -> Path:
    out_dir = _artifact_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_channel = re.sub(r"[^A-Za-z0-9._-]+", "-", channel).strip("-") or "unknown-channel"
    safe_thread = re.sub(r"[^0-9.]+", "-", thread_ts).strip("-") or "unknown-thread"
    filename = f"hfa-{safe_channel}-{safe_thread}-{now.strftime('%Y%m%d-%H%M%S')}.md"
    return out_dir / filename


def _thread_file_rows(*, slack_client: Any, channel: str, thread_ts: str) -> list[dict[str, Any]]:
    messages = _thread_messages(slack_client=slack_client, channel=channel, thread_ts=thread_ts)
    thread_files = _collect_thread_files(messages=messages)
    ingested = _ensure_ingested(slack_client=slack_client, channel=channel, thread_files=thread_files)
    rows: list[dict[str, Any]] = []
    for item in thread_files:
        file_id = str(item.get("file_id") or "")
        row = ingested.get(file_id)
        if row is None:
            continue
        rows.append(row)
    return rows


def file_set_hash(file_ids: list[str]) -> str:
    ordered = sorted(item.strip() for item in file_ids if item and item.strip())
    digest = hashlib.sha256("|".join(ordered).encode("utf-8")).hexdigest()
    return digest


def should_run_dm_autorun(*, channel: str, user_id: str, thread_ts: str, file_ids: list[str], store: HFStore | None = None) -> bool:
    current_store = store or HFStore()
    digest = file_set_hash(file_ids)
    return not current_store.has_dm_autorun(channel=channel, user_id=user_id, thread_ts=thread_ts, file_set_hash=digest)


def record_dm_autorun(*, channel: str, user_id: str, thread_ts: str, file_ids: list[str], store: HFStore | None = None) -> None:
    current_store = store or HFStore()
    current_store.record_dm_autorun(
        channel=channel,
        user_id=user_id,
        thread_ts=thread_ts,
        file_set_hash=file_set_hash(file_ids),
    )


def _memory_fact_lines(*, draft: PromptDraft, artifact_path: str | None, generated_at_utc: str) -> list[str]:
    lines: list[str] = []
    if draft.at_a_glance:
        lines.append(f"thesis: {draft.at_a_glance[0]}")
    for item in draft.catalysts_timeline[:2]:
        lines.append(f"catalyst: {item}")
    for item in draft.key_risks[:2]:
        lines.append(f"risk: {item}")
    lines.append(
        f"scorecard: total={draft.scorecard.weighted_total:.2f}/100 confidence={draft.scorecard.confidence_label}"
    )
    lines.append(f"artifact: {artifact_path or 'dry-run'} generated_at_utc={generated_at_utc}")
    return lines


def analyze_thread(
    *,
    channel: str,
    thread_ts: str,
    question: str | None = None,
    requested_by: str | None = None,
    trigger_mode: str = "manual",
    dry_run: bool = False,
    slack_client: Any | None = None,
    memory_runtime: Any | None = None,
    store: HFStore | None = None,
) -> HFAnalysisResult:
    current_store = store or HFStore()
    model_name = _hfa_model()
    run_id = current_store.start_run(
        channel=channel,
        thread_ts=thread_ts,
        requested_by=requested_by,
        question=question,
        trigger_mode=trigger_mode,
        model=model_name,
    )
    try:
        client = slack_client or _slack_client_from_env()
        rows = _thread_file_rows(slack_client=client, channel=channel, thread_ts=thread_ts)
        if not rows:
            raise HFAError("No thread files found to analyze.")

        extracted = extract_documents(rows)
        docs: list[HFInputDocument] = []
        warnings: list[str] = []
        for row, doc in zip(rows, extracted):
            sha = str(row.get("sha256") or "") or _safe_sha256(str(row.get("local_path") or ""))
            wrapped = HFInputDocument(
                file_id=doc.file_id,
                name=doc.name,
                mime_type=doc.mime_type,
                local_path=doc.local_path,
                source_ts_utc=doc.source_ts_utc,
                extracted_text=doc.extracted_text,
                page_count=doc.page_count,
                char_count=doc.char_count,
                sha256=(sha or None),
                warnings=doc.warnings,
            )
            docs.append(wrapped)
            for warning in wrapped.warnings:
                warnings.append(f"{wrapped.name}:{warning}")
            current_store.add_input(
                run_id=run_id,
                slack_file_id=wrapped.file_id or None,
                original_name=wrapped.name,
                mime_type=wrapped.mime_type,
                local_path=wrapped.local_path,
                sha256=wrapped.sha256,
                page_count=wrapped.page_count,
                char_count=wrapped.char_count,
                source_ts_utc=wrapped.source_ts_utc,
            )

        usable_docs = [doc for doc in docs if doc.extracted_text.strip()]
        if not usable_docs:
            raise HFAError("No parseable text extracted from thread files.")

        combined_text = "\n".join(doc.extracted_text[:1800] for doc in usable_docs[:8])
        combined_text = f"{question or ''}\n{combined_text}"
        tickers = _infer_tickers(combined_text)
        generated_at_utc = _utc_now_iso()

        market_lines, market_sources = _market_context(tickers, as_of_utc=generated_at_utc)
        web_lines, web_sources, web_warnings = _web_context(tickers, as_of_utc=generated_at_utc)
        warnings.extend(web_warnings)
        source_summary = _doc_source_summary(usable_docs) + market_sources + web_sources

        draft = _model_draft(
            docs=usable_docs,
            question=question,
            market_lines=market_lines,
            web_lines=web_lines,
            source_summary=source_summary,
        )
        if draft is None:
            draft = _fallback_draft(
                docs=usable_docs,
                question=question,
                market_lines=market_lines,
                web_lines=web_lines,
                source_summary=source_summary,
                generated_at_utc=generated_at_utc,
            )
            warnings.append("used_fallback_draft")
        else:
            warnings.extend(list(draft.warnings))

        title = f"HFA Decision Memo — {channel} / {thread_ts}"
        markdown = render_markdown(
            title=title,
            generated_at_utc=generated_at_utc,
            draft=draft,
            source_summary=tuple(source_summary),
        )

        section_text_map: dict[str, str] = {
            "aaa_snapshot": "\n".join(draft.at_a_glance) + f"\nActionable: {draft.actionable}\nAsymmetric Insight: {draft.asymmetric_insight}",
            "variant_view": "\n".join(draft.variant_view),
            "scorecard": f"weighted_total={draft.scorecard.weighted_total:.2f}; confidence={draft.scorecard.confidence_label}",
            "catalysts": "\n".join(draft.catalysts_timeline),
            "risks": "\n".join(draft.key_risks),
            "verify_next": "\n".join(draft.verify_next),
        }
        section_title_map = {
            "aaa_snapshot": "AAA Snapshot",
            "variant_view": "Variant View",
            "scorecard": "5-Factor Scorecard",
            "catalysts": "Catalysts & Timeline",
            "risks": "Key Risks / Break Conditions",
            "verify_next": "What To Verify Next",
        }
        for key in ("aaa_snapshot", "variant_view", "scorecard", "catalysts", "risks", "verify_next"):
            citations = [
                {"source_ref": ref.source_ref, "source_ts_utc": ref.source_ts_utc}
                for ref in draft.section_citations.get(key, ())
            ]
            current_store.add_section(
                run_id=run_id,
                section_key=key,
                section_title=section_title_map[key],
                section_text=section_text_map.get(key, ""),
                citations=citations,
                confidence=(0.4 if draft.scorecard.confidence_label == "Low" else 0.7 if draft.scorecard.confidence_label == "Medium" else 0.9),
            )

        artifact_path: str | None = None
        now = datetime.now(UTC)
        if not dry_run:
            path = _artifact_path(channel=channel, thread_ts=thread_ts, now=now)
            path.write_text(markdown, encoding="utf-8")
            artifact_path = str(path)

        summary_text = (
            f"HFA complete: files={len(usable_docs)} score={draft.scorecard.weighted_total:.2f}/100 "
            f"confidence={draft.scorecard.confidence_label} "
            f"{'(low-evidence mode)' if 'low_evidence_mode_enabled' in warnings else ''}".strip()
        )

        memory_facts = tuple(_memory_fact_lines(draft=draft, artifact_path=artifact_path, generated_at_utc=generated_at_utc))
        if (not dry_run) and memory_runtime is not None:
            try:
                memory_runtime.ingest_hfa_facts(
                    requested_by=(requested_by or "hfa-user"),
                    artifact_path=(artifact_path or ""),
                    generated_at_utc=generated_at_utc,
                    thesis=(draft.at_a_glance[0] if draft.at_a_glance else ""),
                    catalysts=list(draft.catalysts_timeline[:2]),
                    risks=list(draft.key_risks[:2]),
                    weighted_total=draft.scorecard.weighted_total,
                    confidence_label=draft.scorecard.confidence_label,
                )
            except Exception:
                warnings.append("memory_writeback_failed")

        current_store.complete_run(
            run_id=run_id,
            summary_text=summary_text,
            artifact_path=artifact_path,
            warnings=warnings,
        )

        return HFAnalysisResult(
            run_id=run_id,
            markdown=markdown,
            summary_text=summary_text,
            artifact_path=artifact_path,
            scorecard=draft.scorecard,
            memory_facts=memory_facts,
            warnings=tuple(warnings),
            files_analyzed=len(usable_docs),
        )
    except Exception as exc:
        current_store.fail_run(run_id=run_id, reason=f"{type(exc).__name__}: {exc}")
        if isinstance(exc, HFAError):
            raise
        raise HFAError(str(exc)) from exc


def hfa_status(*, channel: str | None = None, thread_ts: str | None = None, limit: int = 20, store: HFStore | None = None) -> dict[str, Any]:
    current_store = store or HFStore()
    return {
        "db_path": str(current_store.db_path),
        "runs": current_store.recent_runs(channel=channel, thread_ts=thread_ts, limit=limit),
    }


def parse_hfa_intent(text: str) -> tuple[str | None, str | None]:
    stripped = re.sub(r"<@[^>]+>", " ", text or "").strip()
    lower = stripped.lower()
    if re.search(r"^\s*hfa\s+status\b", lower):
        return ("status", None)
    match = re.search(r"^\s*hfa\s+analyze\b(.*)$", stripped, re.IGNORECASE)
    if match:
        tail = str(match.group(1) or "").strip()
        return ("analyze", tail or None)
    return (None, None)


def format_hfa_slack_summary(result: HFAnalysisResult) -> str:
    lines = [
        "HFA run complete.",
        f"- run_id: `{result.run_id}`",
        f"- files_analyzed: `{result.files_analyzed}`",
        f"- score: `{result.scorecard.weighted_total:.2f}/100`",
        f"- confidence: `{result.scorecard.confidence_label}`",
    ]
    if result.artifact_path:
        lines.append(f"- artifact: `{result.artifact_path}`")
    if result.warnings:
        lines.append(f"- warnings: `{len(result.warnings)}`")
    return "\n".join(lines)

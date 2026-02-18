from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from typing import Any

from coatue_claw.memory_extraction import extract_fact_candidates
from coatue_claw.memory_store import FactCandidate, MemoryHit, MemoryStore

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]

try:
    import lancedb
except Exception:  # pragma: no cover - optional dependency
    lancedb = None  # type: ignore[assignment]


@dataclass(frozen=True)
class RetrievedMemory:
    source_type: str
    category: str
    entity: str
    fact_key: str
    fact_value: str
    rationale: str | None
    score: float


class SemanticMemoryIndex:
    def __init__(self) -> None:
        self.enabled = False
        self.reason = "disabled"
        self._client: Any | None = None
        self._table: Any | None = None

        if lancedb is None:
            self.reason = "lancedb_not_installed"
            return
        if OpenAI is None:
            self.reason = "openai_not_installed"
            return

        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            self.reason = "openai_api_key_missing"
            return

        try:
            data_root = os.environ.get("COATUE_CLAW_DATA_ROOT", "/opt/coatue-claw-data")
            uri = os.environ.get("COATUE_CLAW_MEMORY_VECTOR_DIR", f"{data_root}/db/lancedb")
            self._db = lancedb.connect(uri)
            self._client = OpenAI(api_key=api_key)
            self._model = os.environ.get("COATUE_CLAW_MEMORY_EMBED_MODEL", "text-embedding-3-small")

            tables = set(self._db.table_names())
            if "memory_facts" in tables:
                self._table = self._db.open_table("memory_facts")
            else:
                self._table = self._db.create_table(
                    "memory_facts",
                    [
                        {
                            "memory_id": 0,
                            "text": "seed",
                            "vector": [0.0] * 1536,
                            "category": "seed",
                            "entity": "seed",
                            "fact_key": "seed",
                            "fact_value": "seed",
                        }
                    ],
                    mode="overwrite",
                )
                self._table.delete("memory_id = 0")

            self.enabled = True
            self.reason = "enabled"
        except Exception as exc:  # pragma: no cover - defensive
            self.enabled = False
            self.reason = f"init_failed:{type(exc).__name__}"

    def _embed(self, text: str) -> list[float]:
        if not self._client:
            raise RuntimeError("OpenAI client unavailable")
        response = self._client.embeddings.create(model=self._model, input=text)
        return list(response.data[0].embedding)

    def upsert(self, *, memory_id: int, candidate: FactCandidate) -> None:
        if not self.enabled or not self._table:
            return
        try:
            text = f"{candidate.category} {candidate.entity} {candidate.fact_key} {candidate.fact_value} {candidate.rationale or ''}".strip()
            vector = self._embed(text)
            self._table.delete(f"memory_id = {memory_id}")
            self._table.add(
                [
                    {
                        "memory_id": memory_id,
                        "text": text,
                        "vector": vector,
                        "category": candidate.category,
                        "entity": candidate.entity,
                        "fact_key": candidate.fact_key,
                        "fact_value": candidate.fact_value,
                    }
                ]
            )
        except Exception:
            return

    def query(self, text: str, *, limit: int = 3) -> list[RetrievedMemory]:
        if not self.enabled or not self._table:
            return []
        try:
            vector = self._embed(text)
            rows = self._table.search(vector).limit(limit).to_list()
        except Exception:
            return []

        out: list[RetrievedMemory] = []
        for row in rows:
            out.append(
                RetrievedMemory(
                    source_type="semantic",
                    category=str(row.get("category") or "unknown"),
                    entity=str(row.get("entity") or "unknown"),
                    fact_key=str(row.get("fact_key") or ""),
                    fact_value=str(row.get("fact_value") or ""),
                    rationale=None,
                    score=float(row.get("_distance") or 0.0),
                )
            )
        return out


class MemoryRuntime:
    def __init__(self, store: MemoryStore | None = None, semantic: SemanticMemoryIndex | None = None) -> None:
        self.store = store or MemoryStore()
        self.semantic = semantic or SemanticMemoryIndex()

    def ingest_message(
        self,
        *,
        channel: str | None,
        user_id: str | None,
        text: str,
        source: str,
        source_ts_utc: str | None = None,
    ) -> list[int]:
        if not text.strip():
            return []

        source_ts = source_ts_utc or datetime.now(UTC).isoformat()
        self.store.log_event(
            channel=channel,
            user_id=user_id,
            text=text,
            source=source,
            source_ts_utc=source_ts,
        )

        candidates = extract_fact_candidates(
            text,
            source=source,
            source_ts_utc=source_ts,
            default_entity=(user_id or "user"),
        )
        if not candidates:
            return []

        ids: list[int] = []
        for candidate in candidates:
            memory_id = self.store.upsert_fact(candidate)
            self.semantic.upsert(memory_id=memory_id, candidate=candidate)
            ids.append(memory_id)
        return ids

    def retrieve(self, query: str, *, structured_limit: int = 5, semantic_limit: int = 3) -> list[RetrievedMemory]:
        structured_hits = self.store.query_structured(query, limit=structured_limit)
        structured = [
            RetrievedMemory(
                source_type="structured",
                category=hit.category,
                entity=hit.entity,
                fact_key=hit.fact_key,
                fact_value=hit.fact_value,
                rationale=hit.rationale,
                score=hit.score,
            )
            for hit in structured_hits
        ]

        semantic = self.semantic.query(query, limit=semantic_limit)

        dedupe: dict[tuple[str, str, str], RetrievedMemory] = {}
        for hit in structured + semantic:
            key = (hit.entity.lower(), hit.fact_key.lower(), hit.fact_value.lower())
            if key in dedupe:
                existing = dedupe[key]
                if hit.source_type == "structured" and existing.source_type != "structured":
                    dedupe[key] = hit
                continue
            dedupe[key] = hit

        out = list(dedupe.values())
        out.sort(key=lambda item: (0 if item.source_type == "structured" else 1, item.score))
        return out

    def format_retrieval(self, query: str, *, limit: int = 6) -> str:
        hits = self.retrieve(query, structured_limit=limit, semantic_limit=max(1, limit // 2))
        if not hits:
            return "I couldn't find a matching memory yet."

        lines = [f"Memory results for `{query}`:"]
        for hit in hits[:limit]:
            rationale = f" (why: {hit.rationale})" if hit.rationale else ""
            lines.append(
                f"- [{hit.source_type}] {hit.entity} -> {hit.fact_key}: {hit.fact_value}{rationale}"
            )
        return "\n".join(lines)

    def extract_daily(self, *, days: int, dry_run: bool) -> dict[str, Any]:
        events = self.store.events_since_days(days)
        extracted: list[FactCandidate] = []

        for event in events:
            extracted.extend(
                extract_fact_candidates(
                    str(event.get("text") or ""),
                    source=str(event.get("source") or "daily-scan"),
                    source_ts_utc=str(event.get("created_at_utc") or datetime.now(UTC).isoformat()),
                    default_entity=str(event.get("user_id") or "user"),
                )
            )

        dedupe: dict[tuple[str, str, str, str], FactCandidate] = {}
        for candidate in extracted:
            key = (
                candidate.category.lower(),
                candidate.entity.lower(),
                candidate.fact_key.lower(),
                candidate.fact_value.lower(),
            )
            dedupe[key] = candidate
        candidates = list(dedupe.values())

        inserted_ids: list[int] = []
        if not dry_run:
            for candidate in candidates:
                memory_id = self.store.upsert_fact(candidate)
                self.semantic.upsert(memory_id=memory_id, candidate=candidate)
                inserted_ids.append(memory_id)

        return {
            "events_scanned": len(events),
            "facts_extracted": len(candidates),
            "inserted": len(inserted_ids),
            "dry_run": dry_run,
            "semantic_enabled": self.semantic.enabled,
            "semantic_reason": self.semantic.reason,
        }

    def stats(self) -> dict[str, Any]:
        snapshot = self.store.stats()
        return {
            "facts_total": snapshot.facts_total,
            "facts_by_tier": snapshot.facts_by_tier,
            "checkpoints_total": snapshot.checkpoints_total,
            "events_total": snapshot.events_total,
            "semantic_enabled": self.semantic.enabled,
            "semantic_reason": self.semantic.reason,
        }

    def write_checkpoint(
        self,
        *,
        scope: str,
        action: str,
        state: dict[str, Any],
        expected_outcome: str,
        files: list[str],
        source: str,
        source_ts_utc: str,
    ) -> int:
        return self.store.write_checkpoint(
            scope=scope,
            action=action,
            state=state,
            expected_outcome=expected_outcome,
            files=files,
            source=source,
            source_ts_utc=source_ts_utc,
        )

    def latest_checkpoint_summary(self, *, scope: str | None = None) -> str:
        checkpoint = self.store.latest_checkpoint(scope=scope)
        if checkpoint is None:
            return "No active checkpoint found."
        return (
            f"Latest checkpoint (`{checkpoint.scope}`):\n"
            f"- action: `{checkpoint.action}`\n"
            f"- created_at_utc: `{checkpoint.created_at_utc}`\n"
            f"- expected_outcome: `{checkpoint.expected_outcome}`\n"
            f"- files: `{checkpoint.files_json}`"
        )

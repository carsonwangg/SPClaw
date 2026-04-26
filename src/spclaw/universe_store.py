from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
import re


UNIVERSE_DIR = Path("/opt/spclaw-data/db/universes")
CSV_FIELDS = ["ticker", "added_at_utc", "source", "notes"]

_TICKER_STOPWORDS = {
    "AND",
    "OR",
    "THE",
    "WITH",
    "FOR",
    "TO",
    "IN",
    "UNIVERSE",
    "CREATE",
    "MAKE",
    "BUILD",
    "LIST",
    "SHOW",
    "ADD",
    "REMOVE",
    "FROM",
    "WITH",
    "USE",
    "ONLINE",
    "INCLUDE",
    "EXCLUDE",
    "CHART",
    "GRAPH",
    "PLOT",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        raise ValueError("Universe name cannot be empty")
    return slug


def _normalize_ticker(raw: str) -> str | None:
    ticker = raw.upper().lstrip("$").strip(".,;:!?)]} ")
    core = ticker.replace(".", "").replace("-", "")
    if not core or len(core) > 5 or not core.isalpha():
        return None
    if ticker in _TICKER_STOPWORDS:
        return None
    return ticker


def _ensure_dir() -> None:
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)


def universe_path(name: str) -> Path:
    _ensure_dir()
    return UNIVERSE_DIR / f"{_slugify(name)}.csv"


def list_universes() -> list[str]:
    _ensure_dir()
    return sorted(p.stem for p in UNIVERSE_DIR.glob("*.csv"))


def parse_tickers(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for candidate in re.findall(r"\$?[A-Za-z][A-Za-z.\-]{0,9}", text or ""):
        ticker = _normalize_ticker(candidate)
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def load_universe(name: str) -> list[str]:
    path = universe_path(name)
    if not path.exists():
        return []
    out: list[str] = []
    seen: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = _normalize_ticker(row.get("ticker", ""))
            if ticker and ticker not in seen:
                seen.add(ticker)
                out.append(ticker)
    return out


def save_universe(name: str, tickers: list[str], *, source: str = "manual", notes: str = "") -> Path:
    path = universe_path(name)
    unique = []
    seen: set[str] = set()
    for ticker in tickers:
        norm = _normalize_ticker(ticker)
        if norm and norm not in seen:
            seen.add(norm)
            unique.append(norm)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for ticker in unique:
            writer.writerow(
                {
                    "ticker": ticker,
                    "added_at_utc": _utc_now_iso(),
                    "source": source,
                    "notes": notes,
                }
            )
    return path


def add_to_universe(name: str, tickers: list[str], *, source: str = "manual", notes: str = "") -> tuple[Path, list[str]]:
    existing = load_universe(name)
    existing_set = {t.upper() for t in existing}
    added = []
    for ticker in tickers:
        norm = _normalize_ticker(ticker)
        if norm and norm not in existing_set:
            existing.append(norm)
            existing_set.add(norm)
            added.append(norm)
    path = save_universe(name, existing, source=source, notes=notes)
    return path, added


def remove_from_universe(name: str, tickers: list[str], *, source: str = "manual", notes: str = "") -> tuple[Path, list[str]]:
    existing = load_universe(name)
    remove_set = {t for t in (_normalize_ticker(x) for x in tickers) if t}
    kept = [t for t in existing if t not in remove_set]
    removed = [t for t in existing if t in remove_set]
    path = save_universe(name, kept, source=source, notes=notes)
    return path, removed


def find_relevant_universe_name(text: str) -> str | None:
    text_l = (text or "").lower()
    names = list_universes()
    if not names:
        return None
    for name in names:
        if name.lower() in text_l:
            return name
    tokens = set(re.findall(r"[a-z0-9]+", text_l))
    best = None
    best_score = 0
    for name in names:
        parts = set(name.lower().split("-"))
        score = len(tokens & parts)
        if score > best_score:
            best = name
            best_score = score
    return best

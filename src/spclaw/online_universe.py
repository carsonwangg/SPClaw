from __future__ import annotations

import re

import yfinance as yf

from spclaw.universe_store import parse_tickers


_GOOD_QUOTE_TYPES = {"EQUITY", "ETF"}


def discover_online_tickers(query: str, limit: int = 8) -> list[str]:
    raw = (query or "").strip()
    if not raw:
        return []
    try:
        search = yf.Search(raw, max_results=max(20, limit * 4))
        quotes = getattr(search, "quotes", None) or []
    except Exception:
        quotes = []

    out: list[str] = []
    seen: set[str] = set()
    for quote in quotes:
        if not isinstance(quote, dict):
            continue
        symbol = str(quote.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        qtype = str(quote.get("quoteType") or "").upper()
        if qtype and qtype not in _GOOD_QUOTE_TYPES:
            continue
        if re.search(r"[^A-Z.\-]", symbol):
            continue
        core = symbol.replace(".", "").replace("-", "")
        if not core.isalpha() or len(core) > 5:
            continue
        if symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
        if len(out) >= limit:
            break

    if out:
        return out
    # Fallback to ticker-like token extraction from query text.
    return parse_tickers(raw)[:limit]


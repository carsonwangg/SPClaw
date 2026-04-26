from __future__ import annotations

import re


_SMALL_WORDS = {"and", "or", "of", "the", "for", "to", "vs", "with"}


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def _title_case(value: str) -> str:
    words = re.findall(r"[A-Za-z0-9&/\-']+", value)
    if not words:
        return ""
    out: list[str] = []
    for i, w in enumerate(words):
        lw = w.lower()
        if i > 0 and lw in _SMALL_WORDS:
            out.append(lw)
        else:
            out.append(lw.capitalize())
    return " ".join(out)


def _humanize_slug(value: str) -> str:
    text = value.replace("_", " ").replace("-", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return _title_case(text)


def _from_source_label(source_label: str | None) -> str | None:
    if not source_label:
        return None
    if source_label.startswith("universe:"):
        name = source_label.split(":", 1)[1].strip()
        if not name:
            return None
        return f"{_humanize_slug(name)} Universe"
    if source_label.startswith("online:"):
        query = source_label.split(":", 1)[1].strip()
        if not query:
            return None
        return _title_case(query)
    return None


def _from_prompt(prompt_text: str) -> str | None:
    text = _strip_slack_mentions(prompt_text).lower()
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    m = re.search(r"\bfor\s+([a-z0-9&/\- ]{3,80}?(?:stocks?|companies|peers|basket|universe))\b", text)
    if m:
        phrase = m.group(1).strip()
        return _title_case(phrase)

    m = re.search(r"\bfor\s+([a-z0-9&/\- ]{3,64})$", text)
    if m:
        phrase = m.group(1).strip()
        if not re.fullmatch(r"[a-z0-9,\-.$ ]+", phrase):
            return _title_case(phrase)

    m = re.search(r"\b([a-z0-9&/\- ]{3,40}\s+stocks?)\b", text)
    if m:
        return _title_case(m.group(1))

    return None


def infer_chart_title_context(prompt_text: str, source_label: str | None = None) -> str | None:
    from_prompt = _from_prompt(prompt_text)
    if from_prompt:
        return from_prompt
    return _from_source_label(source_label)


from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class DevBuzzIntent:
    kind: str
    value: str | None = None


def _strip_slack_mentions(text: str) -> str:
    return re.sub(r"<@[^>]+>", " ", text or "").strip()


def parse_dev_buzz_intent(text: str) -> DevBuzzIntent | None:
    stripped = _strip_slack_mentions(text)
    cleaned = re.sub(r"\s+", " ", stripped).strip()
    lower = cleaned.lower()
    if not lower.startswith("dev buzz"):
        return None

    rest = cleaned[len("dev buzz") :].strip()
    rest_lower = rest.lower()
    if not rest or rest_lower == "help":
        return DevBuzzIntent(kind="help")
    if rest_lower == "status":
        return DevBuzzIntent(kind="status")
    if rest_lower == "shortlist":
        return DevBuzzIntent(kind="shortlist")
    if rest_lower in {"collect now", "collect", "run now"}:
        return DevBuzzIntent(kind="collect")
    if rest_lower in {"dry run", "dry-run", "publish dry run", "publish dry-run"}:
        return DevBuzzIntent(kind="publish_dry_run")
    if rest_lower in {"publish now", "publish", "post now"}:
        return DevBuzzIntent(kind="publish_force")

    m = re.fullmatch(r"add source\s+(@?[A-Za-z0-9_]{1,30})", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="add_source", value=m.group(1))
    m = re.fullmatch(r"remove source\s+(@?[A-Za-z0-9_]{1,30})", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="remove_source", value=m.group(1))
    m = re.fullmatch(r"add keyword\s+(.+)", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="add_keyword", value=m.group(1).strip())
    m = re.fullmatch(r"remove keyword\s+(.+)", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="remove_keyword", value=m.group(1).strip())
    m = re.fullmatch(r"pin\s+([A-Za-z0-9_-]+)", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="pin", value=m.group(1))
    m = re.fullmatch(r"drop\s+([A-Za-z0-9_-]+)", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="drop", value=m.group(1))
    m = re.fullmatch(r"explain\s+([A-Za-z0-9_-]+)", rest, re.IGNORECASE)
    if m:
        return DevBuzzIntent(kind="explain", value=m.group(1))

    return DevBuzzIntent(kind="help")

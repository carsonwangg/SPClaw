from __future__ import annotations

import re


_USER_MENTION_RE = re.compile(r"<@([A-Z0-9]+)>")


def extract_user_mentions(text: str) -> list[str]:
    return _USER_MENTION_RE.findall(text or "")


def should_default_route_message(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False
    # If any explicit @-user mention exists, this message is not default-routed.
    return len(extract_user_mentions(stripped)) == 0


def should_route_message_event(*, text: str, channel_type: str | None) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False
    # In a direct message with the app, always route the message even if it contains <@bot>.
    if (channel_type or "").strip().lower() == "im":
        return True
    return should_default_route_message(stripped)


def is_explicit_hfa_command(text: str) -> bool:
    stripped = re.sub(r"<@[^>]+>", " ", text or "").strip()
    if not stripped:
        return False
    lower = stripped.lower()
    if re.search(r"^\s*hfa\b", lower):
        return True
    if re.search(r"^\s*(analyze|quotes?|podcast|status)\b", lower):
        return True
    return False

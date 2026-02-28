from __future__ import annotations

import re


_USER_MENTION_RE = re.compile(r"<@([A-Z0-9]+)>")
_SLACK_MENTION_RE = re.compile(r"<@[^>]+>")


def extract_user_mentions(text: str) -> list[str]:
    return _USER_MENTION_RE.findall(text or "")


def is_explicit_board_seat_command(text: str) -> bool:
    stripped = _SLACK_MENTION_RE.sub(" ", text or "").strip().lower()
    if not stripped:
        return False
    return bool(re.match(r"^(bs|board seat)\b", stripped))


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

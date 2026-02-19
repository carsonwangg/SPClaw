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


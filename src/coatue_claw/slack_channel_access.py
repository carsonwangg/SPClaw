from __future__ import annotations

from typing import Any


def parse_created_channel_id(event: dict[str, Any]) -> str | None:
    channel = event.get("channel")
    if not isinstance(channel, dict):
        return None
    channel_id = str(channel.get("id") or "").strip()
    return channel_id or None


def channels_to_join(channels: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in channels:
        if not isinstance(item, dict):
            continue
        channel_id = str(item.get("id") or "").strip()
        if not channel_id or channel_id in seen:
            continue
        if bool(item.get("is_archived")):
            continue
        if bool(item.get("is_member")):
            continue
        is_private = bool(item.get("is_private"))
        if is_private:
            continue
        seen.add(channel_id)
        out.append(channel_id)
    return out

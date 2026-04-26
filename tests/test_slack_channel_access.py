from __future__ import annotations

from spclaw.slack_channel_access import channels_to_join, parse_created_channel_id


def test_parse_created_channel_id() -> None:
    event = {"channel": {"id": "C123", "name": "new-room"}}
    assert parse_created_channel_id(event) == "C123"
    assert parse_created_channel_id({"channel": {}}) is None
    assert parse_created_channel_id({}) is None


def test_channels_to_join_filters_member_private_archived() -> None:
    channels = [
        {"id": "C1", "is_member": False, "is_private": False, "is_archived": False},
        {"id": "C2", "is_member": True, "is_private": False, "is_archived": False},
        {"id": "C3", "is_member": False, "is_private": True, "is_archived": False},
        {"id": "C4", "is_member": False, "is_private": False, "is_archived": True},
        {"id": "C1", "is_member": False, "is_private": False, "is_archived": False},
    ]
    assert channels_to_join(channels) == ["C1"]

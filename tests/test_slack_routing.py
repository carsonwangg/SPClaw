from __future__ import annotations

from coatue_claw.slack_routing import (
    extract_user_mentions,
    should_default_route_message,
    should_route_message_event,
)


def test_extract_user_mentions() -> None:
    text = "review this <@U123ABC> and <@U999XYZ>"
    assert extract_user_mentions(text) == ["U123ABC", "U999XYZ"]


def test_should_default_route_message_plain_text() -> None:
    assert should_default_route_message("diligence SNOW")
    assert should_default_route_message("what changed since last quarter?")


def test_should_default_route_message_empty_or_mentions() -> None:
    assert not should_default_route_message("")
    assert not should_default_route_message("   ")
    assert not should_default_route_message("check with <@U123ABC> first")
    # App mentions are handled by app_mention flow, not default routing.
    assert not should_default_route_message("hey <@U0AFFR9Q11B> run diligence SNOW")


def test_should_route_message_event_im_always_routes_nonempty() -> None:
    assert should_route_message_event(text="hello", channel_type="im")
    assert should_route_message_event(text="<@U0AFFR9Q11B> hello", channel_type="im")
    assert not should_route_message_event(text="   ", channel_type="im")


def test_should_route_message_event_non_im_follows_default_rules() -> None:
    assert should_route_message_event(text="run diligence SNOW", channel_type="channel")
    assert not should_route_message_event(text="<@U0AFFR9Q11B> run diligence SNOW", channel_type="channel")

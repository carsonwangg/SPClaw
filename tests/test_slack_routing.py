from __future__ import annotations

from coatue_claw.slack_routing import (
    extract_user_mentions,
    is_explicit_board_seat_command,
    is_explicit_hfa_command,
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


def test_is_explicit_hfa_command_prefixed_and_bare() -> None:
    assert is_explicit_hfa_command("hfa analyze")
    assert is_explicit_hfa_command("hfa status")
    assert is_explicit_hfa_command("analyze https://youtu.be/abcDEF12345")
    assert is_explicit_hfa_command("quotes https://youtu.be/abcDEF12345")
    assert is_explicit_hfa_command("podcast https://youtu.be/abcDEF12345")
    assert is_explicit_hfa_command("status")


def test_is_explicit_hfa_command_with_slack_mention_prefix() -> None:
    assert is_explicit_hfa_command("<@U0AFFR9Q11B> hfa analyze")
    assert is_explicit_hfa_command("<@U0AFFR9Q11B> analyze https://youtu.be/abcDEF12345")


def test_is_explicit_hfa_command_ignores_non_hfa_commands() -> None:
    assert not is_explicit_hfa_command("md status")
    assert not is_explicit_hfa_command("bs now")
    assert not is_explicit_hfa_command("x chart from https://x.com/foo/status/123")


def test_is_explicit_board_seat_command_true() -> None:
    assert is_explicit_board_seat_command("bs status")
    assert is_explicit_board_seat_command("bs now")
    assert is_explicit_board_seat_command("board seat status")
    assert is_explicit_board_seat_command("<@U0AFFR9Q11B> bs now")


def test_is_explicit_board_seat_command_false() -> None:
    assert not is_explicit_board_seat_command("md status")
    assert not is_explicit_board_seat_command("hfa analyze")
    assert not is_explicit_board_seat_command("x chart from https://x.com/foo/status/123")
